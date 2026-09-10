package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

const DefaultTargetFileSize = uint64(4 << 20)

type Options struct {
	Directory      string
	L0Trigger      int
	TargetFileSize uint64
	SSTableOptions sstable.Options
	Clock          clock.Clock
	Logger         *slog.Logger
}
type Stats struct{ Started, Succeeded, Failed, Stale, InputFiles, OutputFiles, InputBytes, OutputBytes, Entries, Nanos, OrphanOutputs uint64 }
type Result struct {
	JobID                            uint64
	State                            State
	Inputs, Outputs                  []manifest.TableMetadata
	InputBytes, OutputBytes, Entries uint64
}

type outputWriter interface {
	Add(storage.InternalKey, []byte) error
	Finish() (sstable.Metadata, error)
	Abort() error
}
type outputFactory func(string, uint64, sstable.Options) (outputWriter, error)

// Executor serializes compaction attempts while allowing Version installation to remain independently serialized.
type Executor struct {
	mu            sync.Mutex
	options       Options
	store         *manifest.Store
	picker        Picker
	nextJob       uint64
	stats         Stats
	beforeInstall func() error
	openOutput    outputFactory
}

func New(options Options, store *manifest.Store) (*Executor, error) {
	if options.Directory == "" || store == nil {
		return nil, ErrInvalidOptions
	}
	if options.TargetFileSize == 0 {
		options.TargetFileSize = DefaultTargetFileSize
	}
	if options.L0Trigger == 0 {
		options.L0Trigger = DefaultL0Trigger
	}
	if options.L0Trigger < 1 || options.TargetFileSize > math.MaxInt64 {
		return nil, ErrInvalidOptions
	}
	if options.Clock == nil {
		options.Clock = clock.System()
	}
	if options.Logger == nil {
		options.Logger = rlog.Discard()
	}
	return &Executor{options: options, store: store, picker: Picker{L0Trigger: options.L0Trigger}, nextJob: 1, openOutput: func(directory string, number uint64, options sstable.Options) (outputWriter, error) {
		return sstable.OpenWriter(directory, number, options)
	}}, nil
}
func (e *Executor) Stats() Stats { e.mu.Lock(); defer e.mu.Unlock(); return e.stats }
func (e *Executor) Pick() (*Plan, error) {
	version, err := e.store.Current()
	if err != nil {
		return nil, fmt.Errorf("read current Version: %w", err)
	}
	return e.picker.Pick(version)
}
func (e *Executor) CompactOnce(ctx context.Context) (Result, error) {
	plan, err := e.Pick()
	if err != nil {
		return Result{}, err
	}
	if plan == nil {
		return Result{}, ErrNoCompaction
	}
	return e.Run(ctx, plan)
}
func (e *Executor) Run(ctx context.Context, plan *Plan) (Result, error) {
	if ctx == nil || plan == nil {
		return Result{}, ErrInvalidOptions
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	job := e.nextJob
	e.nextJob++
	result := Result{JobID: job, State: StatePlanned, Inputs: plan.Inputs()}
	e.stats.Started++
	started := e.options.Clock.Now()
	e.options.Logger.Info("compaction started", slog.Uint64("job", job), slog.Uint64("source_level", uint64(plan.SourceLevel)), slog.Uint64("target_level", uint64(plan.TargetLevel)))
	fail := func(err error) (Result, error) {
		result.State = StateFailed
		e.stats.Failed++
		elapsed := e.options.Clock.Since(started)
		if elapsed > 0 {
			e.stats.Nanos += uint64(elapsed) //nolint:gosec // positive duration fits uint64
		}
		e.options.Logger.Error("compaction failed", slog.Uint64("job", job), rlog.Err(err))
		return result, err
	}
	if !validTransition(result.State, StateRunning) {
		return fail(ErrInvalidTransition)
	}
	result.State = StateRunning
	current, err := e.store.Current()
	if err != nil {
		return fail(err)
	}
	if validateErr := plan.Validate(current); validateErr != nil {
		e.stats.Stale++
		result.State = StateStale
		return result, errors.Join(ErrStalePlan, validateErr)
	}
	readers, merge, inputBytes, err := e.openInputs(plan.Inputs())
	if err != nil {
		return fail(err)
	}
	defer func() {
		if closeErr := closeReaders(readers); closeErr != nil {
			e.options.Logger.Error("close compaction inputs", rlog.Err(closeErr))
		}
	}()
	result.InputBytes = inputBytes
	outputs, entries, outputBytes, err := e.buildOutputs(ctx, job, plan.TargetLevel, merge)
	closeErr := merge.Close()
	if err != nil || closeErr != nil {
		result.Outputs = outputs
		e.stats.OrphanOutputs += uint64(len(outputs))
		return fail(errors.Join(err, closeErr))
	}
	result.Outputs, result.Entries, result.OutputBytes = outputs, entries, outputBytes
	result.State = StateOutputDurable
	if e.beforeInstall != nil {
		if hookErr := e.beforeInstall(); hookErr != nil {
			e.stats.OrphanOutputs += uint64(len(outputs))
			return fail(hookErr)
		}
	}
	if !validTransition(result.State, StateInstalling) {
		return fail(ErrInvalidTransition)
	}
	result.State = StateInstalling
	if err := e.store.InstallCompaction(ctx, plan.Inputs(), outputs); err != nil {
		e.stats.OrphanOutputs += uint64(len(outputs))
		if errors.Is(err, manifest.ErrStaleVersion) {
			result.State = StateStale
			e.stats.Stale++
			return result, errors.Join(ErrStalePlan, err)
		}
		return fail(err)
	}
	result.State = StateInstalled
	e.stats.Succeeded++
	e.stats.InputFiles += uint64(len(result.Inputs))
	e.stats.OutputFiles += uint64(len(outputs))
	e.stats.InputBytes += inputBytes
	e.stats.OutputBytes += outputBytes
	e.stats.Entries += entries
	elapsed := e.options.Clock.Since(started)
	if elapsed > 0 {
		e.stats.Nanos += uint64(elapsed) //nolint:gosec // positive duration fits uint64
	}
	e.options.Logger.Info("compaction installed", slog.Uint64("job", job), slog.Uint64("entries", entries), slog.Uint64("bytes", outputBytes))
	return result, nil
}

func (e *Executor) openInputs(tables []manifest.TableMetadata) ([]*sstable.Reader, *MergeIterator, uint64, error) {
	readers := make([]*sstable.Reader, 0, len(tables))
	inputs := make([]MergeInput, 0, len(tables))
	var total uint64
	for _, table := range tables {
		path := filepath.Join(e.options.Directory, sstable.FileName(table.FileNumber))
		reader, err := sstable.Open(path, sstable.ReaderOptions{})
		if err != nil {
			return nil, nil, 0, errors.Join(fmt.Errorf("%w: open file %d: %w", ErrInputCorrupt, table.FileNumber, err), closeReaders(readers))
		}
		if !metadataMatches(table, reader.Metadata()) {
			return nil, nil, 0, errors.Join(fmt.Errorf("%w: metadata file %d", ErrInputCorrupt, table.FileNumber), reader.Close(), closeReaders(readers))
		}
		iterator, err := reader.NewIterator()
		if err != nil {
			return nil, nil, 0, errors.Join(fmt.Errorf("iterate input %d: %w", table.FileNumber, err), reader.Close(), closeReaders(readers))
		}
		readers = append(readers, reader)
		inputs = append(inputs, MergeInput{FileNumber: table.FileNumber, Iterator: iterator})
		total += table.FileSize
	}
	merge, err := NewMergeIterator(inputs)
	if err != nil {
		return nil, nil, 0, errors.Join(err, closeReaders(readers))
	}
	return readers, merge, total, nil
}

func (e *Executor) buildOutputs(ctx context.Context, job uint64, level uint32, merge *MergeIterator) ([]manifest.TableMetadata, uint64, uint64, error) {
	var outputs []manifest.TableMetadata
	var writer outputWriter
	var logical, entries, total uint64
	var smallestSeq, largestSeq uint64
	var haveSeq bool
	var lastUser []byte
	var previous storage.InternalKey
	var havePrevious bool
	finish := func() error {
		if writer == nil {
			return nil
		}
		metadata, err := writer.Finish()
		if err != nil {
			return fmt.Errorf("finish compaction output: %w", err)
		}
		path := filepath.Join(e.options.Directory, sstable.FileName(metadata.FileNumber))
		reader, err := sstable.Open(path, sstable.ReaderOptions{})
		if err != nil {
			return fmt.Errorf("validate output %d: %w", metadata.FileNumber, err)
		}
		actual := reader.Metadata()
		closeErr := reader.Close()
		if closeErr != nil {
			return fmt.Errorf("close validated output: %w", closeErr)
		}
		if !sstableMetadataEqual(metadata, actual) {
			return ErrInputCorrupt
		}
		table, err := manifest.NewTableMetadata(level, job, smallestSeq, largestSeq, metadata)
		if err != nil {
			return fmt.Errorf("construct output metadata: %w", err)
		}
		outputs = append(outputs, table)
		total += metadata.FileSize
		writer = nil
		logical = 0
		haveSeq = false
		return nil
	}
	for merge.Next() {
		if err := ctx.Err(); err != nil {
			if writer != nil {
				if abortErr := writer.Abort(); abortErr != nil {
					return outputs, entries, total, errors.Join(fmt.Errorf("compaction canceled: %w", err), abortErr)
				}
			}
			return outputs, entries, total, fmt.Errorf("compaction canceled: %w", err)
		}
		entry, ok := merge.Entry()
		if !ok {
			return outputs, entries, total, ErrInputCorrupt
		}
		if havePrevious && storage.CompareInternal(previous, entry.Key) == 0 {
			if writer != nil {
				if abortErr := writer.Abort(); abortErr != nil {
					return outputs, entries, total, errors.Join(ErrDuplicateInternalKey, abortErr)
				}
			}
			return outputs, entries, total, ErrDuplicateInternalKey
		}
		user := entry.Key.UserKey()
		if writer != nil && logical >= e.options.TargetFileSize && !bytes.Equal(lastUser, user) {
			if err := finish(); err != nil {
				return outputs, entries, total, fmt.Errorf("allocate compaction output: %w", err)
			}
		}
		if writer == nil {
			number, err := e.store.AllocateFileNumber(ctx)
			if err != nil {
				return outputs, entries, total, fmt.Errorf("allocate compaction output: %w", err)
			}
			writer, err = e.openOutput(e.options.Directory, number, e.options.SSTableOptions)
			if err != nil {
				return outputs, entries, total, fmt.Errorf("open compaction output: %w", err)
			}
		}
		if err := writer.Add(entry.Key, entry.Value); err != nil {
			return outputs, entries, total, errors.Join(fmt.Errorf("add compaction output entry: %w", err), writer.Abort())
		}
		sequence := entry.Key.Sequence()
		if !haveSeq {
			smallestSeq, largestSeq, haveSeq = sequence, sequence, true
		} else {
			smallestSeq = min(smallestSeq, sequence)
			largestSeq = max(largestSeq, sequence)
		}
		logical += uint64(len(entry.Key.Encode()) + len(entry.Value)) //nolint:gosec // slice lengths are nonnegative
		entries++
		lastUser = user
		previous = entry.Key
		havePrevious = true
	}
	if err := merge.Error(); err != nil {
		if writer != nil {
			if abortErr := writer.Abort(); abortErr != nil {
				return outputs, entries, total, errors.Join(err, abortErr)
			}
		}
		return outputs, entries, total, err
	}
	if err := finish(); err != nil {
		return outputs, entries, total, err
	}
	return outputs, entries, total, nil
}

func metadataMatches(table manifest.TableMetadata, actual sstable.Metadata) bool {
	return table.FileNumber == actual.FileNumber && table.FileSize == actual.FileSize && table.EntryCount == actual.EntryCount && table.DeletionCount == actual.DeletionCount && table.RawKeyValueBytes == actual.RawKeyValueBytes && table.DataBlockCount == actual.DataBlockCount && storage.CompareInternal(table.SmallestInternal, actual.SmallestInternal) == 0 && storage.CompareInternal(table.LargestInternal, actual.LargestInternal) == 0 && bytes.Equal(table.SmallestUser, actual.SmallestUser) && bytes.Equal(table.LargestUser, actual.LargestUser)
}
func sstableMetadataEqual(a, b sstable.Metadata) bool {
	return a.FileNumber == b.FileNumber && a.FileSize == b.FileSize && a.EntryCount == b.EntryCount && a.DeletionCount == b.DeletionCount && a.RawKeyValueBytes == b.RawKeyValueBytes && a.DataBlockCount == b.DataBlockCount && storage.CompareInternal(a.SmallestInternal, b.SmallestInternal) == 0 && storage.CompareInternal(a.LargestInternal, b.LargestInternal) == 0 && bytes.Equal(a.SmallestUser, b.SmallestUser) && bytes.Equal(a.LargestUser, b.LargestUser)
}
func closeReaders(readers []*sstable.Reader) error {
	var errs []error
	for _, reader := range readers {
		if err := reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
