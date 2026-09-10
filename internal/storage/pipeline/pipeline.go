package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

const WALFileName = "000000000001.wal"

// Options configures the bounded Phase 1F pipeline.
type Options struct {
	Directory       string
	MemTableBytes   uint64
	MaxImmutables   int
	NextSequence    uint64
	FirstGeneration uint64
	FirstFileNumber uint64
	SSTableOptions  sstable.Options
	Clock           clock.Clock
	Logger          *slog.Logger
	FileAllocator   FileAllocator
	TableInstaller  TableInstaller
	// RecoveredTable is WAL-replayed state installed as the initial active
	// MemTable. Open never writes its contents back to the WAL.
	RecoveredTable    *memtable.MemTable
	SequenceExhausted bool
}

type durableWAL interface {
	Append([]byte) (wal.Position, error)
	Close() error
}

// FileAllocator durably reserves an SSTable identity before publication.
type FileAllocator interface {
	AllocateFileNumber(context.Context) (uint64, error)
}

// TableInstaller makes a physically durable table logically authoritative.
type TableInstaller interface {
	InstallTable(context.Context, TableInstallation) error
}

type flushResult struct {
	metadata  sstable.Metadata
	path      string
	ambiguous bool
}

type flushExecutor func(context.Context, *generation) (flushResult, error)

// WriteResult records the assigned sequence range and active generation.
type WriteResult struct {
	FirstSequence uint64
	LastSequence  uint64
	Generation    uint64
}

// Pipeline owns one synchronous WAL writer, one active MemTable and one FIFO
// immutable flush worker.
type Pipeline struct {
	mu         sync.Mutex
	closeMu    sync.Mutex
	writeToken chan struct{}
	wake       chan struct{}
	changed    chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	wal            durableWAL
	directory      string
	threshold      uint64
	maxImmutable   int
	active         *generation
	immutables     []*generation
	outputs        []FlushOutput
	nextSequence   uint64
	seqExhausted   bool
	nextGeneration uint64
	nextFile       uint64
	flush          flushExecutor
	allocator      FileAllocator
	installer      TableInstaller
	clock          clock.Clock
	logger         *slog.Logger
	stats          Stats
	accepting      bool
	closed         bool
	closeErr       error
}

// Open constructs a pipeline using a SyncBatch WAL and the Phase 1D writer.
// The directory must already exist and be exclusively owned by the caller.
func Open(options Options) (*Pipeline, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	logWriter, err := wal.OpenWriter(filepath.Join(options.Directory, WALFileName), wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		return nil, fmt.Errorf("open pipeline WAL: %w", err)
	}
	p, err := newPipeline(options, logWriter, nil)
	if err != nil {
		closeErr := logWriter.Close()
		return nil, errors.Join(err, closeErr)
	}
	return p, nil
}

func validateOptions(options Options) error {
	if options.Directory == "" || options.MemTableBytes == 0 || options.MaxImmutables < 1 ||
		options.FirstGeneration > math.MaxUint64-1 || options.FirstFileNumber > sstable.MaxFileNumber ||
		options.RecoveredTable != nil && options.RecoveredTable.Frozen() ||
		options.SequenceExhausted && options.NextSequence != 0 {
		return ErrInvalidOptions
	}
	if info, err := os.Stat(options.Directory); err != nil || !info.IsDir() {
		return fmt.Errorf("%w: storage directory", ErrInvalidOptions)
	}
	return nil
}

func newPipeline(options Options, logWriter durableWAL, executor flushExecutor) (*Pipeline, error) {
	if logWriter == nil {
		return nil, ErrInvalidOptions
	}
	if options.Clock == nil {
		options.Clock = clock.System()
	}
	if options.Logger == nil {
		options.Logger = rlog.Discard()
	}
	firstGeneration := options.FirstGeneration
	if firstGeneration == 0 {
		firstGeneration = 1
	}
	firstFile := options.FirstFileNumber
	if firstFile == 0 {
		firstFile = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		writeToken: make(chan struct{}, 1), wake: make(chan struct{}, 1), changed: make(chan struct{}, 1),
		ctx: ctx, cancel: cancel, wal: logWriter, directory: options.Directory,
		threshold: options.MemTableBytes, maxImmutable: options.MaxImmutables,
		nextSequence: options.NextSequence, seqExhausted: options.SequenceExhausted,
		nextGeneration: firstGeneration + 1, nextFile: firstFile,
		clock: options.Clock, logger: rlog.Component(options.Logger, "storage.pipeline"), accepting: true,
		allocator: options.FileAllocator, installer: options.TableInstaller,
	}
	p.writeToken <- struct{}{}
	recovered := options.RecoveredTable
	if recovered == nil {
		recovered = memtable.New()
	}
	p.active = &generation{id: firstGeneration, table: recovered, state: StateActive}
	if recovered.Len() != 0 {
		iterator := recovered.Iterator()
		for iterator.Next() {
			entry, ok := iterator.Entry()
			invariant.Assert(ok, "STORAGE-85", "recovered iterator lost current entry")
			sequence := entry.Key.Sequence()
			if !p.active.haveSeq {
				p.active.smallestSeq, p.active.largestSeq, p.active.haveSeq = sequence, sequence, true
			} else {
				p.active.smallestSeq = min(p.active.smallestSeq, sequence)
				p.active.largestSeq = max(p.active.largestSeq, sequence)
			}
		}
	}
	if executor == nil {
		p.flush = p.flushToSSTable(options.SSTableOptions)
	} else {
		p.flush = executor
	}
	p.wg.Add(1)
	go p.runWorker()
	p.logger.Info("memtable created", slog.Uint64("generation", p.active.id))
	return p, nil
}

// Write assigns, durably appends and atomically applies one nonempty batch.
func (p *Pipeline) Write(ctx context.Context, mutations []storage.Mutation) (WriteResult, error) {
	if ctx == nil {
		return WriteResult{}, ErrInvalidOptions
	}
	if err := validateMutations(mutations); err != nil {
		return WriteResult{}, err
	}
	select {
	case <-ctx.Done():
		return WriteResult{}, fmt.Errorf("wait for write admission: %w", ctx.Err())
	case <-p.writeToken:
	}
	defer func() { p.writeToken <- struct{}{} }()
	if err := p.waitForCapacity(ctx); err != nil {
		return WriteResult{}, fmt.Errorf("before WAL append: %w", err)
	}

	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return WriteResult{}, ErrClosed
	}
	if failure := p.flushFailureLocked(); failure != nil {
		p.mu.Unlock()
		return WriteResult{}, failure
	}
	if p.nextGeneration == 0 {
		p.mu.Unlock()
		return WriteResult{}, ErrGenerationExhausted
	}
	count := uint64(len(mutations)) //nolint:gosec // nonempty slice length
	if p.seqExhausted || p.nextSequence > math.MaxUint64-(count-1) {
		p.mu.Unlock()
		return WriteResult{}, storage.ErrSequenceOverflow
	}
	p.mu.Unlock()
	if err := p.ensureActiveFile(ctx); err != nil {
		return WriteResult{}, err
	}
	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return WriteResult{}, ErrClosed
	}
	if failure := p.flushFailureLocked(); failure != nil {
		p.mu.Unlock()
		return WriteResult{}, failure
	}
	first := p.nextSequence
	generationID := p.active.id
	p.mu.Unlock()

	batch := storage.WriteBatch{FirstSequence: first, Mutations: mutations}
	encoded, err := storage.EncodeWriteBatch(batch)
	if err != nil {
		return WriteResult{}, fmt.Errorf("encode pipeline batch: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return WriteResult{}, fmt.Errorf("before WAL append: %w", err)
	}
	if _, err := p.wal.Append(encoded); err != nil {
		return WriteResult{}, fmt.Errorf("append durable pipeline WAL batch: %w", err)
	}

	p.mu.Lock()
	applyErr := p.active.table.ApplyBatch(batch)
	invariant.Assert(applyErr == nil, "STORAGE-49", "apply prevalidated durable batch: %v", applyErr)
	last := first + count - 1
	if !p.active.haveSeq {
		p.active.smallestSeq = first
	}
	p.active.haveSeq = true
	p.active.largestSeq = last
	if last == math.MaxUint64 {
		p.seqExhausted = true
	} else {
		p.nextSequence = last + 1
	}
	rotate := p.active.table.ReachedSize(p.threshold)
	if rotate {
		p.rotateLocked()
	}
	p.validateIfEnabledLocked()
	p.mu.Unlock()
	if rotate {
		p.notify(p.wake)
	}
	return WriteResult{FirstSequence: first, LastSequence: last, Generation: generationID}, nil
}

func validateMutations(mutations []storage.Mutation) error {
	if len(mutations) == 0 {
		return storage.ErrEmptyBatch
	}
	for index, mutation := range mutations {
		if len(mutation.Key) > sstable.MaxUserKeySize {
			return fmt.Errorf("mutation %d: %w", index, sstable.ErrKeyTooLarge)
		}
		if len(mutation.Value) > sstable.MaxValueSize {
			return fmt.Errorf("mutation %d: %w", index, sstable.ErrValueTooLarge)
		}
		if _, err := storage.NewInternalKey(mutation.Key, 0, mutation.Kind); err != nil {
			return fmt.Errorf("mutation %d: %w", index, err)
		}
		if mutation.Kind == storage.KindDelete && len(mutation.Value) != 0 {
			return fmt.Errorf("mutation %d: %w", index, sstable.ErrDeleteHasValue)
		}
	}
	return nil
}

// Rotate freezes and queues a nonempty active table. Empty rotation is a no-op.
func (p *Pipeline) Rotate(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, ErrInvalidOptions
	}
	select {
	case <-ctx.Done():
		return false, fmt.Errorf("wait for rotation admission: %w", ctx.Err())
	case <-p.writeToken:
	}
	defer func() { p.writeToken <- struct{}{} }()
	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return false, ErrClosed
	}
	empty := p.active.table.Len() == 0
	p.mu.Unlock()
	if empty {
		return false, nil
	}
	if err := p.waitForCapacity(ctx); err != nil {
		return false, err
	}
	p.mu.Lock()
	if !p.accepting {
		p.mu.Unlock()
		return false, ErrClosed
	}
	if failure := p.flushFailureLocked(); failure != nil {
		p.mu.Unlock()
		return false, failure
	}
	p.mu.Unlock()
	if err := p.ensureActiveFile(ctx); err != nil {
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.accepting {
		return false, ErrClosed
	}
	if failure := p.flushFailureLocked(); failure != nil {
		return false, failure
	}
	p.rotateLocked()
	p.validateIfEnabledLocked()
	p.notify(p.wake)
	return true, nil
}

func (p *Pipeline) rotateLocked() {
	old := p.active
	invariant.Assert(old.table.Len() != 0, "STORAGE-50", "rotate empty active generation %d", old.id)
	invariant.Assert(len(p.immutables) < p.maxImmutable, "STORAGE-55", "immutable backlog %d exceeds limit %d", len(p.immutables), p.maxImmutable)
	invariant.Assert(old.fileNumber > 0 && old.fileNumber <= sstable.MaxFileNumber, "STORAGE-61", "file number %d invalid", old.fileNumber)
	p.logger.Info("memtable rotation started", slog.Uint64("generation", old.id), slog.Int("entries", old.table.Len()), slog.Uint64("bytes", old.table.SizeBytes()))
	old.table.Freeze()
	p.logger.Info("memtable frozen", slog.Uint64("generation", old.id), slog.Uint64("smallest_sequence", old.smallestSeq), slog.Uint64("largest_sequence", old.largestSeq))
	if err := old.transition(StateQueued); err != nil {
		invariant.Assert(false, "STORAGE-50", "%v", err)
	}
	p.active = &generation{id: p.nextGeneration, table: memtable.New(), state: StateActive}
	p.nextGeneration++
	p.immutables = append(p.immutables, old)
	p.stats.Rotations++
	p.logger.Info("immutable queued", slog.Uint64("generation", old.id), slog.Uint64("file", old.fileNumber), slog.Int("entries", old.table.Len()), slog.Uint64("bytes", old.table.SizeBytes()))
	p.logger.Info("memtable created", slog.Uint64("generation", p.active.id))
}

func (p *Pipeline) ensureActiveFile(ctx context.Context) error {
	p.mu.Lock()
	if p.nextGeneration == 0 {
		p.mu.Unlock()
		return ErrGenerationExhausted
	}
	if p.active.fileNumber != 0 {
		p.mu.Unlock()
		return nil
	}
	if p.allocator == nil {
		if p.nextFile == 0 || p.nextFile > sstable.MaxFileNumber {
			p.mu.Unlock()
			return ErrFileNumberExhausted
		}
		p.active.fileNumber = p.nextFile
		p.nextFile++
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()
	number, err := p.allocator.AllocateFileNumber(ctx)
	if err != nil {
		return fmt.Errorf("reserve active MemTable file number: %w", err)
	}
	if number == 0 || number > sstable.MaxFileNumber {
		return ErrFileNumberExhausted
	}
	p.mu.Lock()
	p.active.fileNumber = number
	p.mu.Unlock()
	return nil
}

func (p *Pipeline) waitForCapacity(ctx context.Context) error {
	var started bool
	var since = p.clock.Now()
	for {
		p.mu.Lock()
		if !p.accepting {
			p.mu.Unlock()
			return ErrClosed
		}
		if failure := p.flushFailureLocked(); failure != nil {
			p.mu.Unlock()
			return failure
		}
		if len(p.immutables) < p.maxImmutable {
			if started {
				elapsed := p.clock.Since(since)
				if elapsed > 0 {
					p.stats.BackpressureNanos += uint64(elapsed) //nolint:gosec // positive duration fits uint64
				}
				p.logger.Info("backpressure cleared")
			}
			p.mu.Unlock()
			return nil
		}
		if !started {
			started = true
			since = p.clock.Now()
			p.stats.BackpressureEvents++
			p.logger.Info("backpressure entered", slog.Int("immutable_count", len(p.immutables)))
		}
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for immutable capacity: %w", ctx.Err())
		case <-p.changed:
		}
	}
}

func (p *Pipeline) flushFailureLocked() error {
	if len(p.immutables) != 0 && p.immutables[0].state == StateFailed {
		return errors.Join(ErrFlushFailed, p.immutables[0].flushErr)
	}
	return nil
}

// RetryFailed requeues the failed FIFO head when no final-file ambiguity exists.
func (p *Pipeline) RetryFailed() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.immutables) == 0 || p.immutables[0].state != StateFailed {
		return ErrNoFailedFlush
	}
	item := p.immutables[0]
	if item.ambiguous {
		return errors.Join(ErrAmbiguousPublication, item.flushErr)
	}
	if err := item.transition(StateQueued); err != nil {
		return err
	}
	item.flushErr = nil
	p.logger.Info("flush retry", slog.Uint64("generation", item.id), slog.Uint64("file", item.fileNumber))
	p.notify(p.wake)
	p.notify(p.changed)
	return nil
}

// Drain waits for all already-immutable generations to become durable.
func (p *Pipeline) Drain(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	for {
		p.mu.Lock()
		if failure := p.flushFailureLocked(); failure != nil {
			p.mu.Unlock()
			return failure
		}
		done := len(p.immutables) == 0
		p.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("drain immutable queue: %w", ctx.Err())
		case <-p.changed:
		}
	}
}

// Stats returns copied counters and current MemTable accounting.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := p.stats
	stats.ActiveGeneration = p.active.id
	stats.ActiveBytes = p.active.table.SizeBytes()
	stats.ImmutableCount = len(p.immutables)
	for _, item := range p.immutables {
		stats.ImmutableBytes += item.table.SizeBytes()
	}
	stats.NextSequence = p.nextSequence
	stats.SequenceExhausted = p.seqExhausted
	return stats
}

// Outputs returns copied physically durable flush results in FIFO order.
func (p *Pipeline) Outputs() []FlushOutput {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]FlushOutput(nil), p.outputs...)
}

// ReadSnapshot captures active/immutable membership and the sequence boundary
// under one lock. Tables are concurrency-safe and immutable generations are
// retained until their Manifest installation has completed.
func (p *Pipeline) ReadSnapshot() (ReadSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ReadSnapshot{}, ErrClosed
	}
	result := ReadSnapshot{
		Active:            ReadGeneration{Generation: p.active.id, FileNumber: p.active.fileNumber, Table: p.active.table},
		SequenceExhausted: p.seqExhausted,
	}
	result.Immutables = make([]ReadGeneration, 0, len(p.immutables))
	for index := len(p.immutables) - 1; index >= 0; index-- {
		item := p.immutables[index]
		result.Immutables = append(result.Immutables, ReadGeneration{Generation: item.id, FileNumber: item.fileNumber, Table: item.table})
	}
	if p.seqExhausted {
		result.LatestSequence, result.HaveSequence = math.MaxUint64, true
	} else if p.nextSequence != 0 {
		result.LatestSequence, result.HaveSequence = p.nextSequence-1, true
	}
	return result, nil
}

// Close stops writes, drains queued work when possible, joins the worker and
// closes the WAL. A nonempty active table intentionally remains WAL-backed.
func (p *Pipeline) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for shutdown admission: %w", ctx.Err())
	case <-p.writeToken:
	}
	p.mu.Lock()
	if p.closed {
		err := p.closeErr
		p.mu.Unlock()
		p.writeToken <- struct{}{}
		return err
	}
	p.accepting = false
	p.mu.Unlock()
	p.writeToken <- struct{}{}
	p.notify(p.changed)
	drainErr := p.Drain(ctx)
	p.cancel()
	p.wg.Wait()
	walErr := p.wal.Close()
	p.mu.Lock()
	p.closed = true
	p.closeErr = errors.Join(drainErr, walErr)
	p.logger.Info("pipeline shutdown", rlog.Err(p.closeErr))
	err := p.closeErr
	p.mu.Unlock()
	return err
}

func (p *Pipeline) notify(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
