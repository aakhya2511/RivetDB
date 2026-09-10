package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

type readView struct {
	memtables pipeline.ReadSnapshot
	version   *manifest.Version
}

// ReadStage is a deterministic read-lifetime observation boundary.
type ReadStage uint8

const (
	ReadStageGetViewCaptured ReadStage = iota
	ReadStageScanViewCaptured
)

// ReadHook observes a captured Version while the operation still holds the
// Engine read lock. It exists for deterministic lifetime/reclamation tests.
type ReadHook func(ReadStage, uint64)

type candidate struct {
	entry      sstable.Entry
	fileNumber uint64
}

func (e *Engine) captureReadView() (readView, error) {
	memtables, err := e.pipeline.ReadSnapshot()
	if err != nil {
		return readView{}, fmt.Errorf("capture MemTable read snapshot: %w", err)
	}
	version, err := e.manifest.Current()
	if err != nil {
		return readView{}, fmt.Errorf("capture Version read snapshot: %w", err)
	}
	return readView{memtables: memtables, version: version}, nil
}

// Get returns the newest value visible at the operation's captured storage
// sequence. A newest tombstone maps to ErrNotFound.
func (e *Engine) Get(ctx context.Context, key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrClosed
	}
	if ctx == nil || len(key) > sstable.MaxUserKeySize {
		return nil, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("before point read: %w", err)
	}
	e.gets.Add(1)
	view, err := e.captureReadView()
	if err != nil {
		return nil, err
	}
	e.observeRead(ReadStageGetViewCaptured, view.version.Generation())
	if !view.memtables.HaveSequence {
		e.getMisses.Add(1)
		return nil, ErrNotFound
	}
	var best *candidate
	consider := func(value candidate) error {
		if value.entry.Key.Sequence() > view.memtables.LatestSequence {
			return nil
		}
		if best == nil || value.entry.Key.Sequence() > best.entry.Key.Sequence() {
			winner := value
			best = &winner
			return nil
		}
		if value.entry.Key.Sequence() < best.entry.Key.Sequence() {
			return nil
		}
		if value.entry.Key.Kind() != best.entry.Key.Kind() || !bytes.Equal(value.entry.Value, best.entry.Value) {
			return errors.Join(ErrCorruption, ErrDuplicateEntry)
		}
		if value.fileNumber == 0 || value.fileNumber != best.fileNumber {
			return errors.Join(ErrCorruption, ErrDuplicateEntry)
		}
		return nil
	}
	if entry, ok := view.memtables.Active.Table.GetCandidate(key, view.memtables.LatestSequence); ok {
		if err := consider(candidate{entry: sstable.Entry{Key: entry.Key, Value: entry.Value}, fileNumber: view.memtables.Active.FileNumber}); err != nil {
			return nil, err
		}
	}
	for _, generation := range view.memtables.Immutables {
		if entry, ok := generation.Table.GetCandidate(key, view.memtables.LatestSequence); ok {
			if err := consider(candidate{entry: sstable.Entry{Key: entry.Key, Value: entry.Value}, fileNumber: generation.FileNumber}); err != nil {
				return nil, err
			}
		}
	}
	for _, table := range tablesForPoint(view.version, key) {
		if table.Level == 0 {
			e.getL0TableReads.Add(1)
		} else {
			e.getHigherTableReads.Add(1)
		}
		value, found, readErr := e.tableCandidate(table, key, view.memtables.LatestSequence)
		if readErr != nil {
			return nil, readErr
		}
		if found {
			if err := consider(candidate{entry: value, fileNumber: table.FileNumber}); err != nil {
				return nil, err
			}
		}
	}
	if best == nil || best.entry.Key.Kind() == storage.KindDelete {
		e.getMisses.Add(1)
		return nil, ErrNotFound
	}
	e.getHits.Add(1)
	return bytes.Clone(best.entry.Value), nil
}

func tablesForPoint(version *manifest.Version, key []byte) []manifest.TableMetadata {
	var result []manifest.TableMetadata
	for _, table := range version.Files(0) {
		if bytes.Compare(table.SmallestUser, key) <= 0 && bytes.Compare(key, table.LargestUser) <= 0 {
			result = append(result, table)
		}
	}
	for level := uint32(1); level < manifest.MaxLevels; level++ {
		tables := version.Files(level)
		position := sort.Search(len(tables), func(index int) bool {
			return bytes.Compare(tables[index].SmallestUser, key) > 0
		}) - 1
		if position >= 0 && bytes.Compare(key, tables[position].LargestUser) <= 0 {
			result = append(result, tables[position])
		}
	}
	return result
}

func (e *Engine) tableCandidate(table manifest.TableMetadata, key []byte, sequence uint64) (result sstable.Entry, found bool, resultErr error) {
	reader, err := sstable.Open(filepath.Join(e.directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
	if err != nil {
		return result, false, fmt.Errorf("open authoritative table %d: %w", table.FileNumber, errors.Join(ErrCorruption, err))
	}
	defer func() { resultErr = errors.Join(resultErr, reader.Close()) }()
	if !metadataMatches(table, reader.Metadata()) {
		return result, false, errors.Join(ErrCorruption, manifest.ErrMetadataMismatch)
	}
	result, err = reader.GetCandidate(key, sequence)
	if errors.Is(err, sstable.ErrNotFound) {
		return sstable.Entry{}, false, nil
	}
	if err != nil {
		return sstable.Entry{}, false, fmt.Errorf("read authoritative table %d: %w", table.FileNumber, errors.Join(ErrCorruption, err))
	}
	return result, true, nil
}

// Scan materializes the latest logical values in user-key order over
// [start,end). Nil bounds are unbounded; equal bounds are empty.
func (e *Engine) Scan(ctx context.Context, start, end []byte) (result []KV, resultErr error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return nil, ErrClosed
	}
	if ctx == nil || len(start) > sstable.MaxUserKeySize || len(end) > sstable.MaxUserKeySize {
		return nil, ErrInvalidOptions
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, ErrInvalidRange
	}
	if start != nil && end != nil && bytes.Equal(start, end) {
		return []KV{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("before scan: %w", err)
	}
	e.scans.Add(1)
	view, err := e.captureReadView()
	if err != nil {
		return nil, err
	}
	e.observeRead(ReadStageScanViewCaptured, view.version.Generation())
	if !view.memtables.HaveSequence {
		return []KV{}, nil
	}
	inputs, readers, err := e.scanInputs(view, start, end)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, reader := range readers {
			resultErr = errors.Join(resultErr, reader.Close())
		}
	}()
	merge, err := compaction.NewMergeIterator(inputs)
	if err != nil {
		return nil, fmt.Errorf("construct scan merge: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, merge.Close()) }()
	var values []KV
	var currentUser []byte
	var haveUser bool
	var selected *candidate
	emit := func() {
		if selected != nil && selected.entry.Key.Kind() == storage.KindValue {
			values = append(values, KV{Key: bytes.Clone(currentUser), Value: bytes.Clone(selected.entry.Value)})
		}
		selected = nil
	}
	for merge.Next() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("scan canceled: %w", err)
		}
		entry, ok := merge.Entry()
		file, haveFile := merge.SourceFileNumber()
		if !ok || !haveFile {
			return nil, errors.Join(ErrCorruption, compaction.ErrInputCorrupt)
		}
		user := entry.Key.UserKey()
		if !haveUser || !bytes.Equal(currentUser, user) {
			if haveUser {
				emit()
			}
			currentUser = user
			haveUser = true
		}
		if entry.Key.Sequence() > view.memtables.LatestSequence {
			continue
		}
		value := candidate{entry: entry, fileNumber: file}
		if selected == nil {
			selected = &value
			continue
		}
		if entry.Key.Sequence() == selected.entry.Key.Sequence() {
			if entry.Key.Kind() != selected.entry.Key.Kind() || !bytes.Equal(entry.Value, selected.entry.Value) || file == 0 || file != selected.fileNumber {
				return nil, errors.Join(ErrCorruption, ErrDuplicateEntry)
			}
		}
	}
	if err := merge.Error(); err != nil {
		return nil, fmt.Errorf("scan authoritative source: %w", errors.Join(ErrCorruption, err))
	}
	if haveUser {
		emit()
	}
	return values, nil
}

func (e *Engine) observeRead(stage ReadStage, generation uint64) {
	if e.readHook != nil {
		e.readHook(stage, generation)
	}
}

func (e *Engine) scanInputs(view readView, start, end []byte) ([]compaction.MergeInput, []*sstable.Reader, error) {
	inputs := make([]compaction.MergeInput, 0)
	readers := make([]*sstable.Reader, 0)
	addMemory := func(generation pipeline.ReadGeneration) error {
		iterator, err := generation.Table.Range(start, end)
		if err != nil {
			return fmt.Errorf("create MemTable range: %w", err)
		}
		inputs = append(inputs, compaction.MergeInput{FileNumber: generation.FileNumber, Iterator: &memIterator{iterator: iterator}})
		return nil
	}
	if err := addMemory(view.memtables.Active); err != nil {
		return nil, nil, err
	}
	for _, generation := range view.memtables.Immutables {
		if err := addMemory(generation); err != nil {
			return nil, nil, err
		}
	}
	for level := uint32(0); level < manifest.MaxLevels; level++ {
		for _, table := range view.version.Files(level) {
			if !tableOverlapsScan(table, start, end) {
				continue
			}
			reader, err := sstable.Open(filepath.Join(e.directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
			if err != nil {
				return nil, readers, errors.Join(fmt.Errorf("open scan table %d: %w", table.FileNumber, err), closeTableReaders(readers))
			}
			if !metadataMatches(table, reader.Metadata()) {
				return nil, readers, errors.Join(ErrCorruption, manifest.ErrMetadataMismatch, reader.Close(), closeTableReaders(readers))
			}
			iterator, err := reader.Range(start, end)
			if err != nil {
				return nil, readers, errors.Join(fmt.Errorf("range table %d: %w", table.FileNumber, err), reader.Close(), closeTableReaders(readers))
			}
			readers = append(readers, reader)
			e.scanTableReads.Add(1)
			inputs = append(inputs, compaction.MergeInput{FileNumber: table.FileNumber, Iterator: iterator})
		}
	}
	return inputs, readers, nil
}

func tableOverlapsScan(table manifest.TableMetadata, start, end []byte) bool {
	return (start == nil || bytes.Compare(table.LargestUser, start) >= 0) && (end == nil || bytes.Compare(table.SmallestUser, end) < 0)
}

func closeTableReaders(readers []*sstable.Reader) error {
	var result error
	for _, reader := range readers {
		result = errors.Join(result, reader.Close())
	}
	return result
}

type memIterator struct{ iterator *memtable.Iterator }

func (m *memIterator) Next() bool { return m.iterator.Next() }
func (m *memIterator) Entry() (sstable.Entry, bool) {
	entry, ok := m.iterator.Entry()
	return sstable.Entry{Key: entry.Key, Value: entry.Value}, ok
}
func (m *memIterator) Error() error { return nil }
func (m *memIterator) Close() error { return nil }
