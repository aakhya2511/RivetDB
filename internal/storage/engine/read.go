package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	return e.getAt(ctx, key, nil)
}

// GetAt returns the newest value at or below target. A selected tombstone maps
// to ErrNotFound. Callers that promise replica freshness enforce watermarks.
func (e *Engine) GetAt(ctx context.Context, key []byte, target uint64) ([]byte, error) {
	return e.getAt(ctx, key, &target)
}

func (e *Engine) getAt(ctx context.Context, key []byte, requested *uint64) ([]byte, error) {
	entry, err := e.getMVCCAt(ctx, key, requested)
	if err != nil {
		return nil, err
	}
	switch entry.Kind {
	case storage.KindValue:
		return entry.Value, nil
	case storage.KindDelete:
		return nil, ErrNotFound
	case storage.KindIntent:
		return nil, ErrUnresolvedIntent
	default:
		return nil, errors.Join(ErrCorruption, storage.ErrInvalidValueKind)
	}
}

// GetMVCCAt returns the selected physical MVCC entry without interpreting a
// transaction intent. Abort markers are skipped to older history.
func (e *Engine) GetMVCCAt(ctx context.Context, key []byte, target uint64) (MVCCEntry, error) {
	return e.getMVCCAt(ctx, key, &target)
}

func (e *Engine) getMVCCAt(ctx context.Context, key []byte, requested *uint64) (MVCCEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return MVCCEntry{}, ErrClosed
	}
	if ctx == nil || len(key) > sstable.MaxUserKeySize {
		return MVCCEntry{}, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return MVCCEntry{}, fmt.Errorf("before point read: %w", err)
	}
	e.gets.Add(1)
	view, err := e.captureReadView()
	if err != nil {
		return MVCCEntry{}, err
	}
	e.observeRead(ReadStageGetViewCaptured, view.version.Generation())
	if !view.memtables.HaveSequence {
		e.getMisses.Add(1)
		return MVCCEntry{}, ErrNotFound
	}
	target := view.memtables.LatestSequence
	if requested != nil && *requested < target {
		target = *requested
	}
	for {
		var best *candidate
		consider := func(value candidate) error {
			if value.entry.Key.Sequence() > target {
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
			if value.entry.Key.Kind() != best.entry.Key.Kind() {
				if value.entry.Key.Kind() < best.entry.Key.Kind() {
					winner := value
					best = &winner
				}
				return nil
			}
			if !bytes.Equal(value.entry.Value, best.entry.Value) {
				return errors.Join(ErrCorruption, ErrDuplicateEntry)
			}
			if value.fileNumber == 0 || value.fileNumber != best.fileNumber {
				return errors.Join(ErrCorruption, ErrDuplicateEntry)
			}
			return nil
		}
		if entry, ok := view.memtables.Active.Table.GetCandidate(key, target); ok {
			if err := consider(candidate{entry: sstable.Entry{Key: entry.Key, Value: entry.Value}, fileNumber: view.memtables.Active.FileNumber}); err != nil {
				return MVCCEntry{}, err
			}
		}
		for _, generation := range view.memtables.Immutables {
			if entry, ok := generation.Table.GetCandidate(key, target); ok {
				if err := consider(candidate{entry: sstable.Entry{Key: entry.Key, Value: entry.Value}, fileNumber: generation.FileNumber}); err != nil {
					return MVCCEntry{}, err
				}
			}
		}
		for _, table := range tablesForPoint(view.version, key) {
			if table.Level == 0 {
				e.getL0TableReads.Add(1)
			} else {
				e.getHigherTableReads.Add(1)
			}
			value, found, readErr := e.tableCandidate(table, key, target)
			if readErr != nil {
				return MVCCEntry{}, readErr
			}
			if found {
				if err := consider(candidate{entry: value, fileNumber: table.FileNumber}); err != nil {
					return MVCCEntry{}, err
				}
			}
		}
		if best == nil {
			e.getMisses.Add(1)
			return MVCCEntry{}, ErrNotFound
		}
		if best.entry.Key.Kind() == storage.KindTxnAbort {
			if best.entry.Key.Sequence() == 0 {
				e.getMisses.Add(1)
				return MVCCEntry{}, ErrNotFound
			}
			target = best.entry.Key.Sequence() - 1
			continue
		}
		e.getHits.Add(1)
		return MVCCEntry{Key: best.entry.Key.UserKey(), Value: bytes.Clone(best.entry.Value), Timestamp: best.entry.Key.Sequence(), Kind: best.entry.Key.Kind()}, nil
	}
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
	lease, err := e.tableCache.acquire(table)
	if err != nil {
		return result, false, fmt.Errorf("open authoritative table %d: %w", table.FileNumber, errors.Join(ErrCorruption, err))
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Release()) }()
	mayContain, err := lease.Reader.MayContain(key)
	if err != nil {
		return sstable.Entry{}, false, fmt.Errorf("read authoritative table %d filter: %w", table.FileNumber, errors.Join(ErrCorruption, err))
	}
	if !mayContain {
		e.bloomTableSkips.Add(1)
		return sstable.Entry{}, false, nil
	}
	result, err = lease.Reader.GetCandidate(key, sequence)
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
	return e.scanAt(ctx, start, end, nil)
}

// ScanAt materializes one visible value per user key at or below target.
func (e *Engine) ScanAt(ctx context.Context, start, end []byte, target uint64) ([]KV, error) {
	return e.scanAt(ctx, start, end, &target)
}

func (e *Engine) scanAt(ctx context.Context, start, end []byte, requested *uint64) (result []KV, resultErr error) {
	entries, err := e.scanMVCCAt(ctx, start, end, requested)
	if err != nil {
		return nil, err
	}
	values := make([]KV, 0, len(entries))
	for _, entry := range entries {
		switch entry.Kind {
		case storage.KindValue:
			values = append(values, KV{Key: entry.Key, Value: entry.Value})
		case storage.KindDelete:
		case storage.KindIntent:
			return nil, ErrUnresolvedIntent
		default:
			return nil, errors.Join(ErrCorruption, storage.ErrInvalidValueKind)
		}
	}
	return values, nil
}

// ScanMVCCAt selects one physical entry per user key at or below target.
func (e *Engine) ScanMVCCAt(ctx context.Context, start, end []byte, target uint64) ([]MVCCEntry, error) {
	return e.scanMVCCAt(ctx, start, end, &target)
}

func (e *Engine) scanMVCCAt(ctx context.Context, start, end []byte, requested *uint64) (result []MVCCEntry, resultErr error) {
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
		return []MVCCEntry{}, nil
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
		return []MVCCEntry{}, nil
	}
	target := view.memtables.LatestSequence
	if requested != nil && *requested < target {
		target = *requested
	}
	inputs, leases, err := e.scanInputs(view, start, end)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, lease := range leases {
			resultErr = errors.Join(resultErr, lease.Release())
		}
	}()
	merge, err := compaction.NewMergeIterator(inputs)
	if err != nil {
		return nil, fmt.Errorf("construct scan merge: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, merge.Close()) }()
	var values []MVCCEntry
	var currentUser []byte
	var haveUser bool
	var selected candidate
	var haveSelected bool
	emit := func() {
		if haveSelected && selected.entry.Key.Kind() != storage.KindTxnAbort {
			values = append(values, MVCCEntry{Key: currentUser, Value: selected.entry.Value,
				Timestamp: selected.entry.Key.Sequence(), Kind: selected.entry.Key.Kind()})
		}
		haveSelected = false
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
		if entry.Key.Sequence() > target {
			continue
		}
		value := candidate{entry: entry, fileNumber: file}
		if !haveSelected {
			selected = value
			haveSelected = true
			continue
		}
		if entry.Key.Sequence() == selected.entry.Key.Sequence() {
			if entry.Key.Kind() != selected.entry.Key.Kind() {
				if entry.Key.Kind() < selected.entry.Key.Kind() {
					selected = value
				}
				continue
			}
			if !bytes.Equal(entry.Value, selected.entry.Value) || file == 0 || file != selected.fileNumber {
				return nil, errors.Join(ErrCorruption, ErrDuplicateEntry)
			}
			continue
		}
		if selected.entry.Key.Kind() == storage.KindTxnAbort {
			selected = value
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

func (e *Engine) scanInputs(view readView, start, end []byte) ([]compaction.MergeInput, []*tableLease, error) {
	inputs := make([]compaction.MergeInput, 0)
	leases := make([]*tableLease, 0)
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
			lease, err := e.tableCache.acquire(table)
			if err != nil {
				return nil, leases, errors.Join(fmt.Errorf("open scan table %d: %w", table.FileNumber, err), releaseTableLeases(leases))
			}
			iterator, err := lease.Reader.Range(start, end)
			if err != nil {
				return nil, leases, errors.Join(fmt.Errorf("range table %d: %w", table.FileNumber, err), lease.Release(), releaseTableLeases(leases))
			}
			leases = append(leases, lease)
			e.scanTableReads.Add(1)
			inputs = append(inputs, compaction.MergeInput{FileNumber: table.FileNumber, Iterator: iterator})
		}
	}
	return inputs, leases, nil
}

func tableOverlapsScan(table manifest.TableMetadata, start, end []byte) bool {
	return (start == nil || bytes.Compare(table.LargestUser, start) >= 0) && (end == nil || bytes.Compare(table.SmallestUser, end) < 0)
}

func releaseTableLeases(leases []*tableLease) error {
	var result error
	for _, lease := range leases {
		result = errors.Join(result, lease.Release())
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
