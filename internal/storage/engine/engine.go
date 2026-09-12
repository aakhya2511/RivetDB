package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

const (
	DefaultMemTableBytes      = uint64(4 << 20)
	DefaultMaxImmutables      = 4
	DefaultTableCacheCapacity = 32
)

// ReplicatedStage is a deterministic observation boundary in the replicated
// apply and flush lifecycle.
type ReplicatedStage uint8

const (
	ReplicatedStageApplyStarted ReplicatedStage = iota
	ReplicatedStageMemTableApplied
	ReplicatedStageVisibilityPublished
	ReplicatedStageSSTableDurable
	ReplicatedStageManifestFrontierDurable
)

// ReplicatedHook observes replicated durability boundaries for crash tests.
type ReplicatedHook func(ReplicatedStage, uint64)

// Mode selects the durability authority for one database directory.
type Mode = manifest.StorageMode

const (
	ModeStandalone     = manifest.ModeStandalone
	ModeReplicated     = manifest.ModeReplicated
	ModeReplicatedMVCC = manifest.ModeReplicatedMVCC
)

// Options configures one local storage engine.
type Options struct {
	Directory           string
	MemTableBytes       uint64
	MaxImmutables       int
	SSTableOptions      sstable.Options
	L0Trigger           int
	TargetFileSize      uint64
	Clock               clock.Clock
	Logger              *slog.Logger
	WriteHook           pipeline.WriteHook
	ReadHook            ReadHook
	MaintenanceHook     MaintenanceHook
	TableCacheCapacity  int
	Mode                Mode
	ReplicatedApplyHook pipeline.ReplicatedApplyHook
	ReplicatedHook      ReplicatedHook
}

// KV is one latest-state user-key/value result.
type KV struct{ Key, Value []byte }

// MVCCEntry is one uninterpreted selected version. Transaction-aware callers
// use it to consult replicated outcome authority for KindIntent.
type MVCCEntry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Kind      storage.ValueKind
}

// MVCCVersion is one exact logical internal MVCC tuple. Unlike MVCCEntry it is
// not a visibility selection: ExportMVCCVersions returns every version,
// tombstone, abort marker, and intent in canonical internal-key order.
type MVCCVersion struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Kind      storage.ValueKind
}

// Stats is a point-in-time copy of local operation counters.
type Stats struct {
	Puts, Deletes, Gets, GetHits, GetMisses, Scans uint64
	Recoveries, ReplayedEntries                    uint64
	GetL0TableReads, GetHigherTableReads           uint64
	ScanTableReads                                 uint64
	TableCacheHits, TableCacheMisses, TableOpens   uint64
	TableCacheEvictions, CachedTableReaders        uint64
	ActiveTableLeases                              uint64
	BloomTableSkips                                uint64
	ReclaimedTables, MaintenanceFailures           uint64
	Pipeline                                       pipeline.Stats
	Manifest                                       manifest.Stats
	Compaction                                     compaction.Stats
}

// Engine owns the complete Phase 1 local LSM lifecycle.
type Engine struct {
	mu                                             sync.RWMutex
	applyMu                                        sync.Mutex
	directory                                      string
	pipeline                                       *pipeline.Pipeline
	manifest                                       *manifest.Store
	compact                                        *compaction.Executor
	closed                                         bool
	closeErr                                       error
	puts, deletes, gets, getHits, getMisses, scans atomic.Uint64
	recoveries, replayed                           atomic.Uint64
	getL0TableReads, getHigherTableReads           atomic.Uint64
	scanTableReads                                 atomic.Uint64
	bloomTableSkips                                atomic.Uint64
	readHook                                       ReadHook
	removeFile                                     func(string) error
	maintenanceHook                                MaintenanceHook
	tableCache                                     *tableCache
	reclaimed                                      map[uint64]struct{}
	mode                                           Mode
	appliedIdentities                              map[uint64]applyIdentity
	reclaimedTables, maintenanceFailures           atomic.Uint64
}

type applyIdentity struct {
	term   uint64
	digest [sha256.Size]byte
}

// Open creates or recovers one local engine directory.
func Open(options Options) (_ *Engine, resultErr error) {
	if options.Directory == "" {
		return nil, ErrInvalidOptions
	}
	if options.Mode == 0 {
		options.Mode = ModeStandalone
	}
	if options.Mode != ModeStandalone && options.Mode != ModeReplicated && options.Mode != ModeReplicatedMVCC {
		return nil, ErrInvalidOptions
	}
	if options.MemTableBytes == 0 {
		options.MemTableBytes = DefaultMemTableBytes
	}
	if options.MaxImmutables == 0 {
		options.MaxImmutables = DefaultMaxImmutables
	}
	if options.MemTableBytes == 0 || options.MaxImmutables < 1 {
		return nil, ErrInvalidOptions
	}
	if options.TableCacheCapacity == 0 {
		options.TableCacheCapacity = DefaultTableCacheCapacity
	}
	if options.TableCacheCapacity < 1 {
		return nil, ErrInvalidOptions
	}
	if options.Clock == nil {
		options.Clock = clock.System()
	}
	if options.Logger == nil {
		options.Logger = rlog.Discard()
	}
	if err := os.MkdirAll(options.Directory, 0o750); err != nil {
		return nil, fmt.Errorf("create engine directory: %w", err)
	}
	currentPath := filepath.Join(options.Directory, manifest.CurrentFileName)
	_, currentErr := os.Lstat(currentPath)
	if errors.Is(currentErr, os.ErrNotExist) {
		entries, readErr := os.ReadDir(options.Directory)
		if readErr != nil {
			return nil, fmt.Errorf("inspect new engine directory: %w", readErr)
		}
		if len(entries) != 0 {
			return nil, errors.Join(ErrCorruption, ErrDirectoryState)
		}
	} else if currentErr != nil {
		return nil, fmt.Errorf("inspect CURRENT: %w", currentErr)
	}
	wPath := filepath.Join(options.Directory, pipeline.WALFileName)
	if options.Mode == ModeReplicated || options.Mode == ModeReplicatedMVCC {
		if _, err := os.Lstat(wPath); err == nil {
			return nil, ErrUnexpectedWAL
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect forbidden replicated data WAL: %w", err)
		}
	} else if err := repairWALTail(wPath); err != nil {
		return nil, err
	}
	manifestOptions := manifest.Options{Directory: options.Directory, Clock: options.Clock, Logger: options.Logger, Mode: options.Mode}
	if options.ReplicatedHook != nil {
		manifestOptions.ReplicatedFrontierHook = func(index uint64) {
			options.ReplicatedHook(ReplicatedStageManifestFrontierDurable, index)
		}
	}
	var store *manifest.Store
	var err error
	if currentErr == nil {
		store, err = manifest.Open(manifestOptions)
	} else {
		store, err = manifest.Create(manifestOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("open Manifest authority: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, store.Close())
		}
	}()
	version, err := store.Current()
	if err != nil {
		return nil, fmt.Errorf("capture recovered Version: %w", err)
	}
	recovered := memtable.New()
	var replayed uint64
	if options.Mode == ModeStandalone {
		if _, statErr := os.Stat(wPath); statErr == nil {
			var frontier *uint64
			if value, ok := version.ReplayFrontier(); ok {
				frontier = &value
			}
			replayed, err = manifest.ReplayWAL(wPath, frontier, recovered)
			if err != nil {
				return nil, fmt.Errorf("replay engine WAL: %w", err)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("stat WAL: %w", statErr)
		}
	}
	nextSequence, exhausted := store.RecoveredNextSequence()
	var durableApplied uint64
	if options.Mode == ModeReplicated || options.Mode == ModeReplicatedMVCC {
		var ok bool
		durableApplied, ok = version.ReplicatedAppliedThrough()
		if !ok {
			return nil, errors.Join(ErrCorruption, manifest.ErrModeMismatch)
		}
		nextSequence, exhausted = 0, false
	}
	var durableMVCC uint64
	if options.Mode == ModeReplicatedMVCC {
		durableMVCC, _ = version.MaxAppliedMVCC()
	}
	firstGeneration := nextGeneration(version)
	replicatedApplyHook := options.ReplicatedApplyHook
	if options.ReplicatedHook != nil {
		outerHook := replicatedApplyHook
		replicatedApplyHook = func(stage pipeline.WriteStage, result pipeline.WriteResult) {
			if outerHook != nil {
				outerHook(stage, result)
			}
			switch stage {
			case pipeline.WriteStageApplyStarted:
				options.ReplicatedHook(ReplicatedStageApplyStarted, result.LastSequence)
			case pipeline.WriteStageApplyCompleted:
				options.ReplicatedHook(ReplicatedStageMemTableApplied, result.LastSequence)
			case pipeline.WriteStagePublished:
				options.ReplicatedHook(ReplicatedStageVisibilityPublished, result.LastSequence)
			}
		}
	}
	pipe, err := pipeline.Open(pipeline.Options{
		Directory: options.Directory, MemTableBytes: options.MemTableBytes,
		MaxImmutables: options.MaxImmutables, NextSequence: nextSequence,
		SequenceExhausted: exhausted, FirstGeneration: firstGeneration,
		SSTableOptions: options.SSTableOptions, Clock: options.Clock, Logger: options.Logger,
		FileAllocator: store, TableInstaller: store, RecoveredTable: recovered,
		WriteHook:  options.WriteHook,
		Replicated: options.Mode == ModeReplicated || options.Mode == ModeReplicatedMVCC, DurableApplied: durableApplied,
		ReplicatedMVCC: options.Mode == ModeReplicatedMVCC, DurableMVCC: durableMVCC,
		ReplicatedApplyHook: replicatedApplyHook,
		ReplicatedFlushHook: func(index uint64) {
			if options.ReplicatedHook != nil {
				options.ReplicatedHook(ReplicatedStageSSTableDurable, index)
			}
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open write pipeline: %w", err)
	}
	executor, err := compaction.New(compaction.Options{
		Directory: options.Directory, L0Trigger: options.L0Trigger,
		TargetFileSize: options.TargetFileSize, SSTableOptions: options.SSTableOptions,
		Clock: options.Clock, Logger: options.Logger,
	}, store)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open compaction executor: %w", err), pipe.Close(context.Background()))
	}
	e := &Engine{directory: options.Directory, pipeline: pipe, manifest: store, compact: executor, readHook: options.ReadHook, removeFile: os.Remove, maintenanceHook: options.MaintenanceHook, tableCache: newTableCache(options.Directory, options.TableCacheCapacity), reclaimed: make(map[uint64]struct{}), mode: options.Mode, appliedIdentities: make(map[uint64]applyIdentity)}
	e.recoveries.Store(1)
	e.replayed.Store(replayed)
	return e, nil
}

func repairWALTail(path string) error {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat WAL before recovery: %w", err)
	}
	result, err := wal.Recover(path, nil)
	if err != nil {
		return fmt.Errorf("validate WAL before open: %w", err)
	}
	if result.TailTruncated {
		if err := wal.RepairTail(path, result); err != nil {
			return fmt.Errorf("repair WAL tail: %w", err)
		}
	}
	return nil
}

func nextGeneration(version *manifest.Version) uint64 {
	var largest uint64
	for level := uint32(0); level < manifest.MaxLevels; level++ {
		for _, table := range version.Files(level) {
			largest = max(largest, table.FlushGeneration)
		}
	}
	if largest == ^uint64(0) {
		return largest
	}
	return largest + 1
}

func (e *Engine) Put(ctx context.Context, key, value []byte) error {
	return e.write(ctx, storage.Mutation{Kind: storage.KindValue, Key: key, Value: value}, &e.puts)
}
func (e *Engine) Delete(ctx context.Context, key []byte) error {
	return e.write(ctx, storage.Mutation{Kind: storage.KindDelete, Key: key}, &e.deletes)
}
func (e *Engine) write(ctx context.Context, mutation storage.Mutation, counter *atomic.Uint64) error {
	if err := e.writeBatch(ctx, []storage.Mutation{mutation}); err != nil {
		return err
	}
	counter.Add(1)
	return nil
}

// WriteBatch applies one local WAL/MemTable-atomic batch. It is not a
// transaction API and supports no reads or rollback.
func (e *Engine) WriteBatch(ctx context.Context, mutations []storage.Mutation) error {
	if err := e.writeBatch(ctx, mutations); err != nil {
		return err
	}
	for _, mutation := range mutations {
		switch mutation.Kind {
		case storage.KindValue:
			e.puts.Add(1)
		case storage.KindDelete:
			e.deletes.Add(1)
		}
	}
	return nil
}

func (e *Engine) writeBatch(ctx context.Context, mutations []storage.Mutation) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if e.mode != ModeStandalone {
		return ErrWrongMode
	}
	if _, err := e.pipeline.Write(ctx, mutations); err != nil {
		return fmt.Errorf("write local mutation: %w", err)
	}
	return nil
}

// ApplyCommitted materializes one committed Raft mutation without using the
// standalone WAL or local sequence allocator. commandBytes identifies replay.
func (e *Engine) ApplyCommitted(ctx context.Context, index, term uint64, commandBytes []byte, mutation storage.Mutation) error {
	return e.applyCommitted(ctx, index, term, index, commandBytes, mutation, ModeReplicated)
}

// ApplyCommittedMVCC materializes one committed command using timestamp as
// its internal-key version while index remains the replicated apply frontier.
func (e *Engine) ApplyCommittedMVCC(ctx context.Context, index, term, timestamp uint64, commandBytes []byte, mutation storage.Mutation) error {
	return e.applyCommittedBatch(ctx, index, term, timestamp, commandBytes, []storage.Mutation{mutation}, ModeReplicatedMVCC, false)
}

// ApplyCommittedMVCCBatch atomically installs one participant prepare at CT.
func (e *Engine) ApplyCommittedMVCCBatch(ctx context.Context, index, term, timestamp uint64, commandBytes []byte, mutations []storage.Mutation) error {
	return e.applyCommittedBatch(ctx, index, term, timestamp, commandBytes, mutations, ModeReplicatedMVCC, false)
}

// ApplyPreparedMVCCBatch installs provisional versions whose globally chosen
// CT may arrive after a larger unrelated CT on this range.
func (e *Engine) ApplyPreparedMVCCBatch(ctx context.Context, index, term, timestamp uint64, commandBytes []byte, mutations []storage.Mutation) error {
	return e.applyCommittedBatch(ctx, index, term, timestamp, commandBytes, mutations, ModeReplicatedMVCC, true)
}

// ResolveCommittedMVCCBatch materializes a decision at an existing intent CT.
func (e *Engine) ResolveCommittedMVCCBatch(ctx context.Context, index, term, timestamp uint64, commandBytes []byte, mutations []storage.Mutation) error {
	return e.applyCommittedBatch(ctx, index, term, timestamp, commandBytes, mutations, ModeReplicatedMVCC, true)
}

func (e *Engine) applyCommitted(ctx context.Context, index, term, version uint64, commandBytes []byte, mutation storage.Mutation, required Mode) error {
	return e.applyCommittedBatch(ctx, index, term, version, commandBytes, []storage.Mutation{mutation}, required, false)
}

func (e *Engine) applyCommittedBatch(ctx context.Context, index, term, version uint64, commandBytes []byte, mutations []storage.Mutation, required Mode, allowExistingVersion bool) error {
	if ctx == nil || index == 0 || term == 0 || version == 0 || len(commandBytes) == 0 {
		return ErrInvalidOptions
	}
	e.applyMu.Lock()
	defer e.applyMu.Unlock()
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if e.mode != required {
		return ErrWrongMode
	}
	identity := applyIdentity{term: term, digest: sha256.Sum256(commandBytes)}
	if existing, ok := e.appliedIdentities[index]; ok {
		if existing != identity {
			return ErrConflictingApply
		}
		return ErrAlreadyApplied
	}
	storageVersion, err := e.manifest.Current()
	if err != nil {
		return fmt.Errorf("read replicated applied frontier: %w", err)
	}
	if durable, ok := storageVersion.ReplicatedAppliedThrough(); ok && index <= durable {
		return ErrAlreadyApplied
	}
	stats := e.pipeline.Stats()
	if index <= stats.ReplicatedApplied {
		return ErrApplyOrder
	}
	if _, err := e.pipeline.ApplyReplicatedVersionBatch(ctx, index, version, mutations, allowExistingVersion); err != nil {
		return fmt.Errorf("materialize committed Raft mutation: %w", err)
	}
	e.appliedIdentities[index] = identity
	for _, mutation := range mutations {
		if mutation.Kind == storage.KindDelete {
			e.deletes.Add(1)
		} else {
			e.puts.Add(1)
		}
	}
	return nil
}

// AdvanceApplied records complete local application of committed no-op indexes.
func (e *Engine) AdvanceApplied(index uint64) error {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if e.mode != ModeReplicated && e.mode != ModeReplicatedMVCC {
		return ErrWrongMode
	}
	if err := e.pipeline.AdvanceReplicatedApplied(index); err != nil {
		return fmt.Errorf("advance replicated applied progress: %w", err)
	}
	return nil
}

// MaxAppliedMVCC returns the locally applied volatile MVCC watermark.
func (e *Engine) MaxAppliedMVCC() (uint64, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return 0, false, ErrClosed
	}
	if e.mode != ModeReplicatedMVCC {
		return 0, false, ErrWrongMode
	}
	stats := e.pipeline.Stats()
	return stats.VisibleSequence, stats.HaveVisible, nil
}

// DurableMaxAppliedMVCC returns the Manifest-authoritative durable watermark.
func (e *Engine) DurableMaxAppliedMVCC() (uint64, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return 0, false, ErrClosed
	}
	if e.mode != ModeReplicatedMVCC {
		return 0, false, ErrWrongMode
	}
	version, err := e.manifest.Current()
	if err != nil {
		return 0, false, fmt.Errorf("read Manifest MVCC watermark: %w", err)
	}
	value, ok := version.MaxAppliedMVCC()
	return value, ok, nil
}

// DurableAppliedRaftIndex returns the authoritative local Manifest frontier.
func (e *Engine) DurableAppliedRaftIndex() (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return 0, ErrClosed
	}
	version, err := e.manifest.Current()
	if err != nil {
		return 0, fmt.Errorf("read Manifest frontier: %w", err)
	}
	value, ok := version.ReplicatedAppliedThrough()
	if !ok {
		return 0, ErrWrongMode
	}
	return value, nil
}

// Flush rotates the active table and waits for every immutable installation.
func (e *Engine) Flush(ctx context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	if _, err := e.pipeline.Rotate(ctx); err != nil {
		return fmt.Errorf("rotate active MemTable: %w", err)
	}
	if err := e.pipeline.Drain(ctx); err != nil {
		return fmt.Errorf("drain immutable MemTables: %w", err)
	}
	return nil
}

// Compact performs at most one deterministic compaction.
func (e *Engine) Compact(ctx context.Context) (compaction.Result, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return compaction.Result{}, ErrClosed
	}
	result, err := e.compact.CompactOnce(ctx)
	if err != nil {
		return result, fmt.Errorf("compact local engine: %w", err)
	}
	return result, nil
}

// Close stops admission, drains already-immutable flushes, and closes every
// durable authority. A nonempty active MemTable remains WAL-backed.
func (e *Engine) Close(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return e.closeErr
	}
	e.closed = true
	e.closeErr = errors.Join(e.pipeline.Close(context.WithoutCancel(ctx)), e.tableCache.close(), e.manifest.Close())
	return e.closeErr
}

func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := Stats{Puts: e.puts.Load(), Deletes: e.deletes.Load(), Gets: e.gets.Load(), GetHits: e.getHits.Load(), GetMisses: e.getMisses.Load(), Scans: e.scans.Load(), Recoveries: e.recoveries.Load(), ReplayedEntries: e.replayed.Load(), GetL0TableReads: e.getL0TableReads.Load(), GetHigherTableReads: e.getHigherTableReads.Load(), ScanTableReads: e.scanTableReads.Load(), ReclaimedTables: e.reclaimedTables.Load(), MaintenanceFailures: e.maintenanceFailures.Load()}
	cache := e.tableCache.stats()
	result.TableCacheHits, result.TableCacheMisses, result.TableOpens = cache.Hits, cache.Misses, cache.Opens
	result.TableCacheEvictions, result.CachedTableReaders = cache.Evictions, cache.CachedReaders
	result.ActiveTableLeases = cache.ActiveLeases
	result.BloomTableSkips = e.bloomTableSkips.Load()
	if e.pipeline != nil {
		result.Pipeline = e.pipeline.Stats()
	}
	if e.manifest != nil {
		result.Manifest = e.manifest.Stats()
	}
	if e.compact != nil {
		result.Compaction = e.compact.Stats()
	}
	return result
}

func metadataMatches(expected manifest.TableMetadata, actual sstable.Metadata) bool {
	return expected.FileNumber == actual.FileNumber && expected.FileSize == actual.FileSize && expected.EntryCount == actual.EntryCount && expected.DeletionCount == actual.DeletionCount && expected.RawKeyValueBytes == actual.RawKeyValueBytes && expected.DataBlockCount == actual.DataBlockCount && storage.CompareInternal(expected.SmallestInternal, actual.SmallestInternal) == 0 && storage.CompareInternal(expected.LargestInternal, actual.LargestInternal) == 0 && bytes.Equal(expected.SmallestUser, actual.SmallestUser) && bytes.Equal(expected.LargestUser, actual.LargestUser)
}
