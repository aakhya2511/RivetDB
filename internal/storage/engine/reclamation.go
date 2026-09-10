package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

// TableReclamationResult reports physical maintenance that does not alter the
// logical Version.
type TableReclamationResult struct {
	Considered uint64
	Deleted    uint64
	Retained   uint64
}

// MaintenanceStage is a deterministic reclamation-lifetime boundary.
type MaintenanceStage uint8

const (
	MaintenanceStageTableReclaimAttempt MaintenanceStage = iota
	MaintenanceStageTableReclaimExclusive
)

// MaintenanceHook observes reclamation boundaries for deterministic tests.
type MaintenanceHook func(MaintenanceStage)

// ReclaimObsoleteTables deletes only compaction inputs made obsolete in this
// process. The exclusive Engine lock waits for every read, scan, flush and
// compaction operation, which is the conservative Version-lifetime proof.
func (e *Engine) ReclaimObsoleteTables(ctx context.Context) (TableReclamationResult, error) {
	if ctx == nil {
		return TableReclamationResult{}, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return TableReclamationResult{}, fmt.Errorf("before obsolete-table reclamation: %w", err)
	}
	e.observeMaintenance(MaintenanceStageTableReclaimAttempt)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.observeMaintenance(MaintenanceStageTableReclaimExclusive)
	if e.closed {
		return TableReclamationResult{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return TableReclamationResult{}, fmt.Errorf("before obsolete-table deletion: %w", err)
	}
	version, err := e.manifest.Current()
	if err != nil {
		return TableReclamationResult{}, fmt.Errorf("capture current Version for reclamation: %w", err)
	}
	live := make(map[uint64]struct{}, version.LiveTableCount())
	for _, number := range version.LiveFileNumbers() {
		live[number] = struct{}{}
	}
	var result TableReclamationResult
	var resultErr error
	for _, number := range e.manifest.ObsoleteFiles() {
		result.Considered++
		if _, alreadyReclaimed := e.reclaimed[number]; alreadyReclaimed {
			continue
		}
		if _, stillLive := live[number]; stillLive {
			result.Retained++
			resultErr = errors.Join(resultErr, ErrCorruption, errObsoleteStillLive)
			continue
		}
		path := filepath.Join(e.directory, sstable.FileName(number))
		if err := e.removeFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			result.Retained++
			e.maintenanceFailures.Add(1)
			resultErr = errors.Join(resultErr, fmt.Errorf("delete obsolete table %d: %w", number, err))
			continue
		}
		result.Deleted++
		e.reclaimed[number] = struct{}{}
		e.reclaimedTables.Add(1)
	}
	return result, resultErr
}

func (e *Engine) observeMaintenance(stage MaintenanceStage) {
	if e.maintenanceHook != nil {
		e.maintenanceHook(stage)
	}
}

// WALSegmentCoverage describes the one current Phase 1 WAL file. Physical WAL
// deletion remains disabled because it is an open append target, not a closed
// segment that can be atomically retired.
type WALSegmentCoverage struct {
	Name                                    string
	Batches, Entries                        uint64
	SmallestSequence, LargestSequence       uint64
	HaveSequence, EntirelyAtOrBelowFrontier bool
}

// WALReclamationStatus is candidate-only maintenance evidence.
type WALReclamationStatus struct {
	Frontier                uint64
	HaveFrontier            bool
	Segments                []WALSegmentCoverage
	PhysicalDeletionEnabled bool
}

// InspectWALReclamation computes whole-file coverage from complete batches. It
// consumes but never advances the already-durable Manifest frontier.
func (e *Engine) InspectWALReclamation(ctx context.Context) (WALReclamationStatus, error) {
	if ctx == nil {
		return WALReclamationStatus{}, ErrInvalidOptions
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return WALReclamationStatus{}, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return WALReclamationStatus{}, fmt.Errorf("before WAL coverage inspection: %w", err)
	}
	version, err := e.manifest.Current()
	if err != nil {
		return WALReclamationStatus{}, fmt.Errorf("capture WAL frontier: %w", err)
	}
	status := WALReclamationStatus{}
	status.Frontier, status.HaveFrontier = version.ReplayFrontier()
	coverage, err := inspectWALFile(filepath.Join(e.directory, pipeline.WALFileName), status.Frontier, status.HaveFrontier)
	if err != nil {
		return WALReclamationStatus{}, err
	}
	status.Segments = []WALSegmentCoverage{coverage}
	return status, nil
}

func inspectWALFile(path string, frontier uint64, haveFrontier bool) (WALSegmentCoverage, error) {
	coverage := WALSegmentCoverage{Name: filepath.Base(path)}
	_, err := wal.Recover(path, func(record wal.Record) error {
		batch, decodeErr := storage.DecodeWriteBatch(record.Payload)
		if decodeErr != nil {
			return fmt.Errorf("decode WAL coverage batch: %w", decodeErr)
		}
		count := uint64(len(batch.Mutations)) //nolint:gosec // decoder bounds count
		last := batch.FirstSequence + count - 1
		if haveFrontier && batch.FirstSequence <= frontier && last > frontier {
			return manifest.ErrFrontierSplitsBatch
		}
		if !coverage.HaveSequence {
			coverage.SmallestSequence, coverage.LargestSequence, coverage.HaveSequence = batch.FirstSequence, last, true
		} else {
			coverage.SmallestSequence = min(coverage.SmallestSequence, batch.FirstSequence)
			coverage.LargestSequence = max(coverage.LargestSequence, last)
		}
		coverage.Batches++
		coverage.Entries += count
		return nil
	})
	if err != nil {
		return WALSegmentCoverage{}, fmt.Errorf("inspect WAL segment coverage: %w", err)
	}
	coverage.EntirelyAtOrBelowFrontier = coverage.HaveSequence && haveFrontier && coverage.LargestSequence <= frontier
	return coverage, nil
}
