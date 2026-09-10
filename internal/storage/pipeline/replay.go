package pipeline

import (
	"fmt"
	"math"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

// ReplayResult describes complete batches reconstructed from one retained WAL.
type ReplayResult struct {
	Batches           uint64
	Entries           uint64
	NextSequence      uint64
	SequenceExhausted bool
}

// ReplayWAL validates and atomically applies every complete batch to table.
// It deliberately replays all history because Phase 1F has no manifest
// authority with which to skip flushed records.
func ReplayWAL(path string, table *memtable.MemTable) (ReplayResult, error) {
	if table == nil || table.Frozen() {
		return ReplayResult{}, ErrInvalidOptions
	}
	var result ReplayResult
	var haveSequence bool
	_, err := wal.Recover(path, func(record wal.Record) error {
		batch, decodeErr := storage.DecodeWriteBatch(record.Payload)
		if decodeErr != nil {
			return fmt.Errorf("decode batch: %w", decodeErr)
		}
		if haveSequence && (result.SequenceExhausted || batch.FirstSequence != result.NextSequence) {
			return ErrSequenceDiscontinuity
		}
		if applyErr := table.ApplyBatch(batch); applyErr != nil {
			return fmt.Errorf("apply recovered batch: %w", applyErr)
		}
		count := uint64(len(batch.Mutations)) //nolint:gosec // decoded count is bounded
		last := batch.FirstSequence + count - 1
		result.Batches++
		result.Entries += count
		if last == math.MaxUint64 {
			result.SequenceExhausted = true
		} else {
			result.NextSequence = last + 1
		}
		haveSequence = true
		return nil
	})
	if err != nil {
		return result, fmt.Errorf("recover pipeline WAL: %w", err)
	}
	return result, nil
}
