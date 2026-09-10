package engine

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

// Validate performs the O(number of live files + table bytes) integrated
// structural audit when expensive invariants are enabled.
func (e *Engine) Validate() (resultErr error) {
	if !invariant.Expensive() {
		return nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return ErrClosed
	}
	view, err := e.captureReadView()
	if err != nil {
		return err
	}
	if view.memtables.Active.Table == nil || view.memtables.Active.Table.Frozen() {
		return errors.Join(ErrCorruption, errActiveState)
	}
	for _, generation := range view.memtables.Immutables {
		if generation.Table == nil || !generation.Table.Frozen() {
			return errors.Join(ErrCorruption, errImmutableState)
		}
	}
	for level := uint32(0); level < manifest.MaxLevels; level++ {
		tables := view.version.Files(level)
		for index, table := range tables {
			if level > 0 && index > 0 && bytes.Compare(tables[index-1].LargestUser, table.SmallestUser) >= 0 {
				return errors.Join(ErrCorruption, errLevelOverlap)
			}
			if view.memtables.HaveSequence && table.LargestSequence > view.memtables.LatestSequence {
				return errors.Join(ErrCorruption, errSequenceBehind)
			}
			reader, openErr := sstable.Open(filepath.Join(e.directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
			if openErr != nil {
				return fmt.Errorf("validate live table %d: %w", table.FileNumber, errors.Join(ErrCorruption, openErr))
			}
			if !metadataMatches(table, reader.Metadata()) {
				return errors.Join(ErrCorruption, manifest.ErrMetadataMismatch, reader.Close())
			}
			if validateErr := reader.ValidateAll(); validateErr != nil {
				return errors.Join(fmt.Errorf("reread live table %d: %w", table.FileNumber, validateErr), reader.Close())
			}
			if closeErr := reader.Close(); closeErr != nil {
				return fmt.Errorf("close validated table %d: %w", table.FileNumber, closeErr)
			}
		}
	}
	return nil
}
