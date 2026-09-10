package manifest

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

type recoveryResult struct {
	version *Version
	wal     wal.RecoveryResult
}

func recoverManifest(path string) (recoveryResult, error) {
	version := initialVersion()
	var applied uint64
	result, err := wal.Recover(path, func(record wal.Record) error {
		edit, decodeErr := DecodeVersionEdit(record.Payload)
		if decodeErr != nil {
			return fmt.Errorf("decode VersionEdit %d: %w", record.Number, decodeErr)
		}
		candidate, applyErr := version.apply(edit)
		if applyErr != nil {
			return fmt.Errorf("apply VersionEdit %d: %w", record.Number, applyErr)
		}
		version = candidate
		applied++
		return nil
	})
	if err != nil {
		return recoveryResult{}, fmt.Errorf("recover Manifest: %w", errors.Join(ErrManifestCorrupt, err))
	}
	if applied == 0 || version.comparator != ComparatorName {
		return recoveryResult{}, errors.Join(ErrManifestCorrupt, ErrComparatorMismatch)
	}
	return recoveryResult{version: version, wal: result}, nil
}

// Discovery classifies storage files without promoting unlisted SSTables.
type Discovery struct {
	Live      []uint64
	Orphans   []uint64
	Temporary []string
	Invalid   []string
}

func discoverAndValidate(directory string, version *Version) (Discovery, uint64, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return Discovery{}, 0, fmt.Errorf("read storage directory: %w", err)
	}
	live := make(map[uint64]TableMetadata, version.LiveTableCount())
	for _, level := range version.levels {
		for _, table := range level {
			live[table.FileNumber] = table
		}
	}
	report := Discovery{}
	seenLive := make(map[uint64]bool, len(live))
	maxObserved := uint64(0)
	for _, entry := range entries {
		name := entry.Name()
		if number, ok := parseSSTableName(name, false); ok {
			maxObserved = max(maxObserved, number)
			path := filepath.Join(directory, name)
			if expected, authoritative := live[number]; authoritative {
				if err := validatePhysicalTable(path, expected); err != nil {
					return report, maxObserved, err
				}
				seenLive[number] = true
				report.Live = append(report.Live, number)
				continue
			}
			reader, openErr := sstable.Open(path, sstable.ReaderOptions{})
			if openErr != nil {
				report.Invalid = append(report.Invalid, name)
				continue
			}
			if closeErr := reader.Close(); closeErr != nil {
				return report, maxObserved, fmt.Errorf("close orphan SSTable %s: %w", name, closeErr)
			}
			report.Orphans = append(report.Orphans, number)
			continue
		}
		if number, ok := parseSSTableName(name, true); ok {
			maxObserved = max(maxObserved, number)
			report.Temporary = append(report.Temporary, name)
			continue
		}
		if name == currentTempName || strings.HasSuffix(name, ".manifest.tmp") {
			report.Temporary = append(report.Temporary, name)
		}
	}
	for number := range live {
		if !seenLive[number] {
			return report, maxObserved, fmt.Errorf("file %d: %w", number, ErrMissingLiveTable)
		}
	}
	slices.Sort(report.Live)
	slices.Sort(report.Orphans)
	slices.Sort(report.Temporary)
	slices.Sort(report.Invalid)
	return report, maxObserved, nil
}

func validatePhysicalTable(path string, expected TableMetadata) error {
	reader, err := sstable.Open(path, sstable.ReaderOptions{})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("open live SSTable %d: %w", expected.FileNumber, errors.Join(ErrMissingLiveTable, err))
		}
		return fmt.Errorf("open live SSTable %d: %w", expected.FileNumber, errors.Join(ErrCorruptLiveTable, err))
	}
	actual := reader.Metadata()
	closeErr := reader.Close()
	if closeErr != nil {
		return fmt.Errorf("close live SSTable %d: %w", expected.FileNumber, errors.Join(ErrCorruptLiveTable, closeErr))
	}
	if !sstableMetadataEqual(expected, actual) {
		return fmt.Errorf("live SSTable %d: %w", expected.FileNumber, errors.Join(ErrCorruptLiveTable, ErrMetadataMismatch))
	}
	return nil
}

func sstableMetadataEqual(expected TableMetadata, actual sstable.Metadata) bool {
	return expected.FileNumber == actual.FileNumber && expected.FileSize == actual.FileSize &&
		expected.EntryCount == actual.EntryCount && expected.DeletionCount == actual.DeletionCount &&
		expected.RawKeyValueBytes == actual.RawKeyValueBytes && expected.DataBlockCount == actual.DataBlockCount &&
		storage.CompareInternal(expected.SmallestInternal, actual.SmallestInternal) == 0 &&
		storage.CompareInternal(expected.LargestInternal, actual.LargestInternal) == 0 &&
		slices.Equal(expected.SmallestUser, actual.SmallestUser) && slices.Equal(expected.LargestUser, actual.LargestUser)
}

func parseSSTableName(name string, temporary bool) (uint64, bool) {
	suffix := ".sst"
	if temporary {
		suffix += ".tmp"
	}
	if len(name) != 12+len(suffix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	digits := name[:12]
	for _, character := range digits {
		if character < '0' || character > '9' {
			return 0, false
		}
	}
	number, err := strconv.ParseUint(digits, 10, 64)
	return number, err == nil && number != 0 && number <= sstable.MaxFileNumber
}

func maximumWALSequence(directory string) (uint64, bool, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, false, fmt.Errorf("read WAL directory: %w", err)
	}
	var maximum uint64
	var found bool
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		_, recoverErr := wal.Recover(filepath.Join(directory, entry.Name()), func(record wal.Record) error {
			batch, decodeErr := storage.DecodeWriteBatch(record.Payload)
			if decodeErr != nil {
				return fmt.Errorf("decode WAL batch: %w", decodeErr)
			}
			last := batch.FirstSequence + uint64(len(batch.Mutations)) - 1 //nolint:gosec // decoded batch is bounded and validated
			if !found || last > maximum {
				maximum, found = last, true
			}
			return nil
		})
		if recoverErr != nil {
			return 0, false, fmt.Errorf("recover WAL %s for sequence authority: %w", entry.Name(), recoverErr)
		}
	}
	return maximum, found, nil
}

// ReplayWAL applies complete batches strictly beyond the inclusive frontier.
func ReplayWAL(path string, frontier *uint64, table *memtable.MemTable) (uint64, error) {
	if table == nil || table.Frozen() {
		return 0, ErrInvalidOptions
	}
	var applied uint64
	_, err := wal.Recover(path, func(record wal.Record) error {
		batch, decodeErr := storage.DecodeWriteBatch(record.Payload)
		if decodeErr != nil {
			return fmt.Errorf("decode replay batch: %w", decodeErr)
		}
		last := batch.FirstSequence + uint64(len(batch.Mutations)) - 1 //nolint:gosec // decoded batch checked
		if frontier != nil {
			if last <= *frontier {
				return nil
			}
			if batch.FirstSequence <= *frontier {
				return ErrFrontierSplitsBatch
			}
		}
		if applyErr := table.ApplyBatch(batch); applyErr != nil {
			return fmt.Errorf("apply replay batch: %w", applyErr)
		}
		applied += uint64(len(batch.Mutations)) //nolint:gosec // decoded count bounded
		return nil
	})
	if err != nil {
		return applied, fmt.Errorf("replay WAL beyond Manifest frontier: %w", err)
	}
	return applied, nil
}

func nextSequence(maximum uint64, found bool) (uint64, bool) {
	if !found {
		return 0, false
	}
	if maximum == math.MaxUint64 {
		return 0, true
	}
	return maximum + 1, false
}
