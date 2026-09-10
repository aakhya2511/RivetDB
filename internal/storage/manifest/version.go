package manifest

import (
	"bytes"
	"fmt"
	"math"
	"slices"
)

// Version is an immutable logical view. Its methods return copied slices and
// metadata, so a caller cannot mutate VersionSet state.
type Version struct {
	generation       uint64
	comparator       string
	levels           [MaxLevels][]TableMetadata
	nextFileNumber   uint64
	haveLastSequence bool
	lastSequence     uint64
	haveFrontier     bool
	frontier         uint64
}

func initialVersion() *Version { return &Version{nextFileNumber: 1} }

// Generation is the number of durable edits represented by this Version.
func (v *Version) Generation() uint64 { return v.generation }

// Comparator returns the persistent internal-key comparator identity.
func (v *Version) Comparator() string { return v.comparator }

// Files returns a copy of one level's deterministic file ordering.
func (v *Version) Files(level uint32) []TableMetadata {
	if v == nil || level >= MaxLevels {
		return nil
	}
	return cloneTables(v.levels[level])
}

// LiveFileNumbers returns every live file number in ascending order.
func (v *Version) LiveFileNumbers() []uint64 {
	if v == nil {
		return nil
	}
	result := make([]uint64, 0, v.LiveTableCount())
	for _, level := range v.levels {
		for _, table := range level {
			result = append(result, table.FileNumber)
		}
	}
	slices.Sort(result)
	return result
}

// LiveTableCount returns the number of authoritative SSTables.
func (v *Version) LiveTableCount() int {
	if v == nil {
		return 0
	}
	count := 0
	for _, level := range v.levels {
		count += len(level)
	}
	return count
}

// NextFileNumber returns the first number not yet durably reserved.
func (v *Version) NextFileNumber() uint64 { return v.nextFileNumber }

// LastSequence returns the durable sequence high-water mark, if one exists.
func (v *Version) LastSequence() (uint64, bool) { return v.lastSequence, v.haveLastSequence }

// ReplayFrontier returns the inclusive safely installed WAL frontier.
func (v *Version) ReplayFrontier() (uint64, bool) { return v.frontier, v.haveFrontier }

func (v *Version) apply(edit VersionEdit) (*Version, error) {
	if err := validateEditShape(edit); err != nil {
		return nil, err
	}
	next := v.clone()
	if next.generation == math.MaxUint64 {
		return nil, ErrInvalidEdit
	}
	if edit.Comparator != nil {
		if *edit.Comparator != ComparatorName || next.comparator != "" && next.comparator != *edit.Comparator {
			return nil, ErrComparatorMismatch
		}
		next.comparator = *edit.Comparator
	}
	if next.comparator == "" {
		return nil, ErrComparatorMismatch
	}
	if edit.NextFileNumber != nil {
		if *edit.NextFileNumber < next.nextFileNumber || *edit.NextFileNumber == 0 || *edit.NextFileNumber > maxNextFileNumber() {
			return nil, ErrFileNumberCollision
		}
		next.nextFileNumber = *edit.NextFileNumber
	}
	if edit.LastSequence != nil {
		if next.haveLastSequence && *edit.LastSequence < next.lastSequence {
			return nil, ErrSequenceRegression
		}
		next.lastSequence, next.haveLastSequence = *edit.LastSequence, true
	}

	seen := make(map[uint64]struct{}, len(edit.DeletedFiles)+len(edit.AddedFiles))
	for _, deletion := range edit.DeletedFiles {
		if _, duplicate := seen[deletion.FileNumber]; duplicate {
			return nil, ErrInvalidEdit
		}
		seen[deletion.FileNumber] = struct{}{}
		index := findFile(next.levels[deletion.Level], deletion.FileNumber)
		if index < 0 {
			return nil, ErrUnknownFile
		}
		next.levels[deletion.Level] = slices.Delete(next.levels[deletion.Level], index, index+1)
	}
	for _, table := range edit.AddedFiles {
		if _, duplicate := seen[table.FileNumber]; duplicate {
			return nil, ErrInvalidEdit
		}
		seen[table.FileNumber] = struct{}{}
		if next.hasFile(table.FileNumber) {
			return nil, ErrDuplicateFile
		}
		if table.FileNumber >= next.nextFileNumber {
			return nil, ErrFileNumberCollision
		}
		next.levels[table.Level] = append(next.levels[table.Level], cloneTable(table))
	}
	for level := range next.levels {
		next.sortLevel(uint32(level)) //nolint:gosec // fixed MaxLevels bound
		if level > 0 && overlaps(next.levels[level]) {
			return nil, ErrOverlappingLevel
		}
	}
	if edit.ReplayFrontier != nil {
		if next.haveFrontier && *edit.ReplayFrontier < next.frontier {
			return nil, ErrFrontierRegression
		}
		if !next.haveLastSequence || *edit.ReplayFrontier > next.lastSequence {
			return nil, ErrFrontierGap
		}
		if !next.frontierCovered(*edit.ReplayFrontier) {
			return nil, ErrFrontierGap
		}
		next.frontier, next.haveFrontier = *edit.ReplayFrontier, true
	}
	if highest := next.highestFile(); highest >= next.nextFileNumber {
		return nil, ErrFileNumberCollision
	}
	next.generation++
	return next, nil
}

func (v *Version) clone() *Version {
	if v == nil {
		return initialVersion()
	}
	result := *v
	for level := range v.levels {
		result.levels[level] = cloneTables(v.levels[level])
	}
	return &result
}

func (v *Version) hasFile(fileNumber uint64) bool {
	for _, level := range v.levels {
		if findFile(level, fileNumber) >= 0 {
			return true
		}
	}
	return false
}

func (v *Version) highestFile() uint64 {
	var highest uint64
	for _, level := range v.levels {
		for _, table := range level {
			highest = max(highest, table.FileNumber)
		}
	}
	return highest
}

func (v *Version) sortLevel(level uint32) {
	if level == 0 {
		slices.SortFunc(v.levels[level], func(left, right TableMetadata) int {
			if order := cmpUint64(right.LargestSequence, left.LargestSequence); order != 0 {
				return order
			}
			if order := cmpUint64(right.FlushGeneration, left.FlushGeneration); order != 0 {
				return order
			}
			return cmpUint64(right.FileNumber, left.FileNumber)
		})
		return
	}
	slices.SortFunc(v.levels[level], func(left, right TableMetadata) int {
		if order := bytes.Compare(left.SmallestUser, right.SmallestUser); order != 0 {
			return order
		}
		return cmpUint64(left.FileNumber, right.FileNumber)
	})
}

func (v *Version) frontierCovered(target uint64) bool {
	if v.haveFrontier && target == v.frontier {
		return true
	}
	start := uint64(0)
	if v.haveFrontier {
		if v.frontier == math.MaxUint64 {
			return target == math.MaxUint64
		}
		start = v.frontier + 1
	}
	spans := make([]sequenceSpan, 0, v.LiveTableCount())
	for _, level := range v.levels {
		for _, table := range level {
			if table.LargestSequence >= start && table.SmallestSequence <= target {
				spans = append(spans, sequenceSpan{table.SmallestSequence, table.LargestSequence})
			}
		}
	}
	slices.SortFunc(spans, func(left, right sequenceSpan) int {
		if order := cmpUint64(left.first, right.first); order != 0 {
			return order
		}
		return cmpUint64(right.last, left.last)
	})
	cursor := start
	for _, span := range spans {
		if span.first > cursor {
			return false
		}
		if span.last < cursor {
			continue
		}
		if span.last >= target {
			return true
		}
		cursor = span.last + 1
	}
	return false
}

func (v *Version) maximumContiguousFrontier() (uint64, bool) {
	start := uint64(0)
	if v.haveFrontier {
		if v.frontier == math.MaxUint64 {
			return v.frontier, true
		}
		start = v.frontier + 1
	}
	spans := make([]sequenceSpan, 0, v.LiveTableCount())
	for _, level := range v.levels {
		for _, table := range level {
			if table.LargestSequence >= start {
				spans = append(spans, sequenceSpan{table.SmallestSequence, table.LargestSequence})
			}
		}
	}
	slices.SortFunc(spans, func(left, right sequenceSpan) int {
		if order := cmpUint64(left.first, right.first); order != 0 {
			return order
		}
		return cmpUint64(right.last, left.last)
	})
	cursor := start
	advanced := false
	for _, span := range spans {
		if span.first > cursor {
			break
		}
		if span.last < cursor {
			continue
		}
		advanced = true
		if span.last == math.MaxUint64 {
			return math.MaxUint64, true
		}
		cursor = span.last + 1
	}
	if !advanced {
		return v.frontier, v.haveFrontier
	}
	return cursor - 1, true
}

func overlaps(tables []TableMetadata) bool {
	for index := 1; index < len(tables); index++ {
		if bytes.Compare(tables[index-1].LargestUser, tables[index].SmallestUser) >= 0 {
			return true
		}
	}
	return false
}

func findFile(tables []TableMetadata, fileNumber uint64) int {
	return slices.IndexFunc(tables, func(table TableMetadata) bool { return table.FileNumber == fileNumber })
}

func maxNextFileNumber() uint64 {
	// MaxFileNumber itself can be reserved; the following sentinel means no
	// further allocation is possible.
	return sstableMaxFileNumberPlusOne
}

const sstableMaxFileNumberPlusOne = uint64(1_000_000_000_000)

type sequenceSpan struct{ first, last uint64 }

func (v *Version) snapshotEdit() (VersionEdit, error) {
	comparator := v.comparator
	nextFile := v.nextFileNumber
	edit := VersionEdit{Comparator: &comparator, NextFileNumber: &nextFile}
	if v.haveLastSequence {
		last := v.lastSequence
		edit.LastSequence = &last
	}
	if v.haveFrontier {
		frontier := v.frontier
		edit.ReplayFrontier = &frontier
	}
	for _, level := range v.levels {
		edit.AddedFiles = append(edit.AddedFiles, cloneTables(level)...)
	}
	if _, err := initialVersion().apply(edit); err != nil {
		return VersionEdit{}, fmt.Errorf("construct Manifest snapshot: %w", err)
	}
	return edit, nil
}

func validateEquivalent(left, right *Version) error {
	if left.comparator != right.comparator || left.nextFileNumber != right.nextFileNumber ||
		left.haveLastSequence != right.haveLastSequence || left.lastSequence != right.lastSequence ||
		left.haveFrontier != right.haveFrontier || left.frontier != right.frontier {
		return ErrManifestCorrupt
	}
	for level := range left.levels {
		if len(left.levels[level]) != len(right.levels[level]) {
			return ErrManifestCorrupt
		}
		for index := range left.levels[level] {
			if !tableEqual(left.levels[level][index], right.levels[level][index]) {
				return ErrManifestCorrupt
			}
		}
	}
	return nil
}

func tableEqual(left, right TableMetadata) bool {
	return left.Level == right.Level && left.FileNumber == right.FileNumber && left.FlushGeneration == right.FlushGeneration &&
		left.FileSize == right.FileSize && left.EntryCount == right.EntryCount && left.DeletionCount == right.DeletionCount &&
		left.RawKeyValueBytes == right.RawKeyValueBytes && left.DataBlockCount == right.DataBlockCount &&
		left.SmallestSequence == right.SmallestSequence && left.LargestSequence == right.LargestSequence &&
		bytes.Equal(left.SmallestInternal.Encode(), right.SmallestInternal.Encode()) && bytes.Equal(left.LargestInternal.Encode(), right.LargestInternal.Encode()) &&
		bytes.Equal(left.SmallestUser, right.SmallestUser) && bytes.Equal(left.LargestUser, right.LargestUser)
}
