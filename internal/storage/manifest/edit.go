package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

const (
	EditVersion       uint16 = 1
	MaxEditSize              = 16 << 20
	MaxChangesPerEdit        = 4096
	MaxLevels                = 8
	ComparatorName           = "rivetdb.internal-key.v1"

	editHeaderSize         = 8
	fieldHeaderSize        = 5
	editMagic       uint32 = 0x44455652 // little-endian bytes spell RVED
)

const (
	fieldComparator byte = iota + 1
	fieldNextFile
	fieldLastSequence
	fieldReplayFrontier
	fieldDeleteFile
	fieldAddFile
	fieldStorageMode
	fieldReplicatedFrontier
)

// StorageMode identifies which durability authority owns logical writes.
type StorageMode uint8

const (
	ModeStandalone StorageMode = iota + 1
	ModeReplicated
)

// DeletedFile identifies one live file removal. Phase 1G persists this shape
// for Phase 1H but does not execute compaction.
type DeletedFile struct {
	Level      uint32
	FileNumber uint64
}

// TableMetadata is the durable logical metadata for one SSTable.
type TableMetadata struct {
	Level            uint32
	FileNumber       uint64
	FlushGeneration  uint64
	FileSize         uint64
	EntryCount       uint64
	DeletionCount    uint64
	RawKeyValueBytes uint64
	DataBlockCount   uint32
	SmallestSequence uint64
	LargestSequence  uint64
	SmallestInternal storage.InternalKey
	LargestInternal  storage.InternalKey
	SmallestUser     []byte
	LargestUser      []byte
}

// NewTableMetadata converts validated Phase 1D metadata into a manifest entry.
func NewTableMetadata(level uint32, generation, smallestSequence, largestSequence uint64, metadata sstable.Metadata) (TableMetadata, error) {
	table := TableMetadata{
		Level: level, FileNumber: metadata.FileNumber, FlushGeneration: generation,
		FileSize: metadata.FileSize, EntryCount: metadata.EntryCount,
		DeletionCount: metadata.DeletionCount, RawKeyValueBytes: metadata.RawKeyValueBytes,
		DataBlockCount: metadata.DataBlockCount, SmallestSequence: smallestSequence,
		LargestSequence: largestSequence, SmallestInternal: metadata.SmallestInternal,
		LargestInternal: metadata.LargestInternal, SmallestUser: bytes.Clone(metadata.SmallestUser),
		LargestUser: bytes.Clone(metadata.LargestUser),
	}
	if err := validateTable(table); err != nil {
		return TableMetadata{}, err
	}
	return cloneTable(table), nil
}

// VersionEdit atomically describes one immutable Version transition. Pointer
// scalar fields distinguish absence from the valid zero value.
type VersionEdit struct {
	Comparator               *string
	NextFileNumber           *uint64
	LastSequence             *uint64
	ReplayFrontier           *uint64
	StorageMode              *StorageMode
	ReplicatedAppliedThrough *uint64
	DeletedFiles             []DeletedFile
	AddedFiles               []TableMetadata
}

// EncodeVersionEdit returns deterministic bounded binary TLV bytes.
func EncodeVersionEdit(edit VersionEdit) ([]byte, error) {
	if err := validateEditShape(edit); err != nil {
		return nil, err
	}
	fields := make([]encodedField, 0, 4+len(edit.DeletedFiles)+len(edit.AddedFiles))
	if edit.Comparator != nil {
		fields = append(fields, encodedField{tag: fieldComparator, payload: []byte(*edit.Comparator)})
	}
	if edit.NextFileNumber != nil {
		fields = append(fields, encodedField{tag: fieldNextFile, payload: appendUint64(nil, *edit.NextFileNumber)})
	}
	if edit.LastSequence != nil {
		fields = append(fields, encodedField{tag: fieldLastSequence, payload: appendUint64(nil, *edit.LastSequence)})
	}
	if edit.ReplayFrontier != nil {
		fields = append(fields, encodedField{tag: fieldReplayFrontier, payload: appendUint64(nil, *edit.ReplayFrontier)})
	}
	if edit.StorageMode != nil {
		fields = append(fields, encodedField{tag: fieldStorageMode, payload: []byte{byte(*edit.StorageMode)}})
	}
	if edit.ReplicatedAppliedThrough != nil {
		fields = append(fields, encodedField{tag: fieldReplicatedFrontier, payload: appendUint64(nil, *edit.ReplicatedAppliedThrough)})
	}
	deletions := slices.Clone(edit.DeletedFiles)
	slices.SortFunc(deletions, compareDeleted)
	for _, deleted := range deletions {
		payload := binary.LittleEndian.AppendUint32(nil, deleted.Level)
		payload = binary.LittleEndian.AppendUint64(payload, deleted.FileNumber)
		fields = append(fields, encodedField{tag: fieldDeleteFile, payload: payload})
	}
	additions := cloneTables(edit.AddedFiles)
	slices.SortFunc(additions, compareTableIdentity)
	for _, table := range additions {
		payload, err := encodeTable(table)
		if err != nil {
			return nil, err
		}
		fields = append(fields, encodedField{tag: fieldAddFile, payload: payload})
	}
	if len(fields) > math.MaxUint16 {
		return nil, ErrInvalidEdit
	}
	var header [editHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], editMagic)
	binary.LittleEndian.PutUint16(header[4:6], EditVersion)
	binary.LittleEndian.PutUint16(header[6:8], uint16(len(fields))) //nolint:gosec // checked above
	result := make([]byte, 0, editHeaderSize+len(fields)*fieldHeaderSize)
	result = append(result, header[:]...)
	for _, field := range fields {
		if len(field.payload) > math.MaxUint32 || len(result) > MaxEditSize-fieldHeaderSize-len(field.payload) {
			return nil, ErrInvalidEdit
		}
		result = append(result, field.tag)
		result = binary.LittleEndian.AppendUint32(result, uint32(len(field.payload))) //nolint:gosec // checked above
		result = append(result, field.payload...)
	}
	return result, nil
}

// DecodeVersionEdit decodes and validates one complete edit payload.
func DecodeVersionEdit(encoded []byte) (VersionEdit, error) {
	if len(encoded) < editHeaderSize || len(encoded) > MaxEditSize {
		return VersionEdit{}, ErrInvalidEdit
	}
	if binary.LittleEndian.Uint32(encoded[0:4]) != editMagic {
		return VersionEdit{}, errors.Join(ErrInvalidEdit, ErrManifestCorrupt)
	}
	if binary.LittleEndian.Uint16(encoded[4:6]) != EditVersion {
		return VersionEdit{}, ErrUnsupportedVersion
	}
	count := int(binary.LittleEndian.Uint16(encoded[6:8]))
	offset := editHeaderSize
	var edit VersionEdit
	seenScalar := make(map[byte]bool, 4)
	seenFiles := make(map[uint64]byte)
	for index := 0; index < count; index++ {
		if len(encoded)-offset < fieldHeaderSize {
			return VersionEdit{}, ErrInvalidEdit
		}
		tag := encoded[offset]
		length := uint64(binary.LittleEndian.Uint32(encoded[offset+1 : offset+5]))
		offset += fieldHeaderSize
		if length > uint64(len(encoded)-offset) { //nolint:gosec // subtraction is nonnegative after header check
			return VersionEdit{}, ErrInvalidEdit
		}
		payload := encoded[offset : offset+int(length)] //nolint:gosec // length bounded by remaining int
		offset += int(length)                           //nolint:gosec // length bounded by remaining int
		switch tag {
		case fieldComparator:
			if seenScalar[tag] || len(payload) == 0 || len(payload) > 128 {
				return VersionEdit{}, ErrInvalidEdit
			}
			value := string(payload)
			edit.Comparator = &value
			seenScalar[tag] = true
		case fieldNextFile, fieldLastSequence, fieldReplayFrontier, fieldReplicatedFrontier:
			if seenScalar[tag] || len(payload) != 8 {
				return VersionEdit{}, ErrInvalidEdit
			}
			value := binary.LittleEndian.Uint64(payload)
			switch tag {
			case fieldNextFile:
				edit.NextFileNumber = &value
			case fieldLastSequence:
				edit.LastSequence = &value
			case fieldReplayFrontier:
				edit.ReplayFrontier = &value
			case fieldReplicatedFrontier:
				edit.ReplicatedAppliedThrough = &value
			}
			seenScalar[tag] = true
		case fieldStorageMode:
			if seenScalar[tag] || len(payload) != 1 {
				return VersionEdit{}, ErrInvalidEdit
			}
			value := StorageMode(payload[0])
			edit.StorageMode = &value
			seenScalar[tag] = true
		case fieldDeleteFile:
			if len(payload) != 12 || len(edit.DeletedFiles) >= MaxChangesPerEdit {
				return VersionEdit{}, ErrInvalidEdit
			}
			deleted := DeletedFile{Level: binary.LittleEndian.Uint32(payload), FileNumber: binary.LittleEndian.Uint64(payload[4:])}
			if seenFiles[deleted.FileNumber] != 0 {
				return VersionEdit{}, ErrInvalidEdit
			}
			seenFiles[deleted.FileNumber] = tag
			edit.DeletedFiles = append(edit.DeletedFiles, deleted)
		case fieldAddFile:
			if len(edit.AddedFiles) >= MaxChangesPerEdit {
				return VersionEdit{}, ErrInvalidEdit
			}
			table, err := decodeTable(payload)
			if err != nil || seenFiles[table.FileNumber] != 0 {
				return VersionEdit{}, errors.Join(ErrInvalidEdit, err)
			}
			seenFiles[table.FileNumber] = tag
			edit.AddedFiles = append(edit.AddedFiles, table)
		default:
			if tag&0x80 == 0 {
				return VersionEdit{}, ErrUnsupportedVersion
			}
		}
	}
	if offset != len(encoded) {
		return VersionEdit{}, ErrInvalidEdit
	}
	if err := validateEditShape(edit); err != nil {
		return VersionEdit{}, err
	}
	return edit, nil
}

type encodedField struct {
	tag     byte
	payload []byte
}

func validateEditShape(edit VersionEdit) error {
	if edit.Comparator == nil && edit.NextFileNumber == nil && edit.LastSequence == nil && edit.ReplayFrontier == nil && edit.StorageMode == nil && edit.ReplicatedAppliedThrough == nil && len(edit.AddedFiles) == 0 && len(edit.DeletedFiles) == 0 {
		return ErrInvalidEdit
	}
	if len(edit.AddedFiles) > MaxChangesPerEdit || len(edit.DeletedFiles) > MaxChangesPerEdit {
		return ErrInvalidEdit
	}
	if edit.StorageMode != nil && *edit.StorageMode != ModeStandalone && *edit.StorageMode != ModeReplicated || edit.ReplayFrontier != nil && edit.ReplicatedAppliedThrough != nil {
		return ErrInvalidEdit
	}
	if edit.Comparator != nil && (*edit.Comparator == "" || len(*edit.Comparator) > 128) {
		return ErrInvalidEdit
	}
	seen := make(map[uint64]struct{}, len(edit.AddedFiles)+len(edit.DeletedFiles))
	for _, deleted := range edit.DeletedFiles {
		if deleted.Level >= MaxLevels || deleted.FileNumber == 0 || deleted.FileNumber > sstable.MaxFileNumber {
			return ErrInvalidEdit
		}
		if _, exists := seen[deleted.FileNumber]; exists {
			return ErrInvalidEdit
		}
		seen[deleted.FileNumber] = struct{}{}
	}
	for _, table := range edit.AddedFiles {
		if err := validateTable(table); err != nil {
			return err
		}
		if _, exists := seen[table.FileNumber]; exists {
			return ErrInvalidEdit
		}
		seen[table.FileNumber] = struct{}{}
	}
	return nil
}

func validateTable(table TableMetadata) error {
	if table.Level >= MaxLevels {
		return errors.Join(ErrInvalidEdit, ErrInvalidLevel)
	}
	if table.FileNumber == 0 || table.FileNumber > sstable.MaxFileNumber || table.FileSize < sstable.FooterSize ||
		table.EntryCount == 0 || table.DataBlockCount == 0 || table.DeletionCount > table.EntryCount ||
		table.SmallestSequence > table.LargestSequence {
		return ErrInvalidEdit
	}
	if len(table.SmallestUser) > sstable.MaxUserKeySize || len(table.LargestUser) > sstable.MaxUserKeySize ||
		!bytes.Equal(table.SmallestInternal.UserKey(), table.SmallestUser) ||
		!bytes.Equal(table.LargestInternal.UserKey(), table.LargestUser) ||
		storage.CompareInternal(table.SmallestInternal, table.LargestInternal) > 0 ||
		bytes.Compare(table.SmallestUser, table.LargestUser) > 0 {
		return ErrInvalidEdit
	}
	return nil
}

func encodeTable(table TableMetadata) ([]byte, error) {
	if err := validateTable(table); err != nil {
		return nil, err
	}
	result := binary.LittleEndian.AppendUint32(nil, table.Level)
	for _, value := range []uint64{table.FileNumber, table.FlushGeneration, table.FileSize, table.EntryCount, table.DeletionCount, table.RawKeyValueBytes} {
		result = binary.LittleEndian.AppendUint64(result, value)
	}
	result = binary.LittleEndian.AppendUint32(result, table.DataBlockCount)
	result = binary.LittleEndian.AppendUint64(result, table.SmallestSequence)
	result = binary.LittleEndian.AppendUint64(result, table.LargestSequence)
	for _, value := range [][]byte{table.SmallestInternal.Encode(), table.LargestInternal.Encode(), table.SmallestUser, table.LargestUser} {
		if len(value) > math.MaxUint32 {
			return nil, ErrInvalidEdit
		}
		result = binary.LittleEndian.AppendUint32(result, uint32(len(value))) //nolint:gosec // SSTable bounds fit u32
		result = append(result, value...)
	}
	return result, nil
}

func decodeTable(payload []byte) (TableMetadata, error) {
	const fixed = 4 + 6*8 + 4 + 2*8
	if len(payload) < fixed+4*4 {
		return TableMetadata{}, ErrInvalidEdit
	}
	table := TableMetadata{Level: binary.LittleEndian.Uint32(payload)}
	offset := 4
	values := []*uint64{&table.FileNumber, &table.FlushGeneration, &table.FileSize, &table.EntryCount, &table.DeletionCount, &table.RawKeyValueBytes}
	for _, target := range values {
		*target = binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
	}
	table.DataBlockCount = binary.LittleEndian.Uint32(payload[offset : offset+4])
	offset += 4
	table.SmallestSequence = binary.LittleEndian.Uint64(payload[offset : offset+8])
	offset += 8
	table.LargestSequence = binary.LittleEndian.Uint64(payload[offset : offset+8])
	offset += 8
	parts := make([][]byte, 4)
	for index := range parts {
		if len(payload)-offset < 4 {
			return TableMetadata{}, ErrInvalidEdit
		}
		length := uint64(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if length > uint64(len(payload)-offset) { //nolint:gosec // subtraction is nonnegative after prefix check
			return TableMetadata{}, ErrInvalidEdit
		}
		parts[index] = bytes.Clone(payload[offset : offset+int(length)]) //nolint:gosec // bounded by remaining int
		offset += int(length)                                            //nolint:gosec // bounded by remaining int
	}
	if offset != len(payload) {
		return TableMetadata{}, ErrInvalidEdit
	}
	smallestInternal, largestInternal := parts[0], parts[1]
	smallestUser, largestUser := parts[2], parts[3]
	if len(smallestInternal) > sstable.MaxInternalKeySize || len(largestInternal) > sstable.MaxInternalKeySize ||
		len(smallestUser) > sstable.MaxUserKeySize || len(largestUser) > sstable.MaxUserKeySize {
		return TableMetadata{}, ErrInvalidEdit
	}
	var err error
	table.SmallestInternal, err = storage.DecodeInternalKey(smallestInternal)
	if err != nil {
		return TableMetadata{}, fmt.Errorf("decode smallest internal key: %w", err)
	}
	table.LargestInternal, err = storage.DecodeInternalKey(largestInternal)
	if err != nil {
		return TableMetadata{}, fmt.Errorf("decode largest internal key: %w", err)
	}
	table.SmallestUser, table.LargestUser = smallestUser, largestUser
	if err := validateTable(table); err != nil {
		return TableMetadata{}, err
	}
	return table, nil
}

func appendUint64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}

func compareDeleted(left, right DeletedFile) int {
	if left.Level != right.Level {
		return int(left.Level) - int(right.Level)
	}
	return cmpUint64(left.FileNumber, right.FileNumber)
}

func compareTableIdentity(left, right TableMetadata) int {
	if left.Level != right.Level {
		return int(left.Level) - int(right.Level)
	}
	return cmpUint64(left.FileNumber, right.FileNumber)
}

func cmpUint64(left, right uint64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func cloneTable(table TableMetadata) TableMetadata {
	table.SmallestUser = bytes.Clone(table.SmallestUser)
	table.LargestUser = bytes.Clone(table.LargestUser)
	var err error
	table.SmallestInternal, err = storage.DecodeInternalKey(table.SmallestInternal.Encode())
	invariant.Assert(err == nil, "STORAGE-62", "clone valid smallest internal key: %v", err)
	table.LargestInternal, err = storage.DecodeInternalKey(table.LargestInternal.Encode())
	invariant.Assert(err == nil, "STORAGE-62", "clone valid largest internal key: %v", err)
	return table
}

func cloneTables(tables []TableMetadata) []TableMetadata {
	result := make([]TableMetadata, len(tables))
	for index, table := range tables {
		result[index] = cloneTable(table)
	}
	return result
}
