package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/rivetdb/rivetdb/internal/storage"
)

type decodedEntry struct {
	key     storage.InternalKey
	encoded []byte
	value   []byte
}

type decodedDataBlock struct {
	handle         BlockHandle
	restarts       []uint32
	restartEntries []decodedRestart
	entries        []decodedEntry
}

type decodedRestart struct {
	offset     uint32
	entryIndex int
	key        storage.InternalKey
}

type decodedIndexEntry struct {
	key     storage.InternalKey
	encoded []byte
	handle  BlockHandle
}

type decodedMetadata struct {
	entryCount       uint64
	deletionCount    uint64
	dataBlockCount   uint32
	rawKeyValueBytes uint64
	smallestInternal []byte
	largestInternal  []byte
	smallestUser     []byte
	largestUser      []byte
}

func decodeIndex(payload []byte) ([]decodedIndexEntry, error) {
	if len(payload) < 4 {
		return nil, corrupt(ErrInvalidBlock, "short index")
	}
	count := binary.LittleEndian.Uint32(payload)
	remaining := payload[4:]
	const minimumIndexEntry = 1 + 9 + 16
	if count == 0 || uint64(count) > uint64(len(remaining))/minimumIndexEntry {
		return nil, corrupt(ErrInvalidBlock, "impossible index count %d", count)
	}
	entries := make([]decodedIndexEntry, 0, int(count))
	for range count {
		length, consumed, err := readCanonicalUvarint(remaining)
		if err != nil || length < 9 || length > MaxInternalKeySize {
			return nil, corrupt(ErrInvalidBlock, "invalid index key length")
		}
		remaining = remaining[consumed:]
		if length > uint64(len(remaining)) || uint64(len(remaining))-length < 16 {
			return nil, corrupt(ErrInvalidBlock, "truncated index entry")
		}
		encoded := bytes.Clone(remaining[:int(length)])
		remaining = remaining[int(length):]
		key, decodeErr := storage.DecodeInternalKey(encoded)
		if decodeErr != nil {
			return nil, corrupt(ErrInvalidBlock, "index internal key: %v", decodeErr)
		}
		handle := BlockHandle{
			Offset: binary.LittleEndian.Uint64(remaining),
			Length: binary.LittleEndian.Uint64(remaining[8:]),
		}
		remaining = remaining[16:]
		if len(entries) != 0 && storage.CompareInternal(entries[len(entries)-1].key, key) >= 0 {
			return nil, corrupt(ErrInvalidBlock, "index keys are not strictly ordered")
		}
		entries = append(entries, decodedIndexEntry{key: key, encoded: encoded, handle: handle})
	}
	if len(remaining) != 0 {
		return nil, corrupt(ErrInvalidBlock, "index has %d trailing bytes", len(remaining))
	}
	return entries, nil
}

func decodeDataBlock(payload []byte, handle BlockHandle) (decodedDataBlock, error) {
	entriesEnd, restartOffsets, err := decodeRestartLayout(payload)
	if err != nil {
		return decodedDataBlock{}, err
	}

	block := decodedDataBlock{handle: handle, restarts: restartOffsets, restartEntries: make([]decodedRestart, 0, len(restartOffsets))}
	position := uint64(0)
	nextRestart := 0
	var previous []byte
	for position < entriesEnd {
		entryOffset := position
		entry, next, shared, decodeErr := decodeDataEntry(payload, position, entriesEnd, previous)
		if decodeErr != nil {
			return decodedDataBlock{}, decodeErr
		}
		isRestart := nextRestart < len(restartOffsets) && restartOffsets[nextRestart] == uint32(entryOffset) //nolint:gosec // entriesEnd is u32-bounded
		if isRestart && shared != 0 {
			return decodedDataBlock{}, corrupt(ErrInvalidBlock, "restart entry has shared prefix")
		}
		if len(block.entries) != 0 && storage.CompareInternal(block.entries[len(block.entries)-1].key, entry.key) >= 0 {
			return decodedDataBlock{}, corrupt(ErrInvalidBlock, "data entries are not strictly ordered")
		}
		entryIndex := len(block.entries)
		block.entries = append(block.entries, entry)
		if isRestart {
			block.restartEntries = append(block.restartEntries, decodedRestart{offset: uint32(entryOffset), entryIndex: entryIndex, key: entry.key}) //nolint:gosec // entriesEnd is u32-bounded
			nextRestart++
		}
		previous = entry.encoded
		position = next
	}
	if position != entriesEnd || len(block.entries) == 0 || nextRestart != len(restartOffsets) {
		return decodedDataBlock{}, corrupt(ErrInvalidBlock, "restart does not identify an entry boundary")
	}
	return block, nil
}

func decodeRestartLayout(payload []byte) (uint64, []uint32, error) {
	if len(payload) < 8 || len(payload) > MaxDataBlockSize {
		return 0, nil, corrupt(ErrInvalidBlock, "invalid data payload length %d", len(payload))
	}
	restartCount := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if restartCount == 0 || uint64(restartCount) > uint64(len(payload)-4)/4 { //nolint:gosec // payload is at least eight bytes
		return 0, nil, corrupt(ErrInvalidBlock, "invalid restart count %d", restartCount)
	}
	restartBytes := uint64(restartCount) * 4
	entriesEnd := uint64(len(payload)-4) - restartBytes //nolint:gosec // payload is at least eight bytes
	if entriesEnd == 0 || entriesEnd > math.MaxUint32 {
		return 0, nil, corrupt(ErrInvalidBlock, "invalid entry region length")
	}
	restartsRaw := payload[int(entriesEnd) : len(payload)-4]
	restarts := make([]uint32, restartCount)
	for index := range restarts {
		restarts[index] = binary.LittleEndian.Uint32(restartsRaw[index*4:])
		if uint64(restarts[index]) >= entriesEnd || index == 0 && restarts[index] != 0 || index > 0 && restarts[index] <= restarts[index-1] {
			return 0, nil, corrupt(ErrInvalidBlock, "invalid restart offset at %d", index)
		}
	}
	return entriesEnd, restarts, nil
}

func decodeDataEntry(payload []byte, position, entriesEnd uint64, previous []byte) (decodedEntry, uint64, uint64, error) {
	shared, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)]) //nolint:gosec // offsets are payload-bounded
	if err != nil {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "shared length: %v", err)
	}
	position += uint64(consumed)                                                            //nolint:gosec // positive slice-derived count
	unshared, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)]) //nolint:gosec // offsets are payload-bounded
	if err != nil {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "unshared length: %v", err)
	}
	position += uint64(consumed)                                                               //nolint:gosec // positive slice-derived count
	valueLength, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)]) //nolint:gosec // offsets are payload-bounded
	if err != nil {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "value length: %v", err)
	}
	position += uint64(consumed) //nolint:gosec // positive slice-derived count
	if shared > uint64(len(previous)) || unshared > MaxInternalKeySize || valueLength > MaxValueSize {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "entry length exceeds bounds")
	}
	needed, ok := checkedAdd(unshared, valueLength)
	if !ok || needed > entriesEnd-position {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "truncated data entry")
	}
	keyLength, ok := checkedAdd(shared, unshared)
	if !ok || keyLength < 9 || keyLength > MaxInternalKeySize {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "invalid reconstructed key length")
	}
	keyBytes := make([]byte, int(keyLength))
	copy(keyBytes, previous[:int(shared)])                                      //nolint:gosec // shared is bounded by previous length
	copy(keyBytes[int(shared):], payload[int(position):int(position+unshared)]) //nolint:gosec // checked lengths
	position += unshared
	value := bytes.Clone(payload[int(position):int(position+valueLength)]) //nolint:gosec // checked lengths
	position += valueLength
	key, decodeErr := storage.DecodeInternalKey(keyBytes)
	if decodeErr != nil {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "internal key: %v", decodeErr)
	}
	if !key.Kind().CarriesValue() && len(value) != 0 {
		return decodedEntry{}, 0, 0, corrupt(ErrInvalidBlock, "deletion has value")
	}
	return decodedEntry{key: key, encoded: keyBytes, value: value}, position, shared, nil
}

func seekDataBlock(payload []byte, target storage.InternalKey) (decodedEntry, bool, error) {
	entriesEnd, restarts, err := decodeRestartLayout(payload)
	if err != nil {
		return decodedEntry{}, false, err
	}
	left, right := 0, len(restarts)
	for left < right {
		middle := left + (right-left)/2
		entry, _, shared, decodeErr := decodeDataEntry(payload, uint64(restarts[middle]), entriesEnd, nil)
		if decodeErr != nil || shared != 0 {
			return decodedEntry{}, false, corrupt(ErrInvalidBlock, "invalid restart entry %d", middle)
		}
		if storage.CompareInternal(entry.key, target) < 0 {
			left = middle + 1
		} else {
			right = middle
		}
	}
	restart := left
	if restart > 0 {
		restart--
	}
	position := uint64(restarts[restart])
	limit := entriesEnd
	if restart+1 < len(restarts) {
		limit = uint64(restarts[restart+1])
	}
	var previous []byte
	for position < limit {
		entry, next, shared, decodeErr := decodeDataEntry(payload, position, entriesEnd, previous)
		if decodeErr != nil || len(previous) == 0 && shared != 0 {
			return decodedEntry{}, false, corrupt(ErrInvalidBlock, "invalid selected restart interval")
		}
		if storage.CompareInternal(entry.key, target) >= 0 {
			return entry, true, nil
		}
		previous = entry.encoded
		position = next
	}
	if restart+1 < len(restarts) {
		entry, _, shared, decodeErr := decodeDataEntry(payload, uint64(restarts[restart+1]), entriesEnd, nil)
		if decodeErr != nil || shared != 0 {
			return decodedEntry{}, false, corrupt(ErrInvalidBlock, "invalid following restart entry")
		}
		if storage.CompareInternal(entry.key, target) >= 0 {
			return entry, true, nil
		}
	}
	return decodedEntry{}, false, nil
}

func decodeMetadata(payload []byte) (decodedMetadata, error) {
	if len(payload) < 28 {
		return decodedMetadata{}, corrupt(ErrInvalidBlock, "short metadata")
	}
	metadata := decodedMetadata{
		entryCount:       binary.LittleEndian.Uint64(payload),
		deletionCount:    binary.LittleEndian.Uint64(payload[8:]),
		dataBlockCount:   binary.LittleEndian.Uint32(payload[16:]),
		rawKeyValueBytes: binary.LittleEndian.Uint64(payload[20:]),
	}
	remaining := payload[28:]
	fields := []struct {
		destination *[]byte
		maximum     uint32
	}{
		{&metadata.smallestInternal, MaxInternalKeySize},
		{&metadata.largestInternal, MaxInternalKeySize},
		{&metadata.smallestUser, MaxUserKeySize},
		{&metadata.largestUser, MaxUserKeySize},
	}
	for _, field := range fields {
		if len(remaining) < 4 {
			return decodedMetadata{}, corrupt(ErrInvalidBlock, "truncated metadata length")
		}
		length := binary.LittleEndian.Uint32(remaining)
		remaining = remaining[4:]
		if length > field.maximum || uint64(length) > uint64(len(remaining)) {
			return decodedMetadata{}, corrupt(ErrInvalidBlock, "metadata field length exceeds bounds")
		}
		*field.destination = bytes.Clone(remaining[:int(length)])
		remaining = remaining[int(length):]
	}
	if len(remaining) != 0 {
		return decodedMetadata{}, corrupt(ErrInvalidBlock, "metadata has trailing bytes")
	}
	if metadata.entryCount == 0 || metadata.deletionCount > metadata.entryCount || metadata.dataBlockCount == 0 {
		return decodedMetadata{}, corrupt(ErrInvalidBlock, "invalid metadata counts")
	}
	smallest, err := storage.DecodeInternalKey(metadata.smallestInternal)
	if err != nil {
		return decodedMetadata{}, corrupt(ErrInvalidBlock, "smallest internal metadata key: %v", err)
	}
	largest, err := storage.DecodeInternalKey(metadata.largestInternal)
	if err != nil || storage.CompareInternal(smallest, largest) > 0 || bytes.Compare(metadata.smallestUser, metadata.largestUser) > 0 {
		return decodedMetadata{}, corrupt(ErrInvalidBlock, "inconsistent metadata bounds")
	}
	return metadata, nil
}

func readCanonicalUvarint(data []byte) (uint64, int, error) {
	value, consumed := binary.Uvarint(data)
	if consumed <= 0 {
		return 0, 0, ErrInvalidBlock
	}
	var encoded [binary.MaxVarintLen64]byte
	canonical := binary.PutUvarint(encoded[:], value)
	if canonical != consumed || !bytes.Equal(encoded[:consumed], data[:consumed]) {
		return 0, 0, ErrInvalidBlock
	}
	return value, consumed, nil
}

func corrupt(class error, format string, arguments ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrCorruptTable, class, fmt.Sprintf(format, arguments...))
}
