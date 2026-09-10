package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/rivetdb/rivetdb/internal/storage"
)

var (
	errInvalidTable       = errors.New("invalid SSTable")
	errTableChecksum      = errors.New("SSTable checksum mismatch")
	errUnsupportedTable   = errors.New("unsupported SSTable format")
	errInvalidBlockHandle = errors.New("invalid SSTable block handle")
)

type decodedTable struct {
	entries        []decodedEntry
	dataBlocks     []decodedDataBlock
	index          []decodedIndexEntry
	metadata       decodedMetadata
	indexHandle    BlockHandle
	metadataHandle BlockHandle
	filterHandle   BlockHandle
	dataEnd        uint64
}

type decodedEntry struct {
	key     storage.InternalKey
	encoded []byte
	value   []byte
}

type decodedDataBlock struct {
	handle   BlockHandle
	restarts []uint32
	entries  []decodedEntry
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

func validateTable(data []byte) (decodedTable, error) {
	if len(data) < FooterSize {
		return decodedTable{}, fmt.Errorf("%w: file too short", errInvalidTable)
	}
	footerStart := len(data) - FooterSize
	footer := data[footerStart:]
	if binary.LittleEndian.Uint64(footer[footerMagic:]) != fileMagic {
		return decodedTable{}, fmt.Errorf("%w: bad magic", errInvalidTable)
	}
	wantFooterCRC := binary.LittleEndian.Uint32(footer[footerChecksum:])
	footerCRC := crc32.New(checksumTable)
	_, _ = footerCRC.Write(footer[:footerChecksum])
	_, _ = footerCRC.Write(footer[footerMagic:])
	if footerCRC.Sum32() != wantFooterCRC {
		return decodedTable{}, fmt.Errorf("%w: footer", errTableChecksum)
	}
	if binary.LittleEndian.Uint32(footer[footerFormatVersion:]) != FormatVersion {
		return decodedTable{}, fmt.Errorf("%w: major version", errUnsupportedTable)
	}
	if binary.LittleEndian.Uint32(footer[footerReserved:]) != 0 {
		return decodedTable{}, fmt.Errorf("%w: nonzero reserved footer", errInvalidTable)
	}

	result := decodedTable{
		indexHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(footer[footerIndexOffset:]),
			Length: binary.LittleEndian.Uint64(footer[footerIndexLength:]),
		},
		metadataHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(footer[footerMetadataOffset:]),
			Length: binary.LittleEndian.Uint64(footer[footerMetadataLength:]),
		},
		filterHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(footer[footerFilterOffset:]),
			Length: binary.LittleEndian.Uint64(footer[footerFilterLength:]),
		},
		dataEnd: binary.LittleEndian.Uint64(footer[footerDataRegionEnd:]),
	}
	flags := binary.LittleEndian.Uint32(footer[footerFlags:])
	if flags&^footerFilterPresentFlag != 0 {
		return decodedTable{}, fmt.Errorf("%w: unknown footer flags", errUnsupportedTable)
	}
	if flags&footerFilterPresentFlag == 0 {
		if result.filterHandle != (BlockHandle{}) {
			return decodedTable{}, fmt.Errorf("%w: absent filter has a handle", errInvalidBlockHandle)
		}
	} else {
		return decodedTable{}, fmt.Errorf("%w: filter not supported in Phase 1D", errUnsupportedTable)
	}

	footerOffset := uint64(footerStart)
	if result.dataEnd != result.indexHandle.Offset ||
		!handleEndsAt(result.indexHandle, result.metadataHandle.Offset) ||
		!handleEndsAt(result.metadataHandle, footerOffset) ||
		result.indexHandle.Length < BlockTrailerSize || result.metadataHandle.Length < BlockTrailerSize {
		return decodedTable{}, fmt.Errorf("%w: noncontiguous structural handles", errInvalidBlockHandle)
	}

	indexPayload, err := validateBlock(data, result.indexHandle, blockIndex)
	if err != nil {
		return decodedTable{}, err
	}
	result.index, err = decodeIndex(indexPayload)
	if err != nil {
		return decodedTable{}, err
	}
	metadataPayload, err := validateBlock(data, result.metadataHandle, blockMetadata)
	if err != nil {
		return decodedTable{}, err
	}
	result.metadata, err = decodeMetadata(metadataPayload)
	if err != nil {
		return decodedTable{}, err
	}
	if uint32(len(result.index)) != result.metadata.dataBlockCount {
		return decodedTable{}, fmt.Errorf("%w: index/metadata block count", errInvalidTable)
	}

	nextOffset := uint64(0)
	for index, entry := range result.index {
		if entry.handle.Offset != nextOffset || !handleWithin(entry.handle, result.dataEnd) {
			return decodedTable{}, fmt.Errorf("%w: data block %d", errInvalidBlockHandle, index)
		}
		payload, blockErr := validateBlock(data, entry.handle, blockData)
		if blockErr != nil {
			return decodedTable{}, blockErr
		}
		block, blockErr := decodeDataBlock(payload, entry.handle)
		if blockErr != nil {
			return decodedTable{}, blockErr
		}
		if len(block.entries) == 0 || !bytes.Equal(block.entries[len(block.entries)-1].encoded, entry.encoded) {
			return decodedTable{}, fmt.Errorf("%w: index key does not equal block last key", errInvalidTable)
		}
		result.dataBlocks = append(result.dataBlocks, block)
		result.entries = append(result.entries, block.entries...)
		nextOffset = entry.handle.Offset + entry.handle.Length
	}
	if nextOffset != result.dataEnd || len(result.dataBlocks) == 0 {
		return decodedTable{}, fmt.Errorf("%w: data region coverage", errInvalidBlockHandle)
	}
	if err := validateDecodedMetadata(result); err != nil {
		return decodedTable{}, err
	}
	return result, nil
}

func validateBlock(data []byte, handle BlockHandle, expected blockKind) ([]byte, error) {
	if handle.Length < BlockTrailerSize || !handleWithin(handle, uint64(len(data)-FooterSize)) {
		return nil, fmt.Errorf("%w: block kind %d", errInvalidBlockHandle, expected)
	}
	end := handle.Offset + handle.Length
	block := data[int(handle.Offset):int(end)]
	payloadEnd := len(block) - BlockTrailerSize
	want := binary.LittleEndian.Uint32(block[payloadEnd+3:])
	if crc32.Checksum(block[:payloadEnd+3], checksumTable) != want {
		return nil, fmt.Errorf("%w: block kind %d", errTableChecksum, expected)
	}
	if block[payloadEnd] != compressionNone {
		return nil, fmt.Errorf("%w: compression %d", errUnsupportedTable, block[payloadEnd])
	}
	if block[payloadEnd+1] != byte(expected) {
		return nil, fmt.Errorf("%w: block kind %d", errInvalidTable, block[payloadEnd+1])
	}
	if block[payloadEnd+2] != blockVersion {
		return nil, fmt.Errorf("%w: block version %d", errUnsupportedTable, block[payloadEnd+2])
	}
	return block[:payloadEnd], nil
}

func decodeIndex(payload []byte) ([]decodedIndexEntry, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("%w: short index", errInvalidTable)
	}
	count := binary.LittleEndian.Uint32(payload)
	remaining := payload[4:]
	if uint64(count) > uint64(len(remaining))/17+1 {
		return nil, fmt.Errorf("%w: impossible index count", errInvalidTable)
	}
	entries := make([]decodedIndexEntry, 0, int(count))
	for range count {
		length, consumed, err := readCanonicalUvarint(remaining)
		if err != nil || length > MaxInternalKeySize {
			return nil, fmt.Errorf("%w: index key length", errInvalidTable)
		}
		remaining = remaining[consumed:]
		if length > uint64(len(remaining)) || uint64(len(remaining))-length < 16 {
			return nil, fmt.Errorf("%w: truncated index entry", errInvalidTable)
		}
		encoded := bytes.Clone(remaining[:int(length)])
		remaining = remaining[int(length):]
		key, decodeErr := storage.DecodeInternalKey(encoded)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: index internal key: %w", errInvalidTable, decodeErr)
		}
		handle := BlockHandle{Offset: binary.LittleEndian.Uint64(remaining), Length: binary.LittleEndian.Uint64(remaining[8:])}
		remaining = remaining[16:]
		if len(entries) != 0 && storage.CompareInternal(entries[len(entries)-1].key, key) >= 0 {
			return nil, fmt.Errorf("%w: unordered index", errInvalidTable)
		}
		entries = append(entries, decodedIndexEntry{key: key, encoded: encoded, handle: handle})
	}
	if len(remaining) != 0 || len(entries) == 0 {
		return nil, fmt.Errorf("%w: index trailing bytes or empty", errInvalidTable)
	}
	return entries, nil
}

func decodeDataBlock(payload []byte, handle BlockHandle) (decodedDataBlock, error) {
	if len(payload) < 8 || len(payload) > MaxDataBlockSize {
		return decodedDataBlock{}, fmt.Errorf("%w: short data payload", errInvalidTable)
	}
	restartCount := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if restartCount == 0 || uint64(restartCount) > uint64(len(payload)-4)/4 {
		return decodedDataBlock{}, fmt.Errorf("%w: invalid restart count", errInvalidTable)
	}
	restartBytes := uint64(restartCount) * 4
	entriesEnd := uint64(len(payload)-4) - restartBytes
	if entriesEnd == 0 {
		return decodedDataBlock{}, fmt.Errorf("%w: no encoded entries", errInvalidTable)
	}
	restartsRaw := payload[int(entriesEnd) : len(payload)-4]
	restarts := make([]uint32, restartCount)
	for index := range restarts {
		restarts[index] = binary.LittleEndian.Uint32(restartsRaw[index*4:])
		if uint64(restarts[index]) >= entriesEnd || index == 0 && restarts[index] != 0 || index > 0 && restarts[index] <= restarts[index-1] {
			return decodedDataBlock{}, fmt.Errorf("%w: invalid restart offset", errInvalidTable)
		}
	}
	restartSet := make(map[uint32]struct{}, len(restarts))
	for _, restart := range restarts {
		restartSet[restart] = struct{}{}
	}

	block := decodedDataBlock{handle: handle, restarts: restarts}
	position := uint64(0)
	encounteredRestarts := 0
	var previous []byte
	for position < entriesEnd {
		entryOffset := position
		shared, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)])
		if err != nil {
			return decodedDataBlock{}, fmt.Errorf("%w: shared length", errInvalidTable)
		}
		position += uint64(consumed)
		unshared, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)])
		if err != nil {
			return decodedDataBlock{}, fmt.Errorf("%w: unshared length", errInvalidTable)
		}
		position += uint64(consumed)
		valueLength, consumed, err := readCanonicalUvarint(payload[int(position):int(entriesEnd)])
		if err != nil {
			return decodedDataBlock{}, fmt.Errorf("%w: value length", errInvalidTable)
		}
		position += uint64(consumed)
		if shared > uint64(len(previous)) || unshared > MaxInternalKeySize || valueLength > MaxValueSize {
			return decodedDataBlock{}, fmt.Errorf("%w: entry length limits", errInvalidTable)
		}
		needed, ok := checkedAdd(unshared, valueLength)
		if !ok || needed > entriesEnd-position {
			return decodedDataBlock{}, fmt.Errorf("%w: truncated entry", errInvalidTable)
		}
		_, isRestart := restartSet[uint32(entryOffset)] //nolint:gosec // block offsets are u32-bounded
		if isRestart {
			encounteredRestarts++
		}
		if isRestart && shared != 0 {
			return decodedDataBlock{}, fmt.Errorf("%w: restart/shared mismatch", errInvalidTable)
		}
		keyLength, ok := checkedAdd(shared, unshared)
		if !ok || keyLength > MaxInternalKeySize {
			return decodedDataBlock{}, fmt.Errorf("%w: reconstructed key length", errInvalidTable)
		}
		key := make([]byte, int(keyLength))
		copy(key, previous[:int(shared)])
		copy(key[int(shared):], payload[int(position):int(position+unshared)])
		position += unshared
		value := bytes.Clone(payload[int(position):int(position+valueLength)])
		position += valueLength
		internalKey, decodeErr := storage.DecodeInternalKey(key)
		if decodeErr != nil {
			return decodedDataBlock{}, fmt.Errorf("%w: internal key: %w", errInvalidTable, decodeErr)
		}
		if len(block.entries) != 0 && storage.CompareInternal(block.entries[len(block.entries)-1].key, internalKey) >= 0 {
			return decodedDataBlock{}, fmt.Errorf("%w: data entries not strictly ordered", errInvalidTable)
		}
		block.entries = append(block.entries, decodedEntry{key: internalKey, encoded: key, value: value})
		previous = key
	}
	if position != entriesEnd || len(block.entries) == 0 || encounteredRestarts != len(restarts) {
		return decodedDataBlock{}, fmt.Errorf("%w: data payload boundary", errInvalidTable)
	}
	return block, nil
}

func decodeMetadata(payload []byte) (decodedMetadata, error) {
	if len(payload) < 28 {
		return decodedMetadata{}, fmt.Errorf("%w: short metadata", errInvalidTable)
	}
	metadata := decodedMetadata{
		entryCount:       binary.LittleEndian.Uint64(payload),
		deletionCount:    binary.LittleEndian.Uint64(payload[8:]),
		dataBlockCount:   binary.LittleEndian.Uint32(payload[16:]),
		rawKeyValueBytes: binary.LittleEndian.Uint64(payload[20:]),
	}
	remaining := payload[28:]
	fields := []*[]byte{&metadata.smallestInternal, &metadata.largestInternal, &metadata.smallestUser, &metadata.largestUser}
	for _, field := range fields {
		if len(remaining) < 4 {
			return decodedMetadata{}, fmt.Errorf("%w: truncated metadata length", errInvalidTable)
		}
		length := binary.LittleEndian.Uint32(remaining)
		remaining = remaining[4:]
		if uint64(length) > uint64(len(remaining)) || length > MaxInternalKeySize {
			return decodedMetadata{}, fmt.Errorf("%w: metadata field length", errInvalidTable)
		}
		*field = bytes.Clone(remaining[:int(length)])
		remaining = remaining[int(length):]
	}
	if len(remaining) != 0 {
		return decodedMetadata{}, fmt.Errorf("%w: metadata trailing bytes", errInvalidTable)
	}
	return metadata, nil
}

func validateDecodedMetadata(table decodedTable) error {
	metadata := table.metadata
	if metadata.entryCount != uint64(len(table.entries)) || metadata.deletionCount > metadata.entryCount || metadata.dataBlockCount != uint32(len(table.dataBlocks)) {
		return fmt.Errorf("%w: metadata counts", errInvalidTable)
	}
	var deletions, raw uint64
	for index, entry := range table.entries {
		if index != 0 && storage.CompareInternal(table.entries[index-1].key, entry.key) >= 0 {
			return fmt.Errorf("%w: cross-block entry order", errInvalidTable)
		}
		if entry.key.Kind() == storage.KindDelete {
			deletions++
		}
		raw += uint64(len(entry.encoded) + len(entry.value))
	}
	first := table.entries[0]
	last := table.entries[len(table.entries)-1]
	if deletions != metadata.deletionCount || raw != metadata.rawKeyValueBytes ||
		!bytes.Equal(metadata.smallestInternal, first.encoded) || !bytes.Equal(metadata.largestInternal, last.encoded) ||
		!bytes.Equal(metadata.smallestUser, first.key.UserKey()) || !bytes.Equal(metadata.largestUser, last.key.UserKey()) {
		return fmt.Errorf("%w: metadata content", errInvalidTable)
	}
	return nil
}

func handleEndsAt(handle BlockHandle, expected uint64) bool {
	end, ok := checkedAdd(handle.Offset, handle.Length)
	return ok && end == expected
}

func handleWithin(handle BlockHandle, limit uint64) bool {
	if handle.Length == 0 || handle.Offset > limit {
		return false
	}
	end, ok := checkedAdd(handle.Offset, handle.Length)
	return ok && end <= limit
}

func readCanonicalUvarint(data []byte) (uint64, int, error) {
	value, consumed := binary.Uvarint(data)
	if consumed <= 0 {
		return 0, 0, errInvalidTable
	}
	var encoded [binary.MaxVarintLen64]byte
	if binary.PutUvarint(encoded[:], value) != consumed || !bytes.Equal(encoded[:consumed], data[:consumed]) {
		return 0, 0, errInvalidTable
	}
	return value, consumed, nil
}

func recomputeFooterChecksum(data []byte) {
	footer := data[len(data)-FooterSize:]
	checksum := crc32.New(checksumTable)
	_, _ = checksum.Write(footer[:footerChecksum])
	_, _ = checksum.Write(footer[footerMagic:])
	binary.LittleEndian.PutUint32(footer[footerChecksum:], checksum.Sum32())
}

func recomputeBlockChecksum(data []byte, handle BlockHandle) {
	block := data[int(handle.Offset):int(handle.Offset+handle.Length)]
	payloadEnd := len(block) - BlockTrailerSize
	binary.LittleEndian.PutUint32(block[payloadEnd+3:], crc32.Checksum(block[:payloadEnd+3], checksumTable))
}
