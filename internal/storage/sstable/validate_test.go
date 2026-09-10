package sstable

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

var (
	errInvalidTable       = ErrCorruptTable
	errTableChecksum      = ErrChecksum
	errUnsupportedTable   = ErrUnsupportedVersion
	errInvalidBlockHandle = ErrInvalidHandle
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

type bytesReadAtCloser struct{ *bytes.Reader }

func (bytesReadAtCloser) Close() error { return nil }

// validateTable adapts the production reader to Phase 1D's byte-slice tests.
// Parsing and validation remain single-sourced in non-test code.
func validateTable(data []byte) (decodedTable, error) {
	file := bytesReadAtCloser{Reader: bytes.NewReader(data)}
	reader, err := openReader(file, uint64(len(data)), "memory.sst", ReaderOptions{})
	if err != nil {
		return decodedTable{}, err
	}
	footer := data[len(data)-FooterSize:]
	result := decodedTable{
		index: reader.index,
		metadata: decodedMetadata{
			entryCount: reader.metadata.EntryCount, deletionCount: reader.metadata.DeletionCount,
			dataBlockCount: reader.metadata.DataBlockCount, rawKeyValueBytes: reader.metadata.RawKeyValueBytes,
			smallestInternal: reader.metadata.SmallestInternal.Encode(), largestInternal: reader.metadata.LargestInternal.Encode(),
			smallestUser: reader.metadata.SmallestUser, largestUser: reader.metadata.LargestUser,
		},
		indexHandle:    BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerIndexOffset:]), Length: binary.LittleEndian.Uint64(footer[footerIndexLength:])},
		metadataHandle: BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerMetadataOffset:]), Length: binary.LittleEndian.Uint64(footer[footerMetadataLength:])},
		filterHandle:   BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerFilterOffset:]), Length: binary.LittleEndian.Uint64(footer[footerFilterLength:])},
		dataEnd:        binary.LittleEndian.Uint64(footer[footerDataRegionEnd:]),
	}
	for position := range reader.index {
		block, blockErr := reader.loadDataBlockUnlocked(position)
		if blockErr != nil {
			return decodedTable{}, blockErr
		}
		result.dataBlocks = append(result.dataBlocks, block)
		result.entries = append(result.entries, block.entries...)
	}
	return result, nil
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
