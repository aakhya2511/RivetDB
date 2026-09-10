package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

const (
	// FormatVersion is the SSTable major format written by this package.
	FormatVersion uint32 = 1
	// FooterSize is the fixed number of bytes discoverable at EOF.
	FooterSize = 80
	// BlockTrailerSize covers compression, kind, block version and CRC32C.
	BlockTrailerSize = 7
	// DefaultBlockSize is the target data-block payload size.
	DefaultBlockSize = 4 << 10
	// DefaultRestartInterval writes one full key every sixteen entries.
	DefaultRestartInterval = 16

	MaxUserKeySize     = 1 << 20
	MaxInternalKeySize = MaxUserKeySize + 9
	MaxValueSize       = 64 << 20
	MaxEntrySize       = MaxInternalKeySize + MaxValueSize + 30
	MaxDataBlockSize   = MaxEntrySize + 8
	MaxTableSize       = uint64(math.MaxInt64)
	MaxFileNumber      = 999_999_999_999

	compressionNone byte = 0
	blockVersion    byte = 1

	footerIndexOffset       = 0
	footerIndexLength       = 8
	footerMetadataOffset    = 16
	footerMetadataLength    = 24
	footerFilterOffset      = 32
	footerFilterLength      = 40
	footerDataRegionEnd     = 48
	footerFormatVersion     = 56
	footerFlags             = 60
	footerReserved          = 64
	footerChecksum          = 68
	footerMagic             = 72
	footerFilterPresentFlag = uint32(1)

	fileMagic uint64 = 0x5453535445564952 // little-endian bytes spell "RIVETSST"
)

type blockKind byte

const (
	blockData blockKind = iota + 1
	blockIndex
	blockMetadata
	blockFilter
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

// BlockHandle names one complete block, including its common trailer.
type BlockHandle struct {
	Offset uint64
	Length uint64
}

func appendBlockTrailer(payload []byte, kind blockKind) []byte {
	result := make([]byte, 0, len(payload)+BlockTrailerSize)
	result = append(result, payload...)
	result = append(result, compressionNone, byte(kind), blockVersion)
	checksum := crc32.Checksum(result, checksumTable)
	result = binary.LittleEndian.AppendUint32(result, checksum)
	return result
}

func encodeFooter(index, metadata, filter BlockHandle, dataEnd uint64) [FooterSize]byte {
	var footer [FooterSize]byte
	binary.LittleEndian.PutUint64(footer[footerIndexOffset:], index.Offset)
	binary.LittleEndian.PutUint64(footer[footerIndexLength:], index.Length)
	binary.LittleEndian.PutUint64(footer[footerMetadataOffset:], metadata.Offset)
	binary.LittleEndian.PutUint64(footer[footerMetadataLength:], metadata.Length)
	binary.LittleEndian.PutUint64(footer[footerFilterOffset:], filter.Offset)
	binary.LittleEndian.PutUint64(footer[footerFilterLength:], filter.Length)
	binary.LittleEndian.PutUint64(footer[footerDataRegionEnd:], dataEnd)
	binary.LittleEndian.PutUint32(footer[footerFormatVersion:], FormatVersion)
	if filter.Length != 0 {
		binary.LittleEndian.PutUint32(footer[footerFlags:], footerFilterPresentFlag)
	}
	binary.LittleEndian.PutUint32(footer[footerReserved:], 0)
	binary.LittleEndian.PutUint64(footer[footerMagic:], fileMagic)
	checksum := crc32.New(checksumTable)
	_, _ = checksum.Write(footer[:footerChecksum])
	_, _ = checksum.Write(footer[footerMagic:])
	binary.LittleEndian.PutUint32(footer[footerChecksum:], checksum.Sum32())
	return footer
}
