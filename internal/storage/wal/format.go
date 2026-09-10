package wal

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"

	"github.com/rivetdb/rivetdb/internal/storage"
)

const (
	// BlockSize is the physical WAL block size.
	BlockSize = 32 << 10
	// HeaderSize is the physical fragment header size.
	HeaderSize = 11
	// MaxRecordSize bounds a reconstructed logical record.
	MaxRecordSize = storage.MaxRecordSize

	formatVersion byte = 0
)

type fragmentKind byte

const (
	fragmentFull fragmentKind = iota + 1
	fragmentFirst
	fragmentMiddle
	fragmentLast
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

func makeHeader(payload []byte, kind fragmentKind) [HeaderSize]byte {
	var header [HeaderSize]byte
	binary.LittleEndian.PutUint16(header[8:10], uint16(len(payload))) //nolint:gosec // one fragment is block-bounded
	header[10] = byte(kind) | formatVersion<<4
	headerCRC := crc32.Checksum(header[8:11], checksumTable)
	contentCRC := crc32.Update(headerCRC, checksumTable, payload)
	binary.LittleEndian.PutUint32(header[0:4], contentCRC)
	binary.LittleEndian.PutUint32(header[4:8], headerCRC)
	return header
}

func formatOffset(offset int64) string {
	return strconv.FormatInt(offset, 10)
}

func corruption(offset int64, cause error) error {
	return &CorruptionError{Offset: offset, Cause: cause}
}
