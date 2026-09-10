package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Record is one complete logical WAL record and its physical position.
type Record struct {
	Payload []byte
	Position
	Number uint64
}

// Tail describes a structurally incomplete final logical record or padding.
type Tail struct {
	Start int64
	End   int64
}

// Reader scans logical records without loading the whole WAL into memory.
type Reader struct {
	reader     io.ReaderAt
	size       int64
	closer     io.Closer
	offset     int64
	validEnd   int64
	record     Record
	number     uint64
	assembly   []byte
	assembling bool
	recordAt   int64
	tail       *Tail
	err        error
	done       bool
	closed     bool
	closeErr   error
}

// NewReader constructs a bounded scanner over exactly size bytes.
func NewReader(reader io.ReaderAt, size int64) (*Reader, error) {
	return newReader(reader, size, nil)
}

func newReader(reader io.ReaderAt, size int64, closer io.Closer) (*Reader, error) {
	if reader == nil {
		return nil, ErrInvalidReader
	}
	if size < 0 {
		return nil, ErrInvalidSize
	}
	return &Reader{reader: reader, size: size, closer: closer}, nil
}

// OpenReader opens path for sequential logical-record scanning.
func OpenReader(path string) (*Reader, error) {
	file, err := os.Open(path) //nolint:gosec // caller chooses database path
	if err != nil {
		return nil, fmt.Errorf("open WAL reader %s: %w", path, err)
	}
	stat, err := file.Stat()
	if err != nil {
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("stat WAL %s: %w", path, err), closeErr)
	}
	reader, err := newReader(file, stat.Size(), file)
	if err != nil {
		closeErr := file.Close()
		return nil, errors.Join(fmt.Errorf("construct WAL reader %s: %w", path, err), closeErr)
	}
	return reader, nil
}

// Next advances to the next complete logical record.
func (r *Reader) Next() bool {
	if r.done || r.err != nil || r.closed {
		return false
	}

	for {
		if r.offset == r.size {
			if r.assembling {
				r.markTail(r.recordAt)
			} else {
				r.done = true
			}
			return false
		}

		blockRemaining := int64(BlockSize) - r.offset%BlockSize
		if blockRemaining < HeaderSize {
			if !r.consumePadding(blockRemaining) {
				return false
			}
			continue
		}
		if r.size-r.offset < HeaderSize {
			r.markTail(r.incompleteStart())
			return false
		}

		fragmentAt := r.offset
		var header [HeaderSize]byte
		if err := r.readFullAt(header[:], r.offset); err != nil {
			r.err = err
			return false
		}
		storedHeaderCRC := binary.LittleEndian.Uint32(header[4:8])
		actualHeaderCRC := crc32.Checksum(header[8:11], checksumTable)
		if storedHeaderCRC != actualHeaderCRC {
			r.err = corruption(fragmentAt, ErrHeaderChecksumMismatch)
			return false
		}

		encodedType := header[10]
		if encodedType>>4 != formatVersion {
			r.err = corruption(fragmentAt, ErrUnsupportedVersion)
			return false
		}
		kind := fragmentKind(encodedType & 0x0f)
		if kind < fragmentFull || kind > fragmentLast {
			r.err = corruption(fragmentAt, ErrInvalidFragmentType)
			return false
		}
		length := int64(binary.LittleEndian.Uint16(header[8:10]))
		if length > blockRemaining-HeaderSize {
			r.err = corruption(fragmentAt, ErrInvalidLength)
			return false
		}
		payloadAt := r.offset + HeaderSize
		if r.size-payloadAt < length {
			r.markTail(r.incompleteStart())
			return false
		}
		payload := make([]byte, int(length))
		if err := r.readFullAt(payload, payloadAt); err != nil {
			r.err = err
			return false
		}
		storedContentCRC := binary.LittleEndian.Uint32(header[0:4])
		actualContentCRC := crc32.Update(actualHeaderCRC, checksumTable, payload)
		if storedContentCRC != actualContentCRC {
			r.err = corruption(fragmentAt, ErrChecksumMismatch)
			return false
		}
		r.offset = payloadAt + length

		if r.acceptFragment(kind, payload, fragmentAt) {
			return true
		}
		if r.err != nil {
			return false
		}
	}
}

// Record returns the record selected by the most recent successful Next.
func (r *Reader) Record() Record {
	return r.record
}

// Err returns the first corruption or I/O error. Truncated tails are reported
// separately by Tail and are not errors.
func (r *Reader) Err() error {
	return r.err
}

// Tail returns a structurally incomplete tail, if one was found.
func (r *Reader) Tail() (Tail, bool) {
	if r.tail == nil {
		return Tail{}, false
	}
	return *r.tail, true
}

// ValidEnd is the exclusive end offset of the last complete logical record.
func (r *Reader) ValidEnd() int64 {
	return r.validEnd
}

// Size is the byte size fixed when the Reader was constructed.
func (r *Reader) Size() int64 {
	return r.size
}

// Close closes an OpenReader. It is idempotent. Readers created with NewReader
// do not own their ReaderAt and Close is a no-op.
func (r *Reader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	if r.closer != nil {
		if err := r.closer.Close(); err != nil {
			r.closeErr = fmt.Errorf("close WAL reader: %w", err)
		}
	}
	return r.closeErr
}

func (r *Reader) consumePadding(blockRemaining int64) bool {
	available := min(blockRemaining, r.size-r.offset)
	padding := make([]byte, int(available))
	if err := r.readFullAt(padding, r.offset); err != nil {
		r.err = err
		return false
	}
	if !bytes.Equal(padding, make([]byte, len(padding))) {
		r.err = corruption(r.offset, ErrInvalidPadding)
		return false
	}
	r.offset += available
	if available < blockRemaining {
		r.markTail(r.incompleteStart())
		return false
	}
	return true
}

func (r *Reader) acceptFragment(kind fragmentKind, payload []byte, fragmentAt int64) bool {
	switch kind {
	case fragmentFull:
		if r.assembling {
			r.err = corruption(fragmentAt, ErrInvalidFragmentType)
			return false
		}
		return r.finishRecord(payload, fragmentAt)
	case fragmentFirst:
		if r.assembling {
			r.err = corruption(fragmentAt, ErrInvalidFragmentType)
			return false
		}
		r.assembly = append([]byte(nil), payload...)
		r.assembling = true
		r.recordAt = fragmentAt
	case fragmentMiddle:
		if !r.assembling {
			r.err = corruption(fragmentAt, ErrInvalidFragmentType)
			return false
		}
		if !r.appendFragment(payload, fragmentAt) {
			return false
		}
	case fragmentLast:
		if !r.assembling {
			r.err = corruption(fragmentAt, ErrInvalidFragmentType)
			return false
		}
		if !r.appendFragment(payload, fragmentAt) {
			return false
		}
		complete := r.assembly
		start := r.recordAt
		r.assembly = nil
		r.assembling = false
		return r.finishRecord(complete, start)
	}
	return false
}

func (r *Reader) appendFragment(payload []byte, fragmentAt int64) bool {
	if len(payload) > MaxRecordSize-len(r.assembly) {
		r.err = corruption(fragmentAt, ErrRecordTooLarge)
		return false
	}
	r.assembly = append(r.assembly, payload...)
	return true
}

func (r *Reader) finishRecord(payload []byte, start int64) bool {
	if len(payload) > MaxRecordSize {
		r.err = corruption(start, ErrRecordTooLarge)
		return false
	}
	r.record = Record{
		Payload:  payload,
		Position: Position{Start: start, End: r.offset},
		Number:   r.number,
	}
	r.number++
	r.validEnd = r.offset
	return true
}

func (r *Reader) incompleteStart() int64 {
	if r.assembling {
		return r.recordAt
	}
	return r.validEnd
}

func (r *Reader) markTail(start int64) {
	r.tail = &Tail{Start: start, End: r.size}
	r.done = true
}

func (r *Reader) readFullAt(data []byte, offset int64) error {
	if len(data) == 0 {
		return nil
	}
	n, err := r.reader.ReadAt(data, offset)
	if n == len(data) {
		return nil
	}
	if err == nil {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("read WAL at offset %d: got %d of %d bytes: %w", offset, n, len(data), err)
}
