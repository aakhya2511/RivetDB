package storage

import "errors"

var (
	// ErrInvalidBatch means bytes do not encode one complete canonical write batch.
	ErrInvalidBatch = errors.New("invalid write batch")
	// ErrEmptyBatch means a batch has no mutations.
	ErrEmptyBatch = errors.New("write batch is empty")
	// ErrBatchTooLarge means a batch exceeds MaxRecordSize.
	ErrBatchTooLarge = errors.New("write batch exceeds maximum WAL record size")
	// ErrSequenceOverflow means a contiguous batch sequence range wraps uint64.
	ErrSequenceOverflow   = errors.New("write batch sequence range overflows")
	errTruncatedLength    = errors.New("truncated length")
	errOverflowLength     = errors.New("overflowing length")
	errOutOfBoundsLength  = errors.New("declared length exceeds remaining bytes")
	errNonCanonicalLength = errors.New("non-canonical length")
)
