// Package wal implements RivetDB's bounded, checksummed write-ahead log.
package wal

import "errors"

var (
	// ErrCorruptWAL classifies bytes that cannot be a valid WAL prefix.
	ErrCorruptWAL = errors.New("corrupt WAL")
	// ErrHeaderChecksumMismatch means a fragment's length/type authentication failed.
	ErrHeaderChecksumMismatch = errors.New("WAL fragment header checksum mismatch")
	// ErrChecksumMismatch means a fragment's content checksum failed.
	ErrChecksumMismatch = errors.New("WAL fragment content checksum mismatch")
	// ErrUnsupportedVersion means the fragment type names an unknown format version.
	ErrUnsupportedVersion = errors.New("unsupported WAL format version")
	// ErrInvalidFragmentType means the fragment kind is unknown or out of sequence.
	ErrInvalidFragmentType = errors.New("invalid WAL fragment type")
	// ErrInvalidLength means a physical fragment length exceeds its block.
	ErrInvalidLength = errors.New("invalid WAL fragment length")
	// ErrInvalidPadding means bytes reserved for block-tail padding are nonzero.
	ErrInvalidPadding = errors.New("invalid WAL block padding")
	// ErrRecordTooLarge means a logical record exceeds storage.MaxRecordSize.
	ErrRecordTooLarge = errors.New("WAL record exceeds maximum size")
	// ErrTruncatedTail means a writer was opened on an unrepaired incomplete tail.
	ErrTruncatedTail = errors.New("WAL has a truncated tail")
	// ErrNoTruncatedTail means repair was requested without a recovery tail token.
	ErrNoTruncatedTail = errors.New("WAL has no truncated tail to repair")
	// ErrRecoveryStateChanged means the file no longer matches the recovery result.
	ErrRecoveryStateChanged = errors.New("WAL changed since recovery")
	// ErrClosedWriter means an operation was attempted after Writer.Close.
	ErrClosedWriter = errors.New("WAL writer is closed")
	// ErrWriterFailed means an earlier or current I/O error poisoned the writer.
	ErrWriterFailed = errors.New("WAL writer is failed")
	// ErrInvalidDurability means WriterOptions contains an unknown durability mode.
	ErrInvalidDurability = errors.New("invalid WAL durability mode")
	// ErrInvalidReader means NewReader was given no ReaderAt.
	ErrInvalidReader = errors.New("invalid nil WAL reader")
	// ErrInvalidSize means NewReader was given a negative size.
	ErrInvalidSize = errors.New("invalid negative WAL size")
)

// CorruptionError identifies the physical offset and specific reason recovery
// can no longer prove a valid WAL history.
type CorruptionError struct {
	Offset int64
	Cause  error
}

func (e *CorruptionError) Error() string {
	return "corrupt WAL at offset " + formatOffset(e.Offset) + ": " + e.Cause.Error()
}

// Unwrap makes both ErrCorruptWAL and the specific cause discoverable.
func (e *CorruptionError) Unwrap() error {
	return errors.Join(ErrCorruptWAL, e.Cause)
}
