// Package memtable implements RivetDB's mutable ordered write buffer.
package memtable

import "errors"

var (
	// ErrFrozen means an insert was attempted after the MemTable became
	// immutable.
	ErrFrozen = errors.New("memtable is frozen")
	// ErrDeleteHasValue means a tombstone was supplied with value bytes.
	ErrDeleteHasValue = errors.New("memtable deletion has a value")
	// ErrInvalidRange means a range's lower user-key bound is greater than its
	// upper user-key bound.
	ErrInvalidRange = errors.New("invalid memtable user-key range")
)
