// Package pipeline coordinates the Phase 1 WAL, MemTable rotation and SSTable
// flush lifecycle. It is not the final storage engine or manifest authority.
package pipeline

import "errors"

var (
	ErrInvalidOptions        = errors.New("invalid write pipeline options")
	ErrClosed                = errors.New("write pipeline is closed")
	ErrFlushFailed           = errors.New("write pipeline flush failed")
	ErrNoFailedFlush         = errors.New("write pipeline has no failed flush")
	ErrAmbiguousPublication  = errors.New("SSTable publication result is ambiguous")
	ErrInvalidTransition     = errors.New("invalid immutable flush state transition")
	ErrSequenceDiscontinuity = errors.New("WAL batch sequence is not contiguous")
	ErrGenerationExhausted   = errors.New("MemTable generation space exhausted")
	ErrFileNumberExhausted   = errors.New("SSTable file-number space exhausted")
	errFlushMissingEntries   = errors.New("published SSTable is missing entries")
	errFlushContentMismatch  = errors.New("published SSTable differs from immutable MemTable")
	errFlushExtraEntries     = errors.New("published SSTable has extra entries")
	errFlushCountMismatch    = errors.New("published SSTable entry count differs")
)
