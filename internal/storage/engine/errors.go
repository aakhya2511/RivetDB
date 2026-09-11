// Package engine integrates RivetDB's local WAL, MemTables, SSTables,
// Manifest, VersionSet, flush pipeline and compaction executor.
package engine

import "errors"

var (
	ErrClosed            = errors.New("storage engine is closed")
	ErrNotFound          = errors.New("key not found")
	ErrInvalidOptions    = errors.New("invalid engine options")
	ErrInvalidRange      = errors.New("invalid scan range")
	ErrCorruption        = errors.New("authoritative storage state is corrupt")
	ErrDuplicateEntry    = errors.New("duplicate internal entry across authoritative sources")
	ErrDirectoryState    = errors.New("database directory has files but no CURRENT authority")
	ErrWrongMode         = errors.New("storage operation is forbidden in this engine mode")
	ErrUnexpectedWAL     = errors.New("replicated engine directory contains a standalone data WAL")
	ErrAlreadyApplied    = errors.New("replicated command is already durably applied")
	ErrConflictingApply  = errors.New("replicated index has conflicting command identity")
	ErrApplyOrder        = errors.New("replicated apply index did not advance")
	errActiveState       = errors.New("active MemTable state is invalid")
	errImmutableState    = errors.New("immutable MemTable state is invalid")
	errLevelOverlap      = errors.New("higher-level table ranges overlap")
	errSequenceBehind    = errors.New("sequence authority trails live table")
	errObsoleteStillLive = errors.New("obsolete table is still live")
)
