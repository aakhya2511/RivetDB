// Package compaction implements version-preserving structural LSM compaction.
package compaction

import "errors"

var (
	ErrInvalidOptions       = errors.New("invalid compaction options")
	ErrNoCompaction         = errors.New("no compaction selected")
	ErrInvalidPlan          = errors.New("invalid compaction plan")
	ErrStalePlan            = errors.New("stale compaction plan")
	ErrInputCorrupt         = errors.New("compaction input corrupt or mismatched")
	ErrDuplicateInternalKey = errors.New("exact duplicate internal key cannot be represented without loss")
	ErrClosed               = errors.New("compaction iterator closed")
	ErrInvalidTransition    = errors.New("invalid compaction state transition")
)
