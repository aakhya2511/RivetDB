package raft

import "errors"

var (
	ErrInvalidConfig      = errors.New("raft: invalid configuration")
	ErrInvalidMessage     = errors.New("raft: invalid message")
	ErrInvalidState       = errors.New("raft: invalid persistent state")
	ErrNotLeader          = errors.New("raft: not leader")
	ErrNotCommitted       = errors.New("raft: proposal not committed")
	ErrStopped            = errors.New("raft: stopped")
	ErrPersistence        = errors.New("raft: persistence failure")
	ErrApply              = errors.New("raft: state-machine apply failure")
	ErrCompacted          = errors.New("raft: index compacted")
	ErrUnavailable        = errors.New("raft: index unavailable")
	ErrCommittedConflict  = errors.New("raft: committed log conflict")
	ErrCorruptStore       = errors.New("raft: corrupt store")
	ErrUnsupportedVersion = errors.New("raft: unsupported store version")
	ErrResourceLimit      = errors.New("raft: resource limit exceeded")
	ErrInvariantCheck     = errors.New("raft: simulator invariant violation")
)
