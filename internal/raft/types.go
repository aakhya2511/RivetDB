// Package raft implements RivetDB's deterministic single-group Raft core.
// It is deliberately independent of the Phase 1 storage Engine and data WAL.
package raft

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
)

const MaxCommandBytes = 16 << 20

type NodeID uint64

type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", r)
	}
}

type EntryType uint8

const (
	EntryNoOp EntryType = iota + 1
	EntryCommand
)

type Entry struct {
	Index   uint64
	Term    uint64
	Type    EntryType
	Command []byte
}

type HardState struct {
	Term     uint64
	VotedFor NodeID
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type PersistentState struct {
	HardState HardState
	Snapshot  Snapshot
	Entries   []Entry
}

type MessageType uint8

const (
	RequestVote MessageType = iota + 1
	RequestVoteResponse
	AppendEntries
	AppendEntriesResponse
	InstallSnapshot
	InstallSnapshotResponse
)

type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term uint64

	CandidateLastIndex uint64
	CandidateLastTerm  uint64
	VoteGranted        bool

	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	Success      bool
	MatchIndex   uint64
	RejectHint   uint64

	Snapshot Snapshot
}

type StateMachine interface {
	Apply(entry Entry) error
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

type Store interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

// Transport is the runtime boundary for outbound Raft messages. The core
// returns messages directly; a runtime sends them through this interface.
type Transport interface {
	Send(Message) error
}

type Config struct {
	ID                 NodeID
	Peers              []NodeID
	ElectionTimeoutMin uint64
	ElectionTimeoutMax uint64
	HeartbeatInterval  uint64
	Random             *rand.Rand
	Store              Store
	StateMachine       StateMachine
}

type Status struct {
	ID          NodeID
	Role        Role
	Term        uint64
	VotedFor    NodeID
	LeaderID    NodeID
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	LastTerm    uint64
	Snapshot    Snapshot
	NextIndex   map[NodeID]uint64
	MatchIndex  map[NodeID]uint64
	Fatal       error
}

func Quorum(nodes int) int {
	if nodes <= 0 {
		return 0
	}
	return nodes/2 + 1
}

func cloneEntry(entry Entry) Entry {
	entry.Command = bytes.Clone(entry.Command)
	return entry
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	for index := range entries {
		result[index] = cloneEntry(entries[index])
	}
	return result
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Data = bytes.Clone(snapshot.Data)
	return snapshot
}

func clonePersistent(state PersistentState) PersistentState {
	state.Snapshot = cloneSnapshot(state.Snapshot)
	state.Entries = cloneEntries(state.Entries)
	return state
}

func validEntryType(kind EntryType) bool {
	return kind == EntryNoOp || kind == EntryCommand
}

func validatePersistent(state PersistentState) error {
	if state.HardState.Term == 0 && state.HardState.VotedFor != 0 {
		return fmt.Errorf("%w: vote in term zero", ErrInvalidState)
	}
	if state.Snapshot.Index == 0 && state.Snapshot.Term != 0 || state.Snapshot.Index != 0 && state.Snapshot.Term == 0 {
		return fmt.Errorf("%w: invalid snapshot boundary", ErrInvalidState)
	}
	if len(state.Snapshot.Data) > MaxCommandBytes {
		return fmt.Errorf("%w: snapshot exceeds resource limit", ErrInvalidState)
	}
	if state.Snapshot.Index > math.MaxUint64-uint64(len(state.Entries)) { //nolint:gosec // nonnegative slice length
		return fmt.Errorf("%w: log index overflow", ErrInvalidState)
	}
	lastTerm := state.Snapshot.Term
	for offset, entry := range state.Entries {
		want := state.Snapshot.Index + uint64(offset) + 1 //nolint:gosec // bounded by slice memory
		if entry.Index != want || entry.Term == 0 || entry.Term < lastTerm || entry.Term > state.HardState.Term || !validEntryType(entry.Type) || len(entry.Command) > MaxCommandBytes {
			return fmt.Errorf("%w: entry offset %d index=%d term=%d type=%d", ErrInvalidState, offset, entry.Index, entry.Term, entry.Type)
		}
		if entry.Type == EntryNoOp && len(entry.Command) != 0 {
			return fmt.Errorf("%w: no-op %d has command bytes", ErrInvalidState, entry.Index)
		}
		lastTerm = entry.Term
	}
	if state.HardState.Term < lastTerm || state.HardState.Term < state.Snapshot.Term {
		return fmt.Errorf("%w: hard term %d behind log term %d", ErrInvalidState, state.HardState.Term, lastTerm)
	}
	return nil
}

func validateConfig(config Config) error {
	if config.ID == 0 || config.Store == nil || config.StateMachine == nil || config.Random == nil ||
		config.ElectionTimeoutMin == 0 || config.ElectionTimeoutMax < config.ElectionTimeoutMin ||
		config.HeartbeatInterval == 0 || config.HeartbeatInterval >= config.ElectionTimeoutMin || len(config.Peers) == 0 {
		return ErrInvalidConfig
	}
	peers := slices.Clone(config.Peers)
	slices.Sort(peers)
	if peers[0] == 0 || !slices.Contains(peers, config.ID) {
		return ErrInvalidConfig
	}
	for index := 1; index < len(peers); index++ {
		if peers[index] == peers[index-1] {
			return ErrInvalidConfig
		}
	}
	return nil
}
