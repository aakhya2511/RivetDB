package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/rivetdb/rivetdb/internal/invariant"
)

type Node struct {
	id                NodeID
	peers             []NodeID
	quorum            int
	electionMin       uint64
	electionMax       uint64
	heartbeatInterval uint64
	random            interface{ Uint64N(uint64) uint64 }
	store             Store
	stateMachine      StateMachine
	persistent        PersistentState
	role              Role
	leaderID          NodeID
	commitIndex       uint64
	lastApplied       uint64
	electionElapsed   uint64
	electionTimeout   uint64
	heartbeatElapsed  uint64
	votes             map[NodeID]bool
	nextIndex         map[NodeID]uint64
	matchIndex        map[NodeID]uint64
	stopped           bool
	fatal             error
}

func NewNode(config Config) (*Node, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	state, err := config.Store.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft state: %w", err)
	}
	if err := validatePersistent(state); err != nil {
		return nil, err
	}
	if state.HardState.VotedFor != 0 && !slices.Contains(config.Peers, state.HardState.VotedFor) {
		return nil, ErrInvalidState
	}
	peers := slices.Clone(config.Peers)
	slices.Sort(peers)
	n := &Node{
		id:                config.ID,
		peers:             peers,
		quorum:            Quorum(len(peers)),
		electionMin:       config.ElectionTimeoutMin,
		electionMax:       config.ElectionTimeoutMax,
		heartbeatInterval: config.HeartbeatInterval,
		random:            config.Random,
		store:             config.Store,
		stateMachine:      config.StateMachine,
		persistent:        clonePersistent(state),
		role:              Follower,
		commitIndex:       state.Snapshot.Index,
		lastApplied:       state.Snapshot.Index,
	}
	if state.Snapshot.Index != 0 {
		if err := n.stateMachine.Restore(bytes.Clone(state.Snapshot.Data)); err != nil {
			return nil, fmt.Errorf("restore Raft snapshot: %w", errors.Join(ErrApply, err))
		}
	}
	n.resetElectionTimer()
	return n, nil
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) Status() Status {
	lastIndex, lastTerm := n.lastIndex(), n.lastTerm()
	return Status{
		ID: n.id, Role: n.role, Term: n.persistent.HardState.Term,
		VotedFor: n.persistent.HardState.VotedFor, LeaderID: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastIndex: lastIndex, LastTerm: lastTerm,
		Snapshot:  cloneSnapshot(n.persistent.Snapshot),
		NextIndex: cloneIndexMap(n.nextIndex), MatchIndex: cloneIndexMap(n.matchIndex),
		Fatal: n.fatal,
	}
}

func cloneIndexMap(source map[NodeID]uint64) map[NodeID]uint64 {
	if source == nil {
		return nil
	}
	result := make(map[NodeID]uint64, len(source))
	for id, value := range source {
		result[id] = value
	}
	return result
}

func (n *Node) Stop() { n.stopped = true }

// Tick advances this node by one logical tick.
func (n *Node) Tick() ([]Message, error) {
	if err := n.usable(); err != nil {
		return nil, err
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed < n.heartbeatInterval {
			return nil, nil
		}
		n.heartbeatElapsed = 0
		return n.replicationMessages(), nil
	}
	n.electionElapsed++
	if n.electionElapsed < n.electionTimeout {
		return nil, nil
	}
	return n.startElection()
}

// Propose durably admits one opaque command on a leader. The returned index is
// not client success: a runtime may report success only after it is committed
// and applied locally.
func (n *Node) Propose(command []byte) (uint64, []Message, error) {
	if err := n.usable(); err != nil {
		return 0, nil, err
	}
	if n.role != Leader {
		return 0, nil, ErrNotLeader
	}
	if len(command) > MaxCommandBytes {
		return 0, nil, ErrResourceLimit
	}
	if n.lastIndex() == math.MaxUint64 {
		return 0, nil, ErrResourceLimit
	}
	entry := Entry{Index: n.lastIndex() + 1, Term: n.term(), Type: EntryCommand, Command: bytes.Clone(command)}
	n.persistent.Entries = append(n.persistent.Entries, entry)
	if err := n.persist(); err != nil {
		return 0, nil, err
	}
	n.matchIndex[n.id] = entry.Index
	n.nextIndex[n.id] = entry.Index + 1
	if err := n.advanceCommit(); err != nil {
		return 0, nil, err
	}
	return entry.Index, n.replicationMessages(), nil
}

func (n *Node) Step(message Message) ([]Message, error) {
	if err := n.usable(); err != nil {
		return nil, err
	}
	if message.To != n.id || message.From == 0 || !slices.Contains(n.peers, message.From) || !validMessageType(message.Type) {
		return nil, ErrInvalidMessage
	}
	if message.Term > n.term() {
		if err := n.advanceTerm(message.Term); err != nil {
			return nil, err
		}
	}
	switch message.Type {
	case RequestVote:
		return n.handleRequestVote(message)
	case RequestVoteResponse:
		return n.handleVoteResponse(message)
	case AppendEntries:
		return n.handleAppendEntries(message)
	case AppendEntriesResponse:
		return n.handleAppendResponse(message)
	case InstallSnapshot:
		return n.handleInstallSnapshot(message)
	case InstallSnapshotResponse:
		return n.handleSnapshotResponse(message)
	default:
		return nil, ErrInvalidMessage
	}
}

func validMessageType(kind MessageType) bool {
	return kind >= RequestVote && kind <= InstallSnapshotResponse
}

func (n *Node) startElection() ([]Message, error) {
	n.role, n.leaderID = Candidate, 0
	if n.persistent.HardState.Term == math.MaxUint64 {
		return nil, n.fail(ErrResourceLimit)
	}
	n.persistent.HardState.Term++
	n.persistent.HardState.VotedFor = n.id
	n.votes = map[NodeID]bool{n.id: true}
	n.nextIndex, n.matchIndex = nil, nil
	n.resetElectionTimer()
	if err := n.persist(); err != nil {
		return nil, err
	}
	if n.quorum == 1 {
		return n.becomeLeader()
	}
	messages := make([]Message, 0, len(n.peers)-1)
	for _, peer := range n.peers {
		if peer == n.id {
			continue
		}
		messages = append(messages, Message{Type: RequestVote, From: n.id, To: peer, Term: n.term(), CandidateLastIndex: n.lastIndex(), CandidateLastTerm: n.lastTerm()})
	}
	return messages, nil
}

func (n *Node) becomeLeader() ([]Message, error) {
	if n.lastIndex() == math.MaxUint64 {
		return nil, n.fail(ErrResourceLimit)
	}
	n.role, n.leaderID = Leader, n.id
	n.votes = nil
	n.heartbeatElapsed = 0
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	next := n.lastIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = next
		n.matchIndex[peer] = n.persistent.Snapshot.Index
	}
	entry := Entry{Index: next, Term: n.term(), Type: EntryNoOp}
	n.persistent.Entries = append(n.persistent.Entries, entry)
	if err := n.persist(); err != nil {
		return nil, err
	}
	n.matchIndex[n.id], n.nextIndex[n.id] = entry.Index, entry.Index+1
	if err := n.advanceCommit(); err != nil {
		return nil, err
	}
	return n.replicationMessages(), nil
}

func (n *Node) advanceTerm(term uint64) error {
	invariant.Assert(term > n.term(), "RAFT-6", "term advancement %d <= %d", term, n.term())
	n.persistent.HardState.Term = term
	n.persistent.HardState.VotedFor = 0
	n.becomeFollower(0)
	return n.persist()
}

func (n *Node) becomeFollower(leader NodeID) {
	n.role, n.leaderID = Follower, leader
	n.votes, n.nextIndex, n.matchIndex = nil, nil, nil
	n.heartbeatElapsed = 0
	n.resetElectionTimer()
}

func (n *Node) handleRequestVote(message Message) ([]Message, error) {
	if message.CandidateLastTerm > message.Term {
		return nil, ErrInvalidMessage
	}
	granted := false
	if message.Term == n.term() && n.logUpToDate(message.CandidateLastIndex, message.CandidateLastTerm) &&
		(n.persistent.HardState.VotedFor == 0 || n.persistent.HardState.VotedFor == message.From) {
		granted = true
		if n.persistent.HardState.VotedFor != message.From {
			n.persistent.HardState.VotedFor = message.From
			if err := n.persist(); err != nil {
				return nil, err
			}
		}
		n.resetElectionTimer()
	}
	return []Message{{Type: RequestVoteResponse, From: n.id, To: message.From, Term: n.term(), VoteGranted: granted}}, nil
}

func (n *Node) handleVoteResponse(message Message) ([]Message, error) {
	if message.Term < n.term() || n.role != Candidate || message.Term != n.term() {
		return nil, nil
	}
	if message.VoteGranted {
		n.votes[message.From] = true
	}
	granted := 0
	for _, vote := range n.votes {
		if vote {
			granted++
		}
	}
	if granted >= n.quorum {
		return n.becomeLeader()
	}
	return nil, nil
}

func (n *Node) handleAppendEntries(message Message) ([]Message, error) {
	if message.Term < n.term() {
		return []Message{n.appendResponse(message.From, false, 0, n.lastIndex()+1)}, nil
	}
	if n.role == Leader && message.From != n.id {
		invariant.Failf("RAFT-1", "leaders %d and %d observed in term %d", n.id, message.From, n.term())
	}
	if n.role != Follower || n.leaderID != message.From {
		n.becomeFollower(message.From)
	} else {
		n.resetElectionTimer()
	}
	if err := validateAppend(message); err != nil {
		return nil, err
	}
	if message.PrevLogIndex < n.persistent.Snapshot.Index {
		hint := n.persistent.Snapshot.Index
		if hint < math.MaxUint64 {
			hint++
		}
		return []Message{n.appendResponse(message.From, false, 0, hint)}, nil
	}
	previousTerm, err := n.termAt(message.PrevLogIndex)
	if err != nil {
		if errors.Is(err, ErrUnavailable) {
			return []Message{n.appendResponse(message.From, false, 0, n.lastIndex()+1)}, nil
		}
		return nil, err
	}
	if previousTerm != message.PrevLogTerm {
		return []Message{n.appendResponse(message.From, false, 0, n.firstIndexOfTerm(message.PrevLogIndex, previousTerm))}, nil
	}
	changed := false
	for offset, incoming := range message.Entries {
		local, localErr := n.entryAt(incoming.Index)
		if localErr == nil {
			if local.Term == incoming.Term {
				invariant.Assert(local.Type == incoming.Type && bytes.Equal(local.Command, incoming.Command), "RAFT-3", "same index/term %d/%d has different entry", local.Index, local.Term)
				continue
			}
			if incoming.Index <= n.commitIndex {
				invariant.Failf("RAFT-4", "conflict at committed index %d <= %d", incoming.Index, n.commitIndex)
			}
			n.truncateSuffix(incoming.Index)
			n.persistent.Entries = append(n.persistent.Entries, cloneEntries(message.Entries[offset:])...)
			changed = true
			break
		}
		if !errors.Is(localErr, ErrUnavailable) {
			return nil, localErr
		}
		n.persistent.Entries = append(n.persistent.Entries, cloneEntries(message.Entries[offset:])...)
		changed = true
		break
	}
	if changed {
		if err := n.persist(); err != nil {
			return nil, err
		}
	}
	lastCovered := message.PrevLogIndex + uint64(len(message.Entries)) //nolint:gosec // validated contiguous message
	if message.LeaderCommit > n.commitIndex {
		n.commitIndex = min(message.LeaderCommit, lastCovered)
		if err := n.applyCommitted(); err != nil {
			return nil, fmt.Errorf("apply after AppendEntries from %d prev=%d/%d entries=%d covered=%d leader_commit=%d: %w", message.From, message.PrevLogIndex, message.PrevLogTerm, len(message.Entries), lastCovered, message.LeaderCommit, err)
		}
	}
	return []Message{n.appendResponse(message.From, true, lastCovered, 0)}, nil
}

func validateAppend(message Message) error {
	if uint64(len(message.Entries)) > math.MaxUint64-message.PrevLogIndex { //nolint:gosec // nonnegative slice length
		return ErrInvalidMessage
	}
	previousTerm := message.PrevLogTerm
	for offset, entry := range message.Entries {
		want := message.PrevLogIndex + uint64(offset) + 1 //nolint:gosec // message slice bounds
		if entry.Index != want || entry.Term == 0 || entry.Term < previousTerm || entry.Term > message.Term || !validEntryType(entry.Type) || len(entry.Command) > MaxCommandBytes || entry.Type == EntryNoOp && len(entry.Command) != 0 {
			return ErrInvalidMessage
		}
		previousTerm = entry.Term
	}
	return nil
}

func (n *Node) appendResponse(to NodeID, success bool, matchIndex, rejectHint uint64) Message {
	return Message{Type: AppendEntriesResponse, From: n.id, To: to, Term: n.term(), Success: success, MatchIndex: matchIndex, RejectHint: rejectHint}
}

func (n *Node) handleAppendResponse(message Message) ([]Message, error) {
	if message.Term < n.term() || n.role != Leader || message.Term != n.term() {
		return nil, nil
	}
	if message.Success {
		if message.MatchIndex > n.lastIndex() {
			return nil, ErrInvalidMessage
		}
		if message.MatchIndex > n.matchIndex[message.From] {
			n.matchIndex[message.From] = message.MatchIndex
			n.nextIndex[message.From] = message.MatchIndex + 1
		}
		before := n.commitIndex
		if err := n.advanceCommit(); err != nil {
			return nil, err
		}
		if n.commitIndex != before {
			return n.replicationMessages(), nil
		}
		return nil, nil
	}
	current := n.nextIndex[message.From]
	hint := message.RejectHint
	maximumNext := n.lastIndex()
	if maximumNext < math.MaxUint64 {
		maximumNext++
	}
	if hint > current && hint <= maximumNext {
		n.nextIndex[message.From] = hint
		return []Message{n.replicationMessage(message.From)}, nil
	}
	if hint == 0 || hint >= current {
		if current > 1 {
			hint = current - 1
		} else {
			hint = 1
		}
	}
	n.nextIndex[message.From] = hint
	return []Message{n.replicationMessage(message.From)}, nil
}

func (n *Node) advanceCommit() error {
	if n.role != Leader {
		return nil
	}
	for index := n.lastIndex(); index > n.commitIndex; index-- {
		term, err := n.termAt(index)
		if err != nil {
			return err
		}
		if term != n.term() {
			continue
		}
		matched := 0
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= index {
				matched++
			}
		}
		if matched >= n.quorum {
			n.commitIndex = index
			return n.applyCommitted()
		}
	}
	return nil
}

func (n *Node) applyCommitted() error {
	for n.lastApplied < n.commitIndex {
		index := n.lastApplied + 1
		entry, err := n.entryAt(index)
		if err != nil {
			return n.fail(errors.Join(ErrApply, err))
		}
		if entry.Type == EntryCommand {
			if err := n.stateMachine.Apply(cloneEntry(entry)); err != nil {
				return n.fail(fmt.Errorf("apply index %d: %w", index, errors.Join(ErrApply, err)))
			}
		}
		n.lastApplied = index
	}
	invariant.Assert(n.lastApplied <= n.commitIndex, "RAFT-7", "applied %d > commit %d", n.lastApplied, n.commitIndex)
	return nil
}

func (n *Node) replicationMessages() []Message {
	result := make([]Message, 0, len(n.peers)-1)
	for _, peer := range n.peers {
		if peer != n.id {
			result = append(result, n.replicationMessage(peer))
		}
	}
	return result
}

func (n *Node) replicationMessage(peer NodeID) Message {
	next := n.nextIndex[peer]
	if next <= n.persistent.Snapshot.Index {
		return Message{Type: InstallSnapshot, From: n.id, To: peer, Term: n.term(), Snapshot: cloneSnapshot(n.persistent.Snapshot), LeaderCommit: n.commitIndex}
	}
	previous := next - 1
	previousTerm, err := n.termAt(previous)
	invariant.Assert(err == nil, "RAFT-3", "leader missing prev index %d: %v", previous, err)
	entries, err := n.entriesFrom(next)
	invariant.Assert(err == nil, "RAFT-3", "leader missing suffix %d: %v", next, err)
	return Message{Type: AppendEntries, From: n.id, To: peer, Term: n.term(), PrevLogIndex: previous, PrevLogTerm: previousTerm, Entries: entries, LeaderCommit: n.commitIndex}
}

func (n *Node) handleInstallSnapshot(message Message) ([]Message, error) {
	if message.Term < n.term() {
		return []Message{{Type: InstallSnapshotResponse, From: n.id, To: message.From, Term: n.term(), Success: false, MatchIndex: n.persistent.Snapshot.Index}}, nil
	}
	if message.Snapshot.Index == 0 || message.Snapshot.Term == 0 || message.Snapshot.Term > message.Term || len(message.Snapshot.Data) > MaxCommandBytes {
		return nil, ErrInvalidMessage
	}
	if n.role == Leader && message.From != n.id {
		invariant.Failf("RAFT-1", "leaders %d and %d observed in term %d", n.id, message.From, n.term())
	}
	n.becomeFollower(message.From)
	if message.Snapshot.Index <= n.persistent.Snapshot.Index || message.Snapshot.Index <= n.commitIndex {
		covered := max(message.Snapshot.Index, n.persistent.Snapshot.Index)
		return []Message{{Type: InstallSnapshotResponse, From: n.id, To: message.From, Term: n.term(), Success: true, MatchIndex: min(covered, n.lastIndex())}}, nil
	}
	var suffix []Entry
	if term, err := n.termAt(message.Snapshot.Index); err == nil && term == message.Snapshot.Term {
		suffix, err = n.entriesFrom(message.Snapshot.Index + 1)
		if err != nil {
			return nil, err
		}
	}
	candidate := clonePersistent(n.persistent)
	candidate.Snapshot = cloneSnapshot(message.Snapshot)
	candidate.Entries = suffix
	if err := n.save(candidate); err != nil {
		return nil, err
	}
	if err := n.stateMachine.Restore(bytes.Clone(message.Snapshot.Data)); err != nil {
		return nil, n.fail(fmt.Errorf("restore installed snapshot: %w", errors.Join(ErrApply, err)))
	}
	n.persistent = candidate
	n.commitIndex = max(n.commitIndex, message.Snapshot.Index)
	n.lastApplied = message.Snapshot.Index
	// The matching snapshot boundary proves only the prefix through the
	// snapshot. A retained suffix remains unverified until the leader sends
	// AppendEntries; LeaderCommit alone must not authorize applying it.
	return []Message{{Type: InstallSnapshotResponse, From: n.id, To: message.From, Term: n.term(), Success: true, MatchIndex: message.Snapshot.Index}}, nil
}

func (n *Node) handleSnapshotResponse(message Message) ([]Message, error) {
	if message.Term < n.term() || n.role != Leader || message.Term != n.term() {
		return nil, nil
	}
	if !message.Success {
		return nil, nil
	}
	if message.MatchIndex > n.lastIndex() {
		return nil, ErrInvalidMessage
	}
	n.matchIndex[message.From] = max(n.matchIndex[message.From], message.MatchIndex)
	n.nextIndex[message.From] = n.matchIndex[message.From] + 1
	return []Message{n.replicationMessage(message.From)}, nil
}

func (n *Node) CreateSnapshot() (Snapshot, error) {
	if err := n.usable(); err != nil {
		return Snapshot{}, err
	}
	if n.lastApplied == 0 || n.lastApplied <= n.persistent.Snapshot.Index {
		return Snapshot{}, ErrUnavailable
	}
	term, err := n.termAt(n.lastApplied)
	if err != nil {
		return Snapshot{}, err
	}
	data, err := n.stateMachine.Snapshot()
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot state machine: %w", errors.Join(ErrApply, err))
	}
	snapshot := Snapshot{Index: n.lastApplied, Term: term, Data: bytes.Clone(data)}
	remaining, err := n.entriesFrom(snapshot.Index + 1)
	if err != nil {
		return Snapshot{}, err
	}
	candidate := clonePersistent(n.persistent)
	candidate.Snapshot, candidate.Entries = snapshot, remaining
	if err := n.save(candidate); err != nil {
		return Snapshot{}, err
	}
	n.persistent = candidate
	return cloneSnapshot(snapshot), nil
}

func (n *Node) term() uint64 { return n.persistent.HardState.Term }

func (n *Node) lastIndex() uint64 {
	if len(n.persistent.Entries) == 0 {
		return n.persistent.Snapshot.Index
	}
	return n.persistent.Entries[len(n.persistent.Entries)-1].Index
}

func (n *Node) lastTerm() uint64 {
	if len(n.persistent.Entries) == 0 {
		return n.persistent.Snapshot.Term
	}
	return n.persistent.Entries[len(n.persistent.Entries)-1].Term
}

func (n *Node) termAt(index uint64) (uint64, error) {
	if index == n.persistent.Snapshot.Index {
		return n.persistent.Snapshot.Term, nil
	}
	if index < n.persistent.Snapshot.Index {
		return 0, ErrCompacted
	}
	if index > n.lastIndex() {
		return 0, ErrUnavailable
	}
	return n.persistent.Entries[index-n.persistent.Snapshot.Index-1].Term, nil
}

func (n *Node) entryAt(index uint64) (Entry, error) {
	if index <= n.persistent.Snapshot.Index {
		return Entry{}, ErrCompacted
	}
	if index > n.lastIndex() {
		return Entry{}, ErrUnavailable
	}
	return cloneEntry(n.persistent.Entries[index-n.persistent.Snapshot.Index-1]), nil
}

func (n *Node) entriesFrom(index uint64) ([]Entry, error) {
	if index <= n.persistent.Snapshot.Index {
		return nil, ErrCompacted
	}
	if index > n.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if index == n.lastIndex()+1 {
		return []Entry{}, nil
	}
	return cloneEntries(n.persistent.Entries[index-n.persistent.Snapshot.Index-1:]), nil
}

func (n *Node) truncateSuffix(index uint64) {
	invariant.Assert(index > n.commitIndex, "RAFT-4", "truncate %d at/below commit %d", index, n.commitIndex)
	cut := index - n.persistent.Snapshot.Index - 1
	n.persistent.Entries = slices.Clone(n.persistent.Entries[:cut])
}

func (n *Node) firstIndexOfTerm(index, term uint64) uint64 {
	for index > n.persistent.Snapshot.Index {
		previous, err := n.termAt(index - 1)
		if err != nil || previous != term {
			break
		}
		index--
	}
	return index
}

func (n *Node) logUpToDate(index, term uint64) bool {
	return term > n.lastTerm() || term == n.lastTerm() && index >= n.lastIndex()
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	span := n.electionMax - n.electionMin + 1
	n.electionTimeout = n.electionMin
	if span > 1 {
		n.electionTimeout += n.random.Uint64N(span)
	}
}

func (n *Node) persistentCopy() PersistentState { return clonePersistent(n.persistent) }

func (n *Node) persist() error { return n.save(n.persistent) }

func (n *Node) save(state PersistentState) error {
	if err := n.store.Save(clonePersistent(state)); err != nil {
		return n.fail(fmt.Errorf("save Raft state: %w", errors.Join(ErrPersistence, err)))
	}
	return nil
}

func (n *Node) fail(err error) error {
	n.fatal = err
	n.stopped = true
	return err
}

func (n *Node) usable() error {
	if n.fatal != nil {
		return errors.Join(ErrStopped, n.fatal)
	}
	if n.stopped {
		return ErrStopped
	}
	return nil
}
