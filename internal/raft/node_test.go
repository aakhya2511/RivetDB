package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/rivetdb/rivetdb/internal/invariant"
)

type recordingStateMachine struct {
	commands [][]byte
	applyErr error
}

func (s *recordingStateMachine) Apply(entry Entry) error {
	if s.applyErr != nil {
		return s.applyErr
	}
	s.commands = append(s.commands, bytes.Clone(entry.Command))
	return nil
}

func (s *recordingStateMachine) Snapshot() ([]byte, error) {
	var result []byte
	for _, command := range s.commands {
		result = appendU32(result, uint32(len(command))) //nolint:gosec // test values are bounded
		result = append(result, command...)
	}
	return result, nil
}

func (s *recordingStateMachine) Restore(snapshot []byte) error {
	s.commands = nil
	decoder := stateDecoder{data: snapshot}
	for decoder.remaining() != 0 {
		command, ok := decoder.bytes(fileStoreMaxCommand)
		if !ok {
			return ErrInvalidState
		}
		s.commands = append(s.commands, command)
	}
	return nil
}

func (s *recordingStateMachine) digest() string {
	return fmt.Sprintf("%q", s.commands)
}

func testConfig(id NodeID, peers []NodeID, store Store, machine StateMachine) Config {
	return Config{
		ID: id, Peers: peers, ElectionTimeoutMin: 5, ElectionTimeoutMax: 9,
		HeartbeatInterval: 2, Random: rand.New(rand.NewPCG(uint64(id), uint64(id)+99)),
		Store: store, StateMachine: machine,
	}
}

func newTestNode(t *testing.T, id NodeID, peers []NodeID, store Store) (*Node, *recordingStateMachine) {
	t.Helper()
	machine := &recordingStateMachine{}
	node, err := NewNode(testConfig(id, peers, store, machine))
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node, machine
}

func tickUntilMessages(t *testing.T, node *Node, limit int) []Message {
	t.Helper()
	for range limit {
		messages, err := node.Tick()
		if err != nil {
			t.Fatalf("Tick: %v", err)
		}
		if len(messages) != 0 || node.Status().Role == Leader {
			return messages
		}
	}
	t.Fatal("node produced no messages")
	return nil
}

func TestQuorum(t *testing.T) {
	wants := []int{0, 1, 2, 2, 3, 3}
	for nodes, want := range wants {
		if got := Quorum(nodes); got != want {
			t.Fatalf("Quorum(%d)=%d want %d", nodes, got, want)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	valid := testConfig(1, []NodeID{1, 2, 3}, NewMemoryStore(), &recordingStateMachine{})
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "zero node", mutate: func(config *Config) { config.ID = 0 }},
		{name: "missing self", mutate: func(config *Config) { config.Peers = []NodeID{2, 3} }},
		{name: "duplicate peer", mutate: func(config *Config) { config.Peers = []NodeID{1, 2, 2} }},
		{name: "zero peer", mutate: func(config *Config) { config.Peers = []NodeID{0, 1} }},
		{name: "zero election", mutate: func(config *Config) { config.ElectionTimeoutMin = 0 }},
		{name: "reversed election", mutate: func(config *Config) { config.ElectionTimeoutMax = 4 }},
		{name: "zero heartbeat", mutate: func(config *Config) { config.HeartbeatInterval = 0 }},
		{name: "heartbeat not below election", mutate: func(config *Config) { config.HeartbeatInterval = 5 }},
		{name: "nil random", mutate: func(config *Config) { config.Random = nil }},
		{name: "nil store", mutate: func(config *Config) { config.Store = nil }},
		{name: "nil state machine", mutate: func(config *Config) { config.StateMachine = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			config.Peers = append([]NodeID(nil), valid.Peers...)
			test.mutate(&config)
			if _, err := NewNode(config); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("NewNode error=%v", err)
			}
		})
	}
}

func TestBootstrapAndSingleNodeElectionApply(t *testing.T) {
	store := NewMemoryStore()
	node, machine := newTestNode(t, 1, []NodeID{1}, store)
	status := node.Status()
	if status.Role != Follower || status.Term != 0 || status.LastIndex != 0 || status.CommitIndex != 0 {
		t.Fatalf("bootstrap status=%+v", status)
	}
	tickUntilMessages(t, node, 12)
	status = node.Status()
	if status.Role != Leader || status.Term != 1 || status.CommitIndex != 1 || status.LastApplied != 1 {
		t.Fatalf("leader status=%+v", status)
	}
	index, messages, err := node.Propose([]byte("set x=1"))
	if err != nil || index != 2 || len(messages) != 0 {
		t.Fatalf("Propose index=%d messages=%d err=%v", index, len(messages), err)
	}
	if node.Status().CommitIndex != 2 || machine.digest() != "[\"set x=1\"]" {
		t.Fatalf("proposal status=%+v machine=%s", node.Status(), machine.digest())
	}
}

func TestRequestVoteFreshnessAndDurableSingleVote(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{
		HardState: HardState{Term: 3},
		Entries:   []Entry{{Index: 1, Term: 2, Type: EntryCommand, Command: []byte("x")}, {Index: 2, Term: 3, Type: EntryCommand, Command: []byte("y")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	node, _ := newTestNode(t, 1, []NodeID{1, 2, 3}, store)
	responses, err := node.Step(Message{Type: RequestVote, From: 2, To: 1, Term: 3, CandidateLastIndex: 99, CandidateLastTerm: 2})
	if err != nil || responses[0].VoteGranted {
		t.Fatalf("stale-term log vote responses=%+v err=%v", responses, err)
	}
	responses, err = node.Step(Message{Type: RequestVote, From: 2, To: 1, Term: 3, CandidateLastIndex: 2, CandidateLastTerm: 3})
	if err != nil || !responses[0].VoteGranted {
		t.Fatalf("fresh vote responses=%+v err=%v", responses, err)
	}
	restarted, _ := newTestNode(t, 1, []NodeID{1, 2, 3}, store)
	responses, err = restarted.Step(Message{Type: RequestVote, From: 3, To: 1, Term: 3, CandidateLastIndex: 2, CandidateLastTerm: 3})
	if err != nil || responses[0].VoteGranted || restarted.Status().VotedFor != 2 {
		t.Fatalf("second vote responses=%+v status=%+v err=%v", responses, restarted.Status(), err)
	}
}

func TestHigherTermStepsDownAndPersists(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 1, []NodeID{1}, store)
	tickUntilMessages(t, node, 12)
	if node.Status().Role != Leader {
		t.Fatal("not leader")
	}
	_, err := node.Step(Message{Type: RequestVote, From: 1, To: 1, Term: 7})
	if err != nil {
		t.Fatal(err)
	}
	if status := node.Status(); status.Role != Follower || status.Term != 7 || status.VotedFor != 0 {
		t.Fatalf("status=%+v", status)
	}
	state, err := store.Load()
	if err != nil || state.HardState.Term != 7 {
		t.Fatalf("durable state=%+v err=%v", state, err)
	}
}

func TestAppendEntriesConflictRepairAndFollowerCommit(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{
		HardState: HardState{Term: 4},
		Entries: []Entry{
			{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("A")},
			{Index: 2, Term: 2, Type: EntryCommand, Command: []byte("B")},
			{Index: 3, Term: 3, Type: EntryCommand, Command: []byte("X")},
			{Index: 4, Term: 3, Type: EntryCommand, Command: []byte("Y")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	entries := []Entry{{Index: 3, Term: 4, Type: EntryCommand, Command: []byte("C")}, {Index: 4, Term: 4, Type: EntryCommand, Command: []byte("D")}, {Index: 5, Term: 4, Type: EntryCommand, Command: []byte("E")}}
	responses, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 4, PrevLogIndex: 2, PrevLogTerm: 2, Entries: entries, LeaderCommit: 5})
	if err != nil || !responses[0].Success || responses[0].MatchIndex != 5 {
		t.Fatalf("append responses=%+v err=%v", responses, err)
	}
	state, _ := store.Load()
	if got := fmt.Sprintf("%q", []byte{state.Entries[2].Command[0], state.Entries[3].Command[0], state.Entries[4].Command[0]}); got != "\"CDE\"" {
		t.Fatalf("repaired suffix=%s", got)
	}
	if node.Status().CommitIndex != 5 || node.Status().LastApplied != 5 || machine.digest() != "[\"A\" \"B\" \"C\" \"D\" \"E\"]" {
		t.Fatalf("status=%+v machine=%s", node.Status(), machine.digest())
	}
}

func TestAppendRejectHintAndDuplicateSafety(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	request := Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 1, PrevLogTerm: 1}
	responses, err := node.Step(request)
	if err != nil || responses[0].Success || responses[0].RejectHint != 1 {
		t.Fatalf("missing prev response=%+v err=%v", responses, err)
	}
	request.PrevLogIndex, request.PrevLogTerm = 0, 0
	request.Entries = []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("x")}}
	for range 2 {
		responses, err = node.Step(request)
		if err != nil || !responses[0].Success {
			t.Fatalf("duplicate append response=%+v err=%v", responses, err)
		}
	}
	state, _ := store.Load()
	if len(state.Entries) != 1 {
		t.Fatalf("duplicate created %d entries", len(state.Entries))
	}
}

func TestFollowerSnapshotAheadMovesLeaderNextIndexForward(t *testing.T) {
	entries := []Entry{
		{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("a")},
		{Index: 2, Term: 1, Type: EntryCommand, Command: []byte("b")},
		{Index: 3, Term: 1, Type: EntryCommand, Command: []byte("c")},
		{Index: 4, Term: 2, Type: EntryCommand, Command: []byte("d")},
		{Index: 5, Term: 2, Type: EntryCommand, Command: []byte("e")},
	}
	leaderStore, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 2}, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	leader, _ := newTestNode(t, 1, []NodeID{1, 2, 3}, leaderStore)
	leader.role, leader.leaderID = Leader, 1
	leader.nextIndex = map[NodeID]uint64{1: 6, 2: 1, 3: 6}
	leader.matchIndex = map[NodeID]uint64{1: 5, 2: 0, 3: 5}
	followerStore, err := NewMemoryStoreFrom(PersistentState{
		HardState: HardState{Term: 2}, Snapshot: Snapshot{Index: 3, Term: 1},
		Entries: cloneEntries(entries[3:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	follower, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, followerStore)
	rejections, err := follower.Step(leader.replicationMessage(2))
	if err != nil || len(rejections) != 1 || rejections[0].Success || rejections[0].RejectHint != 4 {
		t.Fatalf("snapshot-ahead rejection=%+v err=%v", rejections, err)
	}
	retries, err := leader.Step(rejections[0])
	if err != nil || len(retries) != 1 || retries[0].PrevLogIndex != 3 || len(retries[0].Entries) != 2 {
		t.Fatalf("forward retry=%+v status=%+v err=%v", retries, leader.Status(), err)
	}
	responses, err := follower.Step(retries[0])
	if err != nil || len(responses) != 1 || !responses[0].Success || responses[0].MatchIndex != 5 {
		t.Fatalf("catch-up response=%+v status=%+v err=%v", responses, follower.Status(), err)
	}
}

func TestFollowerCommitIsBoundedByRequestCoverage(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 2}, Entries: []Entry{
		{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("a")},
		{Index: 2, Term: 2, Type: EntryCommand, Command: []byte("local-suffix")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	responses, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2})
	if err != nil || !responses[0].Success || node.Status().CommitIndex != 1 || machine.digest() != "[\"a\"]" {
		t.Fatalf("responses=%+v status=%+v machine=%s err=%v", responses, node.Status(), machine.digest(), err)
	}
}

func TestCommittedConflictIsInvariantViolation(t *testing.T) {
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, NewMemoryStore())
	if _, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("committed")}}, LeaderCommit: 1}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		value := recover()
		violation, ok := value.(*invariant.Violation)
		if !ok || violation.Name != "RAFT-4" {
			t.Fatalf("panic=%v", value)
		}
	}()
	_, _ = node.Step(Message{Type: AppendEntries, From: 3, To: 2, Term: 2, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 2, Type: EntryCommand, Command: []byte("conflict")}}})
}

func TestStaleTermRPCRejected(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 4}})
	if err != nil {
		t.Fatal(err)
	}
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	responses, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 3})
	if err != nil || responses[0].Success || responses[0].Term != 4 || node.Status().Term != 4 {
		t.Fatalf("responses=%+v status=%+v err=%v", responses, node.Status(), err)
	}
}

func TestPersistenceFailureStopsBeforeDependentMessages(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 1, []NodeID{1, 2, 3}, store)
	store.SetSaveError(errors.New("disk uncertain"))
	messages := tickUntilError(t, node, 12)
	if len(messages) != 0 || !errors.Is(node.Status().Fatal, ErrPersistence) {
		t.Fatalf("messages=%+v status=%+v", messages, node.Status())
	}
	if _, err := node.Tick(); !errors.Is(err, ErrStopped) {
		t.Fatalf("Tick after fatal=%v", err)
	}
}

func TestFollowerAppendPersistenceFailureSendsNoSuccess(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	store.SetSaveError(errors.New("fsync failed"))
	messages, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("x")}}})
	if !errors.Is(err, ErrPersistence) || len(messages) != 0 || !errors.Is(node.Status().Fatal, ErrPersistence) {
		t.Fatalf("messages=%+v status=%+v err=%v", messages, node.Status(), err)
	}
}

func TestLeaderProposalPersistenceFailureSendsNoReplication(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 1, []NodeID{1}, store)
	tickUntilMessages(t, node, 12)
	store.SetSaveError(errors.New("disk full"))
	index, messages, err := node.Propose([]byte("not durable"))
	if index != 0 || len(messages) != 0 || !errors.Is(err, ErrPersistence) || !errors.Is(node.Status().Fatal, ErrPersistence) {
		t.Fatalf("index=%d messages=%+v status=%+v err=%v", index, messages, node.Status(), err)
	}
}

func tickUntilError(t *testing.T, node *Node, limit int) []Message {
	t.Helper()
	for range limit {
		messages, err := node.Tick()
		if err != nil {
			if !errors.Is(err, ErrPersistence) {
				t.Fatalf("Tick error=%v", err)
			}
			return messages
		}
	}
	t.Fatal("expected persistence failure")
	return nil
}

func TestNoApplyBeforeCommitAndApplyFailureStopsAtEntry(t *testing.T) {
	store := NewMemoryStore()
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	request := Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("x")}}}
	if _, err := node.Step(request); err != nil {
		t.Fatal(err)
	}
	if machine.digest() != "[]" || node.Status().LastApplied != 0 {
		t.Fatalf("uncommitted applied: %s %+v", machine.digest(), node.Status())
	}
	machine.applyErr = errors.New("bad command")
	request.Entries = nil
	request.PrevLogIndex, request.PrevLogTerm, request.LeaderCommit = 1, 1, 1
	if messages, err := node.Step(request); !errors.Is(err, ErrApply) || len(messages) != 0 {
		t.Fatalf("apply failure messages=%+v err=%v", messages, err)
	}
	if node.Status().LastApplied != 0 || node.Status().CommitIndex != 1 {
		t.Fatalf("apply failure status=%+v", node.Status())
	}
}

func TestSnapshotCreateInstallAndRestart(t *testing.T) {
	leaderStore := NewMemoryStore()
	leader, leaderMachine := newTestNode(t, 1, []NodeID{1}, leaderStore)
	tickUntilMessages(t, leader, 12)
	for _, command := range []string{"a", "b", "c"} {
		if _, _, err := leader.Propose([]byte(command)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := leader.CreateSnapshot()
	if err != nil || snapshot.Index != 4 || snapshot.Term != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if leader.Status().LastIndex != 4 || len(leader.persistentCopy().Entries) != 0 {
		t.Fatalf("compacted status=%+v state=%+v", leader.Status(), leader.persistentCopy())
	}
	followerStore := NewMemoryStore()
	follower, followerMachine := newTestNode(t, 2, []NodeID{1, 2, 3}, followerStore)
	responses, err := follower.Step(Message{Type: InstallSnapshot, From: 1, To: 2, Term: 1, Snapshot: snapshot, LeaderCommit: snapshot.Index})
	if err != nil || !responses[0].Success || followerMachine.digest() != leaderMachine.digest() {
		t.Fatalf("install responses=%+v leader=%s follower=%s err=%v", responses, leaderMachine.digest(), followerMachine.digest(), err)
	}
	restarted, restartedMachine := newTestNode(t, 2, []NodeID{1, 2, 3}, followerStore)
	if restarted.Status().LastApplied != snapshot.Index || restartedMachine.digest() != leaderMachine.digest() {
		t.Fatalf("restart status=%+v machine=%s", restarted.Status(), restartedMachine.digest())
	}
}

func TestSnapshotPersistenceFailureDoesNotCompact(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 1, []NodeID{1}, store)
	tickUntilMessages(t, node, 12)
	if _, _, err := node.Propose([]byte("durable")); err != nil {
		t.Fatal(err)
	}
	before := node.persistentCopy()
	store.SetSaveError(errors.New("snapshot fsync failed"))
	snapshot, err := node.CreateSnapshot()
	if !errors.Is(err, ErrPersistence) || snapshot.Index != 0 || !reflect.DeepEqual(before, node.persistentCopy()) || !errors.Is(node.Status().Fatal, ErrPersistence) {
		t.Fatalf("snapshot=%+v before=%+v after=%+v status=%+v err=%v", snapshot, before, node.persistentCopy(), node.Status(), err)
	}
}

func TestSplitVoteRetriesAtHigherTerm(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 1, []NodeID{1, 2, 3, 4}, store)
	first := tickUntilMessages(t, node, 12)
	if len(first) != 3 || node.Status().Term != 1 || node.Status().Role != Candidate {
		t.Fatalf("first election messages=%d status=%+v", len(first), node.Status())
	}
	second := tickUntilMessages(t, node, 12)
	if len(second) != 3 || node.Status().Term != 2 || node.Status().VotedFor != 1 {
		t.Fatalf("retry messages=%d status=%+v", len(second), node.Status())
	}
}

func TestCandidateMissingCommittedEntryCannotWin(t *testing.T) {
	peers := []NodeID{1, 2, 3}
	upToDateState := PersistentState{HardState: HardState{Term: 1}, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("committed")}}}
	candidate, _ := newTestNode(t, 1, peers, NewMemoryStore())
	voter2Store, err := NewMemoryStoreFrom(upToDateState)
	if err != nil {
		t.Fatal(err)
	}
	voter3Store, err := NewMemoryStoreFrom(upToDateState)
	if err != nil {
		t.Fatal(err)
	}
	voter2, _ := newTestNode(t, 2, peers, voter2Store)
	voter3, _ := newTestNode(t, 3, peers, voter3Store)
	requests := tickUntilMessages(t, candidate, 12)
	for _, request := range requests {
		voter := voter2
		if request.To == 3 {
			voter = voter3
		}
		responses, err := voter.Step(request)
		if err != nil {
			t.Fatal(err)
		}
		if responses[0].VoteGranted {
			t.Fatalf("stale candidate granted by %d", voter.ID())
		}
		if _, err := candidate.Step(responses[0]); err != nil {
			t.Fatal(err)
		}
	}
	if candidate.Status().Role != Candidate {
		t.Fatalf("candidate won without committed entry: %+v", candidate.Status())
	}
}

func TestVoteAndAppendPersistencePrecedeSuccessResponse(t *testing.T) {
	store := NewMemoryStore()
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	var saved PersistentState
	store.SetSaveHook(func(state PersistentState) error {
		saved = state
		return nil
	})
	vote, err := node.Step(Message{Type: RequestVote, From: 1, To: 2, Term: 1})
	if err != nil || !vote[0].VoteGranted || saved.HardState.Term != 1 || saved.HardState.VotedFor != 1 {
		t.Fatalf("vote=%+v saved=%+v err=%v", vote, saved, err)
	}
	appendResponse, err := node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("x")}}})
	if err != nil || !appendResponse[0].Success || len(saved.Entries) != 1 || !bytes.Equal(saved.Entries[0].Command, []byte("x")) {
		t.Fatalf("append=%+v saved=%+v err=%v", appendResponse, saved, err)
	}
}

func TestDuplicateCommitNotificationAppliesOnce(t *testing.T) {
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, NewMemoryStore())
	request := Message{Type: AppendEntries, From: 1, To: 2, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []Entry{{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("x")}}, LeaderCommit: 1}
	for range 2 {
		if _, err := node.Step(request); err != nil {
			t.Fatal(err)
		}
	}
	if machine.digest() != "[\"x\"]" || node.Status().LastApplied != 1 {
		t.Fatalf("machine=%s status=%+v", machine.digest(), node.Status())
	}
}

func TestStaleSnapshotDoesNotRegressState(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 2}, Snapshot: Snapshot{Index: 3, Term: 1, Data: []byte{}}, Entries: []Entry{{Index: 4, Term: 2, Type: EntryCommand, Command: []byte("new")}}})
	if err != nil {
		t.Fatal(err)
	}
	node, _ := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	before := node.persistentCopy()
	responses, err := node.Step(Message{Type: InstallSnapshot, From: 1, To: 2, Term: 2, Snapshot: Snapshot{Index: 2, Term: 1, Data: []byte("old")}})
	if err != nil || !responses[0].Success || !reflect.DeepEqual(before, node.persistentCopy()) {
		t.Fatalf("responses=%+v before=%+v after=%+v err=%v", responses, before, node.persistentCopy(), err)
	}
}

func TestInstallSnapshotPreservesMatchingSuffix(t *testing.T) {
	source := &recordingStateMachine{}
	for index, command := range []string{"a", "b", "c"} {
		if err := source.Apply(Entry{Index: uint64(index + 1), Term: 2, Type: EntryCommand, Command: []byte(command)}); err != nil { //nolint:gosec // bounded test index
			t.Fatal(err)
		}
	}
	snapshotData, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 3}, Entries: []Entry{
		{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("old-a")},
		{Index: 2, Term: 2, Type: EntryCommand, Command: []byte("b")},
		{Index: 3, Term: 2, Type: EntryCommand, Command: []byte("c")},
		{Index: 4, Term: 3, Type: EntryCommand, Command: []byte("d")},
		{Index: 5, Term: 3, Type: EntryCommand, Command: []byte("e")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	responses, err := node.Step(Message{Type: InstallSnapshot, From: 1, To: 2, Term: 3, Snapshot: Snapshot{Index: 3, Term: 2, Data: snapshotData}, LeaderCommit: 5})
	if err != nil || !responses[0].Success || node.Status().Snapshot.Index != 3 || node.Status().LastApplied != 3 || machine.digest() != "[\"a\" \"b\" \"c\"]" {
		t.Fatalf("responses=%+v status=%+v machine=%s err=%v", responses, node.Status(), machine.digest(), err)
	}
	_, err = node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 3, PrevLogIndex: 3, PrevLogTerm: 2, Entries: []Entry{
		{Index: 4, Term: 3, Type: EntryCommand, Command: []byte("d")},
		{Index: 5, Term: 3, Type: EntryCommand, Command: []byte("e")},
	}, LeaderCommit: 5})
	if err != nil || node.Status().LastApplied != 5 || machine.digest() != "[\"a\" \"b\" \"c\" \"d\" \"e\"]" {
		t.Fatalf("verified suffix status=%+v machine=%s err=%v", node.Status(), machine.digest(), err)
	}
	state, err := store.Load()
	if err != nil || len(state.Entries) != 2 || state.Entries[0].Index != 4 {
		t.Fatalf("persistent state=%+v err=%v", state, err)
	}
}

func TestInstallSnapshotDoesNotApplyUnverifiedRetainedSuffix(t *testing.T) {
	source := &recordingStateMachine{}
	for index, command := range []string{"a", "b", "c"} {
		if err := source.Apply(Entry{Index: uint64(index + 1), Term: 2, Type: EntryCommand, Command: []byte(command)}); err != nil {
			t.Fatal(err)
		}
	}
	snapshotData, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 7}, Entries: []Entry{
		{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("old-a")},
		{Index: 2, Term: 2, Type: EntryCommand, Command: []byte("b")},
		{Index: 3, Term: 2, Type: EntryCommand, Command: []byte("c")},
		{Index: 4, Term: 2, Type: EntryCommand, Command: []byte("uncommitted-old")},
	}})
	if err != nil {
		t.Fatal(err)
	}
	node, machine := newTestNode(t, 2, []NodeID{1, 2, 3}, store)
	_, err = node.Step(Message{Type: InstallSnapshot, From: 1, To: 2, Term: 7, Snapshot: Snapshot{Index: 3, Term: 2, Data: snapshotData}, LeaderCommit: 4})
	if err != nil || node.Status().LastApplied != 3 || machine.digest() != "[\"a\" \"b\" \"c\"]" {
		t.Fatalf("unverified suffix applied status=%+v machine=%s err=%v", node.Status(), machine.digest(), err)
	}
	_, err = node.Step(Message{Type: AppendEntries, From: 1, To: 2, Term: 7, PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []Entry{{Index: 4, Term: 7, Type: EntryCommand, Command: []byte("committed-new")}}, LeaderCommit: 4})
	if err != nil || node.Status().LastApplied != 4 || machine.digest() != "[\"a\" \"b\" \"c\" \"committed-new\"]" {
		t.Fatalf("verified replacement status=%+v machine=%s err=%v", node.Status(), machine.digest(), err)
	}
}
