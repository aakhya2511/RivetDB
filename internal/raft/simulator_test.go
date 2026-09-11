package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

func newTestSimulator(t testing.TB, nodes int, seed uint64) *Simulator {
	t.Helper()
	ids := make([]NodeID, nodes)
	for index := range ids {
		ids[index] = NodeID(index + 1)
	}
	simulator, err := NewSimulator(SimulatorConfig{
		NodeIDs: ids, ElectionTimeoutMin: 5, ElectionTimeoutMax: 9,
		HeartbeatInterval: 2, TickDuration: time.Millisecond, Seed: seed,
		Clock: clock.NewMock(), NewStateMachine: func(NodeID) StateMachine { return &recordingStateMachine{} },
	})
	if err != nil {
		t.Fatalf("NewSimulator: %v", err)
	}
	return simulator
}

func electNode(t testing.TB, simulator *Simulator, id NodeID) {
	t.Helper()
	for range 20 {
		if simulator.Node(id).Status().Role == Candidate || simulator.Node(id).Status().Role == Leader {
			break
		}
		if err := simulator.Tick(id); err != nil {
			t.Fatalf("tick election: %v", err)
		}
	}
	if _, err := simulator.DeliverAll(10_000); err != nil {
		t.Fatalf("deliver election: %v", err)
	}
	if simulator.Node(id).Status().Role != Leader {
		t.Fatalf("node %d status=%+v", id, simulator.Node(id).Status())
	}
}

func machineAt(t testing.TB, simulator *Simulator, id NodeID) *recordingStateMachine {
	t.Helper()
	machine, ok := simulator.StateMachine(id).(*recordingStateMachine)
	if !ok {
		t.Fatalf("node %d state machine %T", id, simulator.StateMachine(id))
	}
	return machine
}

func replicateCommand(t testing.TB, simulator *Simulator, leader NodeID, command string) uint64 {
	t.Helper()
	index, err := simulator.ProposeAndApply(leader, []byte(command), 10_000)
	if err != nil {
		t.Fatalf("ProposeAndApply: %v", err)
	}
	if simulator.Node(leader).Status().LastApplied < index {
		t.Fatalf("proposal %d not applied: %+v", index, simulator.Node(leader).Status())
	}
	return index
}

func TestThreeAndFiveNodeElectionReplication(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			simulator := newTestSimulator(t, size, uint64(size))
			electNode(t, simulator, 1)
			replicateCommand(t, simulator, 1, "alpha")
			want := machineAt(t, simulator, 1).digest()
			for id := NodeID(1); id <= NodeID(size); id++ {
				if got := machineAt(t, simulator, id).digest(); got != want {
					t.Fatalf("node %d digest=%s want=%s", id, got, want)
				}
			}
		})
	}
}

func TestFollowerLagCatchupAndConflictRepair(t *testing.T) {
	simulator := newTestSimulator(t, 3, 11)
	electNode(t, simulator, 1)
	if err := simulator.Partition([]NodeID{3}, []NodeID{1, 2}); err != nil {
		t.Fatal(err)
	}
	for index := range 100 {
		replicateCommand(t, simulator, 1, string(rune('a'+index%26)))
	}
	if simulator.Node(3).Status().LastIndex >= simulator.Node(1).Status().LastIndex {
		t.Fatal("partitioned follower did not lag")
	}
	simulator.Heal()
	for range 3 {
		if err := simulator.Tick(1); err != nil {
			t.Fatal(err)
		}
		if _, err := simulator.DeliverAll(50_000); err != nil {
			t.Fatal(err)
		}
	}
	if simulator.Node(3).Status().LastIndex != simulator.Node(1).Status().LastIndex || machineAt(t, simulator, 3).digest() != machineAt(t, simulator, 1).digest() {
		t.Fatalf("lagged follower status=%+v leader=%+v", simulator.Node(3).Status(), simulator.Node(1).Status())
	}
}

func TestLeaderIsolationMinorityCannotCommitAndHeal(t *testing.T) {
	simulator := newTestSimulator(t, 3, 22)
	electNode(t, simulator, 1)
	oldCommit := simulator.Node(1).Status().CommitIndex
	if err := simulator.Partition([]NodeID{1}, []NodeID{2, 3}); err != nil {
		t.Fatal(err)
	}
	uncommitted, err := simulator.Propose(1, []byte("minority"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(100); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().CommitIndex != oldCommit || machineAt(t, simulator, 1).digest() != "[]" {
		t.Fatalf("minority committed index %d: %+v", uncommitted, simulator.Node(1).Status())
	}
	electNode(t, simulator, 2)
	replicateCommand(t, simulator, 2, "majority")
	simulator.Heal()
	for range 4 {
		if err := simulator.Tick(2); err != nil {
			t.Fatal(err)
		}
		if _, err := simulator.DeliverAll(10_000); err != nil {
			t.Fatal(err)
		}
	}
	if simulator.Node(1).Status().Role != Follower || machineAt(t, simulator, 1).digest() != machineAt(t, simulator, 2).digest() {
		t.Fatalf("heal old=%+v oldsm=%s new=%+v newsm=%s", simulator.Node(1).Status(), machineAt(t, simulator, 1).digest(), simulator.Node(2).Status(), machineAt(t, simulator, 2).digest())
	}
}

func TestEvenClusterRequiresThreeVotes(t *testing.T) {
	simulator := newTestSimulator(t, 4, 33)
	electNode(t, simulator, 1)
	if err := simulator.Partition([]NodeID{1, 2}, []NodeID{3, 4}); err != nil {
		t.Fatal(err)
	}
	before := simulator.Node(1).Status().CommitIndex
	index, err := simulator.Propose(1, []byte("two-is-not-quorum"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(100); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().CommitIndex != before || simulator.Node(1).Status().LastApplied >= index {
		t.Fatalf("two-node side committed: %+v", simulator.Node(1).Status())
	}
}

func TestFiveNodeMinorityCannotCommitMajorityProgresses(t *testing.T) {
	simulator := newTestSimulator(t, 5, 34)
	electNode(t, simulator, 1)
	if err := simulator.Partition([]NodeID{1, 2}, []NodeID{3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	before := simulator.Node(1).Status().CommitIndex
	minorityIndex, err := simulator.Propose(1, []byte("minority"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(1_000); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().CommitIndex != before || simulator.Node(1).Status().LastApplied >= minorityIndex {
		t.Fatalf("minority committed: %+v", simulator.Node(1).Status())
	}
	electNode(t, simulator, 3)
	replicateCommand(t, simulator, 3, "majority")
	if machineAt(t, simulator, 4).digest() != "[\"majority\"]" || machineAt(t, simulator, 5).digest() != "[\"majority\"]" {
		t.Fatalf("majority did not progress: node4=%s node5=%s", machineAt(t, simulator, 4).digest(), machineAt(t, simulator, 5).digest())
	}
}

func TestLeaderHeartbeatAndStaleResponseCannotRegress(t *testing.T) {
	simulator := newTestSimulator(t, 3, 35)
	electNode(t, simulator, 1)
	status := simulator.Node(1).Status()
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	before := len(simulator.Pending())
	if before < 2 {
		t.Fatalf("heartbeat pending=%d", before)
	}
	if _, err := simulator.Node(1).Step(Message{Type: AppendEntriesResponse, From: 2, To: 1, Term: status.Term - 1, Success: true, MatchIndex: 0}); err != nil {
		t.Fatal(err)
	}
	after := simulator.Node(1).Status()
	if after.Term != status.Term || after.Role != Leader || after.CommitIndex != status.CommitIndex {
		t.Fatalf("stale response regressed leader: before=%+v after=%+v", status, after)
	}
}

func TestDuplicateReorderedAndAsymmetricMessages(t *testing.T) {
	simulator := newTestSimulator(t, 3, 44)
	for range 9 {
		if err := simulator.Tick(1); err != nil {
			t.Fatal(err)
		}
		if len(simulator.Pending()) != 0 {
			break
		}
	}
	pending := simulator.Pending()
	if len(pending) < 2 {
		t.Fatalf("pending=%v", pending)
	}
	duplicate, err := simulator.Duplicate(pending[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := simulator.SetLink(2, 1, false); err != nil {
		t.Fatal(err)
	}
	if err := simulator.SetLink(3, 1, false); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Deliver(pending[1].ID); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Deliver(duplicate); err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(100); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().Role == Leader {
		t.Fatal("asymmetric response loss unexpectedly elected leader")
	}
	if err := simulator.SetLink(2, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := simulator.SetLink(3, 1, true); err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(100); err != nil {
		t.Fatal(err)
	}
}

func TestNodeCrashRestartPreservesTermVoteAndLog(t *testing.T) {
	simulator := newTestSimulator(t, 3, 55)
	electNode(t, simulator, 1)
	replicateCommand(t, simulator, 1, "durable")
	before := simulator.Node(2).Status()
	if err := simulator.Crash(2); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Restart(2); err != nil {
		t.Fatal(err)
	}
	after := simulator.Node(2).Status()
	if after.Role != Follower || after.Term != before.Term || after.LastIndex != before.LastIndex || after.CommitIndex != after.Snapshot.Index {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(1_000); err != nil {
		t.Fatal(err)
	}
	if machineAt(t, simulator, 2).digest() != machineAt(t, simulator, 1).digest() {
		t.Fatalf("restart digest=%s leader=%s", machineAt(t, simulator, 2).digest(), machineAt(t, simulator, 1).digest())
	}
}

func TestCandidateCrashRestartPreservesSelfVote(t *testing.T) {
	simulator := newTestSimulator(t, 3, 551)
	for simulator.Node(1).Status().Role != Candidate {
		if err := simulator.Tick(1); err != nil {
			t.Fatal(err)
		}
	}
	before := simulator.Node(1).Status()
	for _, envelope := range simulator.Pending() {
		if err := simulator.Drop(envelope.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := simulator.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Restart(1); err != nil {
		t.Fatal(err)
	}
	after := simulator.Node(1).Status()
	if after.Role != Follower || after.Term != before.Term || after.VotedFor != 1 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	responses, err := simulator.Node(1).Step(Message{Type: RequestVote, From: 2, To: 1, Term: after.Term})
	if err != nil || len(responses) != 1 || responses[0].VoteGranted {
		t.Fatalf("same-term vote responses=%+v err=%v", responses, err)
	}
}

func TestLeaderCrashNewLeaderPreservesCommittedHistory(t *testing.T) {
	simulator := newTestSimulator(t, 3, 552)
	electNode(t, simulator, 1)
	replicateCommand(t, simulator, 1, "before-leader-crash")
	if err := simulator.Crash(1); err != nil {
		t.Fatal(err)
	}
	electNode(t, simulator, 2)
	replicateCommand(t, simulator, 2, "after-leader-crash")
	if err := simulator.Restart(1); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err := simulator.Tick(2); err != nil {
			t.Fatal(err)
		}
		if _, err := simulator.DeliverAll(10_000); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := machineAt(t, simulator, 1).digest(), machineAt(t, simulator, 2).digest(); got != want {
		t.Fatalf("restarted old leader digest=%s want=%s", got, want)
	}
}

func TestFullClusterRestartRecoversAndAppliesCommittedLog(t *testing.T) {
	simulator := newTestSimulator(t, 3, 56)
	electNode(t, simulator, 1)
	replicateCommand(t, simulator, 1, "before-restart")
	for _, id := range simulator.ids {
		if err := simulator.Crash(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range simulator.ids {
		if err := simulator.Restart(id); err != nil {
			t.Fatal(err)
		}
	}
	electNode(t, simulator, 1)
	replicateCommand(t, simulator, 1, "after-restart")
	want := machineAt(t, simulator, 1).digest()
	for _, id := range simulator.ids {
		if got := machineAt(t, simulator, id).digest(); got != want {
			t.Fatalf("node %d digest=%s want=%s", id, got, want)
		}
	}
}

func TestLaggedFollowerInstallsSnapshotAndResumesAppend(t *testing.T) {
	simulator := newTestSimulator(t, 3, 66)
	electNode(t, simulator, 1)
	if err := simulator.Partition([]NodeID{3}, []NodeID{1, 2}); err != nil {
		t.Fatal(err)
	}
	for _, command := range []string{"a", "b", "c", "d"} {
		replicateCommand(t, simulator, 1, command)
	}
	snapshot, err := simulator.Node(1).CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Index < 5 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	for _, envelope := range simulator.Pending() {
		if envelope.Message.From == 3 || envelope.Message.To == 3 {
			if err := simulator.Drop(envelope.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	simulator.Heal()
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	if err := simulator.Tick(1); err != nil {
		t.Fatal(err)
	}
	if _, err := simulator.DeliverAll(10_000); err != nil {
		t.Fatal(err)
	}
	replicateCommand(t, simulator, 1, "after")
	if simulator.Node(3).Status().Snapshot.Index != snapshot.Index || machineAt(t, simulator, 3).digest() != machineAt(t, simulator, 1).digest() {
		t.Fatalf("snapshot follower=%+v digest=%s leader=%s", simulator.Node(3).Status(), machineAt(t, simulator, 3).digest(), machineAt(t, simulator, 1).digest())
	}
}

func TestCurrentTermCommitRule(t *testing.T) {
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 2}, Entries: []Entry{
		{Index: 1, Term: 1, Type: EntryCommand, Command: []byte("old")},
		{Index: 2, Term: 2, Type: EntryNoOp},
	}})
	if err != nil {
		t.Fatal(err)
	}
	node, machine := newTestNode(t, 1, []NodeID{1, 2, 3}, store)
	node.role = Leader
	node.nextIndex = map[NodeID]uint64{1: 3, 2: 2, 3: 2}
	node.matchIndex = map[NodeID]uint64{1: 2, 2: 1, 3: 1}
	if err := node.advanceCommit(); err != nil {
		t.Fatal(err)
	}
	if node.Status().CommitIndex != 0 {
		t.Fatalf("old term committed alone: %+v", node.Status())
	}
	node.matchIndex[2] = 2
	if err := node.advanceCommit(); err != nil {
		t.Fatal(err)
	}
	if node.Status().CommitIndex != 2 || machine.digest() != "[\"old\"]" {
		t.Fatalf("transitive commit status=%+v machine=%s", node.Status(), machine.digest())
	}
}

func TestClockDrivenTicksAreDeterministic(t *testing.T) {
	mock := clock.NewMock()
	simulator, err := NewSimulator(SimulatorConfig{
		NodeIDs: []NodeID{1}, ElectionTimeoutMin: 5, ElectionTimeoutMax: 5,
		HeartbeatInterval: 2, TickDuration: time.Millisecond, Seed: 77, Clock: mock,
		NewStateMachine: func(NodeID) StateMachine { return &recordingStateMachine{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	mock.Advance(4 * time.Millisecond)
	if err := simulator.AdvanceClock(); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().Role != Follower {
		t.Fatal("elected early")
	}
	mock.Advance(time.Millisecond)
	if err := simulator.AdvanceClock(); err != nil {
		t.Fatal(err)
	}
	if simulator.Node(1).Status().Role != Leader {
		t.Fatalf("status=%+v", simulator.Node(1).Status())
	}
}

func TestNonLeaderProposal(t *testing.T) {
	simulator := newTestSimulator(t, 3, 88)
	if _, err := simulator.Propose(2, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose error=%v", err)
	}
}
