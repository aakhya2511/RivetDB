package raft

import (
	"errors"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestConfigurationCodecCanonicalAndRejectsCorruption(t *testing.T) {
	want := Configuration{Version: 7, OldVoters: []NodeID{3, 1, 2}, NewVoters: []NodeID{4, 2, 1}, Learners: []NodeID{5}}
	encoded, err := EncodeConfiguration(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeConfiguration(encoded)
	if err != nil {
		t.Fatal(err)
	}
	want = normalizeConfiguration(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("configuration=%+v want=%+v", got, want)
	}
	for offset := range encoded {
		truncated := encoded[:offset]
		if _, err := DecodeConfiguration(truncated); err == nil {
			t.Fatalf("truncation %d accepted", offset)
		}
	}
}

func TestJointQuorumRejectsUnionMajorityCounterexample(t *testing.T) {
	configuration := Configuration{Version: 3, OldVoters: []NodeID{1, 2, 3}, NewVoters: []NodeID{3, 4, 5}}
	acknowledged := map[NodeID]bool{1: true, 2: true, 3: true}
	if configuration.Quorum(func(id NodeID) bool { return acknowledged[id] }) {
		t.Fatal("A,B,C is a union majority but lacks a majority of new C,D,E")
	}
	acknowledged[4] = true
	if !configuration.Quorum(func(id NodeID) bool { return acknowledged[id] }) {
		t.Fatal("A,B,C,D should satisfy both majorities")
	}
}

func TestLearnerNeverCampaignsOrVotes(t *testing.T) {
	store := NewMemoryStore()
	machine := &recordingStateMachine{}
	node, err := NewNode(Config{ID: 4, Peers: []NodeID{1, 2, 3, 4}, InitialConfig: Configuration{Version: 2, OldVoters: []NodeID{1, 2, 3}, Learners: []NodeID{4}},
		ElectionTimeoutMin: 2, ElectionTimeoutMax: 2, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2)), Store: store, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		messages, tickErr := node.Tick()
		if tickErr != nil || len(messages) != 0 || node.Status().Role != Follower || node.Status().Term != 0 {
			t.Fatalf("learner tick messages=%+v status=%+v err=%v", messages, node.Status(), tickErr)
		}
	}
	responses, err := node.Step(Message{Type: RequestVote, From: 1, To: 4, Term: 1})
	if err != nil || len(responses) != 1 || responses[0].VoteGranted {
		t.Fatalf("learner vote responses=%+v err=%v", responses, err)
	}
}

func TestConfigurationTransitionShape(t *testing.T) {
	stable := Configuration{Version: 1, OldVoters: []NodeID{1, 2, 3}}
	learner := Configuration{Version: 2, OldVoters: []NodeID{1, 2, 3}, Learners: []NodeID{4}}
	joint := Configuration{Version: 3, OldVoters: []NodeID{1, 2, 3}, NewVoters: []NodeID{1, 2, 4}}
	final := Configuration{Version: 4, OldVoters: []NodeID{1, 2, 4}}
	for _, pair := range [][2]Configuration{{stable, learner}, {learner, joint}, {joint, final}} {
		if err := validateConfigurationTransition(pair[0], pair[1]); err != nil {
			t.Fatalf("transition %+v -> %+v: %v", pair[0], pair[1], err)
		}
	}
	bad := Configuration{Version: 2, OldVoters: []NodeID{1, 2, 4}}
	if err := validateConfigurationTransition(stable, bad); !errors.Is(err, ErrConfigTransition) {
		t.Fatalf("single-step voter replacement error=%v", err)
	}
}

//nolint:govet // save error is intentionally scoped to its assertion
func TestCommittedConfigurationSurvivesFileStoreRestart(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	configuration := Configuration{Version: 9, OldVoters: []NodeID{1, 2, 4}, Learners: []NodeID{5}}
	state := PersistentState{HardState: HardState{Term: 3}, Config: configuration}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(Config{ID: 2, Peers: configuration.Members(), ElectionTimeoutMin: 5, ElectionTimeoutMax: 5, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2)), Store: store, StateMachine: &recordingStateMachine{}})
	if err != nil {
		t.Fatal(err)
	}
	if got := node.Status().Config; !reflect.DeepEqual(got, normalizeConfiguration(configuration)) {
		t.Fatalf("recovered configuration=%+v", got)
	}
}

func TestCommittedConfigurationSurvivesSnapshotCompactionAndInstall(t *testing.T) {
	initial := Configuration{Version: 1, OldVoters: []NodeID{1, 2, 3}}
	joint := Configuration{Version: 2, OldVoters: []NodeID{1, 2, 3}, NewVoters: []NodeID{1, 2, 4}}
	_, err := EncodeConfiguration(joint)
	if err != nil {
		t.Fatal(err)
	}
	machine := &recordingStateMachine{}
	data, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{Index: 1, Term: 1, Data: data, Config: joint}
	store, err := NewMemoryStoreFrom(PersistentState{HardState: HardState{Term: 1}, Snapshot: snapshot, Config: joint})
	if err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(Config{ID: 1, Peers: initial.Members(), InitialConfig: initial, ElectionTimeoutMin: 5, ElectionTimeoutMax: 5, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(3, 4)), Store: store, StateMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.Config, normalizeConfiguration(joint)) || node.Status().LastIndex != snapshot.Index {
		t.Fatalf("snapshot=%+v status=%+v", snapshot, node.Status())
	}
	target, err := NewNode(Config{ID: 4, Peers: append(initial.Members(), 4), InitialConfig: initial, BootstrapLearner: true, ElectionTimeoutMin: 5, ElectionTimeoutMax: 5, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(5, 6)), Store: NewMemoryStore(), StateMachine: &recordingStateMachine{}})
	if err != nil {
		t.Fatal(err)
	}
	responses, err := target.Step(Message{Type: InstallSnapshot, From: 1, To: 4, Term: 1, Snapshot: snapshot, LeaderCommit: snapshot.Index})
	if err != nil || len(responses) != 1 || !responses[0].Success {
		t.Fatalf("install responses=%+v err=%v", responses, err)
	}
	if got := target.Status().Config; !reflect.DeepEqual(got, normalizeConfiguration(joint)) {
		t.Fatalf("installed configuration=%+v", got)
	}
}
