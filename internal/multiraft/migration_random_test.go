package multiraft

import (
	"context"
	"os"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestRandomizedMigrationMembershipHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_MIGRATION_STRESS") == "" {
		t.Skip("set RIVETDB_MIGRATION_STRESS=1 for 100k membership events")
	}
	random := testutil.Rand(t)
	configuration := raft.Configuration{Version: 1, OldVoters: []raft.NodeID{1, 2, 3}}
	nextReplica := ReplicaID(100)
	lastReplica := ReplicaID(99)
	for event := range 100_000 {
		candidates := []raft.NodeID{3, 4, 5}
		target := candidates[random.IntN(len(candidates))]
		for target == configuration.OldVoters[2] {
			target = candidates[random.IntN(len(candidates))]
		}
		learner := raft.Configuration{Version: configuration.Version + 1, OldVoters: append([]raft.NodeID(nil), configuration.OldVoters...), Learners: []raft.NodeID{target}}
		if learner.Quorum(func(id raft.NodeID) bool { return id == target }) {
			t.Fatalf("event %d learner counted toward quorum", event)
		}
		if !learner.Learner(target) || learner.Voter(target) {
			t.Fatalf("event %d target role is ambiguous", event)
		}
		newVoters := append([]raft.NodeID(nil), configuration.OldVoters[:2]...)
		newVoters = append(newVoters, target)
		joint := raft.Configuration{Version: learner.Version + 1, OldVoters: learner.OldVoters, NewVoters: newVoters}
		acks := map[raft.NodeID]bool{configuration.OldVoters[0]: true, configuration.OldVoters[2]: true}
		if joint.Quorum(func(id raft.NodeID) bool { return acks[id] }) {
			t.Fatalf("event %d joint quorum accepted without new majority", event)
		}
		acks[target] = true
		if !joint.Quorum(func(id raft.NodeID) bool { return acks[id] }) {
			t.Fatalf("event %d dual majority rejected", event)
		}
		configuration = raft.Configuration{Version: joint.Version + 1, OldVoters: newVoters}
		if nextReplica <= lastReplica {
			t.Fatalf("event %d replica identity reused", event)
		}
		lastReplica = nextReplica
		nextReplica++
	}
}

func TestRepeatedDiskBackedMigrations(t *testing.T) {
	if os.Getenv("RIVETDB_MIGRATION_STRESS") == "" {
		t.Skip("set RIVETDB_MIGRATION_STRESS=1 for repeated durable migrations")
	}
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	targets := []raft.NodeID{4, 3}
	var last ReplicaID
	for move := range 20 {
		descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
		var source ReplicaDescriptor
		for _, replica := range descriptor.Replicas {
			if replica.NodeID == 3 || replica.NodeID == 4 {
				source = replica
				break
			}
		}
		record, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, targets[move%len(targets)])
		if err != nil {
			t.Fatalf("move %d: %v", move, err)
		}
		if record.TargetReplicaID <= last {
			t.Fatalf("move %d reused identity %d after %d", move, record.TargetReplicaID, last)
		}
		last = record.TargetReplicaID
	}
}
