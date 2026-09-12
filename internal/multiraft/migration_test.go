package multiraft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func newMigrationCluster(t testing.TB) *multiTestCluster {
	bootstrap := threeRangeBootstrap()
	cluster := &multiTestCluster{t: t, root: t.TempDir(), bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, id := range bootstrap.Nodes {
		cluster.clocks[id] = clock.NewMockAt(time.UnixMilli(20_000 + int64(id)*1000))
	}
	cluster.openRuntime(true)
	t.Cleanup(func() { cluster.close() })
	return cluster
}

func migrationManager(t testing.TB, cluster *multiTestCluster) (*MigrationManager, *MetaRange) {
	t.Helper()
	catalog := mustCatalog(t, cluster.bootstrap)
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: cluster.root + "/metadata", Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router})
	if err != nil {
		t.Fatal(err)
	}
	return manager, meta
}

func TestMigrateActiveLeaderTransfersBeforeRemoval(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(1)
	record, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != MigrationSourceRetired {
		t.Fatalf("record=%+v", record)
	}
	for _, member := range meta.Snapshot().Catalog.Snapshot().Ranges[0].Replicas {
		if member.NodeID == 1 {
			t.Fatal("source leader remained in placement")
		}
	}
	leader, err := manager.leader(context.Background(), 10)
	if err != nil || leader == 1 {
		t.Fatalf("leader=%d err=%v", leader, err)
	}
}

//nolint:govet // sequential operation errors are scoped at their assertions
func TestMigrationPreservesPreparedIntentTxnRecordAndHistory(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	ctx := context.Background()
	first, err := cluster.router.PutMVCC(ctx, []byte("a"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := cluster.router.PutMVCC(ctx, []byte("a"), []byte("v2"))
	if err != nil {
		t.Fatal(err)
	}
	before1, err := cluster.nodes[1].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	digest1, err := before1.DigestAt(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	digest2, err := before1.DigestAt(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := cluster.router.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, readTime := transaction.ID(), transaction.ReadTimestamp()
	descriptor, _ := cluster.router.currentCatalog().LookupByID(10)
	commitTime, err := cluster.router.assignTransactionTimestamp(ctx, descriptor, []byte("b"), readTime)
	if err != nil {
		t.Fatal(err)
	}
	home := txn.Participant{RangeID: uint64(descriptor.RangeID), Generation: descriptor.Generation}
	create := txn.Operation{Type: txn.OpCreate, ID: id, ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: home, Participants: []txn.Participant{home}}
	if err := cluster.router.proposeOperation(ctx, descriptor, []byte("b"), replicatedrange.CommandTxnCreate, create); err != nil {
		t.Fatal(err)
	}
	prepare := txn.Operation{Type: txn.OpPrepare, ID: id, ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: home, Writes: []txn.Write{{Key: []byte("b"), Value: []byte("intent")}}}
	if err := cluster.router.proposeOperation(ctx, descriptor, []byte("b"), replicatedrange.CommandTxnPrepare, prepare); err != nil {
		t.Fatal(err)
	}
	manager, meta := migrationManager(t, cluster)
	source, _ := descriptor.ReplicaOn(3)
	record, err := manager.MoveReplica(ctx, 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	target, err := cluster.nodes[4].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := target.TransactionRecord(id); !ok || got.Status != txn.StatusPending {
		t.Fatalf("txn record=%+v ok=%v", got, ok)
	}
	if got, ok := target.ParticipantRecord(id); !ok || got.Status != txn.ParticipantPrepared {
		t.Fatalf("participant=%+v ok=%v", got, ok)
	}
	if got, _ := target.DigestAt(ctx, first); got != digest1 {
		t.Fatal("historical digest at first timestamp changed")
	}
	if got, _ := target.DigestAt(ctx, second); got != digest2 {
		t.Fatal("historical digest at second timestamp changed")
	}
	if record.State != MigrationSourceRetired || meta.Snapshot().Catalog.Generation() != cluster.bootstrap.Generation+1 {
		t.Fatalf("record=%+v", record)
	}
}

func TestRepeatedMigrationAndMoveBackAllocateNewReplicaIDs(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source3, _ := descriptor.ReplicaOn(3)
	first, err := manager.MoveReplica(context.Background(), 10, source3.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := meta.Snapshot().Catalog.LookupByID(10)
	source2, _ := current.ReplicaOn(2)
	second, err := manager.MoveReplica(context.Background(), 10, source2.ReplicaID, 5)
	if err != nil {
		t.Fatal(err)
	}
	current, _ = meta.Snapshot().Catalog.LookupByID(10)
	source4, _ := current.ReplicaOn(4)
	third, err := manager.MoveReplica(context.Background(), 10, source4.ReplicaID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if first.TargetReplicaID >= second.TargetReplicaID || second.TargetReplicaID >= third.TargetReplicaID || third.TargetReplicaID == source3.ReplicaID {
		t.Fatalf("identities first=%d second=%d third=%d retired=%d", first.TargetReplicaID, second.TargetReplicaID, third.TargetReplicaID, source3.ReplicaID)
	}
	if third.State != MigrationSourceRetired {
		t.Fatalf("third=%+v", third)
	}
}

//nolint:govet // error scopes mirror the migration assertions
func TestOnlineFollowerMigrationPreservesStateAndRetiresSource(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	ctx := context.Background()
	if _, err := cluster.router.PutMVCC(ctx, []byte("a"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(cluster.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: cluster.root + "/metadata", Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage MigrationHookStage, record MigrationRecord) {
		t.Logf("stage=%s state=%s", stage, record.State)
	}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	record, err := manager.MoveReplica(ctx, 10, source.ReplicaID, 4)
	if err != nil {
		for id, node := range cluster.nodes {
			if replica, e := node.Replica(10); e == nil {
				t.Logf("node=%d status=%+v", id, replica.Status().Raft)
			}
		}
		t.Logf("scheduler failures=%+v", cluster.scheduler.Failures())
		t.Fatal(err)
	}
	if record.State != MigrationSourceRetired || record.TargetReplicaID == record.SourceReplicaID {
		t.Fatalf("record=%+v", record)
	}
	current, _ := meta.Snapshot().Catalog.LookupByID(10)
	if current.Generation != descriptor.Generation+1 {
		t.Fatalf("generation=%d", current.Generation)
	}
	if _, ok := current.ReplicaOn(4); !ok {
		t.Fatalf("placement=%+v", current.Replicas)
	}
	if _, err := cluster.nodes[3].Replica(10); !errors.Is(err, ErrUnknownRange) {
		t.Fatalf("old replica lookup err=%v", err)
	}
	target, err := cluster.nodes[4].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	if target.Status().Lifecycle != replicatedrange.LifecycleActive {
		t.Fatal("target remained learner")
	}
	if deletionErr := manager.DeleteSource(record); deletionErr != nil {
		t.Fatal(deletionErr)
	}
	t.Logf("range=%d source=%d@%d target=%d@%d snapshot=%d barrier=%d config=%d", record.RangeID, record.SourceReplicaID, record.SourceNodeID, record.TargetReplicaID, record.TargetNodeID, record.BootstrapIndex, record.PromotionBarrier, record.ConfigVersion)
}

func TestMigrationMetadataIdentityIdempotencyAndExclusion(t *testing.T) {
	catalog := mustCatalog(t, threeRangeBootstrap())
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, _ := catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	first, err := state.beginMigration(10, source.ReplicaID, 4, descriptor.Generation, catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	same, err := state.beginMigration(10, source.ReplicaID, 4, descriptor.Generation, catalog.Generation())
	if err != nil || same != first {
		t.Fatalf("retry=%+v err=%v", same, err)
	}
	if _, err := state.beginMigration(10, source.ReplicaID, 5, descriptor.Generation, catalog.Generation()); !errors.Is(err, ErrMigrationInProgress) {
		t.Fatalf("same-range conflict=%v", err)
	}
	if _, err := state.begin(RangeRef{RangeID: 10, Generation: descriptor.Generation}, []byte("d"), catalog.Generation()); !errors.Is(err, ErrMigrationInProgress) {
		t.Fatalf("split overlap=%v", err)
	}
	if state.nextReplicaID != first.TargetReplicaID+1 || state.nextMigrationID != first.MigrationID+1 {
		t.Fatal("idempotent retry consumed identity")
	}
}

func TestMigrationKeepsTrafficOnlineAndReportsTransitionalStatus(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	ctx := context.Background()
	catalog := mustCatalog(t, cluster.bootstrap)
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: cluster.root + "/metadata", Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	var timestamps []mvcc.Timestamp
	var manager *MigrationManager
	manager, err = NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage MigrationHookStage, record MigrationRecord) {
		if stage != MigrationRecordDurable && stage != LearnerAdded && stage != JointConfigCommitted {
			return
		}
		for range 8 {
			_ = cluster.scheduler.Round()
		}
		key := []byte{'a', byte('0' + writes)}
		leader, leaderErr := manager.leader(ctx, 10)
		if leaderErr != nil {
			t.Fatalf("find leader during %s: %v", stage, leaderErr)
		}
		descriptor, _ := catalog.LookupByID(10)
		timestamp, pending, outbound, putErr := cluster.nodes[leader].ProposeMVCC(ctx, Route{RangeID: 10, Generation: descriptor.Generation, Key: key}, replicatedrange.Command{Type: replicatedrange.CommandPut, Key: key, Value: []byte(stage.String())})
		if putErr != nil {
			t.Fatalf("write during %s: leader=%d err=%v", stage, leader, putErr)
		}
		if driveErr := manager.drivePending(ctx, pending, outbound); driveErr != nil {
			t.Fatalf("commit write during %s: %v", stage, driveErr)
		}
		timestamps = append(timestamps, timestamp)
		writes++
		status, view, statusErr := manager.Status(record.MigrationID)
		if statusErr != nil || status.RangeID != 10 || view.DesiredTarget.NodeID != 4 {
			t.Fatalf("status during %s: status=%+v view=%+v err=%v", stage, status, view, statusErr)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	cluster.elect(10, 1)
	descriptor, _ := catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	record, err := manager.MoveReplica(ctx, 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if writes != 3 || record.State != MigrationSourceRetired {
		t.Fatalf("writes=%d record=%+v", writes, record)
	}
	target, err := cluster.nodes[4].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	for i := range writes {
		value, getErr := target.GetAt(ctx, []byte{'a', byte('0' + i)}, timestamps[i])
		if getErr != nil || len(value) == 0 {
			t.Fatalf("read online-%d value=%q err=%v", i, value, getErr)
		}
	}
}

func TestRetiredSourceDeletionCannotRemoveNewIncarnation(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	first, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := meta.Snapshot().Catalog.LookupByID(10)
	fromTarget, _ := current.ReplicaOn(4)
	back, err := manager.MoveReplica(context.Background(), 10, fromTarget.ReplicaID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.DeleteSource(first); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("old cleanup error=%v", err)
	}
	incarnation := filepath.Join(cluster.nodes[3].directory, "ranges", "10", "replica-"+fmt.Sprint(back.TargetReplicaID))
	if _, err := os.Stat(incarnation); err != nil {
		t.Fatalf("new incarnation removed by old cleanup: %v", err)
	}
}

func TestMigratePhase7ChildPreservesLineage(t *testing.T) {
	cluster, meta, splitter := newSplitHarness(t)
	cluster.elect(10, 1)
	split, err := splitter.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	cluster.elect(split.Left.RangeID, 1)
	migrator, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router})
	if err != nil {
		t.Fatal(err)
	}
	source, _ := split.Left.ReplicaOn(3)
	record, err := migrator.MoveReplica(context.Background(), split.Left.RangeID, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if record.RangeID != split.Left.RangeID || record.State != MigrationSourceRetired {
		t.Fatalf("migration=%+v split=%+v", record, split)
	}
	descendants, err := meta.ResolveRangeRef(RangeRef{RangeID: 10, Generation: 1})
	if err != nil || len(descendants) != 2 || descendants[0].RangeID != split.Left.RangeID || descendants[1].RangeID != split.Right.RangeID {
		t.Fatalf("lineage=%+v err=%v", descendants, err)
	}
}

func TestDifferentRangesMayHoldConcurrentMigrationRecords(t *testing.T) {
	catalog := mustCatalog(t, threeRangeBootstrap())
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	firstDescriptor, _ := catalog.LookupByID(10)
	firstSource, _ := firstDescriptor.ReplicaOn(3)
	first, err := state.beginMigration(10, firstSource.ReplicaID, 4, firstDescriptor.Generation, catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	secondDescriptor, _ := catalog.LookupByID(11)
	secondSource, _ := secondDescriptor.ReplicaOn(3)
	second, err := state.beginMigration(11, secondSource.ReplicaID, 5, secondDescriptor.Generation, catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if first.MigrationID == second.MigrationID || first.TargetReplicaID == second.TargetReplicaID {
		t.Fatalf("cross-range identities collided: first=%+v second=%+v", first, second)
	}
}

func TestBankTransferCommitsWhileParticipantRangeMigrates(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	range11, _ := cluster.router.currentCatalog().LookupByID(11)
	range11Leader := range11.Replicas[0].NodeID
	cluster.elect(11, range11Leader)
	ctx := context.Background()
	for key, balance := range map[string]string{"a-bank": "500", "g-bank": "500"} {
		if _, err := cluster.router.PutMVCC(ctx, []byte(key), []byte(balance)); err != nil {
			t.Fatal(err)
		}
	}
	manager, meta := migrationManager(t, cluster)
	cluster.elect(10, 1)
	cluster.elect(11, range11Leader)
	committed := false
	manager.hook = func(stage MigrationHookStage, _ MigrationRecord) {
		if stage != LearnerAdded {
			return
		}
		transfer, beginErr := cluster.router.Begin(ctx)
		if beginErr != nil {
			t.Fatalf("begin transfer during migration: %v", beginErr)
		}
		from, getErr := transfer.Get(ctx, []byte("a-bank"))
		if getErr != nil {
			t.Fatalf("read source balance: %v", getErr)
		}
		to, getErr := transfer.Get(ctx, []byte("g-bank"))
		if getErr != nil {
			t.Fatalf("read target balance: %v", getErr)
		}
		fromBalance, _ := strconv.Atoi(string(from))
		toBalance, _ := strconv.Atoi(string(to))
		if putErr := transfer.Put([]byte("a-bank"), []byte(strconv.Itoa(fromBalance-25))); putErr != nil {
			t.Fatal(putErr)
		}
		if putErr := transfer.Put([]byte("g-bank"), []byte(strconv.Itoa(toBalance+25))); putErr != nil {
			t.Fatal(putErr)
		}
		if commitErr := transfer.Commit(ctx); commitErr != nil {
			t.Fatalf("commit transfer during migration: %v", commitErr)
		}
		committed = true
	}
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	if _, err := manager.MoveReplica(ctx, 10, source.ReplicaID, 4); err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("transaction hook did not execute")
	}
	reader, err := cluster.router.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Abort(ctx) }()
	total := 0
	for _, key := range []string{"a-bank", "g-bank"} {
		value, getErr := reader.Get(ctx, []byte(key))
		if getErr != nil {
			t.Fatal(getErr)
		}
		balance, convertErr := strconv.Atoi(string(value))
		if convertErr != nil {
			t.Fatal(convertErr)
		}
		total += balance
	}
	if total != 1000 {
		t.Fatalf("bank total=%d", total)
	}
}

func TestElectionAndCommitInJointConfiguration(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	jointLeader := false
	manager.hook = func(stage MigrationHookStage, _ MigrationRecord) {
		if stage != JointConfigCommitted {
			return
		}
		cluster.elect(10, 2)
		status := rangeStatus(t, cluster.nodes[2], 10).Raft
		if status.Role != raft.Leader || !status.Config.Joint() {
			t.Fatalf("joint election status=%+v", status)
		}
		jointLeader = true
	}
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	record, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !jointLeader || record.State != MigrationSourceRetired {
		t.Fatalf("jointLeader=%v record=%+v", jointLeader, record)
	}
}

func TestRetiredReplicaMessageStormCannotChangeActiveState(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	manager, meta := migrationManager(t, cluster)
	before, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := before.ReplicaOn(3)
	if _, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4); err != nil {
		t.Fatal(err)
	}
	after, _ := meta.Snapshot().Catalog.LookupByID(10)
	destination, _ := after.ReplicaOn(1)
	initial := rangeStatus(t, cluster.nodes[1], 10).Raft
	envelope := Envelope{RangeID: 10, MessageRangeID: 10, Generation: after.Generation, DescriptorFingerprint: descriptorFingerprint(after), FromReplica: source.ReplicaID, ToReplica: destination.ReplicaID, Message: raft.Message{Type: raft.AppendEntries, From: 3, To: 1, Term: initial.Term + 100}}
	for range 1_000 {
		if _, err := cluster.nodes[1].Step(envelope); !errors.Is(err, ErrWrongRangeMessage) {
			t.Fatalf("retired message error=%v", err)
		}
	}
	final := rangeStatus(t, cluster.nodes[1], 10).Raft
	if final.Term != initial.Term || final.Config.Version != initial.Config.Version || final.CommitIndex != initial.CommitIndex {
		t.Fatalf("retired traffic changed active state: before=%+v after=%+v", initial, final)
	}
}

func TestPhysicalSourceDeletionAndAuthoritativeFullRestart(t *testing.T) {
	cluster := newMigrationCluster(t)
	cluster.elect(10, 1)
	ctx := context.Background()
	timestamp, err := cluster.router.PutMVCC(ctx, []byte("a-durable"), []byte("survives"))
	if err != nil {
		t.Fatal(err)
	}
	manager, meta := migrationManager(t, cluster)
	descriptor, _ := meta.Snapshot().Catalog.LookupByID(10)
	source, _ := descriptor.ReplicaOn(3)
	record, err := manager.MoveReplica(ctx, 10, source.ReplicaID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if deletionErr := manager.DeleteSource(record); deletionErr != nil {
		t.Fatal(deletionErr)
	}
	snapshot := meta.Snapshot()
	cluster.close()
	reopenSplitCluster(t, cluster, snapshot)
	if _, lookupErr := cluster.nodes[3].Replica(10); !errors.Is(lookupErr, ErrUnknownRange) {
		t.Fatalf("deleted source reopened: %v", lookupErr)
	}
	target, err := cluster.nodes[4].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	value, err := target.GetAt(ctx, []byte("a-durable"), timestamp)
	if err != nil || string(value) != "survives" {
		t.Fatalf("restarted target value=%q err=%v", value, err)
	}
}
