//nolint:govet // tests intentionally reuse err in compact failure-local assertions
package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func newSplitHarness(t *testing.T) (*multiTestCluster, *MetaRange, *SplitManager) {
	t.Helper()
	cluster := newMVCCMultiTestCluster(t)
	catalog, err := NewCatalog(cluster.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) { t.Log(stage) }})
	if err != nil {
		t.Fatal(err)
	}
	return cluster, meta, manager
}

func TestOnlineSplitPreservesMVCCHistoryAndAuthority(t *testing.T) {
	cluster, meta, manager := newSplitHarness(t)
	cluster.elect(10, 1)
	var timestamps []mvcc.Timestamp
	for _, value := range []string{"one", "two", "three"} {
		timestamp, err := cluster.router.PutMVCC(context.Background(), []byte("b"), []byte(value))
		if err != nil {
			t.Fatal(err)
		}
		timestamps = append(timestamps, timestamp)
	}
	deleteAt, err := cluster.router.DeleteMVCC(context.Background(), []byte("e"))
	if err != nil {
		t.Fatal(err)
	}
	parentBefore, err := cluster.router.leaderReplica(context.Background(), mustDescriptor(t, cluster.router.currentCatalog(), 10))
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := parentBefore.SnapshotAt(timestamps[1])
	if err != nil {
		t.Fatal(err)
	}
	defer oldSnapshot.Close()
	record, err := manager.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Logf("scheduler failures: %+v", cluster.scheduler.Failures())
		for id, node := range cluster.nodes {
			t.Logf("node %d status: %+v", id, node.Status().Ranges)
		}
		t.Fatal(err)
	}
	if record.State != SplitCommitted || record.Left.RangeID == 10 || record.Right.RangeID == 10 || record.Left.RangeID == record.Right.RangeID {
		t.Fatalf("bad committed split: %+v", record)
	}
	if got := meta.Snapshot().Catalog.Generation(); got != cluster.bootstrap.Generation+1 {
		t.Fatalf("catalog generation=%d", got)
	}
	left, err := cluster.router.leaderReplica(context.Background(), record.Left)
	if err != nil {
		t.Fatal(err)
	}
	leftStatus := left.Status()
	if leftStatus.BootstrapSplitID != uint64(record.SplitID) || leftStatus.BootstrapParentRangeID != 10 || leftStatus.BootstrapParentGen != 1 ||
		leftStatus.BootstrapParentIndex != record.BootstrapIndex || leftStatus.BootstrapImageDigest == [32]byte{} {
		t.Fatalf("left bootstrap provenance=%+v", leftStatus)
	}
	for index, timestamp := range timestamps {
		value, readErr := left.GetAt(context.Background(), []byte("b"), timestamp)
		if readErr != nil || !bytes.Equal(value, []byte([]string{"one", "two", "three"}[index])) {
			t.Fatalf("historical read %d value=%q err=%v", timestamp, value, readErr)
		}
	}
	rows, err := left.ScanAt(context.Background(), []byte("a"), []byte("d"), timestamps[1])
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].Key, []byte("b")) || !bytes.Equal(rows[0].Value, []byte("two")) {
		t.Fatalf("historical scan rows=%+v err=%v", rows, err)
	}
	oldValue, err := oldSnapshot.Get(context.Background(), []byte("b"))
	if err != nil || !bytes.Equal(oldValue, []byte("two")) {
		t.Fatalf("retained pre-split snapshot value=%q err=%v", oldValue, err)
	}
	right, err := cluster.router.leaderReplica(context.Background(), record.Right)
	if err != nil {
		t.Fatal(err)
	}
	rightStatus := right.Status()
	if rightStatus.BootstrapSplitID != uint64(record.SplitID) || rightStatus.BootstrapParentRangeID != 10 || rightStatus.BootstrapParentGen != 1 ||
		rightStatus.BootstrapParentIndex != record.BootstrapIndex || rightStatus.BootstrapImageDigest == [32]byte{} {
		t.Fatalf("right bootstrap provenance=%+v", rightStatus)
	}
	if _, err := right.GetAt(context.Background(), []byte("e"), deleteAt); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("tombstone=%v", err)
	}
	parent, err := cluster.nodes[1].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	if parent.Status().Lifecycle != replicatedrange.LifecycleRetired {
		t.Fatalf("parent lifecycle=%v", parent.Status().Lifecycle)
	}
	if _, err := cluster.router.PutMVCC(context.Background(), []byte("d"), []byte("right")); err != nil {
		t.Fatal(err)
	}
	if _, err := right.GetAt(context.Background(), []byte("d"), right.Status().MaxAppliedMVCC); err != nil {
		t.Fatal(err)
	}
	descendants, err := meta.ResolveRangeRef(RangeRef{RangeID: 10, Generation: 1})
	if err != nil || len(descendants) != 2 {
		t.Fatalf("descendants=%v err=%v", descendants, err)
	}
	t.Logf("parent=10 left=%d right=%d S=%d F=%d timestamps=%v digest=%x history_gets=3 history_scans=1 old_snapshot=retained",
		record.Left.RangeID, record.Right.RangeID, record.BootstrapIndex, record.FenceIndex, timestamps, record.ImageDigest[:8])
}

func TestMetadataCodecAndSplitValidation(t *testing.T) {
	catalog, err := NewCatalog(threeRangeBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.begin(RangeRef{RangeID: 10, Generation: 1}, []byte("g"), catalog.Generation()); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("endpoint boundary=%v", err)
	}
	record, err := state.begin(RangeRef{RangeID: 10, Generation: 1}, []byte("d"), catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := encodeMetadata(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeMetadata(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := decoded.splits[record.SplitID]; !splitRecordsEqual(record, got) {
		t.Fatalf("round trip mismatch")
	}
	for offset := range len(encoded) {
		if _, err := decodeMetadata(encoded[:offset]); err == nil {
			t.Fatalf("truncation %d accepted", offset)
		}
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[7] = 1
	if _, err := decodeMetadata(corrupt); err == nil {
		t.Fatal("reserved metadata byte accepted")
	}
}

func TestMetaRangeDuplicateBeginIsVerifiedNoOp(t *testing.T) {
	catalog := mustCatalog(t, threeRangeBootstrap())
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: catalog.Snapshot().Nodes, Directory: filepath.Join(t.TempDir(), "metadata"), Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	first, err := meta.BeginSplit(t.Context(), RangeRef{RangeID: 10, Generation: 1}, []byte("d"), catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	second, err := meta.BeginSplit(t.Context(), RangeRef{RangeID: 10, Generation: 1}, []byte("d"), catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if !splitRecordsEqual(first, second) || meta.Snapshot().NextRangeID != first.Right.RangeID+1 || meta.Snapshot().NextSplitID != first.SplitID+1 {
		t.Fatalf("duplicate allocated new identity: first=%+v second=%+v snapshot=%+v", first, second, meta.Snapshot())
	}
}

func TestSplitKeepsOrdinaryWritesOnlineAndFencesTransactions(t *testing.T) {
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	catalog, err := NewCatalog(cluster.bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	writesDuringCopy, prepareRejected := 0, false
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) {
		if stage == ParentSplitFenceActive {
			txnValue, beginErr := cluster.router.Begin(context.Background())
			if beginErr != nil {
				t.Errorf("begin during split: %v", beginErr)
				return
			}
			if putErr := txnValue.Put([]byte("b-txn"), []byte("blocked")); putErr != nil {
				t.Errorf("buffer: %v", putErr)
				return
			}
			prepareRejected = errors.Is(txnValue.Commit(context.Background()), replicatedrange.ErrSplitFenced)
		}
		if stage == LeftImageDurable {
			if _, putErr := cluster.router.PutMVCC(context.Background(), []byte("c-hot"), []byte("during-copy")); putErr != nil {
				t.Errorf("online write: %v", putErr)
			} else {
				writesDuringCopy++
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if writesDuringCopy == 0 || !prepareRejected {
		t.Fatalf("writes=%d prepareRejected=%v", writesDuringCopy, prepareRejected)
	}
	left, err := cluster.router.leaderReplica(context.Background(), record.Left)
	if err != nil {
		t.Fatal(err)
	}
	value, err := left.GetAt(context.Background(), []byte("c-hot"), left.Status().MaxAppliedMVCC)
	if err != nil || !bytes.Equal(value, []byte("during-copy")) {
		t.Fatalf("replayed value=%q err=%v", value, err)
	}
	t.Logf("writes_during_copy=%d prepare_rejected=%v writes_lost=0", writesDuringCopy, prepareRejected)
}

func TestPreSplitBufferedTransactionRetriesAndLineageRepeats(t *testing.T) {
	cluster, meta, manager := newSplitHarness(t)
	cluster.elect(10, 1)
	oldTxn, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := oldTxn.Put([]byte("b"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	first, err := manager.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if err := oldTxn.Commit(context.Background()); !errors.Is(err, ErrRangeSplit) {
		t.Fatalf("old transaction=%v", err)
	}
	second, err := manager.SplitRange(context.Background(), first.Left.RangeID, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	descendants, err := meta.ResolveRangeRef(RangeRef{RangeID: 10, Generation: 1})
	if err != nil || len(descendants) != 3 {
		t.Fatalf("deep descendants=%v err=%v", descendants, err)
	}
	if descendants[0].RangeID != second.Left.RangeID || descendants[1].RangeID != second.Right.RangeID || descendants[2].RangeID != first.Right.RangeID {
		t.Fatalf("unexpected lineage leaves: %+v", descendants)
	}
	t.Logf("lineage=10->(%d,%d);%d->(%d,%d) leaves=%d", first.Left.RangeID, first.Right.RangeID,
		first.Left.RangeID, second.Left.RangeID, second.Right.RangeID, len(descendants))
}

func TestSameParentSplitConflictAndDifferentParentsAllocateIndependently(t *testing.T) {
	catalog, err := NewCatalog(threeRangeBootstrap())
	if err != nil {
		t.Fatal(err)
	}
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	first, err := state.begin(RangeRef{RangeID: 10, Generation: 1}, []byte("d"), catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.begin(first.Parent, []byte("e"), catalog.Generation()); !errors.Is(err, ErrSplitInProgress) {
		t.Fatalf("same parent=%v", err)
	}
	other, err := state.begin(RangeRef{RangeID: 11, Generation: 2}, []byte("k"), catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if other.SplitID == first.SplitID || other.Left.RangeID == first.Left.RangeID {
		t.Fatal("allocator reused identity")
	}
}

func TestAbortBeforeFenceRestoresTransactionAdmissionAndParentAuthority(t *testing.T) {
	cluster, meta, manager := newSplitHarness(t)
	cluster.elect(10, 1)
	parent, _ := cluster.router.currentCatalog().LookupByID(10)
	record, err := meta.BeginSplit(context.Background(), RangeRef{RangeID: 10, Generation: 1}, []byte("d"), meta.Snapshot().Catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.proposeOperation(context.Background(), parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBegin, SplitID: uint64(record.SplitID), Epoch: record.Epoch, ParentRangeID: 10, ParentGeneration: 1}, descriptorAnchor(parent)); err != nil {
		t.Fatal(err)
	}
	record, err = manager.AbortSplit(context.Background(), record.SplitID, "injected pre-fence failure")
	if err != nil || record.State != SplitAborted {
		t.Fatalf("abort record=%+v err=%v", record, err)
	}
	if meta.Snapshot().Catalog.Generation() != cluster.bootstrap.Generation {
		t.Fatal("abort changed catalog authority")
	}
	transaction, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte("b"), []byte("after-abort")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatalf("transaction admission not restored: %v", err)
	}
}

func reopenSplitCluster(t *testing.T, cluster *multiTestCluster, snapshot MetadataSnapshot) {
	t.Helper()
	shadow, retired := snapshot.RecoveryRanges()
	transport, err := NewTransport(100_000)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewScheduler(transport)
	if err != nil {
		t.Fatal(err)
	}
	cluster.nodes, cluster.transport, cluster.scheduler = make(map[raft.NodeID]*Node), transport, scheduler
	for _, nodeID := range cluster.bootstrap.Nodes {
		node, openErr := OpenNode(NodeOptions{NodeID: nodeID, Directory: cluster.nodeRoot(nodeID), Bootstrap: &cluster.bootstrap,
			AuthoritativeCatalog: snapshot.Catalog, ShadowRanges: shadow, RetiredRanges: retired, MemTableBytes: 256 + uint64(nodeID)*128,
			MVCC: true, Clock: cluster.clocks[nodeID]})
		if openErr != nil {
			t.Fatalf("reopen node %d: %v", nodeID, openErr)
		}
		cluster.nodes[nodeID] = node
		if err := scheduler.AddNode(node); err != nil {
			t.Fatal(err)
		}
	}
	cluster.router, err = NewRouter(RouterOptions{Catalog: snapshot.Catalog, Scheduler: scheduler, Transport: transport, MaxWork: 50_000})
	if err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range retired {
		cluster.router.InstallRetiredArchive(descriptor)
	}
}

func TestFullClusterRestartMidSplitAndAfterCutover(t *testing.T) {
	cluster, meta, manager := newSplitHarness(t)
	cluster.elect(10, 1)
	if _, err := cluster.router.PutMVCC(context.Background(), []byte("c"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	parent, _ := cluster.router.currentCatalog().LookupByID(10)
	record, err := meta.BeginSplit(context.Background(), RangeRef{RangeID: 10, Generation: 1}, []byte("d"), meta.Snapshot().Catalog.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.proposeOperation(context.Background(), parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBegin, SplitID: uint64(record.SplitID), Epoch: record.Epoch, ParentRangeID: 10, ParentGeneration: 1}, descriptorAnchor(parent)); err != nil {
		t.Fatal(err)
	}
	if err := manager.drainTransactions(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	barrier, err := manager.proposeOperation(context.Background(), parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBootstrapBarrier, SplitID: uint64(record.SplitID), Epoch: record.Epoch, ParentRangeID: 10, ParentGeneration: 1}, descriptorAnchor(parent))
	if err != nil {
		t.Fatal(err)
	}
	record, err = meta.AdvanceSplit(context.Background(), record.SplitID, record.Epoch, SplitCopying, SplitRecord{BootstrapIndex: barrier})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.openChildren(record); err != nil {
		t.Fatal(err)
	}
	cluster.close()
	meta, err = OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap)})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	reopenSplitCluster(t, cluster, meta.Snapshot())
	manager, err = NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) { t.Log(stage) }})
	if err != nil {
		t.Fatal(err)
	}
	record, err = manager.RecoverSplit(context.Background(), record.SplitID)
	if err != nil {
		t.Logf("scheduler failures: %+v", cluster.scheduler.Failures())
		for id, node := range cluster.nodes {
			t.Logf("node %d: %+v", id, node.Status().Ranges)
		}
		t.Fatal(err)
	}
	if record.State != SplitCommitted {
		t.Fatalf("recovered state=%v", record.State)
	}
	cluster.close()
	meta, err = OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap)})
	if err != nil {
		t.Fatal(err)
	}
	if err := meta.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := meta.Snapshot()
	reopenSplitCluster(t, cluster, snapshot)
	cluster.elect(record.Left.RangeID, record.Left.Replicas[0].NodeID)
	left, err := cluster.router.leaderReplica(context.Background(), record.Left)
	if err != nil {
		t.Fatal(err)
	}
	value, err := left.GetAt(context.Background(), []byte("c"), left.Status().MaxAppliedMVCC)
	if err != nil || !bytes.Equal(value, []byte("durable")) {
		t.Fatalf("post-restart value=%q err=%v", value, err)
	}
	cluster.elect(10, parent.Replicas[0].NodeID)
	parentReplica, err := cluster.nodes[parent.Replicas[0].NodeID].Replica(10)
	if err != nil {
		t.Fatal(err)
	}
	if parentReplica.Status().Lifecycle != replicatedrange.LifecycleRetired {
		t.Fatalf("resurrected parent=%v", parentReplica.Status().Lifecycle)
	}
}

func TestParentMetaAndChildLeaderChangesDuringSplit(t *testing.T) {
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	catalog := mustCatalog(t, cluster.bootstrap)
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: catalog})
	if err != nil {
		t.Fatal(err)
	}
	parentChanged, metaChanged, childChanged := false, false, false
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, record SplitRecord) {
		switch stage {
		case MetaSplitRecordDurable:
			old := meta.Leader()
			if old != 0 {
				if stopErr := meta.StopNode(old); stopErr != nil {
					t.Errorf("stop meta leader: %v", stopErr)
				} else {
					metaChanged = true
				}
			}
		case ParentSplitFenceActive:
			cluster.elect(10, 2)
			parentChanged = true
		case ChildQuorumReady:
			cluster.elect(record.Left.RangeID, record.Left.Replicas[0].NodeID)
			childChanged = true
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if record.State != SplitCommitted || !parentChanged || !metaChanged || !childChanged {
		t.Fatalf("state=%v parent=%v meta=%v child=%v", record.State, parentChanged, metaChanged, childChanged)
	}
	t.Logf("parent_leader_change=%v meta_leader_change=%v child_leader_change=%v", parentChanged, metaChanged, childChanged)
}

func TestStaleRouterRefreshesFromCommittedMetadataRedirect(t *testing.T) {
	cluster, meta, manager := newSplitHarness(t)
	cluster.elect(10, 1)
	stale, err := NewRouter(RouterOptions{Catalog: mustCatalog(t, cluster.bootstrap), Scheduler: cluster.scheduler, Transport: cluster.transport, Meta: meta, MaxWork: 50_000})
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.SplitRange(context.Background(), 10, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stale.PutMVCC(context.Background(), []byte("e"), []byte("redirected")); err != nil {
		t.Fatal(err)
	}
	if stale.currentCatalog().Generation() != record.CutoverCatalogGeneration {
		t.Fatalf("stale cache did not refresh")
	}
	descriptor, err := stale.currentCatalog().Lookup([]byte("e"))
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.RangeID != record.Right.RangeID {
		t.Fatalf("routed to %d", descriptor.RangeID)
	}
	t.Logf("stale_parent=10 refresh_generation=%d destination=%d", stale.currentCatalog().Generation(), descriptor.RangeID)
}

func TestPreparedTransactionDrainBlocksOnHomeQuorumThenRecovers(t *testing.T) {
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	ctx, cancel := context.WithCancel(context.Background())
	prepared := 0
	cluster.router.txnHook = func(stage TxnStage, _ txn.ID) {
		if stage == TxnStageParticipantPrepared {
			prepared++
			if prepared == 2 {
				cancel()
			}
		}
	}
	transaction, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	transactionID := transaction.ID()
	if err := transaction.Put([]byte("b"), []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte("h"), []byte("splitting")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(ctx); err == nil || prepared != 2 {
		t.Fatalf("commit=%v prepared=%d", err, prepared)
	}
	for _, peer := range []raft.NodeID{2, 3} {
		cluster.transport.SetRangeLink(10, 1, peer, true)
		cluster.transport.SetRangeLink(10, peer, 1, true)
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap), MaxWork: 2_000})
	if err != nil {
		t.Fatal(err)
	}
	cluster.router.maxWork = 2_000
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router})
	if err != nil {
		t.Fatal(err)
	}
	record, err := manager.SplitRange(context.Background(), 11, []byte("k"))
	if err == nil {
		t.Fatal("split ignored unavailable transaction outcome authority")
	}
	cluster.transport.Heal()
	cluster.router.maxWork = 50_000
	snapshot := meta.Snapshot()
	if len(snapshot.Splits) != 1 {
		t.Fatalf("split records=%d", len(snapshot.Splits))
	}
	record, err = manager.RecoverSplit(context.Background(), snapshot.Splits[0].SplitID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != SplitCommitted {
		t.Fatalf("state=%v", record.State)
	}
	status, err := cluster.router.GetTransactionStatus(context.Background(), transactionID)
	if err != nil || status.Status != txn.StatusAborted {
		t.Fatalf("terminal archive status=%v err=%v", status.Status, err)
	}
}

func TestSplitConstrainedEnvironmentBaseline(t *testing.T) {
	if os.Getenv("RIVETDB_SPLIT_PROFILE") == "" {
		t.Skip("set RIVETDB_SPLIT_PROFILE=1 for the disk-backed engineering baseline")
	}
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	for index := 0; index < 100; index++ {
		key := []byte(fmt.Sprintf("b-%03d", index))
		if _, err := cluster.router.PutMVCC(t.Context(), key, []byte("0123456789abcdef")); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap)})
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[SplitHookStage]time.Time)
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage SplitHookStage, _ SplitRecord) {
		observed[stage] = time.Now()
	}})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := manager.SplitRange(t.Context(), 10, []byte("d")); err != nil {
		t.Fatal(err)
	}
	t.Logf("CONSTRAINED-ENVIRONMENT BASELINE image_plus_left_bootstrap=%s remaining_bootstrap=%s replay_to_fence=%s final_fence_replay=%s metadata_cutover=%s activation_and_retire=%s total_split=%s",
		observed[LeftImageDurable].Sub(observed[BootstrapBarrierDurable]),
		observed[ChildQuorumReady].Sub(observed[LeftImageDurable]),
		observed[FinalFenceDurable].Sub(observed[ChildQuorumReady]),
		observed[ChildrenReplayedThroughFence].Sub(observed[FinalFenceDurable]),
		observed[MetaCutoverDurable].Sub(observed[ChildrenReplayedThroughFence]),
		observed[ParentRetired].Sub(observed[MetaCutoverDurable]),
		observed[ParentRetired].Sub(started))
}

func mustCatalog(t testing.TB, bootstrap Bootstrap) *Catalog {
	t.Helper()
	catalog, err := NewCatalog(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func mustDescriptor(t *testing.T, catalog *Catalog, rangeID RangeID) RangeDescriptor {
	t.Helper()
	descriptor, err := catalog.LookupByID(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func TestSplitSubprocessCrashHelper(t *testing.T) {
	stage := os.Getenv("RIVETDB_SPLIT_HELPER_STAGE")
	if stage == "" {
		t.Skip("split crash helper")
	}
	root := os.Getenv("RIVETDB_SPLIT_HELPER_ROOT")
	bootstrap := threeRangeBootstrap()
	cluster := &multiTestCluster{t: t, root: root, bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true}
	cluster.openRuntime(true)
	cluster.elect(10, 1)
	if _, err := cluster.router.PutMVCC(context.Background(), []byte("c"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: bootstrap.Nodes, Directory: filepath.Join(root, "metadata"), Bootstrap: mustCatalog(t, bootstrap)})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(observed SplitHookStage, _ SplitRecord) {
		if observed == LeftImageDurable {
			_, _ = cluster.router.PutMVCC(context.Background(), []byte("c"), []byte("during"))
		}
		if observed.String() == stage {
			os.Exit(73)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.SplitRange(context.Background(), 10, []byte("d")); err != nil {
		t.Fatal(err)
	}
	os.Exit(74)
}

func TestSplitSubprocessCrashMatrix(t *testing.T) {
	if os.Getenv("RIVETDB_SPLIT_CRASH") == "" {
		t.Skip("set RIVETDB_SPLIT_CRASH=1 for abrupt split crashes")
	}
	stages := []SplitHookStage{
		MetaSplitRecordDurable,
		ParentSplitFenceActive,
		TxnDrainComplete,
		BootstrapBarrierDurable,
		LeftImageDurable,
		RightImageDurable,
		ChildQuorumReady,
		DeltaReplayProgress,
		FinalFenceDurable,
		ChildrenReplayedThroughFence,
		MetaCutoverDurable,
		ChildActivated,
		ParentRetired,
	}
	for _, stage := range stages {
		t.Run(stage.String(), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "cluster")
			command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestSplitSubprocessCrashHelper$") //nolint:gosec // current signed test executable
			command.Env = append(os.Environ(), "RIVETDB_SPLIT_HELPER_STAGE="+stage.String(), "RIVETDB_SPLIT_HELPER_ROOT="+root)
			err := command.Run()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 73 {
				t.Fatalf("helper exit=%v", err)
			}
			bootstrap := threeRangeBootstrap()
			meta, err := OpenMetaRange(MetaRangeOptions{Nodes: bootstrap.Nodes, Directory: filepath.Join(root, "metadata"), Bootstrap: mustCatalog(t, bootstrap)})
			if err != nil {
				t.Fatal(err)
			}
			if err := meta.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot := meta.Snapshot()
			cluster := &multiTestCluster{t: t, root: root, bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true}
			reopenSplitCluster(t, cluster, snapshot)
			t.Cleanup(cluster.close)
			manager, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router})
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.Splits) != 1 {
				t.Fatalf("split records=%d", len(snapshot.Splits))
			}
			record, err := manager.RecoverSplit(context.Background(), snapshot.Splits[0].SplitID)
			if err != nil {
				t.Fatalf("recover %s: %v", stage, err)
			}
			if record.State != SplitCommitted {
				t.Fatalf("state=%v", record.State)
			}
			descriptor, err := cluster.router.currentCatalog().Lookup([]byte("c"))
			if err != nil {
				t.Fatal(err)
			}
			replica, err := cluster.router.leaderReplica(context.Background(), descriptor)
			if err != nil {
				t.Fatal(err)
			}
			value, err := replica.GetAt(context.Background(), []byte("c"), replica.Status().MaxAppliedMVCC)
			want := []byte("during")
			if stage < LeftImageDurable {
				want = []byte("before")
			}
			if err != nil || !bytes.Equal(value, want) {
				t.Fatalf("stage=%s value=%q err=%v", stage, value, err)
			}
			t.Logf("stage=%s exit=73 recovered_split=%d writes_lost=0", stage, record.SplitID)
		})
	}
}
