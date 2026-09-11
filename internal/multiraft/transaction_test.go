package multiraft

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func transactionCluster(t testing.TB) *multiTestCluster {
	t.Helper()
	cluster := newMVCCMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	return cluster
}

func TestTransactionReadYourWritesScanAndReadOnly(t *testing.T) {
	cluster := transactionCluster(t)
	for key, value := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if _, err := cluster.router.PutMVCC(context.Background(), []byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	transaction, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if err := transaction.Put([]byte("b"), []byte("20")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte("d"), []byte("4")); err != nil {
		t.Fatal(err)
	}
	value, getErr := transaction.Get(context.Background(), []byte("b"))
	if getErr != nil || string(value) != "20" {
		t.Fatalf("Get(b)=%q %v", value, getErr)
	}
	if _, err := transaction.Get(context.Background(), []byte("c")); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Get(c)=%v", err)
	}
	rows, scanErr := transaction.Scan(context.Background(), []byte("a"), []byte("e"))
	if scanErr != nil {
		t.Fatal(scanErr)
	}
	if got := kvStrings(rows); len(got) != 3 || got[0] != "a=1" || got[1] != "b=20" || got[2] != "d=4" {
		t.Fatalf("scan=%v", got)
	}
	if err := transaction.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	readOnly, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := readOnly.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.router.GetTransactionStatus(context.Background(), readOnly.ID()); !errors.Is(err, txn.ErrInvalid) {
		t.Fatalf("read-only record=%v", err)
	}
}

func TestTwoAndThreeRangeTransactionsUseOneCommitTimestamp(t *testing.T) {
	cluster := transactionCluster(t)
	transaction, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	for key, value := range map[string]string{"a": "A", "g": "G", "p": "P"} {
		if err := transaction.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := cluster.router.GetTransactionStatus(context.Background(), transaction.ID())
	if err != nil || record.Status != txn.StatusCommitted || record.CommitTime <= record.ReadTime || len(record.Participants) != 3 {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	for _, participant := range record.Participants {
		descriptor, _ := cluster.bootstrapCatalog().LookupByID(RangeID(participant.RangeID))
		replica := leaderReplicaForTest(t, cluster, descriptor)
		state, ok := replica.ParticipantRecord(transaction.ID())
		if !ok || state.Status != txn.ParticipantCommitted || state.CommitTime != record.CommitTime {
			t.Fatalf("participant=%+v ok=%v", state, ok)
		}
	}
	for _, key := range []string{"a", "g", "p"} {
		value, err := cluster.router.transactionGetAt(context.Background(), []byte(key), mvccTimestamp(record.CommitTime))
		if err != nil || string(value) != string([]byte{key[0] - 32}) {
			t.Fatalf("%s=%q %v", key, value, err)
		}
	}
}

func TestSnapshotIsolationConflictAndAllowedWriteSkew(t *testing.T) {
	cluster := transactionCluster(t)
	for _, key := range []string{"a", "b"} {
		if _, err := cluster.router.PutMVCC(context.Background(), []byte(key), []byte("1")); err != nil {
			t.Fatal(err)
		}
	}
	first, _ := cluster.router.Begin(context.Background())
	second, _ := cluster.router.Begin(context.Background())
	_ = first.Put([]byte("a"), []byte("first"))
	_ = second.Put([]byte("a"), []byte("second"))
	if err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(context.Background()); !errors.Is(err, txn.ErrWriteConflict) {
		t.Fatalf("second conflict=%v", err)
	}

	left, _ := cluster.router.Begin(context.Background())
	right, _ := cluster.router.Begin(context.Background())
	for _, current := range []*Transaction{left, right} {
		if _, err := current.Get(context.Background(), []byte("a")); err != nil {
			t.Fatal(err)
		}
		if _, err := current.Get(context.Background(), []byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	_ = left.Put([]byte("a"), []byte("0"))
	_ = right.Put([]byte("b"), []byte("0"))
	if err := left.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := right.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Both commits are legal because their write sets are disjoint. This is the
	// classic write-skew anomaly and proves the API is not serializable.
}

func TestPendingPreparedTransactionRecoversByEpochFencedAbort(t *testing.T) {
	cluster := transactionCluster(t)
	transaction, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	id, readTime := transaction.ID(), transaction.ReadTimestamp()
	home, _ := cluster.bootstrapCatalog().LookupByID(10)
	other, _ := cluster.bootstrapCatalog().LookupByID(11)
	commitTime, timestampErr := cluster.router.assignTransactionTimestamp(context.Background(), home, []byte("a"), readTime)
	if timestampErr != nil {
		t.Fatal(timestampErr)
	}
	homeID := txn.Participant{RangeID: 10, Generation: home.Generation}
	participants := []txn.Participant{homeID, {RangeID: 11, Generation: other.Generation}}
	create := txn.Operation{Type: txn.OpCreate, ID: id, ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Participants: participants}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a"), replicatedrange.CommandTxnCreate, create); err != nil {
		t.Fatal(err)
	}
	prepare := txn.Operation{Type: txn.OpPrepare, ID: id, ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Writes: []txn.Write{{Key: []byte("a"), Value: []byte("provisional")}}}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a"), replicatedrange.CommandTxnPrepare, prepare); err != nil {
		t.Fatal(err)
	}
	cluster.close()
	cluster.openRuntime(false)
	cluster.elect(10, 2)
	cluster.elect(11, 4)
	cluster.elect(12, 3)
	if err := cluster.router.RecoverTransactions(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, statusErr := cluster.router.GetTransactionStatus(context.Background(), id)
	if statusErr != nil || record.Status != txn.StatusAborted || record.Epoch != 2 {
		t.Fatalf("recovered record=%+v err=%v", record, statusErr)
	}
	if _, err := cluster.router.transactionGetAt(context.Background(), []byte("a"), commitTime); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("aborted intent visible: %v", err)
	}
}

func TestManyParticipantTransactionAndConcurrentConflicts(t *testing.T) {
	configuration := manyRangeBootstrap(11)
	cluster := &multiTestCluster{t: t, root: t.TempDir(), bootstrap: configuration, nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, nodeID := range configuration.Nodes {
		cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(30_000 + int64(nodeID)))
	}
	cluster.openRuntime(true)
	t.Cleanup(cluster.close)
	for _, descriptor := range configuration.Ranges {
		cluster.elect(descriptor.RangeID, 1)
	}
	transaction, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	for index := range 11 {
		key := []byte{byte('a' + index)}
		if err := transaction.Put(key, []byte(fmt.Sprintf("v%d", index))); err != nil {
			t.Fatal(err)
		}
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := cluster.router.GetTransactionStatus(context.Background(), transaction.ID())
	if err != nil || record.Status != txn.StatusCommitted || len(record.Participants) != 11 {
		t.Fatalf("many participant record=%+v %v", record, err)
	}

	const contenders = 12
	transactions := make([]*Transaction, contenders)
	for index := range transactions {
		var transactionErr error
		transactions[index], transactionErr = cluster.router.Begin(context.Background())
		if transactionErr != nil {
			t.Fatal(transactionErr)
		}
		if err := transactions[index].Put([]byte("a-hot"), []byte{byte(index)}); err != nil {
			t.Fatal(err)
		}
	}
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	wait.Add(contenders)
	for _, current := range transactions {
		go func() { defer wait.Done(); results <- current.Commit(context.Background()) }()
	}
	wait.Wait()
	close(results)
	winners := 0
	for commitErr := range results {
		if commitErr == nil {
			winners++
			continue
		}
		if !errors.Is(commitErr, txn.ErrWriteConflict) && !errors.Is(commitErr, txn.ErrAlreadyAborted) {
			t.Fatalf("unexpected conflict result=%v", commitErr)
		}
	}
	if winners != 1 {
		t.Fatalf("hot-key winners=%d, want 1", winners)
	}
}

func TestConcurrentMixedSingleAndMultiRangeTransactions(t *testing.T) {
	cluster := transactionCluster(t)
	type candidate struct {
		transaction *Transaction
		hot         bool
	}
	const perClass = 6
	candidates := make([]candidate, 0, perClass*4)
	for class := range 4 {
		for index := range perClass {
			current, err := cluster.router.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var writes []string
			switch class {
			case 0:
				writes = []string{"a-mixed-hot"}
			case 1:
				writes = []string{fmt.Sprintf("b-mixed-%02d", index)}
			case 2:
				writes = []string{fmt.Sprintf("c-mixed-%02d", index), fmt.Sprintf("g-mixed-%02d", index)}
			default:
				writes = []string{fmt.Sprintf("d-mixed-%02d", index), fmt.Sprintf("h-mixed-%02d", index), fmt.Sprintf("p-mixed-%02d", index)}
			}
			for _, key := range writes {
				if err := current.Put([]byte(key), []byte(key)); err != nil {
					t.Fatal(err)
				}
			}
			candidates = append(candidates, candidate{transaction: current, hot: class == 0})
		}
	}

	results := make(chan struct {
		hot bool
		err error
	}, len(candidates))
	var wait sync.WaitGroup
	wait.Add(len(candidates))
	for _, candidate := range candidates {
		go func() {
			defer wait.Done()
			results <- struct {
				hot bool
				err error
			}{candidate.hot, candidate.transaction.Commit(context.Background())}
		}()
	}
	wait.Wait()
	close(results)
	hotWinners, disjointWinners := 0, 0
	for result := range results {
		if result.err == nil {
			if result.hot {
				hotWinners++
			} else {
				disjointWinners++
			}
			continue
		}
		if !result.hot || !errors.Is(result.err, txn.ErrWriteConflict) && !errors.Is(result.err, txn.ErrAlreadyAborted) {
			t.Fatalf("mixed concurrent result hot=%v err=%v", result.hot, result.err)
		}
	}
	if hotWinners != 1 || disjointWinners != perClass*3 {
		t.Fatalf("mixed winners hot=%d disjoint=%d", hotWinners, disjointWinners)
	}
}

func TestTransactionLeaderChangesAndRangeFailureIsolation(t *testing.T) {
	cluster := transactionCluster(t)
	transaction, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	for _, key := range []string{"a-leader", "g-leader", "p-leader"} {
		if err := transaction.Put([]byte(key), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	prepared := 0
	cluster.router.txnHook = func(stage TxnStage, _ txn.ID) {
		if stage != TxnStageParticipantPrepared {
			return
		}
		prepared++
		if prepared == 1 {
			cluster.elect(10, 2)
		}
		if prepared == 2 {
			cluster.elect(12, 3)
		}
	}
	if err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	cluster.router.txnHook = nil

	for _, from := range cluster.bootstrap.Nodes {
		for _, to := range cluster.bootstrap.Nodes {
			if from != to {
				cluster.transport.SetRangeLink(11, from, to, true)
			}
		}
	}
	oldWork := cluster.router.maxWork
	cluster.router.maxWork = 300
	unaffected, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if err := unaffected.Put([]byte("p-unaffected"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := unaffected.Commit(context.Background()); err != nil {
		t.Fatalf("unaffected transaction=%v", err)
	}
	affected, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if err := affected.Put([]byte("g-unavailable"), []byte("blocked")); err != nil {
		t.Fatal(err)
	}
	if err := affected.Commit(context.Background()); err == nil {
		t.Fatal("transaction on unavailable range committed")
	}
	cluster.router.maxWork = oldWork
	cluster.transport.Heal()
}

func TestTransactionIdempotencyAuthorityAndStaleCoordinatorFence(t *testing.T) {
	cluster := transactionCluster(t)
	committed, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if err := committed.Put([]byte("a-idempotent"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(context.Background()); err != nil {
		t.Fatalf("commit retry=%v", err)
	}
	if err := committed.Abort(context.Background()); !errors.Is(err, txn.ErrAlreadyCommitted) {
		t.Fatalf("abort after commit=%v", err)
	}
	record, statusErr := cluster.router.GetTransactionStatus(context.Background(), committed.ID())
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if err := cluster.router.resolveRecord(context.Background(), record); err != nil {
		t.Fatalf("resolve retry=%v", err)
	}

	pending, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	home, _ := cluster.bootstrapCatalog().LookupByID(10)
	commitTime, timestampErr := cluster.router.assignTransactionTimestamp(context.Background(), home, []byte("a-fence"), pending.ReadTimestamp())
	if timestampErr != nil {
		t.Fatal(timestampErr)
	}
	homeID := txn.Participant{RangeID: 10, Generation: home.Generation}
	create := txn.Operation{Type: txn.OpCreate, ID: pending.ID(), ReadTime: uint64(pending.ReadTimestamp()), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Participants: []txn.Participant{homeID}}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-fence"), replicatedrange.CommandTxnCreate, create); err != nil {
		t.Fatal(err)
	}
	takeover := txn.Operation{Type: txn.OpTakeover, ID: create.ID, ReadTime: create.ReadTime, CommitTime: create.CommitTime, Epoch: 2, Home: homeID}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-fence"), replicatedrange.CommandTxnTakeover, takeover); err != nil {
		t.Fatal(err)
	}
	stalePrepare := txn.Operation{Type: txn.OpPrepare, ID: create.ID, ReadTime: create.ReadTime, CommitTime: create.CommitTime, Epoch: 1, Home: homeID, Writes: []txn.Write{{Key: []byte("a-fence"), Value: []byte("stale")}}}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-fence"), replicatedrange.CommandTxnPrepare, stalePrepare); !errors.Is(err, txn.ErrProtocolConflict) {
		t.Fatalf("stale prepare=%v", err)
	}
	abort := txn.Operation{Type: txn.OpAbort, ID: create.ID, ReadTime: create.ReadTime, CommitTime: create.CommitTime, Epoch: 2, Home: homeID}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-fence"), replicatedrange.CommandTxnAbort, abort); err != nil {
		t.Fatal(err)
	}
	staleCommit := txn.Operation{Type: txn.OpCommit, ID: create.ID, ReadTime: create.ReadTime, CommitTime: create.CommitTime, Epoch: 1, Home: homeID}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-fence"), replicatedrange.CommandTxnCommit, staleCommit); !errors.Is(err, txn.ErrProtocolConflict) && !errors.Is(err, txn.ErrAlreadyAborted) {
		t.Fatalf("stale commit=%v", err)
	}
	record, statusErr = cluster.router.GetTransactionStatus(context.Background(), create.ID)
	if statusErr != nil || record.Status != txn.StatusAborted || record.Epoch != 2 {
		t.Fatalf("fenced record=%+v %v", record, statusErr)
	}
	if err := cluster.router.resolveRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := cluster.router.resolveRecord(context.Background(), record); err != nil {
		t.Fatalf("abort resolution retry=%v", err)
	}

	wrong := stalePrepare
	wrong.Epoch = 2
	wrong.Writes = []txn.Write{{Key: []byte("g-wrong"), Value: []byte("x")}}
	encoded, encodeErr := txn.EncodeOperation(wrong)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	_, _, wrongRangeErr := cluster.nodes[2].ProposeTransaction(context.Background(), Route{RangeID: 10, Generation: 1, Key: []byte("a-fence")}, replicatedrange.Command{Type: replicatedrange.CommandTxnPrepare, Key: []byte("a-fence"), Value: encoded, Timestamp: commitTime})
	if !errors.Is(wrongRangeErr, ErrWrongRangeKey) && !errors.Is(wrongRangeErr, replicatedrange.ErrKeyOutOfRange) {
		t.Fatalf("wrong-range prepare=%v", wrongRangeErr)
	}
}

func TestCommitTimeoutAfterDecisionRetriesByIdentity(t *testing.T) {
	cluster := transactionCluster(t)
	current, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	if err := current.Put([]byte("a-timeout"), []byte("left")); err != nil {
		t.Fatal(err)
	}
	if err := current.Put([]byte("g-timeout"), []byte("right")); err != nil {
		t.Fatal(err)
	}
	commitContext, cancel := context.WithCancel(context.Background())
	cluster.router.txnHook = func(stage TxnStage, id txn.ID) {
		if id == current.ID() && stage == TxnStageCommitDecisionDurable {
			cancel()
		}
	}
	commitErr := current.Commit(commitContext)
	cluster.router.txnHook = nil
	if commitErr == nil {
		t.Fatal("commit returned success after its wait was canceled")
	}
	record, err := cluster.router.GetTransactionStatus(context.Background(), current.ID())
	if err != nil || record.Status != txn.StatusCommitted || record.CommitTime == 0 {
		t.Fatalf("ambiguous commit status=%+v err=%v", record, err)
	}
	if err := current.Commit(context.Background()); err != nil {
		t.Fatalf("commit identity retry=%v", err)
	}
	if current.CommitTimestamp() != mvccTimestamp(record.CommitTime) {
		t.Fatalf("retry CT=%d want=%d", current.CommitTimestamp(), record.CommitTime)
	}
	for key, want := range map[string]string{"a-timeout": "left", "g-timeout": "right"} {
		value, getErr := cluster.router.transactionGetAt(context.Background(), []byte(key), mvccTimestamp(record.CommitTime))
		if getErr != nil || string(value) != want {
			t.Fatalf("retry %s=%q %v", key, value, getErr)
		}
	}
}

func TestTransactionIntentCorruptionIsNeverUserData(t *testing.T) {
	cluster := transactionCluster(t)
	_, err := cluster.router.interpretEntry(context.Background(), engine.MVCCEntry{
		Key: []byte("a-corrupt"), Value: []byte("not-an-intent"), Kind: storage.KindIntent, Timestamp: 10,
	}, 10)
	if !errors.Is(err, engine.ErrCorruption) {
		t.Fatalf("corrupt intent=%v", err)
	}
}

func TestFullClusterRestartRecoversMixedTransactionStates(t *testing.T) {
	cluster := transactionCluster(t)
	type staged struct {
		id     txn.ID
		status txn.Status
		keys   []string
	}
	var cases []staged
	stage := func(label string, prepareCount int, decision txn.Status, resolveCount int) {
		current, err := cluster.router.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		readTime := current.ReadTimestamp()
		keys := []string{"a-" + label, "g-" + label}
		home, _ := cluster.bootstrapCatalog().LookupByID(10)
		middle, _ := cluster.bootstrapCatalog().LookupByID(11)
		commitTime, err := cluster.router.assignTransactionTimestamp(context.Background(), home, []byte(keys[0]), readTime)
		if err != nil {
			t.Fatal(err)
		}
		homeID := txn.Participant{RangeID: 10, Generation: home.Generation}
		participants := []txn.Participant{homeID, {RangeID: 11, Generation: middle.Generation}}
		create := txn.Operation{Type: txn.OpCreate, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Participants: participants}
		if err := cluster.router.proposeOperation(context.Background(), home, []byte(keys[0]), replicatedrange.CommandTxnCreate, create); err != nil {
			t.Fatal(err)
		}
		for index := 0; index < prepareCount; index++ {
			descriptor := []RangeDescriptor{home, middle}[index]
			prepare := txn.Operation{Type: txn.OpPrepare, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID,
				Writes: []txn.Write{{Key: []byte(keys[index]), Value: []byte("value-" + label)}}}
			if err := cluster.router.proposeOperation(context.Background(), descriptor, []byte(keys[index]), replicatedrange.CommandTxnPrepare, prepare); err != nil {
				t.Fatal(err)
			}
		}
		if decision.Terminal() {
			commandType, operationType := replicatedrange.CommandTxnAbort, txn.OpAbort
			if decision == txn.StatusCommitted {
				commandType, operationType = replicatedrange.CommandTxnCommit, txn.OpCommit
			}
			operation := txn.Operation{Type: operationType, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID}
			if err := cluster.router.proposeOperation(context.Background(), home, []byte(keys[0]), commandType, operation); err != nil {
				t.Fatal(err)
			}
			for index := 0; index < resolveCount; index++ {
				commandType, operationType := replicatedrange.CommandTxnResolveAbort, txn.OpResolveAbort
				if decision == txn.StatusCommitted {
					commandType, operationType = replicatedrange.CommandTxnResolveCommit, txn.OpResolveCommit
				}
				descriptor := []RangeDescriptor{home, middle}[index]
				operation := txn.Operation{Type: operationType, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID}
				if err := cluster.router.proposeOperation(context.Background(), descriptor, descriptorAnchor(descriptor), commandType, operation); err != nil {
					t.Fatal(err)
				}
			}
		}
		want := decision
		if !want.Terminal() {
			want = txn.StatusAborted
		}
		cases = append(cases, staged{id: current.ID(), status: want, keys: keys})
	}
	stage("pending", 0, txn.StatusPending, 0)
	stage("partial", 1, txn.StatusPending, 0)
	stage("prepared", 2, txn.StatusPending, 0)
	stage("committed", 2, txn.StatusCommitted, 1)
	stage("aborted", 2, txn.StatusAborted, 1)
	cluster.close()
	cluster.openRuntime(false)
	cluster.elect(10, 2)
	cluster.elect(11, 4)
	cluster.elect(12, 3)
	if err := cluster.router.RecoverTransactions(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Abort(context.Background()) }()
	for _, testCase := range cases {
		record, err := cluster.router.GetTransactionStatus(context.Background(), testCase.id)
		if err != nil || record.Status != testCase.status {
			t.Fatalf("record %x=%+v err=%v want=%d", testCase.id, record, err, testCase.status)
		}
		for _, key := range testCase.keys {
			value, getErr := reader.Get(context.Background(), []byte(key))
			if testCase.status == txn.StatusCommitted {
				if getErr != nil || string(value) != "value-committed" {
					t.Fatalf("committed %s=%q %v", key, value, getErr)
				}
			} else if !errors.Is(getErr, engine.ErrNotFound) {
				t.Fatalf("aborted %s=%q %v", key, value, getErr)
			}
		}
	}
}

func TestCommittedUnresolvedIntentsAreLogicallyAtomic(t *testing.T) {
	cluster := transactionCluster(t)
	current, beginErr := cluster.router.Begin(context.Background())
	if beginErr != nil {
		t.Fatal(beginErr)
	}
	readTime := current.ReadTimestamp()
	home, _ := cluster.bootstrapCatalog().LookupByID(10)
	middle, _ := cluster.bootstrapCatalog().LookupByID(11)
	commitTime, timestampErr := cluster.router.assignTransactionTimestamp(context.Background(), home, []byte("a-logical"), readTime)
	if timestampErr != nil {
		t.Fatal(timestampErr)
	}
	homeID := txn.Participant{RangeID: 10, Generation: 1}
	participants := []txn.Participant{homeID, {RangeID: 11, Generation: 2}}
	create := txn.Operation{Type: txn.OpCreate, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Participants: participants}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-logical"), replicatedrange.CommandTxnCreate, create); err != nil {
		t.Fatal(err)
	}
	for index, descriptor := range []RangeDescriptor{home, middle} {
		key := []byte([]string{"a-logical", "g-logical"}[index])
		prepare := txn.Operation{Type: txn.OpPrepare, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID, Writes: []txn.Write{{Key: key, Value: []byte("visible")}}}
		if err := cluster.router.proposeOperation(context.Background(), descriptor, key, replicatedrange.CommandTxnPrepare, prepare); err != nil {
			t.Fatal(err)
		}
	}
	commit := txn.Operation{Type: txn.OpCommit, ID: current.ID(), ReadTime: uint64(readTime), CommitTime: uint64(commitTime), Epoch: 1, Home: homeID}
	if err := cluster.router.proposeOperation(context.Background(), home, []byte("a-logical"), replicatedrange.CommandTxnCommit, commit); err != nil {
		t.Fatal(err)
	}
	reader, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Abort(context.Background()) }()
	for _, key := range []string{"a-logical", "g-logical"} {
		value, err := reader.Get(context.Background(), []byte(key))
		if err != nil || string(value) != "visible" {
			t.Fatalf("logical committed %s=%q %v", key, value, err)
		}
	}
	replica := leaderReplicaForTest(t, cluster, home)
	if _, err := replica.GetAt(context.Background(), []byte("a-logical"), commitTime); !errors.Is(err, engine.ErrUnresolvedIntent) {
		t.Fatalf("ordinary unresolved read=%v", err)
	}
	record, _ := cluster.router.GetTransactionStatus(context.Background(), current.ID())
	if err := cluster.router.resolveRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func TestMultiAccountTransferPreservesTotal(t *testing.T) {
	cluster := transactionCluster(t)
	accounts := []string{"a-bank", "b-bank", "g-bank", "h-bank", "p-bank", "q-bank"}
	for _, key := range accounts {
		if _, err := cluster.router.PutMVCC(context.Background(), []byte(key), []byte("250")); err != nil {
			t.Fatal(err)
		}
	}
	for round := range 20 {
		from, to := accounts[round%len(accounts)], accounts[(round+3)%len(accounts)]
		current, err := cluster.router.Begin(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fromValue, err := current.Get(context.Background(), []byte(from))
		if err != nil {
			t.Fatal(err)
		}
		toValue, err := current.Get(context.Background(), []byte(to))
		if err != nil {
			t.Fatal(err)
		}
		fromBalance, _ := strconv.Atoi(string(fromValue))
		toBalance, _ := strconv.Atoi(string(toValue))
		if err := current.Put([]byte(from), []byte(strconv.Itoa(fromBalance-7))); err != nil {
			t.Fatal(err)
		}
		if err := current.Put([]byte(to), []byte(strconv.Itoa(toBalance+7))); err != nil {
			t.Fatal(err)
		}
		if err := current.Commit(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Abort(context.Background()) }()
	total := 0
	for _, key := range accounts {
		value, err := reader.Get(context.Background(), []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		balance, err := strconv.Atoi(string(value))
		if err != nil {
			t.Fatal(err)
		}
		total += balance
	}
	if total != 1500 {
		t.Fatalf("bank total=%d, want 1500", total)
	}
}

func manyRangeBootstrap(count int) Bootstrap {
	ranges := make([]RangeDescriptor, count)
	for index := range count {
		start, end := KeyBound{Unbounded: index == 0}, KeyBound{Unbounded: index == count-1}
		if index != 0 {
			start.Key = []byte{byte('a' + index)}
		}
		if index != count-1 {
			end.Key = []byte{byte('a' + index + 1)}
		}
		ranges[index] = RangeDescriptor{RangeID: RangeID(100 + index), Generation: 1, StartKey: start, EndKey: end,
			Replicas: []ReplicaDescriptor{{ReplicaID: 1, NodeID: 1}, {ReplicaID: 2, NodeID: 2}, {ReplicaID: 3, NodeID: 3}}}
	}
	return Bootstrap{Generation: 1, Nodes: []raft.NodeID{1, 2, 3}, ReplicationFactor: 3, Ranges: ranges}
}

func (c *multiTestCluster) bootstrapCatalog() *Catalog { return c.nodes[firstNode(c)].Catalog() }

func firstNode(c *multiTestCluster) raft.NodeID {
	for id := range c.nodes {
		return id
	}
	return 0
}

func leaderReplicaForTest(t testing.TB, cluster *multiTestCluster, descriptor RangeDescriptor) *replicatedrange.Replica {
	t.Helper()
	replica, err := cluster.router.leaderReplica(context.Background(), descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return replica
}

func kvStrings(values []engine.KV) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = string(value.Key) + "=" + string(value.Value)
	}
	return result
}

func mvccTimestamp(value uint64) mvcc.Timestamp { return mvcc.Timestamp(value) }
