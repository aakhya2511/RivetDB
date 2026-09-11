package multiraft

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func TestTransactionSubprocessCrashMatrix(t *testing.T) {
	if os.Getenv("RIVETDB_TXN_CRASH") == "" {
		t.Skip("set RIVETDB_TXN_CRASH=1 for transaction subprocess crashes")
	}
	if root := os.Getenv("RIVETDB_TXN_CRASH_CHILD"); root != "" {
		runTransactionCrashChild(t, root, os.Getenv("RIVETDB_TXN_CRASH_STAGE"))
		t.Fatal("transaction crash stage was not reached")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	stages := []struct {
		name      string
		committed bool
	}{
		{"PENDING", false}, {"ONE_PREPARED", false}, {"ALL_PREPARED", false},
		{"COMMIT_DECISION", true}, {"ONE_RESOLVED", true}, {"ALL_RESOLVED", true},
		{"ABORT_DECISION", false},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			root := t.TempDir()
			command := exec.CommandContext(context.Background(), executable, "-test.run=^TestTransactionSubprocessCrashMatrix$") //nolint:gosec // current test binary
			command.Env = append(os.Environ(), "RIVETDB_TXN_CRASH=1", "RIVETDB_TXN_CRASH_CHILD="+root, "RIVETDB_TXN_CRASH_STAGE="+stage.name)
			assertExit77(t, command.Run())
			cluster := crashTransactionCluster(t, root, false)
			defer cluster.close()
			cluster.elect(10, 2)
			cluster.elect(11, 4)
			cluster.elect(12, 3)
			if err := cluster.router.RecoverTransactions(context.Background()); err != nil {
				t.Fatal(err)
			}
			record, err := cluster.router.GetTransactionStatus(context.Background(), crashTxnID())
			if err != nil {
				t.Fatal(err)
			}
			want := txn.StatusAborted
			if stage.committed {
				want = txn.StatusCommitted
			}
			if record.Status != want {
				t.Fatalf("status=%d want=%d record=%+v", record.Status, want, record)
			}
			reader, err := cluster.router.Begin(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = reader.Abort(context.Background()) }()
			wantValues := map[string]string{"a-account": "1000", "g-account": "500"}
			if stage.committed {
				wantValues = map[string]string{"a-account": "800", "g-account": "700"}
			}
			for key, wantValue := range wantValues {
				value, getErr := reader.Get(context.Background(), []byte(key))
				if getErr != nil || string(value) != wantValue {
					t.Fatalf("%s=%q %v want=%q", key, value, getErr, wantValue)
				}
			}
		})
	}
}

func runTransactionCrashChild(t *testing.T, root, target string) {
	cluster := crashTransactionCluster(t, root, true)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	cluster.router.txnIDGenerator = func() (txn.ID, error) { return crashTxnID(), nil }
	prepared, resolved := 0, 0
	cluster.router.txnHook = func(stage TxnStage, _ txn.ID) {
		match := stage == TxnStagePendingDurable && target == "PENDING" ||
			stage == TxnStageAllPrepared && target == "ALL_PREPARED" ||
			stage == TxnStageCommitDecisionDurable && target == "COMMIT_DECISION" ||
			stage == TxnStageAbortDecisionDurable && target == "ABORT_DECISION" ||
			stage == TxnStageAllResolved && target == "ALL_RESOLVED"
		if stage == TxnStageParticipantPrepared {
			prepared++
			match = target == "ONE_PREPARED" && prepared == 1
		}
		if stage == TxnStageParticipantResolved {
			resolved++
			match = target == "ONE_RESOLVED" && resolved == 1
		}
		if match {
			os.Exit(77)
		}
	}
	if _, err := cluster.router.PutMVCC(context.Background(), []byte("a-account"), []byte("1000")); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.router.PutMVCC(context.Background(), []byte("g-account"), []byte("500")); err != nil {
		t.Fatal(err)
	}
	transaction, err := cluster.router.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte("a-account"), []byte("800")); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Put([]byte("g-account"), []byte("700")); err != nil {
		t.Fatal(err)
	}
	if target == "ABORT_DECISION" {
		if err := transaction.Put([]byte("a-conflict"), []byte("provisional")); err != nil {
			t.Fatal(err)
		}
		if _, err := cluster.router.PutMVCC(context.Background(), []byte("a-conflict"), []byte("conflict")); err != nil {
			t.Fatal(err)
		}
	}
	_ = transaction.Commit(context.Background())
}

func crashTransactionCluster(t testing.TB, root string, bootstrap bool) *multiTestCluster {
	t.Helper()
	configuration := threeRangeBootstrap()
	cluster := &multiTestCluster{t: t, root: root, bootstrap: configuration, nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, nodeID := range configuration.Nodes {
		cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(20_000 + int64(nodeID)*1_000))
	}
	cluster.openRuntime(bootstrap)
	return cluster
}

func crashTxnID() txn.ID {
	var id txn.ID
	copy(id[:], []byte("phase6-crash-id!"))
	return id
}
