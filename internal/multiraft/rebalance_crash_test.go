package multiraft

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

const rebalanceCrashExit = 91

func TestRebalanceControllerSubprocessCrashMatrix(t *testing.T) {
	if os.Getenv("RIVETDB_REBALANCE_CRASH") == "" {
		t.Skip("set RIVETDB_REBALANCE_CRASH=1 for abrupt controller crashes")
	}
	if stage := os.Getenv("RIVETDB_REBALANCE_CRASH_STAGE"); stage != "" {
		runRebalanceCrashChild(t, stage)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"MOVE_ACTION_DURABLE", "MOVE_SUBMITTED", "MOVE_COMPLETED", "SPLIT_SUBMITTED", "SPLIT_COMPLETED"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			command := exec.CommandContext(context.Background(), executable, "-test.run=^TestRebalanceControllerSubprocessCrashMatrix$")
			command.Env = append(os.Environ(), "RIVETDB_REBALANCE_CRASH=1", "RIVETDB_REBALANCE_CRASH_STAGE="+stage, "RIVETDB_REBALANCE_CRASH_ROOT="+root)
			output, runErr := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != rebalanceCrashExit {
				t.Fatalf("stage=%s err=%v output=%s", stage, runErr, output)
			}
			cluster := openMigrationClusterAt(t, root, threeRangeBootstrap())
			defer cluster.close()
			meta, openErr := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap)})
			if openErr != nil {
				t.Fatal(openErr)
			}
			if syncErr := meta.Sync(t.Context()); syncErr != nil {
				t.Fatal(syncErr)
			}
			policy := crashPolicy(stage)
			controller, createErr := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: clock.NewMock(), Policy: policy})
			if createErr != nil {
				t.Fatal(createErr)
			}
			if _, activateErr := controller.Activate(t.Context()); activateErr != nil {
				t.Fatal(activateErr)
			}
			if reconcileErr := controller.Reconcile(t.Context()); reconcileErr != nil {
				t.Fatal(reconcileErr)
			}
			snapshot := meta.Snapshot()
			if len(snapshot.Rebalance.History) != 1 || snapshot.Rebalance.History[0].State != RebalanceActionSucceeded {
				t.Fatalf("stage=%s history=%+v", stage, snapshot.Rebalance.History)
			}
			if len(snapshot.Migrations) > 1 || len(snapshot.Splits) > 1 {
				t.Fatalf("stage=%s migrations=%d splits=%d", stage, len(snapshot.Migrations), len(snapshot.Splits))
			}
			t.Logf("stage=%s actions=1 duplicates=0 state=SUCCEEDED", stage)
		})
	}
}

func runRebalanceCrashChild(t *testing.T, stage string) {
	root := os.Getenv("RIVETDB_REBALANCE_CRASH_ROOT")
	cluster := openMigrationClusterAt(t, root, threeRangeBootstrap())
	cluster.elect(10, 1)
	meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(root, "metadata"), Bootstrap: mustCatalog(t, cluster.bootstrap)})
	if err != nil {
		t.Fatal(err)
	}
	migration, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router, Hook: func(observed MigrationHookStage, _ MigrationRecord) {
		if stage == "MOVE_SUBMITTED" && observed == MigrationRecordDurable {
			os.Exit(rebalanceCrashExit)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	split, err := NewSplitManager(SplitManagerOptions{Meta: meta, Router: cluster.router, Hook: func(observed SplitHookStage, _ SplitRecord) {
		if stage == "SPLIT_SUBMITTED" && observed == MetaSplitRecordDurable {
			os.Exit(rebalanceCrashExit)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	policy := crashPolicy(stage)
	fake := clock.NewMock()
	controller, err := NewRebalanceController(RebalanceControllerOptions{Meta: meta, Router: cluster.router, Clock: fake, Policy: policy, Migration: migration, Split: split,
		Hook: func(observed RebalanceHookStage, _ RebalanceActionRecord) {
			if stage == "MOVE_ACTION_DURABLE" && observed == RebalanceActionDurable || (stage == "MOVE_COMPLETED" || stage == "SPLIT_COMPLETED") && observed == RebalanceCertifiedOperationReturned {
				os.Exit(rebalanceCrashExit)
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Activate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if policy.Moves {
		if _, err := controller.Observe(t.Context()); err != nil {
			t.Fatal(err)
		}
		for sample := 0; sample < int(policy.MinSamples); sample++ {
			for write := 0; write < 5; write++ {
				if _, err := cluster.router.PutMVCC(t.Context(), []byte{byte('a' + sample), byte('0' + write)}, []byte("hot")); err != nil {
					t.Fatal(err)
				}
			}
			fake.Advance(time.Second)
			if _, err := controller.Observe(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		for _, key := range []string{"a", "c", "e"} {
			if _, err := cluster.router.PutMVCC(t.Context(), []byte(key), []byte("large")); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, _ = controller.RunCycle(t.Context())
	os.Exit(92)
}

func crashPolicy(stage string) RebalancePolicy {
	if len(stage) >= 5 && stage[:5] == "SPLIT" {
		policy := DefaultRebalancePolicy()
		policy.Moves, policy.Leaders = false, false
		policy.MinRangeBytes, policy.RangeSplitBytes = 1, 1
		return policy
	}
	policy := moveOnlyPolicy()
	policy.WriteWeight, policy.ReplicaWeight = 0, 1
	policy.MinSamples = 2
	return policy
}
