package multiraft

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
)

const migrationCrashExit = 88

func openMigrationClusterAt(t testing.TB, root string, bootstrap Bootstrap) *multiTestCluster {
	t.Helper()
	cluster := &multiTestCluster{t: t, root: root, bootstrap: bootstrap, nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, id := range bootstrap.Nodes {
		cluster.clocks[id] = clock.NewMockAt(time.UnixMilli(40_000 + int64(id)*1000))
	}
	cluster.openRuntime(true)
	return cluster
}

//nolint:govet // stage-local errors intentionally mirror each recovery assertion
func TestMigrationSubprocessCrashMatrix(t *testing.T) {
	if os.Getenv("RIVETDB_MIGRATION_CRASH") == "" {
		t.Skip("set RIVETDB_MIGRATION_CRASH=1 for abrupt migration crash matrix")
	}
	if stage := os.Getenv("RIVETDB_MIGRATION_CRASH_STAGE"); stage != "" {
		root := os.Getenv("RIVETDB_MIGRATION_CRASH_ROOT")
		cluster := openMigrationClusterAt(t, root, threeRangeBootstrap())
		cluster.elect(10, 1)
		manager, _ := migrationManager(t, cluster)
		descriptor, _ := cluster.router.currentCatalog().LookupByID(10)
		source, _ := descriptor.ReplicaOn(3)
		manager.hook = func(observed MigrationHookStage, _ MigrationRecord) {
			if observed.String() == stage {
				os.Exit(migrationCrashExit)
			}
		}
		record, _ := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4)
		_ = manager.DeleteSource(record)
		os.Exit(89)
	}
	stages := []MigrationHookStage{SnapshotTransferPartial, SnapshotTargetDurable, JointConfigCommitted, FinalConfigCommitted, MetaPlacementCommitted, SourceDeleteStarted}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range stages {
		t.Run(stage.String(), func(t *testing.T) {
			root := t.TempDir()
			command := exec.CommandContext(context.Background(), executable, "-test.run=^TestMigrationSubprocessCrashMatrix$")
			command.Env = append(os.Environ(), "RIVETDB_MIGRATION_CRASH=1", "RIVETDB_MIGRATION_CRASH_STAGE="+stage.String(), "RIVETDB_MIGRATION_CRASH_ROOT="+root)
			runErr := command.Run()
			var exitErr *exec.ExitError
			if !errors.As(runErr, &exitErr) || exitErr.ExitCode() != migrationCrashExit {
				t.Fatalf("crash stage %s err=%v", stage, runErr)
			}
			cluster := openMigrationClusterAt(t, root, threeRangeBootstrap())
			defer cluster.close()
			catalog := mustCatalog(t, threeRangeBootstrap())
			meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: root + "/metadata", Bootstrap: catalog})
			if err != nil {
				t.Fatal(err)
			}
			if err := meta.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			snapshot := meta.Snapshot()
			if len(snapshot.Migrations) != 1 {
				t.Fatalf("migrations=%d", len(snapshot.Migrations))
			}
			manager, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router})
			if err != nil {
				t.Fatal(err)
			}
			record, err := manager.RecoverMigration(context.Background(), snapshot.Migrations[0].MigrationID)
			if err != nil {
				for id, node := range cluster.nodes {
					if replica, e := node.Replica(10); e == nil {
						t.Logf("node=%d status=%+v", id, replica.Status().Raft)
					}
				}
				t.Logf("failures=%+v", cluster.scheduler.Failures())
				t.Fatalf("recover %s: %v", stage, err)
			}
			if record.State != MigrationSourceRetired {
				t.Fatalf("recover %s state=%s", stage, record.State)
			}
			if stage == SourceDeleteStarted {
				if err := manager.DeleteSource(record); err != nil {
					t.Fatal(err)
				}
			}
			target, err := cluster.nodes[4].Replica(10)
			if err != nil {
				t.Fatal(err)
			}
			if target.Status().Raft.Config.Voter(3) {
				t.Fatal("retired source remained voter")
			}
			t.Logf("stage=%s recovery=SOURCE_RETIRED config=%d", stage, record.ConfigVersion)
		})
	}
}
