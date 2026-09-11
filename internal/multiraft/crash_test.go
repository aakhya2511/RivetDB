package multiraft

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
)

func TestCatalogSubprocessCrashAtEveryPublicationStage(t *testing.T) {
	if os.Getenv("RIVETDB_MULTIRAFT_CRASH") == "" {
		t.Skip("set RIVETDB_MULTIRAFT_CRASH=1 for subprocess crashes")
	}
	if root := os.Getenv("RIVETDB_CATALOG_CRASH_CHILD"); root != "" {
		target := os.Getenv("RIVETDB_CATALOG_CRASH_STAGE")
		_, err := LoadOrBootstrapCatalog(root, bootstrapPointer(threeRangeBootstrap()), func(stage CatalogPublishStage) error {
			if catalogStageName(stage) == target {
				os.Exit(77)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("catalog stage %s did not crash", target)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, stage := range []CatalogPublishStage{CatalogTempDurable, CatalogRenamed, CatalogDirectoryDurable} {
		t.Run(catalogStageName(stage), func(t *testing.T) {
			root := t.TempDir()
			command := exec.CommandContext(context.Background(), executable, "-test.run=^TestCatalogSubprocessCrashAtEveryPublicationStage$") //nolint:gosec // current test binary
			command.Env = append(os.Environ(), "RIVETDB_MULTIRAFT_CRASH=1", "RIVETDB_CATALOG_CRASH_CHILD="+root, "RIVETDB_CATALOG_CRASH_STAGE="+catalogStageName(stage))
			assertExit77(t, command.Run())
			catalog, recoverErr := LoadOrBootstrapCatalog(root, bootstrapPointer(threeRangeBootstrap()), nil)
			if recoverErr != nil {
				t.Fatalf("recover stage=%d: %v", stage, recoverErr)
			}
			if descriptor, lookupErr := catalog.Lookup([]byte("g")); lookupErr != nil || descriptor.RangeID != 11 {
				t.Fatalf("stage=%d range=%d err=%v", stage, descriptor.RangeID, lookupErr)
			}
		})
	}
}

func TestBootstrapSubprocessCrashAfterPartialRangeCreation(t *testing.T) {
	if os.Getenv("RIVETDB_MULTIRAFT_CRASH") == "" {
		t.Skip("set RIVETDB_MULTIRAFT_CRASH=1 for subprocess crashes")
	}
	if root := os.Getenv("RIVETDB_BOOTSTRAP_CRASH_CHILD"); root != "" {
		_, err := OpenNode(NodeOptions{NodeID: 3, Directory: root, Bootstrap: bootstrapPointer(threeRangeBootstrap()),
			BootstrapRangeHook: func(rangeID RangeID) error {
				if rangeID == 10 {
					os.Exit(77)
				}
				return nil
			}})
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("partial bootstrap did not crash")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	command := exec.CommandContext(context.Background(), executable, "-test.run=^TestBootstrapSubprocessCrashAfterPartialRangeCreation$") //nolint:gosec // current test binary
	command.Env = append(os.Environ(), "RIVETDB_MULTIRAFT_CRASH=1", "RIVETDB_BOOTSTRAP_CRASH_CHILD="+root)
	assertExit77(t, command.Run())
	node, err := OpenNode(NodeOptions{NodeID: 3, Directory: root})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := node.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	if len(node.Status().Ranges) != 3 || len(node.Status().Failures) != 0 {
		t.Fatalf("recovered status=%+v", node.Status())
	}
}

func TestMultiRaftSubprocessNodeCrashRecovery(t *testing.T) {
	if os.Getenv("RIVETDB_MULTIRAFT_CRASH") == "" {
		t.Skip("set RIVETDB_MULTIRAFT_CRASH=1 for subprocess crashes")
	}
	if root := os.Getenv("RIVETDB_NODE_CRASH_CHILD"); root != "" {
		cluster := &multiTestCluster{t: t, root: root, bootstrap: threeRangeBootstrap(), nodes: make(map[raft.NodeID]*Node)}
		cluster.openRuntime(true)
		cluster.elect(10, 1)
		cluster.elect(11, 3)
		cluster.elect(12, 5)
		if err := cluster.router.Put(context.Background(), []byte("a-process"), []byte("left")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.router.Put(context.Background(), []byte("g-process"), []byte("middle")); err != nil {
			t.Fatal(err)
		}
		if err := cluster.router.Put(context.Background(), []byte("p-process"), []byte("right")); err != nil {
			t.Fatal(err)
		}
		os.Exit(77)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	command := exec.CommandContext(context.Background(), executable, "-test.run=^TestMultiRaftSubprocessNodeCrashRecovery$") //nolint:gosec // current test binary
	command.Env = append(os.Environ(), "RIVETDB_MULTIRAFT_CRASH=1", "RIVETDB_NODE_CRASH_CHILD="+root)
	assertExit77(t, command.Run())
	cluster := &multiTestCluster{t: t, root: root, bootstrap: threeRangeBootstrap(), nodes: make(map[raft.NodeID]*Node)}
	cluster.openRuntime(false)
	defer cluster.close()
	cluster.elect(10, 2)
	cluster.elect(11, 4)
	cluster.elect(12, 3)
	for _, testCase := range []struct {
		rangeID    RangeID
		nodeID     raft.NodeID
		key, value string
	}{
		{10, 2, "a-process", "left"}, {11, 4, "g-process", "middle"}, {12, 3, "p-process", "right"},
	} {
		replica, lookupErr := cluster.nodes[testCase.nodeID].Replica(testCase.rangeID)
		if lookupErr != nil {
			t.Fatal(lookupErr)
		}
		value, getErr := replica.LocalGet(context.Background(), []byte(testCase.key))
		if getErr != nil || string(value) != testCase.value {
			t.Fatalf("range=%d value=%q err=%v", testCase.rangeID, value, getErr)
		}
	}
}

func bootstrapPointer(value Bootstrap) *Bootstrap { return &value }

func catalogStageName(stage CatalogPublishStage) string {
	switch stage {
	case CatalogTempDurable:
		return "TEMP_DURABLE"
	case CatalogRenamed:
		return "RENAMED"
	case CatalogDirectoryDurable:
		return "DIRECTORY_DURABLE"
	default:
		return "UNKNOWN"
	}
}

func assertExit77(t *testing.T, err error) {
	t.Helper()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 77 {
		t.Fatalf("child error=%v", err)
	}
}
