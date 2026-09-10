package testutil

import (
	"fmt"
	"os"
	"testing"
)

// BenchmarkDirectoryEnvironment selects the parent directory for benchmark
// database and test data. The repository and build cache remain unaffected.
const BenchmarkDirectoryEnvironment = "RIVETDB_BENCH_DIR"

// BenchmarkDir creates an owned, automatically cleaned benchmark directory.
// Without RIVETDB_BENCH_DIR it has the same behavior as testing.TB.TempDir.
func BenchmarkDir(t testing.TB) string {
	t.Helper()
	root := os.Getenv(BenchmarkDirectoryEnvironment)
	if root == "" {
		return t.TempDir()
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatalf("create benchmark root: %v", err)
	}
	directory, err := os.MkdirTemp(root, "rivetdb-benchmark-")
	if err != nil {
		t.Fatalf("create benchmark directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("clean owned benchmark directory %s: %v", directory, err)
		}
	})
	return directory
}

// BenchmarkEnvironment returns the configured benchmark root for evidence.
func BenchmarkEnvironment() string {
	if root := os.Getenv(BenchmarkDirectoryEnvironment); root != "" {
		return root
	}
	return fmt.Sprintf("testing.TempDir under %s", os.TempDir())
}
