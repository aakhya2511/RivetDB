package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBenchmarkDirUsesConfiguredOwnedChild(t *testing.T) {
	root := t.TempDir()
	t.Setenv(BenchmarkDirectoryEnvironment, root)
	directory := BenchmarkDir(t)
	if filepath.Dir(directory) != root || !strings.HasPrefix(filepath.Base(directory), "rivetdb-benchmark-") {
		t.Fatalf("BenchmarkDir=%q outside configured root %q", directory, root)
	}
	if _, err := os.Stat(directory); err != nil {
		t.Fatalf("BenchmarkDir missing: %v", err)
	}
	if got := BenchmarkEnvironment(); got != root {
		t.Fatalf("BenchmarkEnvironment=%q want %q", got, root)
	}
}
