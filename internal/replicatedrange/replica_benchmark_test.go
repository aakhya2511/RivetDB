package replicatedrange

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

// BenchmarkThreeNodeDurableCommitApply includes three local FileStore
// publications and leader/follower WAL-free LSM apply. It is a local
// engineering baseline, not a network or production-latency measurement.
func BenchmarkThreeNodeDurableCommitApply(b *testing.B) {
	cluster := newTestClusterAt(b, 3, testutil.BenchmarkDir(b))
	cluster.elect(1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("key-%08d", index)), Value: []byte("value")})
	}
}

func BenchmarkFollowerCatchUp100(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		cluster := newTestClusterAt(b, 3, filepath.Join(testutil.BenchmarkDir(b), fmt.Sprintf("iteration-%d", iteration)))
		cluster.elect(1)
		if err := cluster.replicas[3].Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		cluster.replicas[3] = nil
		for index := 0; index < 100; index++ {
			cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("key-%03d", index)), Value: []byte("value")})
		}
		cluster.openAll()
		b.StartTimer()
		for cluster.replicas[3].Status().Raft.LastApplied < cluster.replicas[1].Status().Raft.LastApplied {
			messages, err := cluster.replicas[1].Tick()
			if err != nil {
				b.Fatal(err)
			}
			cluster.deliver(messages, 10000)
		}
		b.StopTimer()
		cluster.closeAll()
	}
}

func BenchmarkFullRestartReplay100(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		cluster := newTestClusterAt(b, 3, filepath.Join(testutil.BenchmarkDir(b), fmt.Sprintf("iteration-%d", iteration)))
		cluster.elect(1)
		for index := 0; index < 100; index++ {
			cluster.propose(1, Command{Type: CommandPut, Key: []byte(fmt.Sprintf("key-%03d", index)), Value: []byte("value")})
		}
		cluster.closeAll()
		b.StartTimer()
		cluster.openAll()
		cluster.elect(1)
		cluster.assertConverged()
		b.StopTimer()
		cluster.closeAll()
	}
}
