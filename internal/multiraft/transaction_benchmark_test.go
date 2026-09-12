package multiraft

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func BenchmarkTransactions(b *testing.B) {
	for _, testCase := range []struct {
		name      string
		keys      []string
		bootstrap func() Bootstrap
	}{
		{name: "read-only"},
		{name: "single-range", keys: []string{"a"}},
		{name: "two-range", keys: []string{"a", "g"}},
		{name: "three-range", keys: []string{"a", "g", "p"}},
		{name: "ten-participant", keys: []string{"00", "01", "02", "03", "04", "05", "06", "07", "08", "09"}, bootstrap: tenRangeBenchmarkBootstrap},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			configuration := threeRangeBootstrap()
			if testCase.bootstrap != nil {
				configuration = testCase.bootstrap()
			}
			cluster := &multiTestCluster{t: b, root: testutil.BenchmarkDir(b), bootstrap: configuration,
				nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
			for _, nodeID := range configuration.Nodes {
				cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(40_000 + int64(nodeID)))
			}
			cluster.openRuntime(true)
			defer cluster.close()
			for index, descriptor := range configuration.Ranges {
				cluster.elect(descriptor.RangeID, configuration.Nodes[index%len(configuration.Nodes)])
			}
			latencies := make([]time.Duration, 0, min(b.N, 4096))
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				started := time.Now()
				current, err := cluster.router.Begin(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				for _, prefix := range testCase.keys {
					key := []byte(fmt.Sprintf("%s-bench-%09d", prefix, index))
					if err := current.Put(key, []byte("value")); err != nil {
						b.Fatal(err)
					}
				}
				if err := current.Commit(context.Background()); err != nil {
					b.Fatal(err)
				}
				if len(latencies) < cap(latencies) {
					latencies = append(latencies, time.Since(started))
				}
			}
			reportLatencyDistribution(b, latencies)
		})
	}
}

// BenchmarkTransactionConflicts runs pairs sharing a read timestamp and key:
// one commits and one is rejected by first-committer-wins. Setup/election are
// excluded; each operation is therefore a 50% conflict workload.
func BenchmarkTransactionConflicts(b *testing.B) {
	configuration := threeRangeBootstrap()
	cluster := &multiTestCluster{t: b, root: testutil.BenchmarkDir(b), bootstrap: configuration,
		nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
	for _, nodeID := range configuration.Nodes {
		cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(50_000 + int64(nodeID)))
	}
	cluster.openRuntime(true)
	defer cluster.close()
	cluster.elect(10, 1)
	latencies := make([]time.Duration, 0, min(b.N, 4096))
	b.ReportAllocs()
	b.ResetTimer()
	for index := range b.N {
		started := time.Now()
		winner, err := cluster.router.Begin(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		loser, err := cluster.router.Begin(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		key := []byte(fmt.Sprintf("conflict-%08d", index))
		if err := winner.Put(key, []byte("winner")); err != nil {
			b.Fatal(err)
		}
		if err := loser.Put(key, []byte("loser")); err != nil {
			b.Fatal(err)
		}
		if err := winner.Commit(context.Background()); err != nil {
			b.Fatal(err)
		}
		if err := loser.Commit(context.Background()); !errors.Is(err, txn.ErrWriteConflict) {
			b.Fatalf("loser error=%v", err)
		}
		if len(latencies) < cap(latencies) {
			latencies = append(latencies, time.Since(started))
		}
	}
	reportLatencyDistribution(b, latencies)
}

func tenRangeBenchmarkBootstrap() Bootstrap {
	ranges := make([]RangeDescriptor, 10)
	for index := range ranges {
		start := KeyBound{Key: []byte(fmt.Sprintf("%02d", index))}
		end := KeyBound{Key: []byte(fmt.Sprintf("%02d", index+1))}
		if index == 0 {
			start = KeyBound{Unbounded: true}
		}
		if index == len(ranges)-1 {
			end = KeyBound{Unbounded: true}
		}
		ranges[index] = RangeDescriptor{RangeID: RangeID(100 + index), Generation: 1, StartKey: start, EndKey: end, Replicas: replicas(1, 2, 3)}
	}
	return Bootstrap{Generation: 1, Nodes: []raft.NodeID{1, 2, 3}, ReplicationFactor: 3, Ranges: ranges}
}

func reportLatencyDistribution(b *testing.B, values []time.Duration) {
	b.Helper()
	if len(values) == 0 {
		return
	}
	slices.Sort(values)
	percentile := func(numerator int) float64 {
		index := (len(values)*numerator + 99) / 100
		return float64(values[max(0, index-1)].Nanoseconds())
	}
	b.ReportMetric(percentile(50), "p50-ns")
	b.ReportMetric(percentile(95), "p95-ns")
	b.ReportMetric(percentile(99), "p99-ns")
}
