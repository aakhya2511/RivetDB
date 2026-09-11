package multiraft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkTransactions(b *testing.B) {
	for _, testCase := range []struct {
		name string
		keys []string
	}{
		{name: "read-only"},
		{name: "single-range", keys: []string{"a"}},
		{name: "two-range", keys: []string{"a", "g"}},
		{name: "three-range", keys: []string{"a", "g", "p"}},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			configuration := threeRangeBootstrap()
			cluster := &multiTestCluster{t: b, root: testutil.BenchmarkDir(b), bootstrap: configuration,
				nodes: make(map[raft.NodeID]*Node), mvcc: true, clocks: make(map[raft.NodeID]clock.Clock)}
			for _, nodeID := range configuration.Nodes {
				cluster.clocks[nodeID] = clock.NewMockAt(time.UnixMilli(40_000 + int64(nodeID)))
			}
			cluster.openRuntime(true)
			defer cluster.close()
			cluster.elect(10, 1)
			cluster.elect(11, 3)
			cluster.elect(12, 5)
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
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
			}
		})
	}
}
