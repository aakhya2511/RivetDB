package replicatedrange

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkMVCCSnapshotCreation(b *testing.B) {
	replica, err := Open(Options{RangeID: 1, NodeID: 1, ReplicaID: 1, Peers: []raft.NodeID{1}, Directory: testutil.BenchmarkDir(b), MVCC: true, Clock: clock.NewMockAt(time.UnixMilli(1000)), ElectionTimeoutMin: 2, ElectionTimeoutMax: 2, HeartbeatInterval: 1, Random: rand.New(rand.NewPCG(1, 2))})
	if err != nil {
		b.Fatal(err)
	}
	defer replica.Close(context.Background())
	for replica.Status().Raft.Role != raft.Leader {
		_, _ = replica.Tick()
	}
	ts, index, _, waiter, err := replica.ProposeMVCC(context.Background(), Command{Type: CommandPut, Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		b.Fatal(err)
	}
	if err = replica.Await(context.Background(), index, waiter); err != nil {
		b.Fatal(err)
	}
	_ = ts
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot, err := replica.NewSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		if err = snapshot.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
