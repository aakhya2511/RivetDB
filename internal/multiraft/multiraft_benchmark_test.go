package multiraft

import (
	"context"
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkCatalogLookup(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("ranges-%d", count), func(b *testing.B) {
			catalog := benchmarkCatalog(b, count)
			key := []byte(fmt.Sprintf("%08d", count/2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := catalog.Lookup(key); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTransportDemux(b *testing.B) {
	transport, err := NewTransport(1)
	if err != nil {
		b.Fatal(err)
	}
	envelope := testEnvelope(10, 1, 2)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := transport.Send(envelope); err != nil {
			b.Fatal(err)
		}
		if _, ok := transport.Take(); !ok {
			b.Fatal("missing envelope")
		}
	}
}

func BenchmarkSchedulerTick100Groups(b *testing.B) {
	catalog := benchmarkCatalog(b, 100).Snapshot()
	node, err := OpenNode(NodeOptions{NodeID: 1, Directory: testutil.BenchmarkDir(b), Bootstrap: &catalog, MaxHostedRanges: 100})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := node.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
	}()
	transport, _ := NewTransport(10_000)
	scheduler, _ := NewScheduler(transport)
	if err := scheduler.AddNode(node); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := scheduler.TickNext(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoutedMutation(b *testing.B) {
	cluster := newMultiTestClusterAt(b, threeRangeBootstrap(), testutil.BenchmarkDir(b))
	cluster.elect(10, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := range b.N {
		key := []byte(fmt.Sprintf("bench-%08d", index))
		if err := cluster.router.Put(context.Background(), key, []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCatalog(b *testing.B, count int) *Catalog {
	b.Helper()
	ranges := make([]RangeDescriptor, count)
	for index := range ranges {
		start := KeyBound{Key: []byte(fmt.Sprintf("%08d", index))}
		end := KeyBound{Key: []byte(fmt.Sprintf("%08d", index+1))}
		if index == 0 {
			start = KeyBound{Unbounded: true}
		}
		if index == count-1 {
			end = KeyBound{Unbounded: true}
		}
		ranges[index] = RangeDescriptor{RangeID: RangeID(index + 1), Generation: 1, StartKey: start, EndKey: end,
			Replicas: []ReplicaDescriptor{{ReplicaID: ReplicaID(index + 1), NodeID: 1}}}
	}
	catalog, err := NewCatalog(Bootstrap{Generation: 1, Nodes: []raft.NodeID{1}, ReplicationFactor: 1, Ranges: ranges})
	if err != nil {
		b.Fatal(err)
	}
	return catalog
}
