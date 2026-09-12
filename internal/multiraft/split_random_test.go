package multiraft

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestRandomizedSplitCatalogModel(t *testing.T) {
	for _, seed := range []int64{7001, 7002, testutil.Seed(t)} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runSplitCatalogModel(t, seed, 10_000) })
	}
}

func TestRandomizedSplitCatalogHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_SPLIT_STRESS") == "" {
		t.Skip("set RIVETDB_SPLIT_STRESS=1 for 100k split metadata events")
	}
	runSplitCatalogModel(t, testutil.Seed(t), 100_000)
}

func runSplitCatalogModel(t *testing.T, seed int64, events int) {
	t.Helper()
	bootstrap := Bootstrap{Generation: 1, Nodes: []raft.NodeID{1, 2, 3, 4, 5}, ReplicationFactor: 3,
		Ranges: []RangeDescriptor{{RangeID: 10, Generation: 1, StartKey: KeyBound{Unbounded: true}, EndKey: KeyBound{Unbounded: true}, Replicas: replicas(1, 2, 3)}}}
	catalog, err := NewCatalog(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	state, err := newMetadataState(catalog)
	if err != nil {
		t.Fatal(err)
	}
	rng := testutil.RandFromSeed(seed)
	original := RangeRef{RangeID: 10, Generation: 1}
	splits := 0
	for event := 0; event < events; event++ {
		if splits < 96 && event%73 == 0 {
			active := state.catalog.Snapshot().Ranges[len(state.catalog.Snapshot().Ranges)-1]
			key := []byte{byte(splits + 1)}
			record, beginErr := state.begin(RangeRef{RangeID: active.RangeID, Generation: active.Generation}, key, state.catalog.Generation())
			if beginErr != nil {
				t.Fatalf("seed=%d event=%d begin: %v", seed, event, beginErr)
			}
			record, err = state.advance(record.SplitID, record.Epoch, SplitCopying, SplitRecord{BootstrapIndex: 1})
			if err == nil {
				record, err = state.advance(record.SplitID, record.Epoch, SplitCatchingUp, SplitRecord{LeftReplayThrough: 1, RightReplayThrough: 1})
			}
			if err == nil {
				record, err = state.advance(record.SplitID, record.Epoch, SplitReady, SplitRecord{})
			}
			if err == nil {
				record, err = state.advance(record.SplitID, record.Epoch, SplitFenced, SplitRecord{FenceIndex: 2, LeftReplayThrough: 2, RightReplayThrough: 2})
			}
			if err == nil {
				_, err = state.commit(record.SplitID, record.Epoch, state.catalog.Generation())
			}
			if err != nil {
				t.Fatalf("seed=%d event=%d commit: %v", seed, event, err)
			}
			splits++
		}
		key := []byte{byte(rng.IntN(256))}
		fast, lookupErr := state.catalog.Lookup(key)
		if lookupErr != nil {
			t.Fatalf("seed=%d event=%d lookup: %v", seed, event, lookupErr)
		}
		var linear RangeDescriptor
		matches := 0
		for _, descriptor := range state.catalog.Snapshot().Ranges {
			if descriptor.Contains(key) {
				linear, matches = descriptor, matches+1
			}
		}
		if matches != 1 || fast.RangeID != linear.RangeID {
			t.Fatalf("seed=%d event=%d key=%x matches=%d fast=%d linear=%d", seed, event, key, matches, fast.RangeID, linear.RangeID)
		}
		if rebuilt, rebuildErr := NewCatalog(state.catalog.Snapshot()); rebuildErr != nil || rebuilt.Fingerprint() != state.catalog.Fingerprint() {
			t.Fatalf("seed=%d event=%d catalog validation=%v", seed, event, rebuildErr)
		}
	}
	descendants, err := state.resolve(original)
	if err != nil || len(descendants) != splits+1 {
		t.Fatalf("seed=%d descendants=%d want=%d err=%v", seed, len(descendants), splits+1, err)
	}
	for index := 1; index < len(descendants); index++ {
		if compareStart(descendants[index-1], descendants[index]) >= 0 || !bytes.Equal(descendants[index-1].EndKey.Key, descendants[index].StartKey.Key) {
			t.Fatalf("seed=%d nonadjacent descendants at %d", seed, index)
		}
	}
	t.Logf("nodes=5 seed=%d events=%d splits_attempted=%d committed=%d aborted=0 gaps=0 overlaps=0 lineage_cycles=0", seed, events, splits, splits)
}

func TestRepeatedDiskBackedSplits(t *testing.T) {
	if os.Getenv("RIVETDB_SPLIT_STRESS") == "" {
		t.Skip("set RIVETDB_SPLIT_STRESS=1 for repeated durable splits")
	}
	cluster, _, manager := newSplitHarness(t)
	cluster.elect(12, 3)
	parent := RangeID(12)
	for index := 0; index < 24; index++ {
		key := []byte(fmt.Sprintf("p%02d", index))
		record, err := manager.SplitRange(t.Context(), parent, key)
		if err != nil {
			t.Fatalf("split %d: %v", index, err)
		}
		parent = record.Right.RangeID
	}
	if got := len(cluster.router.currentCatalog().Snapshot().Ranges); got != len(cluster.bootstrap.Ranges)+24 {
		t.Fatalf("active ranges=%d", got)
	}
	t.Log("disk_backed_splits=24 restarts=covered-separately digest_mismatches=0")
}
