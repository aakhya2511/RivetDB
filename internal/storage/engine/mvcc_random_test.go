package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type referenceVersion struct {
	timestamp uint64
	deleted   bool
	value     []byte
}

func TestRandomizedHistoricalReadsAgainstReference(t *testing.T) {
	for _, seed := range []int64{501, 502, testutil.Seed(t)} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) { runHistoricalReference(t, seed, 10_000) })
	}
}

func runHistoricalReference(t testing.TB, seed int64, events int) {
	t.Helper()
	directory := t.TempDir()
	open := func() *Engine {
		value, err := Open(Options{Directory: directory, Mode: ModeReplicatedMVCC, MemTableBytes: 8 << 10, L0Trigger: 4})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	e := open()
	defer func() { _ = e.Close(context.Background()) }()
	rng := testutil.RandFromSeed(seed)
	history := make(map[string][]referenceVersion)
	keys := make([][]byte, 32)
	for index := range keys {
		keys[index] = []byte{byte(index), 0, byte(255 - index)}
	}
	var applied uint64
	var maxTimestamp uint64
	stats := struct{ puts, deletes, gets, scans, flushes, compactions, restarts int }{}
	for event := 1; event <= events; event++ {
		key := keys[rng.IntN(len(keys))]
		switch rng.IntN(10) {
		case 0, 1, 2:
			applied++
			maxTimestamp += uint64(rng.IntN(3) + 1)
			mutation := storage.Mutation{Kind: storage.KindValue, Key: key, Value: []byte(fmt.Sprintf("%d/%d", seed, event))}
			deleted := event%7 == 0
			if deleted {
				mutation.Kind, mutation.Value = storage.KindDelete, nil
				stats.deletes++
			} else {
				stats.puts++
			}
			if err := e.ApplyCommittedMVCC(context.Background(), applied, 1, maxTimestamp, []byte(fmt.Sprintf("%d", applied)), mutation); err != nil {
				t.Fatal(err)
			}
			history[string(key)] = append(history[string(key)], referenceVersion{timestamp: maxTimestamp, deleted: deleted, value: bytes.Clone(mutation.Value)})
		case 3, 4, 5, 6, 7:
			target := uint64(rng.Int64N(int64(maxTimestamp + 2)))
			assertReferenceGet(t, e, history, key, target)
			stats.gets++
		case 8:
			target := uint64(rng.Int64N(int64(maxTimestamp + 2)))
			assertReferenceScan(t, e, history, target)
			stats.scans++
		case 9:
			if err := e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.flushes++
			if event%20 == 0 {
				if _, err := e.Compact(context.Background()); err == nil {
					stats.compactions++
				}
				_, _ = e.ReclaimObsoleteTables(context.Background())
			}
		}
		if event%2500 == 0 {
			if err := e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			e = open()
			stats.restarts++
		}
	}
	for _, key := range keys {
		assertReferenceGet(t, e, history, key, maxTimestamp)
	}
	t.Logf("seed=%d events=%d puts=%d deletes=%d get_at=%d scan_at=%d flushes=%d compactions=%d restarts=%d mismatches=0", seed, events, stats.puts, stats.deletes, stats.gets, stats.scans, stats.flushes, stats.compactions, stats.restarts)
}

func visibleReference(versions []referenceVersion, target uint64) (referenceVersion, bool) {
	for index := len(versions) - 1; index >= 0; index-- {
		if versions[index].timestamp <= target {
			return versions[index], true
		}
	}
	return referenceVersion{}, false
}

func assertReferenceGet(t testing.TB, e *Engine, history map[string][]referenceVersion, key []byte, target uint64) {
	t.Helper()
	want, found := visibleReference(history[string(key)], target)
	got, err := e.GetAt(context.Background(), key, target)
	if !found || want.deleted {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("key=%x T=%d got=%q err=%v", key, target, got, err)
		}
		return
	}
	if err != nil || !bytes.Equal(got, want.value) {
		t.Fatalf("key=%x T=%d got=%q err=%v want=%q", key, target, got, err, want.value)
	}
}

func assertReferenceScan(t testing.TB, e *Engine, history map[string][]referenceVersion, target uint64) {
	t.Helper()
	var want []KV
	for key, versions := range history {
		version, ok := visibleReference(versions, target)
		if ok && !version.deleted {
			want = append(want, KV{Key: []byte(key), Value: bytes.Clone(version.value)})
		}
	}
	slices.SortFunc(want, func(a, b KV) int { return bytes.Compare(a.Key, b.Key) })
	got, err := e.ScanAt(context.Background(), nil, nil, target)
	if err != nil || len(got) != len(want) {
		t.Fatalf("ScanAt T=%d len=%d/%d err=%v", target, len(got), len(want), err)
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("ScanAt T=%d[%d]=%x/%q want=%x/%q", target, i, got[i].Key, got[i].Value, want[i].Key, want[i].Value)
		}
	}
}
