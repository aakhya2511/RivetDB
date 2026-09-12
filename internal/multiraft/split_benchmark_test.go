package multiraft

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

func BenchmarkMetadataLookup(b *testing.B) {
	state := benchmarkLineageState(b, 96)
	var result RangeDescriptor
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		descriptor, err := state.catalog.Lookup([]byte{byte(index % 100)})
		if err != nil {
			b.Fatal(err)
		}
		result = descriptor
	}
	runtime.KeepAlive(result)
}

func BenchmarkLineageResolve(b *testing.B) {
	state := benchmarkLineageState(b, 96)
	var result []RangeDescriptor
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		descriptors, err := state.resolve(RangeRef{RangeID: 10, Generation: 1})
		if err != nil {
			b.Fatal(err)
		}
		result = descriptors
	}
	runtime.KeepAlive(result)
}

func BenchmarkLogicalImageDigest(b *testing.B) {
	versions := make([]engine.MVCCVersion, 10_000)
	var result [32]byte
	for index := range versions {
		versions[index] = engine.MVCCVersion{Key: []byte(fmt.Sprintf("key-%06d", index/10)), Timestamp: uint64(10 - index%10), Kind: storage.KindValue, Value: []byte("0123456789abcdef")}
	}
	b.SetBytes(int64(len(versions) * 36))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result = digestVersions(versions)
	}
	runtime.KeepAlive(result)
}

func benchmarkLineageState(b *testing.B, splits int) *metadataState {
	b.Helper()
	catalog, err := NewCatalog(threeRangeBootstrap())
	if err != nil {
		b.Fatal(err)
	}
	state, err := newMetadataState(catalog)
	if err != nil {
		b.Fatal(err)
	}
	parent := RangeRef{RangeID: 10, Generation: 1}
	for index := 1; index <= splits; index++ {
		record, beginErr := state.begin(parent, []byte{byte(index)}, state.catalog.Generation())
		if beginErr != nil {
			b.Fatal(beginErr)
		}
		record, err = state.advance(record.SplitID, record.Epoch, SplitCopying, SplitRecord{BootstrapIndex: 1})
		if err == nil {
			record, err = state.advance(record.SplitID, record.Epoch, SplitCatchingUp, SplitRecord{ImageDigest: [32]byte{1}, LeftReplayThrough: 1, RightReplayThrough: 1})
		}
		if err == nil {
			record, err = state.advance(record.SplitID, record.Epoch, SplitReady, SplitRecord{LeftReplayThrough: 2, RightReplayThrough: 2})
		}
		if err == nil {
			record, err = state.advance(record.SplitID, record.Epoch, SplitFenced, SplitRecord{FenceIndex: 2, LeftReplayThrough: 2, RightReplayThrough: 2})
		}
		if err == nil {
			record, err = state.commit(record.SplitID, record.Epoch, state.catalog.Generation())
		}
		if err != nil {
			b.Fatal(err)
		}
		parent = RangeRef{RangeID: record.Right.RangeID, Generation: record.Right.Generation}
	}
	return state
}
