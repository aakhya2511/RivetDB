package memtable

import (
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
)

var (
	benchmarkEntry Entry
	benchmarkCount int
)

func BenchmarkInsertSequential(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			keys := sequentialBenchmarkKeys(b, size, 1)
			b.ReportAllocs()
			for b.Loop() {
				rng := mathrand.New(mathrand.NewPCG(7, 8))
				table := newWithRandom(rng.Uint64)
				for _, key := range keys {
					if err := table.Insert(key, []byte("value")); err != nil {
						b.Fatalf("Insert: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkInsertRandom(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			keys := randomBenchmarkKeys(b, size)
			b.ReportAllocs()
			for b.Loop() {
				rng := mathrand.New(mathrand.NewPCG(1, 2))
				table := newWithRandom(rng.Uint64)
				for _, key := range keys {
					if err := table.Insert(key, []byte("value")); err != nil {
						b.Fatalf("Insert: %v", err)
					}
				}
			}
		})
	}
}

func BenchmarkSeekHit(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			table, keys := populatedBenchmarkTable(b, size, 1)
			b.ReportAllocs()
			for index := 0; b.Loop(); index++ {
				benchmarkEntry, _ = table.Seek(keys[index%len(keys)])
			}
		})
	}
}

func BenchmarkSeekMiss(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			table, _ := populatedBenchmarkTable(b, size, 1)
			missing := benchmarkInternalKey(b, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 0)
			b.ReportAllocs()
			for b.Loop() {
				benchmarkEntry, _ = table.Seek(missing)
			}
		})
	}
}

func BenchmarkForwardIteration(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			table, _ := populatedBenchmarkTable(b, size, 1)
			b.ReportAllocs()
			for b.Loop() {
				count := 0
				iterator := table.Iterator()
				for iterator.Next() {
					benchmarkEntry, _ = iterator.Entry()
					count++
				}
				benchmarkCount = count
			}
		})
	}
}

func BenchmarkFrozenIteration(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			table, _ := populatedBenchmarkTable(b, size, 1)
			b.ReportAllocs()
			for b.Loop() {
				count := 0
				iterator, err := table.FrozenIterator()
				if err != nil {
					b.Fatal(err)
				}
				for iterator.Next() {
					benchmarkEntry, _ = iterator.Entry()
					count++
				}
				benchmarkCount = count
			}
		})
	}
}

func BenchmarkMixedVersionedKeys(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			const versions = 8
			table, keys := populatedBenchmarkTable(b, size, versions)
			userKey := keys[len(keys)/2].UserKey()
			b.ReportAllocs()
			for target := uint64(0); b.Loop(); target++ {
				benchmarkEntry, _ = table.GetCandidate(userKey, target%versions)
			}
		})
	}
}

func populatedBenchmarkTable(b *testing.B, size, versions int) (*MemTable, []storage.InternalKey) {
	b.Helper()
	keys := sequentialBenchmarkKeys(b, size, versions)
	rng := mathrand.New(mathrand.NewPCG(3, 4))
	table := newWithRandom(rng.Uint64)
	for _, key := range keys {
		if err := table.Insert(key, []byte("value")); err != nil {
			b.Fatalf("Insert: %v", err)
		}
	}
	table.Freeze()
	return table, keys
}

func sequentialBenchmarkKeys(b *testing.B, size, versions int) []storage.InternalKey {
	b.Helper()
	keys := make([]storage.InternalKey, size)
	for index := range size {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(index/versions))
		keys[index] = benchmarkInternalKey(b, encoded[:], uint64(index%versions))
	}
	return keys
}

func randomBenchmarkKeys(b *testing.B, size int) []storage.InternalKey {
	b.Helper()
	rng := mathrand.New(mathrand.NewPCG(5, 6))
	keys := make([]storage.InternalKey, size)
	for index := range size {
		var encoded [16]byte
		binary.BigEndian.PutUint64(encoded[:8], rng.Uint64())
		binary.BigEndian.PutUint64(encoded[8:], uint64(index))
		keys[index] = benchmarkInternalKey(b, encoded[:], rng.Uint64())
	}
	return keys
}

func benchmarkInternalKey(b *testing.B, userKey []byte, sequence uint64) storage.InternalKey {
	b.Helper()
	key, err := storage.NewInternalKey(userKey, sequence, storage.KindValue)
	if err != nil {
		b.Fatalf("NewInternalKey: %v", err)
	}
	return key
}
