package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkPut(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	ctx, key, value := context.Background(), []byte("put"), []byte("value")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := e.Put(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutValueSize(b *testing.B) {
	for _, size := range []int{16, 1 << 10, 4 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			e := openBenchmarkEngine(b, 64)
			defer closeTestEngine(b, e)
			ctx, key, value := context.Background(), []byte("put"), make([]byte, size)
			b.SetBytes(int64(len(key) + len(value)))
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := e.Put(ctx, key, value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	for _, count := range []int{1, 16, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e := openBenchmarkEngine(b, 64)
			defer closeTestEngine(b, e)
			mutations := make([]storage.Mutation, count)
			for index := range mutations {
				mutations[index] = storage.Mutation{Key: []byte(fmt.Sprintf("key-%06d", index)), Value: []byte("value"), Kind: storage.KindValue}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := e.WriteBatch(context.Background(), mutations); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(count), "mutations/op")
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	ctx, key := context.Background(), []byte("delete")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := e.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetActive(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	mustPut(b, e, []byte("key"), []byte("value"))
	benchmarkGet(b, e, []byte("key"))
}

func BenchmarkGetL0(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	mustPut(b, e, []byte("key"), []byte("value"))
	mustFlushBenchmark(b, e)
	benchmarkGet(b, e, []byte("key"))
}

func BenchmarkGetL0EightOverlap(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	for index := 0; index < 8; index++ {
		mustPut(b, e, []byte("key"), []byte{byte(index)})
		mustFlushBenchmark(b, e)
	}
	benchmarkGet(b, e, []byte("key"))
}

func BenchmarkGetL0FourOverlap(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	for index := 0; index < 4; index++ {
		mustPut(b, e, []byte("key"), []byte{byte(index)})
		mustFlushBenchmark(b, e)
	}
	benchmarkGet(b, e, []byte("key"))
}

func BenchmarkGetL0EightOverlapMiss(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	for index := 0; index < 8; index++ {
		if err := e.WriteBatch(context.Background(), []storage.Mutation{
			{Key: []byte("a"), Value: []byte{byte(index)}, Kind: storage.KindValue},
			{Key: []byte("z"), Value: []byte{byte(index)}, Kind: storage.KindValue},
		}); err != nil {
			b.Fatal(err)
		}
		mustFlushBenchmark(b, e)
	}
	benchmarkGet(b, e, []byte("m"))
}

func BenchmarkGetL1(b *testing.B) {
	e := openBenchmarkEngine(b, 4)
	defer closeTestEngine(b, e)
	for index := 0; index < 4; index++ {
		mustPut(b, e, []byte("key"), []byte{byte(index)})
		mustFlushBenchmark(b, e)
	}
	if _, err := e.Compact(context.Background()); err != nil {
		b.Fatal(err)
	}
	benchmarkGet(b, e, []byte("key"))
}

func BenchmarkGetMiss(b *testing.B) {
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	mustPut(b, e, []byte("present"), []byte("value"))
	mustFlushBenchmark(b, e)
	benchmarkGet(b, e, []byte("absent"))
}

func benchmarkGet(b *testing.B, e *Engine, key []byte) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	before := e.Stats()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := e.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	after := e.Stats()
	operations := float64(max(1, b.N))
	b.ReportMetric(float64(after.GetL0TableReads-before.GetL0TableReads)/operations, "l0-tables/op")
	b.ReportMetric(float64(after.GetHigherTableReads-before.GetHigherTableReads)/operations, "higher-tables/op")
	b.ReportMetric(float64(after.TableOpens-before.TableOpens)/operations, "table-opens/op")
	b.ReportMetric(float64(after.BloomTableSkips-before.BloomTableSkips)/operations, "bloom-skips/op")
}

func BenchmarkScanSmall(b *testing.B) { benchmarkScan(b, 100, []byte("key-0020"), []byte("key-0040")) }
func BenchmarkScanLarge(b *testing.B) { benchmarkScan(b, 2000, nil, nil) }
func BenchmarkScan(b *testing.B) {
	for _, count := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) { benchmarkScan(b, count, nil, nil) })
	}
}
func benchmarkScan(b *testing.B, count int, start, end []byte) {
	b.Helper()
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	mutations := make([]storage.Mutation, count)
	for index := 0; index < count; index++ {
		mutations[index] = storage.Mutation{Key: []byte(fmt.Sprintf("key-%04d", index)), Value: []byte("value"), Kind: storage.KindValue}
	}
	if err := e.WriteBatch(context.Background(), mutations); err != nil {
		b.Fatal(err)
	}
	mustFlushBenchmark(b, e)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := e.Scan(ctx, start, end); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMixedWorkload(b *testing.B) {
	e := openBenchmarkEngine(b, 4)
	defer closeTestEngine(b, e)
	ctx := context.Background()
	for index := 0; index < 100; index++ {
		mustPut(b, e, []byte(fmt.Sprintf("key-%03d", index)), []byte("value"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		key := []byte(fmt.Sprintf("key-%03d", index%100))
		switch index % 10 {
		case 0:
			if err := e.Delete(ctx, key); err != nil {
				b.Fatal(err)
			}
		case 1, 2:
			if err := e.Put(ctx, key, []byte("value")); err != nil {
				b.Fatal(err)
			}
		default:
			_, err := e.Get(ctx, key)
			if err != nil && !errors.Is(err, ErrNotFound) {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkOpenRestart(b *testing.B) {
	directory := testutil.BenchmarkDir(b)
	e := openBenchmarkEngineAt(b, directory, 4)
	mustPut(b, e, []byte("key"), []byte("value"))
	closeTestEngine(b, e)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		e = openBenchmarkEngineAt(b, directory, 4)
		closeTestEngine(b, e)
	}
}

func BenchmarkOpenSSTableScale(b *testing.B) {
	for _, count := range []int{1, 10, 100} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			directory := testutil.BenchmarkDir(b)
			e := openBenchmarkEngineAt(b, directory, 1_000)
			for index := 0; index < count; index++ {
				mustPut(b, e, []byte(fmt.Sprintf("key-%06d", index)), []byte("value"))
				mustFlushBenchmark(b, e)
			}
			closeTestEngine(b, e)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				e = openBenchmarkEngineAt(b, directory, 1_000)
				closeTestEngine(b, e)
			}
			b.ReportMetric(float64(count), "live-tables")
		})
	}
}

func BenchmarkOpenWALReplayScale(b *testing.B) {
	for _, batches := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprint(batches), func(b *testing.B) {
			directory := testutil.BenchmarkDir(b)
			store, err := manifest.Create(manifest.Options{Directory: directory})
			if err != nil {
				b.Fatal(err)
			}
			if closeErr := store.Close(); closeErr != nil {
				b.Fatal(closeErr)
			}
			writer, err := wal.OpenWriter(filepath.Join(directory, pipeline.WALFileName), wal.WriterOptions{Durability: wal.SyncNone})
			if err != nil {
				b.Fatal(err)
			}
			for index := 0; index < batches; index++ {
				encoded, encodeErr := storage.EncodeWriteBatch(storage.WriteBatch{FirstSequence: uint64(index), Mutations: []storage.Mutation{{Key: []byte(fmt.Sprintf("key-%06d", index)), Value: []byte("value"), Kind: storage.KindValue}}}) //nolint:gosec // bounded benchmark index
				if encodeErr != nil {
					b.Fatal(encodeErr)
				}
				if _, appendErr := writer.Append(encoded); appendErr != nil {
					b.Fatal(appendErr)
				}
			}
			if err := writer.Sync(); err != nil {
				b.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				e := openBenchmarkEngineAt(b, directory, 64)
				closeTestEngine(b, e)
			}
			b.ReportMetric(float64(batches), "replayed-batches")
		})
	}
}

func BenchmarkWriteFrequentFlush(b *testing.B) {
	e := openBenchmarkEngine(b, 4)
	defer closeTestEngine(b, e)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := e.Put(ctx, []byte("key"), []byte("value")); err != nil {
			b.Fatal(err)
		}
		if err := e.Flush(ctx); err != nil {
			b.Fatal(err)
		}
		if _, err := e.Compact(ctx); err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
			b.Fatal(err)
		}
	}
}

func openBenchmarkEngine(b testing.TB, trigger int) *Engine {
	b.Helper()
	return openBenchmarkEngineAt(b, testutil.BenchmarkDir(b), trigger)
}
func openBenchmarkEngineAt(b testing.TB, directory string, trigger int) *Engine {
	b.Helper()
	e, err := Open(Options{Directory: directory, MemTableBytes: 64 << 20, MaxImmutables: 4, L0Trigger: trigger, TargetFileSize: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	return e
}
func mustFlushBenchmark(b testing.TB, e *Engine) {
	b.Helper()
	if err := e.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
}
