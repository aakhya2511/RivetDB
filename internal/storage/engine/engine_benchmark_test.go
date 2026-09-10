package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage/compaction"
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
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, err := e.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			b.Fatal(err)
		}
	}
}

func BenchmarkScanSmall(b *testing.B) { benchmarkScan(b, 100, []byte("key-0020"), []byte("key-0040")) }
func BenchmarkScanLarge(b *testing.B) { benchmarkScan(b, 2000, nil, nil) }
func benchmarkScan(b *testing.B, count int, start, end []byte) {
	b.Helper()
	e := openBenchmarkEngine(b, 64)
	defer closeTestEngine(b, e)
	for index := 0; index < count; index++ {
		mustPut(b, e, []byte(fmt.Sprintf("key-%04d", index)), []byte("value"))
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
	directory := b.TempDir()
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
	return openBenchmarkEngineAt(b, b.TempDir(), trigger)
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
