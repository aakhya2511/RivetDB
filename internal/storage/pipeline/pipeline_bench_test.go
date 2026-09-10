package pipeline

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
)

func BenchmarkWriteSingleNoRotation(b *testing.B) {
	p := benchmarkPipeline(b, math.MaxUint64, 4, successfulFlush)
	benchmarkWrites(b, p)
}

func BenchmarkWriteSingleFrequentRotation(b *testing.B) {
	p := benchmarkPipeline(b, 1, 8, successfulFlush)
	benchmarkWrites(b, p)
	b.ReportMetric(float64(p.Stats().Rotations), "rotations")
}

func BenchmarkWriteMultipleWriters(b *testing.B) {
	p := benchmarkPipeline(b, math.MaxUint64, 4, successfulFlush)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			if _, err := p.Write(context.Background(), []storage.Mutation{put("parallel")}); err != nil {
				b.Errorf("Write: %v", err)
			}
		}
	})
}

func BenchmarkFlushThroughput(b *testing.B) {
	directory := b.TempDir()
	p, err := newPipeline(pipelineOptions(directory), &fakeWAL{}, nil)
	if err != nil {
		b.Fatalf("newPipeline: %v", err)
	}
	b.Cleanup(func() { _ = p.Close(context.Background()) })
	value := make([]byte, 4096)
	b.SetBytes(int64(len(value)))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		table := memtable.New()
		batch := storage.WriteBatch{FirstSequence: uint64(index), Mutations: []storage.Mutation{{Key: []byte(fmt.Sprintf("key-%012d", index)), Value: value, Kind: storage.KindValue}}}
		if applyErr := table.ApplyBatch(batch); applyErr != nil {
			b.Fatalf("ApplyBatch: %v", applyErr)
		}
		table.Freeze()
		item := &generation{id: uint64(index + 1), fileNumber: uint64(index + 1), table: table, state: StateFlushing}
		if _, flushErr := p.flush(context.Background(), item); flushErr != nil {
			b.Fatalf("flush: %v", flushErr)
		}
	}
}

func BenchmarkWritesWhileFlushActive(b *testing.B) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	executor := func(ctx context.Context, item *generation) (flushResult, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return flushResult{}, fmt.Errorf("benchmark flush canceled: %w", ctx.Err())
		case <-release:
			return successfulFlush(ctx, item)
		}
	}
	p := benchmarkPipeline(b, 1, b.N+1, executor)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("trigger")}); err != nil {
		b.Fatalf("trigger Write: %v", err)
	}
	<-started
	p.threshold = math.MaxUint64
	b.ReportAllocs()
	b.ResetTimer()
	benchmarkWrites(b, p)
	b.StopTimer()
	close(release)
}

func BenchmarkBackpressure(b *testing.B) {
	p := benchmarkPipeline(b, 1, 1, successfulFlush)
	benchmarkWrites(b, p)
	b.ReportMetric(float64(p.Stats().BackpressureEvents), "backpressure-events")
}

func benchmarkPipeline(b *testing.B, threshold uint64, maximum int, executor flushExecutor) *Pipeline {
	b.Helper()
	options := pipelineOptions(b.TempDir())
	options.MemTableBytes = threshold
	options.MaxImmutables = maximum
	p, err := newPipeline(options, &fakeWAL{}, executor)
	if err != nil {
		b.Fatalf("newPipeline: %v", err)
	}
	b.Cleanup(func() {
		if closeErr := p.Close(context.Background()); closeErr != nil {
			b.Errorf("Close: %v", closeErr)
		}
	})
	return p
}

func benchmarkWrites(b *testing.B, p *Pipeline) {
	b.Helper()
	mutation := []storage.Mutation{put("benchmark")}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := p.Write(context.Background(), mutation); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}
