package compaction

import (
	"context"
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

func BenchmarkMerge(b *testing.B) {
	for _, count := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("%d-way", count), func(b *testing.B) {
			directory := b.TempDir()
			store := createStore(b, directory)
			defer store.Close()
			tables := make([]manifest.TableMetadata, 0, count)
			for input := range count {
				entries := make([]testEntry, 256)
				for index := range entries {
					entries[index] = testEntry{user: fmt.Sprintf("%08d", index*count+input), sequence: uint64(index), kind: storage.KindValue, value: "value"}
				}
				tables = append(tables, installTable(b, store, directory, 0, uint64(input+1), entries))
			}
			executor, _ := New(Options{Directory: directory}, store)
			b.ReportAllocs()
			b.SetBytes(int64(count * 256 * 22))
			b.ResetTimer()
			for b.Loop() {
				readers, merge, _, err := executor.openInputs(tables)
				if err != nil {
					b.Fatal(err)
				}
				var entries int
				for merge.Next() {
					entries++
				}
				if merge.Error() != nil || entries != count*256 {
					b.Fatal("merge mismatch")
				}
				if err := merge.Close(); err != nil {
					b.Fatal(err)
				}
				if err := closeReaders(readers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompaction(b *testing.B) {
	for _, test := range []struct {
		name           string
		files, entries int
		target         uint64
	}{{"small", 4, 128, 4 << 20}, {"medium", 8, 1024, 4 << 20}, {"multi-output", 8, 512, 8 << 10}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			var inputBytes, outputBytes uint64
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				directory := b.TempDir()
				store := createStore(b, directory)
				for file := range test.files {
					entries := make([]testEntry, test.entries)
					for index := range entries {
						entries[index] = testEntry{user: fmt.Sprintf("%08d", index), sequence: uint64(test.files - file), kind: storage.KindValue, value: "benchmark-value"}
					}
					installTable(b, store, directory, 0, uint64(file+1), entries)
				}
				executor, _ := New(Options{Directory: directory, L0Trigger: test.files, TargetFileSize: test.target, SSTableOptions: sstable.Options{}}, store)
				b.StartTimer()
				result, err := executor.CompactOnce(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				inputBytes += result.InputBytes
				outputBytes += result.OutputBytes
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
			b.StopTimer()
			if inputBytes != 0 {
				b.ReportMetric(float64(outputBytes)/float64(inputBytes), "write-amp")
			}
			b.ReportMetric(float64(outputBytes)/float64(max(1, b.N)), "output-B/op")
		})
	}
}
