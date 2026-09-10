package sstable

import (
	"errors"
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
)

var (
	benchmarkReaderEntry Entry
	benchmarkReaderCount int
)

func BenchmarkReaderOpen(b *testing.B) {
	_, _, path := buildRealTable(b, 201, Options{}, deterministicEntries(b, 20_000))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reader, err := Open(path, ReaderOptions{})
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		if err := reader.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func BenchmarkReaderSeekHit(b *testing.B) {
	entries, reader := benchmarkReader(b)
	target := entries[len(entries)/2].key
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		entry, err := reader.Seek(target)
		if err != nil {
			b.Fatalf("Seek: %v", err)
		}
		benchmarkReaderEntry = entry
	}
}

func BenchmarkReaderSeekMiss(b *testing.B) {
	_, reader := benchmarkReader(b)
	target, err := storage.NewInternalKey([]byte{0xff}, 0, storage.KindValue)
	if err != nil {
		b.Fatalf("NewInternalKey: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, seekErr := reader.Seek(target)
		if !errors.Is(seekErr, ErrNotFound) {
			b.Fatalf("Seek miss: %v", seekErr)
		}
	}
}

func BenchmarkReaderGet(b *testing.B) {
	entries, reader := benchmarkReader(b)
	target := entries[len(entries)/2].key
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		entry, err := reader.Get(target)
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		benchmarkReaderEntry = entry
	}
}

func BenchmarkReaderGetCandidate(b *testing.B) {
	entries, reader := benchmarkReader(b)
	target := entries[len(entries)/2].key
	user := target.UserKey()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		entry, err := reader.GetCandidate(user, target.Sequence())
		if err != nil {
			b.Fatalf("GetCandidate: %v", err)
		}
		benchmarkReaderEntry = entry
	}
}

func BenchmarkReaderGetCandidateBlockSize(b *testing.B) {
	entries := deterministicEntries(b, 20_000)
	for _, size := range []int{4 << 10, 8 << 10, 16 << 10} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			_, metadata, path := buildRealTable(b, uint64(300+size), Options{BlockSize: size}, entries) //nolint:gosec // bounded benchmark sizes
			reader := mustOpenReader(b, path)
			b.Cleanup(func() { closeReader(b, reader) })
			target := entries[len(entries)/2].key
			user := target.UserKey()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				entry, err := reader.GetCandidate(user, target.Sequence())
				if err != nil {
					b.Fatal(err)
				}
				benchmarkReaderEntry = entry
			}
			b.ReportMetric(float64(metadata.FileSize), "table-bytes")
			b.ReportMetric(float64(metadata.DataBlockCount), "data-blocks")
		})
	}
}

func BenchmarkReaderFullIteration(b *testing.B) {
	entries, reader := benchmarkReader(b)
	b.SetBytes(int64(len(entries)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, err := reader.NewIterator()
		if err != nil {
			b.Fatalf("NewIterator: %v", err)
		}
		count := 0
		for iterator.Next() {
			count++
		}
		if iterator.Error() != nil || count != len(entries) {
			b.Fatalf("iteration count=%d error=%v", count, iterator.Error())
		}
		_ = iterator.Close()
		benchmarkReaderCount = count
	}
}

func BenchmarkReaderRangeIteration(b *testing.B) {
	entries, reader := benchmarkReader(b)
	start := entries[len(entries)/4].key.UserKey()
	end := entries[len(entries)*3/4].key.UserKey()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		iterator, err := reader.Range(start, end)
		if err != nil {
			b.Fatalf("Range: %v", err)
		}
		count := 0
		for iterator.Next() {
			count++
		}
		if iterator.Error() != nil {
			b.Fatalf("range iteration: %v", iterator.Error())
		}
		_ = iterator.Close()
		benchmarkReaderCount = count
	}
}

func benchmarkReader(b *testing.B) ([]testEntry, *Reader) {
	b.Helper()
	entries := deterministicEntries(b, 20_000)
	_, _, path := buildRealTable(b, 202, Options{}, entries)
	reader := mustOpenReader(b, path)
	b.Cleanup(func() { closeReader(b, reader) })
	return entries, reader
}
