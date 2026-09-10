package sstable

import (
	"bytes"
	"encoding/binary"
	mathrand "math/rand/v2"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
)

var benchmarkMetadata Metadata

func BenchmarkWriterSequential100K(b *testing.B) {
	entries := benchmarkSequentialEntries(b, 100_000, 16)
	benchmarkWrite(b, entries)
}

func BenchmarkWriterRandomOrdered100K(b *testing.B) {
	rng := mathrand.New(mathrand.NewPCG(1, 2))
	entries := make([]testEntry, 100_000)
	for index := range entries {
		var userKey [16]byte
		binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
		binary.BigEndian.PutUint64(userKey[8:], uint64(index))
		entries[index] = testEntry{key: benchmarkKey(b, userKey[:], rng.Uint64()), value: bytes.Repeat([]byte{byte(index)}, 16)}
	}
	slices.SortFunc(entries, func(left, right testEntry) int {
		return storage.CompareInternal(left.key, right.key)
	})
	benchmarkWrite(b, entries)
}

func BenchmarkWriterCommonPrefix100K(b *testing.B) {
	prefix := bytes.Repeat([]byte("tenant/0000000001/account/"), 4)
	entries := make([]testEntry, 100_000)
	for index := range entries {
		var suffix [8]byte
		binary.BigEndian.PutUint64(suffix[:], uint64(index))
		userKey := append(bytes.Clone(prefix), suffix[:]...)
		entries[index] = testEntry{key: benchmarkKey(b, userKey, 1), value: bytes.Repeat([]byte("v"), 16)}
	}
	benchmarkWrite(b, entries)
}

func BenchmarkWriterSmallValues100K(b *testing.B) {
	entries := benchmarkSequentialEntries(b, 100_000, 1)
	benchmarkWrite(b, entries)
}

func BenchmarkWriterValues4KiB(b *testing.B) {
	entries := benchmarkSequentialEntries(b, 10_000, 4<<10)
	benchmarkWrite(b, entries)
}

func benchmarkWrite(b *testing.B, entries []testEntry) {
	b.Helper()
	var rawBytes uint64
	for _, entry := range entries {
		rawBytes += uint64(len(entry.key.Encode()) + len(entry.value))
	}
	b.SetBytes(int64(rawBytes)) //nolint:gosec // benchmark datasets are far below MaxInt64
	b.ReportAllocs()
	var tableBytes int
	for b.Loop() {
		writer := newBenchmarkWriter()
		for _, entry := range entries {
			if err := writer.Add(entry.key, entry.value); err != nil {
				b.Fatalf("Add: %v", err)
			}
		}
		metadata, err := writer.Finish()
		if err != nil {
			b.Fatalf("Finish: %v", err)
		}
		benchmarkMetadata = metadata
		tableBytes = writer.file.(*memoryFile).Len()
	}
	b.ReportMetric(float64(tableBytes), "table-bytes/op")
	b.ReportMetric(float64(tableBytes)/float64(rawBytes), "encoded/raw")
}

func benchmarkSequentialEntries(b *testing.B, count, valueSize int) []testEntry {
	b.Helper()
	entries := make([]testEntry, count)
	value := bytes.Repeat([]byte("v"), valueSize)
	for index := range entries {
		var userKey [8]byte
		binary.BigEndian.PutUint64(userKey[:], uint64(index))
		entries[index] = testEntry{key: benchmarkKey(b, userKey[:], 1), value: value}
	}
	return entries
}

func benchmarkKey(b *testing.B, userKey []byte, sequence uint64) storage.InternalKey {
	b.Helper()
	key, err := storage.NewInternalKey(userKey, sequence, storage.KindValue)
	if err != nil {
		b.Fatalf("NewInternalKey: %v", err)
	}
	return key
}

func newBenchmarkWriter() *Writer {
	file := &memoryFile{}
	return newWriter(file, "/benchmark", "/benchmark/table.tmp", "/benchmark/table.sst", 1, DefaultBlockSize, DefaultRestartInterval, publicationOps{
		rename: func(string, string) error { return nil },
		remove: func(string) error { return nil },
		openDirectory: func(string) (syncCloser, error) {
			return &fakeDirectory{}, nil
		},
	})
}
