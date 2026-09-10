package wal

import (
	"bytes"
	"hash/crc32"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkAppend(b *testing.B) {
	benchmarks := []struct {
		name       string
		size       int
		durability Durability
	}{
		{name: "small/no-sync", size: 128, durability: SyncNone},
		{name: "small/sync", size: 128, durability: SyncBatch},
		{name: "medium/no-sync", size: 4 << 10, durability: SyncNone},
		{name: "large/no-sync", size: 1 << 20, durability: SyncNone},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			path := filepath.Join(testutil.BenchmarkDir(b), "bench.log")
			writer, err := OpenWriter(path, WriterOptions{Durability: benchmark.durability})
			if err != nil {
				b.Fatalf("OpenWriter: %v", err)
			}
			defer writer.Close() //nolint:errcheck // benchmark reports append failures directly
			payload := bytes.Repeat([]byte{0xa5}, benchmark.size)
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for range b.N {
				if _, err := writer.Append(payload); err != nil {
					b.Fatalf("Append: %v", err)
				}
			}
		})
	}
}

func BenchmarkSequentialRead(b *testing.B) {
	path := filepath.Join(testutil.BenchmarkDir(b), "bench.log")
	writer, err := OpenWriter(path, WriterOptions{Durability: SyncNone})
	if err != nil {
		b.Fatalf("OpenWriter: %v", err)
	}
	payload := bytes.Repeat([]byte{0x5a}, 4<<10)
	const records = 1_000
	for range records {
		if _, err := writer.Append(payload); err != nil {
			b.Fatalf("Append setup: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("Close setup: %v", err)
	}
	b.SetBytes(records * int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		reader, err := OpenReader(path)
		if err != nil {
			b.Fatalf("OpenReader: %v", err)
		}
		count := 0
		for reader.Next() {
			count++
		}
		if err := reader.Err(); err != nil {
			b.Fatalf("scan: %v", err)
		}
		if err := reader.Close(); err != nil {
			b.Fatalf("Close reader: %v", err)
		}
		if count != records {
			b.Fatalf("read %d records, want %d", count, records)
		}
	}
}

func BenchmarkChecksum(b *testing.B) {
	for _, size := range []int{128, 4 << 10, 1 << 20} {
		payload := bytes.Repeat([]byte{0xcc}, size)
		b.Run(formatOffset(int64(size)), func(b *testing.B) {
			b.SetBytes(int64(size))
			for range b.N {
				_ = crc32.Checksum(payload, checksumTable)
			}
		})
	}
}
