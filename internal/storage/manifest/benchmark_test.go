package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkManifestAppendFsync(b *testing.B) {
	directory := testutil.BenchmarkDir(b)
	writer, err := wal.OpenWriter(filepath.Join(directory, manifestFileName(1)), wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()
	next := uint64(2)
	encoded, _ := EncodeVersionEdit(VersionEdit{NextFileNumber: &next})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := writer.Append(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkManifestRecovery(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("edits-%d", count), func(b *testing.B) {
			directory := testutil.BenchmarkDir(b)
			path := filepath.Join(directory, manifestFileName(1))
			writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncNone})
			if err != nil {
				b.Fatal(err)
			}
			comparator := ComparatorName
			initial, _ := EncodeVersionEdit(VersionEdit{Comparator: &comparator})
			if _, appendErr := writer.Append(initial); appendErr != nil {
				b.Fatal(appendErr)
			}
			for index := 0; index < count; index++ {
				next := uint64(index + 2)
				encoded, _ := EncodeVersionEdit(VersionEdit{NextFileNumber: &next})
				if _, appendErr := writer.Append(encoded); appendErr != nil {
					b.Fatal(appendErr)
				}
			}
			if syncErr := writer.Sync(); syncErr != nil {
				b.Fatal(syncErr)
			}
			if closeErr := writer.Close(); closeErr != nil {
				b.Fatal(closeErr)
			}
			info, err := os.Stat(path)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := recoverManifest(path); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(info.Size()), "manifest-bytes")
		})
	}
}

func BenchmarkManifestRewrite(b *testing.B) {
	directory := testutil.BenchmarkDir(b)
	store, err := Create(Options{Directory: directory})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := store.Rewrite(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVersionInstall(b *testing.B) {
	comparator, next := ComparatorName, uint64(2)
	base, err := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	if err != nil {
		b.Fatal(err)
	}
	table := benchmarkTable(1, "key")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := base.apply(VersionEdit{AddedFiles: []TableMetadata{table}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDurableTableInstall(b *testing.B) {
	directory := testutil.BenchmarkDir(b)
	tableWriter, err := sstable.OpenWriter(directory, 1, sstable.Options{})
	if err != nil {
		b.Fatal(err)
	}
	key, _ := storage.NewInternalKey([]byte("key"), 0, storage.KindValue)
	if addErr := tableWriter.Add(key, []byte("value")); addErr != nil {
		b.Fatal(addErr)
	}
	metadata, err := tableWriter.Finish()
	if err != nil {
		b.Fatal(err)
	}
	table, err := NewTableMetadata(0, 1, 0, 0, metadata)
	if err != nil {
		b.Fatal(err)
	}
	comparator, next := ComparatorName, uint64(2)
	version, _ := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	writer := &memoryManifestWriter{}
	store := &Store{directory: directory, writer: writer, current: version, logger: rlog.Discard()}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		store.current = version
		if installErr := store.Install(VersionEdit{AddedFiles: []TableMetadata{table}}); installErr != nil {
			b.Fatal(installErr)
		}
	}
}
