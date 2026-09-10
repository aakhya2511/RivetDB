package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

func TestTableCacheReusesReadersAndStaysBounded(t *testing.T) {
	directory := t.TempDir()
	e, err := Open(Options{Directory: directory, MemTableBytes: 1 << 20, MaxImmutables: 4, L0Trigger: 64, TargetFileSize: 1 << 20, TableCacheCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer closeTestEngine(t, e)

	for index, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		mustPut(t, e, key, []byte{byte(index)})
		if err := e.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range [][]byte{[]byte("a"), []byte("a"), []byte("b"), []byte("c")} {
		assertValue(t, e, key, []byte{key[0] - 'a'})
	}
	stats := e.Stats()
	if stats.TableCacheHits != 1 || stats.TableCacheMisses != 3 || stats.TableOpens != 3 {
		t.Fatalf("cache reuse stats=%+v", stats)
	}
	if stats.CachedTableReaders != 2 || stats.TableCacheEvictions != 1 {
		t.Fatalf("cache bound stats=%+v", stats)
	}
}

func TestBorrowedObsoleteTableSurvivesUntilLeaseRelease(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	for index := 0; index < 4; index++ {
		mustPut(t, e, []byte("key"), []byte{byte(index)})
		if err := e.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	version, err := e.manifest.Current()
	if err != nil {
		t.Fatal(err)
	}
	inputs := version.Files(0)
	lease, err := e.tableCache.acquire(inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, compactErr := e.Compact(context.Background()); compactErr != nil {
		t.Fatal(compactErr)
	}

	first, err := e.ReclaimObsoleteTables(context.Background())
	if err != nil || first.Deleted != 3 || first.Retained != 1 {
		t.Fatalf("reclaim with lease=%+v err=%v", first, err)
	}
	leasedPath := filepath.Join(directory, sstable.FileName(inputs[0].FileNumber))
	if _, statErr := os.Stat(leasedPath); statErr != nil {
		t.Fatalf("borrowed obsolete table removed: %v", statErr)
	}
	entry, err := lease.Reader.GetCandidate([]byte("key"), ^uint64(0))
	if err != nil || len(entry.Value) != 1 {
		t.Fatalf("borrowed reader unusable: entry=%+v err=%v", entry, err)
	}
	if releaseErr := lease.Release(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if releaseErr := lease.Release(); releaseErr != nil {
		t.Fatalf("idempotent release: %v", releaseErr)
	}

	second, err := e.ReclaimObsoleteTables(context.Background())
	if err != nil || second.Deleted != 1 {
		t.Fatalf("reclaim after release=%+v err=%v", second, err)
	}
	if _, err := os.Stat(leasedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released obsolete table remains: %v", err)
	}
}

func TestEngineCloseDrainsTableCache(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 64)
	mustPut(t, e, []byte("key"), []byte("value"))
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertValue(t, e, []byte("key"), []byte("value"))
	if got := e.Stats().CachedTableReaders; got != 1 {
		t.Fatalf("cached readers=%d want 1", got)
	}
	closeTestEngine(t, e)
	if got := e.tableCache.stats().CachedReaders; got != 0 {
		t.Fatalf("cached readers after Close=%d", got)
	}
}
