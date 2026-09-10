package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	mathrand "math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

var (
	errInjectedMaintenance = errors.New("injected maintenance failure")
	errUnexpectedScan      = errors.New("unexpected scan result")
)

func TestLocalLifecycleLatestStateAndRestart(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	ctx := context.Background()
	mustPut(t, e, []byte("alice"), []byte("one"))
	mustPut(t, e, []byte("bob"), []byte("two"))
	mustPut(t, e, []byte("empty"), []byte{})
	mustDelete(t, e, []byte("alice"))
	assertNotFound(t, e, []byte("alice"))
	assertValue(t, e, []byte("bob"), []byte("two"))
	assertValue(t, e, []byte("empty"), []byte{})
	assertScan(t, e, nil, nil, []KV{{Key: []byte("bob"), Value: []byte("two")}, {Key: []byte("empty"), Value: []byte{}}})
	if err := e.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	assertNotFound(t, e, []byte("alice"))
	closeTestEngine(t, e)

	e = openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	assertNotFound(t, e, []byte("alice"))
	assertValue(t, e, []byte("bob"), []byte("two"))
	assertScan(t, e, []byte("bob"), []byte("empty"), []KV{{Key: []byte("bob"), Value: []byte("two")}})
}

func TestVersionsAcrossActiveImmutableL0L1AndTombstone(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	ctx := context.Background()
	for index, value := range []string{"A", "B", "C", "D"} {
		mustPut(t, e, []byte("foo"), []byte(value))
		mustPut(t, e, []byte(fmt.Sprintf("side-%d", index)), []byte("x"))
		if err := e.Flush(ctx); err != nil {
			t.Fatalf("flush %d: %v", index, err)
		}
		assertValue(t, e, []byte("foo"), []byte(value))
	}
	assertScan(t, e, nil, nil, []KV{{Key: []byte("foo"), Value: []byte("D")}, {Key: []byte("side-0"), Value: []byte("x")}, {Key: []byte("side-1"), Value: []byte("x")}, {Key: []byte("side-2"), Value: []byte("x")}, {Key: []byte("side-3"), Value: []byte("x")}})
	beforeReads := e.Stats().GetL0TableReads
	assertValue(t, e, []byte("foo"), []byte("D"))
	if inspected := e.Stats().GetL0TableReads - beforeReads; inspected != 4 {
		t.Fatalf("overlapping L0 table reads=%d want 4", inspected)
	}
	if _, err := e.Compact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	beforeHigher := e.Stats().GetHigherTableReads
	assertValue(t, e, []byte("foo"), []byte("D"))
	if inspected := e.Stats().GetHigherTableReads - beforeHigher; inspected != 1 {
		t.Fatalf("higher-level table reads=%d want 1", inspected)
	}
	mustPut(t, e, []byte("a"), []byte("left"))
	mustPut(t, e, []byte("z"), []byte("right"))
	if err := e.Flush(ctx); err != nil {
		t.Fatalf("flush miss range: %v", err)
	}
	beforeMiss := e.Stats()
	assertNotFound(t, e, []byte("m"))
	afterMiss := e.Stats()
	if afterMiss.GetL0TableReads-beforeMiss.GetL0TableReads != 1 || afterMiss.GetHigherTableReads-beforeMiss.GetHigherTableReads != 1 {
		t.Fatalf("miss table reads L0=%d higher=%d", afterMiss.GetL0TableReads-beforeMiss.GetL0TableReads, afterMiss.GetHigherTableReads-beforeMiss.GetHigherTableReads)
	}
	mustDelete(t, e, []byte("foo"))
	assertNotFound(t, e, []byte("foo"))
	if err := e.Flush(ctx); err != nil {
		t.Fatalf("flush tombstone: %v", err)
	}
	assertNotFound(t, e, []byte("foo"))
	assertScan(t, e, []byte("foo"), []byte("foo\x00"), []KV{})
	previous := invariant.SetExpensive(true)
	defer invariant.SetExpensive(previous)
	if err := e.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestReadSnapshotsSurviveFlushAndCompactionReplacement(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 4)
	defer closeTestEngine(t, e)
	ctx := context.Background()
	for index := 0; index < 4; index++ {
		mustPut(t, e, []byte("key"), []byte{byte(index)})
		beforeFlush, err := e.captureReadView()
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		entry, ok := beforeFlush.memtables.Active.Table.GetCandidate([]byte("key"), beforeFlush.memtables.LatestSequence)
		if !ok || !bytes.Equal(entry.Value, []byte{byte(index)}) {
			t.Fatalf("old MemTable view lost value %d", index)
		}
		assertValue(t, e, []byte("key"), []byte{byte(index)})
	}
	oldView, err := e.captureReadView()
	if err != nil {
		t.Fatal(err)
	}
	oldInputs := oldView.version.Files(0)
	if _, compactErr := e.Compact(ctx); compactErr != nil {
		t.Fatal(compactErr)
	}
	var oldWinner sstable.Entry
	var haveOldWinner bool
	for _, table := range oldInputs {
		reader, openErr := sstable.Open(filepath.Join(e.directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
		if openErr != nil {
			t.Fatalf("old Version input %d unavailable: %v", table.FileNumber, openErr)
		}
		if readerCloseErr := reader.Close(); readerCloseErr != nil {
			t.Fatal(readerCloseErr)
		}
		entry, found, readErr := e.tableCandidate(table, []byte("key"), oldView.memtables.LatestSequence)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if found && (!haveOldWinner || entry.Key.Sequence() > oldWinner.Key.Sequence()) {
			oldWinner, haveOldWinner = entry, true
		}
	}
	if !haveOldWinner || !bytes.Equal(oldWinner.Value, []byte{3}) {
		t.Fatalf("old Version resolved %#v", oldWinner)
	}
	newView, err := e.captureReadView()
	if err != nil {
		t.Fatal(err)
	}
	if len(newView.version.Files(0)) != 0 || len(newView.version.Files(1)) == 0 {
		t.Fatalf("new Version levels: L0=%d L1=%d", len(newView.version.Files(0)), len(newView.version.Files(1)))
	}
	assertValue(t, e, []byte("key"), []byte{3})
}

func TestBinaryKeysAndHigherLevelRangeLookup(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 4)
	defer closeTestEngine(t, e)
	ctx := context.Background()
	keys := [][]byte{{}, {0}, {0, 0xff}, {'a'}, {'a', 0}, {0xff}}
	for round := 0; round < 4; round++ {
		for index, key := range keys {
			mustPut(t, e, key, []byte{byte(round), byte(index)})
		}
		if err := e.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if _, err := e.Compact(ctx); err != nil {
		t.Fatalf("compact: %v", err)
	}
	for index, key := range keys {
		assertValue(t, e, key, []byte{3, byte(index)})
	}
	all, err := e.Scan(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != len(keys) || !bytes.Equal(all[0].Key, []byte{}) || !bytes.Equal(all[0].Value, []byte{3, 0}) {
		t.Fatalf("empty-key scan result=%#v", all)
	}
	got, err := e.Scan(ctx, []byte{0}, []byte{0xff})
	if err != nil {
		t.Fatalf("binary scan: %v", err)
	}
	want := []KV{{Key: []byte{0}, Value: []byte{3, 1}}, {Key: []byte{0, 0xff}, Value: []byte{3, 2}}, {Key: []byte{'a'}, Value: []byte{3, 3}}, {Key: []byte{'a', 0}, Value: []byte{3, 4}}}
	assertKVs(t, got, want)
}

func TestDirectoryWithoutCurrentIsNeverAdopted(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "000000000001.wal"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	e, err := Open(Options{Directory: directory})
	if err == nil {
		_ = e.Close(context.Background())
		t.Fatal("Open adopted unauthoritative directory")
	}
	if !errors.Is(err, ErrDirectoryState) {
		t.Fatalf("Open error=%v", err)
	}
}

func TestWALReplayDoesNotReappendAndSequenceContinues(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	mustPut(t, e, []byte("before"), []byte("restart"))
	before := e.pipeline.Stats().NextSequence
	closeTestEngine(t, e)
	walPath := filepath.Join(directory, "000000000001.wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}

	e = openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	afterInfo, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat reopened WAL: %v", err)
	}
	if afterInfo.Size() != info.Size() {
		t.Fatalf("WAL replay appended bytes: before=%d after=%d", info.Size(), afterInfo.Size())
	}
	assertValue(t, e, []byte("before"), []byte("restart"))
	mustPut(t, e, []byte("after"), []byte("restart"))
	if got := e.pipeline.Stats().NextSequence; got != before+1 {
		t.Fatalf("next sequence=%d want %d", got, before+1)
	}
}

func TestValidOrphanAndTemporaryNeverAffectReads(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	mustPut(t, e, []byte("live"), []byte("yes"))
	if err := e.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	closeTestEngine(t, e)
	writeStandaloneTable(t, directory, 900, []byte("orphan"), []byte("no"))
	if err := os.WriteFile(filepath.Join(directory, "000000000901.sst.tmp"), []byte("stale"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	e = openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	assertValue(t, e, []byte("live"), []byte("yes"))
	assertNotFound(t, e, []byte("orphan"))
	assertScan(t, e, nil, nil, []KV{{Key: []byte("live"), Value: []byte("yes")}})
	discovery := e.manifest.Discovery()
	if len(discovery.Orphans) != 1 || discovery.Orphans[0] != 900 {
		t.Fatalf("orphans=%v", discovery.Orphans)
	}
	if len(discovery.Temporary) != 1 {
		t.Fatalf("temporary=%v", discovery.Temporary)
	}
}

func TestMissingOrCorruptLiveTablePreventsOpen(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "missing"
		if corrupt {
			name = "corrupt"
		}
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			e := openTestEngine(t, directory, 4)
			mustPut(t, e, []byte("live"), []byte("value"))
			if err := e.Flush(context.Background()); err != nil {
				t.Fatalf("flush: %v", err)
			}
			version, err := e.manifest.Current()
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, sstable.FileName(version.Files(0)[0].FileNumber))
			closeTestEngine(t, e)
			if corrupt {
				file, openErr := os.OpenFile(path, os.O_WRONLY, 0)
				if openErr != nil {
					t.Fatal(openErr)
				}
				if _, writeErr := file.WriteAt([]byte{0xff}, 0); writeErr != nil {
					t.Fatal(writeErr)
				}
				if closeErr := file.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			} else if removeErr := os.Remove(path); removeErr != nil {
				t.Fatal(removeErr)
			}
			opened, err := Open(Options{Directory: directory})
			if err == nil {
				_ = opened.Close(context.Background())
				t.Fatal("Open succeeded")
			}
		})
	}
}

func TestPointReadSurfacesPostOpenCorruption(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	mustPut(t, e, []byte("key"), []byte("value"))
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	version, err := e.manifest.Current()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, sstable.FileName(version.Files(0)[0].FileNumber))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(context.Background(), []byte("key")); !errors.Is(err, ErrCorruption) {
		t.Fatalf("Get corruption=%v", err)
	}
}

func TestDuplicateInternalEntryAcrossDifferentLiveFilesIsCorruption(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	mustPut(t, e, []byte("sequence-floor"), []byte("value"))
	for generation := uint64(1); generation <= 2; generation++ {
		number, err := e.manifest.AllocateFileNumber(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		metadata := writeStandaloneTable(t, directory, number, []byte("duplicate"), []byte("value"))
		table, err := manifest.NewTableMetadata(0, generation, 0, 0, metadata)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.manifest.Install(manifest.VersionEdit{AddedFiles: []manifest.TableMetadata{table}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Get(context.Background(), []byte("duplicate")); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("Get duplicate=%v", err)
	}
	if _, err := e.Scan(context.Background(), nil, nil); !errors.Is(err, ErrDuplicateEntry) {
		t.Fatalf("Scan duplicate=%v", err)
	}
}

func TestRandomizedModelFlushCompactionRestart(t *testing.T) {
	seeds := []int64{101, 9901, testutil.Seed(t)}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			directory := t.TempDir()
			e := openTestEngine(t, directory, 4)
			model := make(map[string][]byte)
			deleted := make(map[string]bool)
			rng := mathrand.New(mathrand.NewSource(seed))
			for operation := 0; operation < 300; operation++ {
				key := []byte{byte(rng.Intn(24)), byte(rng.Intn(4))}
				if rng.Intn(4) == 0 {
					mustDelete(t, e, key)
					delete(model, string(key))
					deleted[string(key)] = true
				} else {
					value := []byte{byte(operation), byte(operation >> 8), byte(rng.Intn(256))}
					mustPut(t, e, key, value)
					model[string(key)] = bytes.Clone(value)
					delete(deleted, string(key))
				}
				if operation%23 == 0 {
					verifyModel(t, e, model, deleted)
					verifyRandomRange(t, e, model, rng)
				}
				if operation%37 == 0 {
					if err := e.Flush(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				if operation%149 == 0 {
					_, err := e.Compact(context.Background())
					if err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
						t.Fatal(err)
					}
				}
				if operation > 0 && operation%75 == 0 {
					closeTestEngine(t, e)
					e = openTestEngine(t, directory, 4)
					verifyModel(t, e, model, deleted)
				}
			}
			verifyModel(t, e, model, deleted)
			closeTestEngine(t, e)
		})
	}
}

func TestFortyRestartCyclesPreserveStateAndAuthorities(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	model := make(map[string][]byte)
	deleted := make(map[string]bool)
	var previousNextFile, previousNextSequence uint64
	for cycle := 0; cycle < 40; cycle++ {
		key := fmt.Sprintf("key-%02d", cycle%11)
		if cycle%5 == 0 {
			mustDelete(t, e, []byte(key))
			delete(model, key)
			deleted[key] = true
		} else {
			value := []byte{byte(cycle)}
			mustPut(t, e, []byte(key), value)
			model[key] = value
			delete(deleted, key)
		}
		if cycle%2 == 0 {
			if err := e.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if cycle%8 == 7 {
			_, err := e.Compact(context.Background())
			if err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
				t.Fatal(err)
			}
		}
		stats := e.Stats()
		if stats.Manifest.VersionGeneration == 0 {
			t.Fatal("zero Version generation")
		}
		version, err := e.manifest.Current()
		if err != nil {
			t.Fatal(err)
		}
		if version.NextFileNumber() < previousNextFile || stats.Pipeline.NextSequence < previousNextSequence {
			t.Fatalf("authority regressed at cycle %d", cycle)
		}
		previousNextFile, previousNextSequence = version.NextFileNumber(), stats.Pipeline.NextSequence
		closeTestEngine(t, e)
		e = openTestEngine(t, directory, 4)
		verifyModel(t, e, model, deleted)
	}
	closeTestEngine(t, e)
}

func TestConcurrentReadsWritesFlushCompactionAndClose(t *testing.T) {
	defer testutil.NoLeaks(t)()
	e := openTestEngine(t, t.TempDir(), 2)
	ctx := context.Background()
	var wg sync.WaitGroup
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < 120; index++ {
				key := []byte(fmt.Sprintf("w%d-%03d", writer, index))
				if err := e.Put(ctx, key, key); err != nil {
					t.Errorf("put: %v", err)
					return
				}
			}
		}()
	}
	for reader := 0; reader < 2; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := 0; index < 120; index++ {
				_, err := e.Get(ctx, []byte(fmt.Sprintf("w%d-%03d", index%2, index)))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("get: %v", err)
					return
				}
				if _, err := e.Scan(ctx, []byte("w"), []byte("x")); err != nil {
					t.Errorf("scan: %v", err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for index := 0; index < 8; index++ {
			if err := e.Flush(ctx); err != nil {
				t.Errorf("flush: %v", err)
				return
			}
			_, err := e.Compact(ctx)
			if err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
				t.Errorf("compact: %v", err)
				return
			}
			if _, err := e.ReclaimObsoleteTables(ctx); err != nil {
				t.Errorf("reclaim: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	for writer := 0; writer < 2; writer++ {
		for index := 0; index < 120; index++ {
			key := []byte(fmt.Sprintf("w%d-%03d", writer, index))
			assertValue(t, e, key, key)
		}
	}
	closeTestEngine(t, e)
	if err := e.Put(ctx, []byte("closed"), []byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Put=%v", err)
	}
	if _, err := e.Get(ctx, []byte("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Get=%v", err)
	}
	if err := e.Delete(ctx, []byte("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Delete=%v", err)
	}
	if _, err := e.Scan(ctx, nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Scan=%v", err)
	}
	if err := e.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Flush=%v", err)
	}
	if _, err := e.Compact(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close Compact=%v", err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatalf("repeated Close=%v", err)
	}
}

func TestCloseRacesWithActiveOperations(t *testing.T) {
	defer testutil.NoLeaks(t)()
	e := openTestEngine(t, t.TempDir(), 4)
	ctx := context.Background()
	for index := 0; index < 40; index++ {
		mustPut(t, e, []byte(fmt.Sprintf("key-%d", index)), []byte("value"))
	}
	start := make(chan struct{})
	results := make(chan error, 16)
	for worker := 0; worker < 16; worker++ {
		go func() {
			<-start
			if worker%2 == 0 {
				_, err := e.Get(ctx, []byte("key-1"))
				results <- err
				return
			}
			_, err := e.Scan(ctx, nil, nil)
			results <- err
		}()
	}
	close(start)
	closeErr := e.Close(ctx)
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	for worker := 0; worker < 16; worker++ {
		err := <-results
		if err != nil && !errors.Is(err, ErrClosed) {
			t.Fatalf("racing operation: %v", err)
		}
	}
}

func TestCloseIgnoresCancellationAfterShutdownBegins(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 4)
	mustPut(t, e, []byte("still-open"), []byte("value"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Close(ctx); err != nil {
		t.Fatalf("canceled Close cleanup: %v", err)
	}
	if _, err := e.Get(context.Background(), []byte("still-open")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close=%v", err)
	}
}

func TestEngineBatchVisibilityUsesPublishedHighWater(t *testing.T) {
	stages := []pipeline.WriteStage{
		pipeline.WriteStageAssigned,
		pipeline.WriteStageWALWritten,
		pipeline.WriteStageWALDurable,
		pipeline.WriteStageApplyStarted,
		pipeline.WriteStageApplyCompleted,
		pipeline.WriteStagePublished,
	}
	for _, target := range stages {
		t.Run(target.String(), func(t *testing.T) {
			reached := make(chan struct{})
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var once sync.Once
			directory := t.TempDir()
			initial := openTestEngine(t, directory, 4)
			mustPut(t, initial, []byte("a"), []byte("old-a"))
			mustPut(t, initial, []byte("b"), []byte("old-b"))
			mustPut(t, initial, []byte("c"), []byte("old-c"))
			closeTestEngine(t, initial)
			e, err := Open(Options{
				Directory: directory, MemTableBytes: 1 << 20, MaxImmutables: 4,
				WriteHook: func(stage pipeline.WriteStage, _ pipeline.WriteResult) {
					if stage == target {
						once.Do(func() { close(reached); <-release })
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestEngine(t, e)

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- e.WriteBatch(context.Background(), []storage.Mutation{
					{Key: []byte("a"), Value: []byte("new-a"), Kind: storage.KindValue},
					{Key: []byte("b"), Value: []byte("new-b"), Kind: storage.KindValue},
					{Key: []byte("c"), Kind: storage.KindDelete},
				})
			}()
			<-reached
			stats := e.Stats().Pipeline
			if !stats.HaveAssigned || stats.LastAssigned != 5 {
				t.Fatalf("assigned authority=%+v", stats)
			}
			if target == pipeline.WriteStagePublished {
				if !stats.HaveVisible || stats.VisibleSequence != 5 {
					t.Fatalf("published authority=%+v", stats)
				}
				assertValue(t, e, []byte("a"), []byte("new-a"))
				assertValue(t, e, []byte("b"), []byte("new-b"))
				assertNotFound(t, e, []byte("c"))
				assertScan(t, e, nil, nil, []KV{{Key: []byte("a"), Value: []byte("new-a")}, {Key: []byte("b"), Value: []byte("new-b")}})
			} else {
				if !stats.HaveVisible || stats.VisibleSequence != 2 {
					t.Fatalf("pre-publication authority=%+v", stats)
				}
				assertValue(t, e, []byte("a"), []byte("old-a"))
				assertValue(t, e, []byte("b"), []byte("old-b"))
				assertValue(t, e, []byte("c"), []byte("old-c"))
				assertScan(t, e, nil, nil, []KV{{Key: []byte("a"), Value: []byte("old-a")}, {Key: []byte("b"), Value: []byte("old-b")}, {Key: []byte("c"), Value: []byte("old-c")}})
			}
			close(release)
			if err := <-writeDone; err != nil {
				t.Fatal(err)
			}
			assertValue(t, e, []byte("a"), []byte("new-a"))
			assertValue(t, e, []byte("b"), []byte("new-b"))
			assertNotFound(t, e, []byte("c"))
		})
	}
}

func TestScanVersionLifetimeBlocksObsoleteTableReclamation(t *testing.T) {
	captured := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	reclaimAttempted := make(chan struct{})
	var captureOnce, reclaimOnce sync.Once
	directory := t.TempDir()
	e, openErr := Open(Options{
		Directory: directory, MemTableBytes: 1 << 20, MaxImmutables: 4, L0Trigger: 4,
		ReadHook: func(stage ReadStage, _ uint64) {
			if stage == ReadStageScanViewCaptured {
				captureOnce.Do(func() { close(captured); <-release })
			}
		},
		MaintenanceHook: func(stage MaintenanceStage) {
			if stage == MaintenanceStageTableReclaimAttempt {
				reclaimOnce.Do(func() { close(reclaimAttempted) })
			}
		},
	})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer func() { _ = e.Close(context.Background()) }()
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
	oldFiles := version.LiveFileNumbers()
	scanDone := make(chan error, 1)
	go func() {
		values, scanErr := e.Scan(context.Background(), nil, nil)
		if scanErr == nil && (len(values) != 1 || !bytes.Equal(values[0].Value, []byte{3})) {
			scanErr = errUnexpectedScan
		}
		scanDone <- scanErr
	}()
	<-captured
	if _, err := e.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	reclaimDone := make(chan struct {
		result TableReclamationResult
		err    error
	}, 1)
	go func() {
		result, reclaimErr := e.ReclaimObsoleteTables(context.Background())
		reclaimDone <- struct {
			result TableReclamationResult
			err    error
		}{result, reclaimErr}
	}()
	<-reclaimAttempted
	select {
	case outcome := <-reclaimDone:
		t.Fatalf("reclamation passed active scan: %+v %v", outcome.result, outcome.err)
	default:
	}
	for _, number := range oldFiles {
		if _, err := os.Stat(filepath.Join(directory, sstable.FileName(number))); err != nil {
			t.Fatalf("old-reader file %d removed early: %v", number, err)
		}
	}
	close(release)
	if err := <-scanDone; err != nil {
		t.Fatalf("old scan: %v", err)
	}
	outcome := <-reclaimDone
	if outcome.err != nil || outcome.result.Deleted != uint64(len(oldFiles)) {
		t.Fatalf("reclaim=%+v err=%v", outcome.result, outcome.err)
	}
	for _, number := range oldFiles {
		if _, err := os.Stat(filepath.Join(directory, sstable.FileName(number))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("obsolete file %d still exists: %v", number, err)
		}
	}
	assertValue(t, e, []byte("key"), []byte{3})
	second, reclaimErr := e.ReclaimObsoleteTables(context.Background())
	if reclaimErr != nil || second.Deleted != 0 {
		t.Fatalf("idempotent reclaim=%+v err=%v", second, reclaimErr)
	}
	closeTestEngine(t, e)
	e = openTestEngine(t, directory, 4)
	assertValue(t, e, []byte("key"), []byte{3})
}

func TestObsoleteTableDeletionFailureIsMaintenanceOnly(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 4)
	defer closeTestEngine(t, e)
	for index := 0; index < 4; index++ {
		mustPut(t, e, []byte("key"), []byte{byte(index)})
		if err := e.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.removeFile = func(string) error { return errInjectedMaintenance }
	result, err := e.ReclaimObsoleteTables(context.Background())
	if !errors.Is(err, errInjectedMaintenance) || result.Retained != 4 {
		t.Fatalf("failed reclaim=%+v err=%v", result, err)
	}
	assertValue(t, e, []byte("key"), []byte{3})
	e.removeFile = os.Remove
	result, err = e.ReclaimObsoleteTables(context.Background())
	if err != nil || result.Deleted != 4 {
		t.Fatalf("retry reclaim=%+v err=%v", result, err)
	}
}

func TestWALReclamationCoverageIsCandidateOnly(t *testing.T) {
	e := openTestEngine(t, t.TempDir(), 4)
	defer closeTestEngine(t, e)
	mustPut(t, e, []byte("flushed"), []byte("value"))
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := e.InspectWALReclamation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.HaveFrontier || status.Frontier != 0 || status.PhysicalDeletionEnabled || len(status.Segments) != 1 || !status.Segments[0].EntirelyAtOrBelowFrontier {
		t.Fatalf("flushed status=%+v", status)
	}
	mustPut(t, e, []byte("active"), []byte("value"))
	status, err = e.InspectWALReclamation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Segments[0].EntirelyAtOrBelowFrontier || status.Segments[0].LargestSequence != 1 {
		t.Fatalf("mixed status=%+v", status)
	}

	path := filepath.Join(t.TempDir(), "straddle.wal")
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := storage.EncodeWriteBatch(storage.WriteBatch{FirstSequence: 0, Mutations: []storage.Mutation{putMutation("a"), putMutation("b"), putMutation("c")}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(encoded); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectWALFile(path, 1, true); !errors.Is(err, manifest.ErrFrontierSplitsBatch) {
		t.Fatalf("straddling coverage=%v", err)
	}
}

func TestSubprocessCrashBoundaries(t *testing.T) {
	if directory := os.Getenv("RIVETDB_CRASH_CHILD_DIR"); directory != "" {
		target := os.Getenv("RIVETDB_CRASH_CHILD_STAGE")
		e, err := Open(Options{Directory: directory, WriteHook: func(stage pipeline.WriteStage, _ pipeline.WriteResult) {
			if stage.String() == target {
				os.Exit(86)
			}
		}})
		if err != nil {
			t.Fatal(err)
		}
		_ = e.WriteBatch(context.Background(), []storage.Mutation{{Key: []byte("a"), Value: []byte("new-a"), Kind: storage.KindValue}, {Key: []byte("b"), Value: []byte("new-b"), Kind: storage.KindValue}, {Key: []byte("c"), Kind: storage.KindDelete}})
		t.Fatalf("child did not crash at %s", target)
	}
	for _, test := range []struct {
		stage   pipeline.WriteStage
		durable bool
	}{{pipeline.WriteStageAssigned, false}, {pipeline.WriteStageWALWritten, true}, {pipeline.WriteStageWALDurable, true}, {pipeline.WriteStageApplyCompleted, true}, {pipeline.WriteStagePublished, true}} {
		t.Run(test.stage.String(), func(t *testing.T) {
			directory := t.TempDir()
			initial := openTestEngine(t, directory, 4)
			mustPut(t, initial, []byte("c"), []byte("old-c"))
			closeTestEngine(t, initial)
			command := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestSubprocessCrashBoundaries$")
			command.Env = append(os.Environ(), "RIVETDB_CRASH_CHILD_DIR="+directory, "RIVETDB_CRASH_CHILD_STAGE="+test.stage.String())
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 86 {
				t.Fatalf("child exit=%v output=%s", err, output)
			}
			recovered := openTestEngine(t, directory, 4)
			if test.durable {
				assertValue(t, recovered, []byte("a"), []byte("new-a"))
				assertValue(t, recovered, []byte("b"), []byte("new-b"))
				assertNotFound(t, recovered, []byte("c"))
			} else {
				assertNotFound(t, recovered, []byte("a"))
				assertNotFound(t, recovered, []byte("b"))
				assertValue(t, recovered, []byte("c"), []byte("old-c"))
			}
			closeTestEngine(t, recovered)
		})
	}
}

func TestRepeatedOpenOfCorruptCurrentIsDeterministicAndNonMutating(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	mustPut(t, e, []byte("durable"), []byte("value"))
	closeTestEngine(t, e)
	if err := os.WriteFile(filepath.Join(directory, manifest.CurrentFileName), []byte("../guessed-manifest\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := directoryPhysicalDigest(t, directory)
	var first string
	for attempt := 0; attempt < 2; attempt++ {
		opened, err := Open(Options{Directory: directory})
		if err == nil {
			_ = opened.Close(context.Background())
			t.Fatal("Open accepted corrupt CURRENT")
		}
		if !errors.Is(err, manifest.ErrCurrentCorrupt) {
			t.Fatalf("attempt %d error=%v", attempt, err)
		}
		if attempt == 0 {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("Open errors differ: %q != %q", err, first)
		}
		if got := directoryPhysicalDigest(t, directory); got != want {
			t.Fatalf("attempt %d mutated directory: %x != %x", attempt, got, want)
		}
	}
}

func TestCloseRacesWithObsoleteTableMaintenance(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 2)
	for index := 0; index < 2; index++ {
		mustPut(t, e, []byte("key"), []byte{byte(index)})
		if err := e.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	maintenanceDone := make(chan error, 1)
	closeDone := make(chan error, 1)
	go func() {
		<-start
		_, err := e.ReclaimObsoleteTables(context.Background())
		maintenanceDone <- err
	}()
	go func() {
		<-start
		closeDone <- e.Close(context.Background())
	}()
	close(start)
	if err := <-maintenanceDone; err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("racing maintenance=%v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("racing Close=%v", err)
	}
	reopened := openTestEngine(t, directory, 2)
	defer closeTestEngine(t, reopened)
	assertValue(t, reopened, []byte("key"), []byte{1})
}

func openTestEngine(t testing.TB, directory string, trigger int) *Engine {
	t.Helper()
	e, err := Open(Options{Directory: directory, MemTableBytes: 1 << 20, MaxImmutables: 4, L0Trigger: trigger, TargetFileSize: 1 << 20})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}
func closeTestEngine(t testing.TB, e *Engine) {
	t.Helper()
	if err := e.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
func mustPut(t testing.TB, e *Engine, key, value []byte) {
	t.Helper()
	if err := e.Put(context.Background(), key, value); err != nil {
		t.Fatalf("Put(%x): %v", key, err)
	}
}
func mustDelete(t testing.TB, e *Engine, key []byte) {
	t.Helper()
	if err := e.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete(%x): %v", key, err)
	}
}
func assertValue(t testing.TB, e *Engine, key, want []byte) {
	t.Helper()
	got, err := e.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%x): %v", key, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%x)=%x want %x", key, got, want)
	}
}
func assertNotFound(t testing.TB, e *Engine, key []byte) {
	t.Helper()
	if value, err := e.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%x)=(%x,%v), want not found", key, value, err)
	}
}
func assertScan(t testing.TB, e *Engine, start, end []byte, want []KV) {
	t.Helper()
	got, err := e.Scan(context.Background(), start, end)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	assertKVs(t, got, want)
}
func assertKVs(t testing.TB, got, want []KV) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("KVs len=%d want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if !bytes.Equal(got[index].Key, want[index].Key) || !bytes.Equal(got[index].Value, want[index].Value) {
			t.Fatalf("KV[%d]=%x:%x want %x:%x", index, got[index].Key, got[index].Value, want[index].Key, want[index].Value)
		}
	}
}

func verifyModel(t testing.TB, e *Engine, model map[string][]byte, deleted map[string]bool) {
	t.Helper()
	for key, value := range model {
		assertValue(t, e, []byte(key), value)
	}
	for key := range deleted {
		assertNotFound(t, e, []byte(key))
	}
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := make([]KV, 0, len(keys))
	for _, key := range keys {
		want = append(want, KV{Key: []byte(key), Value: model[key]})
	}
	assertScan(t, e, nil, nil, want)
}

func verifyRandomRange(t testing.TB, e *Engine, model map[string][]byte, rng *mathrand.Rand) {
	t.Helper()
	start := []byte{byte(rng.Intn(24)), byte(rng.Intn(4))}
	end := []byte{byte(rng.Intn(24)), byte(rng.Intn(4))}
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}
	keys := make([]string, 0)
	for key := range model {
		encoded := []byte(key)
		if bytes.Compare(encoded, start) >= 0 && bytes.Compare(encoded, end) < 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	want := make([]KV, 0, len(keys))
	for _, key := range keys {
		want = append(want, KV{Key: []byte(key), Value: model[key]})
	}
	assertScan(t, e, start, end, want)
}

func writeStandaloneTable(t testing.TB, directory string, number uint64, key, value []byte) sstable.Metadata {
	t.Helper()
	writer, err := sstable.OpenWriter(directory, number, sstable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	internal, err := storage.NewInternalKey(key, 0, storage.KindValue)
	if err != nil {
		t.Fatal(err)
	}
	if addErr := writer.Add(internal, value); addErr != nil {
		t.Fatal(addErr)
	}
	metadata, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func putMutation(key string) storage.Mutation {
	return storage.Mutation{Key: []byte(key), Value: []byte("value-" + key), Kind: storage.KindValue}
}
