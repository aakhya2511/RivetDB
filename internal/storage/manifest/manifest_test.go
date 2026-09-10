package manifest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
)

func TestVersionEditRoundTripDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	table := syntheticTable(t, 7, 3, 10, 12, "a", "z")
	comparator, next, last, frontier := ComparatorName, uint64(9), uint64(12), uint64(12)
	edit := VersionEdit{Comparator: &comparator, NextFileNumber: &next, LastSequence: &last, ReplayFrontier: &frontier, AddedFiles: []TableMetadata{table}}
	first, err := EncodeVersionEdit(edit)
	if err != nil {
		t.Fatalf("EncodeVersionEdit: %v", err)
	}
	decoded, err := DecodeVersionEdit(first)
	if err != nil {
		t.Fatalf("DecodeVersionEdit: %v", err)
	}
	second, err := EncodeVersionEdit(decoded)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("encoding is not deterministic")
	}
	for cut := range first {
		if _, err := DecodeVersionEdit(first[:cut]); err == nil {
			t.Fatalf("truncation %d accepted", cut)
		}
	}
	mutated := bytes.Clone(first)
	mutated[4]++
	if _, err := DecodeVersionEdit(mutated); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("version error = %v", err)
	}
	if _, err := DecodeVersionEdit(make([]byte, MaxEditSize+1)); !errors.Is(err, ErrInvalidEdit) {
		t.Fatalf("oversize error = %v", err)
	}
}

func TestVersionCopyOnWriteOrderingAndAtomicRejection(t *testing.T) {
	t.Parallel()
	comparator, next := ComparatorName, uint64(20)
	base, err := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	if err != nil {
		t.Fatal(err)
	}
	old := base
	newer := syntheticTable(t, 2, 8, 20, 29, "b", "c")
	older := syntheticTable(t, 1, 7, 10, 19, "a", "b")
	last := uint64(29)
	current, err := base.apply(VersionEdit{AddedFiles: []TableMetadata{older, newer}, LastSequence: &last})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	files := current.Files(0)
	if files[0].FileNumber != 2 || files[1].FileNumber != 1 {
		t.Fatalf("L0 order = %v, %v", files[0].FileNumber, files[1].FileNumber)
	}
	if old.LiveTableCount() != 0 {
		t.Fatal("old Version mutated")
	}
	bad := syntheticTable(t, 3, 9, 31, 35, "c", "d")
	badLast, badFrontier := uint64(35), uint64(35)
	if _, err := current.apply(VersionEdit{AddedFiles: []TableMetadata{bad}, LastSequence: &badLast, ReplayFrontier: &badFrontier}); !errors.Is(err, ErrFrontierGap) {
		t.Fatalf("gap error = %v", err)
	}
	if current.LiveTableCount() != 2 {
		t.Fatal("rejected edit mutated current Version")
	}

	l1a := syntheticTable(t, 4, 1, 0, 0, "a", "m")
	l1a.Level = 1
	l1b := syntheticTable(t, 5, 1, 0, 0, "m", "z")
	l1b.Level = 1
	if _, err := base.apply(VersionEdit{AddedFiles: []TableMetadata{l1a, l1b}}); !errors.Is(err, ErrOverlappingLevel) {
		t.Fatalf("overlap error = %v", err)
	}
}

func TestContiguousReplayFrontierStopsAtGapThenCloses(t *testing.T) {
	comparator, next := ComparatorName, uint64(4)
	version, err := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	if err != nil {
		t.Fatal(err)
	}
	first := syntheticTable(t, 1, 1, 0, 99, "a", "a")
	last, frontier := uint64(299), uint64(99)
	version, err = version.apply(VersionEdit{AddedFiles: []TableMetadata{first}, LastSequence: &last, ReplayFrontier: &frontier})
	if err != nil {
		t.Fatal(err)
	}
	later := syntheticTable(t, 3, 3, 200, 299, "c", "c")
	version, err = version.apply(VersionEdit{AddedFiles: []TableMetadata{later}})
	if err != nil {
		t.Fatal(err)
	}
	if maximum, ok := version.maximumContiguousFrontier(); !ok || maximum != 99 {
		t.Fatalf("frontier across gap=%d/%v", maximum, ok)
	}
	middle := syntheticTable(t, 2, 2, 100, 199, "b", "b")
	frontier = 299
	version, err = version.apply(VersionEdit{AddedFiles: []TableMetadata{middle}, ReplayFrontier: &frontier})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := version.ReplayFrontier(); !ok || got != 299 {
		t.Fatalf("closed frontier=%d/%v", got, ok)
	}
}

func TestManifestCrashAtEveryOffsetAndMiddleCorruption(t *testing.T) {
	if os.Getenv("RIVETDB_EXHAUSTIVE") == "" {
		t.Skip("set RIVETDB_EXHAUSTIVE=1 for every-byte Manifest truncation")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, manifestFileName(1))
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	comparator, next := ComparatorName, uint64(1)
	one, _ := EncodeVersionEdit(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	position1, err := writer.Append(one)
	if err != nil {
		t.Fatal(err)
	}
	next = 8
	two, _ := EncodeVersionEdit(VersionEdit{NextFileNumber: &next})
	position2, err := writer.Append(two)
	if err != nil {
		t.Fatal(err)
	}
	next = 10
	three, _ := EncodeVersionEdit(VersionEdit{NextFileNumber: &next})
	position3, err := writer.Append(three)
	if err != nil {
		t.Fatal(err)
	}
	if syncErr := writer.Sync(); syncErr != nil {
		t.Fatal(syncErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	whole, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(whole); cut++ {
		candidate := filepath.Join(directory, fmt.Sprintf("cut-%d", cut))
		if err := os.WriteFile(candidate, whole[:cut], 0o600); err != nil {
			t.Fatal(err)
		}
		recovered, recoverErr := recoverManifest(candidate)
		if int64(cut) < position1.End {
			if recoverErr == nil {
				t.Fatalf("cut %d accepted without initial record", cut)
			}
			continue
		}
		if recoverErr != nil {
			t.Fatalf("cut %d: %v", cut, recoverErr)
		}
		want := uint64(1)
		if int64(cut) >= position2.End {
			want = 8
		}
		if int64(cut) >= position3.End {
			want = 10
		}
		if recovered.version.NextFileNumber() != want {
			t.Fatalf("cut %d next=%d want=%d", cut, recovered.version.NextFileNumber(), want)
		}
	}
	corrupt := bytes.Clone(whole)
	corrupt[int(position2.Start)+wal.HeaderSize] ^= 0x40
	bad := filepath.Join(directory, "corrupt")
	if err := os.WriteFile(bad, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := recoverManifest(bad); !errors.Is(err, ErrManifestCorrupt) {
		t.Fatalf("corruption error = %v", err)
	}
}

func TestChecksumValidSemanticCorruptionFailsRecovery(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, manifestFileName(1))
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatal(err)
	}
	comparator, next := ComparatorName, uint64(10)
	initial, _ := EncodeVersionEdit(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	if _, appendErr := writer.Append(initial); appendErr != nil {
		t.Fatal(appendErr)
	}
	regressed := uint64(9)
	invalid, _ := EncodeVersionEdit(VersionEdit{NextFileNumber: &regressed})
	if _, appendErr := writer.Append(invalid); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if _, recoverErr := recoverManifest(path); !errors.Is(recoverErr, ErrManifestCorrupt) || !errors.Is(recoverErr, ErrFileNumberCollision) {
		t.Fatalf("recovery error=%v", recoverErr)
	}
}

func TestCreateInstallRecoverRewriteAndDiscovery(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	file, err := store.AllocateFileNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	metadata := writeTable(t, directory, file, 0, "key", "value")
	table, err := NewTableMetadata(0, 1, 0, 0, metadata)
	if err != nil {
		t.Fatal(err)
	}
	last, frontier := uint64(0), uint64(0)
	if installErr := store.Install(VersionEdit{AddedFiles: []TableMetadata{table}, LastSequence: &last, ReplayFrontier: &frontier}); installErr != nil {
		t.Fatalf("Install: %v", installErr)
	}
	before, _ := store.Current()
	if rewriteErr := store.Rewrite(); rewriteErr != nil {
		t.Fatalf("Rewrite: %v", rewriteErr)
	}
	if before.LiveTableCount() != 1 {
		t.Fatal("rewrite mutated prior Version")
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()
	version, _ := reopened.Current()
	if version.LiveTableCount() != 1 || version.Files(0)[0].FileNumber != file {
		t.Fatalf("recovered files = %+v", version.Files(0))
	}
	if _, err := os.Stat(filepath.Join(directory, manifestFileName(1))); err != nil {
		t.Fatalf("old Manifest was not retained: %v", err)
	}
	if reopened.Stats().ManifestNumber != 2 {
		t.Fatalf("current Manifest = %d", reopened.Stats().ManifestNumber)
	}
}

func TestOrphansTempsMissingCorruptAndNumberRecovery(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	writeTable(t, directory, 72, 0, "orphan", "v")
	if writeErr := os.WriteFile(filepath.Join(directory, sstable.FileName(73)+".tmp"), []byte("partial"), 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
	reopened, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	report := reopened.Discovery()
	if len(report.Orphans) != 1 || report.Orphans[0] != 72 || len(report.Temporary) != 1 {
		t.Fatalf("discovery = %+v", report)
	}
	number, err := reopened.AllocateFileNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if number != 74 {
		t.Fatalf("allocation=%d want=74", number)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	missingDir := t.TempDir()
	s, _ := Create(Options{Directory: missingDir})
	n, _ := s.AllocateFileNumber(context.Background())
	meta := writeTable(t, missingDir, n, 0, "x", "y")
	tm, _ := NewTableMetadata(0, 1, 0, 0, meta)
	last, front := uint64(0), uint64(0)
	if err := s.Install(VersionEdit{AddedFiles: []TableMetadata{tm}, LastSequence: &last, ReplayFrontier: &front}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := os.Remove(filepath.Join(missingDir, sstable.FileName(n))); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Directory: missingDir}); !errors.Is(err, ErrMissingLiveTable) {
		t.Fatalf("missing error=%v", err)
	}
}

func TestPipelineInstallAndReplayFrontier(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	p, err := pipeline.Open(pipeline.Options{Directory: directory, MemTableBytes: 1, MaxImmutables: 2, FileAllocator: store, TableInstaller: store})
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.Write(context.Background(), []storage.Mutation{{Key: []byte("k"), Value: []byte("v"), Kind: storage.KindValue}})
	if err != nil {
		t.Fatal(err)
	}
	if drainErr := p.Drain(context.Background()); drainErr != nil {
		t.Fatalf("Drain: %v", drainErr)
	}
	if closeErr := p.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	version, _ := store.Current()
	frontier, ok := version.ReplayFrontier()
	if !ok || frontier != result.LastSequence || version.LiveTableCount() != 1 {
		t.Fatalf("version frontier=%d/%v tables=%d", frontier, ok, version.LiveTableCount())
	}
	table := memtable.New()
	applied, err := ReplayWAL(filepath.Join(directory, pipeline.WALFileName), &frontier, table)
	if err != nil || applied != 0 {
		t.Fatalf("ReplayWAL applied=%d err=%v", applied, err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if next, exhausted := reopened.RecoveredNextSequence(); exhausted || next != result.LastSequence+1 {
		t.Fatalf("next=%d exhausted=%v", next, exhausted)
	}
}

func TestReplayWALSkipsWholeBatchesAndRejectsSplit(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "batches.wal")
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range []storage.WriteBatch{
		{FirstSequence: 0, Mutations: []storage.Mutation{{Key: []byte("a"), Value: []byte("0"), Kind: storage.KindValue}, {Key: []byte("b"), Value: []byte("1"), Kind: storage.KindValue}}},
		{FirstSequence: 2, Mutations: []storage.Mutation{{Key: []byte("c"), Value: []byte("2"), Kind: storage.KindValue}}},
	} {
		encoded, encodeErr := storage.EncodeWriteBatch(batch)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if _, appendErr := writer.Append(encoded); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	split := uint64(0)
	if _, replayErr := ReplayWAL(path, &split, memtable.New()); !errors.Is(replayErr, ErrFrontierSplitsBatch) {
		t.Fatalf("split error=%v", replayErr)
	}
	frontier := uint64(1)
	table := memtable.New()
	applied, replayErr := ReplayWAL(path, &frontier, table)
	if replayErr != nil || applied != 1 || table.Len() != 1 {
		t.Fatalf("applied=%d len=%d error=%v", applied, table.Len(), replayErr)
	}
}

func TestConcurrentInstallationsAreSerializedWithoutLoss(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const count = 32
	installations := make([]pipeline.TableInstallation, count)
	for index := range count {
		file, allocateErr := store.AllocateFileNumber(context.Background())
		if allocateErr != nil {
			t.Fatal(allocateErr)
		}
		metadata := writeTable(t, directory, file, uint64(index), fmt.Sprintf("key-%03d", index), "value")
		installations[index] = pipeline.TableInstallation{Generation: uint64(index + 1), SmallestSequence: uint64(index), LargestSequence: uint64(index), Metadata: metadata, Path: filepath.Join(directory, sstable.FileName(file))}
	}
	var group sync.WaitGroup
	failures := make(chan error, count)
	for index := range count {
		group.Add(1)
		go func(installation pipeline.TableInstallation) {
			defer group.Done()
			failures <- store.InstallTable(context.Background(), installation)
		}(installations[count-index-1])
	}
	group.Wait()
	close(failures)
	for installErr := range failures {
		if installErr != nil {
			t.Fatalf("InstallTable: %v", installErr)
		}
	}
	version, _ := store.Current()
	frontier, ok := version.ReplayFrontier()
	if version.LiveTableCount() != count || !ok || frontier != count-1 {
		t.Fatalf("tables=%d frontier=%d/%v", version.LiveTableCount(), frontier, ok)
	}
}

func TestRecoveryUsesWALSequenceMaximum(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	writer, err := wal.OpenWriter(filepath.Join(directory, "retained.wal"), wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatal(err)
	}
	batch, _ := storage.EncodeWriteBatch(storage.WriteBatch{FirstSequence: 41, Mutations: []storage.Mutation{{Key: []byte("k"), Value: []byte("v"), Kind: storage.KindValue}}})
	if _, appendErr := writer.Append(batch); appendErr != nil {
		t.Fatal(appendErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	recovered, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	next, exhausted := recovered.RecoveredNextSequence()
	if exhausted || next != 42 {
		t.Fatalf("next=%d exhausted=%v", next, exhausted)
	}
	version, _ := recovered.Current()
	last, ok := version.LastSequence()
	if !ok || last != 41 {
		t.Fatalf("last=%d/%v", last, ok)
	}
}

func TestRecoverySequenceMaximumFromEachDurableAuthority(t *testing.T) {
	t.Run("Manifest only", func(t *testing.T) {
		directory := t.TempDir()
		store, err := Create(Options{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		last := uint64(999)
		if installErr := store.Install(VersionEdit{LastSequence: &last}); installErr != nil {
			t.Fatal(installErr)
		}
		if closeErr := store.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		recovered, err := Open(Options{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		defer recovered.Close()
		if next, exhausted := recovered.RecoveredNextSequence(); exhausted || next != 1000 {
			t.Fatalf("next=%d exhausted=%v", next, exhausted)
		}
	})
	t.Run("live table only", func(t *testing.T) {
		directory := t.TempDir()
		store, err := Create(Options{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		file, allocateErr := store.AllocateFileNumber(context.Background())
		if allocateErr != nil {
			t.Fatal(allocateErr)
		}
		metadata := writeTable(t, directory, file, 999, "live-max", "value")
		table, tableErr := NewTableMetadata(0, 1, 999, 999, metadata)
		if tableErr != nil {
			t.Fatal(tableErr)
		}
		if installErr := store.Install(VersionEdit{AddedFiles: []TableMetadata{table}}); installErr != nil {
			t.Fatal(installErr)
		}
		if closeErr := store.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		recovered, err := Open(Options{Directory: directory})
		if err != nil {
			t.Fatal(err)
		}
		defer recovered.Close()
		if next, exhausted := recovered.RecoveredNextSequence(); exhausted || next != 1000 {
			t.Fatalf("next=%d exhausted=%v", next, exhausted)
		}
	})
}

func TestDuplicateEditBehavior(t *testing.T) {
	comparator, next := ComparatorName, uint64(3)
	base, err := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	if err != nil {
		t.Fatal(err)
	}
	table := syntheticTable(t, 1, 1, 0, 0, "a", "a")
	installed, err := base.apply(VersionEdit{AddedFiles: []TableMetadata{table}})
	if err != nil {
		t.Fatal(err)
	}
	if _, duplicateErr := installed.apply(VersionEdit{AddedFiles: []TableMetadata{table}}); !errors.Is(duplicateErr, ErrDuplicateFile) {
		t.Fatalf("duplicate add=%v", duplicateErr)
	}
	deleted, err := installed.apply(VersionEdit{DeletedFiles: []DeletedFile{{Level: 0, FileNumber: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, duplicateErr := deleted.apply(VersionEdit{DeletedFiles: []DeletedFile{{Level: 0, FileNumber: 1}}}); !errors.Is(duplicateErr, ErrUnknownFile) {
		t.Fatalf("duplicate delete=%v", duplicateErr)
	}
	if _, idempotentErr := deleted.apply(VersionEdit{NextFileNumber: &next}); idempotentErr != nil {
		t.Fatalf("idempotent scalar=%v", idempotentErr)
	}
	if _, emptyErr := deleted.apply(VersionEdit{}); !errors.Is(emptyErr, ErrInvalidEdit) {
		t.Fatalf("empty edit=%v", emptyErr)
	}
}

func TestDurableAppendFailurePoisonsAndDoesNotPublish(t *testing.T) {
	comparator := ComparatorName
	version, _ := initialVersion().apply(VersionEdit{Comparator: &comparator})
	want := errors.New("disk failed")
	store := &Store{writer: failingWriter{err: want}, current: version, logger: rlog.Discard()}
	next := uint64(2)
	if err := store.Install(VersionEdit{NextFileNumber: &next}); !errors.Is(err, ErrWriterPoisoned) {
		t.Fatalf("first error=%v", err)
	}
	if store.current.NextFileNumber() != 1 {
		t.Fatal("failed edit published")
	}
	if err := store.Install(VersionEdit{NextFileNumber: &next}); !errors.Is(err, ErrWriterPoisoned) {
		t.Fatalf("second error=%v", err)
	}
}

func TestConcurrentVersionReadersSeeWholeSnapshots(t *testing.T) {
	comparator, next := ComparatorName, uint64(1002)
	version, _ := initialVersion().apply(VersionEdit{Comparator: &comparator, NextFileNumber: &next})
	store := &Store{writer: &memoryManifestWriter{}, current: version, logger: rlog.Discard()}
	var readers sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v, _ := store.Current()
					last, ok := v.LastSequence()
					if ok && last+1 > v.Generation() {
						panic("torn Version")
					}
				}
			}
		}()
	}
	for i := uint64(1); i <= 1000; i++ {
		last := i - 1
		if err := store.Install(VersionEdit{LastSequence: &last}); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	readers.Wait()
}

func TestCurrentStrictParsing(t *testing.T) {
	for _, contents := range []string{"", "MANIFEST-000001", "../MANIFEST-000001\n", "MANIFEST-000000\n", "MANIFEST-000001\nextra", "MANIFEST-1\n"} {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, CurrentFileName), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readCurrent(directory); !errors.Is(err, ErrCurrentCorrupt) {
			t.Fatalf("%q error=%v", contents, err)
		}
	}
}

func TestCurrentPublicationFailureMatrix(t *testing.T) {
	failure := errors.New("injected")
	stages := []struct {
		name      string
		stage     string
		ambiguous bool
	}{{"write", "write", false}, {"temp-sync", "file-sync", false}, {"temp-close", "file-close", false}, {"rename", "rename", false}, {"directory-open", "directory-open", true}, {"directory-sync", "directory-sync", true}, {"directory-close", "directory-close", true}}
	for _, test := range stages {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			ops := failureCurrentOps(test.stage, failure)
			ambiguous, err := publishCurrent(directory, 1, ops)
			if !errors.Is(err, failure) || ambiguous != test.ambiguous {
				t.Fatalf("ambiguous=%v error=%v", ambiguous, err)
			}
		})
	}
	directory := t.TempDir()
	ambiguous, err := publishCurrent(directory, 7, osCurrentOps())
	if err != nil || ambiguous {
		t.Fatalf("successful publish ambiguous=%v err=%v", ambiguous, err)
	}
	number, err := readCurrent(directory)
	if err != nil || number != 7 {
		t.Fatalf("CURRENT=%d err=%v", number, err)
	}
}

func TestManifestRewriteCurrentSwitchMatrixRetainsBothStates(t *testing.T) {
	failure := errors.New("rewrite failure")
	for _, stage := range []string{"write", "file-sync", "file-close", "rename", "directory-open", "directory-sync", "directory-close"} {
		t.Run(stage, func(t *testing.T) {
			directory := t.TempDir()
			store, err := Create(Options{Directory: directory})
			if err != nil {
				t.Fatal(err)
			}
			store.currentOps = failureCurrentOps(stage, failure)
			rewriteErr := store.Rewrite()
			if !errors.Is(rewriteErr, failure) {
				t.Fatalf("Rewrite error=%v", rewriteErr)
			}
			postRename := stage == "directory-open" || stage == "directory-sync" || stage == "directory-close"
			if postRename && !errors.Is(rewriteErr, ErrCurrentAmbiguous) {
				t.Fatalf("post-rename error not ambiguous: %v", rewriteErr)
			}
			number, currentErr := readCurrent(directory)
			if currentErr != nil {
				t.Fatalf("read CURRENT: %v", currentErr)
			}
			want := uint64(1)
			if postRename {
				want = 2
			}
			if number != want {
				t.Fatalf("CURRENT=%d want=%d", number, want)
			}
			for _, manifestNumber := range []uint64{1, 2} {
				if _, statErr := os.Stat(filepath.Join(directory, manifestFileName(manifestNumber))); statErr != nil {
					t.Fatalf("Manifest %d not retained: %v", manifestNumber, statErr)
				}
			}
			if closeErr := store.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		})
	}
}

func TestCrashAfterManifestDurabilityRecoversLiveTable(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	file, err := store.AllocateFileNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	metadata := writeTable(t, directory, file, 0, "after", "durable")
	table, _ := NewTableMetadata(0, 1, 0, 0, metadata)
	last, frontier := uint64(0), uint64(0)
	crash := errors.New("simulated crash")
	store.afterDurable = func() error { return crash }
	if installErr := store.Install(VersionEdit{AddedFiles: []TableMetadata{table}, LastSequence: &last, ReplayFrontier: &frontier}); !errors.Is(installErr, crash) {
		t.Fatalf("install error=%v", installErr)
	}
	volatile, _ := store.Current()
	if volatile.LiveTableCount() != 0 {
		t.Fatal("volatile Version advanced across crash seam")
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	recovered, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer recovered.Close()
	current, _ := recovered.Current()
	if current.LiveTableCount() != 1 {
		t.Fatalf("durable table count=%d", current.LiveTableCount())
	}
}

func TestCompactionCrashAfterManifestDurabilityRecoversAtomicReplacement(t *testing.T) {
	directory := t.TempDir()
	store, err := Create(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	inputNumber, _ := store.AllocateFileNumber(context.Background())
	inputMeta := writeTable(t, directory, inputNumber, 7, "key", "input")
	input, _ := NewTableMetadata(0, 1, 7, 7, inputMeta)
	last := uint64(7)
	if installErr := store.Install(VersionEdit{AddedFiles: []TableMetadata{input}, LastSequence: &last}); installErr != nil {
		t.Fatal(installErr)
	}
	outputNumber, _ := store.AllocateFileNumber(context.Background())
	outputMeta := writeTable(t, directory, outputNumber, 7, "key", "input")
	output, _ := NewTableMetadata(1, 2, 7, 7, outputMeta)
	crash := errors.New("post-Manifest crash")
	store.afterDurable = func() error { return crash }
	if installErr := store.InstallCompaction(context.Background(), []TableMetadata{input}, []TableMetadata{output}); !errors.Is(installErr, crash) {
		t.Fatalf("error=%v", installErr)
	}
	volatile, _ := store.Current()
	if !volatile.Contains(input) || volatile.Contains(output) {
		t.Fatal("volatile replacement published")
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	recovered, err := Open(Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	current, _ := recovered.Current()
	if current.Contains(input) || !current.Contains(output) {
		t.Fatal("durable replacement not recovered atomically")
	}
}

func TestCorruptAndMismatchedLiveTableFailRecovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(string) error
	}{
		{"corrupt", func(path string) error {
			file, err := os.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = file.WriteAt([]byte{0xff}, 0)
			return err
		}},
		{"mismatch", func(path string) error {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			return os.Truncate(path, info.Size()-1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			store, err := Create(Options{Directory: directory})
			if err != nil {
				t.Fatal(err)
			}
			file, _ := store.AllocateFileNumber(context.Background())
			metadata := writeTable(t, directory, file, 0, "live", "value")
			table, _ := NewTableMetadata(0, 1, 0, 0, metadata)
			last, frontier := uint64(0), uint64(0)
			if err := store.Install(VersionEdit{AddedFiles: []TableMetadata{table}, LastSequence: &last, ReplayFrontier: &frontier}); err != nil {
				t.Fatal(err)
			}
			store.Close()
			path := filepath.Join(directory, sstable.FileName(file))
			if err := test.mutate(path); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Options{Directory: directory}); !errors.Is(err, ErrCorruptLiveTable) {
				t.Fatalf("Open error=%v", err)
			}
		})
	}
}

func TestManifestReplayPropertyModel(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, manifestFileName(1))
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	comparator, next := ComparatorName, uint64(10_002)
	initial := VersionEdit{Comparator: &comparator, NextFileNumber: &next}
	encoded, _ := EncodeVersionEdit(initial)
	if _, appendErr := writer.Append(encoded); appendErr != nil {
		t.Fatal(appendErr)
	}
	model, _ := initialVersion().apply(initial)
	random := rand.New(rand.NewPCG(77, 99))
	live := make([]uint64, 0, 256)
	referenceLive := make(map[uint64]struct{})
	nextFile := uint64(1)
	for step := 0; step < 10_000; step++ {
		var edit VersionEdit
		if len(live) != 0 && (len(live) >= 128 || random.Uint64()%3 == 0) {
			index := int(random.Uint64() % uint64(len(live)))
			file := live[index]
			edit.DeletedFiles = []DeletedFile{{Level: 0, FileNumber: file}}
			live = append(live[:index], live[index+1:]...)
			delete(referenceLive, file)
		} else if nextFile <= 10_000 {
			file := nextFile
			nextFile++
			edit.AddedFiles = []TableMetadata{syntheticTable(t, file, uint64(step+1), uint64(step), uint64(step), fmt.Sprintf("k%08d", file), fmt.Sprintf("k%08d", file))}
			live = append(live, file)
			referenceLive[file] = struct{}{}
		} else {
			last := uint64(step)
			edit.LastSequence = &last
		}
		candidate, applyErr := model.apply(edit)
		if applyErr != nil {
			t.Fatalf("step %d: %v", step, applyErr)
		}
		model = candidate
		encoded, _ := EncodeVersionEdit(edit)
		if _, appendErr := writer.Append(encoded); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	if syncErr := writer.Sync(); syncErr != nil {
		t.Fatal(syncErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	recovered, err := recoverManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEquivalent(model, recovered.version); err != nil {
		t.Fatal("replay differs from property model")
	}
	if recovered.version.Generation() != 10_001 || recovered.version.NextFileNumber() != 10_002 || recovered.version.LiveTableCount() != len(referenceLive) {
		t.Fatalf("recovered scalar/model mismatch: generation=%d next=%d live=%d/%d", recovered.version.Generation(), recovered.version.NextFileNumber(), recovered.version.LiveTableCount(), len(referenceLive))
	}
	for _, file := range recovered.version.LiveFileNumbers() {
		if _, exists := referenceLive[file]; !exists {
			t.Fatalf("recovery invented file %d", file)
		}
	}
}

func syntheticTable(t *testing.T, file, generation, first, last uint64, smallest, largest string) TableMetadata {
	t.Helper()
	left, err := storage.NewInternalKey([]byte(smallest), last, storage.KindValue)
	if err != nil {
		t.Fatal(err)
	}
	right, err := storage.NewInternalKey([]byte(largest), first, storage.KindValue)
	if err != nil {
		t.Fatal(err)
	}
	return TableMetadata{FileNumber: file, FlushGeneration: generation, FileSize: sstable.FooterSize, EntryCount: 1, RawKeyValueBytes: 1, DataBlockCount: 1, SmallestSequence: first, LargestSequence: last, SmallestInternal: left, LargestInternal: right, SmallestUser: []byte(smallest), LargestUser: []byte(largest)}
}

func writeTable(t *testing.T, directory string, file, sequence uint64, key, value string) sstable.Metadata {
	t.Helper()
	writer, err := sstable.OpenWriter(directory, file, sstable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	internal, _ := storage.NewInternalKey([]byte(key), sequence, storage.KindValue)
	if addErr := writer.Add(internal, []byte(value)); addErr != nil {
		t.Fatal(addErr)
	}
	metadata, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

type failingWriter struct{ err error }

func (w failingWriter) Append([]byte) (wal.Position, error) { return wal.Position{}, w.err }
func (w failingWriter) Close() error                        { return nil }

type memoryManifestWriter struct {
	mu      sync.Mutex
	records [][]byte
}

type injectedDurableFile struct {
	file      *os.File
	stage     string
	failure   error
	directory bool
}

func (f *injectedDurableFile) Write(value []byte) (int, error) {
	if f.stage == "write" {
		return 0, f.failure
	}
	return f.file.Write(value)
}
func (f *injectedDurableFile) Sync() error {
	if (!f.directory && f.stage == "file-sync") || (f.directory && f.stage == "directory-sync") {
		return f.failure
	}
	return f.file.Sync()
}
func (f *injectedDurableFile) Close() error {
	if (!f.directory && f.stage == "file-close") || (f.directory && f.stage == "directory-close") {
		_ = f.file.Close()
		return f.failure
	}
	return f.file.Close()
}

func failureCurrentOps(stage string, failure error) currentOps {
	return currentOps{
		openTemp: func(path string) (durableFile, error) {
			file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				return nil, err
			}
			return &injectedDurableFile{file: file, stage: stage, failure: failure}, nil
		},
		rename: func(oldPath, newPath string) error {
			if stage == "rename" {
				return failure
			}
			return os.Rename(oldPath, newPath)
		},
		openDirectory: func(path string) (syncCloser, error) {
			if stage == "directory-open" {
				return nil, failure
			}
			file, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			return &injectedDurableFile{file: file, stage: stage, failure: failure, directory: true}, nil
		},
		remove: os.Remove,
	}
}

func (w *memoryManifestWriter) Append(value []byte) (wal.Position, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = append(w.records, bytes.Clone(value))
	return wal.Position{}, nil
}
func (w *memoryManifestWriter) Close() error { return nil }

func FuzzVersionEditDecode(f *testing.F) {
	f.Add([]byte("RVED"))
	comparator := ComparatorName
	encoded, _ := EncodeVersionEdit(VersionEdit{Comparator: &comparator})
	f.Add(encoded)
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := DecodeVersionEdit(data)
		if err == nil {
			encoded, encodeErr := EncodeVersionEdit(decoded)
			if encodeErr != nil {
				t.Fatalf("decoded value cannot encode: %v", encodeErr)
			}
			if len(encoded) > MaxEditSize {
				t.Fatal("bound exceeded")
			}
		}
	})
}

func FuzzCurrentParser(f *testing.F) {
	f.Add([]byte("MANIFEST-000001\n"))
	f.Add([]byte("../MANIFEST-000001\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		directory := t.TempDir()
		if writeErr := os.WriteFile(filepath.Join(directory, CurrentFileName), data, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
		number, err := readCurrent(directory)
		if err == nil {
			want := []byte(manifestFileName(number) + "\n")
			if !bytes.Equal(data, want) {
				t.Fatalf("accepted noncanonical CURRENT %q", data)
			}
		}
	})
}

func BenchmarkVersionEditCodec(b *testing.B) {
	random := rand.New(rand.NewPCG(1, 2))
	tables := make([]TableMetadata, 100)
	for i := range tables {
		key := fmt.Sprintf("%016x", random.Uint64())
		tables[i] = benchmarkTable(uint64(i+1), key)
	}
	next := uint64(101)
	edit := VersionEdit{NextFileNumber: &next, AddedFiles: tables}
	encoded, _ := EncodeVersionEdit(edit)
	b.Run("encode-100", func(b *testing.B) {
		for b.Loop() {
			if _, err := EncodeVersionEdit(edit); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("decode-100", func(b *testing.B) {
		for b.Loop() {
			if _, err := DecodeVersionEdit(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkTable(file uint64, key string) TableMetadata {
	left, _ := storage.NewInternalKey([]byte(key), file, storage.KindValue)
	return TableMetadata{FileNumber: file, FlushGeneration: file, FileSize: sstable.FooterSize, EntryCount: 1, DataBlockCount: 1, SmallestSequence: file, LargestSequence: file, SmallestInternal: left, LargestInternal: left, SmallestUser: []byte(key), LargestUser: []byte(key)}
}
