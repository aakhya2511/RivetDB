package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type testEntry struct {
	user     string
	sequence uint64
	kind     storage.ValueKind
	value    string
}

type testingTB interface {
	Helper()
	Fatal(args ...any)
}

func TestPickerTransitiveUserRangeClosureDeterministic(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	installTable(t, store, directory, 0, 1, []testEntry{{"a", 1, storage.KindValue, "a"}, {"f", 1, storage.KindValue, "f"}})
	installTable(t, store, directory, 0, 2, []testEntry{{"e", 2, storage.KindValue, "e"}, {"k", 2, storage.KindValue, "k"}})
	installTable(t, store, directory, 0, 3, []testEntry{{"j", 3, storage.KindValue, "j"}, {"m", 3, storage.KindValue, "m"}})
	installTable(t, store, directory, 0, 4, []testEntry{{"x", 4, storage.KindValue, "x"}, {"z", 4, storage.KindValue, "z"}})
	x := installTable(t, store, directory, 1, 5, []testEntry{{"b", 5, storage.KindValue, "b"}, {"d", 5, storage.KindValue, "d"}})
	y := installTable(t, store, directory, 1, 6, []testEntry{{"h", 6, storage.KindValue, "h"}, {"q", 6, storage.KindValue, "q"}})
	installTable(t, store, directory, 1, 7, []testEntry{{"r", 7, storage.KindValue, "r"}, {"w", 7, storage.KindValue, "w"}})
	version, _ := store.Current()
	picker := Picker{L0Trigger: 4}
	first, err := picker.Pick(version)
	if err != nil {
		t.Fatal(err)
	}
	second, err := picker.Pick(version)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.SourceInputs) != 3 || len(first.TargetInputs) != 2 {
		t.Fatalf("source=%d target=%d range=%q-%q", len(first.SourceInputs), len(first.TargetInputs), first.SmallestUser, first.LargestUser)
	}
	if first.TargetInputs[0].FileNumber != x.FileNumber || first.TargetInputs[1].FileNumber != y.FileNumber {
		t.Fatalf("target closure=%v", first.TargetInputs)
	}
	if fmt.Sprint(fileNumbers(first.Inputs())) != fmt.Sprint(fileNumbers(second.Inputs())) {
		t.Fatal("picker is nondeterministic")
	}
}

func TestCompactionPreservesContentAndSplitsOnlyBetweenUserKeys(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	var inputs []manifest.TableMetadata
	inputs = append(inputs, installTable(t, store, directory, 0, 1, []testEntry{{"a", 10, storage.KindValue, "a10"}, {"foo", 100, storage.KindValue, "v100"}, {"foo", 80, storage.KindValue, "v80"}}))
	inputs = append(inputs, installTable(t, store, directory, 0, 2, []testEntry{{"b", 11, storage.KindValue, "b11"}, {"foo", 90, storage.KindValue, "v90"}, {"foo", 70, storage.KindDelete, ""}}))
	inputs = append(inputs, installTable(t, store, directory, 0, 3, []testEntry{{"c", 12, storage.KindValue, "c12"}, {"g", 12, storage.KindValue, "g12"}}))
	inputs = append(inputs, installTable(t, store, directory, 0, 4, []testEntry{{"d", 13, storage.KindValue, "d13"}, {"h", 13, storage.KindValue, "h13"}}))
	want := collectTables(t, directory, inputs)
	executor, err := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 20}, store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.CompactOnce(context.Background())
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if len(result.Outputs) < 3 {
		t.Fatalf("outputs=%d want multiple", len(result.Outputs))
	}
	got := collectTables(t, directory, result.Outputs)
	if !slices.Equal(want, got) {
		t.Fatalf("content mismatch\nwant=%q\ngot =%q", want, got)
	}
	for index := 1; index < len(result.Outputs); index++ {
		if bytes.Compare(result.Outputs[index-1].LargestUser, result.Outputs[index].SmallestUser) >= 0 {
			t.Fatal("output ranges overlap")
		}
	}
	fooFiles := 0
	tombstones := 0
	for _, output := range result.Outputs {
		if bytes.Compare(output.SmallestUser, []byte("foo")) <= 0 && bytes.Compare(output.LargestUser, []byte("foo")) >= 0 {
			fooFiles++
		}
		tombstones += int(output.DeletionCount)
	}
	if fooFiles != 1 || tombstones != 1 {
		t.Fatalf("foo files=%d tombstones=%d", fooFiles, tombstones)
	}
	version, _ := store.Current()
	if len(version.Files(0)) != 0 || len(version.Files(1)) != len(result.Outputs) {
		t.Fatalf("levels L0=%d L1=%d", len(version.Files(0)), len(version.Files(1)))
	}
	if len(store.ObsoleteFiles()) != 4 {
		t.Fatalf("obsolete=%v", store.ObsoleteFiles())
	}
	old := fileNumbers(inputs)
	for _, number := range old {
		if _, err := os.Stat(filepath.Join(directory, sstable.FileName(number))); err != nil {
			t.Fatalf("obsolete input %d was deleted: %v", number, err)
		}
	}
}

func TestOversizedSingleUserGroupStaysInOneOutput(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	for file := range 4 {
		entries := make([]testEntry, 25)
		for index := range entries {
			entries[index] = testEntry{"huge", uint64(100 - file*25 - index), storage.KindValue, fmt.Sprintf("value-%040d", index)}
		}
		installTable(t, store, directory, 0, uint64(file+1), entries)
	}
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 32}, store)
	result, err := executor.CompactOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Outputs) != 1 || result.Outputs[0].EntryCount != 100 {
		t.Fatalf("outputs=%d entries=%d", len(result.Outputs), result.Outputs[0].EntryCount)
	}
}

func TestCorruptInputAndPartialOutputFailureKeepInputsLive(t *testing.T) {
	t.Run("corrupt input", func(t *testing.T) {
		directory := t.TempDir()
		store := createStore(t, directory)
		defer store.Close()
		var tables []manifest.TableMetadata
		for index := range 4 {
			tables = append(tables, installTable(t, store, directory, 0, uint64(index+1), []testEntry{{"key", uint64(index), storage.KindValue, "v"}}))
		}
		path := filepath.Join(directory, sstable.FileName(tables[0].FileNumber))
		file, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
			t.Fatal(err)
		}
		file.Close()
		executor, _ := New(Options{Directory: directory, L0Trigger: 4}, store)
		if _, err := executor.CompactOnce(context.Background()); !errors.Is(err, ErrInputCorrupt) {
			t.Fatalf("error=%v", err)
		}
		version, _ := store.Current()
		if len(version.Files(0)) != 4 {
			t.Fatal("corruption changed Version")
		}
	})
	t.Run("second output fails", func(t *testing.T) {
		directory := t.TempDir()
		store := createStore(t, directory)
		defer store.Close()
		for index := range 4 {
			installTable(t, store, directory, 0, uint64(index+1), []testEntry{{fmt.Sprintf("k%d", index), uint64(index), storage.KindValue, "value"}})
		}
		executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 1}, store)
		realFactory := executor.openOutput
		calls := 0
		injected := errors.New("second output")
		executor.openOutput = func(directory string, number uint64, options sstable.Options) (outputWriter, error) {
			calls++
			if calls == 2 {
				return &failedOutputWriter{finishErr: injected}, nil
			}
			return realFactory(directory, number, options)
		}
		plan, _ := executor.Pick()
		plan.SourceInputs = append([]manifest.TableMetadata(nil), func() []manifest.TableMetadata { v, _ := store.Current(); return v.Files(0) }()...)
		plan.SmallestUser = []byte("k0")
		plan.LargestUser = []byte("k3")
		result, err := executor.Run(context.Background(), plan)
		if !errors.Is(err, injected) || len(result.Outputs) != 1 {
			t.Fatalf("outputs=%d err=%v", len(result.Outputs), err)
		}
		version, _ := store.Current()
		if len(version.Files(0)) != 4 || len(version.Files(1)) != 0 {
			t.Fatal("partial output installed")
		}
	})
}

func TestStalePlanAfterDurableOutputsCannotInstall(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	for index := range 4 {
		installTable(t, store, directory, 0, uint64(index+1), []testEntry{{fmt.Sprintf("k%d", index), uint64(index), storage.KindValue, "v"}})
	}
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 1}, store)
	plan, _ := executor.Pick()
	removed := plan.SourceInputs[0]
	executor.beforeInstall = func() error {
		return store.Install(manifest.VersionEdit{DeletedFiles: []manifest.DeletedFile{{Level: removed.Level, FileNumber: removed.FileNumber}}})
	}
	result, err := executor.Run(context.Background(), plan)
	if !errors.Is(err, ErrStalePlan) || result.State != StateStale {
		t.Fatalf("state=%s err=%v", result.State, err)
	}
	version, _ := store.Current()
	for _, output := range result.Outputs {
		if version.Contains(output) {
			t.Fatal("stale output installed")
		}
	}
	if len(result.Outputs) == 0 {
		t.Fatal("stale seam occurred before durable output")
	}
}

func TestExactDuplicateFailsWithoutLogicalReplacement(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	entry := []testEntry{{"same", 7, storage.KindValue, "v"}}
	for index := range 4 {
		installTable(t, store, directory, 0, uint64(index+1), entry)
	}
	executor, _ := New(Options{Directory: directory, L0Trigger: 4}, store)
	result, err := executor.CompactOnce(context.Background())
	if !errors.Is(err, ErrDuplicateInternalKey) {
		t.Fatalf("error=%v", err)
	}
	version, _ := store.Current()
	if len(version.Files(0)) != 4 || len(result.Outputs) != 0 {
		t.Fatalf("logical replacement occurred")
	}
}

func TestOutputAndManifestFailuresLeaveInputsAuthoritative(t *testing.T) {
	for _, stage := range []string{"write", "file-sync", "rename", "directory-sync", "Manifest-append"} {
		t.Run(stage, func(t *testing.T) {
			directory := t.TempDir()
			store := createStore(t, directory)
			defer store.Close()
			for index := range 4 {
				installTable(t, store, directory, 0, uint64(index+1), []testEntry{{"key", uint64(index), storage.KindValue, "v"}})
			}
			executor, _ := New(Options{Directory: directory, L0Trigger: 4}, store)
			injected := errors.New(stage)
			if stage == "write" {
				executor.openOutput = func(string, uint64, sstable.Options) (outputWriter, error) {
					return &failedOutputWriter{addErr: injected}, nil
				}
			} else if stage != "Manifest-append" {
				executor.openOutput = func(string, uint64, sstable.Options) (outputWriter, error) {
					return &failedOutputWriter{finishErr: injected}, nil
				}
			} else {
				executor.beforeInstall = func() error { return injected }
			}
			result, err := executor.CompactOnce(context.Background())
			if !errors.Is(err, injected) {
				t.Fatalf("error=%v", err)
			}
			version, _ := store.Current()
			if len(version.Files(0)) != 4 || len(version.Files(1)) != 0 {
				t.Fatalf("stage %s changed authority", stage)
			}
			for _, output := range result.Outputs {
				if version.Contains(output) {
					t.Fatal("partial output installed")
				}
			}
		})
	}
}

func TestPreManifestOutputsRecoverAsOrphansAndInputsStayLive(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	for index := range 4 {
		installTable(t, store, directory, 0, uint64(index+1), []testEntry{{fmt.Sprintf("k%d", index), uint64(index), storage.KindValue, "v"}})
	}
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 1}, store)
	crash := errors.New("before Manifest")
	executor.beforeInstall = func() error { return crash }
	result, err := executor.CompactOnce(context.Background())
	if !errors.Is(err, crash) || len(result.Outputs) == 0 {
		t.Fatalf("outputs=%d err=%v", len(result.Outputs), err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := manifest.Open(manifest.Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	version, _ := reopened.Current()
	if len(version.Files(0)) != 4 || len(version.Files(1)) != 0 {
		t.Fatalf("L0=%d L1=%d", len(version.Files(0)), len(version.Files(1)))
	}
	report := reopened.Discovery()
	if len(report.Orphans) != len(result.Outputs) {
		t.Fatalf("orphans=%v outputs=%d", report.Orphans, len(result.Outputs))
	}
}

func TestRestartAfterInstalledCompactionRecoversOutputs(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	var inputs []manifest.TableMetadata
	for index := range 4 {
		inputs = append(inputs, installTable(t, store, directory, 0, uint64(index+1), []testEntry{{"key", uint64(index), storage.KindValue, fmt.Sprintf("v%d", index)}}))
	}
	want := collectTables(t, directory, inputs)
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 1}, store)
	result, err := executor.CompactOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := store.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened, err := manifest.Open(manifest.Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	version, _ := reopened.Current()
	if len(version.Files(0)) != 0 || len(version.Files(1)) != len(result.Outputs) {
		t.Fatalf("L0=%d L1=%d", len(version.Files(0)), len(version.Files(1)))
	}
	if got := collectTables(t, directory, version.Files(1)); !slices.Equal(want, got) {
		t.Fatal("restart content mismatch")
	}
}

func TestCompactionPreservesReplayFrontier(t *testing.T) {
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	for index := range 4 {
		installTable(t, store, directory, 0, uint64(index+1), []testEntry{{"key", uint64(index), storage.KindValue, fmt.Sprintf("v%d", index)}})
	}
	frontier := uint64(3)
	if err := store.Install(manifest.VersionEdit{ReplayFrontier: &frontier}); err != nil {
		t.Fatal(err)
	}
	executor, _ := New(Options{Directory: directory, L0Trigger: 4}, store)
	if _, err := executor.CompactOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	version, _ := store.Current()
	after, ok := version.ReplayFrontier()
	if !ok || after != frontier {
		t.Fatalf("frontier=%d/%v", after, ok)
	}
}

func TestStateTransitionMatrix(t *testing.T) {
	states := []State{StatePlanned, StateRunning, StateOutputDurable, StateInstalling, StateInstalled, StateFailed, StateStale}
	for _, from := range states {
		for _, to := range states {
			want := from == StatePlanned && to == StateRunning || from == StateRunning && (to == StateOutputDurable || to == StateFailed) || from == StateOutputDurable && (to == StateInstalling || to == StateStale || to == StateFailed) || from == StateInstalling && (to == StateInstalled || to == StateFailed || to == StateStale)
			if validTransition(from, to) != want {
				t.Fatalf("%s -> %s", from, to)
			}
		}
	}
}

func TestHundredTableStress(t *testing.T) {
	if os.Getenv("RIVETDB_STRESS") == "" {
		t.Skip("set RIVETDB_STRESS=1 for the 100-table compaction campaign")
	}
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	random := testutil.RandFromSeed(101)
	var inputs []manifest.TableMetadata
	for index := range 100 {
		key := fmt.Sprintf("%c%02d", byte('a'+random.Uint64()%4), index%10)
		kind := storage.KindValue
		if index%9 == 0 {
			kind = storage.KindDelete
		}
		inputs = append(inputs, installTable(t, store, directory, 0, uint64(index+1), []testEntry{{key, uint64(1000 - index), kind, func() string {
			if kind == storage.KindDelete {
				return ""
			}
			return fmt.Sprintf("v%d", index)
		}()}}))
	}
	want := collectTables(t, directory, inputs)
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 256}, store)
	compactions := 0
	for {
		_, err := executor.CompactOnce(context.Background())
		if errors.Is(err, ErrNoCompaction) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		compactions++
	}
	version, _ := store.Current()
	live := append(version.Files(0), version.Files(1)...)
	got := collectTables(t, directory, live)
	if !slices.Equal(want, got) {
		t.Fatal("100-table multiset mismatch")
	}
	if compactions == 0 || len(version.Files(0)) >= 4 {
		t.Fatalf("compactions=%d L0=%d", compactions, len(version.Files(0)))
	}
}

func TestFreshSeedCompactionModel(t *testing.T) {
	random := testutil.Rand(t)
	directory := t.TempDir()
	store := createStore(t, directory)
	defer store.Close()
	var inputs []manifest.TableMetadata
	for index := range 12 {
		key := []byte{byte(random.Uint64()), byte(index % 3)}
		kind := storage.KindValue
		if index%5 == 0 {
			kind = storage.KindDelete
		}
		value := "v"
		if kind == storage.KindDelete {
			value = ""
		}
		inputs = append(inputs, installTable(t, store, directory, 0, uint64(index+1), []testEntry{{string(key), uint64(100 - index), kind, value}}))
	}
	want := collectTables(t, directory, inputs)
	executor, _ := New(Options{Directory: directory, L0Trigger: 4, TargetFileSize: 64}, store)
	for {
		_, err := executor.CompactOnce(context.Background())
		if errors.Is(err, ErrNoCompaction) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	version, _ := store.Current()
	live := append(version.Files(0), version.Files(1)...)
	if got := collectTables(t, directory, live); !slices.Equal(want, got) {
		t.Fatal("fresh-seed model mismatch")
	}
}

func createStore(t testingTB, directory string) *manifest.Store {
	t.Helper()
	store, err := manifest.Create(manifest.Options{Directory: directory})
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func installTable(t testingTB, store *manifest.Store, directory string, level uint32, generation uint64, entries []testEntry) manifest.TableMetadata {
	t.Helper()
	slices.SortFunc(entries, func(a, b testEntry) int {
		ka, _ := storage.NewInternalKey([]byte(a.user), a.sequence, a.kind)
		kb, _ := storage.NewInternalKey([]byte(b.user), b.sequence, b.kind)
		return storage.CompareInternal(ka, kb)
	})
	number, err := store.AllocateFileNumber(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writer, err := sstable.OpenWriter(directory, number, sstable.Options{})
	if err != nil {
		t.Fatal(err)
	}
	smallest, largest := ^uint64(0), uint64(0)
	for _, entry := range entries {
		key, _ := storage.NewInternalKey([]byte(entry.user), entry.sequence, entry.kind)
		if addErr := writer.Add(key, []byte(entry.value)); addErr != nil {
			t.Fatal(addErr)
		}
		smallest = min(smallest, entry.sequence)
		largest = max(largest, entry.sequence)
	}
	metadata, err := writer.Finish()
	if err != nil {
		t.Fatal(err)
	}
	table, err := manifest.NewTableMetadata(level, generation, smallest, largest, metadata)
	if err != nil {
		t.Fatal(err)
	}
	last := largest
	if current, _ := store.Current(); current != nil {
		if value, ok := current.LastSequence(); ok && value > last {
			last = value
		}
	}
	if err := store.Install(manifest.VersionEdit{AddedFiles: []manifest.TableMetadata{table}, LastSequence: &last}); err != nil {
		t.Fatal(err)
	}
	return table
}
func collectTables(t testingTB, directory string, tables []manifest.TableMetadata) []string {
	t.Helper()
	var result []string
	for _, table := range tables {
		reader, err := sstable.Open(filepath.Join(directory, sstable.FileName(table.FileNumber)), sstable.ReaderOptions{})
		if err != nil {
			t.Fatal(err)
		}
		iterator, err := reader.NewIterator()
		if err != nil {
			t.Fatal(err)
		}
		for iterator.Next() {
			entry, _ := iterator.Entry()
			result = append(result, fmt.Sprintf("%x/%020d/%d/%x", entry.Key.UserKey(), entry.Key.Sequence(), entry.Key.Kind(), entry.Value))
		}
		if err := iterator.Error(); err != nil {
			t.Fatal(err)
		}
		iterator.Close()
		reader.Close()
	}
	slices.Sort(result)
	return result
}
func fileNumbers(tables []manifest.TableMetadata) []uint64 {
	result := make([]uint64, len(tables))
	for i, table := range tables {
		result[i] = table.FileNumber
	}
	return result
}

type failedOutputWriter struct{ addErr, finishErr error }

func (w *failedOutputWriter) Add(storage.InternalKey, []byte) error { return w.addErr }
func (w *failedOutputWriter) Finish() (sstable.Metadata, error) {
	return sstable.Metadata{}, w.finishErr
}
func (w *failedOutputWriter) Abort() error { return nil }
