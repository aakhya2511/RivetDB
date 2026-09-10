package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/storage/wal"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

var errInjected = errors.New("injected pipeline failure")

type fakeWAL struct {
	mu      sync.Mutex
	records [][]byte
	err     error
	closed  bool
	hook    func()
}

func (w *fakeWAL) Append(record []byte) (wal.Position, error) {
	if w.hook != nil {
		w.hook()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return wal.Position{}, w.err
	}
	w.records = append(w.records, bytes.Clone(record))
	return wal.Position{}, nil
}

func (w *fakeWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *fakeWAL) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records)
}

func pipelineOptions(directory string) Options {
	return Options{Directory: directory, MemTableBytes: math.MaxUint64, MaxImmutables: 4, FirstGeneration: 10, FirstFileNumber: 20}
}

func successfulFlush(_ context.Context, item *generation) (flushResult, error) {
	return flushResult{metadata: sstable.Metadata{FileNumber: item.fileNumber, EntryCount: uint64(item.table.Len())}}, nil
}

func newTestPipeline(t *testing.T, options Options, logWriter *fakeWAL, executor flushExecutor) *Pipeline {
	t.Helper()
	if options.Directory == "" {
		options.Directory = t.TempDir()
	}
	p, err := newPipeline(options, logWriter, executor)
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(context.Background()); err != nil && !errors.Is(err, ErrFlushFailed) {
			t.Errorf("Close: %v", err)
		}
	})
	return p
}

func put(key string) storage.Mutation {
	return storage.Mutation{Key: []byte(key), Value: []byte("value-" + key), Kind: storage.KindValue}
}

func TestWriteDurableBeforeAtomicApply(t *testing.T) {
	w := &fakeWAL{}
	options := pipelineOptions(t.TempDir())
	p := newTestPipeline(t, options, w, successfulFlush)
	w.hook = func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.active.table.Len() != 0 {
			t.Error("MemTable changed before WAL append completed")
		}
	}
	result, err := p.Write(context.Background(), []storage.Mutation{put("a"), put("b")})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if result.FirstSequence != 0 || result.LastSequence != 1 || result.Generation != 10 {
		t.Fatalf("Write result = %+v", result)
	}
	p.mu.Lock()
	gotLen := p.active.table.Len()
	p.mu.Unlock()
	if gotLen != 2 || w.count() != 1 {
		t.Fatalf("entries/WAL records = %d/%d, want 2/1", gotLen, w.count())
	}
}

func TestWALFailureChangesNoMemTableOrSequence(t *testing.T) {
	w := &fakeWAL{err: errInjected}
	p := newTestPipeline(t, pipelineOptions(t.TempDir()), w, successfulFlush)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("a")}); !errors.Is(err, errInjected) {
		t.Fatalf("Write error = %v, want injected", err)
	}
	stats := p.Stats()
	if p.active.table.Len() != 0 || stats.NextSequence != 0 || stats.Rotations != 0 {
		t.Fatalf("failure changed state: %+v len=%d", stats, p.active.table.Len())
	}
}

func TestInvalidBatchRejectedBeforeWAL(t *testing.T) {
	t.Parallel()
	w := &fakeWAL{}
	p := newTestPipeline(t, pipelineOptions(t.TempDir()), w, successfulFlush)
	tests := [][]storage.Mutation{
		nil,
		{{Key: []byte("delete"), Value: []byte("invalid"), Kind: storage.KindDelete}},
		{{Key: make([]byte, sstable.MaxUserKeySize+1), Kind: storage.KindValue}},
		{{Key: []byte("kind"), Kind: storage.ValueKind(99)}},
	}
	for index, mutations := range tests {
		if _, err := p.Write(context.Background(), mutations); err == nil {
			t.Fatalf("invalid batch %d succeeded", index)
		}
	}
	if w.count() != 0 || p.active.table.Len() != 0 {
		t.Fatalf("invalid batches changed WAL/MemTable: %d/%d", w.count(), p.active.table.Len())
	}
}

func TestBatchStaysInOneGenerationAndCrossingBatchRotatesAfterApply(t *testing.T) {
	batch := storage.WriteBatch{Mutations: []storage.Mutation{put("a"), put("b")}}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	w := &fakeWAL{}
	p := newTestPipeline(t, options, w, successfulFlush)
	result, err := p.Write(context.Background(), batch.Mutations)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if result.Generation != 10 {
		t.Fatalf("generation = %d, want 10", result.Generation)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	outputs := p.Outputs()
	if len(outputs) != 1 || outputs[0].Generation != 10 || outputs[0].SmallestSeq != 0 || outputs[0].LargestSeq != 1 {
		t.Fatalf("outputs = %+v", outputs)
	}
	if p.Stats().ActiveGeneration != 11 || p.active.table.Len() != 0 {
		t.Fatalf("active generation/len = %d/%d", p.Stats().ActiveGeneration, p.active.table.Len())
	}
}

func TestEmptyRotateAndThresholdBoundary(t *testing.T) {
	options := pipelineOptions(t.TempDir())
	w := &fakeWAL{}
	p := newTestPipeline(t, options, w, successfulFlush)
	rotated, err := p.Rotate(context.Background())
	if err != nil || rotated {
		t.Fatalf("empty Rotate = %v, %v", rotated, err)
	}
	p.threshold = math.MaxUint64
	if _, err := p.Write(context.Background(), []storage.Mutation{put("x")}); err != nil {
		t.Fatalf("below-threshold Write: %v", err)
	}
	if p.Stats().Rotations != 0 {
		t.Fatal("rotated below threshold")
	}
	p.threshold = p.active.table.SizeBytes()
	if _, err := p.Write(context.Background(), []storage.Mutation{put("y")}); err != nil {
		t.Fatalf("at/above-threshold Write: %v", err)
	}
	if p.Stats().Rotations != 1 {
		t.Fatal("did not rotate at threshold")
	}
}

func TestBackpressureBeforeWALAppendAndResumes(t *testing.T) {
	started := make(chan uint64, 2)
	release := make(chan struct{}, 2)
	executor := func(ctx context.Context, item *generation) (flushResult, error) {
		started <- item.id
		select {
		case <-ctx.Done():
			return flushResult{}, ctx.Err()
		case <-release:
			return successfulFlush(ctx, item)
		}
	}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	options.MaxImmutables = 2
	w := &fakeWAL{}
	p := newTestPipeline(t, options, w, executor)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("one")}); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if id := <-started; id != 10 {
		t.Fatalf("first flush generation = %d", id)
	}
	if _, err := p.Write(context.Background(), []storage.Mutation{put("two")}); err != nil {
		t.Fatalf("second Write during flush: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Write(canceled, []storage.Mutation{put("three")}); !errors.Is(err, context.Canceled) {
		t.Fatalf("backpressured Write error = %v", err)
	}
	if w.count() != 2 {
		t.Fatalf("WAL records under backpressure = %d, want 2", w.count())
	}
	release <- struct{}{}
	if id := <-started; id != 11 {
		t.Fatalf("second flush generation = %d", id)
	}
	release <- struct{}{}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	p.threshold = math.MaxUint64
	if _, err := p.Write(context.Background(), []storage.Mutation{put("three")}); err != nil {
		t.Fatalf("Write after capacity returned: %v", err)
	}
}

func TestInsertRacingExplicitRotationPreservesWrite(t *testing.T) {
	defer testutil.NoLeaks(t)()
	p, err := newPipeline(pipelineOptions(t.TempDir()), &fakeWAL{}, successfulFlush)
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	var group sync.WaitGroup
	group.Add(2)
	var writeErr, rotateErr error
	go func() {
		defer group.Done()
		_, writeErr = p.Write(context.Background(), []storage.Mutation{put("raced")})
	}()
	go func() {
		defer group.Done()
		_, rotateErr = p.Rotate(context.Background())
	}()
	group.Wait()
	if writeErr != nil || rotateErr != nil {
		t.Fatalf("write/rotate errors = %v/%v", writeErr, rotateErr)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	p.mu.Lock()
	entries := p.active.table.Len()
	for _, output := range p.outputs {
		entries += int(output.Metadata.EntryCount)
	}
	p.mu.Unlock()
	if entries != 1 {
		t.Fatalf("represented entries = %d, want 1", entries)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFlushFailureRetainedAndExplicitRetryStableIdentity(t *testing.T) {
	var mu sync.Mutex
	var attempts []struct{ generation, file uint64 }
	executor := func(ctx context.Context, item *generation) (flushResult, error) {
		mu.Lock()
		attempts = append(attempts, struct{ generation, file uint64 }{item.id, item.fileNumber})
		attempt := len(attempts)
		mu.Unlock()
		if attempt == 1 {
			return flushResult{}, errInjected
		}
		return successfulFlush(ctx, item)
	}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	p := newTestPipeline(t, options, &fakeWAL{}, executor)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("a")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.Drain(context.Background()); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("Drain error = %v, want ErrFlushFailed", err)
	}
	if p.Stats().ImmutableCount != 1 || p.immutables[0].state != StateFailed {
		t.Fatalf("failed immutable was not retained")
	}
	if _, err := p.Write(context.Background(), []storage.Mutation{put("blocked")}); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("Write with failed head = %v", err)
	}
	if err := p.RetryFailed(); err != nil {
		t.Fatalf("RetryFailed: %v", err)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain retry: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || attempts[0] != attempts[1] {
		t.Fatalf("attempt identities = %+v", attempts)
	}
}

func TestStatsAndStructuredLifecycleLogs(t *testing.T) {
	t.Parallel()
	recorder := rlog.NewRecorder(slog.LevelInfo)
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	options.Logger = recorder.Logger()
	p := newTestPipeline(t, options, &fakeWAL{}, successfulFlush)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("logged")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	stats := p.Stats()
	if stats.Rotations != 1 || stats.Flushes != 1 || stats.ImmutableCount != 0 || stats.ActiveBytes == 0 {
		t.Fatalf("Stats = %+v", stats)
	}
	for _, event := range []string{"memtable rotation started", "memtable frozen", "immutable queued", "flush started", "flush completed"} {
		records := recorder.Find(event)
		if len(records) != 1 {
			t.Fatalf("event %q count = %d", event, len(records))
		}
		if _, ok := records[0].Attr("generation"); !ok {
			t.Fatalf("event %q lacks generation attribute", event)
		}
	}
	completed := recorder.Find("flush completed")[0]
	for _, attribute := range []string{"file", "duration_ns", "bytes", "entries"} {
		if _, ok := completed.Attr(attribute); !ok {
			t.Fatalf("flush completed lacks %q", attribute)
		}
	}
}

func TestAmbiguousPublicationCannotRetryOrDelete(t *testing.T) {
	executor := func(context.Context, *generation) (flushResult, error) {
		return flushResult{ambiguous: true}, errInjected
	}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	p := newTestPipeline(t, options, &fakeWAL{}, executor)
	if _, err := p.Write(context.Background(), []storage.Mutation{put("a")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.Drain(context.Background()); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("Drain = %v", err)
	}
	if err := p.RetryFailed(); !errors.Is(err, ErrAmbiguousPublication) {
		t.Fatalf("RetryFailed = %v, want ambiguity", err)
	}
	if p.Stats().ImmutableCount != 1 {
		t.Fatal("ambiguous immutable discarded")
	}
}

func TestFlushFailureMatrixRetainsImmutable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		ambiguous bool
	}{
		{name: "writer"},
		{name: "file_sync"},
		{name: "close"},
		{name: "rename"},
		{name: "directory_sync", ambiguous: true},
		{name: "reader_validation", ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := func(context.Context, *generation) (flushResult, error) {
				return flushResult{ambiguous: tc.ambiguous}, fmt.Errorf("%s: %w", tc.name, errInjected)
			}
			options := pipelineOptions(t.TempDir())
			options.MemTableBytes = 1
			p := newTestPipeline(t, options, &fakeWAL{}, executor)
			if _, err := p.Write(context.Background(), []storage.Mutation{put(tc.name)}); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := p.Drain(context.Background()); !errors.Is(err, ErrFlushFailed) {
				t.Fatalf("Drain = %v", err)
			}
			if len(p.immutables) != 1 || p.immutables[0].state != StateFailed || p.immutables[0].ambiguous != tc.ambiguous {
				t.Fatalf("retained failure state = %+v", p.immutables)
			}
		})
	}
}

func TestFlushStateTransitionTableAndRandomModel(t *testing.T) {
	t.Parallel()
	valid := map[[2]FlushState]bool{
		{StateActive, StateQueued}: true, {StateQueued, StateFlushing}: true,
		{StateFlushing, StateFailed}: true, {StateFlushing, StateDurable}: true,
		{StateFailed, StateQueued}: true,
	}
	states := []FlushState{StateActive, StateQueued, StateFlushing, StateFailed, StateDurable}
	for _, from := range states {
		for _, to := range states {
			g := &generation{id: 7, state: from}
			err := g.transition(to)
			if valid[[2]FlushState{from, to}] != (err == nil) {
				t.Fatalf("transition %s -> %s error = %v", from, to, err)
			}
		}
	}
	fresh := testutil.Seed(t)
	for _, initialSeed := range []uint64{0, 1, math.MaxUint64, uint64(fresh)} {
		seed := initialSeed
		for step := 0; step < 10_000; step++ {
			from := states[int(seed%uint64(len(states)))]
			seed = seed*6364136223846793005 + 1
			to := states[int(seed%uint64(len(states)))]
			seed = seed*6364136223846793005 + 1
			g := &generation{id: uint64(step + 1), state: from}
			err := g.transition(to)
			if (err == nil) != valid[[2]FlushState{from, to}] {
				t.Fatalf("seed %d step %d transition %s -> %s error = %v", initialSeed, step, from, to, err)
			}
		}
	}
}

func TestSequenceOverflowAndAllocatorExhaustionBeforeWAL(t *testing.T) {
	tests := []struct {
		name    string
		options Options
		want    error
	}{
		{name: "sequence", options: Options{NextSequence: math.MaxUint64}},
		{name: "generation", options: Options{FirstGeneration: math.MaxUint64 - 1}, want: ErrGenerationExhausted},
		{name: "file", options: Options{FirstFileNumber: sstable.MaxFileNumber}, want: ErrFileNumberExhausted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			options := pipelineOptions(t.TempDir())
			if tc.options.NextSequence != 0 {
				options.NextSequence = tc.options.NextSequence
			}
			if tc.options.FirstGeneration != 0 {
				options.FirstGeneration = tc.options.FirstGeneration
			}
			if tc.options.FirstFileNumber != 0 {
				options.FirstFileNumber = tc.options.FirstFileNumber
			}
			w := &fakeWAL{}
			p := newTestPipeline(t, options, w, successfulFlush)
			if tc.name == "generation" {
				p.nextGeneration = 0
			}
			if tc.name == "file" {
				p.nextFile = sstable.MaxFileNumber + 1
			}
			mutations := []storage.Mutation{put("a"), put("b")}
			want := tc.want
			if tc.name == "sequence" {
				want = storage.ErrSequenceOverflow
			}
			if _, err := p.Write(context.Background(), mutations); !errors.Is(err, want) {
				t.Fatalf("Write error = %v, want %v", err, want)
			}
			if w.count() != 0 {
				t.Fatal("exhausted write reached WAL")
			}
		})
	}
}

func TestConcurrentAcceptedWriteAccountingAndFIFO(t *testing.T) {
	var flushedMu sync.Mutex
	flushed := make(map[uint64]string)
	var order []uint64
	executor := func(ctx context.Context, item *generation) (flushResult, error) {
		flushedMu.Lock()
		order = append(order, item.id)
		for iterator := item.table.Iterator(); iterator.Next(); {
			entry, _ := iterator.Entry()
			flushed[entry.Key.Sequence()] = string(entry.Key.UserKey())
		}
		flushedMu.Unlock()
		return successfulFlush(ctx, item)
	}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	options.MaxImmutables = 3
	w := &fakeWAL{}
	p := newTestPipeline(t, options, w, executor)
	const writers = 64
	type accepted struct {
		sequence uint64
		key      string
	}
	results := make(chan accepted, writers)
	var group sync.WaitGroup
	for index := 0; index < writers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			key := fmt.Sprintf("key-%03d", index)
			result, err := p.Write(context.Background(), []storage.Mutation{put(key)})
			if err != nil {
				t.Errorf("Write %s: %v", key, err)
				return
			}
			results <- accepted{sequence: result.FirstSequence, key: key}
		}(index)
	}
	group.Wait()
	close(results)
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	want := make(map[uint64]string)
	for result := range results {
		want[result.sequence] = result.key
	}
	flushedMu.Lock()
	defer flushedMu.Unlock()
	if len(want) != writers || len(flushed) != writers {
		t.Fatalf("accepted/flushed = %d/%d, want %d", len(want), len(flushed), writers)
	}
	for sequence, key := range want {
		if flushed[sequence] != key {
			t.Fatalf("sequence %d = %q, want %q", sequence, flushed[sequence], key)
		}
	}
	if !slices.IsSorted(order) || len(order) != writers {
		t.Fatalf("flush order/count = %v/%d", order, len(order))
	}
	if w.count() != writers {
		t.Fatalf("WAL records = %d, want %d", w.count(), writers)
	}
}

func TestRealFilesystemFlushExactContentsAndWALRetention(t *testing.T) {
	directory := t.TempDir()
	options := pipelineOptions(directory)
	options.MemTableBytes = 1
	p, err := Open(options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := map[uint64]string{}
	for index, mutation := range []storage.Mutation{
		{Key: []byte("same"), Value: []byte("new"), Kind: storage.KindValue},
		{Key: []byte("same"), Kind: storage.KindDelete},
		{Key: []byte{0, 255}, Value: []byte("binary"), Kind: storage.KindValue},
	} {
		result, writeErr := p.Write(context.Background(), []storage.Mutation{mutation})
		if writeErr != nil {
			t.Fatalf("Write %d: %v", index, writeErr)
		}
		want[result.FirstSequence] = string(mutation.Key)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	outputs := p.Outputs()
	if len(outputs) != len(want) {
		t.Fatalf("outputs = %d, want %d", len(outputs), len(want))
	}
	got := map[uint64]string{}
	for _, output := range outputs {
		reader, openErr := sstable.Open(output.Path, sstable.ReaderOptions{})
		if openErr != nil {
			t.Fatalf("Open SSTable: %v", openErr)
		}
		iterator, iteratorErr := reader.NewIterator()
		if iteratorErr != nil {
			t.Fatalf("NewIterator: %v", iteratorErr)
		}
		for iterator.Next() {
			entry, _ := iterator.Entry()
			got[entry.Key.Sequence()] = string(entry.Key.UserKey())
		}
		if iterator.Error() != nil {
			t.Fatalf("iterator: %v", iterator.Error())
		}
		_ = iterator.Close()
		_ = reader.Close()
	}
	if !mapsEqual(got, want) {
		t.Fatalf("SSTable contents = %v, want %v", got, want)
	}
	walPath := filepath.Join(directory, WALFileName)
	recovered := memtable.New()
	replay, replayErr := ReplayWAL(walPath, recovered)
	if replayErr != nil || replay.Entries != uint64(len(want)) || recovered.Len() != len(want) {
		t.Fatalf("ReplayWAL = %+v, %v, entries=%d", replay, replayErr, recovered.Len())
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := ReplayWAL(walPath, memtable.New()); err != nil {
		t.Fatalf("retained WAL after Close: %v", err)
	}
}

func TestCloseLeavesNonemptyActiveRecoverable(t *testing.T) {
	directory := t.TempDir()
	options := pipelineOptions(directory)
	p, err := Open(options)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, writeErr := p.Write(context.Background(), []storage.Mutation{put("active")}); writeErr != nil {
		t.Fatalf("Write: %v", writeErr)
	}
	if closeErr := p.Close(context.Background()); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	table := memtable.New()
	result, err := ReplayWAL(filepath.Join(directory, WALFileName), table)
	if err != nil || result.Entries != 1 || table.Len() != 1 {
		t.Fatalf("Replay active = %+v, %v, len=%d", result, err, table.Len())
	}
}

func TestWALDurableBeforeMemTableApplyCrashModel(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "durable-before-apply.wal")
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	batch := storage.WriteBatch{FirstSequence: 91, Mutations: []storage.Mutation{put("a"), put("b")}}
	encoded, err := storage.EncodeWriteBatch(batch)
	if err != nil {
		t.Fatalf("EncodeWriteBatch: %v", err)
	}
	if _, appendErr := writer.Append(encoded); appendErr != nil {
		t.Fatalf("durable Append: %v", appendErr)
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	table := memtable.New()
	result, err := ReplayWAL(path, table)
	if err != nil || result.Entries != 2 || table.Len() != 2 {
		t.Fatalf("ReplayWAL = %+v, %v, len=%d", result, err, table.Len())
	}
}

func TestCloseDrainsQueuedAndIsIdempotent(t *testing.T) {
	defer testutil.NoLeaks(t)()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	executor := func(ctx context.Context, item *generation) (flushResult, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
			return flushResult{}, fmt.Errorf("flush canceled: %w", ctx.Err())
		case <-release:
			return successfulFlush(ctx, item)
		}
	}
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	w := &fakeWAL{}
	p, err := newPipeline(options, w, executor)
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	if _, err := p.Write(context.Background(), []storage.Mutation{put("queued")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	<-started
	closed := make(chan error, 1)
	go func() { closed <- p.Close(context.Background()) }()
	release <- struct{}{}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(p.Outputs()) != 1 || !w.closed {
		t.Fatalf("Close output/WAL = %d/%v", len(p.Outputs()), w.closed)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestCloseSurfacesFlushFailure(t *testing.T) {
	t.Parallel()
	options := pipelineOptions(t.TempDir())
	options.MemTableBytes = 1
	p, err := newPipeline(options, &fakeWAL{}, func(context.Context, *generation) (flushResult, error) {
		return flushResult{}, errInjected
	})
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	if _, err := p.Write(context.Background(), []storage.Mutation{put("failed")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := p.Close(context.Background()); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("Close = %v, want ErrFlushFailed", err)
	}
}

func TestReplayRejectsSequenceDiscontinuityWithoutApplyingLaterBatch(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	path := filepath.Join(directory, "gap.wal")
	writer, err := wal.OpenWriter(path, wal.WriterOptions{Durability: wal.SyncBatch})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for _, batch := range []storage.WriteBatch{
		{FirstSequence: 1, Mutations: []storage.Mutation{put("one")}},
		{FirstSequence: 3, Mutations: []storage.Mutation{put("three")}},
	} {
		encoded, encodeErr := storage.EncodeWriteBatch(batch)
		if encodeErr != nil {
			t.Fatalf("EncodeWriteBatch: %v", encodeErr)
		}
		if _, appendErr := writer.Append(encoded); appendErr != nil {
			t.Fatalf("Append: %v", appendErr)
		}
	}
	if closeErr := writer.Close(); closeErr != nil {
		t.Fatalf("Close writer: %v", closeErr)
	}
	table := memtable.New()
	result, err := ReplayWAL(path, table)
	if !errors.Is(err, ErrSequenceDiscontinuity) || result.Entries != 1 || table.Len() != 1 {
		t.Fatalf("Replay gap = %+v, %v, len=%d", result, err, table.Len())
	}
}

func TestInvalidOptionsAndClosedOperations(t *testing.T) {
	t.Parallel()
	if _, err := Open(Options{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Open invalid = %v", err)
	}
	p, err := newPipeline(pipelineOptions(t.TempDir()), &fakeWAL{}, successfulFlush)
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := p.Write(context.Background(), []storage.Mutation{put("late")}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write closed = %v", err)
	}
	if _, err := p.Rotate(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Rotate closed = %v", err)
	}
}

func TestCloseJoinsWorkerAndClosesWAL(t *testing.T) {
	defer testutil.NoLeaks(t)()
	w := &fakeWAL{}
	p, err := newPipeline(pipelineOptions(t.TempDir()), w, successfulFlush)
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w.mu.Lock()
	closed := w.closed
	w.mu.Unlock()
	if !closed {
		t.Fatal("WAL was not closed")
	}
}

func mapsEqual(left, right map[uint64]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}
