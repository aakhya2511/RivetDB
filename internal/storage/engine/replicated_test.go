package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/manifest"
	"github.com/rivetdb/rivetdb/internal/storage/pipeline"
)

func TestReplicatedModeUsesRaftIndexWithoutDataWALAndPersistsFrontier(t *testing.T) {
	directory := t.TempDir()
	e, openErr := Open(Options{Directory: directory, Mode: ModeReplicated, MemTableBytes: 1 << 20})
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := e.Put(context.Background(), []byte("forbidden"), []byte("x")); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("replicated Put error=%v", err)
	}
	if err := e.Delete(context.Background(), []byte("forbidden")); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("replicated Delete error=%v", err)
	}
	if err := e.WriteBatch(context.Background(), []storage.Mutation{{Kind: storage.KindValue, Key: []byte("forbidden")}}); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("replicated WriteBatch error=%v", err)
	}
	command := []byte("command-at-index-2")
	if err := e.ApplyCommitted(context.Background(), 2, 1, command, storage.Mutation{Kind: storage.KindValue, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := e.ApplyCommitted(context.Background(), 2, 1, command, storage.Mutation{Kind: storage.KindValue, Key: []byte("k"), Value: []byte("v")}); !errors.Is(err, ErrAlreadyApplied) {
		t.Fatalf("volatile duplicate error=%v", err)
	}
	if err := e.ApplyCommitted(context.Background(), 2, 2, []byte("conflict"), storage.Mutation{Kind: storage.KindValue, Key: []byte("k"), Value: []byte("other")}); !errors.Is(err, ErrConflictingApply) {
		t.Fatalf("conflicting duplicate error=%v", err)
	}
	if err := e.AdvanceApplied(3); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, pipeline.WALFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replicated apply created data WAL: %v", err)
	}
	if got := e.Stats().Pipeline; !got.ReplicatedMode || got.NextSequence != 0 || got.VisibleSequence != 3 {
		t.Fatalf("pipeline stats=%+v", got)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if frontier, err := e.DurableAppliedRaftIndex(); err != nil || frontier != 3 {
		t.Fatalf("frontier=%d err=%v", frontier, err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Directory: directory, Mode: ModeReplicated})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if value, err := reopened.Get(context.Background(), []byte("k")); err != nil || string(value) != "v" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	if err := reopened.ApplyCommitted(context.Background(), 2, 1, command, storage.Mutation{Kind: storage.KindValue, Key: []byte("k"), Value: []byte("v")}); !errors.Is(err, ErrAlreadyApplied) {
		t.Fatalf("durable duplicate error=%v", err)
	}
	if _, err := Open(Options{Directory: directory, Mode: ModeStandalone}); !errors.Is(err, manifest.ErrModeMismatch) {
		t.Fatalf("wrong-mode open error=%v", err)
	}
}

func TestReplicatedCompactionCannotAdvanceAppliedFrontier(t *testing.T) {
	e, err := Open(Options{Directory: t.TempDir(), Mode: ModeReplicated, MemTableBytes: 1 << 20, L0Trigger: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close(context.Background())
	for index := uint64(2); index <= 5; index++ {
		command := []byte{byte(index)}
		mutation := storage.Mutation{Kind: storage.KindValue, Key: []byte("key"), Value: command}
		if applyErr := e.ApplyCommitted(context.Background(), index, 1, command, mutation); applyErr != nil {
			t.Fatal(applyErr)
		}
		if flushErr := e.Flush(context.Background()); flushErr != nil {
			t.Fatal(flushErr)
		}
	}
	before, err := e.DurableAppliedRaftIndex()
	if err != nil {
		t.Fatal(err)
	}
	if _, compactErr := e.Compact(context.Background()); compactErr != nil {
		t.Fatal(compactErr)
	}
	after, err := e.DurableAppliedRaftIndex()
	if err != nil {
		t.Fatal(err)
	}
	if before != after || after != 5 {
		t.Fatalf("frontier changed %d -> %d", before, after)
	}
	value, err := e.Get(context.Background(), []byte("key"))
	if err != nil || !bytes.Equal(value, []byte{5}) {
		t.Fatalf("value=%v err=%v", value, err)
	}
}

func TestReplicatedUnflushedStateRequiresRaftReplay(t *testing.T) {
	directory := t.TempDir()
	e, openErr := Open(Options{Directory: directory, Mode: ModeReplicated})
	if openErr != nil {
		t.Fatal(openErr)
	}
	command := []byte("put")
	mutation := storage.Mutation{Kind: storage.KindValue, Key: []byte("lost-memtable"), Value: []byte("restored")}
	if err := e.ApplyCommitted(context.Background(), 2, 1, command, mutation); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(Options{Directory: directory, Mode: ModeReplicated})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(context.Background())
	if _, err := reopened.Get(context.Background(), mutation.Key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("volatile value survived without authority: %v", err)
	}
	if err := reopened.ApplyCommitted(context.Background(), 2, 1, command, mutation); err != nil {
		t.Fatal(err)
	}
	if value, err := reopened.Get(context.Background(), mutation.Key); err != nil || string(value) != "restored" {
		t.Fatalf("replayed value=%q err=%v", value, err)
	}
}

func TestReplicatedMutationIsInvisibleBeforePublication(t *testing.T) {
	var e *Engine
	checked := false
	opened, err := Open(Options{
		Directory: t.TempDir(), Mode: ModeReplicated,
		ReplicatedHook: func(stage ReplicatedStage, _ uint64) {
			if stage != ReplicatedStageMemTableApplied {
				return
			}
			checked = true
			if _, getErr := e.Get(context.Background(), []byte("hidden")); !errors.Is(getErr, ErrNotFound) {
				t.Fatalf("mutation visible before publication: %v", getErr)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	e = opened
	defer e.Close(context.Background())
	mutation := storage.Mutation{Kind: storage.KindValue, Key: []byte("hidden"), Value: []byte("published")}
	if err := e.ApplyCommitted(context.Background(), 1, 1, []byte("identity"), mutation); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("MemTable-applied hook not observed")
	}
	if value, getErr := e.Get(context.Background(), mutation.Key); getErr != nil || string(value) != "published" {
		t.Fatalf("published value=%q err=%v", value, getErr)
	}
}
