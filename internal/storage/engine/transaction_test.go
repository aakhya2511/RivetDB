package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func TestIntentBatchResolutionFlushCompactionAndRestart(t *testing.T) {
	directory := t.TempDir()
	valueIntent, encodeErr := txn.EncodeIntent(txn.Intent{ID: transactionStorageID(1), ReadTime: 50, CommitTime: 100, Epoch: 1,
		Home: txn.Participant{RangeID: 1, Generation: 1}, Value: []byte("new-a")})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	deleteIntent, encodeErr := txn.EncodeIntent(txn.Intent{ID: transactionStorageID(1), ReadTime: 50, CommitTime: 100, Epoch: 1,
		Home: txn.Participant{RangeID: 1, Generation: 1}, Delete: true})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	e, openErr := Open(Options{Directory: directory, Mode: ModeReplicatedMVCC, MemTableBytes: 1 << 20, L0Trigger: 2})
	if openErr != nil {
		t.Fatal(openErr)
	}
	if err := e.ApplyCommittedMVCCBatch(context.Background(), 1, 1, 100, []byte("prepare"), []storage.Mutation{
		{Key: []byte("a"), Value: valueIntent, Kind: storage.KindIntent},
		{Key: []byte("b"), Value: deleteIntent, Kind: storage.KindIntent},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.GetAt(context.Background(), []byte("a"), 100); !errors.Is(err, ErrUnresolvedIntent) {
		t.Fatalf("ordinary intent read=%v", err)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	e, openErr = Open(Options{Directory: directory, Mode: ModeReplicatedMVCC, MemTableBytes: 1 << 20, L0Trigger: 2})
	if openErr != nil {
		t.Fatal(openErr)
	}
	entry, entryErr := e.GetMVCCAt(context.Background(), []byte("a"), 100)
	if entryErr != nil || entry.Kind != storage.KindIntent {
		t.Fatalf("recovered intent=%+v err=%v", entry, entryErr)
	}
	if err := e.ResolveCommittedMVCCBatch(context.Background(), 2, 1, 100, []byte("commit"), []storage.Mutation{
		{Key: []byte("a"), Value: []byte("new-a"), Kind: storage.KindValue},
		{Key: []byte("b"), Kind: storage.KindDelete},
	}); err != nil {
		t.Fatal(err)
	}
	if value, err := e.GetAt(context.Background(), []byte("a"), 100); err != nil || string(value) != "new-a" {
		t.Fatalf("committed a=%q %v", value, err)
	}
	if _, err := e.GetAt(context.Background(), []byte("b"), 100); !errors.Is(err, ErrNotFound) {
		t.Fatalf("committed delete=%v", err)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	abortIntent, encodeErr := txn.EncodeIntent(txn.Intent{ID: transactionStorageID(2), ReadTime: 100, CommitTime: 200, Epoch: 1,
		Home: txn.Participant{RangeID: 1, Generation: 1}, Value: []byte("aborted")})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if err := e.ApplyCommittedMVCCBatch(context.Background(), 3, 1, 200, []byte("prepare-abort"), []storage.Mutation{{Key: []byte("a"), Value: abortIntent, Kind: storage.KindIntent}}); err != nil {
		t.Fatal(err)
	}
	if err := e.ResolveCommittedMVCCBatch(context.Background(), 4, 1, 200, []byte("abort"), []storage.Mutation{{Key: []byte("a"), Kind: storage.KindTxnAbort}}); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for {
		result, compactErr := e.Compact(context.Background())
		if errors.Is(compactErr, compaction.ErrNoCompaction) {
			break
		}
		if compactErr != nil {
			t.Fatal(compactErr)
		}
		if result.JobID == 0 {
			break
		}
	}
	if err := e.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	e, openErr = Open(Options{Directory: directory, Mode: ModeReplicatedMVCC})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer e.Close(context.Background())
	if value, err := e.GetAt(context.Background(), []byte("a"), 200); err != nil || string(value) != "new-a" {
		t.Fatalf("abort after compaction=%q %v", value, err)
	}
	if _, err := e.GetAt(context.Background(), []byte("a"), 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("historical precommit=%v", err)
	}
}

func transactionStorageID(last byte) txn.ID {
	var id txn.ID
	id[len(id)-1] = last
	return id
}
