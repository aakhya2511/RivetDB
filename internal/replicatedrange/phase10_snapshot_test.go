package replicatedrange

import (
	"context"
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/txn"
)

func TestOlderRaftSnapshotRequiresExactSubsetOfAheadDurableLSM(t *testing.T) {
	local, err := engine.Open(engine.Options{Directory: t.TempDir(), Mode: engine.ModeReplicatedMVCC, MemTableBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := local.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	apply := func(index, timestamp uint64, key, value string) {
		t.Helper()
		if applyErr := local.ApplyCommittedMVCC(context.Background(), index, 1, timestamp, []byte{byte(index)},
			storage.Mutation{Kind: storage.KindValue, Key: []byte(key), Value: []byte(value)}); applyErr != nil {
			t.Fatal(applyErr)
		}
	}
	apply(1, 10, "a", "one")
	apply(2, 20, "b", "two")
	if flushErr := local.Flush(context.Background()); flushErr != nil {
		t.Fatal(flushErr)
	}
	mock := clock.NewMock()
	hlc, err := mvcc.NewClock(mock, 20)
	if err != nil {
		t.Fatal(err)
	}
	machine := &stateMachine{engine: local, mvcc: true, rangeID: 10, generation: 2, maxApplied: 20,
		clock: hlc, records: make(map[txn.ID]txn.Record), participants: make(map[txn.ID]txn.ParticipantRecord),
		lifecycle: LifecycleLearner, provenance: make(map[uint64][32]byte), bootstrapSeen: make(map[[32]byte]struct{})}
	valid := logicalStateImage{rangeID: 10, generation: 1, applied: 1, maxApplied: 10, safeRead: 10, hlc: 10,
		lifecycle: LifecycleActive, versions: []engine.MVCCVersion{{Key: []byte("a"), Value: []byte("one"), Timestamp: 10, Kind: storage.KindValue}}}
	encoded, err := encodeLogicalState(valid)
	if err != nil {
		t.Fatal(err)
	}
	if restoreErr := machine.restoreSnapshot(encoded); restoreErr != nil {
		t.Fatalf("exact subset rejected: %v", restoreErr)
	}
	if machine.maxApplied != 20 || machine.lifecycle != LifecycleLearner {
		t.Fatalf("floor/lifecycle regressed: max=%d lifecycle=%d", machine.maxApplied, machine.lifecycle)
	}
	invalid := valid
	invalid.versions = []engine.MVCCVersion{{Key: []byte("a"), Value: []byte("wrong"), Timestamp: 10, Kind: storage.KindValue}}
	encoded, err = encodeLogicalState(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.restoreSnapshot(encoded); !errors.Is(err, ErrSnapshotBehind) {
		t.Fatalf("mismatched subset error=%v", err)
	}
}
