package multiraft

import (
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
)

func testEnvelope(rangeID RangeID, from, to raft.NodeID) Envelope {
	return Envelope{RangeID: rangeID, MessageRangeID: rangeID, Generation: 1,
		DescriptorFingerprint: [32]byte{1},
		FromReplica:           ReplicaID(from), ToReplica: ReplicaID(to),
		Message: raft.Message{Type: raft.AppendEntries, From: from, To: to, Term: 1}}
}

func TestSharedTransportBoundsCopiesAndRangeFairness(t *testing.T) {
	transport, err := NewTransport(3)
	if err != nil {
		t.Fatal(err)
	}
	first := testEnvelope(10, 1, 2)
	first.Message.Entries = []raft.Entry{{Index: 1, Term: 1, Type: raft.EntryCommand, Command: []byte("owned")}}
	if err := transport.Send(first); err != nil {
		t.Fatal(err)
	}
	first.Message.Entries[0].Command[0] = 'X'
	if err := transport.Send(testEnvelope(10, 1, 3)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(testEnvelope(11, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(testEnvelope(12, 3, 1)); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("capacity=%v", err)
	}
	taken, ok := transport.Take()
	if !ok || string(taken.Message.Entries[0].Command) != "owned" {
		t.Fatalf("copy=%q ok=%v", taken.Message.Entries[0].Command, ok)
	}
	next, _ := transport.Take()
	if next.RangeID != 11 {
		t.Fatalf("hot range starved alternate: %d", next.RangeID)
	}
}

func TestSharedTransportRangeAndNodePartitions(t *testing.T) {
	transport, _ := NewTransport(10)
	transport.SetRangeLink(10, 1, 2, true)
	if err := transport.Send(testEnvelope(10, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send(testEnvelope(11, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if transport.Pending() != 1 {
		t.Fatalf("range partition pending=%d", transport.Pending())
	}
	transport.SetNodeLink(1, 2, true)
	if err := transport.Send(testEnvelope(12, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if transport.Pending() != 1 {
		t.Fatalf("node partition pending=%d", transport.Pending())
	}
	transport.Heal()
	if err := transport.Send(testEnvelope(10, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if transport.Pending() != 2 {
		t.Fatalf("healed pending=%d", transport.Pending())
	}
}

func TestEnvelopeRejectsMismatchedEmbeddedGroup(t *testing.T) {
	transport, _ := NewTransport(10)
	envelope := testEnvelope(10, 1, 2)
	envelope.MessageRangeID = 11
	if err := transport.Send(envelope); !errors.Is(err, ErrWrongRangeMessage) {
		t.Fatalf("mismatch=%v", err)
	}
}
