package raft

import (
	"fmt"
	"testing"
)

func BenchmarkThreeNodeProposeCommitApply(b *testing.B) {
	for b.Loop() {
		simulator := newTestSimulator(b, 3, 101)
		electNode(b, simulator, 1)
		if _, err := simulator.ProposeAndApply(1, []byte("command"), 1_000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFiveNodeProposeCommitApply(b *testing.B) {
	for b.Loop() {
		simulator := newTestSimulator(b, 5, 101)
		electNode(b, simulator, 1)
		if _, err := simulator.ProposeAndApply(1, []byte("command"), 1_000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStateEncodeDecode100k(b *testing.B) {
	state := PersistentState{HardState: HardState{Term: 100}}
	for index := uint64(1); index <= 100_000; index++ {
		state.Entries = append(state.Entries, Entry{Index: index, Term: 100, Type: EntryCommand, Command: []byte(fmt.Sprintf("command-%012d", index))})
	}
	encoded, err := encodeFileState(state)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportMetric(float64(len(encoded)), "store-bytes")
	b.ResetTimer()
	for b.Loop() {
		decoded, err := decodeFileState(encoded)
		if err != nil || len(decoded.Entries) != len(state.Entries) {
			b.Fatalf("decoded=%d err=%v", len(decoded.Entries), err)
		}
	}
}
