package advisor

import (
	"context"
	"testing"

	"github.com/rivetdb/rivetdb/internal/clock"
)

func BenchmarkSnapshotSerialization(b *testing.B) {
	input, _, _ := testInput()
	cfg := DefaultConfig(clock.NewMock())
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := BuildSnapshot(input, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAdviceParse(b *testing.B) {
	input, _, _ := testInput()
	cfg := DefaultConfig(clock.NewMock())
	snapshot, _, digest, err := BuildSnapshot(input, cfg)
	if err != nil {
		b.Fatal(err)
	}
	raw := validAdvice(input, ActionTransferLeader, 20)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := parseAdvice(raw, snapshot, digest, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDisabledAdvisor(b *testing.B) {
	cfg := DefaultConfig(clock.NewMock())
	service, err := New(cfg, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = service.Analyze(context.Background(), SnapshotInput{})
	}
}
