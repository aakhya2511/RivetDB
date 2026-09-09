package rlog_test

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/rlog"
)

func TestRecorderCapturesLevelsAndMessages(t *testing.T) {
	t.Parallel()

	rec := rlog.NewRecorder(slog.LevelInfo)
	logger := rec.Logger()

	logger.Debug("dropped")
	logger.Info("kept")
	logger.Error("also kept")

	records := rec.Records()
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2: %+v", len(records), records)
	}
	if records[0].Message != "kept" || records[0].Level != slog.LevelInfo {
		t.Errorf("record 0 = %+v", records[0])
	}
	if records[1].Message != "also kept" || records[1].Level != slog.LevelError {
		t.Errorf("record 1 = %+v", records[1])
	}
}

func TestRecorderFlattensGroupsAndInheritedAttrs(t *testing.T) {
	t.Parallel()

	rec := rlog.NewRecorder(slog.LevelDebug)
	logger := rec.Logger().
		With(rlog.NodeID(2)).
		WithGroup("raft").
		With(rlog.Term(5))

	logger.Info("state change", slog.String("role", "leader"))

	records := rec.Find("state change")
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	want := map[string]any{
		"node":      uint64(2),
		"raft.term": uint64(5),
		"raft.role": "leader",
	}
	for k, v := range want {
		got, ok := records[0].Attrs[k]
		if !ok {
			t.Errorf("attribute %q missing; have %v", k, records[0].Attrs)
			continue
		}
		if got != v {
			t.Errorf("attribute %q = %v, want %v", k, got, v)
		}
	}
}

// TestRecorderSiblingHandlersAreIndependent guards the clone-on-derive
// behaviour: two loggers derived from the same parent must not observe each
// other's attributes. Sharing the backing slice or map would leak attributes
// between unrelated subsystems.
func TestRecorderSiblingHandlersAreIndependent(t *testing.T) {
	t.Parallel()

	rec := rlog.NewRecorder(slog.LevelDebug)
	parent := rec.Logger().With(slog.String("shared", "yes"))

	parent.With(slog.String("only", "a")).Info("a")
	parent.WithGroup("g").With(slog.String("only", "b")).Info("b")

	a := rec.Find("a")[0]
	b := rec.Find("b")[0]

	if _, ok := a.Attrs["g.only"]; ok {
		t.Errorf("record a picked up sibling group attribute: %v", a.Attrs)
	}
	if got := a.Attrs["only"]; got != "a" {
		t.Errorf("record a attribute only = %v, want a", got)
	}
	if got := b.Attrs["g.only"]; got != "b" {
		t.Errorf("record b attribute g.only = %v, want b (have %v)", got, b.Attrs)
	}
	if _, ok := b.Attrs["only"]; ok {
		t.Errorf("record b has ungrouped attribute: %v", b.Attrs)
	}
	for _, r := range []rlog.Record{a, b} {
		if got := r.Attrs["shared"]; got != "yes" {
			t.Errorf("inherited attribute lost: %v", r.Attrs)
		}
	}
}

func TestRecorderDropsEmptyAttrs(t *testing.T) {
	t.Parallel()

	rec := rlog.NewRecorder(slog.LevelDebug)
	rec.Logger().Info("msg", rlog.Err(nil), slog.Group("empty"))

	records := rec.Records()
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if len(records[0].Attrs) != 0 {
		t.Errorf("expected no attributes, got %v", records[0].Attrs)
	}
}

// TestRecorderConcurrentWriters is the reason Recorder is mutex-guarded: the
// subsystems it will observe (Raft tickers, compaction workers, RPC handlers)
// all log from separate goroutines. Run under -race this fails if the
// recorder's state is not properly guarded.
func TestRecorderConcurrentWriters(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		records = 64
	)

	rec := rlog.NewRecorder(slog.LevelDebug)

	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger := rec.Logger().With(rlog.NodeID(uint64(w)))
			for i := range records {
				logger.Info("write", rlog.Index(uint64(i)))
			}
		}()
	}
	wg.Wait()

	if got := len(rec.Records()); got != writers*records {
		t.Errorf("captured %d records, want %d", got, writers*records)
	}

	rec.Reset()
	if got := len(rec.Records()); got != 0 {
		t.Errorf("Reset left %d records", got)
	}
}
