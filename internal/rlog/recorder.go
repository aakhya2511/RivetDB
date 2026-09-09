package rlog

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"
)

// Record is a captured log record with its attributes flattened into a map.
// Attributes inside slog groups are flattened with dot-separated keys, so a
// group "raft" containing "term" is recorded as "raft.term".
type Record struct {
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   map[string]any
}

// Attr returns the value of a flattened attribute key and whether it was set.
func (r Record) Attr(key string) (any, bool) {
	v, ok := r.Attrs[key]
	return v, ok
}

// Recorder is an slog.Handler that retains records in memory so tests can
// assert on structured logging rather than on formatted output. Asserting on
// attributes keeps tests from breaking when a message string is reworded, and
// lets a test state precisely which event it depends on.
//
// Recorder is safe for concurrent use: RivetDB logs from many goroutines
// (Raft tickers, compaction workers, RPC handlers) and a test harness must be
// able to collect from all of them without a data race.
type Recorder struct {
	mu      sync.Mutex
	records []Record

	// level is the minimum level the recorder captures. It is fixed at
	// construction because a test that changes capture thresholds mid-run
	// produces results that depend on goroutine timing.
	level slog.Level
}

// NewRecorder returns a Recorder capturing records at or above level.
func NewRecorder(level slog.Level) *Recorder {
	return &Recorder{level: level}
}

// Logger returns a logger that writes into the recorder.
func (r *Recorder) Logger() *slog.Logger {
	return slog.New(&recorderHandler{rec: r})
}

// Records returns a copy of the captured records in emission order. The copy
// keeps callers from observing later appends while they iterate.
func (r *Recorder) Records() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

// Find returns the captured records whose message equals msg.
func (r *Recorder) Find(msg string) []Record {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out []Record
	for _, rec := range r.records {
		if rec.Message == msg {
			out = append(out, rec)
		}
	}
	return out
}

// Reset drops all captured records.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
}

func (r *Recorder) append(rec Record) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
}

// recorderHandler is the slog.Handler half of Recorder. It is a separate type
// because slog.Handler is cloned by WithAttrs/WithGroup, while the underlying
// record store must stay shared across all clones.
type recorderHandler struct {
	rec *Recorder
	// attrs are the attributes accumulated by WithAttrs calls, already
	// qualified by any groups that were open when they were added.
	attrs map[string]any
	// groups is the currently open group path, used to qualify keys.
	groups []string
}

func (h *recorderHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.rec.level
}

func (h *recorderHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	for k, v := range h.attrs {
		attrs[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		flatten(attrs, h.groups, a)
		return true
	})

	h.rec.append(Record{
		Time:    r.Time,
		Level:   r.Level,
		Message: r.Message,
		Attrs:   attrs,
	})
	return nil
}

func (h *recorderHandler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	clone := h.clone()
	for _, a := range as {
		flatten(clone.attrs, clone.groups, a)
	}
	return clone
}

func (h *recorderHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := h.clone()
	clone.groups = append(clone.groups, name)
	return clone
}

func (h *recorderHandler) clone() *recorderHandler {
	attrs := make(map[string]any, len(h.attrs))
	for k, v := range h.attrs {
		attrs[k] = v
	}
	// groups is copied rather than appended in place so sibling handlers
	// derived from the same parent cannot overwrite each other's path.
	return &recorderHandler{
		rec:    h.rec,
		attrs:  attrs,
		groups: slices.Clone(h.groups),
	}
}

// flatten writes a, qualified by the open group path, into dst. Group-valued
// attributes recurse; empty attributes are dropped to match the behaviour of
// slog's builtin handlers, which is what Err(nil) relies on.
func flatten(dst map[string]any, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}

	if a.Value.Kind() == slog.KindGroup {
		members := a.Value.Group()
		if len(members) == 0 {
			return
		}
		inner := groups
		if a.Key != "" {
			inner = append(slices.Clone(groups), a.Key)
		}
		for _, m := range members {
			flatten(dst, inner, m)
		}
		return
	}

	dst[qualify(groups, a.Key)] = a.Value.Any()
}

func qualify(groups []string, key string) string {
	if len(groups) == 0 {
		return key
	}
	var b strings.Builder
	for _, g := range groups {
		b.WriteString(g)
		b.WriteByte('.')
	}
	b.WriteString(key)
	return b.String()
}
