package rlog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/rivetdb/rivetdb/internal/rlog"
)

func TestParseLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{in: "debug", want: slog.LevelDebug},
		{in: "INFO", want: slog.LevelInfo},
		{in: "  Warn ", want: slog.LevelWarn},
		{in: "warning", want: slog.LevelWarn},
		{in: "error", want: slog.LevelError},
		{in: "", wantErr: true},
		{in: "trace", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := rlog.ParseLevel(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseLevel(%q) = %v, want error", tc.in, got)
				}
				// Callers validating configuration must be able to tell an
				// unknown setting from an I/O failure.
				if !errors.Is(err, rlog.ErrUnknownLevel) {
					t.Errorf("ParseLevel(%q) error = %v, want it to wrap ErrUnknownLevel", tc.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLevel(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseLevel(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in      string
		want    rlog.Format
		wantErr bool
	}{
		{in: "text", want: rlog.FormatText},
		{in: "JSON", want: rlog.FormatJSON},
		{in: "logfmt", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()

			got, err := rlog.ParseFormat(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseFormat(%q) = %v, want error", tc.in, got)
				}
				if !errors.Is(err, rlog.ErrUnknownFormat) {
					t.Errorf("ParseFormat(%q) error = %v, want it to wrap ErrUnknownFormat", tc.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseFormat(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("ParseFormat(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestJSONOutputCarriesCanonicalKeys pins the wire form of a log line. Log
// queries and dashboards are written against these key names, so a change here
// is a breaking change and should require editing this test.
func TestJSONOutputCarriesCanonicalKeys(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _, err := rlog.New(rlog.Config{Format: rlog.FormatJSON, Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	rlog.Component(logger, "raft").Info("election won", rlog.NodeID(3), rlog.RangeID(7), rlog.Term(12))

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal %q: %v", buf.String(), err)
	}

	want := map[string]any{
		"msg":       "election won",
		"level":     "INFO",
		"component": "raft",
		"node":      float64(3),
		"range":     float64(7),
		"term":      float64(12),
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("field %q = %v, want %v (line: %s)", k, got[k], v, buf.String())
		}
	}
}

func TestLevelVarFiltersAndIsLive(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, level, err := rlog.New(rlog.Config{Level: slog.LevelInfo, Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	logger.Debug("suppressed")
	if buf.Len() != 0 {
		t.Fatalf("debug record emitted at info level: %q", buf.String())
	}

	// The admin API will raise verbosity on a running node through this
	// handle, including for loggers derived before the change.
	derived := rlog.Component(logger, "storage")
	level.Set(slog.LevelDebug)
	derived.Debug("now visible")

	if !strings.Contains(buf.String(), "now visible") {
		t.Fatalf("record missing after level change: %q", buf.String())
	}
}

func TestErrAttrOmitsNilError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, _, err := rlog.New(rlog.Config{Format: rlog.FormatJSON, Output: &buf})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	logger.Info("ok", rlog.Err(nil))
	if strings.Contains(buf.String(), rlog.KeyError) {
		t.Errorf("nil error produced an %q field: %s", rlog.KeyError, buf.String())
	}

	buf.Reset()
	logger.Error("failed", rlog.Err(errors.New("disk full")))
	if !strings.Contains(buf.String(), `"err":"disk full"`) {
		t.Errorf("error field missing: %s", buf.String())
	}
}

func TestFromContextDiscardsWhenUnset(t *testing.T) {
	t.Parallel()

	// Library code handed a bare context must not write anywhere a test
	// cannot see or silence, so the fallback discards rather than using the
	// slog default logger.
	logger := rlog.FromContext(context.Background())
	if logger == nil {
		t.Fatal("FromContext returned nil")
	}
	if logger.Enabled(context.Background(), slog.LevelError) {
		t.Error("fallback logger is enabled; expected a discarding logger")
	}
}

func TestContextAttributesAccumulate(t *testing.T) {
	t.Parallel()

	rec := rlog.NewRecorder(slog.LevelDebug)
	ctx := rlog.NewContext(context.Background(), rec.Logger())
	ctx = rlog.With(ctx, rlog.RangeID(4))
	ctx = rlog.With(ctx, rlog.Term(9))

	rlog.FromContext(ctx).Info("append entries", rlog.Index(101))

	records := rec.Find("append entries")
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}

	for key, want := range map[string]any{"range": uint64(4), "term": uint64(9), "index": uint64(101)} {
		got, ok := records[0].Attr(key)
		if !ok {
			t.Errorf("attribute %q missing; have %v", key, records[0].Attrs)
			continue
		}
		if got != want {
			t.Errorf("attribute %q = %v, want %v", key, got, want)
		}
	}
}

func TestWithNilLoggerLeavesContextUsable(t *testing.T) {
	t.Parallel()

	ctx := rlog.NewContext(context.Background(), nil)
	if got := rlog.FromContext(ctx); got == nil {
		t.Fatal("FromContext returned nil after NewContext with a nil logger")
	}
}

func TestFromEnvRejectsMalformedValues(t *testing.T) {
	// Not parallel: mutates process environment.
	t.Setenv(rlog.EnvLevel, "verbose")
	_, _, err := rlog.FromEnv(&bytes.Buffer{})
	if err == nil {
		t.Fatal("FromEnv accepted an invalid level; a typo in a deployment manifest must fail at startup")
	}
	if !errors.Is(err, rlog.ErrUnknownLevel) {
		t.Errorf("error = %v, want it to wrap ErrUnknownLevel", err)
	}

	t.Setenv(rlog.EnvLevel, "debug")
	t.Setenv(rlog.EnvFormat, "yaml")
	_, _, err = rlog.FromEnv(&bytes.Buffer{})
	if err == nil {
		t.Fatal("FromEnv accepted an invalid format")
	}
	if !errors.Is(err, rlog.ErrUnknownFormat) {
		t.Errorf("error = %v, want it to wrap ErrUnknownFormat", err)
	}
}

func TestFromEnvAppliesConfiguration(t *testing.T) {
	t.Setenv(rlog.EnvLevel, "debug")
	t.Setenv(rlog.EnvFormat, "json")

	var buf bytes.Buffer
	logger, _, err := rlog.FromEnv(&buf)
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}

	logger.Debug("hello")
	if !strings.Contains(buf.String(), `"msg":"hello"`) {
		t.Errorf("expected JSON debug output, got %q", buf.String())
	}
}
