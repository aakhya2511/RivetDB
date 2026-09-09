// Package rlog is RivetDB's structured logging foundation.
//
// Every subsystem in RivetDB logs through a *slog.Logger obtained from this
// package. The package exists (rather than using log/slog directly) to enforce
// three project-wide conventions:
//
//  1. Logs are structured. RivetDB debugging is mostly correlation across
//     nodes, ranges and Raft terms; free-form message strings cannot be joined.
//     Attribute constructors such as NodeID, RangeID and Term exist so that the
//     same key names are used everywhere and remain greppable/joinable.
//
//  2. Loggers are carried in a context.Context. Request-scoped attributes
//     (node, range, Raft term, transaction ID) are attached once at the entry
//     point and inherited by everything downstream, instead of being threaded
//     through every function signature.
//
//  3. Log output is assertable in tests. Recorder implements slog.Handler and
//     captures records as structured values, so tests assert on attributes
//     rather than on substrings of formatted output.
//
// Configuration is read once at process start. Level is the only setting
// intended to change at runtime, via the LevelVar returned by New.
package rlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Sentinel errors, so that a caller validating configuration can distinguish
// an unknown setting from an I/O failure with errors.Is.
var (
	// ErrUnknownLevel is returned for a log level name that is not recognised.
	ErrUnknownLevel = errors.New("rlog: unknown log level")
	// ErrUnknownFormat is returned for a log format name that is not recognised.
	ErrUnknownFormat = errors.New("rlog: unknown log format")
)

// Environment variables read by FromEnv.
const (
	EnvLevel  = "RIVETDB_LOG_LEVEL"
	EnvFormat = "RIVETDB_LOG_FORMAT"
)

// Attribute keys used across RivetDB. They are declared as constants so that
// the same key is used by every subsystem and log queries stay stable.
const (
	KeyComponent = "component"
	KeyNode      = "node"
	KeyRange     = "range"
	KeyTerm      = "term"
	KeyTxn       = "txn"
	KeyIndex     = "index"
	KeyError     = "err"
)

// Format selects the encoding of log records.
type Format string

const (
	// FormatText is human-oriented key=value output, used for local runs.
	FormatText Format = "text"
	// FormatJSON is machine-oriented output, used in CI and containers.
	FormatJSON Format = "json"
)

// Config describes how a logger renders records. The zero Config is valid and
// yields info-level text output on stderr.
type Config struct {
	// Level is the minimum level that will be emitted.
	Level slog.Level
	// Format selects the record encoding. Empty means FormatText.
	Format Format
	// Output receives encoded records. Nil means os.Stderr.
	Output io.Writer
	// AddSource annotates records with the emitting file and line. It costs a
	// runtime.CallersFrames lookup per record, so it is off by default and
	// intended for debugging sessions rather than benchmarks.
	AddSource bool
}

// ParseLevel converts a level name to an slog.Level. Names are case
// insensitive. It accepts the four slog level names plus "warning".
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%w %q", ErrUnknownLevel, s)
	}
}

// ParseFormat converts a format name to a Format. Names are case insensitive.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "text":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("%w %q", ErrUnknownFormat, s)
	}
}

// New builds a logger from cfg. The returned slog.LevelVar is the live level
// control for that logger: mutating it changes the threshold of every logger
// derived from the returned one, which is how the admin API will raise log
// verbosity on a running node without a restart.
func New(cfg Config) (*slog.Logger, *slog.LevelVar, error) {
	out := cfg.Output
	if out == nil {
		out = os.Stderr
	}

	format := cfg.Format
	if format == "" {
		format = FormatText
	}

	levelVar := new(slog.LevelVar)
	levelVar.Set(cfg.Level)

	opts := &slog.HandlerOptions{
		Level:     levelVar,
		AddSource: cfg.AddSource,
	}

	var handler slog.Handler
	switch format {
	case FormatText:
		handler = slog.NewTextHandler(out, opts)
	case FormatJSON:
		handler = slog.NewJSONHandler(out, opts)
	default:
		return nil, nil, fmt.Errorf("%w %q", ErrUnknownFormat, format)
	}

	return slog.New(handler), levelVar, nil
}

// FromEnv builds a logger configured by RIVETDB_LOG_LEVEL and
// RIVETDB_LOG_FORMAT, falling back to info-level text output. Unset variables
// use the defaults; malformed values are reported as errors rather than
// silently ignored, so a typo in a deployment manifest fails loudly at startup.
func FromEnv(out io.Writer) (*slog.Logger, *slog.LevelVar, error) {
	cfg := Config{Output: out}

	if v, ok := os.LookupEnv(EnvLevel); ok && v != "" {
		level, err := ParseLevel(v)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", EnvLevel, err)
		}
		cfg.Level = level
	}

	if v, ok := os.LookupEnv(EnvFormat); ok && v != "" {
		format, err := ParseFormat(v)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", EnvFormat, err)
		}
		cfg.Format = format
	}

	return New(cfg)
}

// Discard returns a logger that drops every record. Tests that exercise code
// paths whose logging is not under test use it to keep output readable.
func Discard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// Component returns a logger tagged with a subsystem name, for example
// "storage.wal" or "raft". Component names are dotted paths from coarse to
// fine so that log filters can select a whole subsystem by prefix.
func Component(l *slog.Logger, name string) *slog.Logger {
	return l.With(slog.String(KeyComponent, name))
}

// NodeID returns the canonical attribute identifying a RivetDB node.
func NodeID(id uint64) slog.Attr { return slog.Uint64(KeyNode, id) }

// RangeID returns the canonical attribute identifying a key range.
func RangeID(id uint64) slog.Attr { return slog.Uint64(KeyRange, id) }

// Term returns the canonical attribute identifying a Raft term.
func Term(term uint64) slog.Attr { return slog.Uint64(KeyTerm, term) }

// Index returns the canonical attribute identifying a Raft log index.
func Index(index uint64) slog.Attr { return slog.Uint64(KeyIndex, index) }

// TxnID returns the canonical attribute identifying a transaction.
func TxnID(id string) slog.Attr { return slog.String(KeyTxn, id) }

// Err returns the canonical attribute carrying an error. A nil error yields an
// empty Attr, which slog handlers omit, so call sites do not need to branch.
func Err(err error) slog.Attr {
	if err == nil {
		return slog.Attr{}
	}
	return slog.String(KeyError, err.Error())
}

// contextKey is unexported so that no other package can collide with or
// overwrite the logger slot in a context.
type contextKey struct{}

// NewContext returns a copy of ctx carrying l. Passing a nil logger returns ctx
// unchanged so callers can forward optional loggers without branching.
func NewContext(ctx context.Context, l *slog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, l)
}

// FromContext returns the logger stored in ctx. If ctx carries no logger it
// returns a discarding logger rather than nil or the global default: library
// code that was never handed a logger must not write to a process-wide sink
// that tests cannot observe or silence.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(contextKey{}).(*slog.Logger); ok {
		return l
	}
	return Discard()
}

// With returns a copy of ctx whose logger carries the additional attributes.
// It is the usual way to attach request scope: rlog.With(ctx, rlog.RangeID(7)).
func With(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	args := make([]any, len(attrs))
	for i, a := range attrs {
		args[i] = a
	}
	return NewContext(ctx, FromContext(ctx).With(args...))
}
