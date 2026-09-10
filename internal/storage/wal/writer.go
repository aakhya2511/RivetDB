package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Durability defines when Append may acknowledge a logical record.
type Durability uint8

const (
	// SyncBatch is the default and fsyncs every complete logical record before
	// Append acknowledges it.
	SyncBatch Durability = iota
	// SyncNone acknowledges after successful writes. A later explicit Sync is
	// required for machine-crash durability.
	SyncNone
)

// WriterOptions configures a Writer.
type WriterOptions struct {
	Durability Durability
}

// Position is the physical half-open byte interval occupied by one logical
// record, from its first fragment header through its final fragment payload.
type Position struct {
	Start int64
	End   int64
}

type writableFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type syncCloser interface {
	Sync() error
	Close() error
}

// Writer appends complete logical records. Its methods are safe for concurrent
// use; Append calls are serialized and fragments never interleave.
type Writer struct {
	mu         sync.Mutex
	file       writableFile
	directory  syncCloser
	offset     int64
	durability Durability
	closed     bool
	failed     error
	closeErr   error
}

// OpenWriter opens or creates path. Existing bytes must form a clean WAL;
// callers must explicitly recover and repair a truncated tail first.
func OpenWriter(path string, options WriterOptions) (*Writer, error) {
	if !options.Durability.valid() {
		return nil, ErrInvalidDurability
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600) //nolint:gosec // caller chooses database path
	created := err == nil
	if errors.Is(err, os.ErrExist) {
		file, err = os.OpenFile(path, os.O_RDWR, 0o600) //nolint:gosec // caller chooses database path
	}
	if err != nil {
		return nil, fmt.Errorf("open WAL writer %s: %w", path, err)
	}
	var directory *os.File
	if created {
		directory, err = os.Open(filepath.Dir(path)) //nolint:gosec // parent of caller-selected database path
		if err != nil {
			closeErr := file.Close()
			return nil, errors.Join(fmt.Errorf("open WAL directory for %s: %w", path, err), closeErr)
		}
	}
	stat, err := file.Stat()
	if err != nil {
		closeErr := closeOpened(file, directory)
		return nil, errors.Join(fmt.Errorf("stat WAL %s: %w", path, err), closeErr)
	}

	reader, err := newReader(file, stat.Size(), nil)
	if err != nil {
		closeErr := closeOpened(file, directory)
		return nil, errors.Join(fmt.Errorf("inspect WAL %s: %w", path, err), closeErr)
	}
	for reader.Next() {
	}
	if scanErr := reader.Err(); scanErr != nil {
		closeErr := closeOpened(file, directory)
		return nil, errors.Join(fmt.Errorf("inspect WAL %s: %w", path, scanErr), closeErr)
	}
	if _, truncated := reader.Tail(); truncated {
		closeErr := closeOpened(file, directory)
		return nil, errors.Join(fmt.Errorf("open WAL %s: %w", path, ErrTruncatedTail), closeErr)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		closeErr := closeOpened(file, directory)
		return nil, errors.Join(fmt.Errorf("seek WAL %s: %w", path, err), closeErr)
	}
	var durableDirectory syncCloser
	if directory != nil {
		durableDirectory = directory
	}
	return newWriterWithDirectory(file, durableDirectory, stat.Size(), options.Durability), nil
}

func newWriter(file writableFile, offset int64, durability Durability) *Writer {
	return newWriterWithDirectory(file, nil, offset, durability)
}

func newWriterWithDirectory(file writableFile, directory syncCloser, offset int64, durability Durability) *Writer {
	return &Writer{file: file, directory: directory, offset: offset, durability: durability}
}

// Append writes one logical record. In SyncBatch mode, a nil error means the
// record's bytes and file-size metadata were successfully fsynced.
func (w *Writer) Append(payload []byte) (Position, error) {
	if len(payload) > MaxRecordSize {
		return Position{}, ErrRecordTooLarge
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return Position{}, ErrClosedWriter
	}
	if w.failed != nil {
		return Position{}, w.failed
	}

	consumed := 0
	first := true
	var position Position
	for first || consumed < len(payload) {
		blockRemaining := BlockSize - int(w.offset%BlockSize)
		if blockRemaining < HeaderSize {
			if err := w.writeAll(make([]byte, blockRemaining)); err != nil {
				return Position{}, err
			}
			blockRemaining = BlockSize
		}
		if first {
			position.Start = w.offset
		}

		fragmentLength := min(len(payload)-consumed, blockRemaining-HeaderSize)
		last := consumed+fragmentLength == len(payload)
		kind := chooseFragmentKind(first, last)
		fragment := payload[consumed : consumed+fragmentLength]
		header := makeHeader(fragment, kind)
		if err := w.writeAll(header[:]); err != nil {
			return Position{}, err
		}
		if err := w.writeAll(fragment); err != nil {
			return Position{}, err
		}
		consumed += fragmentLength
		first = false
	}
	position.End = w.offset

	if w.durability == SyncBatch {
		if err := w.syncLocked(); err != nil {
			return Position{}, err
		}
	}
	return position, nil
}

// Sync establishes a durability boundary for all prior successful appends.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosedWriter
	}
	if w.failed != nil {
		return w.failed
	}
	return w.syncLocked()
}

// Close closes the WAL file. It is idempotent and returns the same close error
// on repeated calls.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.closeErr
	}
	w.closed = true
	var errs []error
	if err := w.file.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close WAL: %w", err))
	}
	if w.directory != nil {
		if err := w.directory.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close WAL directory: %w", err))
		}
		w.directory = nil
	}
	w.closeErr = errors.Join(errs...)
	return w.closeErr
}

func (w *Writer) writeAll(data []byte) error {
	for len(data) > 0 {
		n, err := w.file.Write(data)
		if n < 0 || n > len(data) {
			return w.poison(fmt.Errorf("write WAL returned impossible count %d: %w", n, io.ErrShortWrite))
		}
		w.offset += int64(n)
		data = data[n:]
		if err != nil {
			return w.poison(fmt.Errorf("write WAL at offset %d: %w", w.offset, err))
		}
		if n == 0 {
			return w.poison(fmt.Errorf("write WAL at offset %d: %w", w.offset, io.ErrShortWrite))
		}
	}
	return nil
}

func (w *Writer) poison(cause error) error {
	w.failed = errors.Join(ErrWriterFailed, cause)
	return w.failed
}

func (w *Writer) syncLocked() error {
	if err := w.file.Sync(); err != nil {
		return w.poison(fmt.Errorf("sync WAL file: %w", err))
	}
	if w.directory == nil {
		return nil
	}
	if err := w.directory.Sync(); err != nil {
		return w.poison(fmt.Errorf("sync WAL directory: %w", err))
	}
	directory := w.directory
	w.directory = nil
	if err := directory.Close(); err != nil {
		return w.poison(fmt.Errorf("close synced WAL directory: %w", err))
	}
	return nil
}

func closeOpened(file *os.File, directory *os.File) error {
	var errs []error
	if err := file.Close(); err != nil {
		errs = append(errs, err)
	}
	if directory != nil {
		if err := directory.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (d Durability) valid() bool {
	return d == SyncBatch || d == SyncNone
}

func chooseFragmentKind(first, last bool) fragmentKind {
	switch {
	case first && last:
		return fragmentFull
	case first:
		return fragmentFirst
	case last:
		return fragmentLast
	default:
		return fragmentMiddle
	}
}
