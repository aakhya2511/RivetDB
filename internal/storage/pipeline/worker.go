package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/rlog"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

func (p *Pipeline) runWorker() {
	defer p.wg.Done()
	for {
		item := p.claimFlush()
		if item == nil {
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		started := p.clock.Now()
		p.logger.Info("flush started", slog.Uint64("generation", item.id), slog.Uint64("file", item.fileNumber), slog.Uint64("smallest_sequence", item.smallestSeq), slog.Uint64("largest_sequence", item.largestSeq), slog.Int("entries", item.table.Len()), slog.Uint64("bytes", item.table.SizeBytes()))
		result, err := p.flush(p.ctx, item)
		p.finishFlush(item, result, err, p.clock.Since(started))
	}
}

func (p *Pipeline) claimFlush() *generation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.immutables) == 0 || p.immutables[0].state != StateQueued {
		return nil
	}
	item := p.immutables[0]
	if err := item.transition(StateFlushing); err != nil {
		invariant.Assert(false, "STORAGE-51", "%v", err)
	}
	p.validateIfEnabledLocked()
	return item
}

func (p *Pipeline) finishFlush(item *generation, result flushResult, flushErr error, duration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	invariant.Assert(len(p.immutables) != 0 && p.immutables[0] == item, "STORAGE-51", "flush completion for non-head generation %d", item.id)
	if flushErr != nil {
		if err := item.transition(StateFailed); err != nil {
			invariant.Assert(false, "STORAGE-54", "%v", err)
		}
		item.flushErr = flushErr
		item.ambiguous = result.ambiguous
		p.stats.FlushFailures++
		p.logger.Error("flush failed", slog.Uint64("generation", item.id), slog.Uint64("file", item.fileNumber), slog.Int64("duration_ns", duration.Nanoseconds()), slog.Bool("ambiguous", item.ambiguous), slog.String("error_class", fmt.Sprintf("%T", flushErr)), rlog.Err(flushErr))
		p.notify(p.changed)
		return
	}
	if err := item.transition(StateDurable); err != nil {
		invariant.Assert(false, "STORAGE-53", "%v", err)
	}
	p.outputs = append(p.outputs, FlushOutput{
		Generation: item.id, FileNumber: item.fileNumber, SmallestSeq: item.smallestSeq,
		LargestSeq: item.largestSeq, Metadata: result.metadata, Path: result.path,
	})
	p.stats.Flushes++
	p.stats.SSTableBytes += result.metadata.FileSize
	if duration > 0 {
		p.stats.FlushNanos += uint64(duration) //nolint:gosec // positive duration fits uint64
	}
	p.immutables = p.immutables[1:]
	p.logger.Info("flush completed", slog.Uint64("generation", item.id), slog.Uint64("file", item.fileNumber), slog.Int64("duration_ns", duration.Nanoseconds()), slog.Uint64("bytes", result.metadata.FileSize), slog.Uint64("entries", result.metadata.EntryCount))
	p.validateIfEnabledLocked()
	p.notify(p.changed)
	p.notify(p.wake)
}

func (p *Pipeline) flushToSSTable(options sstable.Options) flushExecutor {
	return func(ctx context.Context, item *generation) (flushResult, error) {
		if err := ctx.Err(); err != nil {
			return flushResult{}, fmt.Errorf("before flush: %w", err)
		}
		writer, err := sstable.OpenWriter(p.directory, item.fileNumber, options)
		if err != nil {
			return flushResult{ambiguous: finalExists(p.directory, item.fileNumber)}, fmt.Errorf("open flush SSTable writer: %w", err)
		}
		iterator := item.table.Iterator()
		for iterator.Next() {
			entry, ok := iterator.Entry()
			invariant.Assert(ok, "STORAGE-52", "valid MemTable iterator has no entry")
			if addErr := writer.Add(entry.Key, entry.Value); addErr != nil {
				abortErr := writer.Abort()
				return flushResult{ambiguous: finalExists(p.directory, item.fileNumber)}, errors.Join(fmt.Errorf("write immutable generation %d: %w", item.id, addErr), abortErr)
			}
		}
		metadata, err := writer.Finish()
		if err != nil {
			abortErr := writer.Abort()
			return flushResult{ambiguous: finalExists(p.directory, item.fileNumber)}, errors.Join(fmt.Errorf("finish immutable generation %d: %w", item.id, err), abortErr)
		}
		path := filepath.Join(p.directory, sstable.FileName(item.fileNumber))
		if err := validateFlushOutput(path, item.table); err != nil {
			return flushResult{metadata: metadata, path: path, ambiguous: true}, err
		}
		return flushResult{metadata: metadata, path: path}, nil
	}
}

func validateFlushOutput(path string, table *memtable.MemTable) (resultErr error) {
	reader, err := sstable.Open(path, sstable.ReaderOptions{})
	if err != nil {
		return fmt.Errorf("open published flush output: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, reader.Close()) }()
	want := table.Iterator()
	got, err := reader.NewIterator()
	if err != nil {
		return fmt.Errorf("iterate published flush output: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, got.Close()) }()
	var count uint64
	for want.Next() {
		wantEntry, ok := want.Entry()
		invariant.Assert(ok, "STORAGE-52", "valid immutable iterator has no entry")
		if !got.Next() {
			if got.Error() != nil {
				return fmt.Errorf("read published flush output: %w", got.Error())
			}
			return errors.Join(ErrFlushFailed, errFlushMissingEntries)
		}
		gotEntry, ok := got.Entry()
		if !ok || storage.CompareInternal(wantEntry.Key, gotEntry.Key) != 0 || !bytes.Equal(wantEntry.Value, gotEntry.Value) {
			return errors.Join(ErrFlushFailed, errFlushContentMismatch)
		}
		count++
	}
	if got.Next() {
		return errors.Join(ErrFlushFailed, errFlushExtraEntries)
	}
	if err := got.Error(); err != nil {
		return fmt.Errorf("finish published flush iteration: %w", err)
	}
	metadata := reader.Metadata()
	if count != uint64(table.Len()) || metadata.EntryCount != count { //nolint:gosec // table length is nonnegative
		return errors.Join(ErrFlushFailed, errFlushCountMismatch)
	}
	return nil
}

func finalExists(directory string, fileNumber uint64) bool {
	_, err := os.Lstat(filepath.Join(directory, sstable.FileName(fileNumber)))
	return err == nil
}
