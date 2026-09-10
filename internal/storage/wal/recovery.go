package wal

import (
	"errors"
	"fmt"
	"os"
)

// RecoveryResult is a non-mutating scan result. It is also the token required
// by RepairTail, which rescans and verifies it before truncating.
type RecoveryResult struct {
	ValidEnd      int64
	FileEnd       int64
	Records       uint64
	TailTruncated bool
}

// Recover streams every complete record to consume and reports the proven
// valid prefix. A nil consume callback performs inspection only.
func Recover(path string, consume func(Record) error) (RecoveryResult, error) {
	reader, err := OpenReader(path)
	if err != nil {
		return RecoveryResult{}, err
	}
	result, scanErr := recoverFromReader(reader, consume)
	closeErr := reader.Close()
	if scanErr != nil || closeErr != nil {
		return result, errors.Join(scanErr, closeErr)
	}
	return result, nil
}

func recoverFromReader(reader *Reader, consume func(Record) error) (RecoveryResult, error) {
	var records uint64
	for reader.Next() {
		record := reader.Record()
		if consume != nil {
			if err := consume(record); err != nil {
				return RecoveryResult{ValidEnd: reader.ValidEnd(), FileEnd: reader.Size(), Records: records}, fmt.Errorf("consume WAL record %d: %w", record.Number, err)
			}
		}
		records++
	}
	result := RecoveryResult{ValidEnd: reader.ValidEnd(), FileEnd: reader.Size(), Records: records}
	if _, truncated := reader.Tail(); truncated {
		result.TailTruncated = true
	}
	if err := reader.Err(); err != nil {
		return result, err
	}
	return result, nil
}

// RepairTail verifies that path still has exactly the truncated tail described
// by recovered, truncates to ValidEnd, and fsyncs the repaired file. The caller
// must hold exclusive ownership of the database directory and have no open WAL
// writer; the later engine LOCK enforces this across processes.
func RepairTail(path string, recovered RecoveryResult) error {
	if !recovered.TailTruncated {
		return ErrNoTruncatedTail
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0o600) //nolint:gosec // caller chooses database path
	if err != nil {
		return fmt.Errorf("open WAL for tail repair %s: %w", path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		closeErr := file.Close()
		return errors.Join(fmt.Errorf("stat WAL for tail repair %s: %w", path, err), closeErr)
	}
	reader, err := newReader(file, stat.Size(), nil)
	if err != nil {
		closeErr := file.Close()
		return errors.Join(fmt.Errorf("construct WAL repair reader %s: %w", path, err), closeErr)
	}
	actual, scanErr := recoverFromReader(reader, nil)
	if scanErr != nil {
		closeErr := file.Close()
		return errors.Join(fmt.Errorf("rescan WAL before tail repair %s: %w", path, scanErr), closeErr)
	}
	if actual != recovered || !actual.TailTruncated {
		closeErr := file.Close()
		return errors.Join(ErrRecoveryStateChanged, closeErr)
	}
	if err := file.Truncate(recovered.ValidEnd); err != nil {
		closeErr := file.Close()
		return errors.Join(fmt.Errorf("truncate WAL %s to %d: %w", path, recovered.ValidEnd, err), closeErr)
	}
	if err := file.Sync(); err != nil {
		closeErr := file.Close()
		return errors.Join(fmt.Errorf("sync repaired WAL %s: %w", path, err), closeErr)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close repaired WAL %s: %w", path, err)
	}
	return nil
}
