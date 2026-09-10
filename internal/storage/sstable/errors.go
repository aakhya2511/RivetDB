// Package sstable implements RivetDB's immutable sorted-table format writer.
package sstable

import "errors"

var (
	ErrInvalidFileNumber = errors.New("invalid SSTable file number")
	ErrInvalidOptions    = errors.New("invalid SSTable writer options")
	ErrFileExists        = errors.New("SSTable path already exists")
	ErrOutOfOrder        = errors.New("SSTable input is out of order")
	ErrDuplicateKey      = errors.New("duplicate SSTable internal key")
	ErrDeleteHasValue    = errors.New("SSTable deletion has a value")
	ErrKeyTooLarge       = errors.New("SSTable key exceeds maximum size")
	ErrValueTooLarge     = errors.New("SSTable value exceeds maximum size")
	ErrEntryTooLarge     = errors.New("SSTable entry exceeds maximum size")
	ErrTableTooLarge     = errors.New("SSTable exceeds maximum size")
	ErrTooManyBlocks     = errors.New("SSTable has too many data blocks")
	ErrEmptyTable        = errors.New("empty SSTable is not permitted")
	ErrWriterFailed      = errors.New("SSTable writer failed")
	ErrFinished          = errors.New("SSTable writer is finished")
	ErrAborted           = errors.New("SSTable writer is aborted")
)
