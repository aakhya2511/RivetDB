package sstable

import (
	"bytes"
	"fmt"
	"math"

	"github.com/rivetdb/rivetdb/internal/storage"
)

// Iterator retains only one decoded data block. Entry returns owned copies.
type Iterator struct {
	reader     *Reader
	block      decodedDataBlock
	blockIndex int
	entryIndex int
	end        []byte
	started    bool
	valid      bool
	closed     bool
	err        error
}

// NewIterator returns an iterator over the complete table.
func (r *Reader) NewIterator() (*Iterator, error) {
	return r.newIteratorAt(nil, false, nil)
}

// IteratorFrom begins at the CompareInternal lower bound of target.
func (r *Reader) IteratorFrom(target storage.InternalKey) (*Iterator, error) {
	return r.newIteratorAt(&target, false, nil)
}

// Range returns all versions whose user key lies in [start,end). Nil is unbounded.
func (r *Reader) Range(start, end []byte) (*Iterator, error) {
	if len(start) > MaxUserKeySize || len(end) > MaxUserKeySize {
		return nil, ErrKeyTooLarge
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, ErrInvalidRange
	}
	var target *storage.InternalKey
	if start != nil {
		key, err := storage.NewInternalKey(start, math.MaxUint64, storage.KindDelete)
		if err != nil {
			return nil, fmt.Errorf("construct range seek key: %w", err)
		}
		target = &key
	}
	return r.newIteratorAt(target, true, end)
}

func (r *Reader) newIteratorAt(target *storage.InternalKey, rangeMode bool, end []byte) (*Iterator, error) {
	if r == nil || r.isClosed() {
		return nil, ErrClosed
	}
	it := &Iterator{reader: r, blockIndex: -1, entryIndex: -1, end: bytes.Clone(end)}
	if target == nil {
		return it, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrClosed
	}
	blockIndex := lowerBoundIndex(r.index, *target)
	if blockIndex == len(r.index) {
		it.started = true
		it.blockIndex = len(r.index)
		return it, nil
	}
	block, err := r.loadDataBlockUnlocked(blockIndex)
	if err != nil {
		return nil, err
	}
	entryIndex := block.lowerBound(*target)
	if entryIndex == len(block.entries) {
		blockIndex++
		if blockIndex == len(r.index) {
			it.started = true
			it.blockIndex = len(r.index)
			return it, nil
		}
		block, err = r.loadDataBlockUnlocked(blockIndex)
		if err != nil {
			return nil, err
		}
		entryIndex = 0
	}
	it.block, it.blockIndex, it.entryIndex = block, blockIndex, entryIndex-1
	if rangeMode && end != nil && bytes.Compare(block.entries[entryIndex].key.UserKey(), end) >= 0 {
		it.block = decodedDataBlock{}
		it.started = true
		it.blockIndex = len(r.index)
	}
	return it, nil
}

// Next advances once. It remains false after EOF, Close or an error.
func (it *Iterator) Next() bool {
	if it == nil || it.closed || it.err != nil || it.reader == nil {
		return false
	}
	if it.reader.isClosed() {
		it.err = ErrClosed
		it.valid = false
		return false
	}
	it.valid = false
	if !it.started {
		it.started = true
		if len(it.block.entries) == 0 {
			if !it.loadBlock(0) {
				return false
			}
		}
	}
	it.entryIndex++
	for it.entryIndex >= len(it.block.entries) {
		if !it.loadBlock(it.blockIndex + 1) {
			return false
		}
		it.entryIndex = 0
	}
	if it.end != nil && bytes.Compare(it.block.entries[it.entryIndex].key.UserKey(), it.end) >= 0 {
		it.block = decodedDataBlock{}
		return false
	}
	it.valid = true
	return true
}

func (it *Iterator) loadBlock(position int) bool {
	it.reader.mu.RLock()
	defer it.reader.mu.RUnlock()
	if it.reader.closed {
		it.err = ErrClosed
		return false
	}
	if position >= len(it.reader.index) {
		it.block = decodedDataBlock{}
		it.blockIndex = len(it.reader.index)
		return false
	}
	block, err := it.reader.loadDataBlockUnlocked(position)
	if err != nil {
		it.err = err
		return false
	}
	it.block = block
	it.blockIndex = position
	return true
}

// Entry returns a copy of the current entry.
func (it *Iterator) Entry() (Entry, bool) {
	if it == nil || !it.valid || it.closed {
		return Entry{}, false
	}
	return cloneDecodedEntry(it.block.entries[it.entryIndex]), true
}

// Error reports corruption, I/O or close failure. Clean EOF returns nil.
func (it *Iterator) Error() error {
	if it == nil {
		return nil
	}
	return it.err
}

// Close releases the current block and is idempotent.
func (it *Iterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	it.valid = false
	it.block = decodedDataBlock{}
	return nil
}
