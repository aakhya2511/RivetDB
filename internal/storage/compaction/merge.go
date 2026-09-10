package compaction

import (
	"container/heap"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

// EntryIterator is the common ordered-entry contract shared by compaction and
// integrated read-side merging.
type EntryIterator interface {
	Next() bool
	Entry() (sstable.Entry, bool)
	Error() error
	Close() error
}

// MergeInput identifies one ordered child iterator.
type MergeInput struct {
	FileNumber uint64
	Iterator   EntryIterator
}
type cursor struct {
	entry   sstable.Entry
	input   MergeInput
	ordinal int
}
type cursorHeap []*cursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	if order := storage.CompareInternal(h[i].entry.Key, h[j].entry.Key); order != 0 {
		return order < 0
	}
	if h[i].input.FileNumber != h[j].input.FileNumber {
		return h[i].input.FileNumber < h[j].input.FileNumber
	}
	return h[i].ordinal < h[j].ordinal
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(value any) {
	item, ok := value.(*cursor)
	if !ok {
		panic("STORAGE-74: non-cursor heap value")
	}
	*h = append(*h, item)
}
func (h *cursorHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

// MergeIterator performs a bounded O(N log K) merge and preserves multiplicity.
type MergeIterator struct {
	heap          cursorHeap
	inputs        []MergeInput
	current       sstable.Entry
	currentFile   uint64
	valid, closed bool
	err           error
}

func NewMergeIterator(inputs []MergeInput) (*MergeIterator, error) {
	m := &MergeIterator{inputs: append([]MergeInput(nil), inputs...)}
	for ordinal, input := range m.inputs {
		if input.Iterator == nil {
			if closeErr := m.Close(); closeErr != nil {
				return nil, closeErr
			}
			return nil, ErrInvalidOptions
		}
		if input.Iterator.Next() {
			entry, ok := input.Iterator.Entry()
			if !ok {
				if closeErr := m.Close(); closeErr != nil {
					return nil, closeErr
				}
				return nil, ErrInputCorrupt
			}
			heap.Push(&m.heap, &cursor{entry: entry, input: input, ordinal: ordinal})
		} else if err := input.Iterator.Error(); err != nil {
			if closeErr := m.Close(); closeErr != nil {
				return nil, closeErr
			}
			return nil, fmt.Errorf("prime input %d: %w", input.FileNumber, err)
		}
	}
	return m, nil
}
func (m *MergeIterator) Next() bool {
	if m == nil || m.closed || m.err != nil || len(m.heap) == 0 {
		return false
	}
	value := heap.Pop(&m.heap)
	item, ok := value.(*cursor)
	if !ok {
		m.err = ErrInputCorrupt
		return false
	}
	m.current, m.currentFile, m.valid = item.entry, item.input.FileNumber, true
	if item.input.Iterator.Next() {
		entry, ok := item.input.Iterator.Entry()
		if !ok {
			m.err, m.valid = ErrInputCorrupt, false
			return false
		}
		item.entry = entry
		heap.Push(&m.heap, item)
	} else if err := item.input.Iterator.Error(); err != nil {
		m.err = fmt.Errorf("advance input %d: %w", item.input.FileNumber, err)
		m.valid = false
		return false
	}
	return true
}

// SourceFileNumber identifies the child that produced the current entry.
func (m *MergeIterator) SourceFileNumber() (uint64, bool) {
	if m == nil || !m.valid || m.closed {
		return 0, false
	}
	return m.currentFile, true
}
func (m *MergeIterator) Entry() (sstable.Entry, bool) {
	if m == nil || !m.valid || m.closed {
		return sstable.Entry{}, false
	}
	return m.current, true
}
func (m *MergeIterator) Error() error {
	if m == nil {
		return nil
	}
	return m.err
}
func (m *MergeIterator) Close() error {
	if m == nil || m.closed {
		return nil
	}
	m.closed = true
	var errs []error
	for _, input := range m.inputs {
		if err := input.Iterator.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close input %d: %w", input.FileNumber, err))
		}
	}
	return errors.Join(errs...)
}
