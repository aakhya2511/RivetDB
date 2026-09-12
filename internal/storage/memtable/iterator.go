package memtable

import "bytes"

// Iterator traverses a stable snapshot. It owns its snapshot and requires no
// Close. The zero value is an exhausted iterator.
type Iterator struct {
	entries []Entry
	index   int
	valid   bool
}

func newIterator(entries []Entry) *Iterator {
	return &Iterator{entries: entries, index: -1}
}

// Next advances to the next entry. It remains false after exhaustion.
func (it *Iterator) Next() bool {
	if it == nil || it.index >= len(it.entries) {
		return false
	}

	it.index++
	it.valid = it.index < len(it.entries)
	return it.valid
}

// Entry returns a copy of the current entry and false before the first Next or
// after exhaustion.
func (it *Iterator) Entry() (Entry, bool) {
	if it == nil || !it.valid {
		return Entry{}, false
	}
	entry := it.entries[it.index]
	entry.Value = bytes.Clone(entry.Value)
	return entry, true
}

// BorrowedEntry returns a read-only view of the current snapshot entry. The
// view remains valid for the Iterator lifetime; callers must not mutate it.
// Unlike Entry, this avoids copying a value that the Iterator snapshot already
// owns. It is intended for internal merge consumers that transfer ownership of
// the snapshot bytes into an independently owned result.
func (it *Iterator) BorrowedEntry() (Entry, bool) {
	if it == nil || !it.valid {
		return Entry{}, false
	}
	return it.entries[it.index], true
}
