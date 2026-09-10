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
