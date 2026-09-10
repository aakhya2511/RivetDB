package memtable

import "bytes"

// FrozenIterator traverses a frozen MemTable directly in comparator order.
// It does not allocate a full-table snapshot. InternalKey fields are private,
// and Entry copies the value, so callers cannot mutate MemTable-owned bytes.
// The zero value is exhausted.
type FrozenIterator struct {
	current *node
	valid   bool
}

// FrozenIterator returns a direct iterator only after Freeze has made the
// level-0 chain permanently immutable.
func (m *MemTable) FrozenIterator() (*FrozenIterator, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.frozen {
		return nil, ErrNotFrozen
	}
	return &FrozenIterator{current: m.head}, nil
}

// Next advances to the next immutable node. It remains false after exhaustion.
func (it *FrozenIterator) Next() bool {
	if it == nil || it.current == nil {
		return false
	}
	it.current = it.current.next[0]
	it.valid = it.current != nil
	return it.valid
}

// Entry returns the current entry without exposing mutable MemTable storage.
func (it *FrozenIterator) Entry() (Entry, bool) {
	if it == nil || !it.valid {
		return Entry{}, false
	}
	return Entry{Key: it.current.key, Value: bytes.Clone(it.current.value)}, true
}
