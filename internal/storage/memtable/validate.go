package memtable

import (
	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage"
)

// Validate checks the complete skip-list structure and panics with a named
// invariant violation if it is corrupt. It is intended for tests and explicit
// diagnostics; Insert calls it automatically only when invariant.Expensive is
// enabled.
func (m *MemTable) Validate() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.validateLocked()
}

func (m *MemTable) validateIfEnabledLocked() {
	if invariant.Expensive() {
		m.validateLocked()
	}
}

func (m *MemTable) validateLocked() {
	invariant.Assert(m.head != nil, "STORAGE-23", "nil head")
	invariant.Assert(len(m.head.next) == maxHeight, "STORAGE-23", "head height %d, want %d", len(m.head.next), maxHeight)
	invariant.Assert(m.height >= 1 && m.height <= maxHeight, "STORAGE-23", "active height %d outside [1,%d]", m.height, maxHeight)

	levelZero := make(map[*node]struct{}, m.count)
	count := 0
	observedHeight := 1
	expectedSize := emptyTableBytes
	var previous *node
	for current := m.head.next[0]; current != nil; current = current.next[0] {
		_, duplicatePointer := levelZero[current]
		invariant.Assert(!duplicatePointer, "STORAGE-23", "cycle or repeated node at level zero")
		levelZero[current] = struct{}{}
		invariant.Assert(len(current.next) >= 1 && len(current.next) <= maxHeight, "STORAGE-23", "node height %d outside [1,%d]", len(current.next), maxHeight)
		if len(current.next) > observedHeight {
			observedHeight = len(current.next)
		}
		invariant.Assert(current.keyBytes == uint64(len(current.key.UserKey())), "STORAGE-29", "recorded key bytes %d do not match key length", current.keyBytes)
		if previous != nil {
			invariant.Assert(storage.CompareInternal(previous.key, current.key) < 0, "STORAGE-24", "level-zero keys are not strictly ordered")
		}
		previous = current
		count++
		expectedSize = saturatingAdd(expectedSize, nodeSize(current))
	}
	invariant.Assert(count == m.count, "STORAGE-23", "level-zero count %d, metadata count %d", count, m.count)
	invariant.Assert(observedHeight == m.height, "STORAGE-23", "observed height %d, metadata height %d", observedHeight, m.height)
	invariant.Assert(expectedSize == m.size, "STORAGE-29", "computed size %d, metadata size %d", expectedSize, m.size)

	for level := 1; level < maxHeight; level++ {
		seen := make(map[*node]struct{})
		previous = nil
		for current := m.head.next[level]; current != nil; current = current.next[level] {
			_, repeated := seen[current]
			invariant.Assert(!repeated, "STORAGE-23", "cycle or repeated node at level %d", level)
			seen[current] = struct{}{}
			_, inLevelZero := levelZero[current]
			invariant.Assert(inLevelZero, "STORAGE-23", "level %d contains a node absent from level zero", level)
			invariant.Assert(len(current.next) > level, "STORAGE-23", "level %d contains node of height %d", level, len(current.next))
			if previous != nil {
				invariant.Assert(storage.CompareInternal(previous.key, current.key) < 0, "STORAGE-23", "level %d is not strictly ordered", level)
			}
			previous = current
		}
	}
}
