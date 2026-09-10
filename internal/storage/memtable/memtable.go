package memtable

import (
	"bytes"
	"math"
	"math/bits"
	"math/rand/v2"
	"sync"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage"
)

const (
	maxHeight     = 20
	promotionMask = uint64(3) // one promotion per two zero bits: p = 1/4

	// SizeBytes is a deterministic approximation, not runtime heap telemetry.
	// These word counts cover the table fields and synchronization metadata,
	// and each node's slices, key metadata and bookkeeping. Payload capacity
	// and link slots are added separately.
	wordBytes          = uint64(bits.UintSize / 8)
	tableMetadataBytes = 12 * wordBytes
	nodeMetadataBytes  = 12 * wordBytes
	emptyTableBytes    = tableMetadataBytes + nodeMetadataBytes + maxHeight*wordBytes
)

// Entry is one complete internal-key/value pair. A deletion is represented by
// Key.Kind() == storage.KindDelete; an empty value remains KindValue.
type Entry struct {
	Key   storage.InternalKey
	Value []byte
}

// MemTable is a concurrency-safe ordered collection of internal entries.
// Exact duplicate internal keys replace their value; different versions are
// retained. Zero-value MemTables are not valid; construct one with New.
type MemTable struct {
	mu     sync.RWMutex
	head   *node
	random func() uint64
	size   uint64
	count  int
	height int
	frozen bool
}

type node struct {
	key      storage.InternalKey
	value    []byte
	next     []*node
	keyBytes uint64
}

// New constructs an empty mutable MemTable. Topology randomness affects only
// expected performance, never ordering or results.
func New() *MemTable {
	return newWithRandom(rand.Uint64)
}

func newWithRandom(random func() uint64) *MemTable {
	return &MemTable{
		head:   &node{next: make([]*node, maxHeight)},
		random: random,
		size:   emptyTableBytes,
		height: 1,
	}
}

// Insert adds key and a copied value. An exactly equal internal key atomically
// replaces its old value. Deletions must not carry value bytes.
func (m *MemTable) Insert(key storage.InternalKey, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrFrozen
	}
	if key.Kind() == storage.KindDelete && len(value) != 0 {
		return ErrDeleteHasValue
	}

	var predecessors [maxHeight]*node
	candidate := m.lowerBoundLocked(key, predecessors[:])
	if candidate != nil && storage.CompareInternal(candidate.key, key) == 0 {
		m.replaceValueLocked(candidate, value)
		m.validateIfEnabledLocked()
		return nil
	}

	ownedKey, keyBytes := cloneKey(key)
	ownedValue := make([]byte, len(value))
	copy(ownedValue, value)
	height := m.randomHeight()
	if height > m.height {
		for level := m.height; level < height; level++ {
			predecessors[level] = m.head
		}
		m.height = height
	}

	n := &node{
		key:      ownedKey,
		value:    ownedValue,
		next:     make([]*node, height),
		keyBytes: keyBytes,
	}
	for level := range n.next {
		// randomHeight is capped at maxHeight, the fixed predecessor-array
		// length. This assertion keeps that dependency local and explicit.
		invariant.Assert(level < len(predecessors), "STORAGE-23", "node level %d exceeds predecessor array", level)
		predecessor := predecessors[level] //nolint:gosec // bounded by the assertion above
		n.next[level] = predecessor.next[level]
		predecessor.next[level] = n
	}

	m.count++
	m.size = saturatingAdd(m.size, nodeSize(n))
	m.validateIfEnabledLocked()
	return nil
}

// Get returns an exact internal-key match. Returned bytes do not alias table
// storage.
func (m *MemTable) Get(key storage.InternalKey) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n := m.lowerBoundLocked(key, nil)
	if n == nil || storage.CompareInternal(n.key, key) != 0 {
		return Entry{}, false
	}
	return cloneEntry(n), true
}

// Seek returns the comparator lower bound: the first entry K' for which
// CompareInternal(K', key) >= 0.
func (m *MemTable) Seek(key storage.InternalKey) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n := m.lowerBoundLocked(key, nil)
	if n == nil {
		return Entry{}, false
	}
	return cloneEntry(n), true
}

// GetCandidate returns the newest entry for userKey whose sequence is at most
// target. It returns tombstones without interpreting them.
func (m *MemTable) GetCandidate(userKey []byte, target uint64) (Entry, bool) {
	seekKey, err := storage.NewInternalKey(userKey, target, storage.KindDelete)
	invariant.Assert(err == nil, "STORAGE-27", "construct valid candidate seek key: %v", err)

	m.mu.RLock()
	defer m.mu.RUnlock()

	n := m.lowerBoundLocked(seekKey, nil)
	if n == nil || !bytes.Equal(n.key.UserKey(), userKey) {
		return Entry{}, false
	}
	return cloneEntry(n), true
}

// Iterator returns a stable snapshot of every entry in comparator order.
func (m *MemTable) Iterator() *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return newIterator(m.snapshotLocked(m.head.next[0], nil))
}

// IteratorFrom returns a stable snapshot beginning at the comparator lower
// bound of key.
func (m *MemTable) IteratorFrom(key storage.InternalKey) *Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return newIterator(m.snapshotLocked(m.lowerBoundLocked(key, nil), nil))
}

// Range returns a stable snapshot of entries whose decoded user keys are in
// [start, end). A nil bound is unbounded. A non-nil empty end is the valid
// empty user-key boundary.
func (m *MemTable) Range(start, end []byte) (*Iterator, error) {
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, ErrInvalidRange
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	first := m.head.next[0]
	if start != nil {
		lower, err := storage.NewInternalKey(start, math.MaxUint64, storage.KindDelete)
		invariant.Assert(err == nil, "STORAGE-28", "construct valid range seek key: %v", err)
		first = m.lowerBoundLocked(lower, nil)
	}
	return newIterator(m.snapshotLocked(first, end)), nil
}

// Freeze permanently makes the table immutable. It is idempotent.
func (m *MemTable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frozen = true
	m.validateIfEnabledLocked()
}

// Frozen reports whether Freeze has completed.
func (m *MemTable) Frozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.frozen
}

// Len returns the number of distinct internal keys.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// SizeBytes returns the deterministic approximate table-owned memory size.
func (m *MemTable) SizeBytes() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// ReachedSize reports whether SizeBytes has reached target. It does not freeze
// or flush the table.
func (m *MemTable) ReachedSize(target uint64) bool {
	return m.SizeBytes() >= target
}

func (m *MemTable) lowerBoundLocked(key storage.InternalKey, predecessors []*node) *node {
	current := m.head
	for level := m.height - 1; level >= 0; level-- {
		for current.next[level] != nil && storage.CompareInternal(current.next[level].key, key) < 0 {
			current = current.next[level]
		}
		if predecessors != nil {
			predecessors[level] = current
		}
	}
	return current.next[0]
}

func (m *MemTable) snapshotLocked(first *node, end []byte) []Entry {
	entries := make([]Entry, 0)
	for current := first; current != nil; current = current.next[0] {
		if end != nil && bytes.Compare(current.key.UserKey(), end) >= 0 {
			break
		}
		entries = append(entries, cloneEntry(current))
	}
	return entries
}

func (m *MemTable) replaceValueLocked(n *node, value []byte) {
	oldCapacity := cap(n.value)
	if len(value) <= oldCapacity {
		oldLength := len(n.value)
		n.value = n.value[:len(value)]
		copy(n.value, value)
		if len(value) < oldLength {
			clear(n.value[len(value):oldLength])
		}
		return
	}

	n.value = make([]byte, len(value))
	copy(n.value, value)
	m.size = saturatingAdd(m.size, uint64(cap(n.value))-uint64(oldCapacity))
}

func (m *MemTable) randomHeight() int {
	height := 1
	value := m.random()
	for height < maxHeight && value&promotionMask == 0 {
		height++
		value >>= 2
	}
	return height
}

func cloneKey(key storage.InternalKey) (storage.InternalKey, uint64) {
	userKey := key.UserKey()
	cloned, err := storage.NewInternalKey(userKey, key.Sequence(), key.Kind())
	invariant.Assert(err == nil, "STORAGE-25", "clone valid internal key: %v", err)
	return cloned, uint64(len(userKey))
}

func cloneEntry(n *node) Entry {
	return Entry{Key: n.key, Value: bytes.Clone(n.value)}
}

func nodeSize(n *node) uint64 {
	size := nodeMetadataBytes
	size = saturatingAdd(size, n.keyBytes)
	size = saturatingAdd(size, uint64(cap(n.value)))
	return saturatingAdd(size, uint64(len(n.next))*wordBytes)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
