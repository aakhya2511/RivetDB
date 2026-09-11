package multiraft

import (
	"sort"
	"sync"

	"github.com/rivetdb/rivetdb/internal/replicatedrange"
)

type RangeRegistry struct {
	mu     sync.RWMutex
	ranges map[RangeID]*replicatedrange.Replica
}

func newRangeRegistry() *RangeRegistry {
	return &RangeRegistry{ranges: make(map[RangeID]*replicatedrange.Replica)}
}

func (r *RangeRegistry) Register(rangeID RangeID, replica *replicatedrange.Replica) error {
	if rangeID == 0 || replica == nil {
		return ErrInvalidDescriptor
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.ranges[rangeID]; exists {
		return ErrDuplicateRange
	}
	r.ranges[rangeID] = replica
	return nil
}

func (r *RangeRegistry) Lookup(rangeID RangeID) (*replicatedrange.Replica, error) {
	r.mu.RLock()
	replica := r.ranges[rangeID]
	r.mu.RUnlock()
	if replica == nil {
		return nil, ErrUnknownRange
	}
	return replica, nil
}

func (r *RangeRegistry) Unregister(rangeID RangeID) (*replicatedrange.Replica, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	replica := r.ranges[rangeID]
	if replica == nil {
		return nil, ErrUnknownRange
	}
	delete(r.ranges, rangeID)
	return replica, nil
}

func (r *RangeRegistry) IDs() []RangeID {
	r.mu.RLock()
	result := make([]RangeID, 0, len(r.ranges))
	for rangeID := range r.ranges {
		result = append(result, rangeID)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	return result
}
