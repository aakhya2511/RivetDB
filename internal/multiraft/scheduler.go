package multiraft

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type groupKey struct {
	nodeID  raft.NodeID
	rangeID RangeID
}

type SchedulerStats struct {
	Ticks      uint64
	Deliveries uint64
	Errors     uint64
}

type ScheduledError struct {
	NodeID  raft.NodeID
	RangeID RangeID
	Err     error
}

// Scheduler fairly drives synchronous Raft groups. It owns no goroutine and
// uses no wall clock; an external runtime decides when a logical round occurs.
type Scheduler struct {
	mu        sync.Mutex
	nodes     map[raft.NodeID]*Node
	transport *Transport
	groups    []groupKey
	cursor    int
	stopped   bool
	stats     SchedulerStats
	failures  []ScheduledError
}

func NewScheduler(transport *Transport) (*Scheduler, error) {
	if transport == nil {
		return nil, ErrResourceLimit
	}
	return &Scheduler{transport: transport, nodes: make(map[raft.NodeID]*Node)}, nil
}

func (s *Scheduler) AddNode(node *Node) error {
	if node == nil {
		return ErrInvalidCatalog
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrSchedulerStopped
	}
	if _, exists := s.nodes[node.id]; exists {
		return fmt.Errorf("%w: %d", ErrDuplicateNode, node.id)
	}
	s.nodes[node.id] = node
	s.rebuildLocked()
	return nil
}

func (s *Scheduler) RemoveNode(nodeID raft.NodeID) {
	s.mu.Lock()
	delete(s.nodes, nodeID)
	s.rebuildLocked()
	s.mu.Unlock()
}

func (s *Scheduler) Refresh() {
	s.mu.Lock()
	s.rebuildLocked()
	s.mu.Unlock()
}

func (s *Scheduler) rebuildLocked() {
	s.groups = s.groups[:0]
	for nodeID, node := range s.nodes {
		for _, rangeID := range node.registry.IDs() {
			s.groups = append(s.groups, groupKey{nodeID: nodeID, rangeID: rangeID})
		}
	}
	sort.Slice(s.groups, func(left, right int) bool {
		if s.groups[left].rangeID != s.groups[right].rangeID {
			return s.groups[left].rangeID < s.groups[right].rangeID
		}
		return s.groups[left].nodeID < s.groups[right].nodeID
	})
	if len(s.groups) == 0 {
		s.cursor = 0
	} else {
		s.cursor %= len(s.groups)
	}
}

func (s *Scheduler) TickNext() (bool, error) {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false, ErrSchedulerStopped
	}
	if len(s.groups) == 0 {
		s.mu.Unlock()
		return false, nil
	}
	key := s.groups[s.cursor]
	s.cursor = (s.cursor + 1) % len(s.groups)
	node := s.nodes[key.nodeID]
	s.mu.Unlock()
	envelopes, err := node.Tick(key.rangeID)
	if err != nil {
		if errors.Is(err, ErrUnknownRange) || errors.Is(err, ErrNodeStopped) {
			s.Refresh()
		}
		s.recordRangeError(key.nodeID, key.rangeID, err)
		return true, nil
	}
	if err := s.enqueue(envelopes); err != nil {
		s.recordError()
		return true, err
	}
	s.mu.Lock()
	s.stats.Ticks++
	s.mu.Unlock()
	return true, nil
}

func (s *Scheduler) DeliverNext() (bool, error) {
	envelope, exists := s.transport.Take()
	if !exists {
		return false, nil
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false, ErrSchedulerStopped
	}
	node := s.nodes[envelope.Message.To]
	s.mu.Unlock()
	if node == nil {
		s.recordRangeError(envelope.Message.To, envelope.RangeID, ErrNodeStopped)
		return true, nil
	}
	outbound, err := node.Step(envelope)
	if err != nil {
		s.recordRangeError(envelope.Message.To, envelope.RangeID, err)
		return true, nil
	}
	if err := s.enqueue(outbound); err != nil {
		s.recordError()
		return true, err
	}
	s.mu.Lock()
	s.stats.Deliveries++
	s.mu.Unlock()
	return true, nil
}

// Round gives one group a tick and then drains a bounded message quantum. The
// quantum prevents heartbeat production from outrunning the shared queue while
// still yielding after fixed work.
func (s *Scheduler) Round() error {
	_, tickErr := s.TickNext()
	result := tickErr
	for range 8 {
		delivered, deliveryErr := s.DeliverNext()
		result = errors.Join(result, deliveryErr)
		if !delivered {
			break
		}
	}
	return result
}

func (s *Scheduler) Run(rounds int) error {
	if rounds < 0 {
		return ErrResourceLimit
	}
	for range rounds {
		if err := s.Round(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) enqueue(envelopes []Envelope) error {
	for _, envelope := range envelopes {
		if err := s.transport.Send(envelope); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) recordError() {
	s.mu.Lock()
	s.stats.Errors++
	s.mu.Unlock()
}

func (s *Scheduler) recordRangeError(nodeID raft.NodeID, rangeID RangeID, err error) {
	s.mu.Lock()
	s.stats.Errors++
	if len(s.failures) == 128 {
		copy(s.failures, s.failures[1:])
		s.failures = s.failures[:127]
	}
	s.failures = append(s.failures, ScheduledError{NodeID: nodeID, RangeID: rangeID, Err: err})
	s.mu.Unlock()
}

func (s *Scheduler) Stats() SchedulerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *Scheduler) Failures() []ScheduledError {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ScheduledError(nil), s.failures...)
}

func (s *Scheduler) Nodes() map[raft.NodeID]*Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[raft.NodeID]*Node, len(s.nodes))
	for nodeID, node := range s.nodes {
		result[nodeID] = node
	}
	return result
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.groups = nil
	s.mu.Unlock()
}
