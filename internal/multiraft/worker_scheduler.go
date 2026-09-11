package multiraft

import (
	"errors"
	"fmt"
	"sync"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type workHandler interface {
	Tick(RangeID) ([]Envelope, error)
	Step(Envelope) ([]Envelope, error)
}

type workItem struct {
	envelope *Envelope
	done     chan error
}

type workGroup struct {
	nodeID  raft.NodeID
	rangeID RangeID
}

type WorkerSchedulerStats struct {
	Completed uint64
	Failed    uint64
}

// WorkerScheduler runs a fixed node-level pool. Per-group FIFO queues and at
// most one active item per group prevent a hot range from occupying every
// worker by blocking them all on the same Replica lock.
type WorkerScheduler struct {
	mu        sync.Mutex
	condition *sync.Cond
	transport *Transport
	handlers  map[raft.NodeID]workHandler
	queues    map[workGroup][]workItem
	active    map[workGroup]bool
	ready     []workGroup
	pending   int
	maximum   int
	workers   int
	stopped   bool
	stats     WorkerSchedulerStats
	wait      sync.WaitGroup
}

func NewWorkerScheduler(transport *Transport, workers, maximumPending int) (*WorkerScheduler, error) {
	if transport == nil || workers < 1 || maximumPending < 1 {
		return nil, ErrResourceLimit
	}
	scheduler := &WorkerScheduler{transport: transport, handlers: make(map[raft.NodeID]workHandler),
		queues: make(map[workGroup][]workItem), active: make(map[workGroup]bool), maximum: maximumPending, workers: workers}
	scheduler.condition = sync.NewCond(&scheduler.mu)
	scheduler.wait.Add(workers)
	for range workers {
		go scheduler.worker()
	}
	return scheduler, nil
}

func (s *WorkerScheduler) AddNode(node *Node) error {
	if node == nil {
		return ErrInvalidCatalog
	}
	return s.addHandler(node.ID(), node)
}

func (s *WorkerScheduler) addHandler(nodeID raft.NodeID, handler workHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrSchedulerStopped
	}
	if nodeID == 0 || handler == nil {
		return ErrInvalidCatalog
	}
	if _, exists := s.handlers[nodeID]; exists {
		return ErrDuplicateNode
	}
	s.handlers[nodeID] = handler
	return nil
}

func (s *WorkerScheduler) SubmitTick(nodeID raft.NodeID, rangeID RangeID) (<-chan error, error) {
	return s.submit(workGroup{nodeID: nodeID, rangeID: rangeID}, workItem{done: make(chan error, 1)})
}

func (s *WorkerScheduler) SubmitEnvelope(envelope Envelope) (<-chan error, error) {
	if err := validateEnvelope(envelope); err != nil {
		return nil, err
	}
	owned := cloneEnvelope(envelope)
	return s.submit(workGroup{nodeID: envelope.Message.To, rangeID: envelope.RangeID}, workItem{envelope: &owned, done: make(chan error, 1)})
}

func (s *WorkerScheduler) submit(group workGroup, item workItem) (<-chan error, error) {
	if group.nodeID == 0 || group.rangeID == 0 {
		return nil, ErrInvalidDescriptor
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return nil, ErrSchedulerStopped
	}
	if s.handlers[group.nodeID] == nil {
		return nil, ErrNodeStopped
	}
	if s.pending >= s.maximum {
		return nil, ErrResourceLimit
	}
	if len(s.queues[group]) == 0 && !s.active[group] {
		s.ready = append(s.ready, group)
	}
	s.queues[group] = append(s.queues[group], item)
	s.pending++
	s.condition.Signal()
	return item.done, nil
}

func (s *WorkerScheduler) worker() {
	defer s.wait.Done()
	for {
		group, item, handler, ok := s.take()
		if !ok {
			return
		}
		var outbound []Envelope
		var err error
		if item.envelope == nil {
			outbound, err = handler.Tick(group.rangeID)
		} else {
			outbound, err = handler.Step(*item.envelope)
		}
		if err == nil {
			for _, envelope := range outbound {
				if sendErr := s.transport.Send(envelope); sendErr != nil {
					err = errors.Join(err, fmt.Errorf("send scheduled range %d message: %w", group.rangeID, sendErr))
					break
				}
			}
		}
		item.done <- err
		close(item.done)
		s.finish(group, err)
	}
}

func (s *WorkerScheduler) take() (workGroup, workItem, workHandler, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.ready) == 0 {
		if s.stopped && s.pending == 0 {
			return workGroup{}, workItem{}, nil, false
		}
		s.condition.Wait()
	}
	group := s.ready[0]
	copy(s.ready, s.ready[1:])
	s.ready = s.ready[:len(s.ready)-1]
	queue := s.queues[group]
	item := queue[0]
	s.queues[group] = queue[1:]
	s.active[group] = true
	return group, item, s.handlers[group.nodeID], true
}

func (s *WorkerScheduler) finish(group workGroup, err error) {
	s.mu.Lock()
	s.active[group] = false
	s.pending--
	if len(s.queues[group]) != 0 {
		s.ready = append(s.ready, group)
		s.condition.Signal()
	} else {
		delete(s.queues, group)
		delete(s.active, group)
	}
	if err == nil {
		s.stats.Completed++
	} else {
		s.stats.Failed++
	}
	if s.stopped && s.pending == 0 {
		s.condition.Broadcast()
	}
	s.mu.Unlock()
}

func (s *WorkerScheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

func (s *WorkerScheduler) WorkerCount() int { return s.workers }

func (s *WorkerScheduler) Stats() WorkerSchedulerStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *WorkerScheduler) Close() {
	s.mu.Lock()
	s.stopped = true
	s.condition.Broadcast()
	s.mu.Unlock()
	s.wait.Wait()
}
