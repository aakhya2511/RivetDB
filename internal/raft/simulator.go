package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

const maxTraceEntries = 512

type MachineFactory func(NodeID) StateMachine

type SimulatorConfig struct {
	NodeIDs            []NodeID
	ElectionTimeoutMin uint64
	ElectionTimeoutMax uint64
	HeartbeatInterval  uint64
	TickDuration       time.Duration
	Seed               uint64
	Clock              clock.Clock
	NewStateMachine    MachineFactory
}

type Envelope struct {
	ID      uint64
	Message Message
}

type TraceEvent struct {
	Sequence    uint64
	LogicalTime time.Time
	Action      string
	Node        NodeID
	MessageID   uint64
	Status      Status
}

type directedLink struct{ from, to NodeID }

type committedRecord struct {
	entry      Entry
	commitTerm uint64
}

type leaderExecution struct {
	node NodeID
	term uint64
}

type executionProgress struct {
	generation uint64
	commit     uint64
	applied    uint64
}

// Simulator is a deterministic, explicitly scheduled in-memory Raft network.
// Pending envelope selection controls delay and reordering; Drop and Duplicate
// make loss and duplication explicit; links are directed for asymmetric faults.
type Simulator struct {
	config        SimulatorConfig
	ids           []NodeID
	stores        map[NodeID]*MemoryStore
	nodes         map[NodeID]*Node
	machines      map[NodeID]StateMachine
	restarts      map[NodeID]uint64
	links         map[directedLink]bool
	pending       []Envelope
	nextMessageID uint64
	nextTraceID   uint64
	lastClock     time.Time
	leaders       map[uint64]NodeID
	leaderLogs    map[leaderExecution]PersistentState
	terms         map[NodeID]uint64
	progress      map[NodeID]executionProgress
	votes         map[NodeID]map[uint64]NodeID
	committed     map[uint64]committedRecord
	applied       map[uint64]Entry
	trace         []TraceEvent
}

func NewSimulator(config SimulatorConfig) (*Simulator, error) {
	if len(config.NodeIDs) == 0 || config.Clock == nil || config.NewStateMachine == nil || config.TickDuration <= 0 {
		return nil, ErrInvalidConfig
	}
	ids := slices.Clone(config.NodeIDs)
	slices.Sort(ids)
	if ids[0] == 0 {
		return nil, ErrInvalidConfig
	}
	for index := 1; index < len(ids); index++ {
		if ids[index] == ids[index-1] {
			return nil, ErrInvalidConfig
		}
	}
	s := &Simulator{
		config: config, ids: ids, stores: make(map[NodeID]*MemoryStore), nodes: make(map[NodeID]*Node),
		machines: make(map[NodeID]StateMachine), restarts: make(map[NodeID]uint64), links: make(map[directedLink]bool),
		lastClock: config.Clock.Now(), leaders: make(map[uint64]NodeID), leaderLogs: make(map[leaderExecution]PersistentState), terms: make(map[NodeID]uint64), progress: make(map[NodeID]executionProgress),
		votes: make(map[NodeID]map[uint64]NodeID), committed: make(map[uint64]committedRecord), applied: make(map[uint64]Entry),
	}
	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				s.links[directedLink{from: from, to: to}] = true
			}
		}
		s.stores[from] = NewMemoryStore()
		if err := s.restart(from); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Simulator) Node(id NodeID) *Node { return s.nodes[id] }

func (s *Simulator) StateMachine(id NodeID) StateMachine { return s.machines[id] }

func (s *Simulator) Pending() []Envelope {
	result := make([]Envelope, len(s.pending))
	for index, envelope := range s.pending {
		result[index] = Envelope{ID: envelope.ID, Message: cloneMessage(envelope.Message)}
	}
	return result
}

func (s *Simulator) Trace() []TraceEvent { return slices.Clone(s.trace) }

func (s *Simulator) AdvanceClock() error {
	now := s.config.Clock.Now()
	elapsed := now.Sub(s.lastClock)
	if elapsed < 0 || elapsed%s.config.TickDuration != 0 {
		return ErrInvalidConfig
	}
	ticks := int(elapsed / s.config.TickDuration)
	for range ticks {
		for _, id := range s.ids {
			if s.nodes[id] == nil {
				continue
			}
			messages, err := s.nodes[id].Tick()
			if err != nil {
				return fmt.Errorf("tick node %d: %w", id, err)
			}
			s.enqueue(messages)
			s.record("tick", id, 0)
		}
	}
	s.lastClock = now
	return s.CheckSafety()
}

func (s *Simulator) Tick(id NodeID) error {
	node := s.nodes[id]
	if node == nil {
		return ErrStopped
	}
	messages, err := node.Tick()
	if err != nil {
		return err
	}
	s.enqueue(messages)
	s.record("tick", id, 0)
	return s.CheckSafety()
}

func (s *Simulator) Propose(id NodeID, command []byte) (uint64, error) {
	node := s.nodes[id]
	if node == nil {
		return 0, ErrStopped
	}
	index, messages, err := node.Propose(command)
	if err != nil {
		return 0, err
	}
	s.enqueue(messages)
	s.record("propose", id, 0)
	return index, s.CheckSafety()
}

// ProposeAndApply is the simulator's client-level completion boundary: it
// succeeds only after the proposal is committed and applied on its leader.
func (s *Simulator) ProposeAndApply(id NodeID, command []byte, deliveryLimit int) (uint64, error) {
	index, err := s.Propose(id, command)
	if err != nil {
		return 0, err
	}
	if _, err := s.DeliverAll(deliveryLimit); err != nil {
		return 0, err
	}
	node := s.nodes[id]
	if node == nil || node.Status().LastApplied < index {
		return index, ErrNotCommitted
	}
	return index, nil
}

// Send implements Transport by placing a cloned message in the deterministic
// pending queue. Delivery remains explicitly controlled by the caller.
func (s *Simulator) Send(message Message) error {
	if message.From == 0 || message.To == 0 || message.From == message.To || !slices.Contains(s.ids, message.From) || !slices.Contains(s.ids, message.To) {
		return ErrInvalidMessage
	}
	s.enqueue([]Message{message})
	return nil
}

func (s *Simulator) Deliver(messageID uint64) error {
	position := s.pendingPosition(messageID)
	if position < 0 {
		return ErrUnavailable
	}
	envelope := s.pending[position]
	if !s.links[directedLink{from: envelope.Message.From, to: envelope.Message.To}] {
		return ErrUnavailable
	}
	s.pending = slices.Delete(s.pending, position, position+1)
	node := s.nodes[envelope.Message.To]
	if node == nil {
		s.record("deliver-to-stopped", envelope.Message.To, envelope.ID)
		return nil
	}
	messages, err := node.Step(cloneMessage(envelope.Message))
	if err != nil {
		return fmt.Errorf("deliver message %d: %w", messageID, err)
	}
	s.enqueue(messages)
	s.record("deliver", envelope.Message.To, envelope.ID)
	return s.CheckSafety()
}

func (s *Simulator) DeliverAll(limit int) (int, error) {
	delivered := 0
	for delivered < limit {
		var selected uint64
		for _, envelope := range s.pending {
			if s.links[directedLink{from: envelope.Message.From, to: envelope.Message.To}] {
				selected = envelope.ID
				break
			}
		}
		if selected == 0 {
			return delivered, nil
		}
		if err := s.Deliver(selected); err != nil {
			return delivered, err
		}
		delivered++
	}
	return delivered, nil
}

func (s *Simulator) Drop(messageID uint64) error {
	position := s.pendingPosition(messageID)
	if position < 0 {
		return ErrUnavailable
	}
	node := s.pending[position].Message.To
	s.pending = slices.Delete(s.pending, position, position+1)
	s.record("drop", node, messageID)
	return nil
}

func (s *Simulator) Duplicate(messageID uint64) (uint64, error) {
	position := s.pendingPosition(messageID)
	if position < 0 {
		return 0, ErrUnavailable
	}
	message := cloneMessage(s.pending[position].Message)
	s.enqueue([]Message{message})
	s.record("duplicate", message.To, messageID)
	return s.nextMessageID, nil
}

func (s *Simulator) SetLink(from, to NodeID, enabled bool) error {
	link := directedLink{from: from, to: to}
	if _, ok := s.links[link]; !ok {
		return ErrInvalidConfig
	}
	s.links[link] = enabled
	s.record("link", to, 0)
	return nil
}

func (s *Simulator) Partition(left, right []NodeID) error {
	for _, from := range left {
		for _, to := range right {
			if err := s.SetLink(from, to, false); err != nil {
				return err
			}
			if err := s.SetLink(to, from, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Simulator) Heal() {
	for link := range s.links {
		s.links[link] = true
	}
	s.record("heal", 0, 0)
}

func (s *Simulator) Crash(id NodeID) error {
	if s.nodes[id] == nil {
		return ErrStopped
	}
	s.nodes[id].Stop()
	s.nodes[id] = nil
	s.machines[id] = nil
	s.record("crash", id, 0)
	return nil
}

func (s *Simulator) Restart(id NodeID) error {
	if s.nodes[id] != nil {
		return ErrInvalidConfig
	}
	if err := s.restart(id); err != nil {
		return err
	}
	s.record("restart", id, 0)
	return s.CheckSafety()
}

func (s *Simulator) restart(id NodeID) error {
	store := s.stores[id]
	if store == nil {
		return ErrInvalidConfig
	}
	machine := s.config.NewStateMachine(id)
	audited := &auditedStateMachine{simulator: s, inner: machine, applied: make(map[uint64]struct{})}
	s.restarts[id]++
	seed := s.config.Seed ^ uint64(id)*0x9e3779b97f4a7c15 ^ s.restarts[id]*0xbf58476d1ce4e5b9
	config := Config{
		ID: id, Peers: s.ids, ElectionTimeoutMin: s.config.ElectionTimeoutMin,
		ElectionTimeoutMax: s.config.ElectionTimeoutMax, HeartbeatInterval: s.config.HeartbeatInterval,
		Random: rand.New(rand.NewPCG(seed, seed^0x94d049bb133111eb)), Store: store, StateMachine: audited, //nolint:gosec // deterministic simulator randomness is required for replay
	}
	node, err := NewNode(config)
	if err != nil {
		return err
	}
	s.nodes[id], s.machines[id] = node, machine
	return nil
}

type auditedStateMachine struct {
	simulator *Simulator
	inner     StateMachine
	applied   map[uint64]struct{}
	lastIndex uint64
}

func (m *auditedStateMachine) Apply(entry Entry) error {
	if _, duplicate := m.applied[entry.Index]; duplicate {
		return fmt.Errorf("%w: RAFT-13 duplicate apply at index %d", ErrInvariantCheck, entry.Index)
	}
	if entry.Index <= m.lastIndex {
		return fmt.Errorf("%w: RAFT-13 apply index %d after %d", ErrInvariantCheck, entry.Index, m.lastIndex)
	}
	if prior, ok := m.simulator.applied[entry.Index]; ok && !sameEntry(prior, entry) {
		return fmt.Errorf("%w: RAFT-5 applied index %d differs", ErrInvariantCheck, entry.Index)
	}
	if err := m.inner.Apply(cloneEntry(entry)); err != nil {
		return fmt.Errorf("apply audited state-machine entry %d: %w", entry.Index, err)
	}
	m.applied[entry.Index] = struct{}{}
	m.lastIndex = entry.Index
	m.simulator.applied[entry.Index] = cloneEntry(entry)
	return nil
}

func (m *auditedStateMachine) Snapshot() ([]byte, error) {
	snapshot, err := m.inner.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("snapshot audited state machine: %w", err)
	}
	return bytes.Clone(snapshot), nil
}

func (m *auditedStateMachine) Restore(snapshot []byte) error {
	if err := m.inner.Restore(bytes.Clone(snapshot)); err != nil {
		return fmt.Errorf("restore audited state machine: %w", err)
	}
	return nil
}

func (s *Simulator) CheckSafety() error {
	for _, id := range s.ids {
		node := s.nodes[id]
		if node == nil {
			continue
		}
		status := node.Status()
		if status.Term < s.terms[id] {
			return fmt.Errorf("%w: RAFT-6 node %d term regressed %d -> %d", ErrInvariantCheck, id, s.terms[id], status.Term)
		}
		s.terms[id] = status.Term
		if status.VotedFor != 0 {
			if s.votes[id] == nil {
				s.votes[id] = make(map[uint64]NodeID)
			}
			if prior := s.votes[id][status.Term]; prior != 0 && prior != status.VotedFor {
				return fmt.Errorf("%w: RAFT-8 node %d voted for %d and %d in term %d", ErrInvariantCheck, id, prior, status.VotedFor, status.Term)
			}
			s.votes[id][status.Term] = status.VotedFor
		}
		if status.LastApplied > status.CommitIndex || status.CommitIndex > status.LastIndex {
			return fmt.Errorf("%w: RAFT-7 node %d applied=%d commit=%d last=%d", ErrInvariantCheck, id, status.LastApplied, status.CommitIndex, status.LastIndex)
		}
		progress := s.progress[id]
		if progress.generation == s.restarts[id] && (status.CommitIndex < progress.commit || status.LastApplied < progress.applied) {
			return fmt.Errorf("%w: RAFT-7/13 node %d progress regressed commit %d->%d applied %d->%d", ErrInvariantCheck, id, progress.commit, status.CommitIndex, progress.applied, status.LastApplied)
		}
		s.progress[id] = executionProgress{generation: s.restarts[id], commit: status.CommitIndex, applied: status.LastApplied}
		if status.Snapshot.Index > status.LastApplied {
			return fmt.Errorf("%w: RAFT-11 node %d snapshot=%d applied=%d", ErrInvariantCheck, id, status.Snapshot.Index, status.LastApplied)
		}
		if status.Role == Leader {
			if prior, ok := s.leaders[status.Term]; ok && prior != id {
				return fmt.Errorf("%w: RAFT-1 term %d leaders %d and %d", ErrInvariantCheck, status.Term, prior, id)
			}
			s.leaders[status.Term] = id
			execution := leaderExecution{node: id, term: status.Term}
			if prior, ok := s.leaderLogs[execution]; ok {
				if err := leaderLogExtends(prior, node.persistentCopy(), status.CommitIndex); err != nil {
					return fmt.Errorf("leader %d term %d: %w", id, status.Term, err)
				}
			}
			s.leaderLogs[execution] = node.persistentCopy()
			for index, committed := range s.committed {
				if status.Term < committed.commitTerm || index <= status.Snapshot.Index {
					continue
				}
				entry, err := node.entryAt(index)
				if err != nil || !sameEntry(entry, committed.entry) {
					return fmt.Errorf("%w: RAFT-4 leader %d term %d lacks committed index %d", ErrInvariantCheck, id, status.Term, index)
				}
			}
		}
		for index := status.Snapshot.Index + 1; index <= status.CommitIndex; index++ {
			entry, err := node.entryAt(index)
			if err != nil {
				return fmt.Errorf("RAFT-7 node %d committed index %d unavailable: %w", id, index, err)
			}
			if prior, ok := s.committed[index]; ok && !sameEntry(prior.entry, entry) {
				return fmt.Errorf("%w: RAFT-5 committed index %d differs", ErrInvariantCheck, index)
			}
			if _, ok := s.committed[index]; !ok {
				s.committed[index] = committedRecord{entry: entry, commitTerm: status.Term}
			}
		}
	}
	for leftIndex, leftID := range s.ids {
		left := s.nodes[leftID]
		if left == nil {
			continue
		}
		for _, rightID := range s.ids[leftIndex+1:] {
			right := s.nodes[rightID]
			if right == nil {
				continue
			}
			if err := compareLogs(left, right); err != nil {
				return fmt.Errorf("nodes %d/%d: %w", leftID, rightID, err)
			}
		}
	}
	return nil
}

func leaderLogExtends(prior, current PersistentState, commitIndex uint64) error {
	priorLast := prior.Snapshot.Index + uint64(len(prior.Entries))       //nolint:gosec // in-memory slice is bounded
	currentLast := current.Snapshot.Index + uint64(len(current.Entries)) //nolint:gosec // in-memory slice is bounded
	if current.Snapshot.Index < prior.Snapshot.Index || currentLast < priorLast {
		return fmt.Errorf("%w: RAFT-2 log regressed from snapshot/last %d/%d to %d/%d", ErrInvariantCheck, prior.Snapshot.Index, priorLast, current.Snapshot.Index, currentLast)
	}
	if current.Snapshot.Index > prior.Snapshot.Index && current.Snapshot.Index > commitIndex {
		return fmt.Errorf("%w: RAFT-11 compacted through %d beyond commit %d", ErrInvariantCheck, current.Snapshot.Index, commitIndex)
	}
	for _, entry := range prior.Entries {
		if entry.Index <= current.Snapshot.Index {
			continue
		}
		offset := entry.Index - current.Snapshot.Index - 1
		if offset >= uint64(len(current.Entries)) || !sameEntry(entry, current.Entries[offset]) { //nolint:gosec // offset checked before indexing
			return fmt.Errorf("%w: RAFT-2 prior entry %d changed", ErrInvariantCheck, entry.Index)
		}
	}
	return nil
}

func compareLogs(left, right *Node) error {
	start := max(left.persistent.Snapshot.Index, right.persistent.Snapshot.Index) + 1
	end := min(left.lastIndex(), right.lastIndex())
	for index := start; index <= end; index++ {
		leftEntry, leftErr := left.entryAt(index)
		rightEntry, rightErr := right.entryAt(index)
		if leftErr != nil || rightErr != nil {
			return errors.Join(leftErr, rightErr)
		}
		if leftEntry.Term == rightEntry.Term {
			for prefix := start; prefix <= index; prefix++ {
				leftPrefix, leftPrefixErr := left.entryAt(prefix)
				rightPrefix, rightPrefixErr := right.entryAt(prefix)
				if leftPrefixErr != nil || rightPrefixErr != nil {
					return errors.Join(leftPrefixErr, rightPrefixErr)
				}
				if !sameEntry(leftPrefix, rightPrefix) {
					return fmt.Errorf("%w: RAFT-3 matching index %d term %d has conflicting prefix %d", ErrInvariantCheck, index, leftEntry.Term, prefix)
				}
			}
		}
	}
	return nil
}

func sameEntry(left, right Entry) bool {
	return left.Index == right.Index && left.Term == right.Term && left.Type == right.Type && slices.Equal(left.Command, right.Command)
}

func (s *Simulator) enqueue(messages []Message) {
	for _, message := range messages {
		s.nextMessageID++
		s.pending = append(s.pending, Envelope{ID: s.nextMessageID, Message: cloneMessage(message)})
	}
}

func cloneMessage(message Message) Message {
	message.Entries = cloneEntries(message.Entries)
	message.Snapshot = cloneSnapshot(message.Snapshot)
	return message
}

func (s *Simulator) pendingPosition(id uint64) int {
	for index := range s.pending {
		if s.pending[index].ID == id {
			return index
		}
	}
	return -1
}

func (s *Simulator) record(action string, node NodeID, messageID uint64) {
	s.nextTraceID++
	status := Status{ID: node}
	if current := s.nodes[node]; current != nil {
		status = current.Status()
	}
	s.trace = append(s.trace, TraceEvent{Sequence: s.nextTraceID, LogicalTime: s.config.Clock.Now(), Action: action, Node: node, MessageID: messageID, Status: status})
	if len(s.trace) > maxTraceEntries {
		s.trace = slices.Delete(s.trace, 0, len(s.trace)-maxTraceEntries)
	}
}
