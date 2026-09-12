package multiraft

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"sync"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type SplitHookStage uint8

const (
	MetaSplitRecordDurable SplitHookStage = iota
	ParentSplitFenceActive
	TxnDrainComplete
	BootstrapBarrierDurable
	LeftImageDurable
	RightImageDurable
	ChildQuorumReady
	DeltaReplayProgress
	FinalFenceDurable
	ChildrenReplayedThroughFence
	MetaCutoverDurable
	ChildActivated
	ParentRetired
)

func (s SplitHookStage) String() string {
	names := [...]string{"META_SPLIT_RECORD_DURABLE", "PARENT_SPLIT_FENCE_ACTIVE", "TXN_DRAIN_COMPLETE", "BOOTSTRAP_BARRIER_DURABLE", "LEFT_IMAGE_DURABLE", "RIGHT_IMAGE_DURABLE", "CHILD_QUORUM_READY", "DELTA_REPLAY_PROGRESS", "FINAL_FENCE_DURABLE", "CHILDREN_REPLAYED_THROUGH_F", "META_CUTOVER_DURABLE", "CHILD_ACTIVATED", "PARENT_RETIRED"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("SPLIT_STAGE_%d", s)
}

type SplitHook func(SplitHookStage, SplitRecord)

type metaMachine struct {
	state *metadataState
	hook  SplitHook
}

func (m *metaMachine) Apply(entry raft.Entry) error {
	next, err := decodeMetadata(entry.Command)
	if err != nil {
		return fmt.Errorf("decode committed metadata: %w", err)
	}
	if err := validateMetadataTransition(m.state, next); err != nil {
		return err
	}
	previous := m.state
	m.state = next
	if m.hook != nil {
		for id, record := range next.splits {
			old, existed := previous.splits[id]
			if !existed {
				m.hook(MetaSplitRecordDurable, cloneSplit(record))
			} else if old.State != record.State && record.State == SplitCommitted {
				m.hook(MetaCutoverDurable, cloneSplit(record))
			}
		}
	}
	return nil
}

func (m *metaMachine) Snapshot() ([]byte, error) { return encodeMetadata(m.state) }
func (m *metaMachine) Restore(data []byte) error {
	state, err := decodeMetadata(data)
	if err != nil {
		return err
	}
	m.state = state
	return nil
}

func validateMetadataTransition(old, next *metadataState) error {
	if old == nil || next == nil || next.nextRangeID < old.nextRangeID || next.nextSplitID < old.nextSplitID ||
		len(next.splits) < len(old.splits) || len(next.lineage) < len(old.lineage) {
		return ErrCorruptMetadata
	}
	for parent, edge := range old.lineage {
		other, ok := next.lineage[parent]
		if !ok || edge.SplitID != other.SplitID || edge.Parent != other.Parent || edge.Children != other.Children ||
			edge.CutoverCatalogGeneration != other.CutoverCatalogGeneration || !bytes.Equal(edge.SplitKey, other.SplitKey) {
			return ErrCorruptMetadata
		}
	}
	changed := 0
	for id, before := range old.splits {
		after, ok := next.splits[id]
		if !ok {
			return ErrCorruptMetadata
		}
		if splitRecordsEqual(before, after) {
			continue
		}
		changed++
		if after.Epoch == before.Epoch+1 && after.State == before.State {
			continue
		}
		if after.Epoch != before.Epoch || !legalSplitTransition(before.State, after.State) {
			return ErrCorruptMetadata
		}
	}
	added := len(next.splits) - len(old.splits)
	if added > 1 || changed+added != 1 {
		return ErrCorruptMetadata
	}
	if added == 1 {
		if next.nextRangeID != old.nextRangeID+2 || next.nextSplitID != old.nextSplitID+1 ||
			next.catalog.Generation() != old.catalog.Generation() || len(next.lineage) != len(old.lineage) {
			return ErrCorruptMetadata
		}
		return nil
	}
	if next.nextRangeID != old.nextRangeID || next.nextSplitID != old.nextSplitID {
		return ErrCorruptMetadata
	}
	if next.catalog.Generation() == old.catalog.Generation()+1 {
		if len(next.lineage) != len(old.lineage)+1 {
			return ErrCorruptMetadata
		}
	} else if next.catalog.Fingerprint() != old.catalog.Fingerprint() || len(next.lineage) != len(old.lineage) {
		return ErrCorruptMetadata
	}
	return next.validateLineage()
}

func splitRecordsEqual(a, b SplitRecord) bool {
	return a.SplitID == b.SplitID && a.Parent == b.Parent && bytes.Equal(a.SplitKey, b.SplitKey) &&
		sameDescriptor(a.Left, b.Left) && sameDescriptor(a.Right, b.Right) && a.Epoch == b.Epoch && a.State == b.State &&
		a.BootstrapIndex == b.BootstrapIndex && a.FenceIndex == b.FenceIndex && a.LeftReplayThrough == b.LeftReplayThrough &&
		a.RightReplayThrough == b.RightReplayThrough && a.ImageDigest == b.ImageDigest && a.CutoverCatalogGeneration == b.CutoverCatalogGeneration && a.LastError == b.LastError
}

type MetaRangeOptions struct {
	Nodes     []raft.NodeID
	Directory string
	Bootstrap *Catalog
	Hook      SplitHook
	MaxWork   int
}

// MetaRange is a statically located independent Raft group. Its methods drive
// deterministic message delivery synchronously; it owns no timers or goroutine.
type MetaRange struct {
	mu       sync.Mutex
	nodes    map[raft.NodeID]*raft.Node
	machines map[raft.NodeID]*metaMachine
	queue    []raft.Message
	peers    []raft.NodeID
	maxWork  int
	stopped  map[raft.NodeID]bool
}

func OpenMetaRange(options MetaRangeOptions) (*MetaRange, error) {
	if len(options.Nodes) == 0 || options.Directory == "" || options.Bootstrap == nil {
		return nil, ErrInvalidCatalog
	}
	if options.MaxWork == 0 {
		options.MaxWork = 100_000
	}
	peers := append([]raft.NodeID(nil), options.Nodes...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	for index, id := range peers {
		if id == 0 || index > 0 && id == peers[index-1] {
			return nil, ErrInvalidCatalog
		}
	}
	group := &MetaRange{nodes: make(map[raft.NodeID]*raft.Node), machines: make(map[raft.NodeID]*metaMachine), peers: peers, maxWork: options.MaxWork, stopped: make(map[raft.NodeID]bool)}
	for _, id := range peers {
		initial, err := newMetadataState(options.Bootstrap)
		if err != nil {
			return nil, err
		}
		machine := &metaMachine{state: initial, hook: options.Hook}
		store, err := raft.OpenFileStore(filepath.Join(options.Directory, fmt.Sprint(uint64(id)), "meta", "raft"))
		if err != nil {
			return nil, fmt.Errorf("open MetaRange store on node %d: %w", id, err)
		}
		node, err := raft.NewNode(raft.Config{ID: id, Peers: peers, ElectionTimeoutMin: 5 + uint64(id%3), ElectionTimeoutMax: 5 + uint64(id%3), HeartbeatInterval: 1,
			Random: rand.New(rand.NewPCG(uint64(id), uint64(MetaRangeID))), Store: store, StateMachine: machine}) //nolint:gosec // deterministic election input
		if err != nil {
			return nil, fmt.Errorf("open MetaRange replica on node %d: %w", id, err)
		}
		group.nodes[id], group.machines[id] = node, machine
	}
	return group, nil
}

func (m *MetaRange) leaderLocked() (raft.NodeID, bool) {
	for _, id := range m.peers {
		if m.stopped[id] {
			continue
		}
		if m.nodes[id].Status().Role == raft.Leader {
			return id, true
		}
	}
	return 0, false
}

func (m *MetaRange) driveLocked(until func() bool) error {
	for work := 0; work < m.maxWork; work++ {
		if until() {
			return nil
		}
		if len(m.queue) != 0 {
			message := m.queue[0]
			m.queue = m.queue[1:]
			if m.stopped[message.To] {
				continue
			}
			out, err := m.nodes[message.To].Step(message)
			if err != nil {
				return fmt.Errorf("deliver MetaRange message: %w", err)
			}
			m.queue = append(m.queue, out...)
			continue
		}
		for _, id := range m.peers {
			if m.stopped[id] {
				continue
			}
			out, err := m.nodes[id].Tick()
			if err != nil {
				return fmt.Errorf("tick MetaRange replica: %w", err)
			}
			m.queue = append(m.queue, out...)
		}
	}
	return ErrLeaderUnknown
}

func (m *MetaRange) mutate(ctx context.Context, change func(*metadataState) error) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.driveLocked(func() bool { _, ok := m.leaderLocked(); return ok }); err != nil {
		return err
	}
	leader, _ := m.leaderLocked()
	next := cloneMetadata(m.machines[leader].state)
	if err := change(next); err != nil {
		return err
	}
	encoded, err := encodeMetadata(next)
	if err != nil {
		return err
	}
	current, err := encodeMetadata(m.machines[leader].state)
	if err != nil {
		return err
	}
	if bytes.Equal(current, encoded) {
		return nil
	}
	index, out, err := m.nodes[leader].Propose(encoded)
	if err != nil {
		return fmt.Errorf("propose MetaRange transition: %w", err)
	}
	m.queue = append(m.queue, out...)
	if err := m.driveLocked(func() bool {
		return m.nodes[leader].Status().LastApplied >= index || m.nodes[leader].Status().Role != raft.Leader
	}); err != nil {
		return err
	}
	if m.nodes[leader].Status().LastApplied < index {
		return raft.ErrNotLeader
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("complete metadata transition: %w", err)
	}
	return nil
}

func (m *MetaRange) Snapshot() MetadataSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	if leader, ok := m.leaderLocked(); ok {
		return m.machines[leader].state.snapshot()
	}
	return m.machines[m.peers[0]].state.snapshot()
}

// Sync elects a metadata leader and causes retained committed history to be
// learned and applied after a full process restart.
func (m *MetaRange) Sync(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.driveLocked(func() bool {
		leader, ok := m.leaderLocked()
		return ok && m.nodes[leader].Status().LastApplied == m.nodes[leader].Status().LastIndex
	}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("synchronize metadata range: %w", err)
	}
	return nil
}

func (m *MetaRange) BeginSplit(ctx context.Context, parent RangeRef, key []byte, catalogGeneration uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(state *metadataState) error {
		var err error
		result, err = state.begin(parent, key, catalogGeneration)
		return err
	})
	return result, err
}

func (m *MetaRange) AdvanceSplit(ctx context.Context, id SplitID, epoch uint64, state SplitState, evidence SplitRecord) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var err error
		result, err = meta.advance(id, epoch, state, evidence)
		return err
	})
	return result, err
}

func (m *MetaRange) TakeoverSplit(ctx context.Context, id SplitID, epoch uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error { var err error; result, err = meta.takeover(id, epoch); return err })
	return result, err
}

func (m *MetaRange) AbortSplit(ctx context.Context, id SplitID, epoch uint64, reason string) (SplitRecord, error) {
	return m.AdvanceSplit(ctx, id, epoch, SplitAborted, SplitRecord{LastError: reason})
}

func (m *MetaRange) CommitSplit(ctx context.Context, id SplitID, epoch, catalogGeneration uint64) (SplitRecord, error) {
	var result SplitRecord
	err := m.mutate(ctx, func(meta *metadataState) error {
		var err error
		result, err = meta.commit(id, epoch, catalogGeneration)
		return err
	})
	return result, err
}

func (m *MetaRange) ResolveRangeRef(ref RangeRef) ([]RangeDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader, ok := m.leaderLocked()
	if !ok {
		leader = m.peers[0]
	}
	return m.machines[leader].state.resolve(ref)
}

func (m *MetaRange) StopNode(id raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node := m.nodes[id]
	if node == nil {
		return ErrUnknownRange
	}
	node.Stop()
	m.stopped[id] = true
	return nil
}

func (m *MetaRange) Leader() raft.NodeID {
	m.mu.Lock()
	defer m.mu.Unlock()
	leader, _ := m.leaderLocked()
	return leader
}

var _ raft.StateMachine = (*metaMachine)(nil)
