package multiraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

const bootstrapCompleteFilename = "bootstrap-complete"

type NodeOptions struct {
	NodeID               raft.NodeID
	Directory            string
	Bootstrap            *Bootstrap
	MaxHostedRanges      int
	MaxProposalWaiters   int
	MemTableBytes        uint64
	CatalogHook          CatalogPublishHook
	RangeHook            func(RangeID) replicatedrange.Hook
	BootstrapRangeHook   func(RangeID) error
	MVCC                 bool
	Clock                clock.Clock
	AuthoritativeCatalog *Catalog
	ShadowRanges         []RangeDescriptor
	RetiredRanges        []RangeDescriptor
}

type RangeFailure struct {
	RangeID RangeID
	Err     error
}

type NodeStatus struct {
	NodeID      raft.NodeID
	Ranges      []replicatedrange.Status
	Descriptors []RangeDescriptor
	Failures    []RangeFailure
	Orphans     []string
}

type Node struct {
	mu        sync.RWMutex
	id        raft.NodeID
	directory string
	catalog   *Catalog
	registry  *RangeRegistry
	options   NodeOptions
	failures  map[RangeID]error
	dynamic   map[RangeID]RangeDescriptor
	retired   map[RangeID][]RangeDescriptor
	orphans   []string
	stopped   bool
}

func OpenNode(options NodeOptions) (_ *Node, resultErr error) {
	if options.NodeID == 0 || options.Directory == "" {
		return nil, ErrInvalidCatalog
	}
	if options.MaxHostedRanges == 0 {
		options.MaxHostedRanges = 1024
	}
	if options.MaxHostedRanges < 1 {
		return nil, ErrResourceLimit
	}
	catalog, err := LoadOrBootstrapCatalog(options.Directory, options.Bootstrap, options.CatalogHook)
	if err != nil {
		return nil, err
	}
	bootstrapCatalog := catalog
	if options.AuthoritativeCatalog != nil {
		if options.AuthoritativeCatalog.Generation() < catalog.Generation() {
			return nil, ErrStaleRange
		}
		catalog = options.AuthoritativeCatalog
	}
	assigned := catalog.Assigned(options.NodeID)
	if len(assigned) > options.MaxHostedRanges {
		return nil, ErrResourceLimit
	}
	node := &Node{id: options.NodeID, directory: options.Directory, catalog: catalog, registry: newRangeRegistry(),
		options: options, failures: make(map[RangeID]error), dynamic: make(map[RangeID]RangeDescriptor), retired: make(map[RangeID][]RangeDescriptor)}
	complete, completionErr := node.bootstrapComplete()
	if completionErr != nil {
		return nil, completionErr
	}
	for _, descriptor := range assigned {
		if complete && !rangeDirectoriesExist(options.Directory, descriptor.RangeID) {
			node.failures[descriptor.RangeID] = ErrMissingRange
			continue
		}
		lifecycle := replicatedrange.LifecycleActive
		if _, baseErr := bootstrapCatalog.LookupByID(descriptor.RangeID); baseErr != nil {
			lifecycle = replicatedrange.LifecycleShadow
		}
		replica, openErr := node.openReplicaLifecycle(descriptor, lifecycle)
		if openErr != nil {
			node.failures[descriptor.RangeID] = openErr
			continue
		}
		if registerErr := node.registry.Register(descriptor.RangeID, replica); registerErr != nil {
			closeErr := replica.Close(context.Background())
			node.failures[descriptor.RangeID] = errors.Join(registerErr, closeErr)
			continue
		}
		if !complete && options.BootstrapRangeHook != nil {
			if hookErr := options.BootstrapRangeHook(descriptor.RangeID); hookErr != nil {
				node.failures[descriptor.RangeID] = hookErr
			}
		}
	}
	for _, descriptor := range append(append([]RangeDescriptor(nil), options.ShadowRanges...), options.RetiredRanges...) {
		if _, assignedHere := descriptor.ReplicaOn(options.NodeID); !assignedHere {
			continue
		}
		if _, existsErr := node.registry.Lookup(descriptor.RangeID); existsErr == nil {
			continue
		}
		initial := replicatedrange.LifecycleShadow
		if containsDescriptor(options.RetiredRanges, descriptor.RangeID) {
			initial = replicatedrange.LifecycleActive
		}
		replica, openErr := node.openReplicaLifecycle(descriptor, initial)
		if openErr != nil {
			node.failures[descriptor.RangeID] = openErr
			continue
		}
		if registerErr := node.registry.Register(descriptor.RangeID, replica); registerErr != nil {
			node.failures[descriptor.RangeID] = errors.Join(registerErr, replica.Close(context.Background()))
			continue
		}
		node.dynamic[descriptor.RangeID] = cloneDescriptor(descriptor)
	}
	if !complete {
		if len(node.failures) != 0 {
			closeErr := node.Close(context.Background())
			return nil, errors.Join(ErrBootstrapIncomplete, failuresError(node.failures), closeErr)
		}
		if err := node.publishBootstrapComplete(); err != nil {
			closeErr := node.Close(context.Background())
			return nil, errors.Join(err, closeErr)
		}
	}
	node.orphans = findOrphans(options.Directory, assigned)
	return node, nil
}

func containsDescriptor(descriptors []RangeDescriptor, id RangeID) bool {
	for _, descriptor := range descriptors {
		if descriptor.RangeID == id {
			return true
		}
	}
	return false
}

func (n *Node) openReplica(descriptor RangeDescriptor) (*replicatedrange.Replica, error) {
	return n.openReplicaLifecycle(descriptor, replicatedrange.LifecycleActive)
}

func (n *Node) openReplicaLifecycle(descriptor RangeDescriptor, lifecycle replicatedrange.Lifecycle) (*replicatedrange.Replica, error) {
	local, exists := descriptor.ReplicaOn(n.id)
	if !exists {
		return nil, ErrUnknownRange
	}
	peers := make([]raft.NodeID, len(descriptor.Replicas))
	for index, replica := range descriptor.Replicas {
		peers[index] = replica.NodeID
	}
	sort.Slice(peers, func(left, right int) bool { return peers[left] < peers[right] })
	root := filepath.Join(n.directory, "ranges", strconv.FormatUint(uint64(descriptor.RangeID), 10))
	var hook replicatedrange.Hook
	if n.options.RangeHook != nil {
		hook = n.options.RangeHook(descriptor.RangeID)
	}
	replica, err := replicatedrange.Open(replicatedrange.Options{
		RangeID: descriptor.RangeID, Generation: descriptor.Generation, NodeID: n.id, ReplicaID: local.ReplicaID,
		Peers: peers, Directory: root, Engine: engine.Options{MemTableBytes: n.options.MemTableBytes},
		ElectionTimeoutMin: 5 + uint64(n.id%3) + uint64(descriptor.RangeID%3),
		ElectionTimeoutMax: 5 + uint64(n.id%3) + uint64(descriptor.RangeID%3), HeartbeatInterval: 1,
		Random: rand.New(rand.NewPCG(uint64(n.id), uint64(descriptor.RangeID))), MaxProposalWaiters: n.options.MaxProposalWaiters, //nolint:gosec // deterministic election source, not security randomness
		ContainsKey: descriptor.Contains, ContainsSpan: descriptor.ContainsSpan, Hook: hook,
		MVCC: n.options.MVCC, Clock: n.options.Clock, Lifecycle: lifecycle,
	})
	if err != nil {
		return nil, fmt.Errorf("open replicated range %d: %w", descriptor.RangeID, err)
	}
	return replica, nil
}

func (n *Node) Catalog() *Catalog { n.mu.RLock(); defer n.mu.RUnlock(); return n.catalog }
func (n *Node) ID() raft.NodeID   { return n.id }

func (n *Node) Tick(rangeID RangeID) ([]Envelope, error) {
	replica, err := n.liveReplica(rangeID)
	if err != nil {
		return nil, err
	}
	messages, err := replica.Tick()
	if err != nil {
		return nil, fmt.Errorf("tick range %d: %w", rangeID, err)
	}
	return n.wrapMessages(rangeID, messages)
}

func (n *Node) Step(envelope Envelope) ([]Envelope, error) {
	if err := validateEnvelope(envelope); err != nil {
		return nil, err
	}
	if envelope.Message.To != n.id {
		return nil, ErrWrongRangeMessage
	}
	descriptor, err := n.descriptorFor(envelope.RangeID)
	if err != nil {
		return nil, ErrUnknownRange
	}
	if envelope.Generation != descriptor.Generation {
		return nil, ErrStaleRange
	}
	if envelope.DescriptorFingerprint != descriptorFingerprint(descriptor) {
		return nil, ErrCatalogMismatch
	}
	from, fromExists := descriptor.ReplicaOn(envelope.Message.From)
	to, toExists := descriptor.ReplicaOn(envelope.Message.To)
	if !fromExists || !toExists || from.ReplicaID != envelope.FromReplica || to.ReplicaID != envelope.ToReplica ||
		envelope.Message.From == envelope.Message.To {
		return nil, ErrWrongRangeMessage
	}
	replica, err := n.liveReplica(envelope.RangeID)
	if err != nil {
		return nil, err
	}
	messages, err := replica.Step(envelope.Message)
	if err != nil {
		return nil, fmt.Errorf("step range %d: %w", envelope.RangeID, err)
	}
	return n.wrapMessages(envelope.RangeID, messages)
}

type Route struct {
	RangeID    RangeID
	Generation uint64
	Key        []byte
}

type Pending struct {
	index   uint64
	waiter  <-chan replicatedrange.Result
	replica *replicatedrange.Replica
}

type NotLeaderError struct {
	RangeID RangeID
	Leader  raft.NodeID
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("%v: range=%d leader=%d", ErrNotLeader, e.RangeID, e.Leader)
}
func (e *NotLeaderError) Unwrap() error { return ErrNotLeader }

func (n *Node) Propose(ctx context.Context, route Route, encoded []byte) (*Pending, []Envelope, error) {
	descriptor, err := n.activeDescriptor(route.RangeID)
	if err != nil {
		return nil, nil, err
	}
	if route.Generation != descriptor.Generation {
		return nil, nil, &StaleRangeError{Current: descriptor}
	}
	if !descriptor.Contains(route.Key) {
		return nil, nil, ErrWrongRangeKey
	}
	command, err := replicatedrange.DecodeCommand(encoded)
	if err != nil {
		return nil, nil, fmt.Errorf("decode routed command: %w", err)
	}
	if !bytes.Equal(command.Key, route.Key) {
		return nil, nil, ErrWrongRangeKey
	}
	replica, err := n.liveReplica(route.RangeID)
	if err != nil {
		return nil, nil, err
	}
	index, messages, waiter, err := replica.Propose(ctx, encoded)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, nil, &NotLeaderError{RangeID: route.RangeID, Leader: replica.Status().Raft.LeaderID}
		}
		return nil, nil, fmt.Errorf("propose to range %d: %w", route.RangeID, err)
	}
	envelopes, err := n.wrapMessages(route.RangeID, messages)
	if err != nil {
		return nil, nil, err
	}
	return &Pending{index: index, waiter: waiter, replica: replica}, envelopes, nil
}

func (n *Node) ProposeMVCC(ctx context.Context, route Route, command replicatedrange.Command) (mvcc.Timestamp, *Pending, []Envelope, error) {
	descriptor, err := n.activeDescriptor(route.RangeID)
	if err != nil {
		return 0, nil, nil, err
	}
	if route.Generation != descriptor.Generation {
		return 0, nil, nil, &StaleRangeError{Current: descriptor}
	}
	if !descriptor.Contains(route.Key) || !bytes.Equal(command.Key, route.Key) {
		return 0, nil, nil, ErrWrongRangeKey
	}
	replica, err := n.liveReplica(route.RangeID)
	if err != nil {
		return 0, nil, nil, err
	}
	timestamp, index, messages, waiter, err := replica.ProposeMVCC(ctx, command)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, nil, nil, &NotLeaderError{RangeID: route.RangeID, Leader: replica.Status().Raft.LeaderID}
		}
		return 0, nil, nil, fmt.Errorf("propose MVCC to range %d: %w", route.RangeID, err)
	}
	envelopes, err := n.wrapMessages(route.RangeID, messages)
	if err != nil {
		return 0, nil, nil, err
	}
	return timestamp, &Pending{index: index, waiter: waiter, replica: replica}, envelopes, nil
}

func (n *Node) ProposeTransaction(ctx context.Context, route Route, command replicatedrange.Command) (*Pending, []Envelope, error) {
	descriptor, err := n.activeDescriptor(route.RangeID)
	if err != nil {
		return nil, nil, err
	}
	if route.Generation != descriptor.Generation {
		return nil, nil, &StaleRangeError{Current: descriptor}
	}
	if !descriptor.Contains(route.Key) || !bytes.Equal(command.Key, route.Key) {
		return nil, nil, ErrWrongRangeKey
	}
	replica, err := n.liveReplica(route.RangeID)
	if err != nil {
		return nil, nil, err
	}
	index, messages, waiter, err := replica.ProposeTransaction(ctx, command)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, nil, &NotLeaderError{RangeID: route.RangeID, Leader: replica.Status().Raft.LeaderID}
		}
		return nil, nil, fmt.Errorf("propose transaction command to range %d: %w", route.RangeID, err)
	}
	envelopes, err := n.wrapMessages(route.RangeID, messages)
	if err != nil {
		return nil, nil, err
	}
	return &Pending{index: index, waiter: waiter, replica: replica}, envelopes, nil
}

func (n *Node) ProposeSplit(ctx context.Context, rangeID RangeID, command replicatedrange.Command) (*Pending, []Envelope, error) {
	descriptor, err := n.descriptorFor(rangeID)
	if err != nil {
		return nil, nil, err
	}
	replica, err := n.liveReplica(rangeID)
	if err != nil {
		return nil, nil, err
	}
	index, messages, waiter, err := replica.ProposeSplit(ctx, command)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, nil, &NotLeaderError{RangeID: rangeID, Leader: replica.Status().Raft.LeaderID}
		}
		return nil, nil, fmt.Errorf("propose split command to range %d: %w", rangeID, err)
	}
	envelopes, err := n.wrapMessages(descriptor.RangeID, messages)
	if err != nil {
		return nil, nil, err
	}
	return &Pending{index: index, waiter: waiter, replica: replica}, envelopes, nil
}

func (n *Node) AssignTransactionTimestamp(ctx context.Context, route Route, floor mvcc.Timestamp) (mvcc.Timestamp, *Pending, []Envelope, error) {
	descriptor, err := n.activeDescriptor(route.RangeID)
	if err != nil {
		return 0, nil, nil, err
	}
	if route.Generation != descriptor.Generation {
		return 0, nil, nil, &StaleRangeError{Current: descriptor}
	}
	if !descriptor.Contains(route.Key) {
		return 0, nil, nil, ErrWrongRangeKey
	}
	replica, err := n.liveReplica(route.RangeID)
	if err != nil {
		return 0, nil, nil, err
	}
	timestamp, index, messages, waiter, err := replica.AssignTransactionTimestamp(ctx, route.Key, floor)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return 0, nil, nil, &NotLeaderError{RangeID: route.RangeID, Leader: replica.Status().Raft.LeaderID}
		}
		return 0, nil, nil, fmt.Errorf("assign transaction timestamp on range %d: %w", route.RangeID, err)
	}
	envelopes, err := n.wrapMessages(route.RangeID, messages)
	if err != nil {
		return 0, nil, nil, err
	}
	return timestamp, &Pending{index: index, waiter: waiter, replica: replica}, envelopes, nil
}

func (p *Pending) poll() (bool, error) {
	select {
	case result := <-p.waiter:
		return true, result.Err
	default:
		return false, nil
	}
}

type StaleRangeError struct{ Current RangeDescriptor }

func (e *StaleRangeError) Error() string {
	return fmt.Sprintf("%v: range=%d generation=%d", ErrStaleRange, e.Current.RangeID, e.Current.Generation)
}
func (e *StaleRangeError) Unwrap() error { return ErrStaleRange }

type RangeSplitError struct {
	Parent   RangeID
	Children []RangeDescriptor
}

func (e *RangeSplitError) Error() string {
	return fmt.Sprintf("%v: parent=%d children=%d", ErrRangeSplit, e.Parent, len(e.Children))
}
func (e *RangeSplitError) Unwrap() error { return ErrRangeSplit }

func (n *Node) activeDescriptor(rangeID RangeID) (RangeDescriptor, error) {
	descriptor, err := n.Catalog().LookupByID(rangeID)
	if err == nil {
		return descriptor, nil
	}
	n.mu.RLock()
	children := append([]RangeDescriptor(nil), n.retired[rangeID]...)
	n.mu.RUnlock()
	if len(children) != 0 {
		return RangeDescriptor{}, &RangeSplitError{Parent: rangeID, Children: children}
	}
	return RangeDescriptor{}, err
}

func (n *Node) InstallRetiredRedirect(parent RangeID, children []RangeDescriptor) {
	n.mu.Lock()
	n.retired[parent] = make([]RangeDescriptor, len(children))
	for index := range children {
		n.retired[parent][index] = cloneDescriptor(children[index])
	}
	n.mu.Unlock()
}

func (n *Node) wrapMessages(rangeID RangeID, messages []raft.Message) ([]Envelope, error) {
	descriptor, err := n.descriptorFor(rangeID)
	if err != nil {
		return nil, err
	}
	result := make([]Envelope, len(messages))
	for index, message := range messages {
		from, fromExists := descriptor.ReplicaOn(message.From)
		to, toExists := descriptor.ReplicaOn(message.To)
		if !fromExists || !toExists {
			return nil, ErrWrongRangeMessage
		}
		result[index] = Envelope{RangeID: rangeID, MessageRangeID: rangeID, Generation: descriptor.Generation, DescriptorFingerprint: descriptorFingerprint(descriptor),
			FromReplica: from.ReplicaID, ToReplica: to.ReplicaID, Message: message}
	}
	return result, nil
}

func (n *Node) descriptorFor(rangeID RangeID) (RangeDescriptor, error) {
	n.mu.RLock()
	if descriptor, ok := n.dynamic[rangeID]; ok {
		n.mu.RUnlock()
		return cloneDescriptor(descriptor), nil
	}
	catalog := n.catalog
	n.mu.RUnlock()
	return catalog.LookupByID(rangeID)
}

// AddShadowRange creates an assigned child group without granting user-serving
// authority. Directory existence alone never activates it.
func (n *Node) AddShadowRange(descriptor RangeDescriptor) error {
	if _, assigned := descriptor.ReplicaOn(n.id); !assigned {
		return ErrUnknownRange
	}
	n.mu.Lock()
	if n.stopped || len(n.registry.IDs()) >= n.options.MaxHostedRanges {
		n.mu.Unlock()
		return ErrResourceLimit
	}
	n.dynamic[descriptor.RangeID] = cloneDescriptor(descriptor)
	n.mu.Unlock()
	if _, err := n.registry.Lookup(descriptor.RangeID); err == nil {
		return nil
	}
	replica, err := n.openReplicaLifecycle(descriptor, replicatedrange.LifecycleShadow)
	if err != nil {
		return err
	}
	if err := n.registry.Register(descriptor.RangeID, replica); err != nil {
		return errors.Join(err, replica.Close(context.Background()))
	}
	return nil
}

func (n *Node) InstallDynamicCatalog(catalog *Catalog) error {
	if catalog == nil {
		return ErrInvalidCatalog
	}
	n.mu.Lock()
	if catalog.Generation() < n.catalog.Generation() {
		n.mu.Unlock()
		return ErrStaleRange
	}
	for _, descriptor := range n.catalog.Snapshot().Ranges {
		n.dynamic[descriptor.RangeID] = cloneDescriptor(descriptor)
	}
	n.catalog = catalog
	for _, descriptor := range catalog.Snapshot().Ranges {
		n.dynamic[descriptor.RangeID] = cloneDescriptor(descriptor)
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) liveReplica(rangeID RangeID) (*replicatedrange.Replica, error) {
	n.mu.RLock()
	stopped := n.stopped
	n.mu.RUnlock()
	if stopped {
		return nil, ErrNodeStopped
	}
	return n.registry.Lookup(rangeID)
}

func (n *Node) Status() NodeStatus {
	result := NodeStatus{NodeID: n.id, Orphans: append([]string(nil), n.orphans...)}
	for _, rangeID := range n.registry.IDs() {
		replica, lookupErr := n.registry.Lookup(rangeID)
		if lookupErr != nil {
			result.Failures = append(result.Failures, RangeFailure{RangeID: rangeID, Err: lookupErr})
			continue
		}
		result.Ranges = append(result.Ranges, replica.Status())
		if descriptor, descriptorErr := n.Catalog().LookupByID(rangeID); descriptorErr == nil {
			result.Descriptors = append(result.Descriptors, descriptor)
		}
	}
	n.mu.RLock()
	for rangeID, err := range n.failures {
		result.Failures = append(result.Failures, RangeFailure{RangeID: rangeID, Err: err})
	}
	n.mu.RUnlock()
	sort.Slice(result.Failures, func(left, right int) bool { return result.Failures[left].RangeID < result.Failures[right].RangeID })
	return result
}

func (n *Node) Replica(rangeID RangeID) (*replicatedrange.Replica, error) {
	return n.registry.Lookup(rangeID)
}

func (n *Node) RestartRange(rangeID RangeID) error {
	descriptor, err := n.descriptorFor(rangeID)
	if err != nil {
		return err
	}
	if _, assigned := descriptor.ReplicaOn(n.id); !assigned {
		return ErrUnknownRange
	}
	if old, unregisterErr := n.registry.Unregister(rangeID); unregisterErr == nil {
		if closeErr := old.Close(context.Background()); closeErr != nil {
			return fmt.Errorf("close range %d before restart: %w", rangeID, closeErr)
		}
	}
	replica, err := n.openReplica(descriptor)
	if err != nil {
		n.mu.Lock()
		n.failures[rangeID] = err
		n.mu.Unlock()
		return err
	}
	if registerErr := n.registry.Register(rangeID, replica); registerErr != nil {
		closeErr := replica.Close(context.Background())
		return errors.Join(registerErr, closeErr)
	}
	n.mu.Lock()
	delete(n.failures, rangeID)
	n.mu.Unlock()
	return nil
}

func (n *Node) Close(ctx context.Context) error {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil
	}
	n.stopped = true
	n.mu.Unlock()
	var result error
	for _, rangeID := range n.registry.IDs() {
		replica, err := n.registry.Unregister(rangeID)
		if err == nil {
			result = errors.Join(result, replica.Close(ctx))
		}
	}
	return result
}

func (n *Node) bootstrapComplete() (bool, error) {
	encoded, err := os.ReadFile(filepath.Join(n.directory, "cluster", bootstrapCompleteFilename)) //nolint:gosec // fixed filename beneath node root
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read bootstrap completion marker: %w", err)
	}
	if len(encoded) != 40 {
		return false, fmt.Errorf("%w: malformed bootstrap completion marker", ErrCorruptCatalog)
	}
	markerGeneration := binary.LittleEndian.Uint64(encoded[:8])
	fingerprint := n.catalog.Fingerprint()
	if markerGeneration < n.catalog.Generation() {
		return false, nil
	}
	if markerGeneration != n.catalog.Generation() || !bytes.Equal(encoded[8:], fingerprint[:]) {
		return false, fmt.Errorf("%w: bootstrap marker/catalog mismatch", ErrCorruptCatalog)
	}
	return true, nil
}

func (n *Node) publishBootstrapComplete() error {
	fingerprint := n.catalog.Fingerprint()
	encoded := make([]byte, 8+len(fingerprint))
	binary.LittleEndian.PutUint64(encoded[:8], n.catalog.Generation())
	copy(encoded[8:], fingerprint[:])
	return publishSmallFile(filepath.Join(n.directory, "cluster"), bootstrapCompleteFilename, encoded)
}

func publishSmallFile(directory, name string, value []byte) (resultErr error) {
	temporary := filepath.Join(directory, "."+name+".tmp")
	file, openErr := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640) //nolint:gosec // node metadata permissions
	if openErr != nil {
		return fmt.Errorf("create %s temporary: %w", name, openErr)
	}
	closed := false
	defer func() {
		if !closed {
			resultErr = errors.Join(resultErr, file.Close())
		}
	}()
	if _, writeErr := file.Write(value); writeErr != nil {
		return fmt.Errorf("write %s: %w", name, writeErr)
	}
	if syncErr := file.Sync(); syncErr != nil {
		return fmt.Errorf("sync %s: %w", name, syncErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return fmt.Errorf("close %s: %w", name, closeErr)
	}
	closed = true
	if renameErr := os.Rename(temporary, filepath.Join(directory, name)); renameErr != nil {
		return fmt.Errorf("rename %s: %w", name, renameErr)
	}
	directoryFile, directoryOpenErr := os.Open(directory) //nolint:gosec // configured metadata directory
	if directoryOpenErr != nil {
		return fmt.Errorf("open metadata directory: %w", directoryOpenErr)
	}
	if directorySyncErr := directoryFile.Sync(); directorySyncErr != nil {
		return errors.Join(fmt.Errorf("sync metadata directory: %w", directorySyncErr), directoryFile.Close())
	}
	if directoryCloseErr := directoryFile.Close(); directoryCloseErr != nil {
		return fmt.Errorf("close metadata directory: %w", directoryCloseErr)
	}
	return nil
}

func rangeDirectoriesExist(directory string, rangeID RangeID) bool {
	root := filepath.Join(directory, "ranges", strconv.FormatUint(uint64(rangeID), 10))
	for _, child := range []string{"raft", "data"} {
		info, err := os.Stat(filepath.Join(root, child))
		if err != nil || !info.IsDir() {
			return false
		}
	}
	return true
}

func findOrphans(directory string, assigned []RangeDescriptor) []string {
	wanted := make(map[string]struct{}, len(assigned))
	for _, descriptor := range assigned {
		wanted[strconv.FormatUint(uint64(descriptor.RangeID), 10)] = struct{}{}
	}
	entries, err := os.ReadDir(filepath.Join(directory, "ranges"))
	if err != nil {
		return nil
	}
	var result []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, exists := wanted[entry.Name()]; !exists {
			result = append(result, entry.Name())
		}
	}
	sort.Strings(result)
	return result
}

func failuresError(failures map[RangeID]error) error {
	var result error
	for rangeID, err := range failures {
		result = errors.Join(result, fmt.Errorf("range %d: %w", rangeID, err))
	}
	return result
}

func ValidateNodeCatalogs(nodes ...*Node) error {
	if len(nodes) == 0 {
		return ErrInvalidCatalog
	}
	fingerprint := nodes[0].catalog.Fingerprint()
	for _, node := range nodes[1:] {
		if node.catalog.Fingerprint() != fingerprint {
			return ErrCatalogMismatch
		}
	}
	return nil
}
