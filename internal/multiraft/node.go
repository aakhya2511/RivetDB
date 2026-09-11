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

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

const bootstrapCompleteFilename = "bootstrap-complete"

type NodeOptions struct {
	NodeID             raft.NodeID
	Directory          string
	Bootstrap          *Bootstrap
	MaxHostedRanges    int
	MaxProposalWaiters int
	MemTableBytes      uint64
	CatalogHook        CatalogPublishHook
	RangeHook          func(RangeID) replicatedrange.Hook
	BootstrapRangeHook func(RangeID) error
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
	assigned := catalog.Assigned(options.NodeID)
	if len(assigned) > options.MaxHostedRanges {
		return nil, ErrResourceLimit
	}
	node := &Node{id: options.NodeID, directory: options.Directory, catalog: catalog, registry: newRangeRegistry(),
		options: options, failures: make(map[RangeID]error)}
	complete, completionErr := node.bootstrapComplete()
	if completionErr != nil {
		return nil, completionErr
	}
	for _, descriptor := range assigned {
		if complete && !rangeDirectoriesExist(options.Directory, descriptor.RangeID) {
			node.failures[descriptor.RangeID] = ErrMissingRange
			continue
		}
		replica, openErr := node.openReplica(descriptor)
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

func (n *Node) openReplica(descriptor RangeDescriptor) (*replicatedrange.Replica, error) {
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
		ContainsKey: descriptor.Contains, Hook: hook,
	})
	if err != nil {
		return nil, fmt.Errorf("open replicated range %d: %w", descriptor.RangeID, err)
	}
	return replica, nil
}

func (n *Node) Catalog() *Catalog { return n.catalog }
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
	descriptor, err := n.catalog.LookupByID(envelope.RangeID)
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
	descriptor, err := n.catalog.LookupByID(route.RangeID)
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

func (n *Node) wrapMessages(rangeID RangeID, messages []raft.Message) ([]Envelope, error) {
	descriptor, err := n.catalog.LookupByID(rangeID)
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
		if descriptor, descriptorErr := n.catalog.LookupByID(rangeID); descriptorErr == nil {
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
	descriptor, err := n.catalog.LookupByID(rangeID)
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
