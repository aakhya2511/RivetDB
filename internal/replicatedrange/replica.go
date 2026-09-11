package replicatedrange

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

type RangeID uint64
type ReplicaID uint64

const StaticRangeID RangeID = 1

// Stage is a deterministic integration boundary used by crash tests.
type Stage uint8

const (
	StageRaftCommitted Stage = iota
	StageApplyStarted
	StageMemTableApplied
	StageVisibilityPublished
	StageSSTableDurable
	StageManifestAppliedFrontierDurable
)

func (s Stage) String() string {
	switch s {
	case StageRaftCommitted:
		return "RAFT_COMMITTED"
	case StageApplyStarted:
		return "APPLY_STARTED"
	case StageMemTableApplied:
		return "MEMTABLE_APPLIED"
	case StageVisibilityPublished:
		return "VISIBILITY_PUBLISHED"
	case StageSSTableDurable:
		return "SSTABLE_DURABLE"
	case StageManifestAppliedFrontierDurable:
		return "MANIFEST_APPLIED_FRONTIER_DURABLE"
	default:
		return fmt.Sprintf("UNKNOWN_STAGE_%d", s)
	}
}

// Hook observes an integration stage. It must not reenter the Replica.
type Hook func(Stage, uint64)

var (
	ErrInvalidOptions = errors.New("replicated range: invalid options")
	ErrLeadershipLost = errors.New("replicated range: leadership lost before local apply")
	ErrStopped        = errors.New("replicated range: stopped")
	ErrTooManyWaiters = errors.New("replicated range: proposal waiter capacity reached")
	ErrKeyOutOfRange  = errors.New("replicated range: command key outside configured range")
	ErrMVCCRegression = errors.New("replicated range: MVCC timestamp regression")
	ErrReplicaBehind  = errors.New("replicated range: requested MVCC timestamp exceeds applied watermark")
	ErrSnapshotClosed = errors.New("replicated range: MVCC snapshot closed")
)

func maximumCommandTimestamp(entries []raft.Entry) (mvcc.Timestamp, error) {
	var maximum mvcc.Timestamp
	for _, entry := range entries {
		if entry.Type != raft.EntryCommand {
			continue
		}
		command, err := DecodeCommand(entry.Command)
		if err != nil {
			return 0, fmt.Errorf("decode durable timestamp history at index %d: %w", entry.Index, err)
		}
		if command.Timestamp > maximum {
			maximum = command.Timestamp
		}
	}
	return maximum, nil
}

type Options struct {
	RangeID            RangeID
	NodeID             raft.NodeID
	ReplicaID          ReplicaID
	Peers              []raft.NodeID
	Directory          string
	Store              raft.Store
	Engine             engine.Options
	ElectionTimeoutMin uint64
	ElectionTimeoutMax uint64
	HeartbeatInterval  uint64
	Random             *rand.Rand
	MaxProposalWaiters int
	Hook               Hook
	Generation         uint64
	ContainsKey        func([]byte) bool
	ContainsSpan       func([]byte, []byte) bool
	MVCC               bool
	Clock              clock.Clock
}

type Result struct {
	Index uint64
	Err   error
}

type Status struct {
	RangeID                 RangeID
	Generation              uint64
	NodeID                  raft.NodeID
	ReplicaID               ReplicaID
	Raft                    raft.Status
	DurableAppliedRaftIndex uint64
	LSMVisibleIndex         uint64
	MaxAppliedMVCC          mvcc.Timestamp
	LiveTableCount          uint64
	MaterializedCommands    uint64
	Fatal                   error
}

// Replica is one deterministic Raft group plus one range-scoped replicated LSM.
// It owns no timer or transport goroutine; callers drive Tick and Step.
type Replica struct {
	mu           sync.Mutex
	rangeID      RangeID
	replicaID    ReplicaID
	generation   uint64
	containsKey  func([]byte) bool
	containsSpan func([]byte, []byte) bool
	node         *raft.Node
	engine       *engine.Engine
	store        raft.Store
	hlc          *mvcc.Clock
	mvcc         bool
	machine      *stateMachine
	waiters      map[uint64]chan Result
	snapshots    map[uint64]mvcc.Timestamp
	nextSnapshot uint64
	maxWaiters   int
	stopped      bool
}

func Open(options Options) (_ *Replica, resultErr error) {
	if options.RangeID == 0 || options.NodeID == 0 || options.ReplicaID == 0 || len(options.Peers) == 0 || options.Random == nil ||
		options.ElectionTimeoutMin == 0 || options.ElectionTimeoutMax < options.ElectionTimeoutMin || options.HeartbeatInterval == 0 {
		return nil, ErrInvalidOptions
	}
	if options.MaxProposalWaiters == 0 {
		options.MaxProposalWaiters = 1024
	}
	if options.Generation == 0 {
		options.Generation = 1
	}
	if options.MaxProposalWaiters < 1 {
		return nil, ErrInvalidOptions
	}
	if options.Engine.Directory == "" {
		if options.Directory == "" {
			return nil, ErrInvalidOptions
		}
		options.Engine.Directory = filepath.Join(options.Directory, "data")
	}
	if options.Clock == nil {
		options.Clock = options.Engine.Clock
	}
	if options.Clock == nil {
		options.Clock = clock.System()
	}
	options.Engine.Clock = options.Clock
	options.Engine.Mode = engine.ModeReplicated
	if options.MVCC {
		options.Engine.Mode = engine.ModeReplicatedMVCC
	}
	if options.Hook != nil {
		engineHook := options.Engine.ReplicatedHook
		options.Engine.ReplicatedHook = func(stage engine.ReplicatedStage, index uint64) {
			if engineHook != nil {
				engineHook(stage, index)
			}
			var mapped Stage
			switch stage {
			case engine.ReplicatedStageApplyStarted:
				mapped = StageApplyStarted
			case engine.ReplicatedStageMemTableApplied:
				mapped = StageMemTableApplied
			case engine.ReplicatedStageVisibilityPublished:
				mapped = StageVisibilityPublished
			case engine.ReplicatedStageSSTableDurable:
				mapped = StageSSTableDurable
			case engine.ReplicatedStageManifestFrontierDurable:
				mapped = StageManifestAppliedFrontierDurable
			default:
				return
			}
			options.Hook(mapped, index)
		}
	}
	local, err := engine.Open(options.Engine)
	if err != nil {
		return nil, fmt.Errorf("open range LSM: %w", err)
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, local.Close(context.Background()))
		}
	}()
	store := options.Store
	if store == nil {
		if options.Directory == "" {
			return nil, ErrInvalidOptions
		}
		store, err = raft.OpenFileStore(filepath.Join(options.Directory, "raft"))
		if err != nil {
			return nil, fmt.Errorf("open range Raft store: %w", err)
		}
	}
	appliedFloor := mvcc.Timestamp(0)
	clockFloor := mvcc.Timestamp(0)
	if options.MVCC {
		if durable, ok, durableErr := local.DurableMaxAppliedMVCC(); durableErr != nil {
			return nil, fmt.Errorf("read durable MVCC watermark: %w", durableErr)
		} else if ok {
			appliedFloor = mvcc.Timestamp(durable)
			clockFloor = appliedFloor
		}
		state, loadErr := store.Load()
		if loadErr != nil {
			return nil, fmt.Errorf("load Raft timestamp history: %w", loadErr)
		}
		observed, observeErr := maximumCommandTimestamp(state.Entries)
		if observeErr != nil {
			return nil, observeErr
		}
		if observed > clockFloor {
			clockFloor = observed
		}
	}
	hlc, err := mvcc.NewClock(options.Clock, clockFloor)
	if err != nil {
		return nil, fmt.Errorf("create range HLC: %w", err)
	}
	machine := &stateMachine{engine: local, hook: options.Hook, containsKey: options.ContainsKey,
		mvcc: options.MVCC, maxApplied: appliedFloor, clock: hlc}
	r := &Replica{rangeID: options.RangeID, replicaID: options.ReplicaID, generation: options.Generation,
		containsKey: options.ContainsKey, containsSpan: options.ContainsSpan, engine: local, store: store, hlc: hlc, mvcc: options.MVCC, machine: machine,
		waiters: make(map[uint64]chan Result), snapshots: make(map[uint64]mvcc.Timestamp), maxWaiters: options.MaxProposalWaiters}
	r.node, err = raft.NewNode(raft.Config{
		ID: options.NodeID, Peers: options.Peers, ElectionTimeoutMin: options.ElectionTimeoutMin,
		ElectionTimeoutMax: options.ElectionTimeoutMax, HeartbeatInterval: options.HeartbeatInterval,
		Random: options.Random, Store: store, StateMachine: machine,
	})
	if err != nil {
		return nil, fmt.Errorf("open range Raft node: %w", err)
	}
	if err := r.syncAppliedLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Replica) Tick() ([]raft.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	before := r.node.Status()
	messages, err := r.node.Tick()
	r.afterRaftLocked(before, err)
	if err != nil {
		return messages, fmt.Errorf("tick range Raft node: %w", err)
	}
	return messages, nil
}

func (r *Replica) Step(message raft.Message) ([]raft.Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return nil, ErrStopped
	}
	before := r.node.Status()
	messages, err := r.node.Step(message)
	r.afterRaftLocked(before, err)
	if err != nil {
		return messages, fmt.Errorf("step range Raft node: %w", err)
	}
	return messages, nil
}

// Propose validates before durable Raft admission and returns an apply waiter.
func (r *Replica) Propose(ctx context.Context, encoded []byte) (uint64, []raft.Message, <-chan Result, error) {
	if ctx == nil {
		return 0, nil, nil, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return 0, nil, nil, fmt.Errorf("before proposal admission: %w", err)
	}
	if r.mvcc {
		return 0, nil, nil, ErrInvalidCommand
	}
	command, err := DecodeCommand(encoded)
	if err != nil {
		return 0, nil, nil, err
	}
	if r.containsKey != nil && !r.containsKey(command.Key) {
		return 0, nil, nil, ErrKeyOutOfRange
	}
	if command.Timestamp != 0 {
		return 0, nil, nil, ErrInvalidCommand
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.proposeLocked(encoded)
}

// ProposeMVCC assigns and replicates one timestamped mutation. The timestamp
// is generated only on a leader and becomes part of the durable command.
func (r *Replica) ProposeMVCC(ctx context.Context, command Command) (mvcc.Timestamp, uint64, []raft.Message, <-chan Result, error) {
	if ctx == nil || !r.mvcc || command.Timestamp != 0 {
		return 0, 0, nil, nil, ErrInvalidOptions
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, nil, nil, fmt.Errorf("before MVCC proposal admission: %w", err)
	}
	if r.containsKey != nil && !r.containsKey(command.Key) {
		return 0, 0, nil, nil, ErrKeyOutOfRange
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.node.Status().Role != raft.Leader {
		return 0, 0, nil, nil, raft.ErrNotLeader
	}
	state, err := r.store.Load()
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("load timestamp floor before proposal: %w", err)
	}
	floor, err := maximumCommandTimestamp(state.Entries)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("assign MVCC timestamp: %w", err)
	}
	r.hlc.Observe(floor)
	timestamp, err := r.hlc.Now()
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("advance range HLC: %w", err)
	}
	command.Timestamp = timestamp
	encoded, err := EncodeCommand(command)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	index, messages, waiter, err := r.proposeLocked(encoded)
	return timestamp, index, messages, waiter, err
}

func (r *Replica) proposeLocked(encoded []byte) (uint64, []raft.Message, <-chan Result, error) {
	if r.stopped {
		return 0, nil, nil, ErrStopped
	}
	if len(r.waiters) >= r.maxWaiters {
		return 0, nil, nil, ErrTooManyWaiters
	}
	before := r.node.Status()
	index, messages, err := r.node.Propose(bytes.Clone(encoded))
	if err != nil {
		r.afterRaftLocked(before, err)
		return 0, nil, nil, fmt.Errorf("admit range proposal: %w", err)
	}
	waiter := make(chan Result, 1)
	r.waiters[index] = waiter
	r.afterRaftLocked(before, nil)
	return index, messages, waiter, nil
}

// Await waits for commit plus leader-local apply. Cancellation only abandons
// the waiter; a durably admitted command continues through consensus.
func (r *Replica) Await(ctx context.Context, index uint64, waiter <-chan Result) error {
	if ctx == nil || index == 0 || waiter == nil {
		return ErrInvalidOptions
	}
	select {
	case result := <-waiter:
		return result.Err
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.waiters, index)
		r.mu.Unlock()
		return fmt.Errorf("wait for proposal apply: %w", ctx.Err())
	}
}

func (r *Replica) afterRaftLocked(before raft.Status, operationErr error) {
	status := r.node.Status()
	if operationErr == nil {
		operationErr = r.syncAppliedLocked()
	}
	if operationErr != nil || status.Fatal != nil {
		failure := operationErr
		if failure == nil {
			failure = status.Fatal
		}
		r.failWaitersLocked(failure)
		return
	}
	for index, waiter := range r.waiters {
		if index <= status.LastApplied {
			waiter <- Result{Index: index}
			close(waiter)
			delete(r.waiters, index)
		}
	}
	if before.Role == raft.Leader && status.Role != raft.Leader {
		r.failWaitersLocked(ErrLeadershipLost)
	}
}

func (r *Replica) syncAppliedLocked() error {
	status := r.node.Status()
	if status.LastApplied == 0 {
		return nil
	}
	if err := r.engine.AdvanceApplied(status.LastApplied); err != nil {
		return fmt.Errorf("publish no-op applied progress: %w", err)
	}
	return nil
}

func (r *Replica) failWaitersLocked(err error) {
	for index, waiter := range r.waiters {
		waiter <- Result{Index: index, Err: err}
		close(waiter)
		delete(r.waiters, index)
	}
}

func (r *Replica) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	raftStatus := r.node.Status()
	durable, err := r.engine.DurableAppliedRaftIndex()
	stats := r.engine.Stats()
	result := Status{RangeID: r.rangeID, Generation: r.generation, NodeID: raftStatus.ID, ReplicaID: r.replicaID, Raft: raftStatus,
		DurableAppliedRaftIndex: durable, LSMVisibleIndex: stats.Pipeline.VisibleSequence,
		LiveTableCount: stats.Manifest.LiveTables, Fatal: errors.Join(raftStatus.Fatal, err)}
	if r.mvcc {
		result.MaxAppliedMVCC = r.machine.maxApplied
	}
	result.MaterializedCommands = stats.Puts + stats.Deletes
	return result
}

func (r *Replica) LocalGet(ctx context.Context, key []byte) ([]byte, error) {
	value, err := r.engine.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("local range get: %w", err)
	}
	return value, nil
}
func (r *Replica) LocalScan(ctx context.Context) ([]engine.KV, error) {
	values, err := r.engine.Scan(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("local range scan: %w", err)
	}
	return values, nil
}
func (r *Replica) Flush(ctx context.Context) error {
	if err := r.engine.Flush(ctx); err != nil {
		return fmt.Errorf("flush range LSM: %w", err)
	}
	return nil
}
func (r *Replica) Compact(ctx context.Context) error {
	if _, err := r.engine.Compact(ctx); err != nil {
		return fmt.Errorf("compact range LSM: %w", err)
	}
	return nil
}

func (r *Replica) ReclaimObsoleteTables(ctx context.Context) (engine.TableReclamationResult, error) {
	result, err := r.engine.ReclaimObsoleteTables(ctx)
	if err != nil {
		return result, fmt.Errorf("reclaim range LSM tables: %w", err)
	}
	return result, nil
}
func (r *Replica) Validate() error {
	if err := r.engine.Validate(); err != nil {
		return fmt.Errorf("validate range LSM: %w", err)
	}
	return nil
}

func (r *Replica) Digest(ctx context.Context) ([sha256.Size]byte, error) {
	values, err := r.LocalScan(ctx)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	hash := sha256.New()
	var field [8]byte
	for _, item := range values {
		binary.LittleEndian.PutUint64(field[:], uint64(len(item.Key)))
		_, _ = hash.Write(field[:])
		_, _ = hash.Write(item.Key)
		binary.LittleEndian.PutUint64(field[:], uint64(len(item.Value)))
		_, _ = hash.Write(field[:])
		_, _ = hash.Write(item.Value)
	}
	binary.LittleEndian.PutUint64(field[:], r.Status().Raft.LastApplied)
	_, _ = hash.Write(field[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func (r *Replica) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	r.node.Stop()
	r.failWaitersLocked(ErrStopped)
	r.mu.Unlock()
	if err := r.engine.Close(ctx); err != nil {
		return fmt.Errorf("close range LSM: %w", err)
	}
	return nil
}

// DirectoryLayout ensures the canonical range-scoped durable roots exist.
func DirectoryLayout(nodeDirectory string, rangeID RangeID) (raftDirectory, dataDirectory string, err error) {
	if nodeDirectory == "" || rangeID == 0 {
		return "", "", ErrInvalidOptions
	}
	root := filepath.Join(nodeDirectory, "ranges", fmt.Sprint(uint64(rangeID)))
	raftDirectory, dataDirectory = filepath.Join(root, "raft"), filepath.Join(root, "data")
	if err := os.MkdirAll(raftDirectory, 0o750); err != nil {
		return "", "", fmt.Errorf("create range Raft directory: %w", err)
	}
	if err := os.MkdirAll(dataDirectory, 0o750); err != nil {
		return "", "", fmt.Errorf("create range data directory: %w", err)
	}
	return raftDirectory, dataDirectory, nil
}
