package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/txn"
)

type TxnStage uint8

const (
	TxnStagePendingDurable TxnStage = iota
	TxnStageParticipantPrepared
	TxnStageAllPrepared
	TxnStageCommitDecisionDurable
	TxnStageAbortDecisionDurable
	TxnStageParticipantResolved
	TxnStageAllResolved
)

type TxnHook func(TxnStage, txn.ID)

type Transaction struct {
	mu         sync.Mutex
	router     *Router
	id         txn.ID
	readTime   mvcc.Timestamp
	writes     map[string]txn.Write
	barriers   map[RangeID]bool
	bytes      int
	state      txn.Status
	commitTime mvcc.Timestamp
	closed     bool
}

func (r *Router) Begin(ctx context.Context) (*Transaction, error) {
	if ctx == nil {
		return nil, ErrInvalidCatalog
	}
	descriptors := r.catalog.Snapshot().Ranges
	if len(descriptors) == 0 {
		return nil, ErrRangeNotFound
	}
	var readTime mvcc.Timestamp
	var selected RangeID
	var selectedDescriptor RangeDescriptor
	var floor mvcc.Timestamp
	for _, descriptor := range descriptors {
		replica, ok := r.immediateLeaderReplica(descriptor)
		if !ok {
			continue
		}
		if selected == 0 {
			selected, selectedDescriptor = descriptor.RangeID, descriptor
		}
		floor = max(floor, replica.Status().HLCFloor)
	}
	if selected == 0 {
		replica, err := r.leaderReplica(ctx, descriptors[0])
		if err != nil {
			return nil, err
		}
		selected, selectedDescriptor, floor = descriptors[0].RangeID, descriptors[0], replica.Status().HLCFloor
	}
	var err error
	readTime, err = r.assignTransactionTimestamp(ctx, selectedDescriptor, descriptorAnchor(selectedDescriptor), floor)
	if err != nil {
		return nil, err
	}
	if readTime == 0 {
		return nil, ErrLeaderUnknown
	}
	var id txn.ID
	for attempt := 0; attempt < 4; attempt++ {
		id, err = r.txnIDGenerator()
		if err != nil || id.IsZero() {
			return nil, fmt.Errorf("allocate transaction identity: %w", errors.Join(txn.ErrInvalid, err))
		}
		if !r.transactionIDKnown(id) {
			break
		}
		id = txn.ID{}
	}
	if id.IsZero() {
		return nil, txn.ErrProtocolConflict
	}
	r.mu.Lock()
	r.protected[id], r.seen[id] = readTime, struct{}{}
	r.mu.Unlock()
	return &Transaction{router: r, id: id, readTime: readTime, writes: make(map[string]txn.Write), barriers: map[RangeID]bool{selected: true}}, nil
}

func (t *Transaction) ID() txn.ID                    { return t.id }
func (t *Transaction) ReadTimestamp() mvcc.Timestamp { return t.readTime }
func (t *Transaction) CommitTimestamp() mvcc.Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commitTime
}

func (t *Transaction) Status() txn.Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == 0 {
		return txn.StatusPending
	}
	return t.state
}

func (t *Transaction) Put(key, value []byte) error {
	return t.buffer(txn.Write{Key: key, Value: value})
}

func (t *Transaction) Delete(key []byte) error {
	return t.buffer(txn.Write{Key: key, Delete: true})
}

func (t *Transaction) buffer(write txn.Write) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return txn.ErrClosed
	}
	if len(write.Key) > sstable.MaxUserKeySize || len(write.Value) > sstable.MaxValueSize {
		return txn.ErrTooLarge
	}
	if _, err := t.router.Route(write.Key); err != nil {
		return err
	}
	key := string(write.Key)
	old, exists := t.writes[key]
	newBytes := len(write.Key) + len(write.Value)
	if exists {
		newBytes -= len(old.Key) + len(old.Value)
	}
	if !exists && len(t.writes) >= t.router.maxTxnWrites || t.bytes+newBytes > t.router.maxTxnBytes {
		return txn.ErrTooLarge
	}
	t.bytes += newBytes
	t.writes[key] = txn.Write{Key: bytes.Clone(write.Key), Value: bytes.Clone(write.Value), Delete: write.Delete}
	return nil
}

func (t *Transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	if ctx == nil {
		return nil, ErrInvalidCatalog
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, txn.ErrClosed
	}
	if write, ok := t.writes[string(key)]; ok {
		t.mu.Unlock()
		if write.Delete {
			return nil, engine.ErrNotFound
		}
		return bytes.Clone(write.Value), nil
	}
	t.mu.Unlock()
	if err := t.ensureKeyBarrier(ctx, key); err != nil {
		return nil, err
	}
	return t.router.transactionGetAt(ctx, key, t.readTime)
}

func (t *Transaction) Scan(ctx context.Context, start, end []byte) ([]engine.KV, error) {
	if ctx == nil {
		return nil, ErrInvalidCatalog
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, engine.ErrInvalidRange
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, txn.ErrClosed
	}
	overlay := txn.CloneWrites(mapWrites(t.writes))
	if err := t.ensureSpanBarriersLocked(ctx, start, end); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	t.mu.Unlock()
	values, err := t.router.transactionScanAt(ctx, start, end, t.readTime)
	if err != nil {
		return nil, err
	}
	merged := make(map[string][]byte, len(values)+len(overlay))
	for _, value := range values {
		merged[string(value.Key)] = value.Value
	}
	for _, write := range overlay {
		if !inSpan(write.Key, start, end) {
			continue
		}
		if write.Delete {
			delete(merged, string(write.Key))
		} else {
			merged[string(write.Key)] = write.Value
		}
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return bytes.Compare([]byte(keys[left]), []byte(keys[right])) < 0 })
	result := make([]engine.KV, 0, len(keys))
	for _, key := range keys {
		result = append(result, engine.KV{Key: []byte(key), Value: bytes.Clone(merged[key])})
	}
	return result, nil
}

func (t *Transaction) Commit(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == txn.StatusCommitted {
		return nil
	}
	if t.state == txn.StatusAborted {
		return txn.ErrAlreadyAborted
	}
	if t.closed {
		return txn.ErrClosed
	}
	if len(t.writes) == 0 {
		t.closed = true
		t.releaseProtection()
		return nil
	}
	writes := mapWrites(t.writes)
	txn.SortWrites(writes)
	groups, participants, homeDescriptor, homeKey, err := t.router.groupTransactionWrites(writes)
	if err != nil {
		return err
	}
	home := txn.Participant{RangeID: uint64(homeDescriptor.RangeID), Generation: homeDescriptor.Generation}
	for _, participant := range participants {
		descriptor, lookupErr := t.router.catalog.LookupByID(RangeID(participant.RangeID))
		if lookupErr != nil {
			return lookupErr
		}
		if barrierErr := t.ensureDescriptorBarrierLocked(ctx, descriptor); barrierErr != nil {
			return barrierErr
		}
	}
	record, exists, err := t.router.recordStatus(ctx, homeDescriptor, t.id)
	if err != nil {
		return err
	}
	var commitTime mvcc.Timestamp
	epoch := uint64(1)
	if exists {
		if record.ReadTime != uint64(t.readTime) || record.Home != home || !sameParticipants(record.Participants, participants) {
			return txn.ErrProtocolConflict
		}
		commitTime, epoch = mvcc.Timestamp(record.CommitTime), record.Epoch
		if record.Status.Terminal() {
			if resolveErr := t.router.resolveRecord(ctx, record); resolveErr != nil {
				return resolveErr
			}
			t.state, t.closed, t.commitTime = record.Status, true, mvcc.Timestamp(record.CommitTime)
			t.releaseProtection()
			if record.Status == txn.StatusCommitted {
				return nil
			}
			return txn.ErrAlreadyAborted
		}
	} else {
		var floor = t.readTime
		for _, participant := range participants {
			descriptor, lookupErr := t.router.catalog.LookupByID(RangeID(participant.RangeID))
			if lookupErr != nil {
				return lookupErr
			}
			replica, leaderErr := t.router.leaderReplica(ctx, descriptor)
			if leaderErr != nil {
				return leaderErr
			}
			floor = max(floor, replica.Status().HLCFloor)
		}
		commitTime, err = t.router.assignTransactionTimestamp(ctx, homeDescriptor, homeKey, floor)
		if err != nil {
			return err
		}
		create := txn.Operation{Type: txn.OpCreate, ID: t.id, ReadTime: uint64(t.readTime), CommitTime: uint64(commitTime), Epoch: epoch, Home: home, Participants: participants}
		if proposeErr := t.router.proposeOperation(ctx, homeDescriptor, homeKey, replicatedrange.CommandTxnCreate, create); proposeErr != nil {
			return proposeErr
		}
		t.router.observeTxn(TxnStagePendingDurable, t.id)
	}
	prepared := true
	for _, participant := range participants {
		descriptor, lookupErr := t.router.catalog.LookupByID(RangeID(participant.RangeID))
		if lookupErr != nil {
			return lookupErr
		}
		participantWrites := groups[RangeID(participant.RangeID)]
		operation := txn.Operation{Type: txn.OpPrepare, ID: t.id, ReadTime: uint64(t.readTime), CommitTime: uint64(commitTime), Epoch: epoch, Home: home, Writes: participantWrites}
		if proposeErr := t.router.proposeOperation(ctx, descriptor, participantWrites[0].Key, replicatedrange.CommandTxnPrepare, operation); proposeErr != nil {
			prepared = false
			break
		}
		record, ok, queryErr := t.router.participantStatus(ctx, descriptor, t.id)
		if queryErr != nil || !ok || record.Status != txn.ParticipantPrepared {
			prepared = false
			break
		}
		t.router.observeTxn(TxnStageParticipantPrepared, t.id)
	}
	decisionType, operationType, terminal := replicatedrange.CommandTxnAbort, txn.OpAbort, txn.StatusAborted
	if prepared {
		decisionType, operationType, terminal = replicatedrange.CommandTxnCommit, txn.OpCommit, txn.StatusCommitted
		t.router.observeTxn(TxnStageAllPrepared, t.id)
	}
	decision := txn.Operation{Type: operationType, ID: t.id, ReadTime: uint64(t.readTime), CommitTime: uint64(commitTime), Epoch: epoch, Home: home}
	if proposeErr := t.router.proposeOperation(ctx, homeDescriptor, homeKey, decisionType, decision); proposeErr != nil {
		return proposeErr
	}
	if terminal == txn.StatusCommitted {
		t.router.observeTxn(TxnStageCommitDecisionDurable, t.id)
	} else {
		t.router.observeTxn(TxnStageAbortDecisionDurable, t.id)
	}
	record, ok, err := t.router.recordStatus(ctx, homeDescriptor, t.id)
	if err != nil || !ok {
		return errors.Join(txn.ErrProtocolConflict, err)
	}
	if record.Status != terminal {
		if record.Status == txn.StatusCommitted {
			t.state = record.Status
			return txn.ErrAlreadyCommitted
		}
		if record.Status == txn.StatusAborted {
			t.state = record.Status
			return txn.ErrAlreadyAborted
		}
		return txn.ErrProtocolConflict
	}
	if err := t.router.resolveRecord(ctx, record); err != nil {
		return err
	}
	t.state, t.closed, t.commitTime = terminal, true, mvcc.Timestamp(record.CommitTime)
	t.releaseProtection()
	if terminal == txn.StatusAborted {
		return txn.ErrWriteConflict
	}
	return nil
}

func (t *Transaction) ensureKeyBarrier(ctx context.Context, key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return txn.ErrClosed
	}
	descriptor, err := t.router.catalog.Lookup(key)
	if err != nil {
		return err
	}
	return t.ensureDescriptorBarrierLocked(ctx, descriptor)
}

func (t *Transaction) ensureSpanBarriersLocked(ctx context.Context, start, end []byte) error {
	for _, descriptor := range t.router.catalog.Snapshot().Ranges {
		if _, _, ok := intersectDescriptor(descriptor, start, end); !ok {
			continue
		}
		if err := t.ensureDescriptorBarrierLocked(ctx, descriptor); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transaction) ensureDescriptorBarrierLocked(ctx context.Context, descriptor RangeDescriptor) error {
	if t.barriers[descriptor.RangeID] {
		return nil
	}
	key := descriptorAnchor(descriptor)
	if err := t.router.proposeTransaction(ctx, descriptor, key, replicatedrange.Command{Type: replicatedrange.CommandTxnBarrier, Key: key, Timestamp: t.readTime}); err != nil {
		return err
	}
	t.barriers[descriptor.RangeID] = true
	return nil
}

func (t *Transaction) Abort(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == txn.StatusCommitted {
		return txn.ErrAlreadyCommitted
	}
	if t.state == txn.StatusAborted {
		return nil
	}
	if t.closed {
		return txn.ErrClosed
	}
	if len(t.writes) != 0 {
		writes := mapWrites(t.writes)
		txn.SortWrites(writes)
		_, _, homeDescriptor, homeKey, err := t.router.groupTransactionWrites(writes)
		if err != nil {
			return err
		}
		record, exists, err := t.router.recordStatus(ctx, homeDescriptor, t.id)
		if err != nil {
			return err
		}
		if exists {
			if record.Status == txn.StatusCommitted {
				t.state = txn.StatusCommitted
				return txn.ErrAlreadyCommitted
			}
			if record.Status == txn.StatusPending {
				operation := txn.Operation{Type: txn.OpAbort, ID: record.ID, ReadTime: record.ReadTime,
					CommitTime: record.CommitTime, Epoch: record.Epoch, Home: record.Home}
				if err := t.router.proposeOperation(ctx, homeDescriptor, homeKey, replicatedrange.CommandTxnAbort, operation); err != nil {
					return err
				}
				record.Status = txn.StatusAborted
			}
			if err := t.router.resolveRecord(ctx, record); err != nil {
				return err
			}
		}
	}
	t.state, t.closed = txn.StatusAborted, true
	t.releaseProtection()
	return nil
}

// Close aborts an open buffered transaction and is idempotent after any close.
func (t *Transaction) Close(ctx context.Context) error {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil
	}
	return t.Abort(ctx)
}

func sameParticipants(left, right []txn.Participant) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (t *Transaction) releaseProtection() {
	t.router.mu.Lock()
	delete(t.router.protected, t.id)
	t.router.mu.Unlock()
}

func (r *Router) OldestProtectedTimestamp() (mvcc.Timestamp, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var oldest mvcc.Timestamp
	first := true
	for _, timestamp := range r.protected {
		if first || timestamp < oldest {
			oldest, first = timestamp, false
		}
	}
	return oldest, !first
}

func (r *Router) transactionIDKnown(id txn.ID) bool {
	r.mu.Lock()
	_, known := r.seen[id]
	r.mu.Unlock()
	if known {
		return true
	}
	for _, descriptor := range r.catalog.Snapshot().Ranges {
		if replica, ok := r.immediateLeaderReplica(descriptor); ok {
			if _, exists := replica.TransactionRecord(id); exists {
				return true
			}
		}
	}
	return false
}

func (r *Router) groupTransactionWrites(writes []txn.Write) (map[RangeID][]txn.Write, []txn.Participant, RangeDescriptor, []byte, error) {
	groups := make(map[RangeID][]txn.Write)
	descriptors := make(map[RangeID]RangeDescriptor)
	for _, write := range writes {
		descriptor, err := r.catalog.Lookup(write.Key)
		if err != nil {
			return nil, nil, RangeDescriptor{}, nil, err
		}
		groups[descriptor.RangeID] = append(groups[descriptor.RangeID], write)
		descriptors[descriptor.RangeID] = descriptor
	}
	if len(groups) > r.maxTxnParticipants {
		return nil, nil, RangeDescriptor{}, nil, txn.ErrTooLarge
	}
	participants := make([]txn.Participant, 0, len(groups))
	for rangeID, descriptor := range descriptors {
		participants = append(participants, txn.Participant{RangeID: uint64(rangeID), Generation: descriptor.Generation})
	}
	txn.SortParticipants(participants)
	homeKey := bytes.Clone(writes[0].Key)
	homeDescriptor, err := r.catalog.Lookup(homeKey)
	if err != nil {
		return nil, nil, RangeDescriptor{}, nil, err
	}
	return groups, participants, homeDescriptor, homeKey, nil
}

func mapWrites(values map[string]txn.Write) []txn.Write {
	result := make([]txn.Write, 0, len(values))
	for _, write := range values {
		result = append(result, write)
	}
	return result
}

func descriptorAnchor(descriptor RangeDescriptor) []byte {
	if descriptor.StartKey.Unbounded {
		return []byte{}
	}
	return bytes.Clone(descriptor.StartKey.Key)
}

func inSpan(key, start, end []byte) bool {
	return (start == nil || bytes.Compare(key, start) >= 0) && (end == nil || bytes.Compare(key, end) < 0)
}

func (r *Router) proposeOperation(ctx context.Context, descriptor RangeDescriptor, key []byte, commandType replicatedrange.CommandType, operation txn.Operation) error {
	if operation.Type == txn.OpCommit || operation.Type == txn.OpAbort {
		record, ok, err := r.recordStatus(ctx, descriptor, operation.ID)
		if err != nil {
			return err
		}
		if !ok || record.ReadTime != operation.ReadTime || record.CommitTime != operation.CommitTime || record.Epoch != operation.Epoch {
			return txn.ErrProtocolConflict
		}
		wanted := txn.StatusAborted
		if operation.Type == txn.OpCommit {
			wanted = txn.StatusCommitted
		}
		if record.Status.Terminal() && record.Status != wanted {
			if record.Status == txn.StatusCommitted {
				return txn.ErrAlreadyCommitted
			}
			return txn.ErrAlreadyAborted
		}
	}
	if operation.Type == txn.OpPrepare || operation.Type == txn.OpResolveCommit || operation.Type == txn.OpResolveAbort {
		home, err := r.catalog.LookupByID(RangeID(operation.Home.RangeID))
		if err != nil {
			return err
		}
		record, ok, err := r.recordStatus(ctx, home, operation.ID)
		if err != nil {
			return err
		}
		if !ok || record.ReadTime != operation.ReadTime || record.CommitTime != operation.CommitTime || record.Epoch != operation.Epoch {
			return txn.ErrProtocolConflict
		}
		if operation.Type == txn.OpPrepare && record.Status != txn.StatusPending {
			return txn.ErrProtocolConflict
		}
		if operation.Type == txn.OpResolveCommit && record.Status != txn.StatusCommitted {
			return txn.ErrProtocolConflict
		}
		if operation.Type == txn.OpResolveAbort && record.Status != txn.StatusAborted {
			return txn.ErrProtocolConflict
		}
	}
	encoded, err := txn.EncodeOperation(operation)
	if err != nil {
		return fmt.Errorf("encode transaction operation: %w", err)
	}
	return r.proposeTransaction(ctx, descriptor, key, replicatedrange.Command{Type: commandType, Key: key, Value: encoded, Timestamp: mvcc.Timestamp(operation.CommitTime)})
}

func (r *Router) proposeTransaction(ctx context.Context, descriptor RangeDescriptor, key []byte, command replicatedrange.Command) error {
	candidates := r.candidates(descriptor)
	nodes := r.scheduler.Nodes()
	for attempts := 0; attempts < min(r.maxAttempts, len(candidates)); attempts++ {
		nodeID := candidates[attempts]
		node := nodes[nodeID]
		if node == nil {
			continue
		}
		pending, outbound, err := node.ProposeTransaction(ctx, Route{RangeID: descriptor.RangeID, Generation: descriptor.Generation, Key: bytes.Clone(key)}, command)
		if err != nil {
			var notLeader *NotLeaderError
			if errors.As(err, &notLeader) {
				if notLeader.Leader != 0 {
					r.RecordLeader(descriptor.RangeID, notLeader.Leader)
				}
				continue
			}
			return err
		}
		if err := r.sendAndAwait(ctx, descriptor.RangeID, nodeID, pending, outbound); err != nil {
			return err
		}
		return nil
	}
	return ErrLeaderUnknown
}

func (r *Router) assignTransactionTimestamp(ctx context.Context, descriptor RangeDescriptor, key []byte, floor mvcc.Timestamp) (mvcc.Timestamp, error) {
	candidates := r.candidates(descriptor)
	nodes := r.scheduler.Nodes()
	for attempts := 0; attempts < min(r.maxAttempts, len(candidates)); attempts++ {
		nodeID := candidates[attempts]
		node := nodes[nodeID]
		if node == nil {
			continue
		}
		timestamp, pending, outbound, err := node.AssignTransactionTimestamp(ctx, Route{RangeID: descriptor.RangeID, Generation: descriptor.Generation, Key: bytes.Clone(key)}, floor)
		if err != nil {
			var notLeader *NotLeaderError
			if errors.As(err, &notLeader) {
				if notLeader.Leader != 0 {
					r.RecordLeader(descriptor.RangeID, notLeader.Leader)
				}
				continue
			}
			return 0, err
		}
		if err := r.sendAndAwait(ctx, descriptor.RangeID, nodeID, pending, outbound); err != nil {
			return 0, err
		}
		return timestamp, nil
	}
	return 0, ErrLeaderUnknown
}

func (r *Router) sendAndAwait(ctx context.Context, rangeID RangeID, nodeID raft.NodeID, pending *Pending, outbound []Envelope) error {
	for _, envelope := range outbound {
		if err := r.transport.Send(envelope); err != nil {
			return err
		}
	}
	for work := 0; work < r.maxWork; work++ {
		done, err := pending.poll()
		if done {
			if err == nil {
				r.RecordLeader(rangeID, nodeID)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for transaction proposal: %w", err)
		}
		delivered, schedulerErr := r.scheduler.DeliverNext()
		if schedulerErr == nil && !delivered {
			_, schedulerErr = r.scheduler.TickNext()
		}
		if schedulerErr != nil && !errors.Is(schedulerErr, ErrUnknownRange) && !errors.Is(schedulerErr, ErrNodeStopped) {
			return schedulerErr
		}
	}
	return fmt.Errorf("%w: transaction proposal exceeded bounded scheduler work", ErrLeaderUnknown)
}

func (r *Router) leaderReplica(ctx context.Context, descriptor RangeDescriptor) (*replicatedrange.Replica, error) {
	for work := 0; work < r.maxWork; work++ {
		for _, candidate := range r.candidates(descriptor) {
			node := r.scheduler.Nodes()[candidate]
			if node == nil {
				continue
			}
			replica, err := node.Replica(descriptor.RangeID)
			if err == nil && replica.Status().Raft.Role == raft.Leader {
				r.RecordLeader(descriptor.RangeID, candidate)
				return replica, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("find transaction leader: %w", err)
		}
		if _, err := r.scheduler.TickNext(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return nil, err
		}
		if _, err := r.scheduler.DeliverNext(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return nil, fmt.Errorf("deliver while finding transaction leader: %w", err)
		}
	}
	return nil, ErrLeaderUnknown
}

func (r *Router) immediateLeaderReplica(descriptor RangeDescriptor) (*replicatedrange.Replica, bool) {
	for _, candidate := range r.candidates(descriptor) {
		node := r.scheduler.Nodes()[candidate]
		if node == nil {
			continue
		}
		replica, err := node.Replica(descriptor.RangeID)
		if err == nil && replica.Status().Raft.Role == raft.Leader {
			return replica, true
		}
	}
	return nil, false
}

func (r *Router) recordStatus(ctx context.Context, descriptor RangeDescriptor, id txn.ID) (txn.Record, bool, error) {
	replica, err := r.leaderReplica(ctx, descriptor)
	if err != nil {
		return txn.Record{}, false, err
	}
	record, ok := replica.TransactionRecord(id)
	return record, ok, nil
}

func (r *Router) participantStatus(ctx context.Context, descriptor RangeDescriptor, id txn.ID) (txn.ParticipantRecord, bool, error) {
	replica, err := r.leaderReplica(ctx, descriptor)
	if err != nil {
		return txn.ParticipantRecord{}, false, err
	}
	record, ok := replica.ParticipantRecord(id)
	return record, ok, nil
}

func (r *Router) GetTransactionStatus(ctx context.Context, id txn.ID) (txn.Record, error) {
	for _, descriptor := range r.catalog.Snapshot().Ranges {
		record, ok, err := r.recordStatus(ctx, descriptor, id)
		if err != nil {
			return txn.Record{}, err
		}
		if ok {
			return record, nil
		}
	}
	return txn.Record{}, txn.ErrInvalid
}

func (r *Router) resolveRecord(ctx context.Context, record txn.Record) error {
	commandType, operationType := replicatedrange.CommandTxnResolveAbort, txn.OpResolveAbort
	if record.Status == txn.StatusCommitted {
		commandType, operationType = replicatedrange.CommandTxnResolveCommit, txn.OpResolveCommit
	}
	for _, participant := range record.Participants {
		descriptor, err := r.catalog.LookupByID(RangeID(participant.RangeID))
		if err != nil {
			return err
		}
		operation := txn.Operation{Type: operationType, ID: record.ID, ReadTime: record.ReadTime, CommitTime: record.CommitTime, Epoch: record.Epoch, Home: record.Home}
		if err := r.proposeOperation(ctx, descriptor, descriptorAnchor(descriptor), commandType, operation); err != nil {
			return err
		}
		r.observeTxn(TxnStageParticipantResolved, record.ID)
	}
	r.observeTxn(TxnStageAllResolved, record.ID)
	return nil
}

func (r *Router) observeTxn(stage TxnStage, id txn.ID) {
	if r.txnHook != nil {
		r.txnHook(stage, id)
	}
}

func (r *Router) transactionGetAt(ctx context.Context, key []byte, timestamp mvcc.Timestamp) ([]byte, error) {
	descriptor, err := r.catalog.Lookup(key)
	if err != nil {
		return nil, err
	}
	replica, err := r.leaderReplica(ctx, descriptor)
	if err != nil {
		return nil, err
	}
	entry, err := replica.GetMVCCAt(ctx, key, timestamp)
	if err != nil {
		return nil, fmt.Errorf("read transaction key: %w", err)
	}
	return r.interpretEntry(ctx, entry, timestamp)
}

func (r *Router) interpretEntry(ctx context.Context, entry engine.MVCCEntry, timestamp mvcc.Timestamp) ([]byte, error) {
	switch entry.Kind {
	case storage.KindDelete:
		return nil, engine.ErrNotFound
	case storage.KindValue:
		return bytes.Clone(entry.Value), nil
	case storage.KindIntent:
		intent, err := txn.DecodeIntent(entry.Value)
		if err != nil {
			return nil, errors.Join(engine.ErrCorruption, err)
		}
		descriptor, err := r.catalog.LookupByID(RangeID(intent.Home.RangeID))
		if err != nil {
			return nil, err
		}
		record, ok, err := r.recordStatus(ctx, descriptor, intent.ID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Join(engine.ErrCorruption, txn.ErrInvalid)
		}
		if record.Status == txn.StatusPending {
			return nil, txn.ErrIntentConflict
		}
		if record.Status == txn.StatusCommitted && record.CommitTime <= uint64(timestamp) {
			if intent.Delete {
				return nil, engine.ErrNotFound
			}
			return bytes.Clone(intent.Value), nil
		}
		if entry.Timestamp == 0 {
			return nil, engine.ErrNotFound
		}
		return r.transactionGetAt(ctx, entry.Key, min(timestamp, mvcc.Timestamp(entry.Timestamp-1)))
	default:
		return nil, engine.ErrUnresolvedIntent
	}
}

func (r *Router) transactionScanAt(ctx context.Context, start, end []byte, timestamp mvcc.Timestamp) ([]engine.KV, error) {
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, engine.ErrInvalidRange
	}
	var result []engine.KV
	for _, descriptor := range r.catalog.Snapshot().Ranges {
		spanStart, spanEnd, ok := intersectDescriptor(descriptor, start, end)
		if !ok {
			continue
		}
		replica, err := r.leaderReplica(ctx, descriptor)
		if err != nil {
			return nil, err
		}
		entries, err := replica.ScanMVCCAt(ctx, spanStart, spanEnd, timestamp)
		if err != nil {
			return nil, fmt.Errorf("scan transaction range: %w", err)
		}
		for _, entry := range entries {
			value, interpretErr := r.interpretEntry(ctx, entry, timestamp)
			if errors.Is(interpretErr, engine.ErrNotFound) {
				continue
			}
			if interpretErr != nil {
				return nil, interpretErr
			}
			result = append(result, engine.KV{Key: entry.Key, Value: value})
		}
	}
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left].Key, result[right].Key) < 0 })
	return result, nil
}

func intersectDescriptor(descriptor RangeDescriptor, start, end []byte) ([]byte, []byte, bool) {
	left, right := start, end
	if !descriptor.StartKey.Unbounded && (left == nil || bytes.Compare(left, descriptor.StartKey.Key) < 0) {
		left = descriptor.StartKey.Key
	}
	if !descriptor.EndKey.Unbounded && (right == nil || bytes.Compare(right, descriptor.EndKey.Key) > 0) {
		right = descriptor.EndKey.Key
	}
	if left != nil && right != nil && bytes.Compare(left, right) >= 0 {
		return nil, nil, false
	}
	return bytes.Clone(left), bytes.Clone(right), true
}

func (r *Router) RecoverTransactions(ctx context.Context) error {
	var records []txn.Record
	for _, descriptor := range r.catalog.Snapshot().Ranges {
		replica, err := r.leaderReplica(ctx, descriptor)
		if err != nil {
			return err
		}
		records = append(records, replica.TransactionRecords()...)
	}
	if len(records) > 4096 {
		return txn.ErrTooLarge
	}
	sort.Slice(records, func(left, right int) bool { return bytes.Compare(records[left].ID[:], records[right].ID[:]) < 0 })
	for _, record := range records {
		home, err := r.catalog.LookupByID(RangeID(record.Home.RangeID))
		if err != nil {
			return err
		}
		anchor := descriptorAnchor(home)
		if record.Status == txn.StatusPending {
			takeover := txn.Operation{Type: txn.OpTakeover, ID: record.ID, ReadTime: record.ReadTime, CommitTime: record.CommitTime, Epoch: record.Epoch + 1, Home: record.Home}
			if err := r.proposeOperation(ctx, home, anchor, replicatedrange.CommandTxnTakeover, takeover); err != nil {
				return err
			}
			record.Epoch++
			abort := txn.Operation{Type: txn.OpAbort, ID: record.ID, ReadTime: record.ReadTime, CommitTime: record.CommitTime, Epoch: record.Epoch, Home: record.Home}
			if err := r.proposeOperation(ctx, home, anchor, replicatedrange.CommandTxnAbort, abort); err != nil {
				return err
			}
			record.Status = txn.StatusAborted
		}
		if err := r.resolveRecord(ctx, record); err != nil {
			return err
		}
	}
	return nil
}
