package replicatedrange

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/txn"
)

var ErrSnapshotDeferred = errors.New("replicated range: LSM-integrated Raft snapshots are deferred")

type stateMachine struct {
	engine       *engine.Engine
	hook         Hook
	containsKey  func([]byte) bool
	mvcc         bool
	maxApplied   mvcc.Timestamp
	safeRead     mvcc.Timestamp
	clock        *mvcc.Clock
	rangeID      RangeID
	generation   uint64
	records      map[txn.ID]txn.Record
	participants map[txn.ID]txn.ParticipantRecord
}

func (m *stateMachine) Apply(entry raft.Entry) error {
	if m.hook != nil {
		m.hook(StageRaftCommitted, entry.Index)
	}
	command, err := DecodeCommand(entry.Command)
	if err != nil {
		return fmt.Errorf("decode committed command: %w", err)
	}
	if m.containsKey != nil && !m.containsKey(command.Key) {
		return ErrKeyOutOfRange
	}
	if m.mvcc != (command.Timestamp != 0) {
		return ErrInvalidCommand
	}
	frontier, err := m.engine.DurableAppliedRaftIndex()
	if err != nil {
		return fmt.Errorf("read durable apply frontier: %w", err)
	}
	alreadyDurable := entry.Index <= frontier
	if command.Type >= CommandTxnBarrier {
		return m.applyTransaction(entry, command, alreadyDurable)
	}
	if m.mvcc && !alreadyDurable && command.Timestamp <= m.maxApplied {
		return ErrMVCCRegression
	}
	mutation := storage.Mutation{Key: command.Key, Value: command.Value}
	switch command.Type {
	case CommandPut:
		mutation.Kind = storage.KindValue
	case CommandDelete:
		mutation.Kind = storage.KindDelete
	default:
		return ErrInvalidCommand
	}
	var applyErr error
	if m.mvcc {
		applyErr = m.engine.ApplyCommittedMVCC(context.Background(), entry.Index, entry.Term, uint64(command.Timestamp), entry.Command, mutation)
	} else {
		applyErr = m.engine.ApplyCommitted(context.Background(), entry.Index, entry.Term, entry.Command, mutation)
	}
	if applyErr != nil && !errors.Is(applyErr, engine.ErrAlreadyApplied) {
		return fmt.Errorf("apply committed command to LSM: %w", applyErr)
	}
	if m.mvcc && !alreadyDurable {
		m.maxApplied = command.Timestamp
		m.clock.Observe(command.Timestamp)
	}
	return nil
}

func (m *stateMachine) applyTransaction(entry raft.Entry, command Command, alreadyDurable bool) error {
	m.clock.Observe(command.Timestamp)
	if command.Type == CommandTxnBarrier {
		m.safeRead = max(m.safeRead, command.Timestamp)
		return m.advanceMetadata(entry.Index, alreadyDurable)
	}
	operation, err := txn.DecodeOperation(command.Value)
	if err != nil {
		return fmt.Errorf("decode committed transaction operation: %w", err)
	}
	switch command.Type {
	case CommandTxnCreate:
		err = m.applyCreate(operation)
	case CommandTxnPrepare:
		err = m.applyPrepare(entry, operation, alreadyDurable)
	case CommandTxnCommit:
		err = m.applyDecision(operation, txn.StatusCommitted)
	case CommandTxnAbort:
		err = m.applyDecision(operation, txn.StatusAborted)
	case CommandTxnTakeover:
		err = m.applyTakeover(operation)
	case CommandTxnResolveCommit:
		err = m.applyResolution(entry, operation, txn.ParticipantCommitted, alreadyDurable)
	case CommandTxnResolveAbort:
		err = m.applyResolution(entry, operation, txn.ParticipantAborted, alreadyDurable)
	default:
		err = ErrInvalidCommand
	}
	if err != nil {
		return err
	}
	if command.Type == CommandTxnPrepare || command.Type == CommandTxnResolveCommit || command.Type == CommandTxnResolveAbort {
		return nil
	}
	return m.advanceMetadata(entry.Index, alreadyDurable)
}

func (m *stateMachine) applyCreate(operation txn.Operation) error {
	if operation.Home.RangeID != uint64(m.rangeID) || operation.Home.Generation != m.generation {
		return ErrKeyOutOfRange
	}
	want := txn.Record{ID: operation.ID, Status: txn.StatusPending, ReadTime: operation.ReadTime,
		CommitTime: operation.CommitTime, Epoch: operation.Epoch, Home: operation.Home,
		Participants: append([]txn.Participant(nil), operation.Participants...)}
	if existing, ok := m.records[operation.ID]; ok {
		if txn.EqualRecord(existing, want) {
			return nil
		}
		return txn.ErrProtocolConflict
	}
	m.records[operation.ID] = want
	return nil
}

func (m *stateMachine) applyDecision(operation txn.Operation, status txn.Status) error {
	record, ok := m.records[operation.ID]
	if !ok || record.Home != operation.Home || record.ReadTime != operation.ReadTime || record.CommitTime != operation.CommitTime {
		return txn.ErrProtocolConflict
	}
	if operation.Epoch < record.Epoch {
		return nil
	}
	if operation.Epoch != record.Epoch {
		return txn.ErrProtocolConflict
	}
	if record.Status == status {
		return nil
	}
	if record.Status.Terminal() {
		return txn.ErrProtocolConflict
	}
	record.Status = status
	m.records[operation.ID] = record
	return nil
}

func (m *stateMachine) applyTakeover(operation txn.Operation) error {
	record, ok := m.records[operation.ID]
	if !ok || record.Status != txn.StatusPending || record.Home != operation.Home || record.ReadTime != operation.ReadTime || record.CommitTime != operation.CommitTime {
		return txn.ErrProtocolConflict
	}
	if operation.Epoch == record.Epoch {
		return nil
	}
	if operation.Epoch != record.Epoch+1 {
		return nil
	}
	record.Epoch = operation.Epoch
	m.records[operation.ID] = record
	return nil
}

func (m *stateMachine) applyPrepare(entry raft.Entry, operation txn.Operation, alreadyDurable bool) error {
	for _, write := range operation.Writes {
		if m.containsKey != nil && !m.containsKey(write.Key) {
			return ErrKeyOutOfRange
		}
	}
	if existing, ok := m.participants[operation.ID]; ok {
		if (existing.Status == txn.ParticipantCommitted || existing.Status == txn.ParticipantAborted) && operation.Epoch <= existing.Epoch {
			return m.advanceMetadata(entry.Index, alreadyDurable)
		}
		same := existing.ReadTime == operation.ReadTime && existing.CommitTime == operation.CommitTime &&
			existing.Epoch == operation.Epoch && existing.Home == operation.Home && txn.EqualWrites(existing.Writes, operation.Writes)
		if same {
			return m.advanceMetadata(entry.Index, alreadyDurable)
		}
		return txn.ErrProtocolConflict
	}
	participant := txn.ParticipantRecord{ID: operation.ID, Status: txn.ParticipantPrepared,
		ReadTime: operation.ReadTime, CommitTime: operation.CommitTime, Epoch: operation.Epoch,
		Home: operation.Home, Writes: txn.CloneWrites(operation.Writes)}
	if !alreadyDurable {
		for _, write := range operation.Writes {
			selected, readErr := m.engine.GetMVCCAt(context.Background(), write.Key, math.MaxUint64)
			if readErr != nil && !errors.Is(readErr, engine.ErrNotFound) {
				return fmt.Errorf("inspect prepare conflict: %w", readErr)
			}
			if readErr == nil && (selected.Kind == storage.KindIntent || selected.Timestamp > operation.ReadTime) {
				participant.Status, participant.Reason = txn.ParticipantRejected, "newer committed write or foreign intent"
				m.participants[operation.ID] = participant
				return m.advanceMetadata(entry.Index, false)
			}
		}
		mutations := make([]storage.Mutation, len(operation.Writes))
		for index, write := range operation.Writes {
			value, encodeErr := txn.EncodeIntent(txn.Intent{ID: operation.ID, ReadTime: operation.ReadTime,
				CommitTime: operation.CommitTime, Epoch: operation.Epoch, Home: operation.Home,
				Delete: write.Delete, Value: write.Value})
			if encodeErr != nil {
				return fmt.Errorf("encode committed intent: %w", encodeErr)
			}
			mutations[index] = storage.Mutation{Key: write.Key, Value: value, Kind: storage.KindIntent}
		}
		if err := m.engine.ApplyPreparedMVCCBatch(context.Background(), entry.Index, entry.Term, operation.CommitTime, entry.Command, mutations); err != nil && !errors.Is(err, engine.ErrAlreadyApplied) {
			return fmt.Errorf("materialize prepared intents: %w", err)
		}
		m.maxApplied = max(m.maxApplied, mvcc.Timestamp(operation.CommitTime))
	}
	m.participants[operation.ID] = participant
	return nil
}

func (m *stateMachine) applyResolution(entry raft.Entry, operation txn.Operation, status txn.ParticipantStatus, alreadyDurable bool) error {
	participant, ok := m.participants[operation.ID]
	if !ok {
		m.participants[operation.ID] = txn.ParticipantRecord{ID: operation.ID, Status: status,
			ReadTime: operation.ReadTime, CommitTime: operation.CommitTime, Epoch: operation.Epoch, Home: operation.Home}
		return m.advanceMetadata(entry.Index, alreadyDurable)
	}
	if participant.ReadTime != operation.ReadTime || participant.CommitTime != operation.CommitTime || participant.Home != operation.Home {
		return txn.ErrProtocolConflict
	}
	if operation.Epoch < participant.Epoch {
		return m.advanceMetadata(entry.Index, alreadyDurable)
	}
	if participant.Status == status {
		return m.advanceMetadata(entry.Index, alreadyDurable)
	}
	if participant.Status == txn.ParticipantCommitted || participant.Status == txn.ParticipantAborted {
		return txn.ErrProtocolConflict
	}
	if participant.Status == txn.ParticipantRejected {
		participant.Status, participant.Epoch = status, operation.Epoch
		m.participants[operation.ID] = participant
		return m.advanceMetadata(entry.Index, alreadyDurable)
	}
	if !alreadyDurable {
		mutations := make([]storage.Mutation, len(participant.Writes))
		for index, write := range participant.Writes {
			kind := storage.KindTxnAbort
			var value []byte
			if status == txn.ParticipantCommitted {
				kind = storage.KindValue
				value = write.Value
				if write.Delete {
					kind = storage.KindDelete
					value = nil
				}
			}
			mutations[index] = storage.Mutation{Key: write.Key, Value: value, Kind: kind}
		}
		if err := m.engine.ResolveCommittedMVCCBatch(context.Background(), entry.Index, entry.Term, participant.CommitTime, entry.Command, mutations); err != nil && !errors.Is(err, engine.ErrAlreadyApplied) {
			return fmt.Errorf("materialize transaction resolution: %w", err)
		}
	}
	participant.Status, participant.Epoch = status, operation.Epoch
	m.participants[operation.ID] = participant
	return nil
}

func (m *stateMachine) advanceMetadata(index uint64, alreadyDurable bool) error {
	if alreadyDurable {
		return nil
	}
	if err := m.engine.AdvanceApplied(index); err != nil {
		return fmt.Errorf("advance transaction metadata apply: %w", err)
	}
	return nil
}

func (*stateMachine) Snapshot() ([]byte, error) { return nil, ErrSnapshotDeferred }
func (*stateMachine) Restore([]byte) error      { return ErrSnapshotDeferred }

var _ raft.StateMachine = (*stateMachine)(nil)
