package replicatedrange

import (
	"context"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

var ErrSnapshotDeferred = errors.New("replicated range: LSM-integrated Raft snapshots are deferred")

type stateMachine struct {
	engine      *engine.Engine
	hook        Hook
	containsKey func([]byte) bool
	mvcc        bool
	maxApplied  mvcc.Timestamp
	clock       *mvcc.Clock
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
	alreadyDurable := false
	if m.mvcc {
		frontier, frontierErr := m.engine.DurableAppliedRaftIndex()
		if frontierErr != nil {
			return fmt.Errorf("read durable apply frontier: %w", frontierErr)
		}
		alreadyDurable = entry.Index <= frontier
		if !alreadyDurable && command.Timestamp <= m.maxApplied {
			return ErrMVCCRegression
		}
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

func (*stateMachine) Snapshot() ([]byte, error) { return nil, ErrSnapshotDeferred }

func (*stateMachine) Restore([]byte) error { return ErrSnapshotDeferred }

var _ raft.StateMachine = (*stateMachine)(nil)
