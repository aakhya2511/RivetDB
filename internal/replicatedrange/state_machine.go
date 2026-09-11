package replicatedrange

import (
	"context"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

var ErrSnapshotDeferred = errors.New("replicated range: LSM-integrated Raft snapshots are deferred")

type stateMachine struct {
	engine *engine.Engine
	hook   Hook
}

func (m *stateMachine) Apply(entry raft.Entry) error {
	if m.hook != nil {
		m.hook(StageRaftCommitted, entry.Index)
	}
	command, err := DecodeCommand(entry.Command)
	if err != nil {
		return fmt.Errorf("decode committed command: %w", err)
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
	if err := m.engine.ApplyCommitted(context.Background(), entry.Index, entry.Term, entry.Command, mutation); err != nil && !errors.Is(err, engine.ErrAlreadyApplied) {
		return fmt.Errorf("apply committed command to LSM: %w", err)
	}
	return nil
}

func (*stateMachine) Snapshot() ([]byte, error) { return nil, ErrSnapshotDeferred }

func (*stateMachine) Restore([]byte) error { return ErrSnapshotDeferred }

var _ raft.StateMachine = (*stateMachine)(nil)
