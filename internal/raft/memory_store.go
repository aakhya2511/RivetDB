package raft

import (
	"fmt"
	"sync"
)

// MemoryStore is a restartable, non-durable Raft Store for protocol tests.
type MemoryStore struct {
	mu       sync.Mutex
	state    PersistentState
	saveErr  error
	saveHook func(PersistentState) error
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func NewMemoryStoreFrom(state PersistentState) (*MemoryStore, error) {
	if err := validatePersistent(state); err != nil {
		return nil, err
	}
	return &MemoryStore{state: clonePersistent(state)}, nil
}

func (s *MemoryStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePersistent(s.state), nil
}

func (s *MemoryStore) Save(state PersistentState) error {
	if err := validatePersistent(state); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.saveHook != nil {
		if err := s.saveHook(clonePersistent(state)); err != nil {
			return fmt.Errorf("memory store hook: %w", err)
		}
	}
	if s.saveErr != nil {
		return fmt.Errorf("memory store save: %w", s.saveErr)
	}
	s.state = clonePersistent(state)
	return nil
}

// SetSaveError injects a persistent Save failure. It is intended for tests.
func (s *MemoryStore) SetSaveError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveErr = err
}

// SetSaveHook installs a test observer/failure injector.
func (s *MemoryStore) SetSaveHook(hook func(PersistentState) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveHook = hook
}
