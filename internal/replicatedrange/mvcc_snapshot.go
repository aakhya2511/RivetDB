package replicatedrange

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

// GetAt reads local applied MVCC state. It certifies timestamp visibility, not
// consensus freshness or linearizability.
func (r *Replica) GetAt(ctx context.Context, key []byte, timestamp mvcc.Timestamp) ([]byte, error) {
	if err := r.checkHistoricalRead(key, timestamp); err != nil {
		return nil, err
	}
	value, err := r.engine.GetAt(ctx, key, uint64(timestamp))
	if err != nil {
		return nil, fmt.Errorf("local MVCC get: %w", err)
	}
	return value, nil
}

func (r *Replica) ScanAt(ctx context.Context, start, end []byte, timestamp mvcc.Timestamp) ([]engine.KV, error) {
	if r.containsSpan != nil && !r.containsSpan(start, end) {
		return nil, ErrKeyOutOfRange
	}
	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, engine.ErrInvalidRange
	}
	if err := r.checkHistoricalRead(nil, timestamp); err != nil {
		return nil, err
	}
	values, err := r.engine.ScanAt(ctx, start, end, uint64(timestamp))
	if err != nil {
		return nil, fmt.Errorf("local MVCC scan: %w", err)
	}
	for _, value := range values {
		if r.containsKey != nil && !r.containsKey(value.Key) {
			return nil, ErrKeyOutOfRange
		}
	}
	return values, nil
}

func (r *Replica) checkHistoricalRead(key []byte, timestamp mvcc.Timestamp) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.mvcc {
		return ErrInvalidOptions
	}
	if r.stopped {
		return ErrStopped
	}
	if fatal := r.node.Status().Fatal; fatal != nil {
		return fatal
	}
	if key != nil && r.containsKey != nil && !r.containsKey(key) {
		return ErrKeyOutOfRange
	}
	if timestamp > r.machine.maxApplied {
		return ErrReplicaBehind
	}
	return nil
}

func (r *Replica) DigestAt(ctx context.Context, timestamp mvcc.Timestamp) ([sha256.Size]byte, error) {
	values, err := r.ScanAt(ctx, nil, nil, timestamp)
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
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

type Snapshot struct {
	mu         sync.Mutex
	replica    *Replica
	id         uint64
	rangeID    RangeID
	generation uint64
	timestamp  mvcc.Timestamp
	closed     bool
}

func (r *Replica) NewSnapshot() (*Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.newSnapshotLocked(r.machine.maxApplied)
}

func (r *Replica) SnapshotAt(timestamp mvcc.Timestamp) (*Snapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if timestamp > r.machine.maxApplied {
		return nil, ErrReplicaBehind
	}
	return r.newSnapshotLocked(timestamp)
}

func (r *Replica) newSnapshotLocked(timestamp mvcc.Timestamp) (*Snapshot, error) {
	if !r.mvcc {
		return nil, ErrInvalidOptions
	}
	if r.stopped {
		return nil, ErrStopped
	}
	if r.node.Status().Fatal != nil {
		return nil, r.node.Status().Fatal
	}
	r.nextSnapshot++
	if r.nextSnapshot == 0 {
		return nil, ErrInvalidOptions
	}
	id := r.nextSnapshot
	r.snapshots[id] = timestamp
	return &Snapshot{replica: r, id: id, rangeID: r.rangeID, generation: r.generation, timestamp: timestamp}, nil
}

func (s *Snapshot) Timestamp() mvcc.Timestamp { return s.timestamp }
func (s *Snapshot) RangeID() RangeID          { return s.rangeID }
func (s *Snapshot) Generation() uint64        { return s.generation }

func (s *Snapshot) Get(ctx context.Context, key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrSnapshotClosed
	}
	return s.replica.GetAt(ctx, key, s.timestamp)
}

func (s *Snapshot) Scan(ctx context.Context, start, end []byte) ([]engine.KV, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrSnapshotClosed
	}
	return s.replica.ScanAt(ctx, start, end, s.timestamp)
}

func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.replica.mu.Lock()
	delete(s.replica.snapshots, s.id)
	s.replica.mu.Unlock()
	s.closed = true
	return nil
}

func (r *Replica) OldestSnapshotTimestamp() (mvcc.Timestamp, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var oldest mvcc.Timestamp
	first := true
	for _, timestamp := range r.snapshots {
		if first || timestamp < oldest {
			oldest, first = timestamp, false
		}
	}
	return oldest, !first
}
