// Package multiraft composes independent replicated ranges behind static
// ordered metadata and shared node-level scheduling and transport.
package multiraft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

type RangeID = replicatedrange.RangeID
type ReplicaID = replicatedrange.ReplicaID

// KeyBound is explicitly unbounded or contains a real user key. Unbounded in
// StartKey position means -infinity; in EndKey position it means +infinity.
type KeyBound struct {
	Unbounded bool
	Key       []byte
}

type ReplicaDescriptor struct {
	ReplicaID ReplicaID
	NodeID    raft.NodeID
}

type RangeDescriptor struct {
	RangeID    RangeID
	Generation uint64
	StartKey   KeyBound
	EndKey     KeyBound
	Replicas   []ReplicaDescriptor
}

func (d RangeDescriptor) Validate() error {
	if d.RangeID == 0 || d.Generation == 0 || len(d.Replicas) == 0 {
		return ErrInvalidDescriptor
	}
	if d.StartKey.Unbounded && len(d.StartKey.Key) != 0 || d.EndKey.Unbounded && len(d.EndKey.Key) != 0 {
		return fmt.Errorf("%w: unbounded endpoint contains bytes", ErrInvalidDescriptor)
	}
	if len(d.StartKey.Key) > sstable.MaxUserKeySize || len(d.EndKey.Key) > sstable.MaxUserKeySize {
		return fmt.Errorf("%w: endpoint exceeds key bound", ErrInvalidDescriptor)
	}
	if !d.StartKey.Unbounded && !d.EndKey.Unbounded && bytes.Compare(d.StartKey.Key, d.EndKey.Key) >= 0 {
		return fmt.Errorf("%w: empty or reversed interval", ErrInvalidDescriptor)
	}
	replicaIDs := make(map[ReplicaID]struct{}, len(d.Replicas))
	nodeIDs := make(map[raft.NodeID]struct{}, len(d.Replicas))
	for _, replica := range d.Replicas {
		if replica.ReplicaID == 0 || replica.NodeID == 0 {
			return fmt.Errorf("%w: zero replica identity", ErrInvalidDescriptor)
		}
		if _, exists := replicaIDs[replica.ReplicaID]; exists {
			return fmt.Errorf("%w: duplicate replica %d", ErrInvalidDescriptor, replica.ReplicaID)
		}
		if _, exists := nodeIDs[replica.NodeID]; exists {
			return fmt.Errorf("%w: duplicate node %d", ErrInvalidDescriptor, replica.NodeID)
		}
		replicaIDs[replica.ReplicaID] = struct{}{}
		nodeIDs[replica.NodeID] = struct{}{}
	}
	return nil
}

func (d RangeDescriptor) Contains(key []byte) bool {
	return (d.StartKey.Unbounded || bytes.Compare(key, d.StartKey.Key) >= 0) &&
		(d.EndKey.Unbounded || bytes.Compare(key, d.EndKey.Key) < 0)
}

func (d RangeDescriptor) ContainsSpan(start, end []byte) bool {
	if start != nil && !d.Contains(start) {
		return false
	}
	if end != nil {
		if !d.EndKey.Unbounded && bytes.Compare(end, d.EndKey.Key) > 0 {
			return false
		}
		if !d.StartKey.Unbounded && bytes.Compare(end, d.StartKey.Key) <= 0 {
			return false
		}
	}
	return start == nil || end == nil || bytes.Compare(start, end) <= 0
}

func (d RangeDescriptor) ReplicaOn(nodeID raft.NodeID) (ReplicaDescriptor, bool) {
	for _, replica := range d.Replicas {
		if replica.NodeID == nodeID {
			return replica, true
		}
	}
	return ReplicaDescriptor{}, false
}

func cloneDescriptor(d RangeDescriptor) RangeDescriptor {
	d.StartKey.Key = bytes.Clone(d.StartKey.Key)
	d.EndKey.Key = bytes.Clone(d.EndKey.Key)
	d.Replicas = slices.Clone(d.Replicas)
	return d
}

func sameDescriptor(left, right RangeDescriptor) bool {
	if left.RangeID != right.RangeID || left.Generation != right.Generation ||
		left.StartKey.Unbounded != right.StartKey.Unbounded || left.EndKey.Unbounded != right.EndKey.Unbounded ||
		!bytes.Equal(left.StartKey.Key, right.StartKey.Key) || !bytes.Equal(left.EndKey.Key, right.EndKey.Key) ||
		len(left.Replicas) != len(right.Replicas) {
		return false
	}
	return slices.Equal(left.Replicas, right.Replicas)
}

func descriptorFingerprint(descriptor RangeDescriptor) [sha256.Size]byte {
	hash := sha256.New()
	var field [8]byte
	binary.LittleEndian.PutUint64(field[:], uint64(descriptor.RangeID))
	_, _ = hash.Write(field[:])
	binary.LittleEndian.PutUint64(field[:], descriptor.Generation)
	_, _ = hash.Write(field[:])
	if descriptor.StartKey.Unbounded {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	binary.LittleEndian.PutUint64(field[:], uint64(len(descriptor.StartKey.Key)))
	_, _ = hash.Write(field[:])
	_, _ = hash.Write(descriptor.StartKey.Key)
	if descriptor.EndKey.Unbounded {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	binary.LittleEndian.PutUint64(field[:], uint64(len(descriptor.EndKey.Key)))
	_, _ = hash.Write(field[:])
	_, _ = hash.Write(descriptor.EndKey.Key)
	for _, replica := range descriptor.Replicas {
		binary.LittleEndian.PutUint64(field[:], uint64(replica.ReplicaID))
		_, _ = hash.Write(field[:])
		binary.LittleEndian.PutUint64(field[:], uint64(replica.NodeID))
		_, _ = hash.Write(field[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
