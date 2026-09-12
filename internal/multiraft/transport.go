package multiraft

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type Envelope struct {
	RangeID               RangeID
	MessageRangeID        RangeID
	Generation            uint64
	DescriptorFingerprint [32]byte
	FromReplica           ReplicaID
	ToReplica             ReplicaID
	Message               raft.Message
}

type transportStats struct {
	Sent, Delivered, Dropped uint64
}

type nodeLink struct{ from, to raft.NodeID }
type rangeLink struct {
	rangeID  RangeID
	from, to raft.NodeID
}

// Transport is one bounded, range-aware queue shared by all groups.
type Transport struct {
	mu         sync.Mutex
	pending    []Envelope
	maximum    int
	lastRange  RangeID
	nodeLinks  map[nodeLink]bool
	rangeLinks map[rangeLink]bool
	stopped    bool
	stats      transportStats
}

func NewTransport(maximum int) (*Transport, error) {
	if maximum < 1 {
		return nil, ErrResourceLimit
	}
	return &Transport{maximum: maximum, nodeLinks: make(map[nodeLink]bool), rangeLinks: make(map[rangeLink]bool)}, nil
}

func (t *Transport) Send(envelope Envelope) error {
	if err := validateEnvelope(envelope); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return ErrTransportStopped
	}
	if blocked, exists := t.nodeLinks[nodeLink{envelope.Message.From, envelope.Message.To}]; exists && blocked {
		t.stats.Dropped++
		return nil
	}
	if blocked, exists := t.rangeLinks[rangeLink{envelope.RangeID, envelope.Message.From, envelope.Message.To}]; exists && blocked {
		t.stats.Dropped++
		return nil
	}
	if len(t.pending) >= t.maximum {
		return ErrResourceLimit
	}
	t.pending = append(t.pending, cloneEnvelope(envelope))
	t.stats.Sent++
	return nil
}

func (t *Transport) Take() (Envelope, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.pending) == 0 || t.stopped {
		return Envelope{}, false
	}
	position := 0
	for index := range t.pending {
		if t.pending[index].RangeID != t.lastRange {
			position = index
			break
		}
	}
	result := t.pending[position]
	copy(t.pending[position:], t.pending[position+1:])
	t.pending = t.pending[:len(t.pending)-1]
	t.lastRange = result.RangeID
	t.stats.Delivered++
	return cloneEnvelope(result), true
}

func (t *Transport) SetNodeLink(from, to raft.NodeID, blocked bool) {
	t.mu.Lock()
	t.nodeLinks[nodeLink{from, to}] = blocked
	t.mu.Unlock()
}

func (t *Transport) SetRangeLink(rangeID RangeID, from, to raft.NodeID, blocked bool) {
	t.mu.Lock()
	t.rangeLinks[rangeLink{rangeID, from, to}] = blocked
	t.mu.Unlock()
}

func (t *Transport) Heal() {
	t.mu.Lock()
	clear(t.nodeLinks)
	clear(t.rangeLinks)
	t.mu.Unlock()
}

func (t *Transport) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

func (t *Transport) Stats() transportStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stats
}

func (t *Transport) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.pending = nil
	t.mu.Unlock()
}

func validateEnvelope(envelope Envelope) error {
	if envelope.RangeID == 0 || envelope.MessageRangeID == 0 || envelope.Generation == 0 || envelope.DescriptorFingerprint == [32]byte{} || envelope.FromReplica == 0 || envelope.ToReplica == 0 {
		return ErrWrongRangeMessage
	}
	if envelope.RangeID != envelope.MessageRangeID {
		return ErrWrongRangeMessage
	}
	if envelope.Message.From == 0 || envelope.Message.To == 0 || envelope.Message.From == envelope.Message.To {
		return fmt.Errorf("%w: invalid Raft endpoints", ErrWrongRangeMessage)
	}
	return nil
}

func cloneEnvelope(envelope Envelope) Envelope {
	entries := make([]raft.Entry, len(envelope.Message.Entries))
	for index, entry := range envelope.Message.Entries {
		entries[index] = entry
		entries[index].Command = bytes.Clone(entry.Command)
	}
	envelope.Message.Entries = entries
	envelope.Message.Snapshot.Data = bytes.Clone(envelope.Message.Snapshot.Data)
	envelope.Message.Snapshot.Config.OldVoters = append([]raft.NodeID(nil), envelope.Message.Snapshot.Config.OldVoters...)
	envelope.Message.Snapshot.Config.NewVoters = append([]raft.NodeID(nil), envelope.Message.Snapshot.Config.NewVoters...)
	envelope.Message.Snapshot.Config.Learners = append([]raft.NodeID(nil), envelope.Message.Snapshot.Config.Learners...)
	return envelope
}
