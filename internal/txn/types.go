// Package txn defines the canonical durable transaction protocol values shared
// by replicated ranges and the Multi-Raft coordinator.
package txn

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
)

const (
	IDSize          = 16
	MaxWrites       = 1024
	MaxParticipants = 64
	MaxWriteBytes   = 8 << 20
)

var (
	ErrInvalid          = errors.New("transaction: invalid protocol value")
	ErrTooLarge         = errors.New("transaction: resource limit exceeded")
	ErrProtocolConflict = errors.New("transaction: conflicting protocol retry")
	ErrWriteConflict    = errors.New("transaction: write conflict")
	ErrIntentConflict   = errors.New("transaction: unresolved intent conflict")
	ErrAlreadyCommitted = errors.New("transaction: already committed")
	ErrAlreadyAborted   = errors.New("transaction: already aborted")
	ErrClosed           = errors.New("transaction: closed")
	ErrReadOnly         = errors.New("transaction: read-only")
)

// ID is the stable, opaque transaction identity.
type ID [IDSize]byte

func NewID() (ID, error) {
	var id ID
	if _, err := rand.Read(id[:]); err != nil {
		return ID{}, fmt.Errorf("generate transaction ID: %w", err)
	}
	return id, nil
}

func (id ID) IsZero() bool   { return id == ID{} }
func (id ID) String() string { return fmt.Sprintf("%x", id[:]) }

type Status uint8

const (
	StatusPending Status = iota + 1
	StatusCommitted
	StatusAborted
)

func (s Status) Valid() bool {
	return s == StatusPending || s == StatusCommitted || s == StatusAborted
}

func (s Status) Terminal() bool { return s == StatusCommitted || s == StatusAborted }

type ParticipantStatus uint8

const (
	ParticipantPrepared ParticipantStatus = iota + 1
	ParticipantRejected
	ParticipantCommitted
	ParticipantAborted
)

func (s ParticipantStatus) Valid() bool {
	return s >= ParticipantPrepared && s <= ParticipantAborted
}

type Participant struct {
	RangeID    uint64
	Generation uint64
}

type Write struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type Record struct {
	ID           ID
	Status       Status
	ReadTime     uint64
	CommitTime   uint64
	Epoch        uint64
	Home         Participant
	Participants []Participant
}

type ParticipantRecord struct {
	ID         ID
	Status     ParticipantStatus
	ReadTime   uint64
	CommitTime uint64
	Epoch      uint64
	Home       Participant
	Writes     []Write
	Reason     string
}

type OperationType uint8

const (
	OpCreate OperationType = iota + 1
	OpPrepare
	OpCommit
	OpAbort
	OpTakeover
	OpResolveCommit
	OpResolveAbort
)

type Operation struct {
	Type         OperationType
	ID           ID
	ReadTime     uint64
	CommitTime   uint64
	Epoch        uint64
	Home         Participant
	Participants []Participant
	Writes       []Write
}

type Intent struct {
	ID         ID
	ReadTime   uint64
	CommitTime uint64
	Epoch      uint64
	Home       Participant
	Delete     bool
	Value      []byte
}

func CloneRecord(record Record) Record {
	record.Participants = append([]Participant(nil), record.Participants...)
	return record
}

func CloneParticipant(record ParticipantRecord) ParticipantRecord {
	record.Writes = CloneWrites(record.Writes)
	return record
}

func CloneWrites(writes []Write) []Write {
	result := make([]Write, len(writes))
	for index, write := range writes {
		result[index] = Write{Key: bytes.Clone(write.Key), Value: bytes.Clone(write.Value), Delete: write.Delete}
	}
	return result
}

func SortWrites(writes []Write) {
	sort.Slice(writes, func(left, right int) bool { return bytes.Compare(writes[left].Key, writes[right].Key) < 0 })
}

func SortParticipants(participants []Participant) {
	sort.Slice(participants, func(left, right int) bool {
		if participants[left].RangeID != participants[right].RangeID {
			return participants[left].RangeID < participants[right].RangeID
		}
		return participants[left].Generation < participants[right].Generation
	})
}

func EqualWrites(left, right []Write) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Delete != right[index].Delete || !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}

func EqualRecord(left, right Record) bool {
	if left.ID != right.ID || left.Status != right.Status || left.ReadTime != right.ReadTime || left.CommitTime != right.CommitTime || left.Epoch != right.Epoch || left.Home != right.Home || len(left.Participants) != len(right.Participants) {
		return false
	}
	for index := range left.Participants {
		if left.Participants[index] != right.Participants[index] {
			return false
		}
	}
	return true
}
