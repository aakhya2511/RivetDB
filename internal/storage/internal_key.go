// Package storage defines contracts for RivetDB's local ordered key-value storage.
//
// Phase 1 has not begun. This file defines only the internal-key contract that
// every sorted Phase 1 component will use.
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const internalKeyTrailerLen = 9

var (
	// ErrInternalKeyTooShort means an encoded internal key has no complete
	// sequence-and-kind trailer.
	ErrInternalKeyTooShort = errors.New("internal key is shorter than its trailer")
	// ErrInvalidValueKind means an encoded or constructed key uses a kind that
	// is not part of the on-disk format.
	ErrInvalidValueKind = errors.New("invalid internal-key value kind")
)

// ValueKind distinguishes a stored value from a deletion tombstone.
type ValueKind uint8

const (
	// KindDelete is a deletion tombstone. It sorts before KindValue when the
	// user key and sequence are identical.
	KindDelete ValueKind = iota
	// KindValue is a stored value.
	KindValue
)

// InternalKey is the logical key used by MemTables and SSTables. Its fields
// are private so every value is validated and owns its user-key bytes.
type InternalKey struct {
	userKey  []byte
	sequence uint64
	kind     ValueKind
}

// NewInternalKey constructs a validated internal key and copies userKey.
func NewInternalKey(userKey []byte, sequence uint64, kind ValueKind) (InternalKey, error) {
	if !kind.valid() {
		return InternalKey{}, fmt.Errorf("%w: %d", ErrInvalidValueKind, kind)
	}
	return InternalKey{
		userKey:  bytes.Clone(userKey),
		sequence: sequence,
		kind:     kind,
	}, nil
}

// DecodeInternalKey decodes user_key || BE64(^sequence) || kind and validates
// the kind. The user-key prefix may contain arbitrary bytes, including zero.
func DecodeInternalKey(encoded []byte) (InternalKey, error) {
	if len(encoded) < internalKeyTrailerLen {
		return InternalKey{}, ErrInternalKeyTooShort
	}

	trailer := len(encoded) - internalKeyTrailerLen
	kind := ValueKind(encoded[len(encoded)-1])
	if !kind.valid() {
		return InternalKey{}, fmt.Errorf("%w: %d", ErrInvalidValueKind, kind)
	}

	return InternalKey{
		userKey:  bytes.Clone(encoded[:trailer]),
		sequence: ^binary.BigEndian.Uint64(encoded[trailer : trailer+8]),
		kind:     kind,
	}, nil
}

// Encode returns user_key || BE64(^sequence) || kind. The complemented,
// big-endian sequence preserves newest-first ordering for equal user keys, but
// callers must use CompareInternal rather than comparing these bytes directly:
// the encoding is self-parsing, not globally order-preserving.
func (k InternalKey) Encode() []byte {
	encoded := make([]byte, len(k.userKey)+internalKeyTrailerLen)
	copy(encoded, k.userKey)
	binary.BigEndian.PutUint64(encoded[len(k.userKey):], ^k.sequence)
	encoded[len(encoded)-1] = byte(k.kind)
	return encoded
}

// UserKey returns a copy of the logical user key.
func (k InternalKey) UserKey() []byte {
	return bytes.Clone(k.userKey)
}

// Sequence returns the key's sequence number.
func (k InternalKey) Sequence() uint64 {
	return k.sequence
}

// Kind returns the key's value kind.
func (k InternalKey) Kind() ValueKind {
	return k.kind
}

// CompareInternal defines the single ordering used by every sorted storage
// component: user key ascending, sequence descending, then kind ascending.
func CompareInternal(a, b InternalKey) int {
	if userOrder := bytes.Compare(a.userKey, b.userKey); userOrder != 0 {
		return userOrder
	}
	if a.sequence > b.sequence {
		return -1
	}
	if a.sequence < b.sequence {
		return 1
	}
	if a.kind < b.kind {
		return -1
	}
	if a.kind > b.kind {
		return 1
	}
	return 0
}

func (k ValueKind) valid() bool {
	return k == KindDelete || k == KindValue
}
