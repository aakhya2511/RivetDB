package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const batchHeaderLen = 12

// Mutation is one operation in a write batch. Value is ignored for KindDelete
// and encoded, including when empty, for KindValue.
type Mutation struct {
	Key   []byte
	Value []byte
	Kind  ValueKind
}

// WriteBatch is the atomic logical payload stored in one WAL record. Mutation
// i owns sequence FirstSequence+uint64(i).
type WriteBatch struct {
	FirstSequence uint64
	Mutations     []Mutation
}

// EncodeWriteBatch returns the canonical little-endian/LEB128 batch encoding.
func EncodeWriteBatch(batch WriteBatch) ([]byte, error) {
	count := len(batch.Mutations)
	if count == 0 {
		return nil, ErrEmptyBatch
	}
	count64 := uint64(count) //nolint:gosec // slice length is nonnegative
	if count64 > math.MaxUint32 {
		return nil, ErrBatchTooLarge
	}
	if batch.FirstSequence > math.MaxUint64-(count64-1) {
		return nil, ErrSequenceOverflow
	}

	size := batchHeaderLen
	for i, mutation := range batch.Mutations {
		if !mutation.Kind.valid() {
			return nil, fmt.Errorf("mutation %d: %w: %d", i, ErrInvalidValueKind, mutation.Kind)
		}
		keyLength := uint64(len(mutation.Key)) //nolint:gosec // slice length is nonnegative
		keySize := 1 + uvarintLen(keyLength) + len(mutation.Key)
		if keySize > MaxRecordSize-size {
			return nil, ErrBatchTooLarge
		}
		size += keySize
		if mutation.Kind == KindValue {
			valueLength := uint64(len(mutation.Value)) //nolint:gosec // slice length is nonnegative
			valueSize := uvarintLen(valueLength) + len(mutation.Value)
			if valueSize > MaxRecordSize-size {
				return nil, ErrBatchTooLarge
			}
			size += valueSize
		}
	}

	encoded := make([]byte, batchHeaderLen, size)
	binary.LittleEndian.PutUint64(encoded[0:8], batch.FirstSequence)
	binary.LittleEndian.PutUint32(encoded[8:12], uint32(count)) //nolint:gosec // bounded above
	for _, mutation := range batch.Mutations {
		encoded = append(encoded, byte(mutation.Kind))
		encoded = binary.AppendUvarint(encoded, uint64(len(mutation.Key)))
		encoded = append(encoded, mutation.Key...)
		if mutation.Kind == KindValue {
			encoded = binary.AppendUvarint(encoded, uint64(len(mutation.Value)))
			encoded = append(encoded, mutation.Value...)
		}
	}
	return encoded, nil
}

// DecodeWriteBatch validates and decodes one complete canonical write batch.
func DecodeWriteBatch(encoded []byte) (WriteBatch, error) {
	if len(encoded) > MaxRecordSize {
		return WriteBatch{}, ErrBatchTooLarge
	}
	if len(encoded) < batchHeaderLen {
		return WriteBatch{}, fmt.Errorf("%w: header is truncated", ErrInvalidBatch)
	}

	first := binary.LittleEndian.Uint64(encoded[0:8])
	count := binary.LittleEndian.Uint32(encoded[8:12])
	if count == 0 {
		return WriteBatch{}, errors.Join(ErrInvalidBatch, ErrEmptyBatch)
	}
	if first > math.MaxUint64-uint64(count-1) {
		return WriteBatch{}, errors.Join(ErrInvalidBatch, ErrSequenceOverflow)
	}
	// Every mutation needs at least a kind and a key-length byte. Check before
	// allocating from the attacker-controlled count.
	remainingBytes := uint64(len(encoded) - batchHeaderLen) //nolint:gosec // nonnegative after header check
	if uint64(count) > remainingBytes/2 {
		return WriteBatch{}, fmt.Errorf("%w: mutation count exceeds remaining bytes", ErrInvalidBatch)
	}

	mutations := make([]Mutation, 0, int(count))
	offset := batchHeaderLen
	for i := uint32(0); i < count; i++ {
		if offset >= len(encoded) {
			return WriteBatch{}, fmt.Errorf("%w: mutation %d kind is truncated", ErrInvalidBatch, i)
		}
		kind := ValueKind(encoded[offset])
		offset++
		if !kind.valid() {
			return WriteBatch{}, fmt.Errorf("%w: mutation %d: %w: %d", ErrInvalidBatch, i, ErrInvalidValueKind, kind)
		}

		key, next, err := decodeLengthPrefixed(encoded, offset)
		if err != nil {
			return WriteBatch{}, fmt.Errorf("%w: mutation %d key: %w", ErrInvalidBatch, i, err)
		}
		offset = next
		mutation := Mutation{Kind: kind, Key: key}
		if kind == KindValue {
			value, valueNext, valueErr := decodeLengthPrefixed(encoded, offset)
			if valueErr != nil {
				return WriteBatch{}, fmt.Errorf("%w: mutation %d value: %w", ErrInvalidBatch, i, valueErr)
			}
			offset = valueNext
			mutation.Value = value
		}
		mutations = append(mutations, mutation)
	}
	if offset != len(encoded) {
		return WriteBatch{}, fmt.Errorf("%w: %d trailing bytes", ErrInvalidBatch, len(encoded)-offset)
	}

	return WriteBatch{FirstSequence: first, Mutations: mutations}, nil
}

func decodeLengthPrefixed(encoded []byte, offset int) ([]byte, int, error) {
	length, width := binary.Uvarint(encoded[offset:])
	if width == 0 {
		return nil, offset, errTruncatedLength
	}
	if width < 0 {
		return nil, offset, errOverflowLength
	}
	if width != uvarintLen(length) {
		return nil, offset, errNonCanonicalLength
	}
	start := offset + width
	remaining := len(encoded) - start
	remaining64 := uint64(remaining) //nolint:gosec // remaining is nonnegative
	if length > remaining64 {
		return nil, offset, errOutOfBoundsLength
	}
	lengthInt := int(length) //nolint:gosec // checked against an int-sized remaining slice
	end := start + lengthInt
	return append([]byte(nil), encoded[start:end]...), end, nil
}

func uvarintLen(value uint64) int {
	var scratch [binary.MaxVarintLen64]byte
	return binary.PutUvarint(scratch[:], value)
}
