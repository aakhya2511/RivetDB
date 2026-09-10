package sstable

import (
	"encoding/binary"
)

type dataBlockBuilder struct {
	buffer          []byte
	restarts        []uint32
	previousKey     []byte
	restartInterval int
	entries         int
}

func newDataBlockBuilder(restartInterval int) *dataBlockBuilder {
	return &dataBlockBuilder{restartInterval: restartInterval}
}

func (b *dataBlockBuilder) empty() bool {
	return b.entries == 0
}

func (b *dataBlockBuilder) projectedSize(key, value []byte) int {
	restart := b.entries%b.restartInterval == 0
	shared := 0
	if !restart {
		shared = commonPrefix(b.previousKey, key)
	}
	unshared := len(key) - shared
	restartCount := len(b.restarts)
	if restart {
		restartCount++
	}
	return len(b.buffer) + uvarintLen(uint64(shared)) + //nolint:gosec // slice-derived non-negative length
		uvarintLen(uint64(unshared)) + //nolint:gosec // slice-derived non-negative length
		uvarintLen(uint64(len(value))) + unshared + len(value) + restartCount*4 + 4
}

func (b *dataBlockBuilder) add(key, value []byte) {
	restart := b.entries%b.restartInterval == 0
	shared := 0
	if restart {
		b.restarts = append(b.restarts, uint32(len(b.buffer))) //nolint:gosec // data blocks are u32-bounded
	} else {
		shared = commonPrefix(b.previousKey, key)
	}
	unshared := len(key) - shared
	b.buffer = binary.AppendUvarint(b.buffer, uint64(shared))   //nolint:gosec // slice-derived non-negative length
	b.buffer = binary.AppendUvarint(b.buffer, uint64(unshared)) //nolint:gosec // slice-derived non-negative length
	b.buffer = binary.AppendUvarint(b.buffer, uint64(len(value)))
	b.buffer = append(b.buffer, key[shared:]...)
	b.buffer = append(b.buffer, value...)
	b.previousKey = append(b.previousKey[:0], key...)
	b.entries++
}

func (b *dataBlockBuilder) finish() []byte {
	payload := make([]byte, 0, len(b.buffer)+len(b.restarts)*4+4)
	payload = append(payload, b.buffer...)
	for _, restart := range b.restarts {
		payload = binary.LittleEndian.AppendUint32(payload, restart)
	}
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(b.restarts))) //nolint:gosec // bounded by block size
	return payload
}

func (b *dataBlockBuilder) reset() {
	b.buffer = b.buffer[:0]
	b.restarts = b.restarts[:0]
	b.previousKey = b.previousKey[:0]
	b.entries = 0
}

func commonPrefix(left, right []byte) int {
	limit := min(len(left), len(right))
	for index := 0; index < limit; index++ {
		if left[index] != right[index] {
			return index
		}
	}
	return limit
}

func uvarintLen(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}
