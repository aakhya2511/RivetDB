package sstable

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	bloomAlgorithm  = byte(1)
	bloomProbes     = byte(7)
	bloomBitsPerKey = uint64(10)
	bloomHeaderSize = 16
	bloomSeed       = uint64(0x9e3779b97f4a7c15)
	maxBloomBytes   = uint64(256 << 20)
)

type bloomHash struct{ first, delta uint64 }
type bloomFilter struct {
	bits   []byte
	probes uint8
}

func hashBloomKey(key []byte) bloomHash {
	value := uint64(14695981039346656037) ^ bloomSeed
	for _, octet := range key {
		value ^= uint64(octet)
		value *= 1099511628211
	}
	delta := mixBloom(value+bloomSeed) | 1
	return bloomHash{first: value, delta: delta}
}

func mixBloom(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func encodeBloom(hashes []bloomHash) ([]byte, error) {
	if uint64(len(hashes)) > math.MaxUint32/bloomBitsPerKey { //nolint:gosec // nonnegative slice length
		return nil, ErrResourceLimit
	}
	bitCount := uint64(len(hashes)) * bloomBitsPerKey //nolint:gosec // table entry count is memory bounded
	if bitCount < 64 {
		bitCount = 64
	}
	bitCount = (bitCount + 7) &^ 7
	byteCount := bitCount / 8
	if byteCount > maxBloomBytes || bitCount > math.MaxUint32 {
		return nil, ErrResourceLimit
	}
	payload := make([]byte, bloomHeaderSize+int(byteCount)) //nolint:gosec // bounded above
	payload[0], payload[1] = bloomAlgorithm, bloomProbes
	binary.LittleEndian.PutUint32(payload[4:], uint32(bitCount)) //nolint:gosec // checked above
	binary.LittleEndian.PutUint64(payload[8:], bloomSeed)
	filter := bloomFilter{bits: payload[bloomHeaderSize:], probes: bloomProbes}
	for _, hash := range hashes {
		filter.add(hash)
	}
	return payload, nil
}

func decodeBloom(payload []byte) (bloomFilter, error) {
	if len(payload) < bloomHeaderSize || payload[0] != bloomAlgorithm || payload[1] != bloomProbes ||
		payload[2] != 0 || payload[3] != 0 || binary.LittleEndian.Uint64(payload[8:]) != bloomSeed {
		return bloomFilter{}, errors.Join(ErrCorruptTable, ErrInvalidBlock)
	}
	bitCount := uint64(binary.LittleEndian.Uint32(payload[4:]))
	if bitCount < 64 || bitCount%8 != 0 || bitCount/8 > maxBloomBytes || uint64(len(payload)-bloomHeaderSize) != bitCount/8 { //nolint:gosec // nonnegative after header check
		return bloomFilter{}, errors.Join(ErrCorruptTable, ErrInvalidBlock)
	}
	return bloomFilter{bits: payload[bloomHeaderSize:], probes: bloomProbes}, nil
}

func (f bloomFilter) add(hash bloomHash) {
	bits := uint64(len(f.bits) * 8) //nolint:gosec // bounded filter length
	for probe := uint8(0); probe < f.probes; probe++ {
		position := (hash.first + uint64(probe)*hash.delta) % bits
		f.bits[position/8] |= 1 << (position % 8)
	}
}

func (f bloomFilter) mayContain(key []byte) bool {
	if len(f.bits) == 0 {
		return true
	}
	hash := hashBloomKey(key)
	bits := uint64(len(f.bits) * 8) //nolint:gosec // bounded filter length
	for probe := uint8(0); probe < f.probes; probe++ {
		position := (hash.first + uint64(probe)*hash.delta) % bits
		if f.bits[position/8]&(1<<(position%8)) == 0 {
			return false
		}
	}
	return true
}
