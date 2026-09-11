package multiraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/rivetdb/rivetdb/internal/raft"
)

const (
	catalogHeaderSize = 28
	catalogVersion    = uint16(1)
	maxCatalogBytes   = 32 << 20
)

var (
	catalogMagic = [4]byte{'R', 'V', 'C', 'T'}
	catalogCRC   = crc32.MakeTable(crc32.Castagnoli)
)

func encodeCatalog(catalog *Catalog) ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	buffer.Write(catalogMagic[:])
	writeU16(buffer, catalogVersion)
	var flags uint16
	if catalog.allowPartial {
		flags = 1
	}
	writeU16(buffer, flags)
	writeU64(buffer, catalog.generation)
	writeU32(buffer, uint32(catalog.replicationFactor)) //nolint:gosec // validated bound
	writeU32(buffer, uint32(len(catalog.nodes)))        //nolint:gosec // validated bound
	writeU32(buffer, uint32(len(catalog.ranges)))       //nolint:gosec // validated bound
	for _, nodeID := range catalog.nodes {
		writeU64(buffer, uint64(nodeID))
	}
	for _, descriptor := range catalog.ranges {
		writeU64(buffer, uint64(descriptor.RangeID))
		writeU64(buffer, descriptor.Generation)
		var endpointFlags uint8
		if descriptor.StartKey.Unbounded {
			endpointFlags |= 1
		}
		if descriptor.EndKey.Unbounded {
			endpointFlags |= 2
		}
		buffer.WriteByte(endpointFlags)
		buffer.Write([]byte{0, 0, 0})
		writeU32(buffer, uint32(len(descriptor.StartKey.Key))) //nolint:gosec // storage key bound
		writeU32(buffer, uint32(len(descriptor.EndKey.Key)))   //nolint:gosec // storage key bound
		writeU32(buffer, uint32(len(descriptor.Replicas)))     //nolint:gosec // validated bound
		buffer.Write(descriptor.StartKey.Key)
		buffer.Write(descriptor.EndKey.Key)
		for _, replica := range descriptor.Replicas {
			writeU64(buffer, uint64(replica.ReplicaID))
			writeU64(buffer, uint64(replica.NodeID))
		}
	}
	if buffer.Len() > maxCatalogBytes-4 {
		return nil, ErrResourceLimit
	}
	writeU32(buffer, crc32.Checksum(buffer.Bytes(), catalogCRC))
	return buffer.Bytes(), nil
}

func decodeCatalog(encoded []byte) (*Catalog, error) {
	if len(encoded) < catalogHeaderSize+4 || len(encoded) > maxCatalogBytes || !bytes.Equal(encoded[:4], catalogMagic[:]) {
		return nil, ErrCorruptCatalog
	}
	if binary.LittleEndian.Uint16(encoded[4:6]) != catalogVersion {
		return nil, ErrUnsupportedCatalog
	}
	flags := binary.LittleEndian.Uint16(encoded[6:8])
	if flags&^uint16(1) != 0 {
		return nil, ErrCorruptCatalog
	}
	wantCRC := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	if crc32.Checksum(encoded[:len(encoded)-4], catalogCRC) != wantCRC {
		return nil, ErrCorruptCatalog
	}
	reader := catalogReader{data: encoded[8 : len(encoded)-4]}
	generation, ok := reader.u64()
	if !ok {
		return nil, ErrCorruptCatalog
	}
	replication, ok := reader.u32()
	if !ok || replication > MaxRangeReplicas {
		return nil, ErrCorruptCatalog
	}
	nodeCount, ok := reader.u32()
	if !ok || nodeCount > MaxCatalogNodes {
		return nil, ErrCorruptCatalog
	}
	rangeCount, ok := reader.u32()
	if !ok || rangeCount > MaxCatalogRanges {
		return nil, ErrCorruptCatalog
	}
	bootstrap := Bootstrap{Generation: generation, ReplicationFactor: int(replication), AllowPartial: flags&1 != 0,
		Nodes: make([]raft.NodeID, int(nodeCount)), Ranges: make([]RangeDescriptor, int(rangeCount))}
	for index := range bootstrap.Nodes {
		value, valid := reader.u64()
		if !valid {
			return nil, ErrCorruptCatalog
		}
		bootstrap.Nodes[index] = raft.NodeID(value)
	}
	for index := range bootstrap.Ranges {
		rangeID, validRange := reader.u64()
		generationValue, validGeneration := reader.u64()
		endpointFlags, validFlags := reader.u8()
		reserved, validReserved := reader.take(3)
		startLength, validStart := reader.u32()
		endLength, validEnd := reader.u32()
		replicaCount, validReplicas := reader.u32()
		if !validRange || !validGeneration || !validFlags || !validReserved || !validStart || !validEnd || !validReplicas ||
			endpointFlags&^uint8(3) != 0 || !bytes.Equal(reserved, []byte{0, 0, 0}) || replicaCount > MaxRangeReplicas ||
			uint64(startLength)+uint64(endLength) > math.MaxInt {
			return nil, ErrCorruptCatalog
		}
		start, validStartBytes := reader.take(int(startLength))
		end, validEndBytes := reader.take(int(endLength))
		if !validStartBytes || !validEndBytes {
			return nil, ErrCorruptCatalog
		}
		descriptor := RangeDescriptor{RangeID: RangeID(rangeID), Generation: generationValue,
			StartKey: KeyBound{Unbounded: endpointFlags&1 != 0, Key: bytes.Clone(start)},
			EndKey:   KeyBound{Unbounded: endpointFlags&2 != 0, Key: bytes.Clone(end)},
			Replicas: make([]ReplicaDescriptor, int(replicaCount))}
		for replicaIndex := range descriptor.Replicas {
			replicaID, validReplica := reader.u64()
			nodeID, validNode := reader.u64()
			if !validReplica || !validNode {
				return nil, ErrCorruptCatalog
			}
			descriptor.Replicas[replicaIndex] = ReplicaDescriptor{ReplicaID: ReplicaID(replicaID), NodeID: raft.NodeID(nodeID)}
		}
		bootstrap.Ranges[index] = descriptor
	}
	if len(reader.data) != 0 {
		return nil, fmt.Errorf("%w: trailing bytes", ErrCorruptCatalog)
	}
	catalog, err := NewCatalog(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("validate decoded catalog: %w", errors.Join(ErrCorruptCatalog, err))
	}
	canonical, err := encodeCatalog(catalog)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, ErrCorruptCatalog
	}
	return catalog, nil
}

type catalogReader struct{ data []byte }

func (r *catalogReader) take(length int) ([]byte, bool) {
	if length < 0 || length > len(r.data) {
		return nil, false
	}
	result := r.data[:length]
	r.data = r.data[length:]
	return result, true
}
func (r *catalogReader) u8() (uint8, bool) {
	value, ok := r.take(1)
	if !ok {
		return 0, false
	}
	return value[0], true
}
func (r *catalogReader) u32() (uint32, bool) {
	value, ok := r.take(4)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint32(value), true
}
func (r *catalogReader) u64() (uint64, bool) {
	value, ok := r.take(8)
	if !ok {
		return 0, false
	}
	return binary.LittleEndian.Uint64(value), true
}
func writeU16(buffer *bytes.Buffer, value uint16) {
	var field [2]byte
	binary.LittleEndian.PutUint16(field[:], value)
	buffer.Write(field[:])
}
func writeU32(buffer *bytes.Buffer, value uint32) {
	var field [4]byte
	binary.LittleEndian.PutUint32(field[:], value)
	buffer.Write(field[:])
}
func writeU64(buffer *bytes.Buffer, value uint64) {
	var field [8]byte
	binary.LittleEndian.PutUint64(field[:], value)
	buffer.Write(field[:])
}
