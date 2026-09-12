package multiraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/rivetdb/rivetdb/internal/raft"
)

const (
	MetaRangeID             RangeID = ^RangeID(0)
	MaxSplitRecords                 = 4096
	MaxMigrationRecords             = 4096
	MaxLineageDepth                 = 128
	maxMetadataCatalogBytes         = 32 << 20
	metadataVersion                 = uint16(2)
)

var metadataMagic = [4]byte{'R', 'V', 'M', 'D'}

type SplitID uint64
type MigrationID uint64

type MigrationState uint8

const (
	MigrationPlanned MigrationState = iota + 1
	MigrationBootstrapping
	MigrationLearner
	MigrationCatchingUp
	MigrationReady
	MigrationJoint
	MigrationPromoted
	MigrationSourceRemoving
	MigrationCommitted
	MigrationSourceRetired
	MigrationAborted
)

func (s MigrationState) String() string {
	names := [...]string{"", "PLANNED", "BOOTSTRAPPING", "LEARNER", "CATCHING_UP", "READY", "JOINT", "PROMOTED", "SOURCE_REMOVING", "COMMITTED", "SOURCE_RETIRED", "ABORTED"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("MIGRATION_STATE_%d", s)
}
func (s MigrationState) valid() bool { return s >= MigrationPlanned && s <= MigrationAborted }

type MigrationRecord struct {
	MigrationID       MigrationID
	RangeID           RangeID
	RangeGeneration   uint64
	SourceReplicaID   ReplicaID
	SourceNodeID      raft.NodeID
	TargetReplicaID   ReplicaID
	TargetNodeID      raft.NodeID
	Epoch             uint64
	State             MigrationState
	BootstrapIndex    uint64
	CatchUpIndex      uint64
	PromotionBarrier  uint64
	ConfigVersion     uint64
	CatalogGeneration uint64
	StateDigest       [32]byte
	LastError         string
}

type SplitState uint8

const (
	SplitPreparing SplitState = iota + 1
	SplitCopying
	SplitCatchingUp
	SplitReady
	SplitFenced
	SplitCommitted
	SplitAborted
)

func (s SplitState) String() string {
	return map[SplitState]string{SplitPreparing: "PREPARING", SplitCopying: "COPYING", SplitCatchingUp: "CATCHING_UP", SplitReady: "READY", SplitFenced: "FENCED", SplitCommitted: "COMMITTED", SplitAborted: "ABORTED"}[s]
}

func (s SplitState) valid() bool { return s >= SplitPreparing && s <= SplitAborted }

type RangeRef struct {
	RangeID    RangeID
	Generation uint64
}

type SplitRecord struct {
	SplitID                  SplitID
	Parent                   RangeRef
	SplitKey                 []byte
	Left                     RangeDescriptor
	Right                    RangeDescriptor
	Epoch                    uint64
	State                    SplitState
	BootstrapIndex           uint64
	FenceIndex               uint64
	LeftReplayThrough        uint64
	RightReplayThrough       uint64
	ImageDigest              [32]byte
	CutoverCatalogGeneration uint64
	LastError                string
}

type LineageRecord struct {
	SplitID                  SplitID
	Parent                   RangeRef
	Children                 [2]RangeRef
	SplitKey                 []byte
	CutoverCatalogGeneration uint64
}

type MetadataSnapshot struct {
	Catalog         *Catalog
	NextRangeID     RangeID
	NextSplitID     SplitID
	Splits          []SplitRecord
	NextReplicaID   ReplicaID
	NextMigrationID MigrationID
	Migrations      []MigrationRecord
	Lineage         []LineageRecord
}

// RecoveryRanges returns non-authoritative physical groups that a node may
// reopen only because replicated metadata names them. It never scans disk.
func (s MetadataSnapshot) RecoveryRanges() (shadow, retired []RangeDescriptor) {
	for _, record := range s.Splits {
		switch record.State {
		case SplitPreparing, SplitCopying, SplitCatchingUp, SplitReady, SplitFenced:
			shadow = append(shadow, cloneDescriptor(record.Left), cloneDescriptor(record.Right))
		case SplitCommitted:
			parent := RangeDescriptor{RangeID: record.Parent.RangeID, Generation: record.Parent.Generation,
				StartKey: record.Left.StartKey, EndKey: record.Right.EndKey, Replicas: slices.Clone(record.Left.Replicas)}
			retired = append(retired, cloneDescriptor(parent))
		}
	}
	return shadow, retired
}

type metadataState struct {
	catalog         *Catalog
	nextRangeID     RangeID
	nextSplitID     SplitID
	splits          map[SplitID]SplitRecord
	nextReplicaID   ReplicaID
	nextMigrationID MigrationID
	migrations      map[MigrationID]MigrationRecord
	lineage         map[RangeRef]LineageRecord
}

func newMetadataState(catalog *Catalog) (*metadataState, error) {
	if catalog == nil {
		return nil, ErrInvalidCatalog
	}
	var largest RangeID
	var largestReplica ReplicaID
	for _, descriptor := range catalog.Snapshot().Ranges {
		largest = max(largest, descriptor.RangeID)
		for _, replica := range descriptor.Replicas {
			largestReplica = max(largestReplica, replica.ReplicaID)
		}
	}
	if largest == ^RangeID(0) || largest+1 == MetaRangeID {
		return nil, ErrResourceLimit
	}
	return &metadataState{catalog: catalog, nextRangeID: largest + 1, nextSplitID: 1, nextReplicaID: largestReplica + 1, nextMigrationID: 1,
		splits: make(map[SplitID]SplitRecord), migrations: make(map[MigrationID]MigrationRecord), lineage: make(map[RangeRef]LineageRecord)}, nil
}

func cloneSplit(value SplitRecord) SplitRecord {
	value.SplitKey = bytes.Clone(value.SplitKey)
	value.Left, value.Right = cloneDescriptor(value.Left), cloneDescriptor(value.Right)
	return value
}

func cloneMetadata(source *metadataState) *metadataState {
	result := &metadataState{catalog: source.catalog, nextRangeID: source.nextRangeID, nextSplitID: source.nextSplitID,
		nextReplicaID: source.nextReplicaID, nextMigrationID: source.nextMigrationID, splits: make(map[SplitID]SplitRecord, len(source.splits)), migrations: make(map[MigrationID]MigrationRecord, len(source.migrations)), lineage: make(map[RangeRef]LineageRecord, len(source.lineage))}
	for id, record := range source.splits {
		result.splits[id] = cloneSplit(record)
	}
	for id, record := range source.migrations {
		result.migrations[id] = record
	}
	for parent, record := range source.lineage {
		record.SplitKey = bytes.Clone(record.SplitKey)
		result.lineage[parent] = record
	}
	return result
}

func (s *metadataState) snapshot() MetadataSnapshot {
	result := MetadataSnapshot{Catalog: s.catalog, NextRangeID: s.nextRangeID, NextSplitID: s.nextSplitID, NextReplicaID: s.nextReplicaID, NextMigrationID: s.nextMigrationID}
	for _, record := range s.splits {
		result.Splits = append(result.Splits, cloneSplit(record))
	}
	for _, record := range s.migrations {
		result.Migrations = append(result.Migrations, record)
	}
	for _, record := range s.lineage {
		record.SplitKey = bytes.Clone(record.SplitKey)
		result.Lineage = append(result.Lineage, record)
	}
	sort.Slice(result.Splits, func(i, j int) bool { return result.Splits[i].SplitID < result.Splits[j].SplitID })
	sort.Slice(result.Migrations, func(i, j int) bool { return result.Migrations[i].MigrationID < result.Migrations[j].MigrationID })
	sort.Slice(result.Lineage, func(i, j int) bool { return result.Lineage[i].Parent.RangeID < result.Lineage[j].Parent.RangeID })
	return result
}

func (s *metadataState) begin(parent RangeRef, splitKey []byte, expectedGeneration uint64) (SplitRecord, error) {
	if s.catalog.Generation() != expectedGeneration {
		return SplitRecord{}, ErrStaleRange
	}
	descriptor, err := s.catalog.LookupByID(parent.RangeID)
	if err != nil || descriptor.Generation != parent.Generation {
		return SplitRecord{}, ErrStaleRange
	}
	if !strictInterior(descriptor, splitKey) {
		return SplitRecord{}, ErrInvalidDescriptor
	}
	for _, existing := range s.splits {
		if existing.Parent == parent && existing.State != SplitAborted && existing.State != SplitCommitted {
			if bytes.Equal(existing.SplitKey, splitKey) {
				return cloneSplit(existing), nil
			}
			return SplitRecord{}, ErrSplitInProgress
		}
	}
	for _, migration := range s.migrations {
		if migration.RangeID == parent.RangeID && migration.State != MigrationAborted && migration.State != MigrationSourceRetired {
			return SplitRecord{}, ErrMigrationInProgress
		}
	}
	if s.nextRangeID == 0 || s.nextRangeID >= MetaRangeID-1 || s.nextSplitID == 0 {
		return SplitRecord{}, ErrResourceLimit
	}
	leftID, rightID, splitID := s.nextRangeID, s.nextRangeID+1, s.nextSplitID
	s.nextRangeID += 2
	s.nextSplitID++
	left := cloneDescriptor(descriptor)
	left.RangeID, left.Generation, left.EndKey = leftID, 1, KeyBound{Key: bytes.Clone(splitKey)}
	right := cloneDescriptor(descriptor)
	right.RangeID, right.Generation, right.StartKey = rightID, 1, KeyBound{Key: bytes.Clone(splitKey)}
	record := SplitRecord{SplitID: splitID, Parent: parent, SplitKey: bytes.Clone(splitKey), Left: left, Right: right, Epoch: 1, State: SplitPreparing}
	s.splits[splitID] = record
	return cloneSplit(record), nil
}

func strictInterior(parent RangeDescriptor, key []byte) bool {
	return (parent.StartKey.Unbounded || bytes.Compare(key, parent.StartKey.Key) > 0) &&
		(parent.EndKey.Unbounded || bytes.Compare(key, parent.EndKey.Key) < 0)
}

func legalSplitTransition(from, to SplitState) bool {
	if to == SplitAborted {
		return from == SplitPreparing || from == SplitCopying || from == SplitCatchingUp || from == SplitReady
	}
	return to == from+1 && from >= SplitPreparing && from < SplitCommitted
}

func (s *metadataState) advance(id SplitID, epoch uint64, to SplitState, update SplitRecord) (SplitRecord, error) {
	record, ok := s.splits[id]
	if !ok || epoch != record.Epoch || !legalSplitTransition(record.State, to) {
		return SplitRecord{}, ErrSplitConflict
	}
	record.State = to
	if update.BootstrapIndex != 0 {
		record.BootstrapIndex = update.BootstrapIndex
	}
	if update.FenceIndex != 0 {
		record.FenceIndex = update.FenceIndex
	}
	record.LeftReplayThrough = max(record.LeftReplayThrough, update.LeftReplayThrough)
	record.RightReplayThrough = max(record.RightReplayThrough, update.RightReplayThrough)
	if update.ImageDigest != [32]byte{} {
		record.ImageDigest = update.ImageDigest
	}
	record.LastError = update.LastError
	s.splits[id] = record
	return cloneSplit(record), nil
}

func (s *metadataState) takeover(id SplitID, epoch uint64) (SplitRecord, error) {
	record, ok := s.splits[id]
	if !ok || record.State == SplitCommitted || record.State == SplitAborted || epoch != record.Epoch || record.Epoch == ^uint64(0) {
		return SplitRecord{}, ErrSplitConflict
	}
	record.Epoch++
	s.splits[id] = record
	return cloneSplit(record), nil
}

func (s *metadataState) commit(id SplitID, epoch, expectedCatalog uint64) (SplitRecord, error) {
	record, ok := s.splits[id]
	if !ok || record.Epoch != epoch || record.State != SplitFenced || s.catalog.Generation() != expectedCatalog ||
		record.FenceIndex == 0 || record.LeftReplayThrough != record.FenceIndex || record.RightReplayThrough != record.FenceIndex {
		return SplitRecord{}, ErrSplitConflict
	}
	bootstrap := s.catalog.Snapshot()
	bootstrap.Generation++
	var ranges []RangeDescriptor
	for _, descriptor := range bootstrap.Ranges {
		if descriptor.RangeID == record.Parent.RangeID {
			ranges = append(ranges, record.Left, record.Right)
		} else {
			ranges = append(ranges, descriptor)
		}
	}
	bootstrap.Ranges = ranges
	candidate, err := NewCatalog(bootstrap)
	if err != nil {
		return SplitRecord{}, fmt.Errorf("validate split catalog: %w", err)
	}
	lineage := LineageRecord{SplitID: id, Parent: record.Parent,
		Children: [2]RangeRef{{RangeID: record.Left.RangeID, Generation: record.Left.Generation}, {RangeID: record.Right.RangeID, Generation: record.Right.Generation}},
		SplitKey: bytes.Clone(record.SplitKey), CutoverCatalogGeneration: candidate.Generation()}
	if _, exists := s.lineage[record.Parent]; exists {
		return SplitRecord{}, ErrSplitConflict
	}
	s.catalog, s.lineage[record.Parent] = candidate, lineage
	record.State, record.CutoverCatalogGeneration = SplitCommitted, candidate.Generation()
	s.splits[id] = record
	if err := s.validateLineage(); err != nil {
		return SplitRecord{}, err
	}
	return cloneSplit(record), nil
}

func (s *metadataState) resolve(ref RangeRef) ([]RangeDescriptor, error) {
	if descriptor, err := s.catalog.LookupByID(ref.RangeID); err == nil && descriptor.Generation == ref.Generation {
		return []RangeDescriptor{descriptor}, nil
	}
	type pendingRef struct {
		ref   RangeRef
		depth int
	}
	queue := []pendingRef{{ref: ref}}
	seen := make(map[RangeRef]struct{})
	var result []RangeDescriptor
	for len(queue) != 0 {
		currentItem := queue[0]
		queue = queue[1:]
		if currentItem.depth > MaxLineageDepth {
			return nil, ErrResourceLimit
		}
		current := currentItem.ref
		if _, exists := seen[current]; exists {
			return nil, ErrCorruptMetadata
		}
		seen[current] = struct{}{}
		if descriptor, err := s.catalog.LookupByID(current.RangeID); err == nil && descriptor.Generation >= current.Generation {
			result = append(result, descriptor)
			continue
		}
		edge, exists := s.lineage[current]
		if !exists {
			return nil, ErrRangeNotFound
		}
		queue = append(queue, pendingRef{ref: edge.Children[0], depth: currentItem.depth + 1}, pendingRef{ref: edge.Children[1], depth: currentItem.depth + 1})
		if len(queue)+len(result) > MaxCatalogRanges {
			return nil, ErrResourceLimit
		}
	}
	sort.Slice(result, func(i, j int) bool { return compareStart(result[i], result[j]) < 0 })
	return result, nil
}

func (s *metadataState) validateLineage() error {
	for parent, edge := range s.lineage {
		if parent != edge.Parent || edge.Children[0] == edge.Children[1] || edge.Children[0] == parent || edge.Children[1] == parent {
			return ErrCorruptMetadata
		}
		if _, err := s.resolve(parent); err != nil {
			return errors.Join(ErrCorruptMetadata, err)
		}
	}
	return nil
}

func encodeMetadata(s *metadataState) ([]byte, error) {
	if s == nil || len(s.splits) > MaxSplitRecords || len(s.migrations) > MaxMigrationRecords || len(s.lineage) > MaxSplitRecords {
		return nil, ErrResourceLimit
	}
	catalogBytes, err := encodeCatalog(s.catalog)
	if err != nil {
		return nil, err
	}
	result := make([]byte, 0, 64+len(catalogBytes))
	result = append(result, metadataMagic[:]...)
	result = binary.LittleEndian.AppendUint16(result, metadataVersion)
	result = append(result, 0, 0)
	result = binary.LittleEndian.AppendUint64(result, uint64(s.nextRangeID))
	result = binary.LittleEndian.AppendUint64(result, uint64(s.nextSplitID))
	result = binary.LittleEndian.AppendUint64(result, uint64(s.nextReplicaID))
	result = binary.LittleEndian.AppendUint64(result, uint64(s.nextMigrationID))
	result = binary.LittleEndian.AppendUint32(result, uint32(len(catalogBytes))) //nolint:gosec // bounded catalog
	result = append(result, catalogBytes...)
	snapshot := s.snapshot()
	result = binary.LittleEndian.AppendUint32(result, uint32(len(snapshot.Splits))) //nolint:gosec // bounded above
	for _, record := range snapshot.Splits {
		result = appendSplitRecord(result, record)
	}
	result = binary.LittleEndian.AppendUint32(result, uint32(len(snapshot.Migrations))) //nolint:gosec
	for _, record := range snapshot.Migrations {
		result = appendMigrationRecord(result, record)
	}
	result = binary.LittleEndian.AppendUint32(result, uint32(len(snapshot.Lineage))) //nolint:gosec // bounded above
	for _, edge := range snapshot.Lineage {
		result = appendLineage(result, edge)
	}
	if len(result) > raft.MaxCommandBytes {
		return nil, ErrResourceLimit
	}
	return result, nil
}

func appendDescriptorBytes(dst []byte, descriptor RangeDescriptor) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(descriptor.RangeID))
	dst = binary.LittleEndian.AppendUint64(dst, descriptor.Generation)
	flags := byte(0)
	if descriptor.StartKey.Unbounded {
		flags |= 1
	}
	if descriptor.EndKey.Unbounded {
		flags |= 2
	}
	dst = append(dst, flags, 0, 0, 0)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(descriptor.StartKey.Key))) //nolint:gosec
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(descriptor.EndKey.Key)))   //nolint:gosec
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(descriptor.Replicas)))     //nolint:gosec
	dst = append(dst, descriptor.StartKey.Key...)
	dst = append(dst, descriptor.EndKey.Key...)
	for _, replica := range descriptor.Replicas {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(replica.ReplicaID))
		dst = binary.LittleEndian.AppendUint64(dst, uint64(replica.NodeID))
	}
	return dst
}

func appendSplitRecord(dst []byte, r SplitRecord) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, uint64(r.SplitID))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(r.Parent.RangeID))
	dst = binary.LittleEndian.AppendUint64(dst, r.Parent.Generation)
	dst = binary.LittleEndian.AppendUint64(dst, r.Epoch)
	dst = append(dst, byte(r.State), 0, 0, 0)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.SplitKey))) //nolint:gosec
	dst = binary.LittleEndian.AppendUint64(dst, r.BootstrapIndex)
	dst = binary.LittleEndian.AppendUint64(dst, r.FenceIndex)
	dst = binary.LittleEndian.AppendUint64(dst, r.LeftReplayThrough)
	dst = binary.LittleEndian.AppendUint64(dst, r.RightReplayThrough)
	dst = binary.LittleEndian.AppendUint64(dst, r.CutoverCatalogGeneration)
	dst = append(dst, r.ImageDigest[:]...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.LastError))) //nolint:gosec
	dst = append(dst, r.SplitKey...)
	dst = append(dst, r.LastError...)
	dst = appendDescriptorBytes(dst, r.Left)
	return appendDescriptorBytes(dst, r.Right)
}

func appendLineage(dst []byte, r LineageRecord) []byte {
	values := []uint64{uint64(r.SplitID), uint64(r.Parent.RangeID), r.Parent.Generation, uint64(r.Children[0].RangeID), r.Children[0].Generation,
		uint64(r.Children[1].RangeID), r.Children[1].Generation, r.CutoverCatalogGeneration}
	for _, value := range values {
		dst = binary.LittleEndian.AppendUint64(dst, value)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.SplitKey))) //nolint:gosec
	return append(dst, r.SplitKey...)
}

func appendMigrationRecord(dst []byte, r MigrationRecord) []byte {
	values := []uint64{uint64(r.MigrationID), uint64(r.RangeID), r.RangeGeneration, uint64(r.SourceReplicaID), uint64(r.SourceNodeID), uint64(r.TargetReplicaID), uint64(r.TargetNodeID), r.Epoch, r.BootstrapIndex, r.CatchUpIndex, r.PromotionBarrier, r.ConfigVersion, r.CatalogGeneration}
	for _, v := range values {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	dst = append(dst, byte(r.State), 0, 0, 0)
	dst = append(dst, r.StateDigest[:]...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.LastError))) //nolint:gosec // validation bounds metadata strings before encoding
	return append(dst, r.LastError...)
}

type metadataDecoder struct {
	data []byte
	at   int
}

func (d *metadataDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.at > len(d.data)-n {
		return nil, ErrCorruptMetadata
	}
	value := d.data[d.at : d.at+n]
	d.at += n
	return value, nil
}
func (d *metadataDecoder) u8() (byte, error) {
	b, e := d.take(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}
func (d *metadataDecoder) u32() (uint32, error) {
	b, e := d.take(4)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (d *metadataDecoder) u64() (uint64, error) {
	b, e := d.take(8)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(b), nil
}

func decodeDescriptorBytes(d *metadataDecoder) (RangeDescriptor, error) {
	id, e := d.u64()
	if e != nil {
		return RangeDescriptor{}, e
	}
	generation, e := d.u64()
	if e != nil {
		return RangeDescriptor{}, e
	}
	flags, e := d.u8()
	if e != nil {
		return RangeDescriptor{}, e
	}
	reserved, e := d.take(3)
	if e != nil || flags&^byte(3) != 0 || !bytes.Equal(reserved, []byte{0, 0, 0}) {
		return RangeDescriptor{}, ErrCorruptMetadata
	}
	startN, e := d.u32()
	if e != nil {
		return RangeDescriptor{}, e
	}
	endN, e := d.u32()
	if e != nil {
		return RangeDescriptor{}, e
	}
	replicasN, e := d.u32()
	if e != nil {
		return RangeDescriptor{}, e
	}
	if startN > uint32(1<<20) || endN > uint32(1<<20) || replicasN > MaxRangeReplicas {
		return RangeDescriptor{}, ErrResourceLimit
	}
	start, e := d.take(int(startN))
	if e != nil {
		return RangeDescriptor{}, e
	}
	end, e := d.take(int(endN))
	if e != nil {
		return RangeDescriptor{}, e
	}
	descriptor := RangeDescriptor{RangeID: RangeID(id), Generation: generation, StartKey: KeyBound{Unbounded: flags&1 != 0, Key: bytes.Clone(start)}, EndKey: KeyBound{Unbounded: flags&2 != 0, Key: bytes.Clone(end)}}
	for range replicasN {
		rid, x := d.u64()
		if x != nil {
			return RangeDescriptor{}, x
		}
		nid, x := d.u64()
		if x != nil {
			return RangeDescriptor{}, x
		}
		descriptor.Replicas = append(descriptor.Replicas, ReplicaDescriptor{ReplicaID: ReplicaID(rid), NodeID: raft.NodeID(nid)})
	}
	if e := descriptor.Validate(); e != nil {
		return RangeDescriptor{}, errors.Join(ErrCorruptMetadata, e)
	}
	return descriptor, nil
}

func decodeMetadata(data []byte) (*metadataState, error) {
	d := metadataDecoder{data: data}
	magic, e := d.take(4)
	if e != nil || !bytes.Equal(magic, metadataMagic[:]) {
		return nil, ErrCorruptMetadata
	}
	versionBytes, e := d.take(2)
	if e != nil {
		return nil, e
	}
	if binary.LittleEndian.Uint16(versionBytes) != metadataVersion {
		return nil, ErrUnsupportedMetadata
	}
	reserved, e := d.take(2)
	if e != nil || reserved[0] != 0 || reserved[1] != 0 {
		return nil, ErrCorruptMetadata
	}
	nextRange, e := d.u64()
	if e != nil {
		return nil, e
	}
	nextSplit, e := d.u64()
	if e != nil {
		return nil, e
	}
	nextReplica, e := d.u64()
	if e != nil {
		return nil, e
	}
	nextMigration, e := d.u64()
	if e != nil {
		return nil, e
	}
	catalogN, e := d.u32()
	if e != nil {
		return nil, e
	}
	if catalogN > maxMetadataCatalogBytes {
		return nil, ErrResourceLimit
	}
	encodedCatalog, e := d.take(int(catalogN))
	if e != nil {
		return nil, e
	}
	catalog, e := decodeCatalog(encodedCatalog)
	if e != nil {
		return nil, errors.Join(ErrCorruptMetadata, e)
	}
	state := &metadataState{catalog: catalog, nextRangeID: RangeID(nextRange), nextSplitID: SplitID(nextSplit), nextReplicaID: ReplicaID(nextReplica), nextMigrationID: MigrationID(nextMigration), splits: make(map[SplitID]SplitRecord), migrations: make(map[MigrationID]MigrationRecord), lineage: make(map[RangeRef]LineageRecord)}
	splitN, e := d.u32()
	if e != nil {
		return nil, e
	}
	if splitN > MaxSplitRecords {
		return nil, ErrResourceLimit
	}
	for range splitN {
		record, x := decodeSplitRecord(&d)
		if x != nil {
			return nil, x
		}
		if _, ok := state.splits[record.SplitID]; ok {
			return nil, ErrCorruptMetadata
		}
		state.splits[record.SplitID] = record
	}
	migrationN, e := d.u32()
	if e != nil {
		return nil, e
	}
	if migrationN > MaxMigrationRecords {
		return nil, ErrResourceLimit
	}
	for range migrationN {
		record, x := decodeMigrationRecord(&d)
		if x != nil {
			return nil, x
		}
		if _, ok := state.migrations[record.MigrationID]; ok {
			return nil, ErrCorruptMetadata
		}
		state.migrations[record.MigrationID] = record
	}
	lineageN, e := d.u32()
	if e != nil {
		return nil, e
	}
	if lineageN > MaxSplitRecords {
		return nil, ErrResourceLimit
	}
	for range lineageN {
		edge, x := decodeLineage(&d)
		if x != nil {
			return nil, x
		}
		if _, ok := state.lineage[edge.Parent]; ok {
			return nil, ErrCorruptMetadata
		}
		state.lineage[edge.Parent] = edge
	}
	if d.at != len(data) || state.nextRangeID == 0 || state.nextSplitID == 0 || state.nextReplicaID == 0 || state.nextMigrationID == 0 {
		return nil, ErrCorruptMetadata
	}
	if e := state.validateLineage(); e != nil {
		return nil, e
	}
	return state, nil
}

func decodeMigrationRecord(d *metadataDecoder) (MigrationRecord, error) {
	values := make([]uint64, 13)
	for i := range values {
		v, e := d.u64()
		if e != nil {
			return MigrationRecord{}, e
		}
		values[i] = v
	}
	state, e := d.u8()
	if e != nil {
		return MigrationRecord{}, e
	}
	reserved, e := d.take(3)
	if e != nil || !bytes.Equal(reserved, []byte{0, 0, 0}) {
		return MigrationRecord{}, ErrCorruptMetadata
	}
	digest, e := d.take(32)
	if e != nil {
		return MigrationRecord{}, e
	}
	n, e := d.u32()
	if e != nil || n > 1<<16 {
		return MigrationRecord{}, ErrResourceLimit
	}
	last, e := d.take(int(n))
	if e != nil {
		return MigrationRecord{}, e
	}
	r := MigrationRecord{MigrationID: MigrationID(values[0]), RangeID: RangeID(values[1]), RangeGeneration: values[2], SourceReplicaID: ReplicaID(values[3]), SourceNodeID: raft.NodeID(values[4]), TargetReplicaID: ReplicaID(values[5]), TargetNodeID: raft.NodeID(values[6]), Epoch: values[7], State: MigrationState(state), BootstrapIndex: values[8], CatchUpIndex: values[9], PromotionBarrier: values[10], ConfigVersion: values[11], CatalogGeneration: values[12], LastError: string(last)}
	copy(r.StateDigest[:], digest)
	if r.MigrationID == 0 || r.RangeID == 0 || r.RangeGeneration == 0 || r.SourceReplicaID == 0 || r.SourceNodeID == 0 || r.TargetReplicaID == 0 || r.TargetNodeID == 0 || r.Epoch == 0 || !r.State.valid() {
		return MigrationRecord{}, ErrCorruptMetadata
	}
	return r, nil
}

func decodeSplitRecord(d *metadataDecoder) (SplitRecord, error) {
	id, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	parent, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	generation, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	epoch, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	stateByte, e := d.u8()
	if e != nil {
		return SplitRecord{}, e
	}
	reserved, e := d.take(3)
	if e != nil || !bytes.Equal(reserved, []byte{0, 0, 0}) {
		return SplitRecord{}, ErrCorruptMetadata
	}
	keyN, e := d.u32()
	if e != nil {
		return SplitRecord{}, e
	}
	bootstrap, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	fence, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	leftReplay, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	rightReplay, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	cutover, e := d.u64()
	if e != nil {
		return SplitRecord{}, e
	}
	digest, e := d.take(32)
	if e != nil {
		return SplitRecord{}, e
	}
	errN, e := d.u32()
	if e != nil {
		return SplitRecord{}, e
	}
	if keyN > 1<<20 || errN > 1<<16 {
		return SplitRecord{}, ErrResourceLimit
	}
	key, e := d.take(int(keyN))
	if e != nil {
		return SplitRecord{}, e
	}
	last, e := d.take(int(errN))
	if e != nil {
		return SplitRecord{}, e
	}
	left, e := decodeDescriptorBytes(d)
	if e != nil {
		return SplitRecord{}, e
	}
	right, e := decodeDescriptorBytes(d)
	if e != nil {
		return SplitRecord{}, e
	}
	r := SplitRecord{SplitID: SplitID(id), Parent: RangeRef{RangeID: RangeID(parent), Generation: generation}, Epoch: epoch, State: SplitState(stateByte), SplitKey: bytes.Clone(key), BootstrapIndex: bootstrap, FenceIndex: fence, LeftReplayThrough: leftReplay, RightReplayThrough: rightReplay, CutoverCatalogGeneration: cutover, LastError: string(last), Left: left, Right: right}
	copy(r.ImageDigest[:], digest)
	if r.SplitID == 0 || r.Parent.RangeID == 0 || r.Parent.Generation == 0 || r.Epoch == 0 || !r.State.valid() || !strictInterior(RangeDescriptor{StartKey: left.StartKey, EndKey: right.EndKey}, r.SplitKey) || left.EndKey.Unbounded || right.StartKey.Unbounded || !bytes.Equal(left.EndKey.Key, r.SplitKey) || !bytes.Equal(right.StartKey.Key, r.SplitKey) || !slices.Equal(left.Replicas, right.Replicas) {
		return SplitRecord{}, ErrCorruptMetadata
	}
	return r, nil
}

func decodeLineage(d *metadataDecoder) (LineageRecord, error) {
	values := make([]uint64, 8)
	for i := range values {
		v, e := d.u64()
		if e != nil {
			return LineageRecord{}, e
		}
		values[i] = v
	}
	n, e := d.u32()
	if e != nil {
		return LineageRecord{}, e
	}
	if n > 1<<20 {
		return LineageRecord{}, ErrResourceLimit
	}
	key, e := d.take(int(n))
	if e != nil {
		return LineageRecord{}, e
	}
	r := LineageRecord{SplitID: SplitID(values[0]), Parent: RangeRef{RangeID: RangeID(values[1]), Generation: values[2]}, Children: [2]RangeRef{{RangeID: RangeID(values[3]), Generation: values[4]}, {RangeID: RangeID(values[5]), Generation: values[6]}}, CutoverCatalogGeneration: values[7], SplitKey: bytes.Clone(key)}
	if r.SplitID == 0 || r.Parent.RangeID == 0 || r.Children[0].RangeID == 0 || r.Children[1].RangeID == 0 {
		return LineageRecord{}, ErrCorruptMetadata
	}
	return r, nil
}
