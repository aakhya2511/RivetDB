package replicatedrange

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/txn"
)

const stateSnapshotVersion = uint16(1)

var stateSnapshotMagic = [4]byte{'R', 'V', 'R', 'S'}
var stateSnapshotCRC = crc32.MakeTable(crc32.Castagnoli)

type logicalStateImage struct {
	rangeID, generation                uint64
	applied, maxApplied, safeRead, hlc uint64
	lifecycle                          Lifecycle
	versions                           []engine.MVCCVersion
	records                            []txn.Record
	participants                       []txn.ParticipantRecord
}

func (m *stateMachine) encodeSnapshot() ([]byte, error) {
	versions, err := m.engine.ExportMVCCVersions(context.Background(), nil, nil)
	if err != nil {
		return nil, fmt.Errorf("export snapshot MVCC history: %w", err)
	}
	applied := m.engine.Stats().Pipeline.ReplicatedApplied
	image := logicalStateImage{rangeID: uint64(m.rangeID), generation: m.generation, applied: applied,
		maxApplied: uint64(m.maxApplied), safeRead: uint64(m.safeRead), hlc: uint64(m.clock.Last()), lifecycle: m.lifecycle, versions: versions}
	for _, record := range m.records {
		image.records = append(image.records, txn.CloneRecord(record))
	}
	for _, record := range m.participants {
		image.participants = append(image.participants, txn.CloneParticipant(record))
	}
	sort.Slice(image.records, func(i, j int) bool { return bytes.Compare(image.records[i].ID[:], image.records[j].ID[:]) < 0 })
	sort.Slice(image.participants, func(i, j int) bool {
		return bytes.Compare(image.participants[i].ID[:], image.participants[j].ID[:]) < 0
	})
	return encodeLogicalState(image)
}

func encodeLogicalState(image logicalStateImage) ([]byte, error) {
	if image.rangeID == 0 || image.generation == 0 || len(image.versions) > 4_000_000 || len(image.records) > 1_000_000 || len(image.participants) > 1_000_000 {
		return nil, ErrInvalidOptions
	}
	result := append([]byte(nil), stateSnapshotMagic[:]...)
	result = binary.LittleEndian.AppendUint16(result, stateSnapshotVersion)
	result = append(result, byte(image.lifecycle), 0)
	for _, value := range []uint64{image.rangeID, image.generation, image.applied, image.maxApplied, image.safeRead, image.hlc} {
		result = binary.LittleEndian.AppendUint64(result, value)
	}
	result = binary.LittleEndian.AppendUint32(result, uint32(len(image.versions))) //nolint:gosec
	for _, version := range image.versions {
		result = appendBytes(result, version.Key)
		result = appendBytes(result, version.Value)
		result = binary.LittleEndian.AppendUint64(result, version.Timestamp)
		result = append(result, byte(version.Kind), 0, 0, 0)
	}
	result = binary.LittleEndian.AppendUint32(result, uint32(len(image.records))) //nolint:gosec
	for _, record := range image.records {
		result = appendTxnRecord(result, record)
	}
	result = binary.LittleEndian.AppendUint32(result, uint32(len(image.participants))) //nolint:gosec
	for _, record := range image.participants {
		result = appendParticipantRecord(result, record)
	}
	if len(result)+4 > 16<<20 {
		return nil, ErrInvalidOptions
	}
	return binary.LittleEndian.AppendUint32(result, crc32.Checksum(result, stateSnapshotCRC)), nil
}

func appendBytes(dst, value []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(value))) //nolint:gosec
	return append(dst, value...)
}

func appendTxnRecord(dst []byte, r txn.Record) []byte {
	dst = append(dst, r.ID[:]...)
	dst = append(dst, byte(r.Status), 0, 0, 0)
	for _, v := range []uint64{r.ReadTime, r.CommitTime, r.Epoch, r.Home.RangeID, r.Home.Generation} {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.Participants))) //nolint:gosec
	for _, p := range r.Participants {
		dst = binary.LittleEndian.AppendUint64(dst, p.RangeID)
		dst = binary.LittleEndian.AppendUint64(dst, p.Generation)
	}
	return dst
}

func appendParticipantRecord(dst []byte, r txn.ParticipantRecord) []byte {
	dst = append(dst, r.ID[:]...)
	dst = append(dst, byte(r.Status), 0, 0, 0)
	for _, v := range []uint64{r.ReadTime, r.CommitTime, r.Epoch, r.Home.RangeID, r.Home.Generation} {
		dst = binary.LittleEndian.AppendUint64(dst, v)
	}
	dst = appendBytes(dst, []byte(r.Reason))
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.Writes))) //nolint:gosec
	for _, w := range r.Writes {
		if w.Delete {
			dst = append(dst, 1)
		} else {
			dst = append(dst, 0)
		}
		dst = appendBytes(dst, w.Key)
		dst = appendBytes(dst, w.Value)
	}
	return dst
}

type stateImageDecoder struct {
	data []byte
	at   int
}

func (d *stateImageDecoder) take(n int) ([]byte, error) {
	if n < 0 || d.at > len(d.data)-n {
		return nil, ErrInvalidOptions
	}
	v := d.data[d.at : d.at+n]
	d.at += n
	return v, nil
}
func (d *stateImageDecoder) u32() (uint32, error) {
	b, e := d.take(4)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint32(b), nil
}
func (d *stateImageDecoder) u64() (uint64, error) {
	b, e := d.take(8)
	if e != nil {
		return 0, e
	}
	return binary.LittleEndian.Uint64(b), nil
}
func (d *stateImageDecoder) bytes(limit uint32) ([]byte, error) {
	n, e := d.u32()
	if e != nil || n > limit {
		return nil, ErrInvalidOptions
	}
	b, e := d.take(int(n))
	return bytes.Clone(b), e
}

func decodeLogicalState(encoded []byte) (logicalStateImage, error) {
	if len(encoded) < 64 || len(encoded) > 16<<20 {
		return logicalStateImage{}, ErrInvalidOptions
	}
	want := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	payload := encoded[:len(encoded)-4]
	if crc32.Checksum(payload, stateSnapshotCRC) != want {
		return logicalStateImage{}, ErrInvalidOptions
	}
	d := stateImageDecoder{data: payload}
	magic, e := d.take(4)
	if e != nil || !bytes.Equal(magic, stateSnapshotMagic[:]) {
		return logicalStateImage{}, ErrInvalidOptions
	}
	ver, e := d.take(2)
	if e != nil || binary.LittleEndian.Uint16(ver) != stateSnapshotVersion {
		return logicalStateImage{}, ErrInvalidOptions
	}
	flags, e := d.take(2)
	if e != nil || flags[1] != 0 {
		return logicalStateImage{}, ErrInvalidOptions
	}
	image := logicalStateImage{lifecycle: Lifecycle(flags[0])}
	values := []*uint64{&image.rangeID, &image.generation, &image.applied, &image.maxApplied, &image.safeRead, &image.hlc}
	for _, target := range values {
		*target, e = d.u64()
		if e != nil {
			return logicalStateImage{}, e
		}
	}
	count, e := d.u32()
	if e != nil || count > 4_000_000 {
		return logicalStateImage{}, ErrInvalidOptions
	}
	for range count {
		key, x := d.bytes(1 << 20)
		if x != nil {
			return logicalStateImage{}, x
		}
		value, x := d.bytes(8 << 20)
		if x != nil {
			return logicalStateImage{}, x
		}
		ts, x := d.u64()
		if x != nil {
			return logicalStateImage{}, x
		}
		kind, x := d.take(4)
		if x != nil || kind[1] != 0 || kind[2] != 0 || kind[3] != 0 {
			return logicalStateImage{}, ErrInvalidOptions
		}
		image.versions = append(image.versions, engine.MVCCVersion{Key: key, Value: value, Timestamp: ts, Kind: storage.ValueKind(kind[0])})
	}
	recordN, e := d.u32()
	if e != nil || recordN > 1_000_000 {
		return logicalStateImage{}, ErrInvalidOptions
	}
	for range recordN {
		r, x := decodeTxnRecord(&d)
		if x != nil {
			return logicalStateImage{}, x
		}
		image.records = append(image.records, r)
	}
	participantN, e := d.u32()
	if e != nil || participantN > 1_000_000 {
		return logicalStateImage{}, ErrInvalidOptions
	}
	for range participantN {
		r, x := decodeParticipantRecord(&d)
		if x != nil {
			return logicalStateImage{}, x
		}
		image.participants = append(image.participants, r)
	}
	if d.at != len(d.data) || image.rangeID == 0 || image.generation == 0 || image.applied == 0 || image.lifecycle < LifecycleActive || image.lifecycle > LifecycleLearner {
		return logicalStateImage{}, ErrInvalidOptions
	}
	return image, nil
}

func decodeTxnRecord(d *stateImageDecoder) (txn.Record, error) {
	b, e := d.take(20)
	if e != nil {
		return txn.Record{}, e
	}
	var r txn.Record
	copy(r.ID[:], b[:16])
	r.Status = txn.Status(b[16])
	if b[17] != 0 || b[18] != 0 || b[19] != 0 {
		return r, ErrInvalidOptions
	}
	values := []*uint64{&r.ReadTime, &r.CommitTime, &r.Epoch, &r.Home.RangeID, &r.Home.Generation}
	for _, p := range values {
		*p, e = d.u64()
		if e != nil {
			return r, e
		}
	}
	n, e := d.u32()
	if e != nil || n > txn.MaxParticipants {
		return r, ErrInvalidOptions
	}
	for range n {
		rid, x := d.u64()
		if x != nil {
			return r, x
		}
		gen, x := d.u64()
		if x != nil {
			return r, x
		}
		r.Participants = append(r.Participants, txn.Participant{RangeID: rid, Generation: gen})
	}
	if r.ID.IsZero() || !r.Status.Valid() || r.Epoch == 0 {
		return r, ErrInvalidOptions
	}
	return r, nil
}

func decodeParticipantRecord(d *stateImageDecoder) (txn.ParticipantRecord, error) {
	b, e := d.take(20)
	if e != nil {
		return txn.ParticipantRecord{}, e
	}
	var r txn.ParticipantRecord
	copy(r.ID[:], b[:16])
	r.Status = txn.ParticipantStatus(b[16])
	if b[17] != 0 || b[18] != 0 || b[19] != 0 {
		return r, ErrInvalidOptions
	}
	values := []*uint64{&r.ReadTime, &r.CommitTime, &r.Epoch, &r.Home.RangeID, &r.Home.Generation}
	for _, p := range values {
		*p, e = d.u64()
		if e != nil {
			return r, e
		}
	}
	reason, e := d.bytes(1 << 16)
	if e != nil {
		return r, e
	}
	r.Reason = string(reason)
	n, e := d.u32()
	if e != nil || n > txn.MaxWrites {
		return r, ErrInvalidOptions
	}
	for range n {
		flag, x := d.take(1)
		if x != nil || flag[0] > 1 {
			return r, ErrInvalidOptions
		}
		key, x := d.bytes(1 << 20)
		if x != nil {
			return r, x
		}
		value, x := d.bytes(8 << 20)
		if x != nil {
			return r, x
		}
		r.Writes = append(r.Writes, txn.Write{Key: key, Value: value, Delete: flag[0] == 1})
	}
	if r.ID.IsZero() || !r.Status.Valid() || r.Epoch == 0 {
		return r, ErrInvalidOptions
	}
	return r, nil
}

func (m *stateMachine) restoreSnapshot(encoded []byte) error { //nolint:contextcheck // raft.StateMachine Restore has no context parameter
	image, err := decodeLogicalState(encoded)
	if err != nil {
		return fmt.Errorf("decode logical range snapshot: %w", err)
	}
	if RangeID(image.rangeID) != m.rangeID || image.generation > m.generation {
		return ErrKeyOutOfRange
	}
	durable, err := m.engine.DurableAppliedRaftIndex()
	if err != nil {
		return fmt.Errorf("read durable apply frontier: %w", err)
	}
	if durable > image.applied {
		return ErrSnapshotBehind
	}
	if durable <= image.applied {
		existing, exportErr := m.engine.ExportMVCCVersions(context.Background(), nil, nil)
		if exportErr != nil {
			return fmt.Errorf("export existing MVCC state: %w", exportErr)
		}
		identity := func(v engine.MVCCVersion) string {
			b := appendBytes(nil, v.Key)
			b = binary.LittleEndian.AppendUint64(b, v.Timestamp)
			b = append(b, byte(v.Kind))
			return string(b)
		}
		have := make(map[string][]byte, len(existing))
		for _, v := range existing {
			have[identity(v)] = v.Value
		}
		versions := make([]engine.MVCCVersion, 0, len(image.versions))
		for _, v := range image.versions {
			if value, ok := have[identity(v)]; ok {
				if !bytes.Equal(value, v.Value) {
					return ErrInvalidOptions
				}
				continue
			}
			versions = append(versions, v)
		}
		sort.SliceStable(versions, func(i, j int) bool { return versions[i].Timestamp < versions[j].Timestamp })
		index := durable
		for at := 0; at < len(versions); {
			end := at + 1
			for end < len(versions) && versions[end].Timestamp == versions[at].Timestamp {
				end++
			}
			index++
			mutations := make([]storage.Mutation, 0, end-at)
			for _, v := range versions[at:end] {
				mutations = append(mutations, storage.Mutation{Key: v.Key, Value: v.Value, Kind: v.Kind})
			}
			identity := sha256.Sum256(binary.LittleEndian.AppendUint64(nil, index))
			if applyErr := m.engine.ApplyPreparedMVCCBatch(context.Background(), index, 1, versions[at].Timestamp, identity[:], mutations); applyErr != nil {
				return fmt.Errorf("restore MVCC snapshot batch: %w", applyErr)
			}
			at = end
		}
		if index > image.applied {
			return ErrInvalidOptions
		}
		if index < image.applied {
			if err := m.engine.AdvanceApplied(image.applied); err != nil {
				return fmt.Errorf("advance restored apply frontier: %w", err)
			}
		}
		if err := m.engine.Flush(context.Background()); err != nil {
			return fmt.Errorf("flush restored MVCC snapshot: %w", err)
		}
	}
	m.records = make(map[txn.ID]txn.Record, len(image.records))
	for _, r := range image.records {
		m.records[r.ID] = txn.CloneRecord(r)
	}
	m.participants = make(map[txn.ID]txn.ParticipantRecord, len(image.participants))
	for _, r := range image.participants {
		m.participants[r.ID] = txn.CloneParticipant(r)
	}
	m.maxApplied, m.safeRead = mvcc.Timestamp(image.maxApplied), mvcc.Timestamp(image.safeRead)
	m.clock.Observe(mvcc.Timestamp(image.hlc))
	if m.lifecycle != LifecycleLearner {
		m.lifecycle = image.lifecycle
	}
	return nil
}

func (m *stateMachine) logicalDigest() ([sha256.Size]byte, error) {
	encoded, err := m.encodeSnapshot()
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	image, err := decodeLogicalState(encoded)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	image.lifecycle = LifecycleActive
	encoded, err = encodeLogicalState(image)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}
