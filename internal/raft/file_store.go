package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	fileStoreName       = "RAFTSTATE"
	fileStoreTemporary  = "RAFTSTATE.tmp"
	fileStoreVersion    = uint16(2)
	fileStoreHeaderSize = 16
	fileStoreMaxBytes   = uint64(256 << 20)
	fileStoreMaxCommand = uint64(16 << 20)
	fileStoreMaxEntries = uint64(4_000_000)
)

var (
	fileStoreMagic = [8]byte{'R', 'I', 'V', 'R', 'A', 'F', 'T', 0}
	raftCRCTable   = crc32.MakeTable(crc32.Castagnoli)
)

type durableFile interface {
	io.Reader
	io.Writer
	Sync() error
	Close() error
}

type fileStoreOps struct {
	openRead  func(string) (durableFile, error)
	openWrite func(string) (durableFile, error)
	rename    func(string, string) error
	openDir   func(string) (durableFile, error)
}

func osFileStoreOps() fileStoreOps {
	return fileStoreOps{
		openRead: func(path string) (durableFile, error) { return os.Open(path) }, //nolint:gosec // fixed basename under the configured store directory
		openWrite: func(path string) (durableFile, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // fixed basename under configured directory
		},
		rename:  os.Rename,
		openDir: func(path string) (durableFile, error) { return os.Open(path) }, //nolint:gosec // configured store directory
	}
}

// FileStore atomically persists a complete Raft hard-state/log/snapshot value.
// It is independent from the Phase 1 data WAL.
type FileStore struct {
	directory string
	ops       fileStoreOps
}

func OpenFileStore(directory string) (*FileStore, error) {
	if directory == "" {
		return nil, ErrInvalidConfig
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create Raft store directory: %w", err)
	}
	return &FileStore{directory: directory, ops: osFileStoreOps()}, nil
}

func (s *FileStore) Load() (PersistentState, error) {
	file, err := s.ops.openRead(filepath.Join(s.directory, fileStoreName))
	if errors.Is(err, os.ErrNotExist) {
		return PersistentState{}, nil
	}
	if err != nil {
		return PersistentState{}, fmt.Errorf("open Raft state: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, int64(fileStoreMaxBytes)+1)) //nolint:gosec // constant bound
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return PersistentState{}, errors.Join(fmt.Errorf("read Raft state: %w", readErr), closeErr)
	}
	if uint64(len(data)) > fileStoreMaxBytes { //nolint:gosec // nonnegative length
		return PersistentState{}, ErrResourceLimit
	}
	state, err := decodeFileState(data)
	if err != nil {
		return PersistentState{}, err
	}
	return state, nil
}

func (s *FileStore) Save(state PersistentState) error {
	if err := validatePersistent(state); err != nil {
		return err
	}
	data, err := encodeFileState(state)
	if err != nil {
		return err
	}
	temporary := filepath.Join(s.directory, fileStoreTemporary)
	final := filepath.Join(s.directory, fileStoreName)
	file, err := s.ops.openWrite(temporary)
	if err != nil {
		return fmt.Errorf("create Raft state temporary: %w", err)
	}
	if writeErr := writeFull(file, data); writeErr != nil {
		return errors.Join(fmt.Errorf("write Raft state temporary: %w", writeErr), file.Close())
	}
	if syncErr := file.Sync(); syncErr != nil {
		return errors.Join(fmt.Errorf("sync Raft state temporary: %w", syncErr), file.Close())
	}
	if closeErr := file.Close(); closeErr != nil {
		return fmt.Errorf("close Raft state temporary: %w", closeErr)
	}
	if renameErr := s.ops.rename(temporary, final); renameErr != nil {
		return fmt.Errorf("publish Raft state: %w", renameErr)
	}
	directory, err := s.ops.openDir(s.directory)
	if err != nil {
		return fmt.Errorf("open Raft state directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync Raft state directory: %w", err), directory.Close())
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close Raft state directory: %w", err)
	}
	return nil
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if err != nil {
			return fmt.Errorf("write bytes: %w", err)
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func encodeFileState(state PersistentState) ([]byte, error) {
	payload, err := encodeStatePayload(state)
	if err != nil {
		return nil, err
	}
	if uint64(len(payload))+fileStoreHeaderSize+4 > fileStoreMaxBytes { //nolint:gosec // nonnegative length
		return nil, ErrResourceLimit
	}
	result := make([]byte, fileStoreHeaderSize+len(payload)+4)
	copy(result[:8], fileStoreMagic[:])
	binary.LittleEndian.PutUint16(result[8:], fileStoreVersion)
	binary.LittleEndian.PutUint32(result[12:], uint32(len(payload))) //nolint:gosec // total size checked
	copy(result[fileStoreHeaderSize:], payload)
	crc := crc32.Checksum(result[:len(result)-4], raftCRCTable)
	binary.LittleEndian.PutUint32(result[len(result)-4:], crc)
	return result, nil
}

func encodeStatePayload(state PersistentState) ([]byte, error) {
	if uint64(len(state.Snapshot.Data)) > fileStoreMaxCommand || uint64(len(state.Entries)) > fileStoreMaxEntries { //nolint:gosec // nonnegative lengths
		return nil, ErrResourceLimit
	}
	configuration := []byte(nil)
	snapshotConfiguration := []byte(nil)
	var err error
	if state.Config.Version != 0 {
		configuration, err = EncodeConfiguration(state.Config)
		if err != nil {
			return nil, err
		}
	}
	if state.Snapshot.Config.Version != 0 {
		snapshotConfiguration, err = EncodeConfiguration(state.Snapshot.Config)
		if err != nil {
			return nil, err
		}
	}
	size := uint64(8*4 + 4 + len(state.Snapshot.Data) + 4 + 4 + len(snapshotConfiguration) + 4 + len(configuration)) //nolint:gosec // checked below
	for _, entry := range state.Entries {
		if uint64(len(entry.Command)) > fileStoreMaxCommand { //nolint:gosec // nonnegative length
			return nil, ErrResourceLimit
		}
		size += 24 + uint64(len(entry.Command)) //nolint:gosec // bounded command
		if size > fileStoreMaxBytes {
			return nil, ErrResourceLimit
		}
	}
	payload := make([]byte, 0, int(size)) //nolint:gosec // bounded above
	payload = appendU64(payload, state.HardState.Term)
	payload = appendU64(payload, uint64(state.HardState.VotedFor))
	payload = appendU64(payload, state.Snapshot.Index)
	payload = appendU64(payload, state.Snapshot.Term)
	payload = appendU32(payload, uint32(len(state.Snapshot.Data))) //nolint:gosec // bounded above
	payload = append(payload, state.Snapshot.Data...)
	payload = appendU32(payload, uint32(len(state.Entries))) //nolint:gosec // bounded above
	for _, entry := range state.Entries {
		payload = appendU64(payload, entry.Index)
		payload = appendU64(payload, entry.Term)
		payload = append(payload, byte(entry.Type), 0, 0, 0)
		payload = appendU32(payload, uint32(len(entry.Command))) //nolint:gosec // bounded above
		payload = append(payload, entry.Command...)
	}
	payload = appendU32(payload, uint32(len(snapshotConfiguration))) //nolint:gosec
	payload = append(payload, snapshotConfiguration...)
	payload = appendU32(payload, uint32(len(configuration))) //nolint:gosec // membership is bounded
	payload = append(payload, configuration...)
	return payload, nil
}

func decodeFileState(data []byte) (PersistentState, error) {
	if len(data) < fileStoreHeaderSize+4 || !equalMagic(data[:8]) {
		return PersistentState{}, ErrCorruptStore
	}
	if binary.LittleEndian.Uint16(data[8:]) != fileStoreVersion {
		return PersistentState{}, ErrUnsupportedVersion
	}
	if data[10] != 0 || data[11] != 0 {
		return PersistentState{}, ErrCorruptStore
	}
	payloadLength := uint64(binary.LittleEndian.Uint32(data[12:]))
	if payloadLength != uint64(len(data)-fileStoreHeaderSize-4) { //nolint:gosec // header length established
		return PersistentState{}, ErrCorruptStore
	}
	want := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(data[:len(data)-4], raftCRCTable) != want {
		return PersistentState{}, ErrCorruptStore
	}
	decoder := stateDecoder{data: data[fileStoreHeaderSize : len(data)-4]}
	state, err := decoder.decode()
	if err != nil {
		return PersistentState{}, err
	}
	if err := validatePersistent(state); err != nil {
		return PersistentState{}, errors.Join(ErrCorruptStore, err)
	}
	return state, nil
}

func equalMagic(data []byte) bool {
	for index := range fileStoreMagic {
		if data[index] != fileStoreMagic[index] {
			return false
		}
	}
	return true
}

type stateDecoder struct {
	data   []byte
	offset int
}

func (d *stateDecoder) decode() (PersistentState, error) {
	term, ok := d.u64()
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	vote, ok := d.u64()
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	snapshotIndex, ok := d.u64()
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	snapshotTerm, ok := d.u64()
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	snapshotData, ok := d.bytes(fileStoreMaxCommand)
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	count, ok := d.u32()
	if !ok || uint64(count) > fileStoreMaxEntries {
		return PersistentState{}, ErrCorruptStore
	}
	state := PersistentState{HardState: HardState{Term: term, VotedFor: NodeID(vote)}, Snapshot: Snapshot{Index: snapshotIndex, Term: snapshotTerm, Data: snapshotData}, Entries: make([]Entry, 0, count)}
	for range count {
		index, indexOK := d.u64()
		entryTerm, termOK := d.u64()
		if !indexOK || !termOK || d.remaining() < 4 {
			return PersistentState{}, ErrCorruptStore
		}
		kind := EntryType(d.data[d.offset])
		if d.data[d.offset+1] != 0 || d.data[d.offset+2] != 0 || d.data[d.offset+3] != 0 {
			return PersistentState{}, ErrCorruptStore
		}
		d.offset += 4
		command, commandOK := d.bytes(fileStoreMaxCommand)
		if !commandOK {
			return PersistentState{}, ErrCorruptStore
		}
		state.Entries = append(state.Entries, Entry{Index: index, Term: entryTerm, Type: kind, Command: command})
	}
	snapshotConfiguration, ok := d.bytes(1 << 20)
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	if len(snapshotConfiguration) != 0 {
		var configErr error
		state.Snapshot.Config, configErr = DecodeConfiguration(snapshotConfiguration)
		if configErr != nil {
			return PersistentState{}, ErrCorruptStore
		}
	}
	configuration, ok := d.bytes(1 << 20)
	if !ok {
		return PersistentState{}, ErrCorruptStore
	}
	if len(configuration) != 0 {
		var configErr error
		state.Config, configErr = DecodeConfiguration(configuration)
		if configErr != nil {
			return PersistentState{}, ErrCorruptStore
		}
	}
	if d.remaining() != 0 {
		return PersistentState{}, ErrCorruptStore
	}
	return state, nil
}

func (d *stateDecoder) remaining() int { return len(d.data) - d.offset }

func (d *stateDecoder) u32() (uint32, bool) {
	if d.remaining() < 4 {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(d.data[d.offset:])
	d.offset += 4
	return value, true
}

func (d *stateDecoder) u64() (uint64, bool) {
	if d.remaining() < 8 {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(d.data[d.offset:])
	d.offset += 8
	return value, true
}

func (d *stateDecoder) bytes(limit uint64) ([]byte, bool) {
	length, ok := d.u32()
	if !ok || uint64(length) > limit || uint64(d.remaining()) < uint64(length) { //nolint:gosec // remaining is nonnegative
		return nil, false
	}
	result := append([]byte(nil), d.data[d.offset:d.offset+int(length)]...) //nolint:gosec // checked against remaining
	d.offset += int(length)                                                 //nolint:gosec // checked against remaining
	return result, true
}

func appendU32(target []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(target, encoded[:]...)
}

func appendU64(target []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(target, encoded[:]...)
}
