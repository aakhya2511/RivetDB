package multiraft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/rivetdb/rivetdb/internal/raft"
)

const migrationSnapshotChunkBytes = 64 << 10
const migrationSnapshotHeaderSize = 4 + 8*6 + 32

var migrationSnapshotMagic = [4]byte{'R', 'V', 'M', 'S'}
var migrationSnapshotCRC = crc32.MakeTable(crc32.Castagnoli)

type migrationSnapshotHeader struct {
	MigrationID MigrationID
	RangeID     RangeID
	ReplicaID   ReplicaID
	Index       uint64
	Term        uint64
	Total       uint64
	Digest      [32]byte
}

func encodeMigrationSnapshotHeader(h migrationSnapshotHeader) []byte {
	b := append([]byte(nil), migrationSnapshotMagic[:]...)
	for _, v := range []uint64{uint64(h.MigrationID), uint64(h.RangeID), uint64(h.ReplicaID), h.Index, h.Term, h.Total} {
		b = binary.LittleEndian.AppendUint64(b, v)
	}
	return append(b, h.Digest[:]...)
}

//nolint:govet // I/O errors are deliberately scoped to the operation being wrapped
func stageSnapshotChunk(directory string, h migrationSnapshotHeader, offset uint64, data []byte, checksum uint32) (resultErr error) {
	if directory == "" || h.MigrationID == 0 || h.RangeID == 0 || h.ReplicaID == 0 || h.Index == 0 || h.Term == 0 || h.Total == 0 || h.Total > math.MaxInt64-migrationSnapshotHeaderSize || offset > h.Total || uint64(len(data)) > h.Total-offset || crc32.Checksum(data, migrationSnapshotCRC) != checksum {
		return ErrMigrationConflict
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create snapshot staging directory: %w", err)
	}
	path := filepath.Join(directory, fmt.Sprintf("migration-%d.snapshot.partial", h.MigrationID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // path is rooted in the configured replica staging directory
	if err != nil {
		return fmt.Errorf("open snapshot staging file: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat snapshot staging file: %w", err)
	}
	header := encodeMigrationSnapshotHeader(h)
	if info.Size() == 0 {
		if _, err := file.Write(header); err != nil {
			return fmt.Errorf("write snapshot header: %w", err)
		}
		info, err = file.Stat()
		if err != nil {
			return fmt.Errorf("stat written snapshot header: %w", err)
		}
	} else {
		existing := make([]byte, len(header))
		if _, err := file.ReadAt(existing, 0); err != nil || !bytes.Equal(existing, header) {
			return ErrMigrationConflict
		}
	}
	position := int64(migrationSnapshotHeaderSize) + int64(offset) //nolint:gosec // total was bounded by MaxInt64 above
	if info.Size() > position {
		existing := make([]byte, len(data))
		n, readErr := file.ReadAt(existing, position)
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("read duplicate snapshot chunk: %w", readErr)
		}
		if n != len(data) || !bytes.Equal(existing, data) {
			return ErrMigrationConflict
		}
		return nil
	}
	if info.Size() != position {
		return ErrMigrationConflict
	}
	if _, err := file.WriteAt(data, position); err != nil {
		return fmt.Errorf("write snapshot chunk: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync snapshot chunk: %w", err)
	}
	return nil
}

func finalizeStagedSnapshot(directory string, h migrationSnapshotHeader) error {
	partial := filepath.Join(directory, fmt.Sprintf("migration-%d.snapshot.partial", h.MigrationID))
	file, err := os.Open(partial) //nolint:gosec // path is rooted in the configured replica staging directory
	if err != nil {
		return fmt.Errorf("open staged snapshot: %w", err)
	}
	if _, seekErr := file.Seek(migrationSnapshotHeaderSize, io.SeekStart); seekErr != nil {
		return errors.Join(fmt.Errorf("seek staged snapshot: %w", seekErr), file.Close())
	}
	if h.Total > math.MaxInt64-1 {
		return errors.Join(ErrMigrationConflict, file.Close())
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, int64(h.Total)+1)) //nolint:gosec // bounded immediately above
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || uint64(len(payload)) != h.Total || sha256.Sum256(payload) != h.Digest {
		return errors.Join(ErrMigrationConflict, readErr, closeErr)
	}
	final := filepath.Join(directory, fmt.Sprintf("migration-%d.snapshot", h.MigrationID))
	if renameErr := os.Rename(partial, final); renameErr != nil {
		return fmt.Errorf("publish migration snapshot: %w", renameErr)
	}
	dir, err := os.Open(directory) //nolint:gosec // configured replica staging directory
	if err != nil {
		return fmt.Errorf("open snapshot staging directory: %w", err)
	}
	if syncErr := dir.Sync(); syncErr != nil {
		return errors.Join(fmt.Errorf("sync snapshot staging directory: %w", syncErr), dir.Close())
	}
	if closeErr := dir.Close(); closeErr != nil {
		return fmt.Errorf("close snapshot staging directory: %w", closeErr)
	}
	return nil
}

func migrationSnapshotHeaderFor(r MigrationRecord, s raft.Snapshot) migrationSnapshotHeader {
	return migrationSnapshotHeader{MigrationID: r.MigrationID, RangeID: r.RangeID, ReplicaID: r.TargetReplicaID, Index: s.Index, Term: s.Term, Total: uint64(len(s.Data)), Digest: sha256.Sum256(s.Data)} //nolint:gosec // slice length is non-negative and bounded by address space
}
