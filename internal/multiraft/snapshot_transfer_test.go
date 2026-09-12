package multiraft

import (
	"crypto/sha256"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotChunkResumeIdempotencyIntegrityAndPublication(t *testing.T) {
	directory := t.TempDir()
	payload := make([]byte, migrationSnapshotChunkBytes+17)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	header := migrationSnapshotHeader{MigrationID: 7, RangeID: 10, ReplicaID: 99, Index: 42, Term: 3, Total: uint64(len(payload)), Digest: sha256.Sum256(payload)}
	first := payload[:migrationSnapshotChunkBytes]
	checksum := crc32.Checksum(first, migrationSnapshotCRC)
	if err := stageSnapshotChunk(directory, header, 0, first, checksum); err != nil {
		t.Fatal(err)
	}
	if err := stageSnapshotChunk(directory, header, 0, first, checksum); err != nil {
		t.Fatalf("duplicate chunk: %v", err)
	}
	corrupt := append([]byte(nil), first...)
	corrupt[0] ^= 1
	if err := stageSnapshotChunk(directory, header, 0, corrupt, crc32.Checksum(corrupt, migrationSnapshotCRC)); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("conflicting duplicate=%v", err)
	}
	tail := payload[len(first):]
	if err := stageSnapshotChunk(directory, header, uint64(len(first)), tail, crc32.Checksum(tail, migrationSnapshotCRC)); err != nil {
		t.Fatal(err)
	}
	if err := finalizeStagedSnapshot(directory, header); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, "migration-7.snapshot")); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotChunkRejectsWrongIdentityChecksumAndGap(t *testing.T) {
	directory := t.TempDir()
	data := []byte("payload")
	header := migrationSnapshotHeader{MigrationID: 1, RangeID: 2, ReplicaID: 3, Index: 4, Term: 5, Total: uint64(len(data)), Digest: sha256.Sum256(data)}
	if err := stageSnapshotChunk(directory, header, 0, data, 0); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("checksum=%v", err)
	}
	if err := stageSnapshotChunk(directory, header, 2, data[2:], crc32.Checksum(data[2:], migrationSnapshotCRC)); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("gap=%v", err)
	}
	if err := stageSnapshotChunk(directory, header, 0, data, crc32.Checksum(data, migrationSnapshotCRC)); err != nil {
		t.Fatal(err)
	}
	wrong := header
	wrong.ReplicaID++
	if err := stageSnapshotChunk(directory, wrong, 0, data, crc32.Checksum(data, migrationSnapshotCRC)); !errors.Is(err, ErrMigrationConflict) {
		t.Fatalf("identity=%v", err)
	}
}
