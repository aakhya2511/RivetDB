package storage

// MaxRecordSize is the largest logical WAL record and encoded write batch.
// It bounds recovery memory independently of corrupted on-disk lengths.
const MaxRecordSize = 64 << 20
