package sstable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestWriterPublishesValidTable(t *testing.T) {
	t.Parallel()

	entries := []testEntry{
		{key: mustKey(t, nil, math.MaxUint64, storage.KindDelete)},
		{key: mustKey(t, []byte{0x00}, 9, storage.KindValue), value: []byte{}},
		{key: mustKey(t, []byte{0x00}, 3, storage.KindDelete)},
		{key: mustKey(t, []byte{0x00, 0xff}, 7, storage.KindValue), value: []byte{0x00, 0xff}},
		{key: mustKey(t, []byte{0xff}, 0, storage.KindValue), value: []byte("last")},
	}
	data, metadata, finalPath := buildRealTable(t, 7, Options{BlockSize: 48, RestartInterval: 2}, entries)
	decoded := mustValidate(t, data)
	assertDecodedEntries(t, decoded.entries, entries)
	if metadata.FileNumber != 7 || metadata.FileSize != uint64(len(data)) || metadata.EntryCount != uint64(len(entries)) || metadata.DeletionCount != 2 {
		t.Fatalf("metadata = %+v", metadata)
	}
	if metadata.DataBlockCount != uint32(len(decoded.dataBlocks)) || metadata.DataBlockCount < 2 {
		t.Fatalf("data blocks metadata=%d decoded=%d", metadata.DataBlockCount, len(decoded.dataBlocks))
	}
	if _, err := os.Stat(finalPath + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary path still exists: %v", err)
	}
	if filepath.Base(finalPath) != "000000000007.sst" {
		t.Fatalf("final filename = %s", filepath.Base(finalPath))
	}
}

func TestWriterLifecycleAndEmptyPolicy(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	writer, err := OpenWriter(directory, 1, Options{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if _, finishErr := writer.Finish(); !errors.Is(finishErr, ErrEmptyTable) {
		t.Fatalf("empty Finish error = %v, want ErrEmptyTable", finishErr)
	}
	key := mustKey(t, []byte("a"), 1, storage.KindValue)
	if addErr := writer.Add(key, []byte("value")); addErr != nil {
		t.Fatalf("Add after empty Finish: %v", addErr)
	}
	first, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	first.SmallestUser[0] ^= 0xff
	second, err := writer.Finish()
	if err != nil {
		t.Fatalf("repeat Finish: %v", err)
	}
	if !metadataEqual(first, second) {
		// The first result was deliberately mutated; the writer's retained
		// metadata must not have changed with it.
		first.SmallestUser = second.SmallestUser
		if !metadataEqual(first, second) {
			t.Fatalf("repeat Finish metadata differs: %+v / %+v", first, second)
		}
	}
	if addErr := writer.Add(mustKey(t, []byte("b"), 1, storage.KindValue), nil); !errors.Is(addErr, ErrFinished) {
		t.Fatalf("Add after Finish error = %v, want ErrFinished", addErr)
	}
	if abortErr := writer.Abort(); !errors.Is(abortErr, ErrFinished) {
		t.Fatalf("Abort after Finish error = %v, want ErrFinished", abortErr)
	}

	aborted, err := OpenWriter(directory, 2, Options{})
	if err != nil {
		t.Fatalf("OpenWriter abort case: %v", err)
	}
	if err := aborted.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if err := aborted.Abort(); err != nil {
		t.Fatalf("repeat Abort: %v", err)
	}
	if err := aborted.Add(key, nil); !errors.Is(err, ErrAborted) {
		t.Fatalf("Add after Abort error = %v, want ErrAborted", err)
	}
	if _, err := os.Stat(filepath.Join(directory, FileName(2)+".tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("aborted temporary path exists: %v", err)
	}
}

func TestOpenWriterRejectsExistingFinalOrTemporaryPath(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	finalPath := filepath.Join(directory, FileName(1))
	if err := os.WriteFile(finalPath, []byte("existing"), 0o600); err != nil {
		t.Fatalf("write final fixture: %v", err)
	}
	if _, err := OpenWriter(directory, 1, Options{}); !errors.Is(err, ErrFileExists) {
		t.Fatalf("existing final error = %v, want ErrFileExists", err)
	}
	if err := os.WriteFile(filepath.Join(directory, FileName(2)+".tmp"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write temporary fixture: %v", err)
	}
	if _, err := OpenWriter(directory, 2, Options{}); !errors.Is(err, ErrFileExists) {
		t.Fatalf("existing temporary error = %v, want ErrFileExists", err)
	}
}

func TestWriterRejectsDuplicateAndOutOfOrderInput(t *testing.T) {
	t.Parallel()

	shorter := mustKey(t, nil, 0, storage.KindDelete)
	longer := mustKey(t, []byte{0x00}, math.MaxUint64, storage.KindValue)
	if bytes.Compare(shorter.Encode(), longer.Encode()) <= 0 {
		t.Fatal("test does not exercise raw-byte ordering disagreement")
	}

	writer := newMemoryWriter(t, Options{})
	if err := writer.Add(longer, []byte("longer")); err != nil {
		t.Fatalf("Add longer: %v", err)
	}
	if err := writer.Add(shorter, nil); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("prefix counterexample error = %v, want ErrOutOfOrder", err)
	}
	duplicate := newMemoryWriter(t, Options{})
	if err := duplicate.Add(shorter, nil); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := duplicate.Add(shorter, nil); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateKey", err)
	}

	versions := newMemoryWriter(t, Options{})
	for _, sequence := range []uint64{30, 20, 10} {
		if err := versions.Add(mustKey(t, []byte("foo"), sequence, storage.KindValue), []byte("v")); err != nil {
			t.Fatalf("valid version %d rejected: %v", sequence, err)
		}
	}
}

func TestWriterCopiesBufferedValue(t *testing.T) {
	t.Parallel()

	writer := newMemoryWriter(t, Options{})
	key := mustKey(t, []byte("owned"), 1, storage.KindValue)
	value := []byte("original")
	if err := writer.Add(key, value); err != nil {
		t.Fatalf("Add: %v", err)
	}
	for index := range value {
		value[index] = 'x'
	}
	if _, err := writer.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	decoded := mustValidate(t, writer.file.(*memoryFile).Bytes())
	if string(decoded.entries[0].value) != "original" {
		t.Fatalf("buffered value = %q, want original", decoded.entries[0].value)
	}
}

func TestPrefixCompressionAndRestarts(t *testing.T) {
	t.Parallel()

	prefix := bytes.Repeat([]byte{0x00, 0xff, 0x80}, 350)
	entries := make([]testEntry, 10)
	for index := range entries {
		userKey := append(bytes.Clone(prefix), byte(index))
		entries[index] = testEntry{key: mustKey(t, userKey, uint64(100-index), storage.KindValue), value: []byte{byte(index)}}
	}
	data, _, _ := buildRealTable(t, 8, Options{BlockSize: 1 << 20, RestartInterval: 3}, entries)
	decoded := mustValidate(t, data)
	if len(decoded.dataBlocks) != 1 {
		t.Fatalf("data block count = %d, want 1", len(decoded.dataBlocks))
	}
	if len(decoded.dataBlocks[0].restarts) != 4 || decoded.dataBlocks[0].restarts[0] != 0 {
		t.Fatalf("restarts = %v, want four starting at zero", decoded.dataBlocks[0].restarts)
	}
	assertDecodedEntries(t, decoded.entries, entries)
}

func TestBlockSizeBoundariesAndOversizedEntry(t *testing.T) {
	t.Parallel()

	key := mustKey(t, nil, 1, storage.KindValue).Encode()
	for _, tc := range []struct {
		valueLength int
		want        int
	}{{43, 63}, {44, 64}, {45, 65}} {
		builder := newDataBlockBuilder(DefaultRestartInterval)
		if got := builder.projectedSize(key, make([]byte, tc.valueLength)); got != tc.want {
			t.Fatalf("projected size for value %d = %d, want %d", tc.valueLength, got, tc.want)
		}
	}

	entries := []testEntry{
		{key: mustKey(t, nil, 2, storage.KindValue), value: bytes.Repeat([]byte("x"), 44)},
		{key: mustKey(t, []byte{0x00}, 1, storage.KindValue), value: []byte("next")},
	}
	data, _, _ := buildRealTable(t, 9, Options{BlockSize: 64}, entries)
	decoded := mustValidate(t, data)
	if len(decoded.dataBlocks) != 2 {
		t.Fatalf("exact-fit then overflow block count = %d, want 2", len(decoded.dataBlocks))
	}

	oversized := []testEntry{{key: mustKey(t, []byte("large"), 1, storage.KindValue), value: bytes.Repeat([]byte{0xaa}, 8192)}}
	data, _, _ = buildRealTable(t, 10, Options{BlockSize: 64}, oversized)
	decoded = mustValidate(t, data)
	if len(decoded.dataBlocks) != 1 || decoded.dataBlocks[0].handle.Length <= 64 {
		t.Fatalf("oversized legal entry block = %+v", decoded.dataBlocks)
	}
}

func TestVersionsMayCrossBlockBoundary(t *testing.T) {
	t.Parallel()

	entries := make([]testEntry, 0, 40)
	for sequence := uint64(40); sequence > 0; sequence-- {
		kind := storage.KindValue
		value := bytes.Repeat([]byte{byte(sequence)}, 24)
		if sequence%7 == 0 {
			kind = storage.KindDelete
			value = nil
		}
		entries = append(entries, testEntry{key: mustKey(t, []byte("foo"), sequence, kind), value: value})
	}
	data, metadata, _ := buildRealTable(t, 11, Options{BlockSize: 96, RestartInterval: 4}, entries)
	decoded := mustValidate(t, data)
	if len(decoded.dataBlocks) < 3 {
		t.Fatalf("data block count = %d, want at least 3", len(decoded.dataBlocks))
	}
	for _, block := range decoded.dataBlocks {
		for _, entry := range block.entries {
			if !bytes.Equal(entry.key.UserKey(), []byte("foo")) {
				t.Fatalf("unexpected user key %x", entry.key.UserKey())
			}
		}
	}
	if !bytes.Equal(metadata.SmallestUser, []byte("foo")) || !bytes.Equal(metadata.LargestUser, []byte("foo")) {
		t.Fatalf("user metadata = %q/%q", metadata.SmallestUser, metadata.LargestUser)
	}
	assertDecodedEntries(t, decoded.entries, entries)
}

func TestDeterministicOutput(t *testing.T) {
	t.Parallel()

	entries := deterministicEntries(t, 2_000)
	first, _, _ := buildRealTable(t, 21, Options{}, entries)
	second, _, _ := buildRealTable(t, 22, Options{}, entries)
	if !bytes.Equal(first, second) {
		t.Fatalf("identical inputs differ: sha256 %x != %x", sha256.Sum256(first), sha256.Sum256(second))
	}
	digest := sha256.Sum256(first)
	wantDigest := [sha256.Size]byte{0x7c, 0xea, 0x59, 0x28, 0x13, 0xde, 0x6c, 0x4f, 0xb9, 0x15, 0x59, 0x40, 0x0b, 0xe3, 0x93, 0x3d, 0x32, 0x2a, 0x0b, 0x83, 0x90, 0x70, 0x1b, 0x71, 0x51, 0xa8, 0x41, 0xdc, 0x70, 0x37, 0x97, 0x0a}
	if digest != wantDigest || len(first) != 50_871 {
		t.Fatalf("format golden = sha256 %x size %d", digest, len(first))
	}
	t.Logf("deterministic table sha256=%x size=%d", digest, len(first))
}

func TestMemTableIteratorCompatibility(t *testing.T) {
	t.Parallel()

	table := memtable.New()
	for index := 99; index >= 0; index-- {
		key := mustKey(t, []byte(fmt.Sprintf("key-%03d", index)), uint64(index), storage.KindValue)
		if err := table.Insert(key, []byte(fmt.Sprintf("value-%03d", index))); err != nil {
			t.Fatalf("MemTable Insert: %v", err)
		}
	}
	table.Freeze()
	writer := newMemoryWriter(t, Options{})
	iterator := table.Iterator()
	var entries []testEntry
	for iterator.Next() {
		entry, ok := iterator.Entry()
		if !ok {
			t.Fatal("MemTable iterator invalid")
		}
		entries = append(entries, testEntry{key: entry.Key, value: entry.Value})
		if err := writer.Add(entry.Key, entry.Value); err != nil {
			t.Fatalf("SSTable Add: %v", err)
		}
	}
	metadata, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	decoded := mustValidate(t, writer.file.(*memoryFile).Bytes())
	assertDecodedEntries(t, decoded.entries, entries)
	if metadata.EntryCount != 100 {
		t.Fatalf("entry count = %d, want 100", metadata.EntryCount)
	}
}

func TestWriterResourceLimitsAndOptions(t *testing.T) {
	t.Parallel()

	if _, err := OpenWriter(t.TempDir(), 0, Options{}); !errors.Is(err, ErrInvalidFileNumber) {
		t.Fatalf("file number zero error = %v", err)
	}
	if _, err := OpenWriter(t.TempDir(), MaxFileNumber+1, Options{}); !errors.Is(err, ErrInvalidFileNumber) {
		t.Fatalf("large file number error = %v", err)
	}
	if _, err := OpenWriter(t.TempDir(), 1, Options{BlockSize: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("invalid block option error = %v", err)
	}

	writer := newMemoryWriter(t, Options{})
	tooLargeKey, err := storage.NewInternalKey(make([]byte, MaxUserKeySize+1), 1, storage.KindValue)
	if err != nil {
		t.Fatalf("construct large key: %v", err)
	}
	if err := writer.Add(tooLargeKey, nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("large key error = %v", err)
	}
	validKey := mustKey(t, []byte("a"), 1, storage.KindValue)
	if err := writer.Add(validKey, make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("large value error = %v", err)
	}
	deleteKey := mustKey(t, []byte("a"), 1, storage.KindDelete)
	if err := writer.Add(deleteKey, []byte("value")); !errors.Is(err, ErrDeleteHasValue) {
		t.Fatalf("delete value error = %v", err)
	}
	if _, ok := checkedAdd(math.MaxUint64, 1); ok {
		t.Fatal("checkedAdd accepted overflow")
	}
}

func TestRandomizedWriterAgainstReference(t *testing.T) {
	seeds := append([]int64{0, 1, -1, 8134472901, -1234567890123}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			runRandomizedWriter(t, seed, 2_000)
		})
	}
}

func runRandomizedWriter(t *testing.T, seed int64, count int) {
	t.Helper()
	rng := testutil.RandFromSeed(seed)
	byKey := make(map[string]testEntry, count)
	for len(byKey) < count {
		userKey := randomBytes(rng, int(rng.Uint64()%96))
		if len(byKey)%9 == 0 {
			userKey = append(bytes.Repeat([]byte{0x00, 0xff}, 20), userKey...)
		}
		kind := storage.ValueKind(rng.Uint64() % 2)
		key := mustKey(t, userKey, rng.Uint64()%512, kind)
		value := randomBytes(rng, int(rng.Uint64()%256))
		if kind == storage.KindDelete {
			value = nil
		}
		byKey[string(key.Encode())] = testEntry{key: key, value: value}
	}
	entries := make([]testEntry, 0, len(byKey))
	for _, entry := range byKey {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(left, right testEntry) int {
		return storage.CompareInternal(left.key, right.key)
	})
	data, _, _ := buildRealTable(t, seedBits(seed)%MaxFileNumber+1, Options{}, entries)
	decoded := mustValidate(t, data)
	assertDecodedEntries(t, decoded.entries, entries)
}

func TestBlockAndFooterCorruptionDetected(t *testing.T) {
	t.Parallel()

	entries := deterministicEntries(t, 100)
	data, _, _ := buildRealTable(t, 31, Options{BlockSize: 128}, entries)
	valid := mustValidate(t, data)
	tests := []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{name: "data payload", mutate: func(dataCopy []byte) { dataCopy[int(valid.dataBlocks[0].handle.Offset)] ^= 0x80 }, want: errTableChecksum},
		{name: "data trailer", mutate: func(dataCopy []byte) {
			dataCopy[int(valid.dataBlocks[0].handle.Offset+valid.dataBlocks[0].handle.Length)-BlockTrailerSize] ^= 1
		}, want: errUnsupportedTable},
		{name: "index", mutate: func(dataCopy []byte) { dataCopy[int(valid.indexHandle.Offset)] ^= 1 }, want: errTableChecksum},
		{name: "metadata", mutate: func(dataCopy []byte) { dataCopy[int(valid.metadataHandle.Offset)] ^= 1 }, want: errTableChecksum},
		{name: "footer checksum", mutate: func(dataCopy []byte) { dataCopy[len(dataCopy)-FooterSize+footerChecksum] ^= 1 }, want: errTableChecksum},
		{name: "footer magic", mutate: func(dataCopy []byte) { dataCopy[len(dataCopy)-1] ^= 1 }, want: errInvalidTable},
		{name: "unsupported version", mutate: func(dataCopy []byte) {
			binary.LittleEndian.PutUint32(dataCopy[len(dataCopy)-FooterSize+footerFormatVersion:], FormatVersion+1)
			recomputeFooterChecksum(dataCopy)
		}, want: errUnsupportedTable},
		{name: "invalid index handle", mutate: func(dataCopy []byte) {
			binary.LittleEndian.PutUint64(dataCopy[len(dataCopy)-FooterSize+footerIndexOffset:], math.MaxUint64)
			recomputeFooterChecksum(dataCopy)
		}, want: errInvalidBlockHandle},
		{name: "wrong data kind", mutate: func(dataCopy []byte) {
			handle := valid.dataBlocks[0].handle
			dataCopy[int(handle.Offset+handle.Length)-BlockTrailerSize+1] = byte(blockMetadata)
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidTable},
		{name: "noncanonical entry length", mutate: func(dataCopy []byte) {
			handle := valid.dataBlocks[0].handle
			start := int(handle.Offset)
			dataCopy[start] = 0x80
			dataCopy[start+1] = 0x00
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidTable},
		{name: "restart count out of bounds", mutate: func(dataCopy []byte) {
			handle := valid.dataBlocks[0].handle
			payloadEnd := int(handle.Offset+handle.Length) - BlockTrailerSize
			binary.LittleEndian.PutUint32(dataCopy[payloadEnd-4:], math.MaxUint32)
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidTable},
		{name: "restart offset out of bounds", mutate: func(dataCopy []byte) {
			handle := valid.dataBlocks[0].handle
			payloadEnd := int(handle.Offset+handle.Length) - BlockTrailerSize
			count := binary.LittleEndian.Uint32(dataCopy[payloadEnd-4:])
			restartStart := payloadEnd - 4 - int(count)*4
			binary.LittleEndian.PutUint32(dataCopy[restartStart:], math.MaxUint32)
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidTable},
		{name: "index data handle out of range", mutate: func(dataCopy []byte) {
			handle := valid.indexHandle
			payload := dataCopy[int(handle.Offset) : int(handle.Offset+handle.Length)-BlockTrailerSize]
			keyLength, consumed := binary.Uvarint(payload[4:])
			handleOffset := 4 + consumed + int(keyLength)
			binary.LittleEndian.PutUint64(payload[handleOffset:], math.MaxUint64)
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidBlockHandle},
		{name: "metadata count mismatch", mutate: func(dataCopy []byte) {
			handle := valid.metadataHandle
			binary.LittleEndian.PutUint64(dataCopy[int(handle.Offset):], math.MaxUint64)
			recomputeBlockChecksum(dataCopy, handle)
		}, want: errInvalidTable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := bytes.Clone(data)
			tc.mutate(corrupt)
			if _, err := validateTable(corrupt); !errors.Is(err, tc.want) {
				t.Fatalf("validation error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestTrailingGarbageIsInvalid(t *testing.T) {
	t.Parallel()

	data, _, _ := buildRealTable(t, 33, Options{}, deterministicEntries(t, 10))
	withGarbage := append(bytes.Clone(data), 0)
	if _, err := validateTable(withGarbage); err == nil {
		t.Fatal("table with trailing garbage validated")
	}
}

func TestEveryTruncationIsInvalid(t *testing.T) {
	if os.Getenv("RIVETDB_EXHAUSTIVE") == "" {
		t.Skip("set RIVETDB_EXHAUSTIVE=1 for every-byte SSTable truncation")
	}
	t.Parallel()

	data, _, _ := buildRealTable(t, 32, Options{BlockSize: 96}, deterministicEntries(t, 20))
	for offset := range len(data) {
		if _, err := validateTable(data[:offset]); err == nil {
			t.Fatalf("truncation at %d/%d validated", offset, len(data))
		}
	}
}

func TestWriterShortWritesAndPublicationOrdering(t *testing.T) {
	t.Parallel()

	events := make([]string, 0)
	file := &memoryFile{maxWrite: 3, events: &events}
	directory := &fakeDirectory{events: &events}
	writer := newWriter(file, "/db", "/db/table.tmp", "/db/table.sst", 1, DefaultBlockSize, DefaultRestartInterval, publicationOps{
		rename: func(_, _ string) error {
			events = append(events, "rename")
			return nil
		},
		remove: func(string) error { return nil },
		openDirectory: func(string) (syncCloser, error) {
			events = append(events, "open-directory")
			return directory, nil
		},
	})
	if err := writer.Add(mustKey(t, []byte("a"), 1, storage.KindValue), []byte("value")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := writer.Finish(); err != nil {
		t.Fatalf("Finish with short writes: %v", err)
	}
	wantSuffix := []string{"file-sync", "file-close", "rename", "open-directory", "directory-sync", "directory-close"}
	if len(events) < len(wantSuffix) || !slices.Equal(events[len(events)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("publication events = %v, want suffix %v", events, wantSuffix)
	}
	if _, err := validateTable(file.Bytes()); err != nil {
		t.Fatalf("short-write output invalid: %v", err)
	}
}

func TestWriterFailuresPoison(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected")
	tests := []struct {
		name      string
		configure func(*memoryFile, *fakeDirectory, *publicationOps)
	}{
		{name: "write", configure: func(file *memoryFile, _ *fakeDirectory, _ *publicationOps) { file.writeErr = injected }},
		{name: "sync", configure: func(file *memoryFile, _ *fakeDirectory, _ *publicationOps) { file.syncErr = injected }},
		{name: "close", configure: func(file *memoryFile, _ *fakeDirectory, _ *publicationOps) { file.closeErr = injected }},
		{name: "rename", configure: func(_ *memoryFile, _ *fakeDirectory, ops *publicationOps) {
			ops.rename = func(string, string) error { return injected }
		}},
		{name: "open directory", configure: func(_ *memoryFile, _ *fakeDirectory, ops *publicationOps) {
			ops.openDirectory = func(string) (syncCloser, error) { return nil, injected }
		}},
		{name: "directory sync", configure: func(_ *memoryFile, directory *fakeDirectory, _ *publicationOps) { directory.syncErr = injected }},
		{name: "directory close", configure: func(_ *memoryFile, directory *fakeDirectory, _ *publicationOps) { directory.closeErr = injected }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			file := &memoryFile{}
			directory := &fakeDirectory{}
			ops := publicationOps{
				rename:        func(string, string) error { return nil },
				remove:        func(string) error { return nil },
				openDirectory: func(string) (syncCloser, error) { return directory, nil },
			}
			tc.configure(file, directory, &ops)
			writer := newWriter(file, "/db", "/db/table.tmp", "/db/table.sst", 1, DefaultBlockSize, DefaultRestartInterval, ops)
			key := mustKey(t, []byte("a"), 1, storage.KindValue)
			if err := writer.Add(key, []byte("value")); err != nil {
				t.Fatalf("Add before failure: %v", err)
			}
			_, finishErr := writer.Finish()
			if !errors.Is(finishErr, ErrWriterFailed) || !errors.Is(finishErr, injected) {
				t.Fatalf("Finish error = %v, want poisoned injected error", finishErr)
			}
			if err := writer.Add(mustKey(t, []byte("b"), 1, storage.KindValue), nil); !errors.Is(err, ErrWriterFailed) {
				t.Fatalf("Add after failure error = %v, want ErrWriterFailed", err)
			}
			if _, err := writer.Finish(); !errors.Is(err, ErrWriterFailed) {
				t.Fatalf("repeat Finish error = %v, want ErrWriterFailed", err)
			}
		})
	}
}

func TestTableOffsetOverflowPoisonsWriter(t *testing.T) {
	t.Parallel()

	writer := newMemoryWriter(t, Options{})
	if err := writer.Add(mustKey(t, []byte("a"), 1, storage.KindValue), []byte("value")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	writer.offset = MaxTableSize
	if _, err := writer.Finish(); !errors.Is(err, ErrTableTooLarge) || !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Finish overflow error = %v", err)
	}
}

func TestLargeTableStructuralStress(t *testing.T) {
	if os.Getenv("RIVETDB_STRESS") == "" {
		t.Skip("set RIVETDB_STRESS=1 to run the 100k-entry SSTable gate")
	}
	seed := testutil.Seed(t)
	rng := testutil.RandFromSeed(seed)
	entries := make([]testEntry, 0, 100_000)
	for user := 0; user < 25_000; user++ {
		var prefix [8]byte
		binary.BigEndian.PutUint64(prefix[:], uint64(user))
		for version := uint64(4); version > 0; version-- {
			kind := storage.KindValue
			value := randomBytes(rng, int(rng.Uint64()%128))
			if version == 2 && user%5 == 0 {
				kind = storage.KindDelete
				value = nil
			}
			userKey := append([]byte{0x00, 0xff}, prefix[:]...)
			entries = append(entries, testEntry{key: mustKey(t, userKey, version, kind), value: value})
		}
	}
	first, metadata, _ := buildRealTable(t, 41, Options{}, entries)
	decoded := mustValidate(t, first)
	assertDecodedEntries(t, decoded.entries, entries)
	second, _, _ := buildRealTable(t, 42, Options{}, entries)
	if !bytes.Equal(first, second) {
		t.Fatalf("large table nondeterministic: %x != %x", sha256.Sum256(first), sha256.Sum256(second))
	}
	digest := sha256.Sum256(first)
	t.Logf("entries=%d blocks=%d bytes=%d sha256=%x", metadata.EntryCount, metadata.DataBlockCount, len(first), digest)
}

type testEntry struct {
	key   storage.InternalKey
	value []byte
}

func buildRealTable(t testing.TB, fileNumber uint64, options Options, entries []testEntry) ([]byte, Metadata, string) {
	t.Helper()
	directory := testutil.BenchmarkDir(t)
	writer, err := OpenWriter(directory, fileNumber, options)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Abort() })
	for _, entry := range entries {
		if addErr := writer.Add(entry.key, entry.value); addErr != nil {
			t.Fatalf("Add %s: %v", formatKey(entry.key), addErr)
		}
	}
	metadata, err := writer.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	finalPath := filepath.Join(directory, FileName(fileNumber))
	data, err := os.ReadFile(finalPath) //nolint:gosec // test-owned temporary path
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return data, metadata, finalPath
}

func mustValidate(t testing.TB, data []byte) decodedTable {
	t.Helper()
	decoded, err := validateTable(data)
	if err != nil {
		t.Fatalf("validateTable: %v", err)
	}
	return decoded
}

func assertDecodedEntries(t testing.TB, got []decodedEntry, want []testEntry) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded entry count = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if storage.CompareInternal(got[index].key, want[index].key) != 0 || !bytes.Equal(got[index].value, want[index].value) {
			t.Fatalf("entry %d = %s/%x, want %s/%x", index, formatKey(got[index].key), got[index].value, formatKey(want[index].key), want[index].value)
		}
	}
}

func deterministicEntries(t testing.TB, count int) []testEntry {
	t.Helper()
	entries := make([]testEntry, count)
	for index := range count {
		var userKey [8]byte
		binary.BigEndian.PutUint64(userKey[:], uint64(index/4))
		sequence := uint64(4 - index%4)
		kind := storage.KindValue
		value := bytes.Repeat([]byte{byte(index)}, index%37)
		if index%17 == 0 {
			kind = storage.KindDelete
			value = nil
		}
		entries[index] = testEntry{key: mustKey(t, userKey[:], sequence, kind), value: value}
	}
	return entries
}

func mustKey(t testing.TB, userKey []byte, sequence uint64, kind storage.ValueKind) storage.InternalKey {
	t.Helper()
	key, err := storage.NewInternalKey(userKey, sequence, kind)
	if err != nil {
		t.Fatalf("NewInternalKey: %v", err)
	}
	return key
}

func formatKey(key storage.InternalKey) string {
	return fmt.Sprintf("key=%x seq=%d kind=%d", key.UserKey(), key.Sequence(), key.Kind())
}

func randomBytes(rng *mathrand.Rand, length int) []byte {
	value := make([]byte, length)
	for index := range value {
		value[index] = byte(rng.Uint64())
	}
	return value
}

func seedBits(seed int64) uint64 {
	return uint64(seed) //nolint:gosec // deterministic two's-complement reinterpretation
}

func metadataEqual(left, right Metadata) bool {
	return left.FileNumber == right.FileNumber && left.FileSize == right.FileSize && left.EntryCount == right.EntryCount &&
		left.DeletionCount == right.DeletionCount && left.RawKeyValueBytes == right.RawKeyValueBytes && left.DataBlockCount == right.DataBlockCount &&
		storage.CompareInternal(left.SmallestInternal, right.SmallestInternal) == 0 && storage.CompareInternal(left.LargestInternal, right.LargestInternal) == 0 &&
		bytes.Equal(left.SmallestUser, right.SmallestUser) && bytes.Equal(left.LargestUser, right.LargestUser)
}

func newMemoryWriter(t testing.TB, options Options) *Writer {
	t.Helper()
	blockSize, restartInterval, err := normalizeOptions(options)
	if err != nil {
		t.Fatalf("normalizeOptions: %v", err)
	}
	file := &memoryFile{}
	return newWriter(file, "/db", "/db/table.tmp", "/db/table.sst", 1, blockSize, restartInterval, publicationOps{
		rename:        func(string, string) error { return nil },
		remove:        func(string) error { return nil },
		openDirectory: func(string) (syncCloser, error) { return &fakeDirectory{}, nil },
	})
}

type memoryFile struct {
	bytes.Buffer
	maxWrite int
	writeErr error
	syncErr  error
	closeErr error
	events   *[]string
}

func (f *memoryFile) Write(data []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.maxWrite > 0 && len(data) > f.maxWrite {
		data = data[:f.maxWrite]
	}
	return f.Buffer.Write(data)
}

func (f *memoryFile) Sync() error {
	if f.events != nil {
		*f.events = append(*f.events, "file-sync")
	}
	return f.syncErr
}

func (f *memoryFile) Close() error {
	if f.events != nil {
		*f.events = append(*f.events, "file-close")
	}
	return f.closeErr
}

type fakeDirectory struct {
	syncErr  error
	closeErr error
	events   *[]string
}

func (d *fakeDirectory) Sync() error {
	if d.events != nil {
		*d.events = append(*d.events, "directory-sync")
	}
	return d.syncErr
}

func (d *fakeDirectory) Close() error {
	if d.events != nil {
		*d.events = append(*d.events, "directory-close")
	}
	return d.closeErr
}
