package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestReaderSeekGetCandidateAndMetadata(t *testing.T) {
	entries := []testEntry{
		{key: mustKey(t, nil, math.MaxUint64, storage.KindValue), value: []byte("empty")},
		{key: mustKey(t, []byte("a"), 30, storage.KindValue), value: []byte("a30")},
		{key: mustKey(t, []byte("foo"), 30, storage.KindValue), value: []byte("v30")},
		{key: mustKey(t, []byte("foo"), 20, storage.KindDelete)},
		{key: mustKey(t, []byte("foo"), 20, storage.KindValue), value: []byte("v20")},
		{key: mustKey(t, []byte("foo"), 10, storage.KindValue), value: []byte("v10")},
		{key: mustKey(t, []byte{0xff}, 0, storage.KindValue), value: []byte("last")},
	}
	_, written, path := buildRealTable(t, 101, Options{BlockSize: 42, RestartInterval: 2}, entries)
	r := mustOpenReader(t, path)
	defer closeReader(t, r)

	metadata := r.Metadata()
	if !metadataEqual(metadata, written) {
		t.Fatalf("reader metadata = %+v, writer metadata = %+v", metadata, written)
	}
	metadata.SmallestUser = append(metadata.SmallestUser, 1)
	if metadataEqual(metadata, r.Metadata()) {
		t.Fatal("Metadata returned aliased bounds")
	}

	for _, entry := range entries {
		got, err := r.Get(entry.key)
		assertEntry(t, got, err, entry)
	}
	missing := mustKey(t, []byte("missing"), 1, storage.KindValue)
	if _, err := r.Get(missing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing error = %v", err)
	}

	cases := []struct {
		name   string
		target uint64
		want   int
	}{
		{"newer than newest", 40, 2},
		{"equal newest", 30, 2},
		{"between", 25, 3},
		{"equal delete/value sequence", 20, 3},
		{"older", 9, -1},
		{"maximum", math.MaxUint64, 2},
		{"zero", 0, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.GetCandidate([]byte("foo"), tc.target)
			if tc.want < 0 {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("error = %v, want ErrNotFound", err)
				}
				return
			}
			assertEntry(t, got, err, entries[tc.want])
		})
	}
	if err := r.ValidateAll(); err != nil {
		t.Fatalf("ValidateAll: %v", err)
	}
}

func TestReaderSeekCrossBlockMatrixAndVersions(t *testing.T) {
	entries := make([]testEntry, 0, 104)
	entries = append(entries, testEntry{key: mustKey(t, []byte("a"), 1, storage.KindValue), value: []byte("a")})
	for sequence := uint64(100); sequence > 0; sequence-- {
		kind := storage.KindValue
		value := []byte{byte(sequence)}
		if sequence%11 == 0 {
			kind, value = storage.KindDelete, nil
		}
		entries = append(entries, testEntry{key: mustKey(t, []byte("foo"), sequence, kind), value: value})
	}
	entries = append(entries,
		testEntry{key: mustKey(t, []byte("z"), 2, storage.KindValue), value: []byte("z2")},
		testEntry{key: mustKey(t, []byte("z"), 1, storage.KindValue), value: []byte("z1")},
	)
	_, _, path := buildRealTable(t, 102, Options{BlockSize: 64, RestartInterval: 3}, entries)
	r := mustOpenReader(t, path)
	defer closeReader(t, r)
	if len(r.index) < 10 {
		t.Fatalf("block count = %d, want many", len(r.index))
	}

	targets := []storage.InternalKey{
		mustKey(t, nil, math.MaxUint64, storage.KindDelete),
		entries[0].key,
		entries[len(entries)-1].key,
		mustKey(t, []byte("foo"), 101, storage.KindDelete),
		mustKey(t, []byte("foo"), 55, storage.KindDelete),
		mustKey(t, []byte("foo"), 55, storage.KindValue),
		mustKey(t, []byte("foo"), 0, storage.KindValue),
		mustKey(t, []byte{0xff}, 0, storage.KindValue),
	}
	for _, target := range targets {
		want := referenceLowerBound(entries, target)
		got, err := r.Seek(target)
		if want == len(entries) {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Seek(%s) error = %v", formatKey(target), err)
			}
			continue
		}
		assertEntry(t, got, err, entries[want])
	}
	for sequence := uint64(0); sequence <= 101; sequence++ {
		want := referenceCandidate(entries, []byte("foo"), sequence)
		got, err := r.GetCandidate([]byte("foo"), sequence)
		if want < 0 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("candidate %d error = %v", sequence, err)
			}
			continue
		}
		assertEntry(t, got, err, entries[want])
	}
}

func TestReaderIteratorsAndRanges(t *testing.T) {
	entries := deterministicEntries(t, 2_000)
	_, _, path := buildRealTable(t, 103, Options{BlockSize: 96, RestartInterval: 4}, entries)
	r := mustOpenReader(t, path)
	defer closeReader(t, r)

	assertIterator(t, mustNewIterator(t, r), entries)
	from := 777
	assertIterator(t, mustIteratorFrom(t, r, entries[from].key), entries[from:])
	after := mustKey(t, []byte{0xff}, 0, storage.KindValue)
	assertIterator(t, mustIteratorFrom(t, r, after), nil)

	start, end := entries[400].key.UserKey(), entries[1_200].key.UserKey()
	want := referenceRange(entries, start, end)
	assertIterator(t, mustRange(t, r, start, end), want)
	assertIterator(t, mustRange(t, r, nil, nil), entries)
	assertIterator(t, mustRange(t, r, start, start), nil)
	assertIterator(t, mustRange(t, r, nil, []byte{}), nil)
	if _, err := r.Range(end, start); !errors.Is(err, ErrInvalidRange) {
		t.Fatalf("reversed range error = %v", err)
	}
	if _, err := r.Range(make([]byte, MaxUserKeySize+1), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("oversized range error = %v", err)
	}

	it := mustNewIterator(t, r)
	for it.Next() {
		entry, _ := it.Entry()
		entry.Value = append(entry.Value, 0xff)
	}
	if it.Error() != nil || it.Next() || it.Next() {
		t.Fatalf("unstable EOF: error=%v", it.Error())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close iterator: %v", err)
	}
	if it.Next() {
		t.Fatal("closed iterator advanced")
	}
}

func TestRandomizedReaderAgainstReference(t *testing.T) {
	seeds := append([]int64{0, 1, -1, 8134472901, -1234567890123}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runRandomizedReader(t, seed) })
	}
}

func runRandomizedReader(t *testing.T, seed int64) {
	rng := testutil.RandFromSeed(seed)
	byKey := make(map[string]testEntry, 1_000)
	for len(byKey) < 1_000 {
		user := randomBytes(rng, int(rng.Uint64()%24))
		key := mustKey(t, user, rng.Uint64()%128, storage.ValueKind(rng.Uint64()%2))
		value := randomBytes(rng, int(rng.Uint64()%48))
		if key.Kind() == storage.KindDelete {
			value = nil
		}
		byKey[string(key.Encode())] = testEntry{key: key, value: value}
	}
	entries := make([]testEntry, 0, len(byKey))
	for _, entry := range byKey {
		entries = append(entries, entry)
	}
	slices.SortFunc(entries, func(a, b testEntry) int { return storage.CompareInternal(a.key, b.key) })
	_, _, path := buildRealTable(t, seedBits(seed)%MaxFileNumber+1, Options{BlockSize: 128, RestartInterval: 5}, entries)
	r := mustOpenReader(t, path)
	defer closeReader(t, r)
	assertIterator(t, mustNewIterator(t, r), entries)
	for range 5_000 {
		user := randomBytes(rng, int(rng.Uint64()%24))
		if rng.Uint64()%2 == 0 {
			user = entries[rng.Uint64()%uint64(len(entries))].key.UserKey() //nolint:gosec // modulo is slice-bounded
		}
		target := mustKey(t, user, rng.Uint64()%160, storage.ValueKind(rng.Uint64()%2))
		want := referenceLowerBound(entries, target)
		got, err := r.Seek(target)
		if want == len(entries) {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Seek error = %v", err)
			}
		} else {
			assertEntry(t, got, err, entries[want])
		}
		user = randomBytes(rng, int(rng.Uint64()%24))
		if rng.Uint64()%2 == 0 {
			user = entries[rng.Uint64()%uint64(len(entries))].key.UserKey() //nolint:gosec // modulo is slice-bounded
		}
		sequence := rng.Uint64() % 160
		candidate := referenceCandidate(entries, user, sequence)
		got, err = r.GetCandidate(user, sequence)
		if candidate < 0 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("GetCandidate error = %v", err)
			}
		} else {
			assertEntry(t, got, err, entries[candidate])
		}
	}
	for range 500 {
		left, right := randomBytes(rng, int(rng.Uint64()%12)), randomBytes(rng, int(rng.Uint64()%12))
		if bytes.Compare(left, right) > 0 {
			left, right = right, left
		}
		assertIterator(t, mustRange(t, r, left, right), referenceRange(entries, left, right))
	}
}

func TestReaderHostileBytesAndPostOpenCorruption(t *testing.T) {
	entries := deterministicEntries(t, 40)
	data, _, path := buildRealTable(t, 104, Options{BlockSize: 128}, entries)
	valid := mustValidate(t, data)

	handleCases := []struct {
		name   string
		mutate func([]byte)
	}{
		{"max offset", func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerIndexOffset:], math.MaxUint64) }},
		{"max length", func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerIndexLength:], math.MaxUint64) }},
		{"zero index", func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerIndexLength:], 0) }},
		{"index metadata overlap", func(b []byte) {
			binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerMetadataOffset:], valid.indexHandle.Offset)
		}},
		{"metadata into footer", func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerMetadataLength:], 1) }},
		{"absent filter handle", func(b []byte) { binary.LittleEndian.PutUint64(b[len(b)-FooterSize+footerFilterLength:], 1) }},
	}
	for _, tc := range handleCases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := bytes.Clone(data)
			tc.mutate(mutated)
			recomputeFooterChecksum(mutated)
			if _, err := openBytesReader(mutated); err == nil {
				t.Fatal("hostile handle accepted")
			}
		})
	}

	// Full-open validation means every single-byte mutation is rejected before
	// the index can be used. This also campaigns trailer, restart, varint,
	// index, metadata and footer bytes without special-casing their offsets.
	for offset := range data {
		mutated := bytes.Clone(data)
		mutated[offset] ^= 0x80
		if reader, err := openBytesReader(mutated); err == nil {
			_ = reader.Close()
			t.Fatalf("byte mutation at %d accepted", offset)
		}
	}

	r := mustOpenReader(t, path)
	file, err := os.OpenFile(path, os.O_WRONLY, 0) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	mutatedAfterOpen := bytes.Clone(data)
	mutatedAfterOpen[int(valid.dataBlocks[0].handle.Offset)] ^= 1
	recomputeBlockChecksum(mutatedAfterOpen, valid.dataBlocks[0].handle)
	blockEnd := valid.dataBlocks[0].handle.Offset + valid.dataBlocks[0].handle.Length
	changedBlock := mutatedAfterOpen[int(valid.dataBlocks[0].handle.Offset):int(blockEnd)]
	if _, err := file.WriteAt(changedBlock, int64(valid.dataBlocks[0].handle.Offset)); err != nil {
		t.Fatalf("WriteAt corruption: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corruption writer: %v", err)
	}
	if _, err := r.Seek(entries[0].key); !errors.Is(err, ErrChecksum) {
		t.Fatalf("post-open corruption error = %v", err)
	}
	_ = r.Close()
}

func TestReaderHostileVarintsAndRestartEntries(t *testing.T) {
	varintCases := [][]byte{
		{},
		{0x80},
		{0x80, 0x00},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02},
	}
	for _, encoded := range varintCases {
		if _, _, err := readCanonicalUvarint(encoded); err == nil {
			t.Fatalf("hostile varint %x accepted", encoded)
		}
	}
	entries := deterministicEntries(t, 40)
	data, _, _ := buildRealTable(t, 108, Options{BlockSize: 1 << 20, RestartInterval: 2}, entries)
	valid := mustValidate(t, data)
	handle := valid.dataBlocks[0].handle
	mutations := []struct {
		name   string
		mutate func([]byte)
	}{
		{"shared exceeds previous", func(b []byte) { b[int(handle.Offset)] = 0x7f }},
		{"restart shared nonzero", func(b []byte) {
			restart := valid.dataBlocks[0].restarts[1]
			b[int(handle.Offset)+int(restart)] = 1
		}},
		{"restart order", func(b []byte) {
			payloadEnd := int(handle.Offset+handle.Length) - BlockTrailerSize
			count := binary.LittleEndian.Uint32(b[payloadEnd-4:])
			restartStart := payloadEnd - 4 - int(count)*4
			binary.LittleEndian.PutUint32(b[restartStart+4:], 0)
		}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			mutated := bytes.Clone(data)
			tc.mutate(mutated)
			recomputeBlockChecksum(mutated, handle)
			if _, err := openBytesReader(mutated); !errors.Is(err, ErrInvalidBlock) {
				t.Fatalf("error = %v, want ErrInvalidBlock", err)
			}
		})
	}
}

func TestIteratorSurfacesPostOpenBlockCorruption(t *testing.T) {
	data, _, path := buildRealTable(t, 111, Options{BlockSize: 64}, deterministicEntries(t, 200))
	valid := mustValidate(t, data)
	if len(valid.dataBlocks) < 2 {
		t.Fatal("test requires multiple blocks")
	}
	r := mustOpenReader(t, path)
	defer func() { _ = r.Close() }()
	it := mustNewIterator(t, r)
	handle := valid.dataBlocks[len(valid.dataBlocks)-1].handle
	file, err := os.OpenFile(path, os.O_WRONLY, 0) //nolint:gosec // test-owned path
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	mutated := bytes.Clone(data)
	mutated[int(handle.Offset)] ^= 1
	recomputeBlockChecksum(mutated, handle)
	end := handle.Offset + handle.Length
	if _, err := file.WriteAt(mutated[int(handle.Offset):int(end)], int64(handle.Offset)); err != nil {
		t.Fatalf("write corruption: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close corruption writer: %v", err)
	}
	for it.Next() {
	}
	if !errors.Is(it.Error(), ErrChecksum) {
		t.Fatalf("iterator corruption error = %v", it.Error())
	}
}

func TestReaderRandomCorruptionCampaign(t *testing.T) {
	rng := testutil.Rand(t)
	data, _, _ := buildRealTable(t, 109, Options{BlockSize: 128}, deterministicEntries(t, 2_000))
	for mutation := range 1_000 {
		mutated := bytes.Clone(data)
		offset := int(rng.Uint64() % uint64(len(mutated))) //nolint:gosec // modulo is bounded by slice length
		mutated[offset] ^= byte(1 << (rng.Uint64() % 8))
		if reader, err := openBytesReader(mutated); err == nil {
			_ = reader.Close()
			t.Fatalf("random mutation %d at %d accepted", mutation, offset)
		}
	}
}

func TestReaderIndexResourceLimit(t *testing.T) {
	data, _, _ := buildRealTable(t, 110, Options{BlockSize: 32}, deterministicEntries(t, 100))
	file := bytesReadAtCloser{Reader: bytes.NewReader(data)}
	if _, err := openReader(file, uint64(len(data)), "limited.sst", ReaderOptions{MaxIndexBlockSize: BlockTrailerSize}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("resource limit error = %v", err)
	}
}

func TestReaderIOAndCloseSemantics(t *testing.T) {
	data, _, _ := buildRealTable(t, 105, Options{}, deterministicEntries(t, 10))
	injected := errors.New("injected read failure")
	failing := &faultReadAtCloser{data: data, failOffset: 0, readErr: injected}
	if _, err := openReader(failing, uint64(len(data)), "fault.sst", ReaderOptions{}); !errors.Is(err, injected) {
		t.Fatalf("I/O error = %v", err)
	}
	short := &faultReadAtCloser{data: data, short: true}
	if _, err := openReader(short, uint64(len(data)), "short.sst", ReaderOptions{}); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("short read error = %v", err)
	}
	r, err := openBytesReader(data)
	if err != nil {
		t.Fatalf("openBytesReader: %v", err)
	}
	it := mustNewIterator(t, r)
	if closeErr := r.Close(); closeErr != nil || r.Close() != nil {
		t.Fatalf("idempotent Close = %v", closeErr)
	}
	key := deterministicEntries(t, 1)[0].key
	if _, getErr := r.Get(key); !errors.Is(getErr, ErrClosed) {
		t.Fatalf("Get after Close = %v", getErr)
	}
	if _, iteratorErr := r.NewIterator(); !errors.Is(iteratorErr, ErrClosed) {
		t.Fatalf("iterator after Close = %v", iteratorErr)
	}
	if it.Next() || !errors.Is(it.Error(), ErrClosed) {
		t.Fatalf("existing iterator after Close: next=%v error=%v", it.Next(), it.Error())
	}

	closeFailure := errors.New("close failure")
	file := &faultReadAtCloser{data: data, closeErr: closeFailure}
	r, err = openReader(file, uint64(len(data)), "close.sst", ReaderOptions{})
	if err != nil {
		t.Fatalf("open close-failure reader: %v", err)
	}
	if err := r.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("close error = %v", err)
	}
}

func TestReaderConcurrentOperationsAndClose(t *testing.T) {
	entries := deterministicEntries(t, 5_000)
	_, _, path := buildRealTable(t, 106, Options{BlockSize: 256}, entries)
	r := mustOpenReader(t, path)
	var wait sync.WaitGroup
	for worker := range 32 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for step := range 200 {
				position := (worker*131 + step*17) % len(entries)
				if got, err := r.Get(entries[position].key); err == nil {
					if storage.CompareInternal(got.Key, entries[position].key) != 0 {
						t.Errorf("wrong concurrent Get result")
					}
				} else if !errors.Is(err, ErrClosed) {
					t.Errorf("concurrent Get: %v", err)
				}
				it, err := r.IteratorFrom(entries[position].key)
				if err == nil {
					_ = it.Next()
					_ = it.Close()
				} else if !errors.Is(err, ErrClosed) {
					t.Errorf("concurrent iterator: %v", err)
				}
			}
		}(worker)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wait.Wait()
}

func TestReaderLargeTableStress(t *testing.T) {
	if os.Getenv("RIVETDB_STRESS") == "" {
		t.Skip("set RIVETDB_STRESS=1 for the 100,000-entry reader gate")
	}
	rng := testutil.RandFromSeed(testutil.Seed(t))
	entries := make([]testEntry, 0, 100_000)
	for user := range 25_000 {
		var prefix [8]byte
		binary.BigEndian.PutUint64(prefix[:], uint64(user)) //nolint:gosec // bounded non-negative loop index
		for version := uint64(4); version > 0; version-- {
			kind := storage.KindValue
			value := randomBytes(rng, int(rng.Uint64()%128))
			if version == 2 && user%5 == 0 {
				kind, value = storage.KindDelete, nil
			}
			userKey := append([]byte{0x00, 0xff}, prefix[:]...)
			entries = append(entries, testEntry{key: mustKey(t, userKey, version, kind), value: value})
		}
	}
	_, metadata, path := buildRealTable(t, 107, Options{}, entries)
	r := mustOpenReader(t, path)
	defer closeReader(t, r)
	if err := r.ValidateAll(); err != nil {
		t.Fatalf("ValidateAll: %v", err)
	}
	assertIterator(t, mustNewIterator(t, r), entries)
	for position := 0; position < len(entries); position += 97 {
		got, err := r.Seek(entries[position].key)
		assertEntry(t, got, err, entries[position])
		candidate := referenceCandidate(entries, entries[position].key.UserKey(), entries[position].key.Sequence())
		got, err = r.GetCandidate(entries[position].key.UserKey(), entries[position].key.Sequence())
		assertEntry(t, got, err, entries[candidate])
	}
	assertIterator(t, mustRange(t, r, entries[25_000].key.UserKey(), entries[75_000].key.UserKey()), entries[25_000:75_000])
	var wait sync.WaitGroup
	for worker := range 8 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for step := range 100 {
				position := (worker*997 + step*101) % len(entries)
				if _, err := r.Get(entries[position].key); err != nil {
					t.Errorf("large concurrent Get: %v", err)
				}
			}
		}(worker)
	}
	wait.Wait()
	t.Logf("entries=%d blocks=%d bytes=%d", metadata.EntryCount, metadata.DataBlockCount, metadata.FileSize)
}

type faultReadAtCloser struct {
	data       []byte
	failOffset int64
	readErr    error
	closeErr   error
	short      bool
}

func (f *faultReadAtCloser) ReadAt(p []byte, offset int64) (int, error) {
	if f.readErr != nil && offset == f.failOffset {
		return 0, f.readErr
	}
	if offset < 0 || offset >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[offset:])
	if f.short && n > 0 {
		n--
	}
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *faultReadAtCloser) Close() error { return f.closeErr }

func openBytesReader(data []byte) (*Reader, error) {
	return openReader(bytesReadAtCloser{Reader: bytes.NewReader(data)}, uint64(len(data)), "memory.sst", ReaderOptions{})
}

func mustOpenReader(t testing.TB, path string) *Reader {
	t.Helper()
	r, err := Open(path, ReaderOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return r
}

func closeReader(t testing.TB, r *Reader) {
	t.Helper()
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func mustIterator(t testing.TB, iterator *Iterator, err error) *Iterator {
	t.Helper()
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return iterator
}

func mustNewIterator(t testing.TB, r *Reader) *Iterator {
	t.Helper()
	iterator, err := r.NewIterator()
	return mustIterator(t, iterator, err)
}

func mustIteratorFrom(t testing.TB, r *Reader, key storage.InternalKey) *Iterator {
	t.Helper()
	iterator, err := r.IteratorFrom(key)
	return mustIterator(t, iterator, err)
}

func mustRange(t testing.TB, r *Reader, start, end []byte) *Iterator {
	t.Helper()
	iterator, err := r.Range(start, end)
	return mustIterator(t, iterator, err)
}

func assertIterator(t testing.TB, iterator *Iterator, want []testEntry) {
	t.Helper()
	defer func() { _ = iterator.Close() }()
	position := 0
	for iterator.Next() {
		entry, ok := iterator.Entry()
		if !ok || position >= len(want) {
			t.Fatalf("unexpected iterator entry %d", position)
		}
		assertEntry(t, entry, nil, want[position])
		position++
	}
	if err := iterator.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if position != len(want) {
		t.Fatalf("iterator count = %d, want %d", position, len(want))
	}
}

func assertEntry(t testing.TB, got Entry, err error, want testEntry) {
	t.Helper()
	if err != nil {
		t.Fatalf("entry error: %v", err)
	}
	if storage.CompareInternal(got.Key, want.key) != 0 || !bytes.Equal(got.Value, want.value) {
		t.Fatalf("entry = %s/%x, want %s/%x", formatKey(got.Key), got.Value, formatKey(want.key), want.value)
	}
}

func referenceLowerBound(entries []testEntry, target storage.InternalKey) int {
	left, right := 0, len(entries)
	for left < right {
		middle := left + (right-left)/2
		if storage.CompareInternal(entries[middle].key, target) < 0 {
			left = middle + 1
		} else {
			right = middle
		}
	}
	return left
}

func referenceCandidate(entries []testEntry, user []byte, sequence uint64) int {
	key, _ := storage.NewInternalKey(user, sequence, storage.KindDelete)
	position := referenceLowerBound(entries, key)
	if position == len(entries) || !bytes.Equal(entries[position].key.UserKey(), user) {
		return -1
	}
	return position
}

func referenceRange(entries []testEntry, start, end []byte) []testEntry {
	result := make([]testEntry, 0)
	for _, entry := range entries {
		user := entry.key.UserKey()
		if (start == nil || bytes.Compare(user, start) >= 0) && (end == nil || bytes.Compare(user, end) < 0) {
			result = append(result, entry)
		}
	}
	return result
}
