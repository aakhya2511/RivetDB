package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestReaderEmptyFile(t *testing.T) {
	t.Parallel()
	records, reader := scanBytes(t, nil)
	if len(records) != 0 || reader.Err() != nil || reader.ValidEnd() != 0 {
		t.Fatalf("empty scan = records %d error %v end %d", len(records), reader.Err(), reader.ValidEnd())
	}
	if _, truncated := reader.Tail(); truncated {
		t.Fatal("empty WAL reported a truncated tail")
	}
}

func TestWriterReaderRoundTripAndOffsets(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{
		nil,
		{0x00},
		{0x00, 0xff, 0x80, 0x01},
		bytes.Repeat([]byte{0xa5}, BlockSize+137),
		[]byte("last"),
	}
	data, positions := encodeRecords(t, payloads)
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if _, truncated := reader.Tail(); truncated {
		t.Fatal("clean WAL reported a truncated tail")
	}
	if reader.ValidEnd() != int64(len(data)) {
		t.Fatalf("valid end = %d, want %d", reader.ValidEnd(), len(data))
	}
	assertRecords(t, records, payloads)
	for i, record := range records {
		if record.Number != uint64(i) || record.Position != positions[i] {
			t.Errorf("record %d metadata = number %d position %+v, want %d %+v", i, record.Number, record.Position, i, positions[i])
		}
	}
}

func TestWriterPadsBlockTailAndFragments(t *testing.T) {
	t.Parallel()

	first := bytes.Repeat([]byte{1}, BlockSize-HeaderSize-5)
	second := bytes.Repeat([]byte{2}, BlockSize)
	data, positions := encodeRecords(t, [][]byte{first, second})
	if positions[1].Start != BlockSize {
		t.Fatalf("second record starts at %d, want block boundary %d", positions[1].Start, BlockSize)
	}
	for offset := positions[0].End; offset < positions[1].Start; offset++ {
		if data[offset] != 0 {
			t.Fatalf("padding byte %d = %x, want zero", offset, data[offset])
		}
	}
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	assertRecords(t, records, [][]byte{first, second})
}

func TestZeroLengthFirstFragmentAtBlockBoundary(t *testing.T) {
	t.Parallel()

	first := bytes.Repeat([]byte{1}, BlockSize-2*HeaderSize)
	second := []byte("cross-boundary")
	data, positions := encodeRecords(t, [][]byte{first, second})
	if positions[1].Start != BlockSize-HeaderSize {
		t.Fatalf("second record starts at %d, want %d", positions[1].Start, BlockSize-HeaderSize)
	}
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	assertRecords(t, records, [][]byte{first, second})
}

func TestFilesystemCreateSyncCloseReopenAppendRecover(t *testing.T) {
	t.Parallel()
	defer testutil.NoLeaks(t)()

	path := filepath.Join(t.TempDir(), "000001.log")
	writer, openErr := OpenWriter(path, WriterOptions{})
	if openErr != nil {
		t.Fatalf("OpenWriter: %v", openErr)
	}
	for _, payload := range [][]byte{[]byte("A"), []byte("B")} {
		if _, err := writer.Append(payload); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	writer, reopenErr := OpenWriter(path, WriterOptions{})
	if reopenErr != nil {
		t.Fatalf("reopen writer: %v", reopenErr)
	}
	if _, err := writer.Append([]byte("C")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	var got [][]byte
	result, recoverErr := Recover(path, func(record Record) error {
		got = append(got, append([]byte(nil), record.Payload...))
		return nil
	})
	if recoverErr != nil {
		t.Fatalf("Recover: %v", recoverErr)
	}
	assertPayloads(t, got, [][]byte{[]byte("A"), []byte("B"), []byte("C")})
	if result.Records != 3 || result.TailTruncated {
		t.Fatalf("recovery result = %+v, want 3 clean records", result)
	}
}

func TestWriteBatchThroughWALRecovery(t *testing.T) {
	t.Parallel()

	want := storage.WriteBatch{FirstSequence: 41, Mutations: []storage.Mutation{
		{Kind: storage.KindValue, Key: []byte("key"), Value: nil},
		{Kind: storage.KindDelete, Key: []byte{0x00, 0xff}},
	}}
	payload, encodeErr := storage.EncodeWriteBatch(want)
	if encodeErr != nil {
		t.Fatalf("EncodeWriteBatch: %v", encodeErr)
	}
	data, _ := encodeRecords(t, [][]byte{payload})
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan WAL: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("recovered %d records, want 1", len(records))
	}
	got, decodeErr := storage.DecodeWriteBatch(records[0].Payload)
	if decodeErr != nil {
		t.Fatalf("DecodeWriteBatch: %v", decodeErr)
	}
	if got.FirstSequence != want.FirstSequence || len(got.Mutations) != len(want.Mutations) {
		t.Fatalf("decoded batch header = %+v, want %+v", got, want)
	}
}

func TestThousandsOfRecordsRoundTrip(t *testing.T) {
	t.Parallel()

	const count = 3_000
	payloads := make([][]byte, count)
	for i := range payloads {
		payloads[i] = []byte(fmt.Sprintf("record-%04d-%c", i, byte(i)))
	}
	data, _ := encodeRecords(t, payloads)
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	assertRecords(t, records, payloads)
}

func TestRecoveryRepairAppendLifecycle(t *testing.T) {
	t.Parallel()
	defer testutil.NoLeaks(t)()

	path := filepath.Join(t.TempDir(), "000001.log")
	writer, openErr := OpenWriter(path, WriterOptions{Durability: SyncNone})
	if openErr != nil {
		t.Fatalf("OpenWriter: %v", openErr)
	}
	for _, payload := range [][]byte{[]byte("A"), []byte("B")} {
		if _, err := writer.Append(payload); err != nil {
			t.Fatalf("append durable prefix: %v", err)
		}
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("sync A/B: %v", err)
	}
	cPosition, appendCErr := writer.Append(bytes.Repeat([]byte("C"), 100))
	if appendCErr != nil {
		t.Fatalf("append uncommitted C: %v", appendCErr)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close before simulated crash: %v", err)
	}
	cut := cPosition.Start + HeaderSize + 17
	if err := os.Truncate(path, cut); err != nil {
		t.Fatalf("truncate C: %v", err)
	}

	got, recovered := recoverPayloads(t, path)
	assertPayloads(t, got, [][]byte{[]byte("A"), []byte("B")})
	if !recovered.TailTruncated || recovered.ValidEnd != cPosition.Start {
		t.Fatalf("recovery = %+v, want tail at %d", recovered, cPosition.Start)
	}
	if _, err := OpenWriter(path, WriterOptions{}); !errors.Is(err, ErrTruncatedTail) {
		t.Fatalf("OpenWriter before repair error = %v, want ErrTruncatedTail", err)
	}
	if err := RepairTail(path, recovered); err != nil {
		t.Fatalf("RepairTail: %v", err)
	}

	writer, reopenErr := OpenWriter(path, WriterOptions{})
	if reopenErr != nil {
		t.Fatalf("OpenWriter after repair: %v", reopenErr)
	}
	for _, payload := range [][]byte{[]byte("D"), []byte("E")} {
		if _, err := writer.Append(payload); err != nil {
			t.Fatalf("append after repair: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close repaired WAL: %v", err)
	}

	got, final := recoverPayloads(t, path)
	assertPayloads(t, got, [][]byte{[]byte("A"), []byte("B"), []byte("D"), []byte("E")})
	if final.TailTruncated {
		t.Fatal("repaired and appended WAL still reports a tail")
	}
}

func TestRecoveryIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "000001.log")
	writer, err := OpenWriter(path, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if _, err := writer.Append([]byte("stable")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	firstPayloads, first := recoverPayloads(t, path)
	secondPayloads, second := recoverPayloads(t, path)
	assertPayloads(t, secondPayloads, firstPayloads)
	if first != second {
		t.Fatalf("recovery changed: first %+v second %+v", first, second)
	}
}

func TestExhaustiveTruncationReturnsMaximalPrefix(t *testing.T) {
	payloads := [][]byte{
		[]byte("first"),
		bytes.Repeat([]byte{0x7e}, BlockSize+113),
		[]byte("last"),
	}
	data, positions := encodeRecords(t, payloads)
	cleanCut := map[int64]bool{0: true, int64(len(data)): true}
	for _, position := range positions {
		cleanCut[position.Start] = true
		cleanCut[position.End] = true
	}

	for cut := range len(data) + 1 {
		reader, err := NewReader(bytes.NewReader(data[:cut]), int64(cut))
		if err != nil {
			t.Fatalf("cut %d: NewReader: %v", cut, err)
		}
		var got []Record
		for reader.Next() {
			got = append(got, reader.Record())
		}
		if err := reader.Err(); err != nil {
			t.Fatalf("cut %d: pure truncation classified as corruption: %v", cut, err)
		}
		expected := 0
		var validEnd int64
		for i, position := range positions {
			if position.End <= int64(cut) {
				expected = i + 1
				validEnd = position.End
			}
		}
		if len(got) != expected {
			t.Fatalf("cut %d: recovered %d records, want %d", cut, len(got), expected)
		}
		for i := range got {
			if !bytes.Equal(got[i].Payload, payloads[i]) {
				t.Fatalf("cut %d: record %d was invented or partial", cut, i)
			}
		}
		if reader.ValidEnd() != validEnd {
			t.Fatalf("cut %d: valid end = %d, want %d", cut, reader.ValidEnd(), validEnd)
		}
		_, truncated := reader.Tail()
		if truncated == cleanCut[int64(cut)] {
			t.Fatalf("cut %d: truncated = %t, clean boundary = %t", cut, truncated, cleanCut[int64(cut)])
		}
	}
}

func TestBitFlipInEveryProtectedByteStopsAtCorruption(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{[]byte("before"), []byte("protected-middle"), []byte("after")}
	data, positions := encodeRecords(t, payloads)
	middle := positions[1]
	for offset := middle.Start; offset < middle.End; offset++ {
		mutated := append([]byte(nil), data...)
		mutated[offset] ^= 0x01
		records, reader := scanBytes(t, mutated)
		if !errors.Is(reader.Err(), ErrCorruptWAL) {
			t.Fatalf("flip offset %d: error = %v, want ErrCorruptWAL", offset, reader.Err())
		}
		if len(records) != 1 || !bytes.Equal(records[0].Payload, payloads[0]) {
			t.Fatalf("flip offset %d: returned corrupted or later record: %+v", offset, records)
		}
		if _, truncated := reader.Tail(); truncated {
			t.Fatalf("flip offset %d: corruption misclassified as tail", offset)
		}
	}
}

func TestReaderRejectsMalformedHeadersAndFragmentSequences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "unsupported version", data: rawFrame(0x11, nil, 0), want: ErrUnsupportedVersion},
		{name: "invalid kind", data: rawFrame(0x00, nil, 0), want: ErrInvalidFragmentType},
		{name: "middle without first", data: rawFrame(byte(fragmentMiddle), []byte("x"), 0), want: ErrInvalidFragmentType},
		{name: "last without first", data: rawFrame(byte(fragmentLast), []byte("x"), 0), want: ErrInvalidFragmentType},
		{name: "authenticated oversized physical length", data: rawFrame(byte(fragmentFull), nil, math.MaxUint16), want: ErrInvalidLength},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, reader := scanBytes(t, tc.data)
			if !errors.Is(reader.Err(), tc.want) || !errors.Is(reader.Err(), ErrCorruptWAL) {
				t.Fatalf("error = %v, want %v and ErrCorruptWAL", reader.Err(), tc.want)
			}
		})
	}
}

func TestReaderClassifiesIncompleteHeaderAndPayloadAsTail(t *testing.T) {
	t.Parallel()

	data, _ := encodeRecords(t, [][]byte{[]byte("payload")})
	for _, cut := range []int{1, HeaderSize - 1, HeaderSize, HeaderSize + 3} {
		_, reader := scanBytes(t, data[:cut])
		if err := reader.Err(); err != nil {
			t.Fatalf("cut %d: error = %v", cut, err)
		}
		if _, truncated := reader.Tail(); !truncated {
			t.Fatalf("cut %d: missing truncated-tail classification", cut)
		}
	}
}

func TestReaderRejectsNonzeroPadding(t *testing.T) {
	t.Parallel()

	first := bytes.Repeat([]byte{1}, BlockSize-HeaderSize-5)
	data, positions := encodeRecords(t, [][]byte{first, []byte("next")})
	mutated := append([]byte(nil), data...)
	mutated[positions[0].End+2] = 1
	_, reader := scanBytes(t, mutated)
	if !errors.Is(reader.Err(), ErrCorruptWAL) || !errors.Is(reader.Err(), ErrInvalidPadding) {
		t.Fatalf("padding error = %v, want ErrCorruptWAL and ErrInvalidPadding", reader.Err())
	}
}

func TestReaderChecksumFailuresAreCorruptionEvenAtTail(t *testing.T) {
	t.Parallel()

	data, _ := encodeRecords(t, [][]byte{[]byte("last")})
	tests := []struct {
		offset int
		want   error
	}{
		{offset: 0, want: ErrChecksumMismatch},
		{offset: 4, want: ErrHeaderChecksumMismatch},
		{offset: 8, want: ErrHeaderChecksumMismatch},
		{offset: HeaderSize, want: ErrChecksumMismatch},
	}
	for _, tc := range tests {
		mutated := append([]byte(nil), data...)
		mutated[tc.offset] ^= 1
		_, reader := scanBytes(t, mutated)
		if !errors.Is(reader.Err(), ErrCorruptWAL) || !errors.Is(reader.Err(), tc.want) {
			t.Fatalf("offset %d error = %v, want corruption and %v", tc.offset, reader.Err(), tc.want)
		}
		if _, truncated := reader.Tail(); truncated {
			t.Fatalf("offset %d checksum failure classified as tail", tc.offset)
		}
	}
}

func TestReaderArbitraryBytesNeverPanicOrExceedBounds(t *testing.T) {
	seeds := append([]int64{0, 1, -1, 8134472901}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		rng := testutil.RandFromSeed(seed)
		for range 10_000 {
			data := make([]byte, rng.Uint64()%(2*BlockSize+1))
			for i := range data {
				data[i] = byte(rng.Uint64())
			}
			reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatalf("seed %d: NewReader: %v", seed, err)
			}
			records := 0
			for reader.Next() {
				records++
				if len(reader.Record().Payload) > MaxRecordSize {
					t.Fatalf("seed %d: oversized record returned", seed)
				}
				if records > len(data)/HeaderSize+1 {
					t.Fatalf("seed %d: reader failed to make progress", seed)
				}
			}
		}
	}
}

func TestMaximumRecordAndOversizeBoundary(t *testing.T) {
	payload := make([]byte, MaxRecordSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	data, _ := encodeRecords(t, [][]byte{payload})
	records, reader := scanBytes(t, data)
	if err := reader.Err(); err != nil {
		t.Fatalf("read maximum record: %v", err)
	}
	assertRecords(t, records, [][]byte{payload})

	file := &memoryFile{failAfter: -1}
	writer := newWriter(file, 0, SyncNone)
	if _, err := writer.Append(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversize error = %v, want ErrRecordTooLarge", err)
	}
	if len(file.data) != 0 {
		t.Fatal("oversized record wrote bytes")
	}
}

func TestWriterHandlesShortWrites(t *testing.T) {
	t.Parallel()

	file := &memoryFile{maxWrite: 3, failAfter: -1}
	writer := newWriter(file, 0, SyncBatch)
	position, err := writer.Append([]byte("short-write-safe"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if position.Start != 0 || position.End != int64(len(file.data)) || file.syncs != 1 {
		t.Fatalf("position/sync = %+v/%d, bytes %d", position, file.syncs, len(file.data))
	}
	records, reader := scanBytes(t, file.data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	assertRecords(t, records, [][]byte{[]byte("short-write-safe")})
}

func TestSyncNoneRequiresExplicitDurabilityBoundary(t *testing.T) {
	t.Parallel()

	file := &memoryFile{failAfter: -1}
	writer := newWriter(file, 0, SyncNone)
	if _, err := writer.Append([]byte("record")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if file.syncs != 0 {
		t.Fatalf("SyncNone append issued %d syncs", file.syncs)
	}
	if err := writer.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if file.syncs != 1 {
		t.Fatalf("explicit Sync count = %d, want 1", file.syncs)
	}
}

func TestWriterFailuresPoisonLaterOperations(t *testing.T) {
	t.Parallel()

	writeFailure := errors.New("injected write failure")
	file := &memoryFile{failAfter: 7, writeErr: writeFailure}
	writer := newWriter(file, 0, SyncNone)
	if _, err := writer.Append([]byte("record")); !errors.Is(err, writeFailure) || !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("write error = %v", err)
	}
	if _, err := writer.Append([]byte("later")); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("append after failure = %v, want ErrWriterFailed", err)
	}
	if err := writer.Sync(); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("sync after failure = %v, want ErrWriterFailed", err)
	}

	syncFailure := errors.New("injected sync failure")
	syncFile := &memoryFile{failAfter: -1, syncErr: syncFailure}
	syncWriter := newWriter(syncFile, 0, SyncBatch)
	if _, err := syncWriter.Append([]byte("record")); !errors.Is(err, syncFailure) || !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("sync error = %v", err)
	}
	if _, err := syncWriter.Append([]byte("later")); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("append after sync failure = %v, want ErrWriterFailed", err)
	}
}

func TestFirstDurabilityBoundarySyncsAndClosesDirectory(t *testing.T) {
	t.Parallel()

	var order []string
	file := &memoryFile{failAfter: -1, syncHook: func() { order = append(order, "file") }}
	directory := &memoryFile{failAfter: -1, syncHook: func() { order = append(order, "directory") }}
	writer := newWriterWithDirectory(file, directory, 0, SyncBatch)
	if _, err := writer.Append([]byte("first")); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if file.syncs != 1 || directory.syncs != 1 || directory.closes != 1 {
		t.Fatalf("first boundary sync/close = file %d directory %d/%d", file.syncs, directory.syncs, directory.closes)
	}
	if !slices.Equal(order, []string{"file", "directory"}) {
		t.Fatalf("first durability order = %v, want [file directory]", order)
	}
	if _, err := writer.Append([]byte("second")); err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if file.syncs != 2 || directory.syncs != 1 {
		t.Fatalf("second boundary syncs = file %d directory %d", file.syncs, directory.syncs)
	}
	if !slices.Equal(order, []string{"file", "directory", "file"}) {
		t.Fatalf("second durability order = %v", order)
	}
}

func TestDirectorySyncFailurePoisonsWriter(t *testing.T) {
	t.Parallel()

	want := errors.New("injected directory sync failure")
	file := &memoryFile{failAfter: -1}
	directory := &memoryFile{failAfter: -1, syncErr: want}
	writer := newWriterWithDirectory(file, directory, 0, SyncBatch)
	if _, err := writer.Append([]byte("record")); !errors.Is(err, want) || !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("Append error = %v, want directory cause and ErrWriterFailed", err)
	}
	if _, err := writer.Append([]byte("later")); !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("later Append error = %v, want ErrWriterFailed", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if directory.closes != 1 {
		t.Fatalf("directory closes = %d, want 1", directory.closes)
	}
}

func TestWriterCloseContract(t *testing.T) {
	t.Parallel()

	closeFailure := errors.New("injected close failure")
	file := &memoryFile{failAfter: -1, closeErr: closeFailure}
	writer := newWriter(file, 0, SyncNone)
	if err := writer.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("Close error = %v", err)
	}
	if err := writer.Close(); !errors.Is(err, closeFailure) || file.closes != 1 {
		t.Fatalf("second Close = %v, closes %d", err, file.closes)
	}
	if _, err := writer.Append(nil); !errors.Is(err, ErrClosedWriter) {
		t.Fatalf("Append after Close = %v", err)
	}
	if err := writer.Sync(); !errors.Is(err, ErrClosedWriter) {
		t.Fatalf("Sync after Close = %v", err)
	}
}

func TestConcurrentAppendsAreCompleteAndNoninterleaved(t *testing.T) {
	t.Parallel()

	file := &memoryFile{failAfter: -1}
	writer := newWriter(file, 0, SyncNone)
	const workers = 128
	positions := make([]Position, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for i := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			payload := []byte(fmt.Sprintf("record-%03d", i))
			positions[i], errs[i] = writer.Append(payload)
		}()
	}
	wait.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	slices.SortFunc(positions, func(a, b Position) int { return int(a.Start - b.Start) })
	for i := 1; i < len(positions); i++ {
		if positions[i-1].End > positions[i].Start {
			t.Fatalf("positions overlap: %+v and %+v", positions[i-1], positions[i])
		}
	}
	records, reader := scanBytes(t, file.data)
	if err := reader.Err(); err != nil {
		t.Fatalf("scan concurrent WAL: %v", err)
	}
	if len(records) != workers {
		t.Fatalf("recovered %d records, want %d", len(records), workers)
	}
	seen := make(map[string]bool, workers)
	for _, record := range records {
		seen[string(record.Payload)] = true
	}
	for i := range workers {
		if !seen[fmt.Sprintf("record-%03d", i)] {
			t.Fatalf("missing worker record %d", i)
		}
	}
}

func TestRepairRejectsChangedOrCleanWAL(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "000001.log")
	data, positions := encodeRecords(t, [][]byte{[]byte("A"), []byte("incomplete")})
	cut := positions[1].Start + HeaderSize + 1
	if err := os.WriteFile(path, data[:cut], 0o600); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	_, recovered := recoverPayloads(t, path)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open to change: %v", err)
	}
	if _, err := file.Write([]byte{0}); err != nil {
		t.Fatalf("change WAL: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close changed WAL: %v", err)
	}
	if err := RepairTail(path, recovered); !errors.Is(err, ErrRecoveryStateChanged) {
		t.Fatalf("repair changed WAL = %v, want ErrRecoveryStateChanged", err)
	}

	cleanPath := filepath.Join(t.TempDir(), "clean.log")
	if err := os.WriteFile(cleanPath, nil, 0o600); err != nil {
		t.Fatalf("write clean WAL: %v", err)
	}
	if err := RepairTail(cleanPath, RecoveryResult{}); !errors.Is(err, ErrNoTruncatedTail) {
		t.Fatalf("repair clean WAL = %v, want ErrNoTruncatedTail", err)
	}
}

func TestCorruptionIsNeverRepairableAsTail(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "000001.log")
	data, positions := encodeRecords(t, [][]byte{[]byte("A"), []byte("corrupt")})
	data[positions[1].Start+HeaderSize] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write corrupt WAL: %v", err)
	}
	result, err := Recover(path, nil)
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Recover error = %v, want ErrCorruptWAL", err)
	}
	if result.TailTruncated {
		t.Fatal("corruption returned a tail-repair token")
	}
	if err := RepairTail(path, result); !errors.Is(err, ErrNoTruncatedTail) {
		t.Fatalf("RepairTail corruption result = %v, want ErrNoTruncatedTail", err)
	}
}

func TestRecoverPreservesConsumerError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "000001.log")
	data, _ := encodeRecords(t, [][]byte{[]byte("record")})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write WAL: %v", err)
	}
	want := errors.New("consumer stopped")
	if _, err := Recover(path, func(Record) error { return want }); !errors.Is(err, want) {
		t.Fatalf("Recover error = %v, want consumer cause", err)
	}
}

func TestOpenCloseCyclesReleaseResources(t *testing.T) {
	defer testutil.NoLeaks(t)()

	path := filepath.Join(t.TempDir(), "000001.log")
	for range 100 {
		writer, openErr := OpenWriter(path, WriterOptions{Durability: SyncNone})
		if openErr != nil {
			t.Fatalf("OpenWriter: %v", openErr)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		reader, openReaderErr := OpenReader(path)
		if openReaderErr != nil {
			t.Fatalf("OpenReader: %v", openReaderErr)
		}
		if err := reader.Close(); err != nil {
			t.Fatalf("reader Close: %v", err)
		}
	}
}

func TestOpenWriterRejectsInvalidDurability(t *testing.T) {
	t.Parallel()
	if _, err := OpenWriter(filepath.Join(t.TempDir(), "wal"), WriterOptions{Durability: 99}); !errors.Is(err, ErrInvalidDurability) {
		t.Fatalf("OpenWriter error = %v, want ErrInvalidDurability", err)
	}
}

func TestNewReaderRejectsInvalidConstruction(t *testing.T) {
	t.Parallel()
	if _, err := NewReader(nil, 0); err == nil {
		t.Fatal("NewReader accepted nil ReaderAt")
	}
	if _, err := NewReader(bytes.NewReader(nil), -1); err == nil {
		t.Fatal("NewReader accepted negative size")
	}
}

func TestReaderCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "empty.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("write empty WAL: %v", err)
	}
	reader, err := OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

type memoryFile struct {
	data      []byte
	maxWrite  int
	failAfter int
	writeErr  error
	syncErr   error
	closeErr  error
	syncHook  func()
	syncs     int
	closes    int
}

func (f *memoryFile) Write(data []byte) (int, error) {
	if f.failAfter >= 0 {
		remaining := f.failAfter - len(f.data)
		if remaining <= 0 {
			return 0, f.writeErr
		}
		if len(data) > remaining {
			f.data = append(f.data, data[:remaining]...)
			return remaining, f.writeErr
		}
	}
	length := len(data)
	if f.maxWrite > 0 {
		length = min(length, f.maxWrite)
	}
	f.data = append(f.data, data[:length]...)
	return length, nil
}

func (f *memoryFile) Sync() error {
	f.syncs++
	if f.syncHook != nil {
		f.syncHook()
	}
	return f.syncErr
}

func (f *memoryFile) Close() error {
	f.closes++
	return f.closeErr
}

func encodeRecords(t *testing.T, payloads [][]byte) ([]byte, []Position) {
	t.Helper()
	file := &memoryFile{failAfter: -1}
	writer := newWriter(file, 0, SyncNone)
	positions := make([]Position, len(payloads))
	for i, payload := range payloads {
		position, err := writer.Append(payload)
		if err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
		positions[i] = position
	}
	return append([]byte(nil), file.data...), positions
}

func scanBytes(t *testing.T, data []byte) ([]Record, *Reader) {
	t.Helper()
	reader, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var records []Record
	for reader.Next() {
		records = append(records, reader.Record())
	}
	return records, reader
}

func recoverPayloads(t *testing.T, path string) ([][]byte, RecoveryResult) {
	t.Helper()
	var payloads [][]byte
	result, err := Recover(path, func(record Record) error {
		payloads = append(payloads, append([]byte(nil), record.Payload...))
		return nil
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return payloads, result
}

func assertRecords(t *testing.T, got []Record, want [][]byte) {
	t.Helper()
	payloads := make([][]byte, len(got))
	for i := range got {
		payloads[i] = got[i].Payload
	}
	assertPayloads(t, payloads, want)
}

func assertPayloads(t *testing.T, got, want [][]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d payloads, want %d", len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("payload %d = %x, want %x", i, got[i], want[i])
		}
	}
}

func rawFrame(encodedType byte, payload []byte, declaredLength uint16) []byte {
	if declaredLength == 0 {
		declaredLength = uint16(len(payload)) //nolint:gosec // test payload is tiny
	}
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint16(header[8:10], declaredLength)
	header[10] = encodedType
	headerCRC := crc32.Checksum(header[8:11], checksumTable)
	contentCRC := crc32.Update(headerCRC, checksumTable, payload)
	binary.LittleEndian.PutUint32(header[0:4], contentCRC)
	binary.LittleEndian.PutUint32(header[4:8], headerCRC)
	frame := make([]byte, 0, len(header)+len(payload))
	frame = append(frame, header...)
	return append(frame, payload...)
}

var _ io.Writer = (*memoryFile)(nil)
