package memtable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestEmptyMemTable(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	key := mustKey(t, []byte("a"), 1, storage.KindValue)
	if _, ok := table.Get(key); ok {
		t.Fatal("Get found an entry in an empty table")
	}
	if _, ok := table.Seek(key); ok {
		t.Fatal("Seek found an entry in an empty table")
	}
	if _, ok := table.GetCandidate([]byte("a"), 1); ok {
		t.Fatal("GetCandidate found an entry in an empty table")
	}
	if table.Len() != 0 {
		t.Fatalf("Len = %d, want 0", table.Len())
	}
	assertIteratorEntries(t, table.Iterator(), nil)
	table.Validate()
}

func TestInsertGetReplaceAndVersions(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	newer := mustKey(t, []byte("key"), 9, storage.KindValue)
	older := mustKey(t, []byte("key"), 3, storage.KindValue)
	deletion := mustKey(t, []byte("key"), 9, storage.KindDelete)

	mustInsert(t, table, older, []byte("old"))
	mustInsert(t, table, newer, []byte("new"))
	mustInsert(t, table, deletion, nil)
	mustInsert(t, table, newer, []byte("replacement"))

	if table.Len() != 3 {
		t.Fatalf("Len = %d, want 3", table.Len())
	}
	got, ok := table.Get(newer)
	if !ok || string(got.Value) != "replacement" {
		t.Fatalf("Get replacement = %q, %v", got.Value, ok)
	}
	assertIteratorEntries(t, table.Iterator(), []Entry{
		{Key: deletion},
		{Key: newer, Value: []byte("replacement")},
		{Key: older, Value: []byte("old")},
	})
	table.Validate()
}

func TestDeleteDiffersFromEmptyValue(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	deletion := mustKey(t, []byte("k"), 7, storage.KindDelete)
	emptyValue := mustKey(t, []byte("k"), 7, storage.KindValue)
	mustInsert(t, table, deletion, nil)
	mustInsert(t, table, emptyValue, []byte{})

	if err := table.Insert(deletion, []byte("not allowed")); !errors.Is(err, ErrDeleteHasValue) {
		t.Fatalf("delete with value error = %v, want ErrDeleteHasValue", err)
	}
	entries := collect(t, table.Iterator())
	if len(entries) != 2 || entries[0].Key.Kind() != storage.KindDelete || entries[1].Key.Kind() != storage.KindValue {
		t.Fatalf("delete/empty value entries = %s", formatEntries(entries))
	}
}

func TestBinaryPrefixAndVersionOrdering(t *testing.T) {
	t.Parallel()

	inputs := []struct {
		user []byte
		seq  uint64
		kind storage.ValueKind
	}{
		{user: []byte{0xff, 0x00}, seq: 0, kind: storage.KindValue},
		{user: bytes.Repeat([]byte{0x80}, 4096), seq: 5, kind: storage.KindValue},
		{user: []byte{0x00}, seq: math.MaxUint64, kind: storage.KindValue},
		{user: nil, seq: 0, kind: storage.KindDelete},
		{user: []byte{0x00, 0x00}, seq: 5, kind: storage.KindValue},
		{user: []byte{0x00}, seq: math.MaxUint64, kind: storage.KindDelete},
		{user: []byte{0xff}, seq: 1, kind: storage.KindValue},
		{user: []byte{0x00}, seq: 1, kind: storage.KindValue},
	}

	table := deterministicTable()
	want := make([]Entry, 0, len(inputs))
	for i := len(inputs) - 1; i >= 0; i-- {
		key := mustKey(t, inputs[i].user, inputs[i].seq, inputs[i].kind)
		value := []byte{byte(i)}
		if inputs[i].kind == storage.KindDelete {
			value = nil
		}
		mustInsert(t, table, key, value)
		want = append(want, Entry{Key: key, Value: value})
	}
	slices.SortFunc(want, compareEntries)
	assertIteratorEntries(t, table.Iterator(), want)
	table.Validate()
}

func TestComparatorPrefixCounterexample(t *testing.T) {
	t.Parallel()

	shorter := mustKey(t, nil, 0, storage.KindDelete)
	longer := mustKey(t, []byte{0x00}, math.MaxUint64, storage.KindValue)
	if bytes.Compare(shorter.Encode(), longer.Encode()) <= 0 {
		t.Fatal("test does not exercise the raw-encoding counterexample")
	}

	table := deterministicTable()
	mustInsert(t, table, longer, []byte("longer"))
	mustInsert(t, table, shorter, nil)
	entries := collect(t, table.Iterator())
	if len(entries) != 2 || storage.CompareInternal(entries[0].Key, shorter) != 0 {
		t.Fatalf("MemTable followed encoded order: %s", formatEntries(entries))
	}
}

func TestSeekLowerBound(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	keys := []storage.InternalKey{
		mustKey(t, []byte("a"), 9, storage.KindValue),
		mustKey(t, []byte("a"), 3, storage.KindValue),
		mustKey(t, []byte("c"), 7, storage.KindValue),
	}
	for _, key := range keys {
		mustInsert(t, table, key, key.UserKey())
	}

	tests := []struct {
		name string
		seek storage.InternalKey
		want *storage.InternalKey
	}{
		{name: "before first", seek: mustKey(t, nil, math.MaxUint64, storage.KindDelete), want: &keys[0]},
		{name: "exact", seek: keys[1], want: &keys[1]},
		{name: "between users", seek: mustKey(t, []byte("b"), 4, storage.KindValue), want: &keys[2]},
		{name: "after last", seek: mustKey(t, []byte("z"), 0, storage.KindValue)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := table.Seek(tc.seek)
			if tc.want == nil {
				if ok {
					t.Fatalf("Seek unexpectedly found %x", got.Key.UserKey())
				}
				return
			}
			if !ok || storage.CompareInternal(got.Key, *tc.want) != 0 {
				t.Fatalf("Seek = %s, %v, want %s", formatEntry(got), ok, formatKey(*tc.want))
			}
		})
	}
}

func TestGetCandidateBySequence(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	for _, seq := range []uint64{20, 10, 5} {
		mustInsert(t, table, mustKey(t, []byte("k"), seq, storage.KindValue), []byte(fmt.Sprint(seq)))
	}

	tests := []struct {
		name   string
		target uint64
		want   uint64
		found  bool
	}{
		{name: "newer than newest", target: 30, want: 20, found: true},
		{name: "exact", target: 10, want: 10, found: true},
		{name: "between", target: 17, want: 10, found: true},
		{name: "older than oldest", target: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := table.GetCandidate([]byte("k"), tc.target)
			if ok != tc.found || ok && got.Key.Sequence() != tc.want {
				t.Fatalf("GetCandidate(%d) = seq %d, %v, want %d, %v", tc.target, got.Key.Sequence(), ok, tc.want, tc.found)
			}
		})
	}
	if _, ok := table.GetCandidate([]byte("missing"), math.MaxUint64); ok {
		t.Fatal("GetCandidate found a different user key")
	}
}

func TestRangeUsesHalfOpenUserKeyBounds(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	for _, user := range [][]byte{nil, []byte("a"), []byte("m"), []byte("z")} {
		for _, seq := range []uint64{9, 1} {
			mustInsert(t, table, mustKey(t, user, seq, storage.KindValue), append(bytes.Clone(user), byte(seq)))
		}
	}

	it, err := table.Range([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	entries := collect(t, it)
	if len(entries) != 4 {
		t.Fatalf("[a,z) returned %d entries, want 4: %s", len(entries), formatEntries(entries))
	}
	for _, entry := range entries {
		if !bytes.Equal(entry.Key.UserKey(), []byte("a")) && !bytes.Equal(entry.Key.UserKey(), []byte("m")) {
			t.Fatalf("out-of-range entry %s", formatEntry(entry))
		}
	}

	unbounded, err := table.Range(nil, nil)
	if err != nil {
		t.Fatalf("unbounded Range: %v", err)
	}
	if got := len(collect(t, unbounded)); got != 8 {
		t.Fatalf("unbounded range count = %d, want 8", got)
	}
	empty, err := table.Range([]byte("m"), []byte("m"))
	if err != nil {
		t.Fatalf("empty Range: %v", err)
	}
	assertIteratorEntries(t, empty, nil)
	if _, rangeErr := table.Range([]byte("z"), []byte("a")); !errors.Is(rangeErr, ErrInvalidRange) {
		t.Fatalf("reversed range error = %v, want ErrInvalidRange", rangeErr)
	}
	emptyUpper, err := table.Range(nil, []byte{})
	if err != nil {
		t.Fatalf("empty upper Range: %v", err)
	}
	assertIteratorEntries(t, emptyUpper, nil)
}

func TestIteratorSnapshotAndExhaustion(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	a := mustKey(t, []byte("a"), 1, storage.KindValue)
	b := mustKey(t, []byte("b"), 1, storage.KindValue)
	mustInsert(t, table, a, []byte("old"))
	it := table.Iterator()
	mustInsert(t, table, a, []byte("new"))
	mustInsert(t, table, b, []byte("b"))
	table.Freeze()

	entries := collect(t, it)
	if len(entries) != 1 || string(entries[0].Value) != "old" {
		t.Fatalf("snapshot entries = %s, want old a only", formatEntries(entries))
	}
	if it.Next() {
		t.Fatal("Next became true after exhaustion")
	}
	if _, ok := it.Entry(); ok {
		t.Fatal("Entry valid after exhaustion")
	}
	assertIteratorEntries(t, table.IteratorFrom(b), []Entry{{Key: b, Value: []byte("b")}})
}

func TestOwnershipCopiesInputsAndOutputs(t *testing.T) {
	t.Parallel()

	userKey := []byte{0x00, 0xff, 0x01}
	value := []byte("original")
	key := mustKey(t, userKey, 4, storage.KindValue)
	table := deterministicTable()
	mustInsert(t, table, key, value)

	for i := range userKey {
		userKey[i] = 0x55
	}
	for i := range value {
		value[i] = 'x'
	}
	got, ok := table.Get(key)
	if !ok || !bytes.Equal(got.Key.UserKey(), []byte{0x00, 0xff, 0x01}) || string(got.Value) != "original" {
		t.Fatalf("stored entry changed through input: %s, %v", formatEntry(got), ok)
	}
	got.Value[0] = 'X'
	again, ok := table.Get(key)
	if !ok || string(again.Value) != "original" {
		t.Fatalf("stored value changed through output: %q, %v", again.Value, ok)
	}
}

func TestFreezeIsPermanentAndIdempotent(t *testing.T) {
	t.Parallel()

	table := deterministicTable()
	key := mustKey(t, []byte("a"), 1, storage.KindValue)
	mustInsert(t, table, key, []byte("a"))
	before := table.SizeBytes()
	table.Freeze()
	table.Freeze()
	if !table.Frozen() {
		t.Fatal("Frozen = false after Freeze")
	}
	if err := table.Insert(mustKey(t, []byte("b"), 1, storage.KindValue), []byte("b")); !errors.Is(err, ErrFrozen) {
		t.Fatalf("Insert after Freeze error = %v, want ErrFrozen", err)
	}
	if table.SizeBytes() != before || table.Len() != 1 {
		t.Fatalf("frozen table changed: size %d/%d len %d", table.SizeBytes(), before, table.Len())
	}
	if _, ok := table.Get(key); !ok {
		t.Fatal("read failed after Freeze")
	}
	assertIteratorEntries(t, table.Iterator(), []Entry{{Key: key, Value: []byte("a")}})
}

func TestMemoryAccounting(t *testing.T) {
	t.Parallel()

	levels := []uint64{1, 4, 1}
	next := 0
	table := newWithRandom(func() uint64 {
		value := levels[next]
		next++
		return value
	})
	if got := table.SizeBytes(); got != emptyTableBytes {
		t.Fatalf("empty SizeBytes = %d, want %d", got, emptyTableBytes)
	}

	first := mustKey(t, []byte("abc"), 1, storage.KindValue)
	mustInsert(t, table, first, []byte("value"))
	want := emptyTableBytes + nodeMetadataBytes + 3 + 5 + wordBytes
	if got := table.SizeBytes(); got != want {
		t.Fatalf("SizeBytes after first = %d, want %d", got, want)
	}

	beforeShortReplace := table.SizeBytes()
	mustInsert(t, table, first, []byte("x"))
	if got := table.SizeBytes(); got != beforeShortReplace {
		t.Fatalf("short replacement size = %d, want stable %d", got, beforeShortReplace)
	}
	mustInsert(t, table, first, bytes.Repeat([]byte("v"), 1000))
	if got := table.SizeBytes(); got != beforeShortReplace+995 {
		t.Fatalf("growing replacement size = %d, want %d", got, beforeShortReplace+995)
	}

	second := mustKey(t, bytes.Repeat([]byte{0xff}, 4096), 2, storage.KindValue)
	beforeSecond := table.SizeBytes()
	mustInsert(t, table, second, bytes.Repeat([]byte{0x80}, 8192))
	// random value 4 gives height 2: low two bits promote once, then stop.
	wantDelta := nodeMetadataBytes + 4096 + 8192 + 2*wordBytes
	if got := table.SizeBytes(); got != beforeSecond+wantDelta {
		t.Fatalf("large insert size delta = %d, want %d", got-beforeSecond, wantDelta)
	}
	if !table.ReachedSize(table.SizeBytes()) || table.ReachedSize(table.SizeBytes()+1) {
		t.Fatal("ReachedSize does not use an inclusive threshold")
	}
	table.Freeze()
	if got := table.SizeBytes(); got != beforeSecond+wantDelta {
		t.Fatalf("size changed on Freeze: %d", got)
	}
	table.Validate()
}

func TestSaturatingMemoryArithmetic(t *testing.T) {
	t.Parallel()

	if got := saturatingAdd(math.MaxUint64-5, 6); got != math.MaxUint64 {
		t.Fatalf("saturatingAdd overflow = %d, want MaxUint64", got)
	}
	if got := saturatingAdd(math.MaxUint64-5, 5); got != math.MaxUint64 {
		t.Fatalf("saturatingAdd exact = %d, want MaxUint64", got)
	}
}

func TestRandomizedReferenceModel(t *testing.T) {
	seeds := append([]int64{0, 1, -1, 8134472901, -1234567890123}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			runReferenceScenario(t, seed, 5_000)
		})
	}
}

func runReferenceScenario(t *testing.T, seed int64, operations int) {
	t.Helper()

	rng := testutil.RandFromSeed(seed)
	table := newWithRandom(rng.Uint64)
	model := make(map[string]Entry)
	for operation := range operations {
		userKey := randomBytes(rng, int(rng.Uint64()%40))
		if operation%11 == 0 {
			userKey = append([]byte{0x00, 0xff}, userKey...)
		}
		sequence := rng.Uint64() % 128
		kind := storage.ValueKind(rng.Uint64() % 2)
		key := mustKey(t, userKey, sequence, kind)
		value := randomBytes(rng, int(rng.Uint64()%96))
		if kind == storage.KindDelete {
			value = nil
		}
		mustInsert(t, table, key, value)
		model[string(key.Encode())] = Entry{Key: key, Value: bytes.Clone(value)}

		if operation%97 == 0 {
			assertAgainstModel(t, table, model, rng)
			table.Validate()
		}
	}
	assertAgainstModel(t, table, model, rng)
	table.Freeze()
	table.Validate()
}

func assertAgainstModel(t *testing.T, table *MemTable, model map[string]Entry, rng *mathrand.Rand) {
	t.Helper()

	want := make([]Entry, 0, len(model))
	for _, entry := range model {
		want = append(want, entry)
	}
	slices.SortFunc(want, compareEntries)
	assertIteratorEntries(t, table.Iterator(), want)
	if table.Len() != len(want) {
		t.Fatalf("Len = %d, want %d", table.Len(), len(want))
	}

	if len(want) != 0 {
		exact := want[int(rng.Uint64()%uint64(len(want)))]
		got, ok := table.Get(exact.Key)
		if !ok || compareEntries(got, exact) != 0 || !bytes.Equal(got.Value, exact.Value) {
			t.Fatalf("exact Get = %s, %v, want %s", formatEntry(got), ok, formatEntry(exact))
		}
	}

	seek := mustKey(t, randomBytes(rng, int(rng.Uint64()%32)), rng.Uint64()%128, storage.ValueKind(rng.Uint64()%2))
	index, found := slices.BinarySearchFunc(want, seek, func(entry Entry, key storage.InternalKey) int {
		return storage.CompareInternal(entry.Key, key)
	})
	_ = found
	got, ok := table.Seek(seek)
	if index == len(want) {
		if ok {
			t.Fatalf("Seek(%s) = %s, want none", formatKey(seek), formatEntry(got))
		}
	} else if !ok || storage.CompareInternal(got.Key, want[index].Key) != 0 || !bytes.Equal(got.Value, want[index].Value) {
		t.Fatalf("Seek(%s) = %s, %v, want %s", formatKey(seek), formatEntry(got), ok, formatEntry(want[index]))
	}

	candidateUser := randomBytes(rng, int(rng.Uint64()%24))
	if len(want) != 0 && rng.Uint64()%2 == 0 {
		candidateUser = want[int(rng.Uint64()%uint64(len(want)))].Key.UserKey()
	}
	target := rng.Uint64() % 128
	candidateSeek := mustKey(t, candidateUser, target, storage.KindDelete)
	candidateIndex, _ := slices.BinarySearchFunc(want, candidateSeek, func(entry Entry, key storage.InternalKey) int {
		return storage.CompareInternal(entry.Key, key)
	})
	wantCandidate := candidateIndex < len(want) && bytes.Equal(want[candidateIndex].Key.UserKey(), candidateUser)
	gotCandidate, foundCandidate := table.GetCandidate(candidateUser, target)
	if foundCandidate != wantCandidate {
		t.Fatalf("GetCandidate(%x,%d) found = %v, want %v", candidateUser, target, foundCandidate, wantCandidate)
	}
	if wantCandidate && (storage.CompareInternal(gotCandidate.Key, want[candidateIndex].Key) != 0 || !bytes.Equal(gotCandidate.Value, want[candidateIndex].Value)) {
		t.Fatalf("GetCandidate(%x,%d) = %s, want %s", candidateUser, target, formatEntry(gotCandidate), formatEntry(want[candidateIndex]))
	}

	for index := 1; index < len(want); index++ {
		if storage.CompareInternal(want[index-1].Key, want[index].Key) >= 0 {
			t.Fatalf("reference order is not strict at %d", index)
		}
	}
	assertVersionContiguity(t, want)
}

func TestConcurrentWritersReadersAndIterators(t *testing.T) {
	defer testutil.NoLeaks(t)()

	const (
		writers   = 128
		perWriter = 100
		readers   = 16
	)
	table := New()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for writer := range writers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for item := range perWriter {
				key := concurrentKey(t, writer, item)
				if err := table.Insert(key, []byte{byte(writer), byte(item)}); err != nil {
					t.Errorf("Insert: %v", err)
					return
				}
			}
		}()
	}
	for reader := range readers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for attempt := 0; attempt < 200; attempt++ {
				key := concurrentKey(t, reader%writers, attempt%perWriter)
				table.Get(key)
				table.Seek(key)
				it := table.Iterator()
				if it.Next() {
					it.Entry()
				}
			}
		}()
	}
	close(start)
	workers.Wait()

	if table.Len() != writers*perWriter {
		t.Fatalf("Len = %d, want %d", table.Len(), writers*perWriter)
	}
	for writer := range writers {
		for item := range perWriter {
			if _, ok := table.Get(concurrentKey(t, writer, item)); !ok {
				t.Fatalf("missing writer %d item %d", writer, item)
			}
		}
	}
	entries := collect(t, table.Iterator())
	if len(entries) != writers*perWriter {
		t.Fatalf("iteration count = %d, want %d", len(entries), writers*perWriter)
	}
	assertStrictOrder(t, entries)
	table.Validate()
}

func TestInsertFreezeRace(t *testing.T) {
	defer testutil.NoLeaks(t)()

	const writers = 128
	table := New()
	start := make(chan struct{})
	var accepted atomic.Int64
	var workers sync.WaitGroup
	for writer := range writers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			err := table.Insert(concurrentKey(t, writer, 0), []byte{byte(writer)})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrFrozen):
			default:
				t.Errorf("Insert error = %v", err)
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		table.Freeze()
	}()
	close(start)
	workers.Wait()

	if !table.Frozen() {
		t.Fatal("table is not frozen")
	}
	if got, want := int64(table.Len()), accepted.Load(); got != want {
		t.Fatalf("Len = %d, accepted = %d", got, want)
	}
	if err := table.Insert(concurrentKey(t, writers+1, 0), nil); !errors.Is(err, ErrFrozen) {
		t.Fatalf("post-freeze Insert error = %v, want ErrFrozen", err)
	}
	if got := len(collect(t, table.Iterator())); got != table.Len() {
		t.Fatalf("post-freeze iteration count = %d, Len = %d", got, table.Len())
	}
	table.Validate()
}

func TestLargeStructuralStress(t *testing.T) {
	if os.Getenv("RIVETDB_STRESS") == "" {
		t.Skip("set RIVETDB_STRESS=1 to run the 100k-entry structural gate")
	}

	const operations = 100_000
	seed := testutil.Seed(t)
	rng := testutil.RandFromSeed(seed)
	table := newWithRandom(rng.Uint64)
	model := make(map[string]struct{}, operations)
	for operation := range operations {
		userKey := randomBytes(rng, int(rng.Uint64()%64))
		key := mustKey(t, userKey, rng.Uint64()%4096, storage.ValueKind(rng.Uint64()%2))
		value := randomBytes(rng, int(rng.Uint64()%128))
		if key.Kind() == storage.KindDelete {
			value = nil
		}
		mustInsert(t, table, key, value)
		model[string(key.Encode())] = struct{}{}
		if operation%10_000 == 0 {
			table.Validate()
		}
	}
	if table.Len() != len(model) {
		t.Fatalf("Len = %d, unique model entries = %d", table.Len(), len(model))
	}
	entries := collect(t, table.Iterator())
	assertStrictOrder(t, entries)
	assertVersionContiguity(t, entries)
	table.Validate()
}

func deterministicTable() *MemTable {
	return newWithRandom(func() uint64 { return 1 })
}

func mustKey(t testing.TB, userKey []byte, sequence uint64, kind storage.ValueKind) storage.InternalKey {
	t.Helper()
	key, err := storage.NewInternalKey(userKey, sequence, kind)
	if err != nil {
		t.Fatalf("NewInternalKey: %v", err)
	}
	return key
}

func mustInsert(t testing.TB, table *MemTable, key storage.InternalKey, value []byte) {
	t.Helper()
	if err := table.Insert(key, value); err != nil {
		t.Fatalf("Insert(%s): %v", formatKey(key), err)
	}
}

func collect(t testing.TB, iterator *Iterator) []Entry {
	t.Helper()
	var entries []Entry
	if _, ok := iterator.Entry(); ok {
		t.Fatal("iterator Entry valid before first Next")
	}
	for iterator.Next() {
		entry, ok := iterator.Entry()
		if !ok {
			t.Fatal("iterator Entry invalid after successful Next")
		}
		entries = append(entries, entry)
	}
	return entries
}

func assertIteratorEntries(t testing.TB, iterator *Iterator, want []Entry) {
	t.Helper()
	got := collect(t, iterator)
	if len(got) != len(want) {
		t.Fatalf("iterator length = %d, want %d\ngot: %s\nwant: %s", len(got), len(want), formatEntries(got), formatEntries(want))
	}
	for i := range want {
		if storage.CompareInternal(got[i].Key, want[i].Key) != 0 || !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("entry %d = %s, want %s", i, formatEntry(got[i]), formatEntry(want[i]))
		}
	}
}

func assertStrictOrder(t testing.TB, entries []Entry) {
	t.Helper()
	for index := 1; index < len(entries); index++ {
		if storage.CompareInternal(entries[index-1].Key, entries[index].Key) >= 0 {
			t.Fatalf("entries %d and %d are not strictly ordered: %s then %s", index-1, index, formatEntry(entries[index-1]), formatEntry(entries[index]))
		}
	}
}

func assertVersionContiguity(t testing.TB, entries []Entry) {
	t.Helper()
	seen := make(map[string]bool)
	var previous []byte
	for index, entry := range entries {
		userKey := entry.Key.UserKey()
		if index == 0 || !bytes.Equal(userKey, previous) {
			if seen[string(userKey)] {
				t.Fatalf("versions of user key %x are not contiguous", userKey)
			}
			seen[string(userKey)] = true
			previous = userKey
		}
	}
}

func compareEntries(left, right Entry) int {
	return storage.CompareInternal(left.Key, right.Key)
}

func randomBytes(rng *mathrand.Rand, length int) []byte {
	value := make([]byte, length)
	for index := range value {
		value[index] = byte(rng.Uint64())
	}
	return value
}

func concurrentKey(t testing.TB, writer, item int) storage.InternalKey {
	t.Helper()
	userKey := []byte{
		byte(uint64(writer) >> 24), byte(uint64(writer) >> 16), byte(uint64(writer) >> 8), byte(writer),
		byte(uint64(item) >> 24), byte(uint64(item) >> 16), byte(uint64(item) >> 8), byte(item),
	}
	return mustKey(t, userKey, uint64(item), storage.KindValue)
}

func formatEntries(entries []Entry) string {
	result := "["
	for index, entry := range entries {
		if index != 0 {
			result += ", "
		}
		result += formatEntry(entry)
	}
	return result + "]"
}

func formatEntry(entry Entry) string {
	return fmt.Sprintf("{%s value=%x}", formatKey(entry.Key), entry.Value)
}

func formatKey(key storage.InternalKey) string {
	return fmt.Sprintf("key=%x seq=%d kind=%d", key.UserKey(), key.Sequence(), key.Kind())
}
