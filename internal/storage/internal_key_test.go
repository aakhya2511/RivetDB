package storage_test

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestInternalKeyDeterministicOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    []byte
		b    []byte
	}{
		{name: "empty before non-empty", a: nil, b: []byte("a")},
		{name: "text prefix", a: []byte("a"), b: []byte("aa")},
		{name: "text differing suffix", a: []byte("a"), b: []byte("ab")},
		{name: "text siblings", a: []byte("aa"), b: []byte("ab")},
		{name: "zero prefix", a: []byte{0x00}, b: []byte{0x00, 0x00}},
		{name: "ff prefix", a: []byte{0xff}, b: []byte{0xff, 0x00}},
		{name: "all boundary bytes", a: []byte{0x00, 0x01, 0x7f, 0x80, 0xfe}, b: []byte{0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff}},
		{name: "long common prefix", a: append(bytes.Repeat([]byte{0x80}, 4096), 0xfe), b: append(bytes.Repeat([]byte{0x80}, 4096), 0xff)},
	}

	sequences := []uint64{0, 1, math.MaxUint64}
	kinds := []storage.ValueKind{storage.KindDelete, storage.KindValue}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, seqA := range sequences {
				for _, seqB := range sequences {
					for _, kindA := range kinds {
						for _, kindB := range kinds {
							a := mustInternalKey(t, tc.a, seqA, kindA)
							b := mustInternalKey(t, tc.b, seqB, kindB)
							if got := storage.CompareInternal(a, b); got >= 0 {
								t.Fatalf("CompareInternal(%x/%d/%d, %x/%d/%d) = %d, want < 0", tc.a, seqA, kindA, tc.b, seqB, kindB, got)
							}
						}
					}
				}
			}
		})
	}
}

func TestInternalKeySameUserKeyOrdering(t *testing.T) {
	t.Parallel()

	key := []byte{0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff}
	old := mustInternalKey(t, key, 1, storage.KindValue)
	newer := mustInternalKey(t, key, 2, storage.KindValue)
	if got := storage.CompareInternal(newer, old); got >= 0 {
		t.Fatalf("newer version compared as %d, want < 0", got)
	}

	minimum := mustInternalKey(t, key, 0, storage.KindValue)
	maximum := mustInternalKey(t, key, math.MaxUint64, storage.KindValue)
	if got := storage.CompareInternal(maximum, minimum); got >= 0 {
		t.Fatalf("maximum sequence compared as %d, want < 0", got)
	}

	deletion := mustInternalKey(t, key, 7, storage.KindDelete)
	value := mustInternalKey(t, key, 7, storage.KindValue)
	if got := storage.CompareInternal(deletion, value); got >= 0 {
		t.Fatalf("deletion compared as %d, want < 0", got)
	}
}

func TestEncodedInternalKeysRequireExplicitComparator(t *testing.T) {
	t.Parallel()

	// This is the smallest counterexample to the superseded raw-byte design:
	// the empty key sorts before {0x00}, but its trailer begins with 0xff.
	shorter := mustInternalKey(t, nil, 0, storage.KindDelete)
	longer := mustInternalKey(t, []byte{0x00}, math.MaxUint64, storage.KindValue)
	if got := bytes.Compare(shorter.Encode(), longer.Encode()); got <= 0 {
		t.Fatalf("raw encoded comparison = %d, want > 0 for the counterexample", got)
	}
	if got := storage.CompareInternal(shorter, longer); got >= 0 {
		t.Fatalf("explicit internal comparison = %d, want < 0", got)
	}
}

func TestInternalKeyVersionsAreContiguous(t *testing.T) {
	t.Parallel()

	keys := []storage.InternalKey{
		mustInternalKey(t, []byte("aa"), 1, storage.KindValue),
		mustInternalKey(t, []byte("a"), 1, storage.KindValue),
		mustInternalKey(t, []byte("aa"), math.MaxUint64, storage.KindDelete),
		mustInternalKey(t, []byte("a"), 8, storage.KindDelete),
		mustInternalKey(t, []byte("ab"), 3, storage.KindValue),
		mustInternalKey(t, []byte("aa"), 9, storage.KindValue),
	}
	slices.SortFunc(keys, storage.CompareInternal)

	seen := make(map[string]bool)
	var previous []byte
	for i, key := range keys {
		userKey := key.UserKey()
		if i == 0 || !bytes.Equal(userKey, previous) {
			if seen[string(userKey)] {
				t.Fatalf("versions of user key %x are not contiguous", userKey)
			}
			seen[string(userKey)] = true
			previous = userKey
		}
	}
}

func TestInternalKeyEncodeDecodeRoundTrip(t *testing.T) {
	t.Parallel()

	for _, userKey := range [][]byte{nil, {}, {0x00}, {0xff}, {0x00, 0xff, 0x00}} {
		for _, sequence := range []uint64{0, 1, math.MaxUint64} {
			for _, kind := range []storage.ValueKind{storage.KindDelete, storage.KindValue, storage.KindTxnAbort, storage.KindIntent} {
				want := mustInternalKey(t, userKey, sequence, kind)
				got, err := storage.DecodeInternalKey(want.Encode())
				if err != nil {
					t.Fatalf("DecodeInternalKey: %v", err)
				}
				if storage.CompareInternal(got, want) != 0 {
					t.Fatalf("round trip got %x/%d/%d, want %x/%d/%d", got.UserKey(), got.Sequence(), got.Kind(), want.UserKey(), want.Sequence(), want.Kind())
				}
			}
		}
	}
}

func TestInternalKeyRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	if _, err := storage.DecodeInternalKey(make([]byte, 8)); !errors.Is(err, storage.ErrInternalKeyTooShort) {
		t.Fatalf("short key error = %v, want ErrInternalKeyTooShort", err)
	}
	badKind := make([]byte, 9)
	badKind[8] = 4
	if _, err := storage.DecodeInternalKey(badKind); !errors.Is(err, storage.ErrInvalidValueKind) {
		t.Fatalf("bad kind error = %v, want ErrInvalidValueKind", err)
	}
	if _, err := storage.NewInternalKey(nil, 0, 4); !errors.Is(err, storage.ErrInvalidValueKind) {
		t.Fatalf("constructor bad kind error = %v, want ErrInvalidValueKind", err)
	}
}

func TestInternalKeyComparatorProperties(t *testing.T) {
	seeds := append([]int64{0, 1, -1, 8134472901, -1234567890123}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		t.Run("seed", func(t *testing.T) {
			runComparatorProperties(t, seed, 10_000)
		})
	}
}

func runComparatorProperties(t *testing.T, seed int64, iterations int) {
	t.Helper()

	rng := testutil.RandFromSeed(seed)
	for range iterations {
		a := randomInternalKey(t, rngBytes(rng, 96), rng.Uint64(), storage.ValueKind(rng.Uint64()%2))
		b := randomInternalKey(t, rngBytes(rng, 96), rng.Uint64(), storage.ValueKind(rng.Uint64()%2))
		c := randomInternalKey(t, rngBytes(rng, 96), rng.Uint64(), storage.ValueKind(rng.Uint64()%2))
		switch rng.Uint64() % 5 {
		case 0:
			b = randomInternalKey(t, a.UserKey(), b.Sequence(), b.Kind())
		case 1:
			b = randomInternalKey(t, a.UserKey(), a.Sequence(), a.Kind())
		}

		userOrder := sign(bytes.Compare(a.UserKey(), b.UserKey()))
		internalOrder := sign(storage.CompareInternal(a, b))
		if userOrder != 0 && internalOrder != userOrder {
			t.Fatalf("seed %d: IK-1 user order %d, internal order %d: a=%x/%d/%d b=%x/%d/%d", seed, userOrder, internalOrder, a.UserKey(), a.Sequence(), a.Kind(), b.UserKey(), b.Sequence(), b.Kind())
		}

		if got, want := internalOrder, -sign(storage.CompareInternal(b, a)); got != want {
			t.Fatalf("seed %d: IK-3 antisymmetry: got %d, want %d", seed, got, want)
		}

		equalFields := bytes.Equal(a.UserKey(), b.UserKey()) && a.Sequence() == b.Sequence() && a.Kind() == b.Kind()
		if (internalOrder == 0) != equalFields {
			t.Fatalf("seed %d: IK-4 equality mismatch", seed)
		}

		ab := storage.CompareInternal(a, b)
		bc := storage.CompareInternal(b, c)
		if ab <= 0 && bc <= 0 && storage.CompareInternal(a, c) > 0 {
			t.Fatalf("seed %d: IK-5 transitivity violated", seed)
		}

		newerSequence := rng.Uint64()
		if newerSequence == 0 {
			newerSequence = 1
		}
		olderSequence := rng.Uint64() % newerSequence
		newer := randomInternalKey(t, a.UserKey(), newerSequence, storage.KindValue)
		older := randomInternalKey(t, a.UserKey(), olderSequence, storage.KindValue)
		if storage.CompareInternal(newer, older) >= 0 {
			t.Fatalf("seed %d: IK-2 newest-first violated: %d <= %d", seed, newerSequence, olderSequence)
		}
	}
}

func randomInternalKey(t *testing.T, key []byte, sequence uint64, kind storage.ValueKind) storage.InternalKey {
	t.Helper()
	return mustInternalKey(t, key, sequence, kind)
}

type randomSource interface {
	Uint64() uint64
}

func rngBytes(rng randomSource, maxLen uint64) []byte {
	data := make([]byte, rng.Uint64()%(maxLen+1))
	for i := range data {
		data[i] = byte(rng.Uint64())
	}
	return data
}

func mustInternalKey(t *testing.T, userKey []byte, sequence uint64, kind storage.ValueKind) storage.InternalKey {
	t.Helper()
	key, err := storage.NewInternalKey(userKey, sequence, kind)
	if err != nil {
		t.Fatalf("NewInternalKey: %v", err)
	}
	return key
}

func sign(value int) int {
	switch {
	case value < 0:
		return -1
	case value > 0:
		return 1
	default:
		return 0
	}
}
