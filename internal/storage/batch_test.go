package storage_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestWriteBatchRoundTrip(t *testing.T) {
	t.Parallel()

	want := storage.WriteBatch{
		FirstSequence: math.MaxUint64 - 2,
		Mutations: []storage.Mutation{
			{Kind: storage.KindValue, Key: nil, Value: nil},
			{Kind: storage.KindDelete, Key: []byte{0x00, 0xff}},
			{Kind: storage.KindValue, Key: []byte{0xff, 0x00, 0x80}, Value: []byte{0x00, 0xff, 0x01}},
		},
	}
	encoded, err := storage.EncodeWriteBatch(want)
	if err != nil {
		t.Fatalf("EncodeWriteBatch: %v", err)
	}
	got, err := storage.DecodeWriteBatch(encoded)
	if err != nil {
		t.Fatalf("DecodeWriteBatch: %v", err)
	}
	assertBatchEqual(t, got, want)
}

func TestWriteBatchDistinguishesDeleteFromEmptyValue(t *testing.T) {
	t.Parallel()

	put, err := storage.EncodeWriteBatch(storage.WriteBatch{Mutations: []storage.Mutation{{Kind: storage.KindValue, Key: []byte("k")}}})
	if err != nil {
		t.Fatalf("encode empty put: %v", err)
	}
	deleteRecord, err := storage.EncodeWriteBatch(storage.WriteBatch{Mutations: []storage.Mutation{{Kind: storage.KindDelete, Key: []byte("k")}}})
	if err != nil {
		t.Fatalf("encode delete: %v", err)
	}
	if bytes.Equal(put, deleteRecord) {
		t.Fatal("PUT of empty value and DELETE have identical encodings")
	}
}

func TestWriteBatchRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		batch storage.WriteBatch
		want  error
	}{
		{name: "empty", batch: storage.WriteBatch{}, want: storage.ErrEmptyBatch},
		{name: "invalid kind", batch: storage.WriteBatch{Mutations: []storage.Mutation{{Kind: 2}}}, want: storage.ErrInvalidValueKind},
		{name: "sequence overflow", batch: storage.WriteBatch{FirstSequence: math.MaxUint64, Mutations: []storage.Mutation{{Kind: storage.KindDelete}, {Kind: storage.KindDelete}}}, want: storage.ErrSequenceOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := storage.EncodeWriteBatch(tc.batch); !errors.Is(err, tc.want) {
				t.Fatalf("EncodeWriteBatch error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeWriteBatchRejectsMalformedBytes(t *testing.T) {
	t.Parallel()

	valid, err := storage.EncodeWriteBatch(storage.WriteBatch{FirstSequence: 9, Mutations: []storage.Mutation{{Kind: storage.KindValue, Key: []byte("k"), Value: []byte("v")}}})
	if err != nil {
		t.Fatalf("EncodeWriteBatch: %v", err)
	}

	badCount := make([]byte, 12)
	binary.LittleEndian.PutUint32(badCount[8:], math.MaxUint32)
	overflowSequence := append([]byte(nil), valid...)
	binary.LittleEndian.PutUint64(overflowSequence[0:], math.MaxUint64)
	binary.LittleEndian.PutUint32(overflowSequence[8:], 2)
	badKind := append([]byte(nil), valid...)
	badKind[12] = 9
	overflowLength := append([]byte(nil), valid[:13]...)
	overflowLength = append(overflowLength, bytes.Repeat([]byte{0xff}, binary.MaxVarintLen64)...)
	nonCanonicalLength := append([]byte(nil), valid[:13]...)
	nonCanonicalLength = append(nonCanonicalLength, 0x81, 0x00, 'k', 0x01, 'v')

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty", data: nil, want: storage.ErrInvalidBatch},
		{name: "truncated header", data: valid[:11], want: storage.ErrInvalidBatch},
		{name: "zero count", data: make([]byte, 12), want: storage.ErrEmptyBatch},
		{name: "malicious count", data: badCount, want: storage.ErrInvalidBatch},
		{name: "sequence overflow", data: overflowSequence, want: storage.ErrSequenceOverflow},
		{name: "invalid kind", data: badKind, want: storage.ErrInvalidValueKind},
		{name: "truncated key length", data: valid[:13], want: storage.ErrInvalidBatch},
		{name: "overflowing key length", data: overflowLength, want: storage.ErrInvalidBatch},
		{name: "non-canonical key length", data: nonCanonicalLength, want: storage.ErrInvalidBatch},
		{name: "truncated key", data: valid[:14], want: storage.ErrInvalidBatch},
		{name: "truncated value length", data: valid[:15], want: storage.ErrInvalidBatch},
		{name: "trailing bytes", data: append(append([]byte(nil), valid...), 0), want: storage.ErrInvalidBatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, decodeErr := storage.DecodeWriteBatch(tc.data); !errors.Is(decodeErr, tc.want) {
				t.Fatalf("DecodeWriteBatch error = %v, want %v", decodeErr, tc.want)
			}
		})
	}
}

func TestWriteBatchMaximumSizeBoundary(t *testing.T) {
	key := make([]byte, storage.MaxRecordSize-17)
	batch := storage.WriteBatch{Mutations: []storage.Mutation{{Kind: storage.KindDelete, Key: key}}}
	encoded, err := storage.EncodeWriteBatch(batch)
	if err != nil {
		t.Fatalf("EncodeWriteBatch at maximum: %v", err)
	}
	if len(encoded) != storage.MaxRecordSize {
		t.Fatalf("encoded size = %d, want %d", len(encoded), storage.MaxRecordSize)
	}
	oversizedKey := make([]byte, len(key)+1)
	if _, err := storage.EncodeWriteBatch(storage.WriteBatch{Mutations: []storage.Mutation{{Kind: storage.KindDelete, Key: oversizedKey}}}); !errors.Is(err, storage.ErrBatchTooLarge) {
		t.Fatalf("oversized batch error = %v, want ErrBatchTooLarge", err)
	}
}

func TestWriteBatchRandomRoundTrips(t *testing.T) {
	seeds := append([]int64{1, -1, 8134472901}, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		rng := testutil.RandFromSeed(seed)
		for range 2_000 {
			count := 1 + int(rng.Uint64()%16)
			first := rng.Uint64()
			if first > math.MaxUint64-uint64(count-1) {
				first -= uint64(count - 1)
			}
			want := storage.WriteBatch{FirstSequence: first, Mutations: make([]storage.Mutation, count)}
			for i := range want.Mutations {
				want.Mutations[i] = storage.Mutation{
					Kind:  storage.ValueKind(rng.Uint64() % 2),
					Key:   rngBytes(rng, 128),
					Value: rngBytes(rng, 256),
				}
			}
			encoded, err := storage.EncodeWriteBatch(want)
			if err != nil {
				t.Fatalf("seed %d: encode: %v", seed, err)
			}
			got, err := storage.DecodeWriteBatch(encoded)
			if err != nil {
				t.Fatalf("seed %d: decode: %v", seed, err)
			}
			assertBatchEqual(t, got, want)
		}
	}
}

func assertBatchEqual(t *testing.T, got, want storage.WriteBatch) {
	t.Helper()
	if got.FirstSequence != want.FirstSequence || len(got.Mutations) != len(want.Mutations) {
		t.Fatalf("batch header = (%d, %d), want (%d, %d)", got.FirstSequence, len(got.Mutations), want.FirstSequence, len(want.Mutations))
	}
	for i := range got.Mutations {
		if got.Mutations[i].Kind != want.Mutations[i].Kind || !bytes.Equal(got.Mutations[i].Key, want.Mutations[i].Key) {
			t.Fatalf("mutation %d metadata differs: got %+v want %+v", i, got.Mutations[i], want.Mutations[i])
		}
		if got.Mutations[i].Kind == storage.KindValue && !bytes.Equal(got.Mutations[i].Value, want.Mutations[i].Value) {
			t.Fatalf("mutation %d value = %x, want %x", i, got.Mutations[i].Value, want.Mutations[i].Value)
		}
	}
}
