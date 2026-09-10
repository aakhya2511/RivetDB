package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestBloomDeterministicNoFalseNegativesAndBoundedFalsePositives(t *testing.T) {
	hashes := make([]bloomHash, 10_000)
	for index := range hashes {
		hashes[index] = hashBloomKey([]byte(fmt.Sprintf("present-%08d", index)))
	}
	first, err := encodeBloom(hashes)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeBloom(hashes)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("deterministic Bloom: equal=%t err=%v", bytes.Equal(first, second), err)
	}
	filter, err := decodeBloom(first)
	if err != nil {
		t.Fatal(err)
	}
	for index := range hashes {
		if !filter.mayContain([]byte(fmt.Sprintf("present-%08d", index))) {
			t.Fatalf("false negative at %d", index)
		}
	}
	falsePositives := 0
	for index := range 10_000 {
		if filter.mayContain([]byte(fmt.Sprintf("absent-%08d", index))) {
			falsePositives++
		}
	}
	if falsePositives > 300 {
		t.Fatalf("false-positive rate %.4f exceeds 3%%", float64(falsePositives)/10_000)
	}
	t.Logf("false positives=%d/10000 (%.4f), payload=%d bytes (%.4f bytes/key)", falsePositives, float64(falsePositives)/10_000, len(first), float64(len(first))/10_000)
}

func TestReaderBloomRejectsFalseNegativeEvenWithValidChecksums(t *testing.T) {
	entries := deterministicEntries(t, 100)
	data, _, _ := buildRealTable(t, 401, Options{}, entries)
	footer := data[len(data)-FooterSize:]
	handle := BlockHandle{Offset: binary.LittleEndian.Uint64(footer[footerFilterOffset:]), Length: binary.LittleEndian.Uint64(footer[footerFilterLength:])}
	if handle.Length == 0 {
		t.Fatal("writer omitted Bloom filter")
	}
	filterBlock := data[int(handle.Offset):int(handle.Offset+handle.Length)]
	clear(filterBlock[bloomHeaderSize : len(filterBlock)-BlockTrailerSize])
	recomputeBlockChecksum(data, handle)
	if _, err := validateTable(data); !errors.Is(err, ErrCorruptTable) {
		t.Fatalf("false-negative filter error=%v", err)
	}
}

func TestReaderBloomChecksumDetectsBitFlip(t *testing.T) {
	data, _, _ := buildRealTable(t, 403, Options{}, deterministicEntries(t, 100))
	footer := data[len(data)-FooterSize:]
	offset := binary.LittleEndian.Uint64(footer[footerFilterOffset:])
	data[int(offset)+bloomHeaderSize] ^= 1
	if _, err := validateTable(data); !errors.Is(err, ErrChecksum) {
		t.Fatalf("filter bit-flip error=%v", err)
	}
}

func TestReaderBloomSkipsAbsentUserKey(t *testing.T) {
	entries := deterministicEntries(t, 1_000)
	_, _, path := buildRealTable(t, 402, Options{}, entries)
	reader := mustOpenReader(t, path)
	defer closeReader(t, reader)
	for _, entry := range entries {
		present, err := reader.MayContain(entry.key.UserKey())
		if err != nil || !present {
			t.Fatalf("present key filter=%t err=%v", present, err)
		}
	}
	absent, err := reader.MayContain([]byte("definitely-absent-user-key"))
	if err != nil {
		t.Fatal(err)
	}
	if absent {
		t.Fatal("chosen absent key was a Bloom false positive")
	}
}

func TestReaderAcceptsVersionOneTableWithoutBloom(t *testing.T) {
	_, _, path := buildRealTable(t, 404, Options{DisableBloom: true}, deterministicEntries(t, 10))
	reader := mustOpenReader(t, path)
	defer closeReader(t, reader)
	mayContain, err := reader.MayContain([]byte("absent"))
	if err != nil || !mayContain {
		t.Fatalf("absent-filter result=%t err=%v", mayContain, err)
	}
}
