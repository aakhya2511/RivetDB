package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/rivetdb/rivetdb/internal/invariant"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

const phase1JStressEnvironment = "RIVETDB_PHASE1J_STRESS"

type phase1JStressCounts struct {
	operations, puts, deletes, gets, scans uint64
	flushes, compactions, restarts         uint64
	reclaims, reclaimedTables              uint64
}

// TestPhase1JDeepStress is opt-in because every Put/Delete crosses a real
// fdatasync and every restart revalidates authoritative persistent state.
// Phase evidence records a completed run; ordinary and race gates retain the
// smaller always-on campaigns in engine_test.go.
func TestPhase1JDeepStress(t *testing.T) {
	if os.Getenv(phase1JStressEnvironment) == "" {
		t.Skip("set RIVETDB_PHASE1J_STRESS=1 for the 50k-operation/120-restart campaign")
	}
	defer testutil.NoLeaks(t)()
	seeds := []int64{101, 9_901, 8_134_472_901, testutil.Seed(t)}
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	model := make(map[string][]byte)
	deleted := make(map[string]bool)
	previousLive := make(map[uint64]struct{})
	retiredFiles := make(map[uint64]struct{})
	var counts phase1JStressCounts
	var previousNextFile, previousNextSequence uint64
	previousExpensive := invariant.SetExpensive(true)
	defer invariant.SetExpensive(previousExpensive)

	checkpoint := func(operation int) {
		t.Helper()
		verifyModel(t, e, model, deleted)
		if err := e.Validate(); err != nil {
			t.Fatalf("operation %d Validate: %v", operation, err)
		}
		version, err := e.manifest.Current()
		if err != nil {
			t.Fatalf("operation %d Version: %v", operation, err)
		}
		stats := e.Stats().Pipeline
		if version.NextFileNumber() < previousNextFile || stats.NextSequence < previousNextSequence {
			t.Fatalf("operation %d authority regression file=%d/%d sequence=%d/%d", operation, version.NextFileNumber(), previousNextFile, stats.NextSequence, previousNextSequence)
		}
		if counts.puts+counts.deletes != stats.NextSequence || !stats.HaveAssigned || !stats.HaveVisible || stats.LastAssigned != stats.VisibleSequence || stats.VisibleSequence+1 != stats.NextSequence {
			t.Fatalf("operation %d sequence authority counts=%+v pipeline=%+v", operation, counts, stats)
		}
		live := make(map[uint64]struct{})
		for _, number := range version.LiveFileNumbers() {
			if _, reused := retiredFiles[number]; reused {
				t.Fatalf("operation %d reused retired file number %d", operation, number)
			}
			live[number] = struct{}{}
		}
		for number := range previousLive {
			if _, stillLive := live[number]; !stillLive {
				retiredFiles[number] = struct{}{}
			}
		}
		previousLive = live
		previousNextFile, previousNextSequence = version.NextFileNumber(), stats.NextSequence
		if got, want := engineLogicalDigest(t, e), modelLogicalDigest(model); got != want {
			t.Fatalf("operation %d digest=%x want=%x", operation, got, want)
		}
	}

	for seedIndex, seed := range seeds {
		rng := mathrand.New(mathrand.NewSource(seed))
		for local := 0; local < 12_500; local++ {
			operation := seedIndex*12_500 + local
			key := []byte{byte(rng.Intn(64)), byte(rng.Intn(2))}
			switch draw := rng.Intn(100); {
			case draw < 15:
				value := []byte{byte(operation), byte(operation >> 8), byte(operation >> 16), byte(rng.Intn(256))}
				mustPut(t, e, key, value)
				model[string(key)] = bytes.Clone(value)
				delete(deleted, string(key))
				counts.puts++
			case draw < 20:
				mustDelete(t, e, key)
				delete(model, string(key))
				deleted[string(key)] = true
				counts.deletes++
			case draw < 65:
				value, err := e.Get(context.Background(), key)
				want, exists := model[string(key)]
				if exists && (err != nil || !bytes.Equal(value, want)) {
					t.Fatalf("operation %d Get(%x)=(%x,%v), want %x", operation, key, value, err, want)
				}
				if !exists && !errors.Is(err, ErrNotFound) {
					t.Fatalf("operation %d Get(%x)=(%x,%v), want not found", operation, key, value, err)
				}
				counts.gets++
			default:
				verifyRandomRange(t, e, model, rng)
				counts.scans++
			}
			counts.operations++

			if (local+1)%500 == 0 {
				if err := e.Flush(context.Background()); err != nil {
					t.Fatalf("operation %d Flush: %v", operation, err)
				}
				counts.flushes++
			}
			if (local+1)%2_000 == 0 {
				_, err := e.Compact(context.Background())
				if err != nil && !errors.Is(err, compaction.ErrNoCompaction) {
					t.Fatalf("operation %d Compact: %v", operation, err)
				}
				if err == nil {
					counts.compactions++
				}
				result, reclaimErr := e.ReclaimObsoleteTables(context.Background())
				if reclaimErr != nil {
					t.Fatalf("operation %d Reclaim: %v", operation, reclaimErr)
				}
				counts.reclaims++
				counts.reclaimedTables += result.Deleted
			}
			if (local+1)%416 == 0 {
				checkpoint(operation)
				closeTestEngine(t, e)
				e = openTestEngine(t, directory, 4)
				counts.restarts++
				checkpoint(operation)
			}
		}
	}
	checkpoint(50_000)
	closeTestEngine(t, e)
	if counts.operations != 50_000 || counts.restarts != 120 || counts.flushes != 100 || counts.reclaims != 24 {
		t.Fatalf("unexpected campaign scale: %+v", counts)
	}
	t.Logf("Phase 1J seeds=%v counts=%+v final_digest=%x next_file=%d next_sequence=%d reuse=0", seeds, counts, modelLogicalDigest(model), previousNextFile, previousNextSequence)
}

func TestTombstonesNeverResurrectAcrossLifecycle(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 3)
	for index := 0; index < 64; index++ {
		key := []byte(fmt.Sprintf("key-%02d", index))
		mustPut(t, e, key, []byte("A"))
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		key := []byte(fmt.Sprintf("key-%02d", index))
		mustPut(t, e, key, []byte("B"))
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		mustDelete(t, e, []byte(fmt.Sprintf("key-%02d", index)))
	}
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeTestEngine(t, e)
	e = openTestEngine(t, directory, 3)
	result, err := e.ReclaimObsoleteTables(context.Background())
	if err != nil || result.Deleted != 0 {
		t.Fatalf("post-restart conservative reclaim=%+v err=%v", result, err)
	}
	for index := 0; index < 64; index++ {
		assertNotFound(t, e, []byte(fmt.Sprintf("key-%02d", index)))
	}
	assertScan(t, e, nil, nil, nil)
	closeTestEngine(t, e)
	e = openTestEngine(t, directory, 3)
	defer closeTestEngine(t, e)
	for index := 0; index < 64; index++ {
		assertNotFound(t, e, []byte(fmt.Sprintf("key-%02d", index)))
	}
}

func TestCorruptOrphanDoesNotInfluenceAuthority(t *testing.T) {
	directory := t.TempDir()
	e := openTestEngine(t, directory, 4)
	mustPut(t, e, []byte("live"), []byte("value"))
	if err := e.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeTestEngine(t, e)
	path := filepath.Join(directory, "000000000900.sst")
	if err := os.WriteFile(path, []byte("deliberately corrupt orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	e = openTestEngine(t, directory, 4)
	defer closeTestEngine(t, e)
	assertValue(t, e, []byte("live"), []byte("value"))
	assertNotFound(t, e, []byte("orphan"))
	discovery := e.manifest.Discovery()
	if len(discovery.Orphans) != 0 || len(discovery.Invalid) != 1 || discovery.Invalid[0] != filepath.Base(path) {
		t.Fatalf("corrupt orphan classification=%+v", discovery)
	}
}

func modelLogicalDigest(model map[string][]byte) [sha256.Size]byte {
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		value := model[key]
		_, _ = hash.Write([]byte{byte(len(key) >> 8), byte(len(key))})
		_, _ = hash.Write([]byte(key))
		_, _ = hash.Write([]byte{byte(len(value) >> 8), byte(len(value))})
		_, _ = hash.Write(value)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func engineLogicalDigest(t testing.TB, e *Engine) [sha256.Size]byte {
	t.Helper()
	values, err := e.Scan(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	model := make(map[string][]byte, len(values))
	for _, value := range values {
		model[string(value.Key)] = value.Value
	}
	return modelLogicalDigest(model)
}

func directoryPhysicalDigest(t testing.TB, directory string) [sha256.Size]byte {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	for _, entry := range entries {
		_, _ = hash.Write([]byte(entry.Name()))
		if entry.IsDir() {
			continue
		}
		contents, readErr := os.ReadFile(filepath.Join(directory, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		_, _ = hash.Write(contents)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
