// Package testutil is RivetDB's shared test harness.
//
// It holds the pieces every level of the test pyramid needs: reproducible
// randomness with a persisted failure corpus, goroutine-leak detection, and
// bounded polling helpers. Higher-level harnesses (the fault injector, the
// chaos runner, the history checker) are built on these and live with the
// subsystems they exercise.
//
// The package imports testing and is only ever imported by _test.go files.
package testutil

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// EnvSeed forces a specific seed for every randomized test in a run. It is the
// mechanism for reproducing a reported failure:
//
//	RIVETDB_SEED=8134472901 go test ./internal/... -run TestFoo
const EnvSeed = "RIVETDB_SEED"

// seedCorpusDir is the per-package directory holding seeds that have failed
// before. It is committed to the repository: a seed that once found a bug is a
// regression test, and regenerating it by chance is not a plan.
const seedCorpusDir = "testdata/seeds"

// Seed returns the seed for a randomized test and arranges for it to be
// reported and preserved.
//
// The seed comes from RIVETDB_SEED when set, and is otherwise drawn from the
// system CSPRNG so that repeated CI runs explore different interleavings — a
// randomized test that runs the same schedule every night is a fixed test
// wearing a costume.
//
// The seed is always logged, and on failure it is appended to this package's
// seed corpus along with the exact command to replay it. That closes the loop
// that makes randomized testing usable: a nightly chaos failure arrives with a
// seed, the seed is committed, and it runs on every subsequent build.
func Seed(t testing.TB) int64 {
	t.Helper()

	seed, fromEnv := seedFromEnv(t)
	if !fromEnv {
		seed = randomSeed(t)
	}

	if fromEnv {
		t.Logf("seed %d (from %s)", seed, EnvSeed)
	} else {
		t.Logf("seed %d (replay with %s=%d go test -run '^%s$' ./...)",
			seed, EnvSeed, seed, t.Name())
	}

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		path, err := recordFailingSeed(t.Name(), seed)
		if err != nil {
			t.Logf("could not persist failing seed %d: %v", seed, err)
			return
		}
		t.Logf("recorded failing seed %d in %s; replay with %s=%d go test -run '^%s$' ./...",
			seed, path, EnvSeed, seed, t.Name())
	})

	return seed
}

// Rand returns a deterministic random source for t, seeded by Seed.
//
// The returned *rand.Rand is not safe for concurrent use. A test driving
// several workers should either derive one source per worker from this one, or
// generate its whole schedule up front — sharing a source across goroutines
// makes the run non-reproducible even though it is seeded, because the order of
// draws then depends on the scheduler.
func Rand(t testing.TB) *mathrand.Rand {
	t.Helper()
	return RandFromSeed(Seed(t))
}

// RandFromSeed returns a deterministic random source for an explicit seed. It
// is used to derive per-worker sources and to replay corpus seeds.
func RandFromSeed(seed int64) *mathrand.Rand {
	// Reinterpreting the seed's bits, not converting its value: every int64 is
	// a legal seed and negative seeds must stay usable.
	u := uint64(seed) //nolint:gosec // deliberate two's-complement reinterpretation

	// PCG has a specified algorithm, so a seed reproduces the same stream on
	// any platform and any Go version. The two words are given different
	// values because PCG seeded with (x, x) is a valid but needlessly
	// correlated starting state.
	//
	//nolint:gosec // a test source must be reproducible, which rules out crypto/rand
	return mathrand.New(mathrand.NewPCG(u, u^0x9e3779b97f4a7c15))
}

// SeedCorpus returns the previously recorded failing seeds for the named test,
// oldest first. A randomized test should run these before exploring new seeds:
//
//	for _, seed := range testutil.SeedCorpus(t, t.Name()) {
//	    t.Run(fmt.Sprint(seed), func(t *testing.T) { runScenario(t, seed) })
//	}
//
// It returns nil when the test has no recorded failures.
func SeedCorpus(t testing.TB, name string) []int64 {
	t.Helper()

	path := corpusPath(name)
	seeds, err := existingSeeds(path)
	if err != nil {
		t.Fatalf("read seed corpus %s: %v", path, err)
	}
	return seeds
}

func seedFromEnv(t testing.TB) (int64, bool) {
	t.Helper()

	raw, ok := os.LookupEnv(EnvSeed)
	if !ok || raw == "" {
		return 0, false
	}

	seed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		// Failing loudly matters more than falling back: a typo in the
		// variable would otherwise silently produce a non-reproducing run
		// that looks like the bug disappeared.
		t.Fatalf("%s=%q is not a valid int64 seed: %v", EnvSeed, raw, err)
	}
	return seed, true
}

func randomSeed(t testing.TB) int64 {
	t.Helper()

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate random seed: %v", err)
	}
	// The full 64-bit range is wanted, so the high bit becoming a sign bit is
	// intended rather than an overflow.
	return int64(binary.LittleEndian.Uint64(b[:])) //nolint:gosec // reinterpretation, not conversion
}

// recordFailingSeed appends seed to the corpus for name, skipping seeds that
// are already present, and returns the corpus path.
func recordFailingSeed(name string, seed int64) (string, error) {
	// corpusPath sanitises name, so the path is always inside seedCorpusDir
	// even for a subtest name containing separators; see
	// TestSanitizeTestNameCannotEscapeCorpusDir.
	path := corpusPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("create seed corpus directory: %w", err)
	}

	existing, err := existingSeeds(path)
	if err != nil {
		return "", err
	}
	if slices.Contains(existing, seed) {
		return path, nil
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // path is sanitised by corpusPath
	if err != nil {
		return "", fmt.Errorf("open seed corpus %s: %w", path, err)
	}

	if _, err := fmt.Fprintf(f, "%d\n", seed); err != nil {
		f.Close() //nolint:errcheck,gosec // the write error is the one worth reporting
		return "", fmt.Errorf("append seed to %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close seed corpus %s: %w", path, err)
	}
	return path, nil
}

func existingSeeds(path string) ([]int64, error) {
	f, err := os.Open(path) //nolint:gosec // path is sanitised by corpusPath
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open seed corpus %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck,gosec // read-only file

	return readSeeds(f)
}

func readSeeds(f *os.File) ([]int64, error) {
	var seeds []int64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		seed, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: malformed seed %q: %w", f.Name(), line, err)
		}
		seeds = append(seeds, seed)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read seed corpus %s: %w", f.Name(), err)
	}
	return seeds, nil
}

func corpusPath(name string) string {
	return filepath.Join(seedCorpusDir, sanitizeTestName(name)+".seeds")
}

// sanitizeTestName maps a test name to a filename. Subtest separators and any
// other character that is awkward in a path become underscores, so that
// TestSplit/crash_during_apply yields one predictable file.
func sanitizeTestName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
