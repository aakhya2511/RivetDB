package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSanitizeTestName(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{"TestSplit", "TestSplit"},
		{"TestSplit/crash during apply", "TestSplit_crash_during_apply"},
		{"TestRaft/partition#01", "TestRaft_partition_01"},
		{"Test.With-Dots_2", "Test.With-Dots_2"},
		{"../escape", ".._escape"},
	}

	for _, tc := range tests {
		if got := sanitizeTestName(tc.in); got != tc.want {
			t.Errorf("sanitizeTestName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestSanitizeTestNameCannotEscapeCorpusDir guards a small security property:
// the corpus path is derived from a test name, and a name containing path
// separators must not be able to write outside testdata/seeds.
func TestSanitizeTestNameCannotEscapeCorpusDir(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"../../etc/passwd", "a/b/../../../c", "/absolute"} {
		got := corpusPath(name)
		if !strings.HasPrefix(filepath.Clean(got), filepath.Clean(seedCorpusDir)+string(filepath.Separator)) {
			t.Errorf("corpusPath(%q) = %q, which escapes %q", name, got, seedCorpusDir)
		}
	}
}

func TestPromoteSeedAppendsAndDeduplicates(t *testing.T) {
	t.Parallel()

	packageDir := t.TempDir()
	path, err := PromoteSeed(packageDir, "TestExample", 42)
	if err != nil {
		t.Fatalf("PromoteSeed: %v", err)
	}
	if _, dupErr := PromoteSeed(packageDir, "TestExample", 42); dupErr != nil {
		t.Fatalf("PromoteSeed (duplicate): %v", dupErr)
	}
	if _, secondErr := PromoteSeed(packageDir, "TestExample", -7); secondErr != nil {
		t.Fatalf("PromoteSeed (second seed): %v", secondErr)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus: %v", err)
	}
	if got, want := string(data), "42\n-7\n"; got != want {
		t.Errorf("corpus contents = %q, want %q", got, want)
	}
}

func TestExistingSeedsIgnoresCommentsAndBlanks(t *testing.T) {
	// Not parallel: chdir is process-wide.
	chdirTemp(t)

	path := corpusPath("TestExample")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	contents := "# found by nightly chaos run 2026-02-11\n\n17\n  -3  \n"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write corpus: %v", err)
	}

	seeds, err := existingSeeds(path)
	if err != nil {
		t.Fatalf("existingSeeds: %v", err)
	}
	if len(seeds) != 2 || seeds[0] != 17 || seeds[1] != -3 {
		t.Errorf("existingSeeds = %v, want [17 -3]", seeds)
	}
}

func TestExistingSeedsRejectsMalformedCorpus(t *testing.T) {
	// Not parallel: chdir is process-wide.
	chdirTemp(t)

	path := corpusPath("TestExample")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("17\nnot-a-seed\n"), 0o644); err != nil {
		t.Fatalf("write corpus: %v", err)
	}

	if _, err := existingSeeds(path); err == nil {
		t.Fatal("existingSeeds accepted a malformed corpus file")
	}
}

func TestExistingSeedsMissingFileIsNotAnError(t *testing.T) {
	t.Parallel()

	seeds, err := existingSeeds(filepath.Join(t.TempDir(), "absent.seeds"))
	if err != nil {
		t.Fatalf("existingSeeds on a missing file: %v", err)
	}
	if seeds != nil {
		t.Errorf("existingSeeds = %v, want nil", seeds)
	}
}

func TestPollUntilReturnsAsSoonAsConditionHolds(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	ok := pollUntil(time.Second, time.Millisecond, func() bool {
		return calls.Add(1) >= 3
	})
	if !ok {
		t.Fatal("pollUntil reported failure for a condition that became true")
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("cond evaluated %d times, want 3", got)
	}
}

func TestPollUntilEvaluatesOnceWithZeroTimeout(t *testing.T) {
	t.Parallel()

	// A condition that is already satisfied must not depend on the clock.
	if !pollUntil(0, time.Millisecond, func() bool { return true }) {
		t.Error("pollUntil(0) failed for an already-true condition")
	}
	if pollUntil(0, time.Millisecond, func() bool { return false }) {
		t.Error("pollUntil(0) succeeded for a false condition")
	}
}

func TestPollUntilTimesOut(t *testing.T) {
	t.Parallel()

	start := time.Now()
	if pollUntil(20*time.Millisecond, time.Millisecond, func() bool { return false }) {
		t.Fatal("pollUntil succeeded for a condition that never holds")
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Errorf("pollUntil returned after %s, want at least 20ms", elapsed)
	}
}

func TestPollWhileDetectsTransientViolation(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64
	ok := pollWhile(time.Second, time.Millisecond, func() bool {
		return calls.Add(1) < 3
	})
	if ok {
		t.Fatal("pollWhile reported success for a condition that stopped holding")
	}
}

func TestPollWhileHoldsForWindow(t *testing.T) {
	t.Parallel()

	if !pollWhile(10*time.Millisecond, time.Millisecond, func() bool { return true }) {
		t.Error("pollWhile failed for a condition that always holds")
	}
}

// TestLeakedSinceDetectsAndClears exercises the leak detector's core: a
// goroutine started after the snapshot is reported, and stops being reported
// once it exits.
func TestLeakedSinceDetectsAndClears(t *testing.T) {
	t.Parallel()

	before := goroutineIDs()

	release := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		<-release
	}()

	// The goroutine may not be scheduled yet; wait for it to appear.
	var leaked []goroutine
	if !pollUntil(testWaitTimeout, time.Millisecond, func() bool {
		leaked = leakedSince(before)
		return len(leaked) == 1
	}) {
		t.Fatalf("expected exactly 1 leaked goroutine, found %d", len(leaked))
	}

	close(release)
	<-exited

	if !pollUntil(testWaitTimeout, time.Millisecond, func() bool {
		return len(leakedSince(before)) == 0
	}) {
		t.Errorf("goroutine still reported as leaked after it exited: %v", leakedSince(before))
	}
}

func TestParseGoroutineID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in     string
		want   uint64
		wantOK bool
	}{
		{in: "goroutine 17 [running]:\nmain.main()", want: 17, wantOK: true},
		{in: "goroutine 1 [chan receive]:", want: 1, wantOK: true},
		{in: "not a goroutine block", wantOK: false},
		{in: "goroutine abc [running]:", wantOK: false},
		{in: "goroutine 17", wantOK: false},
	}

	for _, tc := range tests {
		got, ok := parseGoroutineID(tc.in)
		if ok != tc.wantOK {
			t.Errorf("parseGoroutineID(%q) ok = %v, want %v", tc.in, ok, tc.wantOK)
			continue
		}
		if ok && got != tc.want {
			t.Errorf("parseGoroutineID(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestGoroutinesIncludesCaller(t *testing.T) {
	t.Parallel()

	if len(goroutines()) == 0 {
		t.Fatal("goroutines() returned nothing; the runtime stack format may have changed")
	}
}

func TestIsBenign(t *testing.T) {
	t.Parallel()

	if !isBenign("goroutine 5 [chan receive]:\ntesting.(*T).Parallel(...)") {
		t.Error("testing-internal goroutine not recognised as benign")
	}
	if isBenign("goroutine 5 [chan receive]:\ngithub.com/rivetdb/rivetdb/internal/storage.(*Engine).compactLoop()") {
		t.Error("a RivetDB background goroutine was treated as benign")
	}
}

const testWaitTimeout = 5 * time.Second

// chdirTemp moves the process into a scratch directory so that corpus writes
// under the relative testdata/seeds path do not touch the repository.
func chdirTemp(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
}
