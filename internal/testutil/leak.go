package testutil

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// leakSettleTimeout bounds how long NoLeaks waits for goroutines to drain.
// Shutdown in RivetDB is cooperative — a Close cancels a context and waits —
// so a goroutine still running well after Close either ignores cancellation or
// is blocked on a channel nobody will write to. Both are the bugs this check
// exists to find.
const leakSettleTimeout = 2 * time.Second

// NoLeaks fails the test if goroutines started during it are still running when
// it ends.
//
// It is registered at the top of tests that construct a subsystem and close it:
//
//	func TestEngineShutdown(t *testing.T) {
//	    defer testutil.NoLeaks(t)()
//	    ...
//	}
//
// Why this matters here specifically: RivetDB runs long-lived background
// goroutines everywhere — WAL sync, MemTable flush, compaction, Raft ticking
// and replication per range, migration workers, the rebalancer loop. A node
// that hosts hundreds of ranges and leaks one goroutine per range teardown
// degrades into unbounded memory and CPU use, and the leak is invisible in a
// test that only asserts on returned values. Detecting it at the unit level is
// far cheaper than diagnosing it from a soak run.
//
// The check snapshots goroutine identifiers on entry and reports only
// identifiers that appear afterwards, so unrelated goroutines belonging to the
// test binary or to parallel tests are not attributed to this test. It polls
// until leakSettleTimeout because an orderly shutdown is allowed to take a
// moment; only goroutines that outlive that window are reported.
//
// The returned function performs the check. Call it with defer.
func NoLeaks(t testing.TB) func() {
	t.Helper()
	before := goroutineIDs()

	return func() {
		t.Helper()

		// A test that already failed has usually skipped its cleanup, so
		// leaked goroutines are a consequence of the first failure rather
		// than an independent finding. Reporting them adds noise to a
		// diagnosis that is already underway.
		if t.Failed() {
			return
		}

		var leaked []goroutine
		deadline := time.Now().Add(leakSettleTimeout)
		for {
			leaked = leakedSince(before)
			if len(leaked) == 0 || time.Now().After(deadline) {
				break
			}
			// Yield first: most "leaks" are goroutines a few instructions
			// from returning, and a Gosched resolves them without sleeping.
			runtime.Gosched()
			time.Sleep(time.Millisecond)
		}

		if len(leaked) == 0 {
			return
		}

		var b strings.Builder
		b.WriteString(strconv.Itoa(len(leaked)))
		b.WriteString(" goroutine(s) leaked after test:\n")
		for _, g := range leaked {
			b.WriteString("\n")
			b.WriteString(g.stack)
			b.WriteString("\n")
		}
		t.Error(b.String())
	}
}

type goroutine struct {
	id    uint64
	stack string
}

func leakedSince(before map[uint64]struct{}) []goroutine {
	var leaked []goroutine
	for _, g := range goroutines() {
		if _, existed := before[g.id]; existed {
			continue
		}
		if isBenign(g.stack) {
			continue
		}
		leaked = append(leaked, g)
	}
	return leaked
}

func goroutineIDs() map[uint64]struct{} {
	ids := make(map[uint64]struct{})
	for _, g := range goroutines() {
		ids[g.id] = struct{}{}
	}
	return ids
}

// goroutines parses the runtime's all-goroutine stack dump. There is no
// supported API for enumerating goroutines, so parsing the dump is the only
// option; the format ("goroutine N [state]:") has been stable for many
// releases, and a parse failure degrades to reporting nothing rather than to a
// false positive.
func goroutines() []goroutine {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	var out []goroutine
	for _, block := range strings.Split(string(buf), "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		id, ok := parseGoroutineID(block)
		if !ok {
			continue
		}
		out = append(out, goroutine{id: id, stack: block})
	}
	return out
}

func parseGoroutineID(block string) (uint64, bool) {
	const prefix = "goroutine "
	if !strings.HasPrefix(block, prefix) {
		return 0, false
	}
	rest := block[len(prefix):]
	space := strings.IndexByte(rest, ' ')
	if space < 0 {
		return 0, false
	}
	id, err := strconv.ParseUint(rest[:space], 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// benignFrames name goroutines that the runtime and the testing package start
// on their own schedule. They are not started by the code under test, so
// attributing them to it would make the check unusable.
var benignFrames = []string{
	"testing.(*T).Run",
	"testing.(*T).Parallel",
	"testing.runTests.func",
	"testing.(*M).before.func", // the -timeout watchdog
	"testing.tRunner.func",     // cleanup helper goroutines
	"runtime.gcBgMarkWorker",
	"runtime.bgsweep",
	"runtime.bgscavenge",
	"runtime.forcegchelper",
	"runtime.ensureSigM",
	"os/signal.signal_recv",
	"os/signal.loop",
	"runtime/pprof.",
	"created by runtime.doInit",
}

func isBenign(stack string) bool {
	for _, frame := range benignFrames {
		if strings.Contains(stack, frame) {
			return true
		}
	}
	return false
}
