package testutil

import (
	"testing"
	"time"
)

// DefaultPollInterval is the gap between condition checks in Eventually and
// Consistently.
const DefaultPollInterval = time.Millisecond

// Eventually fails the test if cond has not become true within timeout.
//
// It is for asserting on state that a background goroutine converges to and
// that has no channel to wait on: a follower's applied index catching up, a
// compaction reducing the L0 file count, a range descriptor propagating to a
// router cache. Prefer an explicit signal (a channel, a WaitGroup, or a mock
// clock step) when the code offers one — polling is a last resort, because it
// turns a missing happens-before edge into a timing-dependent pass.
//
// desc is included in the failure message and should state the condition being
// waited on, since a bare "condition not met" says nothing about which
// subsystem stalled.
func Eventually(t testing.TB, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	if !pollUntil(timeout, DefaultPollInterval, cond) {
		t.Fatalf("condition never became true within %s: %s", timeout, desc)
	}
}

// Consistently fails the test if cond becomes false at any point during the
// window.
//
// It asserts on properties that must never be violated rather than eventually
// hold: no second leader appears in a term, an aborted transaction's writes
// stay invisible, a range's replica count never drops below the replication
// factor during a migration. Eventually can pass on a system that is briefly
// correct; Consistently is what catches a transient violation.
func Consistently(t testing.TB, window time.Duration, desc string, cond func() bool) {
	t.Helper()
	if !pollWhile(window, DefaultPollInterval, cond) {
		t.Fatalf("condition stopped holding during %s window: %s", window, desc)
	}
}

// pollUntil reports whether cond became true within timeout. It always
// evaluates cond at least once, so a condition that is already satisfied does
// not depend on the clock.
func pollUntil(timeout, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// pollWhile reports whether cond held for the whole window. It evaluates cond
// at least once so a zero window still checks the current state.
func pollWhile(window, interval time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(window)
	for {
		if !cond() {
			return false
		}
		if !time.Now().Before(deadline) {
			return true
		}
		time.Sleep(interval)
	}
}
