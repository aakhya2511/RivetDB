// Package clock abstracts the passage of time so that time-dependent RivetDB
// logic can be tested deterministically.
//
// Motivation: the subsystems whose bugs matter most are the time-dependent
// ones. Raft election timeouts, heartbeat intervals, lease expiry, transaction
// timeouts and rebalancer cooldowns all decide correctness-relevant questions
// based on elapsed time. Tests written against the real clock have to choose
// between being slow (real 150ms election timeouts) and being flaky (short
// timeouts racing a loaded CI machine). Neither is acceptable for a test suite
// that is supposed to be evidence of correctness.
//
// Every RivetDB component that needs time takes a Clock. Production wiring
// passes System(); tests pass a *Mock and advance it explicitly, so a test that
// asserts "a follower with no heartbeat starts an election after the timeout"
// runs in microseconds and gives the same answer on every machine.
//
// Concurrency: System is stateless and safe for concurrent use. Mock is
// mutex-guarded and safe for concurrent use, with the ordering guarantees
// described on Mock.Advance.
package clock

import "time"

// Clock provides the subset of the time package that RivetDB code may use.
// Code under internal/ must not call time.Now, time.After, time.NewTimer or
// time.NewTicker directly; it takes a Clock instead.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// Since returns the time elapsed since t.
	Since(t time.Time) time.Duration

	// NewTimer returns a Timer that fires once after d.
	NewTimer(d time.Duration) Timer

	// NewTicker returns a Ticker that fires every d. d must be positive.
	NewTicker(d time.Duration) Ticker

	// After returns a channel that receives once after d. It leaks the
	// underlying timer until it fires, exactly like time.After, so prefer
	// NewTimer with an explicit Stop on hot paths.
	After(d time.Duration) <-chan time.Time

	// Sleep blocks for d.
	Sleep(d time.Duration)
}

// Timer is a one-shot timer. It mirrors *time.Timer, except that the channel
// is obtained through a method so that Mock can supply its own channel.
type Timer interface {
	// C returns the channel on which the fire time is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing and reports whether it was still
	// pending. As with time.Timer, a false result does not guarantee the
	// channel is empty.
	Stop() bool
	// Reset reschedules the timer to fire after d, reporting whether it was
	// still pending. Callers must drain C before Reset if they require the
	// channel to be empty afterwards, exactly as with time.Timer.
	Reset(d time.Duration) bool
}

// Ticker fires repeatedly. It mirrors *time.Ticker.
type Ticker interface {
	// C returns the channel on which fire times are delivered.
	C() <-chan time.Time
	// Stop halts the ticker. It does not close the channel.
	Stop()
	// Reset changes the tick period to d, which must be positive.
	Reset(d time.Duration)
}
