package clock

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Epoch is the default starting instant of a Mock. It is a fixed, arbitrary
// point in time: fixing it keeps failure output byte-identical between runs,
// which matters when a chaos seed is replayed and its log is diffed against
// the original failure.
var Epoch = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

// Mock is a Clock whose time only moves when a test moves it.
//
// Ordering guarantee: Advance fires pending timers in deadline order, and
// while a timer fires the mock's Now already reports that timer's deadline
// rather than the final target. Code that reads Now inside a timer callback
// therefore observes the same value it would observe under a real clock, which
// is what makes lease- and timeout-comparison logic testable.
//
// Delivery is non-blocking, matching time.Timer: each timer channel is
// buffered with capacity one and a fire whose buffer is full is dropped. A
// slow receiver thus misses ticks instead of deadlocking the advancing
// goroutine.
//
// Mock is safe for concurrent use. Advance may be called from a different
// goroutine than the one that created the timers, which is the normal pattern:
// the code under test waits on timers while the test goroutine advances time.
type Mock struct {
	mu      sync.Mutex
	cond    *sync.Cond
	now     time.Time
	nextID  uint64
	waiters []*waiter
}

// NewMock returns a Mock positioned at Epoch.
func NewMock() *Mock { return NewMockAt(Epoch) }

// NewMockAt returns a Mock positioned at start.
func NewMockAt(start time.Time) *Mock {
	m := &Mock{now: start}
	m.cond = sync.NewCond(&m.mu)
	return m
}

var _ Clock = (*Mock)(nil)

// waiter is a registered timer or ticker. A single type serves both: period
// zero means one-shot.
type waiter struct {
	id       uint64
	deadline time.Time
	period   time.Duration
	ch       chan time.Time
	// stopped waiters are retained until the next sweep rather than removed
	// eagerly, so that Stop is O(1) and cannot invalidate an in-progress
	// Advance scan.
	stopped bool
}

// Now returns the mock's current time.
func (m *Mock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Since returns the mock time elapsed since t.
func (m *Mock) Since(t time.Time) time.Duration {
	return m.Now().Sub(t)
}

// Advance moves the clock forward by d, firing every timer and ticker whose
// deadline falls in the interval. d must not be negative: RivetDB's Raft and
// MVCC code assumes a monotonically non-decreasing clock, and a test that
// moved time backwards would be exercising a situation the production clock
// cannot produce.
func (m *Mock) Advance(d time.Duration) {
	if d < 0 {
		panic(fmt.Sprintf("clock: Mock.Advance with negative duration %s", d))
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	target := m.now.Add(d)
	for {
		w := m.earliestLocked(target)
		if w == nil {
			break
		}

		// Publish the firing instant before delivering it, so that a
		// receiver observing Now sees the deadline, not the target.
		m.now = w.deadline
		select {
		case w.ch <- w.deadline:
		default:
		}

		if w.period > 0 {
			w.deadline = w.deadline.Add(w.period)
		} else {
			w.stopped = true
		}
	}

	m.now = target
	m.sweepLocked()
	m.cond.Broadcast()
}

// AdvanceTo moves the clock forward to t. It panics if t precedes the current
// time, for the reason given on Advance.
func (m *Mock) AdvanceTo(t time.Time) {
	m.mu.Lock()
	d := t.Sub(m.now)
	m.mu.Unlock()

	if d < 0 {
		panic(fmt.Sprintf("clock: Mock.AdvanceTo target %s precedes current time by %s", t, -d))
	}
	m.Advance(d)
}

// earliestLocked returns the pending waiter with the smallest deadline not
// after limit, breaking ties by registration order so that two timers armed
// for the same instant always fire in the same sequence.
func (m *Mock) earliestLocked(limit time.Time) *waiter {
	var best *waiter
	for _, w := range m.waiters {
		if w.stopped || w.deadline.After(limit) {
			continue
		}
		if best == nil || w.deadline.Before(best.deadline) ||
			(w.deadline.Equal(best.deadline) && w.id < best.id) {
			best = w
		}
	}
	return best
}

func (m *Mock) sweepLocked() {
	m.waiters = slices.DeleteFunc(m.waiters, func(w *waiter) bool { return w.stopped })
}

// Waiters reports how many timers, tickers and sleepers are currently pending.
func (m *Mock) Waiters() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pendingLocked()
}

func (m *Mock) pendingLocked() int {
	n := 0
	for _, w := range m.waiters {
		if !w.stopped {
			n++
		}
	}
	return n
}

// BlockUntil waits until at least n timers, tickers or sleepers are pending.
//
// It exists to remove the race between "the test advances time" and "the code
// under test arms its timer". Without it, a test that starts a goroutine and
// immediately advances the clock may advance past a timer that has not been
// created yet, and the resulting flake looks like a logic bug. BlockUntil has
// no timeout by design; if the expected timer is never armed the test hangs
// and the Go test timeout reports the goroutine dump, which names the bug
// more precisely than a bare assertion failure would.
func (m *Mock) BlockUntil(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for m.pendingLocked() < n {
		m.cond.Wait()
	}
}

func (m *Mock) register(d time.Duration, period time.Duration) *waiter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registerLocked(d, period)
}

func (m *Mock) registerLocked(d time.Duration, period time.Duration) *waiter {
	m.nextID++
	w := &waiter{
		id:       m.nextID,
		deadline: m.now.Add(d),
		period:   period,
		ch:       make(chan time.Time, 1),
	}
	m.waiters = append(m.waiters, w)
	m.cond.Broadcast()
	return w
}

// stop marks w inactive and reports whether it was still pending.
func (m *Mock) stop(w *waiter) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	wasActive := !w.stopped
	w.stopped = true
	m.cond.Broadcast()
	return wasActive
}

// reset reschedules w for d from now and reports whether it was still pending.
func (m *Mock) reset(w *waiter, d time.Duration, period time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	wasActive := !w.stopped
	w.deadline = m.now.Add(d)
	w.period = period
	if w.stopped {
		w.stopped = false
		// A waiter can be swept between Stop and Reset, so re-register it if
		// it is no longer tracked.
		if !slices.Contains(m.waiters, w) {
			m.waiters = append(m.waiters, w)
		}
	}
	m.cond.Broadcast()
	return wasActive
}

// NewTimer returns a Timer that fires once, d of mock time from now. A
// non-positive d arms the timer for the current instant, so it fires on the
// next Advance rather than immediately; tests that need an already-elapsed
// timer should Advance(0).
func (m *Mock) NewTimer(d time.Duration) Timer {
	return &mockTimer{m: m, w: m.register(d, 0)}
}

// NewTicker returns a Ticker firing every d of mock time. It panics on a
// non-positive d, matching time.NewTicker.
func (m *Mock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: Mock.NewTicker with non-positive period")
	}
	return &mockTicker{m: m, w: m.register(d, d)}
}

// After returns a channel that receives once, d of mock time from now.
func (m *Mock) After(d time.Duration) <-chan time.Time {
	return m.register(d, 0).ch
}

// Sleep blocks the calling goroutine until the mock advances past d. A test
// therefore has to advance the clock for a sleeping goroutine to make
// progress, which is the point: sleeps become explicit scheduling steps.
func (m *Mock) Sleep(d time.Duration) {
	if d <= 0 {
		return
	}
	<-m.After(d)
}

type mockTimer struct {
	m *Mock
	w *waiter
}

func (t *mockTimer) C() <-chan time.Time { return t.w.ch }

func (t *mockTimer) Stop() bool { return t.m.stop(t.w) }

func (t *mockTimer) Reset(d time.Duration) bool { return t.m.reset(t.w, d, 0) }

type mockTicker struct {
	m *Mock
	w *waiter
}

func (t *mockTicker) C() <-chan time.Time { return t.w.ch }

func (t *mockTicker) Stop() { t.m.stop(t.w) }

func (t *mockTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: Ticker.Reset with non-positive period")
	}
	t.m.reset(t.w, d, d)
}
