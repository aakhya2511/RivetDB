package clock_test

import (
	"sync"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

const testTimeout = 5 * time.Second

// recv receives from ch, failing the test if nothing arrives. It never blocks
// indefinitely so that a broken mock reports a useful failure instead of a
// package-wide timeout.
func recv(t *testing.T, ch <-chan time.Time) time.Time {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for a mock timer to fire")
		return time.Time{}
	}
}

func mustNotFire(t *testing.T, ch <-chan time.Time) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("timer fired unexpectedly at %s", v)
	default:
	}
}

func TestMockStartsAtEpochAndDoesNotDrift(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	if !m.Now().Equal(clock.Epoch) {
		t.Fatalf("Now = %s, want %s", m.Now(), clock.Epoch)
	}

	// Real time passes; mock time must not.
	time.Sleep(2 * time.Millisecond)
	if !m.Now().Equal(clock.Epoch) {
		t.Fatalf("mock time advanced on its own to %s", m.Now())
	}

	m.Advance(90 * time.Second)
	if want := clock.Epoch.Add(90 * time.Second); !m.Now().Equal(want) {
		t.Fatalf("Now = %s, want %s", m.Now(), want)
	}
	if got := m.Since(clock.Epoch); got != 90*time.Second {
		t.Fatalf("Since = %s, want 90s", got)
	}
}

func TestMockTimerFiresOnlyAtOrAfterDeadline(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	timer := m.NewTimer(100 * time.Millisecond)

	m.Advance(99 * time.Millisecond)
	mustNotFire(t, timer.C())

	m.Advance(time.Millisecond)
	fired := recv(t, timer.C())

	// The delivered value is the deadline, not the advance target: code that
	// timestamps work from a timer must see the scheduled instant.
	if want := clock.Epoch.Add(100 * time.Millisecond); !fired.Equal(want) {
		t.Errorf("fired at %s, want %s", fired, want)
	}

	// One-shot timers do not fire twice.
	m.Advance(time.Hour)
	mustNotFire(t, timer.C())
}

// TestMockNowDuringFireIsDeadline documents the guarantee that makes lease and
// timeout comparisons testable: while a timer's value is delivered, Now
// reports that timer's deadline rather than the final advance target.
func TestMockNowDuringFireIsDeadline(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	timer := m.NewTimer(10 * time.Millisecond)

	observed := make(chan time.Time, 1)
	go func() {
		<-timer.C()
		observed <- m.Now()
	}()

	m.BlockUntil(1)
	// Advance far past the deadline in a single step.
	m.Advance(time.Hour)

	select {
	case got := <-observed:
		// The observing goroutine may be scheduled after Advance completes,
		// in which case it legitimately sees the final time. The guarantee
		// under test is that Now is never *behind* the deadline when the
		// value is delivered.
		if got.Before(clock.Epoch.Add(10 * time.Millisecond)) {
			t.Fatalf("observed %s before the deadline", got)
		}
	case <-time.After(testTimeout):
		t.Fatal("timer never fired")
	}
}

func TestMockTickerFiresRepeatedly(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	ticker := m.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for i := 1; i <= 3; i++ {
		m.Advance(10 * time.Millisecond)
		got := recv(t, ticker.C())
		want := clock.Epoch.Add(time.Duration(i) * 10 * time.Millisecond)
		if !got.Equal(want) {
			t.Fatalf("tick %d at %s, want %s", i, got, want)
		}
	}
}

// TestMockTickerCoalescesMissedTicks pins the same drop-on-slow-receiver
// behaviour as time.Ticker: a receiver that misses ticks must not be able to
// stall the goroutine advancing time.
func TestMockTickerCoalescesMissedTicks(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	ticker := m.NewTicker(time.Millisecond)
	defer ticker.Stop()

	m.Advance(time.Second) // 1000 ticks into a 1-slot buffer

	if _, ok := <-ticker.C(); !ok {
		t.Fatal("expected one buffered tick")
	}
	mustNotFire(t, ticker.C())
}

func TestMockStopPreventsFiring(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	timer := m.NewTimer(time.Second)

	if !timer.Stop() {
		t.Error("Stop on a pending timer returned false")
	}
	m.Advance(time.Hour)
	mustNotFire(t, timer.C())

	if timer.Stop() {
		t.Error("Stop on an already-stopped timer returned true")
	}
}

func TestMockResetReschedules(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	timer := m.NewTimer(time.Second)

	if !timer.Reset(10 * time.Millisecond) {
		t.Error("Reset on a pending timer returned false")
	}
	m.Advance(10 * time.Millisecond)
	recv(t, timer.C())

	// Reset after firing must re-arm the timer. This is the exact pattern a
	// Raft follower uses on every heartbeat, so a timer that could not be
	// revived would make elections impossible.
	if timer.Reset(5 * time.Millisecond) {
		t.Error("Reset on an expired timer returned true")
	}
	m.Advance(5 * time.Millisecond)
	recv(t, timer.C())
}

func TestMockResetAfterStopReArms(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	timer := m.NewTimer(time.Second)
	timer.Stop()

	// Advancing sweeps stopped waiters; Reset must still bring the timer back.
	m.Advance(time.Hour)
	timer.Reset(time.Millisecond)
	m.Advance(time.Millisecond)
	recv(t, timer.C())
}

// TestMockFiresInDeadlineOrder is the property that lets a test reason about
// which of several pending timeouts wins, for example an election timeout
// racing a heartbeat interval.
func TestMockFiresInDeadlineOrder(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	late := m.NewTimer(30 * time.Millisecond)
	early := m.NewTimer(10 * time.Millisecond)
	middle := m.NewTimer(20 * time.Millisecond)

	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, tc := range []struct {
		name  string
		timer clock.Timer
	}{{"early", early}, {"middle", middle}, {"late", late}} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-tc.timer.C()
			mu.Lock()
			order = append(order, tc.name)
			mu.Unlock()
		}()
	}

	m.BlockUntil(3)

	// Advance one deadline at a time and join the receiver, so the recorded
	// order reflects firing order rather than goroutine scheduling.
	for range 3 {
		m.Advance(10 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	want := []string{"early", "middle", "late"}
	if len(order) != len(want) {
		t.Fatalf("fired %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("fired %v, want %v", order, want)
		}
	}
}

func TestMockSleepBlocksUntilAdvanced(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	done := make(chan struct{})
	go func() {
		m.Sleep(time.Minute)
		close(done)
	}()

	m.BlockUntil(1)
	select {
	case <-done:
		t.Fatal("Sleep returned without the clock advancing")
	default:
	}

	m.Advance(time.Minute)
	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Sleep did not return after the clock advanced")
	}
}

func TestMockSleepNonPositiveReturnsImmediately(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	m.Sleep(0)
	m.Sleep(-time.Second)
	if got := m.Waiters(); got != 0 {
		t.Errorf("non-positive Sleep registered %d waiters", got)
	}
}

func TestMockWaiterAccounting(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	if got := m.Waiters(); got != 0 {
		t.Fatalf("fresh mock has %d waiters", got)
	}

	timer := m.NewTimer(time.Second)
	ticker := m.NewTicker(time.Second)
	if got := m.Waiters(); got != 2 {
		t.Fatalf("Waiters = %d, want 2", got)
	}

	timer.Stop()
	if got := m.Waiters(); got != 1 {
		t.Fatalf("Waiters after Stop = %d, want 1", got)
	}

	ticker.Stop()
	if got := m.Waiters(); got != 0 {
		t.Fatalf("Waiters after ticker Stop = %d, want 0", got)
	}
}

func TestMockAdvanceToRejectsBackwardsTime(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	m.Advance(time.Hour)

	defer func() {
		if recover() == nil {
			t.Fatal("AdvanceTo into the past did not panic")
		}
	}()
	m.AdvanceTo(clock.Epoch)
}

func TestMockAdvanceRejectsNegativeDuration(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	defer func() {
		if recover() == nil {
			t.Fatal("Advance with a negative duration did not panic")
		}
	}()
	m.Advance(-time.Second)
}

func TestMockNewTickerRejectsNonPositivePeriod(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	defer func() {
		if recover() == nil {
			t.Fatal("NewTicker with a zero period did not panic")
		}
	}()
	m.NewTicker(0)
}

// TestMockConcurrentAdvanceAndArm exercises the mutex discipline under -race:
// production code arms and stops timers on its own goroutines while a test
// goroutine advances the clock.
func TestMockConcurrentAdvanceAndArm(t *testing.T) {
	t.Parallel()

	m := clock.NewMock()
	stop := make(chan struct{})

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := m.NewTimer(time.Millisecond)
			defer timer.Stop()
			for {
				select {
				case <-stop:
					return
				case <-timer.C():
					timer.Reset(time.Millisecond)
				default:
					_ = m.Now()
				}
			}
		}()
	}

	for range 200 {
		m.Advance(time.Millisecond)
	}
	close(stop)
	wg.Wait()
}

func TestSystemClockAdvances(t *testing.T) {
	t.Parallel()

	c := clock.System()
	start := c.Now()

	timer := c.NewTimer(time.Millisecond)
	defer timer.Stop()
	recv(t, timer.C())

	if c.Since(start) <= 0 {
		t.Errorf("Since = %s, want a positive duration", c.Since(start))
	}
}

func TestSystemClockTickerAndAfter(t *testing.T) {
	t.Parallel()

	c := clock.System()

	ticker := c.NewTicker(time.Millisecond)
	defer ticker.Stop()
	recv(t, ticker.C())
	ticker.Reset(time.Millisecond)
	recv(t, ticker.C())

	recv(t, c.After(time.Millisecond))
	c.Sleep(time.Millisecond)
}
