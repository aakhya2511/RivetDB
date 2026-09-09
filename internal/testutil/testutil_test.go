package testutil_test

import (
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestSeedHonoursEnvironmentOverride(t *testing.T) {
	// Not parallel: mutates process environment.
	t.Setenv(testutil.EnvSeed, "8134472901")

	if got := testutil.Seed(t); got != 8134472901 {
		t.Errorf("Seed = %d, want 8134472901", got)
	}
}

// TestRandIsReproducible is the property the whole randomized-testing strategy
// rests on: a recorded seed must replay the same schedule, or a persisted
// failing seed is worthless.
func TestRandIsReproducible(t *testing.T) {
	t.Parallel()

	const seed = -1234567890123

	first := make([]uint64, 32)
	r := testutil.RandFromSeed(seed)
	for i := range first {
		first[i] = r.Uint64()
	}

	second := testutil.RandFromSeed(seed)
	for i := range first {
		if got := second.Uint64(); got != first[i] {
			t.Fatalf("draw %d = %d on replay, want %d", i, got, first[i])
		}
	}
}

func TestRandDiffersAcrossSeeds(t *testing.T) {
	t.Parallel()

	a := testutil.RandFromSeed(1).Uint64()
	b := testutil.RandFromSeed(2).Uint64()
	if a == b {
		t.Errorf("distinct seeds produced the same first draw %d", a)
	}
}

func TestSeedCorpusIsEmptyForUnknownTest(t *testing.T) {
	t.Parallel()

	if got := testutil.SeedCorpus(t, "TestThatHasNeverFailed"); got != nil {
		t.Errorf("SeedCorpus = %v, want nil", got)
	}
}

func TestNoLeaksAcceptsCleanShutdown(t *testing.T) {
	t.Parallel()
	defer testutil.NoLeaks(t)()

	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		<-stop
	}()

	close(stop)
	<-done
}

func TestEventuallySucceedsOnConvergence(t *testing.T) {
	t.Parallel()

	ready := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Millisecond)
		close(ready)
	}()

	testutil.Eventually(t, time.Second, "background goroutine signals ready", func() bool {
		select {
		case <-ready:
			return true
		default:
			return false
		}
	})
}

func TestConsistentlyHoldsForStableCondition(t *testing.T) {
	t.Parallel()

	testutil.Consistently(t, 10*time.Millisecond, "constant condition", func() bool { return true })
}
