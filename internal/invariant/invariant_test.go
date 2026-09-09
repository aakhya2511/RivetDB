package invariant_test

import (
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/invariant"
)

func TestAssertPassesSilently(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("satisfied assertion panicked: %v", r)
		}
	}()
	invariant.Assert(true, "RAFT-1", "term %d", 7)
}

// TestAssertPanicsWithTypedViolation matters because the chaos harness
// recovers panics to attribute a crash to a named invariant. A bare string
// panic would be indistinguishable from a nil dereference.
func TestAssertPanicsWithTypedViolation(t *testing.T) {
	t.Parallel()

	v := recoverViolation(t, func() {
		invariant.Assert(false, "RAFT-2", "commit index %d moved backwards from %d", 4, 9)
	})

	if v.Name != "RAFT-2" {
		t.Errorf("Name = %q, want RAFT-2", v.Name)
	}
	if v.Detail != "commit index 4 moved backwards from 9" {
		t.Errorf("Detail = %q", v.Detail)
	}
	if got, want := v.Error(), "invariant RAFT-2 violated: commit index 4 moved backwards from 9"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestViolationSatisfiesError(t *testing.T) {
	t.Parallel()

	var err error = &invariant.Violation{Name: "STORAGE-1", Detail: "checksum mismatch"}

	var v *invariant.Violation
	if !errors.As(err, &v) {
		t.Fatal("Violation is not recoverable through errors.As")
	}
	if v.Name != "STORAGE-1" {
		t.Errorf("Name = %q", v.Name)
	}
}

func TestUnnamedViolationMessage(t *testing.T) {
	t.Parallel()

	v := &invariant.Violation{Detail: "unreachable branch"}
	if got, want := v.Error(), "invariant violated: unreachable branch"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestFailfAlwaysPanics(t *testing.T) {
	t.Parallel()

	v := recoverViolation(t, func() {
		invariant.Failf("MVCC-3", "unknown intent state %d", 42)
	})
	if v.Detail != "unknown intent state 42" {
		t.Errorf("Detail = %q", v.Detail)
	}
}

func TestExpensiveToggle(t *testing.T) {
	// Not parallel: mutates a process-wide flag.
	prev := invariant.SetExpensive(false)
	t.Cleanup(func() { invariant.SetExpensive(prev) })

	if invariant.Expensive() {
		t.Fatal("Expensive reported true after being disabled")
	}

	if was := invariant.SetExpensive(true); was {
		t.Error("SetExpensive returned the new value rather than the previous one")
	}
	if !invariant.Expensive() {
		t.Fatal("Expensive reported false after being enabled")
	}
}

func recoverViolation(t *testing.T, fn func()) *invariant.Violation {
	t.Helper()

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		fn()
	}()

	if recovered == nil {
		t.Fatal("expected a panic, got none")
	}
	v, ok := recovered.(*invariant.Violation)
	if !ok {
		t.Fatalf("panicked with %T (%v), want *invariant.Violation", recovered, recovered)
	}
	return v
}
