// Package invariant provides the assertion mechanism RivetDB uses to guard
// safety properties.
//
// Policy: an invariant in RivetDB is a property whose violation means the
// system has already lost correctness — a committed Raft entry that changed, a
// range whose bounds overlap a sibling, an SSTable whose keys are not sorted.
// Continuing to run after such a violation risks turning a detectable bug into
// silent, persisted data corruption, and it destroys the evidence needed to
// diagnose it. Assertions therefore panic rather than return an error.
//
// This is deliberately not the mechanism for expected failures. Corrupt input
// from disk or the network, a full disk, a stale routing entry and a rejected
// RPC are all normal conditions in a distributed database; they are returned as
// errors and handled. Assert is only for statements the code believes cannot be
// false, and the message must say which invariant was broken so the panic is
// self-describing in a chaos log.
//
// Assertions are always compiled in. They sit on control paths (a Raft state
// transition, a compaction boundary, a range split commit), not in per-key
// inner loops, so the cost is not measurable; and an assertion disabled in
// production is exactly the assertion that would have caught the outage.
// Anything hot enough for that cost to matter belongs behind Expensive.
package invariant

import (
	"fmt"
	"sync/atomic"
)

// Violation is the panic value produced by a failed assertion. Test harnesses
// and the chaos runner recover it to attribute a crash to a named invariant
// instead of an anonymous panic.
type Violation struct {
	// Name identifies the invariant, using the identifiers from
	// docs/invariants.md (for example "RAFT-3" or "STORAGE-1") where one
	// applies.
	Name string
	// Detail describes the observed state that violated it.
	Detail string
}

func (v *Violation) Error() string {
	if v.Name == "" {
		return "invariant violated: " + v.Detail
	}
	return "invariant " + v.Name + " violated: " + v.Detail
}

// Assert panics with a *Violation if cond is false. name should identify the
// invariant; format and args describe the observed state.
func Assert(cond bool, name, format string, args ...any) {
	if cond {
		return
	}
	panic(&Violation{Name: name, Detail: fmt.Sprintf(format, args...)})
}

// Failf panics unconditionally. It is for branches that must be unreachable,
// such as the default case of a switch over an exhaustive enum.
func Failf(name, format string, args ...any) {
	panic(&Violation{Name: name, Detail: fmt.Sprintf(format, args...)})
}

// expensive controls checks whose cost is proportional to data size.
var expensive atomic.Bool

// SetExpensive enables or disables expensive invariant checks and returns the
// previous setting.
//
// Expensive checks are whole-structure validations: verifying that every key in
// a freshly written SSTable is ordered, that a range's key bounds tile the
// keyspace without gaps, that a Raft log has no index discontinuity. They are
// O(n) in the data they inspect, so they are off by default and enabled by
// tests, the chaos harness and debugging sessions — the settings where paying
// for a full structural check on every operation buys real signal.
func SetExpensive(enabled bool) bool {
	return expensive.Swap(enabled)
}

// Expensive reports whether expensive invariant checks are enabled. Guard
// costly validation with it:
//
//	if invariant.Expensive() {
//	    checkSSTableOrdering(t)
//	}
func Expensive() bool {
	return expensive.Load()
}
