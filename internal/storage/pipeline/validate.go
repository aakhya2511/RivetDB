package pipeline

import (
	"github.com/rivetdb/rivetdb/internal/invariant"
)

func (p *Pipeline) validateIfEnabledLocked() {
	if !invariant.Expensive() {
		return
	}
	invariant.Assert(p.active != nil && p.active.state == StateActive && !p.active.table.Frozen(), "STORAGE-50", "pipeline lacks one mutable active table")
	invariant.Assert(!p.haveVisible || p.haveAssigned && p.visibleSequence <= p.lastAssigned, "STORAGE-98", "visible sequence exceeds assigned authority")
	invariant.Assert(len(p.immutables) <= p.maxImmutable, "STORAGE-55", "immutable count %d exceeds %d", len(p.immutables), p.maxImmutable)
	seen := map[uint64]struct{}{p.active.id: {}}
	for index, item := range p.immutables {
		_, duplicate := seen[item.id]
		invariant.Assert(!duplicate, "STORAGE-50", "duplicate generation %d", item.id)
		seen[item.id] = struct{}{}
		invariant.Assert(item.table.Frozen(), "STORAGE-51", "immutable generation %d is mutable", item.id)
		invariant.Assert(item.haveSeq && item.smallestSeq <= item.largestSeq, "STORAGE-57", "immutable generation %d has invalid sequence range", item.id)
		invariant.Assert(item.state == StateQueued || item.state == StateFlushing || item.state == StateInstalling || item.state == StateFailed, "STORAGE-51", "live immutable %d has state %s", item.id, item.state)
		if index > 0 {
			invariant.Assert(p.immutables[index-1].id < item.id, "STORAGE-57", "generation FIFO is unordered")
			invariant.Assert(p.immutables[index-1].largestSeq < item.smallestSeq, "STORAGE-57", "generation sequence ranges overlap")
		}
	}
}
