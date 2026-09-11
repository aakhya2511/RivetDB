package pipeline

import (
	"fmt"

	"github.com/rivetdb/rivetdb/internal/storage/memtable"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

// FlushState is one explicit state in an immutable generation's lifecycle.
type FlushState uint8

const (
	StateActive FlushState = iota
	StateQueued
	StateFlushing
	StateFailed
	StateDurable
	StateInstalling
	StateInstalled
)

func (s FlushState) String() string {
	switch s {
	case StateActive:
		return "active"
	case StateQueued:
		return "queued"
	case StateFlushing:
		return "flushing"
	case StateFailed:
		return "failed"
	case StateDurable:
		return "durable"
	case StateInstalling:
		return "installing"
	case StateInstalled:
		return "installed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

type generation struct {
	id           uint64
	table        *memtable.MemTable
	state        FlushState
	fileNumber   uint64
	haveSeq      bool
	smallestSeq  uint64
	largestSeq   uint64
	flushErr     error
	ambiguous    bool
	physical     bool
	haveApplied  bool
	firstApplied uint64
	lastApplied  uint64
}

func (g *generation) transition(next FlushState) error {
	valid := g.state == StateActive && next == StateQueued ||
		g.state == StateQueued && next == StateFlushing ||
		g.state == StateFlushing && (next == StateDurable || next == StateFailed) ||
		g.state == StateDurable && next == StateInstalling ||
		g.state == StateInstalling && (next == StateInstalled || next == StateFailed) ||
		g.state == StateFailed && next == StateQueued && !g.physical
	if !valid {
		return fmt.Errorf("%w: generation %d %s -> %s", ErrInvalidTransition, g.id, g.state, next)
	}
	g.state = next
	return nil
}

// FlushOutput identifies one physically durable, validated SSTable. It is not
// logically installed until Phase 1G records it in a manifest.
type FlushOutput struct {
	Generation  uint64
	FileNumber  uint64
	SmallestSeq uint64
	LargestSeq  uint64
	Metadata    sstable.Metadata
	Path        string
}

// TableInstallation is a physically durable, validated SSTable awaiting
// logical installation by a Manifest authority.
type TableInstallation struct {
	Generation          uint64
	SmallestSequence    uint64
	LargestSequence     uint64
	Metadata            sstable.Metadata
	Path                string
	HaveAppliedCoverage bool
	FirstAppliedIndex   uint64
	LastAppliedIndex    uint64
	MaxMVCCTimestamp    uint64
}

// Stats is a point-in-time copy of pipeline lifecycle counters and memory.
type Stats struct {
	ActiveGeneration     uint64
	ActiveBytes          uint64
	ImmutableCount       int
	ImmutableBytes       uint64
	NextSequence         uint64
	SequenceExhausted    bool
	LastAssigned         uint64
	HaveAssigned         bool
	VisibleSequence      uint64
	HaveVisible          bool
	Rotations            uint64
	Flushes              uint64
	Installs             uint64
	FlushFailures        uint64
	FlushNanos           uint64
	SSTableBytes         uint64
	BackpressureEvents   uint64
	BackpressureNanos    uint64
	ReplicatedMode       bool
	ReplicatedApplied    uint64
	DurableAppliedAtOpen uint64
}

// ReadGeneration is one MemTable participating in a coherent read view.
// Table remains valid for the lifetime of the Pipeline.
type ReadGeneration struct {
	Generation uint64
	FileNumber uint64
	Table      *memtable.MemTable
}

// ReadSnapshot is an immutable description of the MemTable side of one read.
type ReadSnapshot struct {
	Active            ReadGeneration
	Immutables        []ReadGeneration
	LatestSequence    uint64
	HaveSequence      bool
	SequenceExhausted bool
}
