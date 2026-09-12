package multiraft

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"sync"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
)

// TrafficCounters are monotonic request counters. Callers must use saturating
// counters: a decrease is treated as a process restart, never as a huge rate.
type TrafficCounters struct {
	Reads, Writes, Requests uint64
}

type trafficSample struct {
	at       time.Time
	counters TrafficCounters
	read     uint64
	write    uint64
	request  uint64
	samples  uint32
}

type trafficIdentity struct {
	rangeID   RangeID
	replicaID ReplicaID
	nodeID    raft.NodeID
}

// TelemetrySampler derives deterministic integer EWMA rates per durable
// replica identity. It owns no timer; the controller decides when to sample.
type TelemetrySampler struct {
	mu      sync.Mutex
	clock   clock.Clock
	alpha   uint64
	samples map[trafficIdentity]trafficSample
}

func NewTelemetrySampler(c clock.Clock, alphaPPM uint64) (*TelemetrySampler, error) {
	if c == nil || alphaPPM == 0 || alphaPPM > rateScale {
		return nil, ErrInvalidRebalancePolicy
	}
	return &TelemetrySampler{clock: c, alpha: alphaPPM, samples: make(map[trafficIdentity]trafficSample)}, nil
}

// Observe returns per-second EWMA rates. The first observation and an
// observation after counter/time regression establish a new baseline.
func (s *TelemetrySampler) Observe(id ReplicaID, counters TrafficCounters) (read, write, request uint64, samples uint32, err error) {
	return s.observe(trafficIdentity{replicaID: id}, counters)
}

func (s *TelemetrySampler) ObserveReplica(rangeID RangeID, id ReplicaID, nodeID raft.NodeID, counters TrafficCounters) (read, write, request uint64, samples uint32, err error) {
	if rangeID == 0 || nodeID == 0 {
		return 0, 0, 0, 0, ErrInvalidDescriptor
	}
	return s.observe(trafficIdentity{rangeID: rangeID, replicaID: id, nodeID: nodeID}, counters)
}

func (s *TelemetrySampler) observe(identity trafficIdentity, counters TrafficCounters) (read, write, request uint64, samples uint32, err error) {
	id := identity.replicaID
	if id == 0 {
		return 0, 0, 0, 0, ErrInvalidDescriptor
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	previous, exists := s.samples[identity]
	if !exists || now.Before(previous.at) || counters.Reads < previous.counters.Reads || counters.Writes < previous.counters.Writes || counters.Requests < previous.counters.Requests {
		s.samples[identity] = trafficSample{at: now, counters: counters}
		return 0, 0, 0, 0, nil
	}
	if now.Equal(previous.at) {
		return previous.read, previous.write, previous.request, previous.samples, nil
	}
	elapsed := now.Sub(previous.at)
	instantRead := perSecond(counters.Reads-previous.counters.Reads, elapsed)
	instantWrite := perSecond(counters.Writes-previous.counters.Writes, elapsed)
	instantRequest := perSecond(counters.Requests-previous.counters.Requests, elapsed)
	previous.read = ewma(previous.read, instantRead, s.alpha, previous.samples != 0)
	previous.write = ewma(previous.write, instantWrite, s.alpha, previous.samples != 0)
	previous.request = ewma(previous.request, instantRequest, s.alpha, previous.samples != 0)
	previous.samples = saturatingInc32(previous.samples)
	previous.at, previous.counters = now, counters
	s.samples[identity] = previous
	return previous.read, previous.write, previous.request, previous.samples, nil
}

func perSecond(delta uint64, elapsed time.Duration) uint64 {
	if delta == 0 || elapsed <= 0 {
		return 0
	}
	nanos := uint64(elapsed) //nolint:gosec // elapsed is positive and time.Duration is int64
	quotient, remainder := delta/nanos, delta%nanos
	if quotient > math.MaxUint64/uint64(time.Second) {
		return math.MaxUint64
	}
	result := quotient * uint64(time.Second)
	high, low := bits.Mul64(remainder, uint64(time.Second))
	fraction, _ := bits.Div64(high, low, nanos)
	return saturatingAdd(result, fraction)
}

func ewma(previous, current, alpha uint64, established bool) uint64 {
	if !established {
		return current
	}
	left := mulDivSaturating(current, alpha, rateScale)
	right := mulDivSaturating(previous, rateScale-alpha, rateScale)
	return saturatingAdd(left, right)
}

func mulDivSaturating(value, multiplier, divisor uint64) uint64 {
	if value == 0 || multiplier == 0 {
		return 0
	}
	high, low := bits.Mul64(value, multiplier)
	if high >= divisor {
		return math.MaxUint64
	}
	quotient, _ := bits.Div64(high, low, divisor)
	return quotient
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func saturatingInc32(value uint32) uint32 {
	if value == math.MaxUint32 {
		return value
	}
	return value + 1
}

// CanonicalizeRebalanceSnapshot deep-copies and sorts an observation. It also
// rejects duplicate identities because accepting them makes tie-breaking
// dependent on collector iteration order.
func CanonicalizeRebalanceSnapshot(source RebalanceClusterSnapshot) (RebalanceClusterSnapshot, error) {
	result := source
	result.Nodes = append([]RebalanceNodeMetric(nil), source.Nodes...)
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].NodeID < result.Nodes[j].NodeID })
	for index := 1; index < len(result.Nodes); index++ {
		if result.Nodes[index-1].NodeID == result.Nodes[index].NodeID {
			return RebalanceClusterSnapshot{}, fmt.Errorf("%w: duplicate node %d", ErrInvalidTelemetry, result.Nodes[index].NodeID)
		}
	}
	result.Ranges = append([]RebalanceRangeMetric(nil), source.Ranges...)
	for index := range result.Ranges {
		rangeMetric := &result.Ranges[index]
		rangeMetric.StartKey = append([]byte(nil), rangeMetric.StartKey...)
		rangeMetric.EndKey = append([]byte(nil), rangeMetric.EndKey...)
		rangeMetric.UserKeys = cloneAndSortKeys(rangeMetric.UserKeys)
		rangeMetric.Replicas = append([]RebalanceReplicaMetric(nil), rangeMetric.Replicas...)
		sort.Slice(rangeMetric.Replicas, func(i, j int) bool {
			if rangeMetric.Replicas[i].ReplicaID != rangeMetric.Replicas[j].ReplicaID {
				return rangeMetric.Replicas[i].ReplicaID < rangeMetric.Replicas[j].ReplicaID
			}
			return rangeMetric.Replicas[i].NodeID < rangeMetric.Replicas[j].NodeID
		})
		for replicaIndex := 1; replicaIndex < len(rangeMetric.Replicas); replicaIndex++ {
			if rangeMetric.Replicas[replicaIndex-1].ReplicaID == rangeMetric.Replicas[replicaIndex].ReplicaID {
				return RebalanceClusterSnapshot{}, fmt.Errorf("%w: duplicate replica identity", ErrInvalidTelemetry)
			}
		}
	}
	sort.Slice(result.Ranges, func(i, j int) bool { return result.Ranges[i].RangeID < result.Ranges[j].RangeID })
	for index := 1; index < len(result.Ranges); index++ {
		if result.Ranges[index-1].RangeID == result.Ranges[index].RangeID {
			return RebalanceClusterSnapshot{}, fmt.Errorf("%w: duplicate range %d", ErrInvalidTelemetry, result.Ranges[index].RangeID)
		}
	}
	return result, nil
}

func cloneAndSortKeys(source [][]byte) [][]byte {
	result := make([][]byte, len(source))
	for index := range source {
		result[index] = append([]byte(nil), source[index]...)
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result
}
