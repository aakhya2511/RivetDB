package multiraft

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
)

type RebalanceCollector struct {
	mu              sync.Mutex
	meta            *MetaRange
	router          *Router
	clock           clock.Clock
	sampler         *TelemetrySampler
	minSamples      uint32
	wasUnavailable  map[raft.NodeID]bool
	recoverySamples map[raft.NodeID]uint32
}

func NewRebalanceCollector(meta *MetaRange, router *Router, c clock.Clock, alphaPPM uint64, minSamples uint32) (*RebalanceCollector, error) {
	if meta == nil || router == nil || c == nil || minSamples == 0 {
		return nil, ErrInvalidTelemetry
	}
	sampler, err := NewTelemetrySampler(c, alphaPPM)
	if err != nil {
		return nil, err
	}
	return &RebalanceCollector{meta: meta, router: router, clock: c, sampler: sampler, minSamples: minSamples,
		wasUnavailable: make(map[raft.NodeID]bool), recoverySamples: make(map[raft.NodeID]uint32)}, nil
}

func (c *RebalanceCollector) Collect(ctx context.Context) (RebalanceClusterSnapshot, error) {
	if ctx == nil {
		return RebalanceClusterSnapshot{}, ErrInvalidTelemetry
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	metadata := c.meta.Snapshot()
	bootstrap := metadata.Catalog.Snapshot()
	nodes := c.router.scheduler.Nodes()
	result := RebalanceClusterSnapshot{SnapshotTime: c.clock.Now(), CatalogGeneration: bootstrap.Generation,
		PolicyVersion: metadata.Rebalance.PolicyVersion, ControllerEpoch: metadata.Rebalance.ControllerEpoch}
	nodeMetrics := make(map[raft.NodeID]*RebalanceNodeMetric, len(bootstrap.Nodes))
	for _, nodeID := range bootstrap.Nodes {
		available := nodes[nodeID] != nil
		if !available {
			c.wasUnavailable[nodeID], c.recoverySamples[nodeID] = true, 0
		} else if c.wasUnavailable[nodeID] {
			c.recoverySamples[nodeID] = saturatingInc32(c.recoverySamples[nodeID])
			if c.recoverySamples[nodeID] >= c.minSamples {
				c.wasUnavailable[nodeID] = false
			}
		}
		metric := &RebalanceNodeMetric{NodeID: nodeID, Available: available, Healthy: available, Warming: available && c.wasUnavailable[nodeID]}
		nodeMetrics[nodeID] = metric
	}
	for _, migration := range metadata.Migrations {
		if migration.State == MigrationAborted || migration.State == MigrationSourceRetired {
			continue
		}
		if metric := nodeMetrics[migration.SourceNodeID]; metric != nil {
			metric.MigrationsOut++
			metric.MigrationLoad++
		}
		if metric := nodeMetrics[migration.TargetNodeID]; metric != nil {
			metric.MigrationsIn++
			metric.MigrationLoad++
		}
	}
	for _, split := range metadata.Splits {
		if split.State == SplitAborted || split.State == SplitCommitted {
			continue
		}
		for _, replica := range split.Left.Replicas {
			if metric := nodeMetrics[replica.NodeID]; metric != nil {
				metric.SplitsInFlight++
			}
		}
	}
	for _, descriptor := range bootstrap.Ranges {
		rangeMetric := RebalanceRangeMetric{RangeID: descriptor.RangeID, Generation: descriptor.Generation,
			StartKey: append([]byte(nil), descriptor.StartKey.Key...), EndKey: append([]byte(nil), descriptor.EndKey.Key...),
			StartUnbounded: descriptor.StartKey.Unbounded, EndUnbounded: descriptor.EndKey.Unbounded, Active: true}
		for _, split := range metadata.Splits {
			if split.Parent.RangeID == descriptor.RangeID && split.State != SplitAborted && split.State != SplitCommitted {
				rangeMetric.Splitting = true
			}
		}
		for _, migration := range metadata.Migrations {
			if migration.RangeID == descriptor.RangeID && migration.State != MigrationAborted && migration.State != MigrationSourceRetired {
				rangeMetric.Migrating = true
			}
		}
		for _, placement := range descriptor.Replicas {
			node := nodes[placement.NodeID]
			if node == nil {
				rangeMetric.Replicas = append(rangeMetric.Replicas, RebalanceReplicaMetric{RangeID: descriptor.RangeID, ReplicaID: placement.ReplicaID, NodeID: placement.NodeID})
				continue
			}
			replica, err := node.Replica(descriptor.RangeID)
			if err != nil {
				nodeMetrics[placement.NodeID].Healthy = false
				continue
			}
			status := replica.Status()
			local, err := replica.RebalanceTelemetry(ctx)
			if err != nil {
				return RebalanceClusterSnapshot{}, fmt.Errorf("collect range %d replica %d: %w", descriptor.RangeID, placement.ReplicaID, err)
			}
			readRate, writeRate, requestRate, samples, err := c.sampler.ObserveReplica(descriptor.RangeID, placement.ReplicaID, placement.NodeID, TrafficCounters{Reads: local.Reads, Writes: local.Writes, Requests: local.Requests})
			if err != nil {
				return RebalanceClusterSnapshot{}, fmt.Errorf("sample replica %d: %w", placement.ReplicaID, err)
			}
			lag := uint64(0)
			if status.Raft.CommitIndex > status.Raft.LastApplied {
				lag = status.Raft.CommitIndex - status.Raft.LastApplied
			}
			replicaMetric := RebalanceReplicaMetric{RangeID: descriptor.RangeID, ReplicaID: placement.ReplicaID, NodeID: placement.NodeID,
				Role: status.MembershipRole.String(), Leader: status.Raft.Role == raft.Leader, MatchIndex: status.Raft.MatchIndex[placement.NodeID],
				LastApplied: status.Raft.LastApplied, Lag: lag, LocalBytes: local.HistoricalBytes, Healthy: status.Fatal == nil && status.MembershipRole != replicatedrange.MembershipRetired}
			rangeMetric.Replicas = append(rangeMetric.Replicas, replicaMetric)
			nodeMetric := nodeMetrics[placement.NodeID]
			if status.MembershipRole == replicatedrange.MembershipVoter && status.Lifecycle == replicatedrange.LifecycleActive {
				nodeMetric.HostedReplicas++
				nodeMetric.LogicalBytes = saturatingAdd(nodeMetric.LogicalBytes, local.LogicalBytes)
				nodeMetric.PhysicalBytes = saturatingAdd(nodeMetric.PhysicalBytes, local.HistoricalBytes)
				nodeMetric.ReadRate = saturatingAdd(nodeMetric.ReadRate, readRate)
				nodeMetric.WriteRate = saturatingAdd(nodeMetric.WriteRate, writeRate)
				nodeMetric.RequestRate = saturatingAdd(nodeMetric.RequestRate, requestRate)
				nodeMetric.ApplyBacklog = saturatingAdd(nodeMetric.ApplyBacklog, lag)
			}
			if !replicaMetric.Healthy {
				nodeMetric.Healthy = false
			}
			if local.LogicalBytes > rangeMetric.LogicalBytes {
				rangeMetric.LogicalBytes, rangeMetric.KeyCount, rangeMetric.UserKeys = local.LogicalBytes, local.KeyCount, local.UserKeys
			}
			if local.HistoricalBytes > rangeMetric.PhysicalBytes {
				rangeMetric.PhysicalBytes = local.HistoricalBytes
			}
			rangeMetric.ReadRate = saturatingAdd(rangeMetric.ReadRate, readRate)
			rangeMetric.RequestRate = saturatingAdd(rangeMetric.RequestRate, requestRate)
			if samples > rangeMetric.Samples {
				rangeMetric.Samples = samples
			}
			if replicaMetric.Leader {
				rangeMetric.Leader, rangeMetric.CommitIndex, rangeMetric.LastApplied, rangeMetric.WriteRate = placement.NodeID, status.Raft.CommitIndex, status.Raft.LastApplied, writeRate
				rangeMetric.MVCCWatermark = uint64(status.MaxAppliedMVCC)
				nodeMetric.Leaders++
			}
		}
		if rangeMetric.CommitIndex > rangeMetric.LastApplied {
			rangeMetric.ApplyBacklog = rangeMetric.CommitIndex - rangeMetric.LastApplied
		}
		result.Ranges = append(result.Ranges, rangeMetric)
	}
	for _, metric := range nodeMetrics {
		result.Nodes = append(result.Nodes, *metric)
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].NodeID < result.Nodes[j].NodeID })
	return CanonicalizeRebalanceSnapshot(result)
}
