package multiraft

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"
)

const rebalancePolicyValueCount = 36

func appendRebalancePolicy(dst []byte, policy RebalancePolicy) []byte {
	flags := byte(0)
	for bit, enabled := range []bool{policy.Enabled, policy.Moves, policy.Splits, policy.Leaders, policy.EmergencyCapacityOverridesCooldown} {
		if enabled {
			flags |= 1 << bit
		}
	}
	dst = append(dst, flags, 0, 0, 0, 0, 0, 0, 0)
	values := []uint64{policy.Version, uint64(policy.SampleInterval), policy.EWMAAlphaPPM, uint64(policy.MinSamples), policy.NodeImbalanceStartPPM, policy.NodeImbalanceRecoveryPPM, //nolint:gosec // duration stored bit-for-bit
		policy.BytesWeight, policy.WriteWeight, policy.ReadWeight, policy.LeaderWeight, policy.BacklogWeight, policy.ReplicaWeight,
		policy.RangeBytesWeight, policy.RangeWriteWeight, policy.RangeReadWeight, policy.RangeBacklogWeight,
		policy.MaxLogicalBytesPerNode, policy.MaxReplicaCountPerNode, policy.MaxLeaderCountPerNode, policy.MaxWriteRatePerNode,
		policy.RangeSplitBytes, policy.RangeHotReadRate, policy.RangeHotWriteRate, policy.MinRangeBytes, policy.MaxRanges, policy.MinExpectedImprovement,
		uint64(policy.MaxActionsPerCycle), uint64(policy.MaxConcurrentMigrationsCluster), uint64(policy.MaxConcurrentSplitsCluster), uint64(policy.MaxOutgoingMigrationsPerNode), uint64(policy.MaxIncomingMigrationsPerNode), uint64(policy.MaxSplitsPerNode),
		uint64(policy.Cooldown), uint64(policy.SplitCooldown), uint64(policy.FailureCooldown), uint64(policy.HistoryLimit)} //nolint:gosec // durations are stored bit-for-bit
	for _, value := range values {
		dst = binary.LittleEndian.AppendUint64(dst, value)
	}
	return dst
}

func decodeRebalancePolicy(d *metadataDecoder) (RebalancePolicy, error) {
	flags, err := d.u8()
	if err != nil {
		return RebalancePolicy{}, err
	}
	reserved, err := d.take(7)
	if err != nil || flags&^byte(31) != 0 || !bytes.Equal(reserved, make([]byte, 7)) {
		return RebalancePolicy{}, ErrCorruptMetadata
	}
	values := make([]uint64, rebalancePolicyValueCount)
	for index := range values {
		values[index], err = d.u64()
		if err != nil {
			return RebalancePolicy{}, err
		}
	}
	for _, index := range []int{3, 26, 27, 28, 29, 30, 31, 35} {
		if values[index] > math.MaxUint32 {
			return RebalancePolicy{}, ErrCorruptMetadata
		}
	}
	policy := RebalancePolicy{Version: values[0], Enabled: flags&1 != 0, Moves: flags&2 != 0, Splits: flags&4 != 0, Leaders: flags&8 != 0,
		SampleInterval: time.Duration(int64(values[1])), EWMAAlphaPPM: values[2], MinSamples: uint32(values[3]), NodeImbalanceStartPPM: values[4], NodeImbalanceRecoveryPPM: values[5], //nolint:gosec // duration stored bit-for-bit; uint32 validated
		BytesWeight: values[6], WriteWeight: values[7], ReadWeight: values[8], LeaderWeight: values[9], BacklogWeight: values[10], ReplicaWeight: values[11],
		RangeBytesWeight: values[12], RangeWriteWeight: values[13], RangeReadWeight: values[14], RangeBacklogWeight: values[15],
		MaxLogicalBytesPerNode: values[16], MaxReplicaCountPerNode: values[17], MaxLeaderCountPerNode: values[18], MaxWriteRatePerNode: values[19],
		RangeSplitBytes: values[20], RangeHotReadRate: values[21], RangeHotWriteRate: values[22], MinRangeBytes: values[23], MaxRanges: values[24], MinExpectedImprovement: values[25],
		MaxActionsPerCycle: uint32(values[26]), MaxConcurrentMigrationsCluster: uint32(values[27]), MaxConcurrentSplitsCluster: uint32(values[28]), MaxOutgoingMigrationsPerNode: uint32(values[29]), MaxIncomingMigrationsPerNode: uint32(values[30]), MaxSplitsPerNode: uint32(values[31]), //nolint:gosec // values validated above
		Cooldown: time.Duration(int64(values[32])), SplitCooldown: time.Duration(int64(values[33])), FailureCooldown: time.Duration(int64(values[34])), HistoryLimit: uint32(values[35]), EmergencyCapacityOverridesCooldown: flags&16 != 0} //nolint:gosec // validated widths and bit-preserving durations
	if policy.Version != 0 {
		if err := policy.Validate(); err != nil {
			return RebalancePolicy{}, ErrCorruptMetadata
		}
	}
	return policy, nil
}
