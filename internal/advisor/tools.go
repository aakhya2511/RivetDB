package advisor

import "errors"

var ErrSummaryNotFound = errors.New("advisor: summary not found")

// GetClusterSummary returns the bounded health projection.
func GetClusterSummary(snapshot AdvisorSnapshot) HealthSummary { return snapshot.Health }

func GetNodeSummary(snapshot AdvisorSnapshot, nodeID uint64) (NodeSummary, error) {
	for _, node := range snapshot.Nodes {
		if node.NodeID == nodeID {
			return node, nil
		}
	}
	return NodeSummary{}, ErrSummaryNotFound
}

func GetRangeSummary(snapshot AdvisorSnapshot, rangeID uint64) (RangeSummary, error) {
	for _, item := range snapshot.Ranges {
		if item.RangeID == rangeID {
			return item, nil
		}
	}
	return RangeSummary{}, ErrSummaryNotFound
}

func GetControllerDecision(snapshot AdvisorSnapshot, actionID uint64) (ActionSummary, error) {
	for _, action := range snapshot.RecentActions {
		if action.ActionID == actionID {
			return action, nil
		}
	}
	return ActionSummary{}, ErrSummaryNotFound
}

func GetBenchmarkSummary(snapshot AdvisorSnapshot, benchmark, scale string) (PerformanceRecord, error) {
	for _, result := range snapshot.Performance {
		if result.Benchmark == benchmark && result.Scale == scale {
			return result, nil
		}
	}
	return PerformanceRecord{}, ErrSummaryNotFound
}
