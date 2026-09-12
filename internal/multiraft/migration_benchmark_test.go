package multiraft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkReplicaMigration is an engineering baseline, not a production
// throughput claim. Use -benchtime=1x so each iteration performs one complete
// durable leader-replica migration.
func BenchmarkReplicaMigration(b *testing.B) {
	for range b.N {
		cluster := newMigrationCluster(b)
		cluster.elect(10, 1)
		catalog := mustCatalog(b, cluster.bootstrap)
		meta, err := OpenMetaRange(MetaRangeOptions{Nodes: cluster.bootstrap.Nodes, Directory: filepath.Join(cluster.root, "metadata"), Bootstrap: catalog})
		if err != nil {
			b.Fatal(err)
		}
		started := time.Now()
		milestones := make(map[MigrationHookStage]time.Duration)
		manager, err := NewMigrationManager(MigrationManagerOptions{Meta: meta, Router: cluster.router, Hook: func(stage MigrationHookStage, _ MigrationRecord) {
			milestones[stage] = time.Since(started)
		}})
		if err != nil {
			b.Fatal(err)
		}
		descriptor, _ := catalog.LookupByID(10)
		source, _ := descriptor.ReplicaOn(1)
		b.ResetTimer()
		record, err := manager.MoveReplica(context.Background(), 10, source.ReplicaID, 4)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		staged := filepath.Join(cluster.nodes[4].directory, "ranges", "10", fmt.Sprintf("replica-%d", record.TargetReplicaID), "staging", fmt.Sprintf("migration-%d.snapshot", record.MigrationID))
		if info, statErr := os.Stat(staged); statErr == nil {
			b.ReportMetric(float64(info.Size()), "snapshot-bytes")
		}
		b.ReportMetric(float64(record.PromotionBarrier-record.BootstrapIndex), "catchup-entries")
		for stage, name := range map[MigrationHookStage]string{SnapshotTargetDurable: "snapshot-durable-ns", TargetReady: "ready-ns", JointConfigCommitted: "joint-commit-ns", LeadershipTransferComplete: "leader-transfer-ns", MetaPlacementCommitted: "meta-cutover-ns"} {
			if elapsed, ok := milestones[stage]; ok {
				b.ReportMetric(float64(elapsed.Nanoseconds()), name)
			}
		}
		cluster.close()
	}
}
