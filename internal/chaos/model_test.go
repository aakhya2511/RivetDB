package chaos

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestChaosConfigProfilesAndValidation(t *testing.T) {
	for _, profile := range []Profile{Smoke, Normal, Heavy, Overnight} {
		config := ProfileConfig(profile)
		if config.Version != ConfigVersion || config.Events <= 0 || config.TraceLimit > 10_000 {
			t.Fatalf("profile=%s config=%+v", profile, config)
		}
		config.Seed = 1
		if _, err := New(config); err != nil {
			t.Fatalf("profile=%s: %v", profile, err)
		}
	}
	bad := ProfileConfig(Smoke)
	bad.Nodes = 3
	if _, err := New(bad); err == nil {
		t.Fatal("three-node Phase 10 config accepted")
	}
}

func TestChaosModelNormal(t *testing.T) {
	var aggregate Counters
	for _, seed := range []int64{10_001, 10_002, testutil.Seed(t)} {
		config := ProfileConfig(Normal)
		config.Seed, config.Events = seed, 40_000
		result := runModel(t, config)
		addCounters(&aggregate, result.Counters)
	}
	assertCoverage(t, aggregate)
	t.Logf("normal nodes=7 initial_ranges=4 seeds=3 events=120000 counters=%+v", aggregate)
}

func TestChaosModelReplay(t *testing.T) {
	seed := envInt64(t, "RIVETDB_CHAOS_SEED", 10_003)
	events := int(envInt64(t, "RIVETDB_CHAOS_EVENTS", 10_000))
	config := ProfileConfig(Smoke)
	config.Seed, config.Events = seed, events
	first := runModel(t, config)
	second := runModel(t, config)
	if first.LatestDigest != second.LatestDigest || first.HistoricalDigest != second.HistoricalDigest || first.Counters != second.Counters {
		t.Fatalf("seed=%d events=%d schedule did not replay", seed, events)
	}
	t.Logf("seed=%d events=%d replay='RIVETDB_CHAOS_SEED=%d RIVETDB_CHAOS_EVENTS=%d go test -run TestChaosModelReplay ./internal/chaos'", seed, events, seed, events)
}

func TestChaosTraceAndStateRemainBounded(t *testing.T) {
	config := ProfileConfig(Smoke)
	config.Seed, config.Events, config.TraceLimit, config.MaxTransactions = 10_004, 50_000, 257, 8
	result := runModel(t, config)
	if result.MaxTraceObserved != 257 || len(result.Trace) != 257 || result.OpenTransactions != 0 || result.ActiveRanges > config.MaxRanges {
		t.Fatalf("result=%+v", result)
	}
}

func TestChaosModelHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_CHAOS_STRESS") == "" {
		t.Skip("set RIVETDB_CHAOS_STRESS=1 for 1,000,000 events")
	}
	config := ProfileConfig(Heavy)
	config.Seed = 100_010
	result := runModel(t, config)
	assertCoverage(t, result.Counters)
	t.Logf("heavy seed=%d nodes=%d initial_ranges=%d active_ranges=%d events=%d bank_total=%d counters=%+v",
		config.Seed, config.Nodes, config.InitialRanges, result.ActiveRanges, config.Events, result.BankTotal, result.Counters)
}

func TestChaosModelOvernight(t *testing.T) {
	if os.Getenv("RIVETDB_CHAOS_OVERNIGHT") == "" {
		t.Skip("set RIVETDB_CHAOS_OVERNIGHT=1 for 5,000,000 events")
	}
	config := ProfileConfig(Overnight)
	config.Seed = 100_012
	result := runModel(t, config)
	assertCoverage(t, result.Counters)
	t.Logf("overnight seed=%d events=%d counters=%+v", config.Seed, config.Events, result.Counters)
}

func TestChaosTargetedOverlapMatrix(t *testing.T) {
	// The deterministic vocabulary places each pair/triple inside every 48-event
	// window while an earlier transaction and range operation remain active.
	config := ProfileConfig(Smoke)
	config.Seed, config.Events = 100_011, 4_800
	result := runModel(t, config)
	stats := result.Counters
	if stats.TxnBegun == 0 || stats.MigrationsStarted == 0 || stats.SplitsStarted == 0 || stats.LeadershipTransfers == 0 ||
		stats.ControllerCrashes == 0 || stats.MetaOutages == 0 || stats.Partitions == 0 || stats.HistoricalDigestChecks == 0 {
		t.Fatalf("pair/triple coverage missing: %+v", stats)
	}
	if stats.CatalogViolations+stats.MembershipViolations+stats.AtomicityViolations+stats.SIViolations != 0 {
		t.Fatalf("SAFETY violations: %+v", stats)
	}
}

func TestChaosCheckerClassificationAndInspectionIsolation(t *testing.T) {
	config := ProfileConfig(Smoke)
	config.Seed, config.Events = 100_013, 96
	harness, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for event := 0; event < config.Events; event++ {
		if err := harness.step(event); err != nil {
			t.Fatal(err)
		}
	}
	before := harness.stats
	if err := harness.checkCheap(); err != nil {
		t.Fatal(err)
	}
	if err := harness.checkExpensive(); err != nil {
		t.Fatal(err)
	}
	if harness.stats.Put != before.Put || harness.stats.GetAt != before.GetAt || harness.stats.AutomaticActions != before.AutomaticActions {
		t.Fatal("model inspection perturbed workload/controller counters")
	}
	harness.observed.versions[1] = append(harness.observed.versions[1], version{timestamp: 1, value: 99})
	if err := harness.checkExpensive(); !errors.Is(err, ErrLatestMismatch) {
		t.Fatalf("classification=%v", err)
	}
}

func runModel(t testing.TB, config Config) Result {
	t.Helper()
	harness, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	result, err := harness.Run()
	if err != nil {
		t.Fatal(err)
	}
	if result.BankTotal != 1_000_000 || result.Counters.LostAcknowledged != 0 || result.Counters.LatestMismatches != 0 || result.Counters.HistoricalMismatches != 0 {
		t.Fatalf("seed=%d invalid result=%+v", config.Seed, result)
	}
	return result
}

func assertCoverage(t testing.TB, c Counters) {
	t.Helper()
	values := []uint64{c.Put, c.Delete, c.GetAt, c.ScanAt, c.TxnBegun, c.TxnCommitted, c.TxnAborted,
		c.SplitsStarted, c.SplitsCompleted, c.MigrationsStarted, c.MigrationsCompleted, c.JointConfigs,
		c.LeadershipTransfers, c.AutomaticActions, c.NodeCrashes, c.NodeRestarts, c.RangeCrashes,
		c.RangeRestarts, c.Partitions, c.Heals, c.MetaOutages, c.FullClusterRestarts, c.Flushes,
		c.Compactions, c.Reclamations, c.HistoricalDigestChecks, c.InvariantChecks, c.StaleRejections}
	for index, value := range values {
		if value == 0 {
			t.Fatalf("required counter %d is zero: %+v", index, c)
		}
	}
}

func addCounters(target *Counters, source Counters) {
	// Keep this mechanical aggregation local to evidence; reflection would make
	// counter type changes silently affect the deterministic gate.
	target.Put += source.Put
	target.Delete += source.Delete
	target.GetAt += source.GetAt
	target.ScanAt += source.ScanAt
	target.TxnBegun += source.TxnBegun
	target.TxnCommitted += source.TxnCommitted
	target.TxnAborted += source.TxnAborted
	target.TxnConflicts += source.TxnConflicts
	target.SplitsStarted += source.SplitsStarted
	target.SplitsCompleted += source.SplitsCompleted
	target.SplitsAborted += source.SplitsAborted
	target.MigrationsStarted += source.MigrationsStarted
	target.MigrationsCompleted += source.MigrationsCompleted
	target.MigrationsAborted += source.MigrationsAborted
	target.JointConfigs += source.JointConfigs
	target.LeadershipTransfers += source.LeadershipTransfers
	target.AutomaticActions += source.AutomaticActions
	target.NodeCrashes += source.NodeCrashes
	target.NodeRestarts += source.NodeRestarts
	target.RangeCrashes += source.RangeCrashes
	target.RangeRestarts += source.RangeRestarts
	target.Partitions += source.Partitions
	target.Heals += source.Heals
	target.Drops += source.Drops
	target.Delays += source.Delays
	target.Duplicates += source.Duplicates
	target.Reorders += source.Reorders
	target.MetaOutages += source.MetaOutages
	target.MetaRecoveries += source.MetaRecoveries
	target.ControllerCrashes += source.ControllerCrashes
	target.ControllerRestarts += source.ControllerRestarts
	target.FullClusterRestarts += source.FullClusterRestarts
	target.Flushes += source.Flushes
	target.Compactions += source.Compactions
	target.Reclamations += source.Reclamations
	target.HistoricalDigestChecks += source.HistoricalDigestChecks
	target.InvariantChecks += source.InvariantChecks
	target.ExpensiveChecks += source.ExpensiveChecks
	target.StaleMessages += source.StaleMessages
	target.StaleRejections += source.StaleRejections
	target.SnapshotCreates += source.SnapshotCreates
	target.SnapshotReads += source.SnapshotReads
	target.RaftTicks += source.RaftTicks
	target.MessagesDelivered += source.MessagesDelivered
	target.TransactionReads += source.TransactionReads
	target.TransactionWrites += source.TransactionWrites
	target.ControllerNoops += source.ControllerNoops
	target.RecoveryEvents += source.RecoveryEvents
}

func envInt64(t testing.TB, name string, fallback int64) int64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		t.Fatalf("%s=%q: %v", name, raw, err)
	}
	return value
}

func ExampleProfileConfig() {
	config := ProfileConfig(Normal)
	fmt.Printf("version=%d profile=%s nodes=%d events=%d\n", config.Version, config.Profile, config.Nodes, config.Events)
	// Output: version=1 profile=normal nodes=7 events=40000
}
