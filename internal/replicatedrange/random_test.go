package replicatedrange

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type simulationMachine struct {
	engine *engine.Engine
	inner  stateMachine
}

func (m *simulationMachine) Apply(entry raft.Entry) error { return m.inner.Apply(entry) }
func (m *simulationMachine) Snapshot() ([]byte, error)    { return nil, ErrSnapshotDeferred }
func (m *simulationMachine) Restore([]byte) error         { return ErrSnapshotDeferred }

func TestRandomizedReplicatedRange(t *testing.T) {
	fresh := testutil.Seed(t)
	campaigns := []struct {
		nodes int
		seed  int64
	}{{3, 301}, {5, 501}}
	for _, seed := range testutil.SeedCorpus(t, t.Name()) {
		campaigns = append(campaigns, struct {
			nodes int
			seed  int64
		}{3, seed})
	}
	campaigns = append(campaigns, struct {
		nodes int
		seed  int64
	}{3, fresh})
	for _, campaign := range campaigns {
		t.Run(fmt.Sprintf("nodes-%d-seed-%d", campaign.nodes, campaign.seed), func(t *testing.T) {
			runRangeSimulation(t, campaign.nodes, campaign.seed, 10_000)
		})
	}
}

func TestRandomizedReplicatedRangeHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_RANGE_STRESS") == "" {
		t.Skip("set RIVETDB_RANGE_STRESS=1 for 100k integration events")
	}
	runRangeSimulation(t, 5, testutil.Seed(t), 100_000)
}

func runRangeSimulation(t *testing.T, nodes int, seed int64, events int) {
	t.Helper()
	root := t.TempDir()
	ids := make([]raft.NodeID, nodes)
	for index := range ids {
		ids[index] = raft.NodeID(index + 1)
	}
	machines := make(map[raft.NodeID]*simulationMachine)
	simulator, err := raft.NewSimulator(raft.SimulatorConfig{
		NodeIDs: ids, ElectionTimeoutMin: 5, ElectionTimeoutMax: 9, HeartbeatInterval: 2,
		TickDuration: time.Millisecond, Seed: uint64(seed), Clock: clock.NewMock(),
		NewStateMachine: func(id raft.NodeID) raft.StateMachine {
			local, openErr := engine.Open(engine.Options{Directory: filepath.Join(root, fmt.Sprintf("node-%d", id)), Mode: engine.ModeReplicated, MemTableBytes: 8 << 10})
			if openErr != nil {
				t.Fatalf("open simulated node %d: %v", id, openErr)
			}
			machine := &simulationMachine{engine: local}
			machine.inner.engine = local
			machines[id] = machine
			return machine
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, machine := range machines {
			_ = machine.engine.Close(context.Background())
		}
	})
	rng := testutil.RandFromSeed(seed)
	links := make(map[[2]raft.NodeID]bool)
	for _, from := range ids {
		for _, to := range ids {
			if from != to {
				links[[2]raft.NodeID{from, to}] = true
			}
		}
	}
	proposals, crashes, restarts, flushes, compactions := 0, 0, 0, 0, 0
	for step := 0; step < events; step++ {
		action := rng.Uint64N(100)
		switch {
		case action < 38:
			if id := randomRunningRange(simulator, ids, rng); id != 0 {
				checkRangeAction(t, simulator.Tick(id), seed, step)
			}
		case action < 68:
			pending := simulator.Pending()
			if len(pending) != 0 {
				envelope := pending[rng.IntN(len(pending))]
				if links[[2]raft.NodeID{envelope.Message.From, envelope.Message.To}] {
					checkRangeAction(t, simulator.Deliver(envelope.ID), seed, step)
				} else {
					checkRangeAction(t, simulator.Drop(envelope.ID), seed, step)
				}
			}
		case action < 74:
			pending := simulator.Pending()
			if len(pending) != 0 {
				checkRangeAction(t, simulator.Drop(pending[rng.IntN(len(pending))].ID), seed, step)
			}
		case action < 80:
			from, to := ids[rng.IntN(len(ids))], ids[rng.IntN(len(ids))]
			if from != to {
				enabled := rng.Uint64N(2) == 0
				links[[2]raft.NodeID{from, to}] = enabled
				checkRangeAction(t, simulator.SetLink(from, to, enabled), seed, step)
			}
		case action < 84:
			for link := range links {
				links[link] = true
			}
			simulator.Heal()
		case action < 87:
			if runningRangeCount(simulator, ids) > 1 {
				id := randomRunningRange(simulator, ids, rng)
				if machine := machines[id]; machine != nil {
					checkRangeAction(t, machine.engine.Close(context.Background()), seed, step)
					delete(machines, id)
				}
				checkRangeAction(t, simulator.Crash(id), seed, step)
				crashes++
			}
		case action < 90:
			stopped := stoppedRangeIDs(simulator, ids)
			if len(stopped) != 0 {
				checkRangeAction(t, simulator.Restart(stopped[rng.IntN(len(stopped))]), seed, step)
				restarts++
			}
		case action < 96:
			leaders := rangeLeaders(simulator, ids)
			if len(leaders) != 0 {
				command := Command{Type: CommandPut, Key: []byte(fmt.Sprintf("key-%02d", proposals%31)), Value: []byte(fmt.Sprintf("v-%06d", proposals))}
				if proposals%9 == 0 {
					command.Type, command.Value = CommandDelete, nil
				}
				encoded, encodeErr := EncodeCommand(command)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				if _, proposeErr := simulator.Propose(leaders[rng.IntN(len(leaders))], encoded); proposeErr == nil {
					proposals++
				} else if !errors.Is(proposeErr, raft.ErrNotLeader) {
					checkRangeAction(t, proposeErr, seed, step)
				}
			}
		case action < 99:
			if id := randomRunningRange(simulator, ids, rng); id != 0 {
				checkRangeAction(t, machines[id].engine.Flush(context.Background()), seed, step)
				flushes++
			}
		default:
			if id := randomRunningRange(simulator, ids, rng); id != 0 {
				_, compactErr := machines[id].engine.Compact(context.Background())
				if compactErr == nil {
					compactions++
				} else if !errors.Is(compactErr, compaction.ErrNoCompaction) {
					checkRangeAction(t, compactErr, seed, step)
				}
			}
		}
		checkRangeAction(t, simulator.CheckSafety(), seed, step)
	}
	for link := range links {
		links[link] = true
	}
	simulator.Heal()
	for _, id := range stoppedRangeIDs(simulator, ids) {
		checkRangeAction(t, simulator.Restart(id), seed, -1)
		restarts++
	}
	for _, envelope := range simulator.Pending() {
		checkRangeAction(t, simulator.Drop(envelope.ID), seed, -1)
	}
	var marker uint64
	for round := 0; round < 300; round++ {
		for _, id := range ids {
			checkRangeAction(t, simulator.Tick(id), seed, -round-2)
		}
		_, deliverErr := simulator.DeliverAll(100_000)
		checkRangeAction(t, deliverErr, seed, -round-2)
		leaders := rangeLeaders(simulator, ids)
		if len(leaders) == 1 {
			encoded, _ := EncodeCommand(Command{Type: CommandPut, Key: []byte("convergence-marker"), Value: []byte("yes")})
			marker, err = simulator.ProposeAndApply(leaders[0], encoded, 100_000)
			if err == nil {
				_, _ = simulator.DeliverAll(100_000)
				caughtUp := true
				for _, id := range ids {
					caughtUp = caughtUp && simulator.Node(id).Status().LastApplied >= marker
				}
				if caughtUp {
					break
				}
			}
		}
	}
	if marker == 0 {
		t.Fatalf("seed=%d failed to converge", seed)
	}
	var expected []engine.KV
	for offset, id := range ids {
		values, scanErr := machines[id].engine.Scan(context.Background(), nil, nil)
		if scanErr != nil {
			t.Fatal(scanErr)
		}
		if validateErr := machines[id].engine.Validate(); validateErr != nil {
			t.Fatal(validateErr)
		}
		if offset == 0 {
			expected = values
		} else if !equalLogicalState(expected, values) {
			t.Fatalf("seed=%d node=%d logical mismatch", seed, id)
		}
	}
	t.Logf("seed=%d nodes=%d events=%d proposals=%d crashes=%d restarts=%d flushes=%d compactions=%d digest_mismatches=0", seed, nodes, events, proposals, crashes, restarts, flushes, compactions)
}

func equalLogicalState(left, right []engine.KV) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}

func randomRunningRange(s *raft.Simulator, ids []raft.NodeID, rng *rand.Rand) raft.NodeID {
	var running []raft.NodeID
	for _, id := range ids {
		if s.Node(id) != nil {
			running = append(running, id)
		}
	}
	if len(running) == 0 {
		return 0
	}
	return running[rng.IntN(len(running))]
}
func runningRangeCount(s *raft.Simulator, ids []raft.NodeID) int {
	count := 0
	for _, id := range ids {
		if s.Node(id) != nil {
			count++
		}
	}
	return count
}
func stoppedRangeIDs(s *raft.Simulator, ids []raft.NodeID) []raft.NodeID {
	var result []raft.NodeID
	for _, id := range ids {
		if s.Node(id) == nil {
			result = append(result, id)
		}
	}
	return result
}
func rangeLeaders(s *raft.Simulator, ids []raft.NodeID) []raft.NodeID {
	var result []raft.NodeID
	for _, id := range ids {
		if node := s.Node(id); node != nil && node.Status().Role == raft.Leader {
			result = append(result, id)
		}
	}
	return result
}
func checkRangeAction(t *testing.T, err error, seed int64, step int) {
	t.Helper()
	if err != nil {
		t.Fatalf("seed=%d step=%d: %v", seed, step, err)
	}
}
