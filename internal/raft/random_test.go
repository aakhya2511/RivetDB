package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"testing"

	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestRandomizedClusterSafety(t *testing.T) {
	fresh := testutil.Seed(t)
	for _, size := range []int{3, 5} {
		for _, seed := range []int64{101, 9_901, fresh} {
			t.Run(fmt.Sprintf("nodes-%d-seed-%d", size, seed), func(t *testing.T) {
				runRandomCluster(t, size, seed, 10_000)
			})
		}
	}
}

func TestRandomizedClusterHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_RAFT_STRESS") == "" {
		t.Skip("set RIVETDB_RAFT_STRESS=1 for the 100k-event Raft campaign")
	}
	runRandomCluster(t, 5, testutil.Seed(t), 100_000)
}

func runRandomCluster(t *testing.T, size int, seed int64, events int) {
	t.Helper()
	rng := testutil.RandFromSeed(seed)
	simulator := newTestSimulator(t, size, uint64(seed)) //nolint:gosec // replayable two's-complement seed
	proposals, partitions, crashes, restarts := 0, 0, 0, 0
	for step := range events {
		action := rng.Uint64N(100)
		switch {
		case action < 38:
			id := randomRunning(simulator, rng)
			if id != 0 {
				mustRandomAction(t, simulator.Tick(id), simulator, seed, step)
			}
		case action < 68:
			deliverable := deliverableMessages(simulator)
			if len(deliverable) != 0 {
				envelope := deliverable[rng.IntN(len(deliverable))]
				mustRandomAction(t, simulator.Deliver(envelope.ID), simulator, seed, step)
			}
		case action < 73:
			pending := simulator.Pending()
			if len(pending) != 0 {
				_, err := simulator.Duplicate(pending[rng.IntN(len(pending))].ID)
				mustRandomAction(t, err, simulator, seed, step)
			}
		case action < 78:
			pending := simulator.Pending()
			if len(pending) != 0 {
				mustRandomAction(t, simulator.Drop(pending[rng.IntN(len(pending))].ID), simulator, seed, step)
			}
		case action < 83:
			from, to := randomDistinctIDs(simulator.ids, rng)
			enabled := rng.Uint64N(2) == 0
			mustRandomAction(t, simulator.SetLink(from, to, enabled), simulator, seed, step)
			partitions++
		case action < 87:
			simulator.Heal()
		case action < 91:
			if runningCount(simulator) > 1 {
				id := randomRunning(simulator, rng)
				mustRandomAction(t, simulator.Crash(id), simulator, seed, step)
				crashes++
			}
		case action < 94:
			stopped := stoppedIDs(simulator)
			if len(stopped) != 0 {
				mustRandomAction(t, simulator.Restart(stopped[rng.IntN(len(stopped))]), simulator, seed, step)
				restarts++
			}
		case action < 99:
			leaders := currentLeaders(simulator)
			if len(leaders) != 0 {
				leader := leaders[rng.IntN(len(leaders))]
				_, err := simulator.Propose(leader, []byte(fmt.Sprintf("p:%d:%d", seed, proposals)))
				if err == nil {
					proposals++
				} else if !errors.Is(err, ErrNotLeader) {
					mustRandomAction(t, err, simulator, seed, step)
				}
			}
		default:
			leaders := currentLeaders(simulator)
			if len(leaders) != 0 {
				leader := simulator.Node(leaders[0])
				if leader.Status().LastApplied > leader.Status().Snapshot.Index {
					_, err := leader.CreateSnapshot()
					mustRandomAction(t, err, simulator, seed, step)
				}
			}
		}
		mustRandomAction(t, simulator.CheckSafety(), simulator, seed, step)
	}
	convergeCluster(t, simulator, seed)
	t.Logf("seed=%d nodes=%d events=%d proposals=%d link_changes=%d crashes=%d restarts=%d leaders=%d committed=%d", seed, size, events, proposals, partitions, crashes, restarts, len(simulator.leaders), len(simulator.committed))
}

func convergeCluster(t *testing.T, simulator *Simulator, seed int64) {
	t.Helper()
	simulator.Heal()
	for _, id := range stoppedIDs(simulator) {
		mustRandomAction(t, simulator.Restart(id), simulator, seed, -1)
	}
	for _, envelope := range simulator.Pending() {
		if err := simulator.Drop(envelope.ID); err != nil {
			t.Fatal(err)
		}
	}
	for round := range 200 {
		for _, id := range simulator.ids {
			mustRandomAction(t, simulator.Tick(id), simulator, seed, -round-2)
		}
		if _, err := simulator.DeliverAll(100_000); err != nil {
			mustRandomAction(t, err, simulator, seed, -round-2)
		}
		leaders := currentLeaders(simulator)
		if len(leaders) == 1 {
			index, err := simulator.Propose(leaders[0], []byte("convergence-marker"))
			if err == nil {
				if _, err := simulator.DeliverAll(100_000); err != nil {
					mustRandomAction(t, err, simulator, seed, -round-2)
				}
				if allApplied(simulator, index) {
					want := machineAt(t, simulator, leaders[0]).digest()
					for _, id := range simulator.ids {
						if got := machineAt(t, simulator, id).digest(); got != want {
							t.Fatalf("seed=%d converged node %d digest=%s want=%s", seed, id, got, want)
						}
					}
					return
				}
			}
		}
	}
	t.Fatalf("seed=%d cluster did not converge; trace=%+v", seed, simulator.Trace())
}

func allApplied(simulator *Simulator, index uint64) bool {
	for _, id := range simulator.ids {
		if simulator.Node(id).Status().LastApplied < index {
			return false
		}
	}
	return true
}

func currentLeaders(simulator *Simulator) []NodeID {
	var result []NodeID
	for _, id := range simulator.ids {
		if node := simulator.Node(id); node != nil && node.Status().Role == Leader {
			result = append(result, id)
		}
	}
	return result
}

func deliverableMessages(simulator *Simulator) []Envelope {
	var result []Envelope
	for _, envelope := range simulator.Pending() {
		if simulator.links[directedLink{from: envelope.Message.From, to: envelope.Message.To}] {
			result = append(result, envelope)
		}
	}
	return result
}

func randomRunning(simulator *Simulator, rng *rand.Rand) NodeID {
	var ids []NodeID
	for _, id := range simulator.ids {
		if simulator.Node(id) != nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 0
	}
	return ids[rng.IntN(len(ids))]
}

func stoppedIDs(simulator *Simulator) []NodeID {
	return slices.DeleteFunc(slices.Clone(simulator.ids), func(id NodeID) bool { return simulator.Node(id) != nil })
}

func runningCount(simulator *Simulator) int { return len(simulator.ids) - len(stoppedIDs(simulator)) }

func randomDistinctIDs(ids []NodeID, rng *rand.Rand) (NodeID, NodeID) {
	from := rng.IntN(len(ids))
	to := rng.IntN(len(ids) - 1)
	if to >= from {
		to++
	}
	return ids[from], ids[to]
}

func mustRandomAction(t *testing.T, err error, simulator *Simulator, seed int64, step int) {
	t.Helper()
	if err != nil {
		t.Fatalf("seed=%d step=%d error=%v trace=%+v", seed, step, err, simulator.Trace())
	}
}
