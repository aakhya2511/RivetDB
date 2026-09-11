package multiraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage/compaction"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func TestRandomizedDiskBackedMultiRangeRecovery(t *testing.T) {
	seed := testutil.Seed(t)
	rng := testutil.RandFromSeed(seed)
	cluster := newMultiTestCluster(t)
	cluster.elect(10, 1)
	cluster.elect(11, 3)
	cluster.elect(12, 5)
	reference := make(map[string][]byte)
	stats := struct{ proposals, rangePartitions, nodePartitions, rangeCrashes, nodeCrashes, restarts, flushes, compactions int }{}
	for event := 0; event < 400; event++ {
		descriptor := cluster.bootstrap.Ranges[rng.IntN(len(cluster.bootstrap.Ranges))]
		member := descriptor.Replicas[rng.IntN(len(descriptor.Replicas))]
		switch rng.IntN(10) {
		case 0, 1, 2:
			cluster.transport.Heal()
			ensureDiskLeaders(t, cluster)
			key := diskKey(descriptor.RangeID, event)
			value := []byte(fmt.Sprintf("seed-%d-event-%d", seed, event))
			if event%7 == 0 {
				if err := cluster.router.Delete(context.Background(), key); err != nil {
					t.Fatalf("seed=%d event=%d delete=%x: %v", seed, event, key, err)
				}
				delete(reference, string(key))
			} else {
				if err := cluster.router.Put(context.Background(), key, value); err != nil {
					t.Fatalf("seed=%d event=%d put=%x: %v", seed, event, key, err)
				}
				reference[string(key)] = value
			}
			stats.proposals++
		case 3:
			from, to := distinctNodes(rng, descriptor)
			cluster.transport.SetRangeLink(descriptor.RangeID, from, to, true)
			stats.rangePartitions++
		case 4:
			from, to := distinctClusterNodes(rng)
			cluster.transport.SetNodeLink(from, to, true)
			stats.nodePartitions++
		case 5:
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			if err := replica.Flush(context.Background()); err != nil {
				t.Fatal(err)
			}
			stats.flushes++
		case 6:
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			if err := replica.Compact(context.Background()); err == nil {
				stats.compactions++
			} else if !errors.Is(err, compaction.ErrNoCompaction) {
				t.Fatal(err)
			}
		case 7:
			if err := cluster.nodes[member.NodeID].RestartRange(descriptor.RangeID); err != nil {
				t.Fatal(err)
			}
			cluster.scheduler.Refresh()
			stats.rangeCrashes++
			stats.restarts++
		case 8:
			cluster.scheduler.RemoveNode(member.NodeID)
			if err := cluster.nodes[member.NodeID].Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenNode(NodeOptions{NodeID: member.NodeID, Directory: cluster.nodeRoot(member.NodeID), MemTableBytes: 256 + uint64(member.NodeID)*128})
			if err != nil {
				t.Fatal(err)
			}
			cluster.nodes[member.NodeID] = reopened
			if err := cluster.scheduler.AddNode(reopened); err != nil {
				t.Fatal(err)
			}
			stats.nodeCrashes++
			stats.restarts++
		case 9:
			cluster.transport.Heal()
		}
	}
	cluster.transport.Heal()
	ensureDiskLeaders(t, cluster)
	convergeDiskRanges(t, cluster)
	actual := make(map[string][]byte)
	for _, descriptor := range cluster.bootstrap.Ranges {
		var digest [32]byte
		for index, member := range descriptor.Replicas {
			replica, err := cluster.nodes[member.NodeID].Replica(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := replica.Digest(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if index == 0 {
				digest = current
				values, scanErr := replica.LocalScan(context.Background())
				if scanErr != nil {
					t.Fatal(scanErr)
				}
				for _, value := range values {
					actual[string(value.Key)] = value.Value
				}
			} else if current != digest {
				t.Fatalf("seed=%d range=%d digest mismatch", seed, descriptor.RangeID)
			}
		}
	}
	if !stateEqual(actual, reference) {
		t.Fatalf("seed=%d global reference mismatch actual=%d reference=%d", seed, len(actual), len(reference))
	}
	t.Logf("seed=%d nodes=5 ranges=3 events=400 proposals=%d range_partitions=%d node_partitions=%d range_crashes=%d node_crashes=%d restarts=%d flushes=%d compactions=%d invariant_violations=0 digest_mismatches=0", seed, stats.proposals, stats.rangePartitions, stats.nodePartitions, stats.rangeCrashes, stats.nodeCrashes, stats.restarts, stats.flushes, stats.compactions)
}

func convergeDiskRanges(t *testing.T, cluster *multiTestCluster) {
	t.Helper()
	for _, descriptor := range cluster.bootstrap.Ranges {
		converged := false
		for round := 0; round < 100 && !converged; round++ {
			var leader raft.NodeID
			var target uint64
			for _, member := range descriptor.Replicas {
				status := rangeStatus(t, cluster.nodes[member.NodeID], descriptor.RangeID)
				if status.Raft.Role == raft.Leader {
					leader, target = member.NodeID, status.Raft.LastApplied
					break
				}
			}
			if leader == 0 {
				ensureDiskLeaders(t, cluster)
				continue
			}
			outbound, err := cluster.nodes[leader].Tick(descriptor.RangeID)
			if err != nil {
				t.Fatal(err)
			}
			cluster.send(outbound)
			cluster.drain(100_000)
			converged = true
			for _, member := range descriptor.Replicas {
				if rangeStatus(t, cluster.nodes[member.NodeID], descriptor.RangeID).Raft.LastApplied != target {
					converged = false
				}
			}
		}
		if !converged {
			t.Fatalf("range=%d did not converge", descriptor.RangeID)
		}
	}
}

func ensureDiskLeaders(t *testing.T, cluster *multiTestCluster) {
	t.Helper()
	for round := 0; round < 500; round++ {
		all := true
		for _, descriptor := range cluster.bootstrap.Ranges {
			leader := raft.NodeID(0)
			for _, member := range descriptor.Replicas {
				status := rangeStatus(t, cluster.nodes[member.NodeID], descriptor.RangeID)
				if status.Raft.Role == raft.Leader {
					leader = member.NodeID
					break
				}
			}
			if leader == 0 {
				all = false
			} else {
				cluster.router.RecordLeader(descriptor.RangeID, leader)
			}
		}
		if all {
			return
		}
		if err := cluster.scheduler.Run(15); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("disk-backed groups failed to elect")
}

func diskKey(rangeID RangeID, event int) []byte {
	switch rangeID {
	case 10:
		return []byte(fmt.Sprintf("a-random-%04d", event))
	case 11:
		return []byte(fmt.Sprintf("g-random-%04d", event))
	default:
		return []byte(fmt.Sprintf("p-random-%04d", event))
	}
}

type multiSimulation struct {
	t           *testing.T
	descriptors []RangeDescriptor
	groups      map[RangeID]*raft.Simulator
	committed   map[RangeID]map[uint64]replicatedrange.Command
}

type simulationMachine struct {
	descriptor RangeDescriptor
	committed  map[uint64]replicatedrange.Command
	state      map[string][]byte
}

func (m *simulationMachine) Apply(entry raft.Entry) error {
	command, err := replicatedrange.DecodeCommand(entry.Command)
	if err != nil {
		return err
	}
	if !m.descriptor.Contains(command.Key) {
		return ErrWrongRangeKey
	}
	if prior, exists := m.committed[entry.Index]; exists {
		if prior.Type != command.Type || !bytes.Equal(prior.Key, command.Key) || !bytes.Equal(prior.Value, command.Value) {
			return fmt.Errorf("%w: range=%d conflicting command index=%d", raft.ErrInvariantCheck, m.descriptor.RangeID, entry.Index)
		}
	} else {
		m.committed[entry.Index] = command
	}
	if command.Type == replicatedrange.CommandDelete {
		delete(m.state, string(command.Key))
	} else {
		m.state[string(command.Key)] = bytes.Clone(command.Value)
	}
	return nil
}
func (*simulationMachine) Snapshot() ([]byte, error) {
	return nil, errors.New("multiraft simulation snapshots disabled")
}
func (*simulationMachine) Restore([]byte) error {
	return errors.New("multiraft simulation snapshots disabled")
}

func newMultiSimulation(t *testing.T, rangeCount int, seed int64) *multiSimulation {
	t.Helper()
	descriptors := simulationDescriptors(rangeCount)
	result := &multiSimulation{t: t, descriptors: descriptors, groups: make(map[RangeID]*raft.Simulator), committed: make(map[RangeID]map[uint64]replicatedrange.Command)}
	for _, descriptor := range descriptors {
		committed := make(map[uint64]replicatedrange.Command)
		result.committed[descriptor.RangeID] = committed
		nodes := make([]raft.NodeID, len(descriptor.Replicas))
		for index, replica := range descriptor.Replicas {
			nodes[index] = replica.NodeID
		}
		simulator, err := raft.NewSimulator(raft.SimulatorConfig{NodeIDs: nodes, ElectionTimeoutMin: 5, ElectionTimeoutMax: 9,
			HeartbeatInterval: 1, TickDuration: time.Millisecond, Seed: uint64(seed) ^ uint64(descriptor.RangeID), Clock: clock.NewMock(),
			NewStateMachine: func(raft.NodeID) raft.StateMachine {
				return &simulationMachine{descriptor: cloneDescriptor(descriptor), committed: committed, state: make(map[string][]byte)}
			}})
		if err != nil {
			t.Fatal(err)
		}
		result.groups[descriptor.RangeID] = simulator
	}
	for _, descriptor := range descriptors {
		group := result.groups[descriptor.RangeID]
		for round := 0; round < 30 && currentSimulationLeader(group, descriptor) == 0; round++ {
			for _, replica := range descriptor.Replicas {
				if err := group.Tick(replica.NodeID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := group.DeliverAll(500); err != nil {
				t.Fatal(err)
			}
		}
		if currentSimulationLeader(group, descriptor) == 0 {
			t.Fatalf("range %d initial election failed", descriptor.RangeID)
		}
	}
	return result
}

func simulationDescriptors(count int) []RangeDescriptor {
	result := make([]RangeDescriptor, count)
	for index := range result {
		startValue := byte(index * 256 / count)
		endValue := byte((index + 1) * 256 / count)
		start, end := KeyBound{Key: []byte{startValue}}, KeyBound{Key: []byte{endValue}}
		if index == 0 {
			start = KeyBound{Unbounded: true}
		}
		if index == count-1 {
			end = KeyBound{Unbounded: true}
		}
		n1 := raft.NodeID(index%5 + 1)
		n2 := raft.NodeID((index+1)%5 + 1)
		n3 := raft.NodeID((index+2)%5 + 1)
		result[index] = RangeDescriptor{RangeID: RangeID(100 + index), Generation: 1, StartKey: start, EndKey: end,
			Replicas: []ReplicaDescriptor{{ReplicaID: ReplicaID(index*10 + 1), NodeID: n1}, {ReplicaID: ReplicaID(index*10 + 2), NodeID: n2}, {ReplicaID: ReplicaID(index*10 + 3), NodeID: n3}}}
	}
	return result
}

type chaosStats struct {
	proposals, rangePartitions, nodePartitions, rangeCrashes, nodeCrashes, restarts, duplicates, drops int
}

func TestRandomizedMultiRaftSafety(t *testing.T) {
	seeds := []int64{401, 402}
	seeds = append(seeds, testutil.SeedCorpus(t, t.Name())...)
	seeds = append(seeds, testutil.Seed(t))
	for _, seed := range seeds {
		runMultiRaftChaos(t, seed, 5, 10_000)
	}
}

func TestRandomizedMultiRaftHeavy(t *testing.T) {
	if os.Getenv("RIVETDB_MULTIRAFT_STRESS") == "" {
		t.Skip("set RIVETDB_MULTIRAFT_STRESS=1 for 100k Multi-Raft events")
	}
	runMultiRaftChaos(t, testutil.Seed(t), 25, 100_000)
}

func runMultiRaftChaos(t *testing.T, seed int64, rangeCount, events int) {
	t.Helper()
	simulation := newMultiSimulation(t, rangeCount, seed)
	rng := testutil.RandFromSeed(seed)
	stats := chaosStats{}
	for event := 0; event < events; event++ {
		descriptor := simulation.descriptors[rng.IntN(len(simulation.descriptors))]
		group := simulation.groups[descriptor.RangeID]
		node := descriptor.Replicas[rng.IntN(len(descriptor.Replicas))].NodeID
		var err error
		switch rng.IntN(38) {
		case 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11:
			if group.Node(node) != nil {
				err = group.Tick(node)
			}
		case 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22:
			pending := group.Pending()
			if len(pending) != 0 {
				err = group.Deliver(pending[rng.IntN(len(pending))].ID)
			}
		case 28:
			pending := group.Pending()
			if len(pending) != 0 {
				err = group.Drop(pending[rng.IntN(len(pending))].ID)
				stats.drops++
			}
		case 29:
			pending := group.Pending()
			if len(pending) != 0 {
				_, err = group.Duplicate(pending[rng.IntN(len(pending))].ID)
				stats.duplicates++
			}
		case 30:
			from, to := distinctNodes(rng, descriptor)
			err = group.SetLink(from, to, false)
			stats.rangePartitions++
		case 31:
			from, to := distinctClusterNodes(rng)
			for _, candidateDescriptor := range simulation.descriptors {
				_, hasFrom := candidateDescriptor.ReplicaOn(from)
				_, hasTo := candidateDescriptor.ReplicaOn(to)
				if hasFrom && hasTo {
					_ = simulation.groups[candidateDescriptor.RangeID].SetLink(from, to, false)
				}
			}
			stats.nodePartitions++
		case 32:
			group.Heal()
		case 33:
			for _, candidate := range simulation.groups {
				candidate.Heal()
			}
		case 34:
			if group.Node(node) != nil {
				err = group.Crash(node)
				stats.rangeCrashes++
			}
		case 35:
			if group.Node(node) == nil {
				err = group.Restart(node)
				stats.restarts++
			}
		case 36:
			for _, candidateDescriptor := range simulation.descriptors {
				if _, member := candidateDescriptor.ReplicaOn(node); member {
					candidate := simulation.groups[candidateDescriptor.RangeID]
					if candidate.Node(node) != nil {
						_ = candidate.Crash(node)
					}
				}
			}
			stats.nodeCrashes++
		case 37:
			for _, candidateDescriptor := range simulation.descriptors {
				if _, member := candidateDescriptor.ReplicaOn(node); member {
					candidate := simulation.groups[candidateDescriptor.RangeID]
					if candidate.Node(node) == nil {
						_ = candidate.Restart(node)
						stats.restarts++
					}
				}
			}
		case 23, 24, 25, 26, 27:
			leader := currentSimulationLeader(group, descriptor)
			if leader != 0 {
				key := simulationKey(rng, descriptor)
				kind := replicatedrange.CommandPut
				value := []byte(fmt.Sprintf("v-%d-%d", event, seed))
				if event%7 == 0 {
					kind, value = replicatedrange.CommandDelete, nil
				}
				encoded, encodeErr := replicatedrange.EncodeCommand(replicatedrange.Command{Type: kind, Key: key, Value: value})
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				_, err = group.Propose(leader, encoded)
				if err == nil {
					stats.proposals++
				}
			}
		}
		if err != nil && !errors.Is(err, raft.ErrUnavailable) && !errors.Is(err, raft.ErrStopped) && !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("seed=%d event=%d range=%d node=%d: %v trace=%+v", seed, event, descriptor.RangeID, node, err, group.Trace())
		}
		if err := group.CheckSafety(); err != nil {
			t.Fatalf("seed=%d event=%d range=%d: %v", seed, event, descriptor.RangeID, err)
		}
	}
	simulation.converge(seed)
	t.Logf("seed=%d nodes=5 ranges=%d events=%d proposals=%d range_partitions=%d node_partitions=%d range_crashes=%d node_crashes=%d restarts=%d duplicates=%d drops=%d invariant_violations=0 digest_mismatches=0", seed, rangeCount, events, stats.proposals, stats.rangePartitions, stats.nodePartitions, stats.rangeCrashes, stats.nodeCrashes, stats.restarts, stats.duplicates, stats.drops)
}

func (s *multiSimulation) converge(seed int64) {
	s.t.Helper()
	for _, descriptor := range s.descriptors {
		group := s.groups[descriptor.RangeID]
		group.Heal()
		for _, replica := range descriptor.Replicas {
			if group.Node(replica.NodeID) == nil {
				if err := group.Restart(replica.NodeID); err != nil {
					s.t.Fatal(err)
				}
			}
		}
		converged := false
		for round := 0; round < 300 && !converged; round++ {
			for _, replica := range descriptor.Replicas {
				if err := group.Tick(replica.NodeID); err != nil {
					s.t.Fatal(err)
				}
			}
			if _, err := group.DeliverAll(500); err != nil {
				s.t.Fatalf("seed=%d range=%d converge: %v", seed, descriptor.RangeID, err)
			}
			leader := currentSimulationLeader(group, descriptor)
			if leader != 0 {
				key := simulationKey(rand.New(rand.NewPCG(uint64(seed), uint64(descriptor.RangeID))), descriptor)
				encoded, _ := replicatedrange.EncodeCommand(replicatedrange.Command{Type: replicatedrange.CommandPut, Key: key, Value: []byte("convergence")})
				index, err := group.Propose(leader, encoded)
				if err == nil {
					_, _ = group.DeliverAll(500)
					converged = allSimulationApplied(group, descriptor, index)
				}
			}
			if !converged {
				for _, envelope := range group.Pending() {
					_ = group.Drop(envelope.ID)
				}
			}
		}
		if !converged {
			s.t.Fatalf("seed=%d range=%d failed convergence trace=%+v", seed, descriptor.RangeID, group.Trace())
		}
		expected := expectedState(s.committed[descriptor.RangeID])
		for _, replica := range descriptor.Replicas {
			machine := group.StateMachine(replica.NodeID).(*simulationMachine)
			if !stateEqual(machine.state, expected) {
				s.t.Fatalf("seed=%d range=%d node=%d state mismatch", seed, descriptor.RangeID, replica.NodeID)
			}
			for key := range machine.state {
				if !descriptor.Contains([]byte(key)) {
					s.t.Fatalf("seed=%d range=%d contamination key=%x", seed, descriptor.RangeID, key)
				}
			}
		}
	}
}

func currentSimulationLeader(group *raft.Simulator, descriptor RangeDescriptor) raft.NodeID {
	var leader raft.NodeID
	for _, replica := range descriptor.Replicas {
		if node := group.Node(replica.NodeID); node != nil && node.Status().Role == raft.Leader {
			if leader != 0 {
				return 0
			}
			leader = replica.NodeID
		}
	}
	return leader
}

func allSimulationApplied(group *raft.Simulator, descriptor RangeDescriptor, index uint64) bool {
	for _, replica := range descriptor.Replicas {
		if group.Node(replica.NodeID).Status().LastApplied < index {
			return false
		}
	}
	return true
}

func expectedState(commands map[uint64]replicatedrange.Command) map[string][]byte {
	indices := make([]uint64, 0, len(commands))
	for index := range commands {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(left, right int) bool { return indices[left] < indices[right] })
	result := make(map[string][]byte)
	for _, index := range indices {
		command := commands[index]
		if command.Type == replicatedrange.CommandDelete {
			delete(result, string(command.Key))
		} else {
			result[string(command.Key)] = command.Value
		}
	}
	return result
}

func stateEqual(left, right map[string][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if !bytes.Equal(value, right[key]) {
			return false
		}
	}
	return true
}

func simulationKey(rng *rand.Rand, descriptor RangeDescriptor) []byte {
	lower := 0
	if !descriptor.StartKey.Unbounded {
		lower = int(descriptor.StartKey.Key[0])
	}
	upper := 256
	if !descriptor.EndKey.Unbounded {
		upper = int(descriptor.EndKey.Key[0])
	}
	return []byte{byte(lower + rng.IntN(upper-lower))}
}

func distinctNodes(rng *rand.Rand, descriptor RangeDescriptor) (raft.NodeID, raft.NodeID) {
	left := descriptor.Replicas[rng.IntN(len(descriptor.Replicas))].NodeID
	right := left
	for right == left {
		right = descriptor.Replicas[rng.IntN(len(descriptor.Replicas))].NodeID
	}
	return left, right
}

func distinctClusterNodes(rng *rand.Rand) (raft.NodeID, raft.NodeID) {
	left := raft.NodeID(rng.IntN(5) + 1)
	right := left
	for right == left {
		right = raft.NodeID(rng.IntN(5) + 1)
	}
	return left, right
}
