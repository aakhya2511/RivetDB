// Package chaos implements the bounded deterministic Phase 10 logical model.
package chaos

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
)

const ConfigVersion = 1

var (
	ErrInvalidConfig       = errors.New("CHAOS-1: invalid chaos config")
	ErrBankConservation    = errors.New("CHAOS-5 SAFETY: bank conservation")
	ErrDuplicateRange      = errors.New("CHAOS-6 SAFETY: duplicate RangeID")
	ErrRetiredRange        = errors.New("CHAOS-7 SAFETY: retired range active")
	ErrRetiredReplica      = errors.New("CHAOS-7 SAFETY: retired replica voter")
	ErrDuplicateReplica    = errors.New("CHAOS-15 SAFETY: duplicate ReplicaID")
	ErrLearnerVotes        = errors.New("CHAOS-8 SAFETY: learner votes")
	ErrCatalogBoundary     = errors.New("CHAOS-6 SAFETY: catalog boundary")
	ErrCatalogTiling       = errors.New("CHAOS-6 SAFETY: catalog gap/overlap")
	ErrVersionOrder        = errors.New("CHAOS-3 SAFETY: invalid version order")
	ErrTraceUnbounded      = errors.New("CHAOS-13 TEST-HARNESS: trace unbounded")
	ErrLatestMismatch      = errors.New("CHAOS-2 SAFETY: latest digest mismatch")
	ErrHistoricalMismatch  = errors.New("CHAOS-3 SAFETY: historical digest mismatch")
	ErrTransactionMismatch = errors.New("CHAOS-4 SAFETY: transaction mismatch")
)

// Profile names a reproducible campaign shape. Values, not names, are evidence.
type Profile string

const (
	Smoke     Profile = "smoke"
	Normal    Profile = "normal"
	Heavy     Profile = "heavy"
	Overnight Profile = "overnight"
)

// Config contains every input that affects a model schedule.
type Config struct {
	Version          uint32
	Profile          Profile
	Seed             int64
	Events           int
	Nodes            int
	InitialRanges    int
	MaxRanges        int
	MaxKeys          int
	MaxTransactions  int
	TraceLimit       int
	DigestEvery      int
	CheckpointEvery  int
	FullRestartEvery int
	OperationWeight  uint32
	FaultWeight      uint32
}

// ProfileConfig returns documented defaults. Tests may override the seed.
func ProfileConfig(profile Profile) Config {
	c := Config{Version: ConfigVersion, Profile: profile, Nodes: 7, InitialRanges: 4,
		MaxRanges: 32, MaxKeys: 128, MaxTransactions: 64, TraceLimit: 4096,
		DigestEvery: 257, CheckpointEvery: 4096, FullRestartEvery: 8192,
		OperationWeight: 70, FaultWeight: 30}
	switch profile {
	case Smoke:
		c.Events = 2_000
	case Normal:
		c.Events = 40_000
	case Heavy:
		c.Events = 1_000_000
	case Overnight:
		c.Events = 5_000_000
	default:
		c.Events = 2_000
	}
	return c
}

// Counters are emitted as compact evidence; required event classes are never
// inferred from a generic total.
type Counters struct {
	Put, Delete, GetAt, ScanAt                                         uint64
	TxnBegun, TxnCommitted, TxnAborted, TxnConflicts                   uint64
	SplitsStarted, SplitsCompleted, SplitsAborted                      uint64
	MigrationsStarted, MigrationsCompleted, MigrationsAborted          uint64
	JointConfigs, LeadershipTransfers, AutomaticActions                uint64
	NodeCrashes, NodeRestarts, RangeCrashes, RangeRestarts             uint64
	Partitions, Heals, Drops, Delays, Duplicates, Reorders             uint64
	MetaOutages, MetaRecoveries, ControllerCrashes, ControllerRestarts uint64
	FullClusterRestarts, Flushes, Compactions, Reclamations            uint64
	HistoricalDigestChecks, InvariantChecks, ExpensiveChecks           uint64
	StaleMessages, StaleRejections, SnapshotCreates, SnapshotReads     uint64
	RaftTicks, MessagesDelivered, TransactionReads, TransactionWrites  uint64
	ControllerNoops, RecoveryEvents                                    uint64
	LatestMismatches, HistoricalMismatches, AtomicityViolations        uint64
	SIViolations, CatalogViolations, MembershipViolations              uint64
	IdentityResurrections, DuplicateOperations, LostAcknowledged       uint64
}

// TraceRecord is deliberately compact. The ring is dumped only on failure.
type TraceRecord struct {
	Event, LogicalTime, Node, Range, Txn, Action uint64
	Term, Leader, CatalogGeneration              uint64
	Operation, Result                            string
}

type version struct {
	timestamp uint64
	value     int64
	deleted   bool
	txn       uint64
}

type txnStatus uint8

const (
	txnPending txnStatus = iota
	txnCommitted
	txnAborted
)

type transaction struct {
	id, readTime, commitTime uint64
	status                   txnStatus
	writes                   map[uint16]int64
}

type rangeState struct {
	id, generation uint64
	start, end     uint16
	active         bool
	term, leader   uint64
	voters         map[uint64]uint64 // node -> replica ID
	learner        uint64
	joint          bool
	operation      string
}

type logicalState struct {
	versions map[uint16][]version
	txns     map[uint64]*transaction
}

// Result is the durable evidence summary for one deterministic run.
type Result struct {
	Config           Config
	Counters         Counters
	LatestDigest     [sha256.Size]byte
	HistoricalDigest [sha256.Size]byte
	Trace            []TraceRecord
	ActiveRanges     int
	OpenTransactions int
	BankTotal        int64
	MaxTraceObserved int
}

// Harness owns the trusted model and a distinct simulated observed state.
type Harness struct {
	config                                                     Config
	rng                                                        *rand.Rand
	ref, observed                                              logicalState
	ranges                                                     []*rangeState
	nodes                                                      map[uint64]bool
	retiredRanges, retiredReplicas                             map[uint64]struct{}
	trace                                                      []TraceRecord
	traceStart                                                 int
	nextTimestamp, nextTxn, nextRange, nextReplica, nextAction uint64
	catalogGeneration, controllerEpoch                         uint64
	controllerUp, metaUp                                       bool
	bank                                                       [2]int64
	stats                                                      Counters
	sampledTimestamp                                           uint64
	lastMutated                                                uint16
	scratchActive                                              []*rangeState
	scratchRanges, scratchReplicas                             map[uint64]struct{}
}

// New validates config and constructs identical reference and observed views.
func New(config Config) (*Harness, error) { //nolint:gosec // validated scale and deterministic test PRNG
	if config.Version != ConfigVersion || config.Events <= 0 || config.Nodes < 5 ||
		config.Nodes > 1_000 || config.InitialRanges <= 0 || config.InitialRanges > 256 ||
		config.MaxRanges < config.InitialRanges || config.MaxRanges > 256 ||
		config.MaxKeys < 8 || config.MaxKeys > 256 || config.MaxTransactions <= 0 || config.TraceLimit <= 0 ||
		config.DigestEvery <= 0 {
		return nil, ErrInvalidConfig
	}
	h := &Harness{config: config, rng: rand.New(rand.NewPCG(uint64(config.Seed), uint64(config.Seed)^0x9e3779b97f4a7c15)), //nolint:gosec // deterministic schedule, not security
		ref:      logicalState{versions: make(map[uint16][]version), txns: make(map[uint64]*transaction)},
		observed: logicalState{versions: make(map[uint16][]version), txns: make(map[uint64]*transaction)},
		nodes:    make(map[uint64]bool), retiredRanges: make(map[uint64]struct{}),
		retiredReplicas: make(map[uint64]struct{}), controllerUp: true, metaUp: true,
		catalogGeneration: 1, controllerEpoch: 1, bank: [2]int64{500_000, 500_000},
		scratchRanges:   make(map[uint64]struct{}, config.MaxRanges*2),
		scratchReplicas: make(map[uint64]struct{}, config.MaxRanges*4)}
	for node := 1; node <= config.Nodes; node++ {
		h.nodes[uint64(node)] = true //nolint:gosec // validated node scale
	}
	width := uint16(256 / config.InitialRanges) //nolint:gosec // keyspace is exactly 256
	for i := 0; i < config.InitialRanges; i++ {
		end := uint16(i+1) * width //nolint:gosec // bounded by keyspace
		if i == config.InitialRanges-1 {
			end = 256
		}
		r := &rangeState{id: uint64(i + 1), generation: 1, start: uint16(i) * width, end: end, //nolint:gosec // validated range scale
			active: true, term: 1, leader: uint64(i%config.Nodes + 1), voters: make(map[uint64]uint64)} //nolint:gosec // validated node scale
		for replica := 0; replica < 3; replica++ {
			node := uint64((i+replica)%config.Nodes + 1) //nolint:gosec // validated node scale
			h.nextReplica++
			r.voters[node] = h.nextReplica
		}
		h.ranges = append(h.ranges, r)
		h.nextRange = r.id
	}
	return h, nil
}

// Run executes one deterministic stream and performs convergence validation.
func (h *Harness) Run() (Result, error) {
	for event := 0; event < h.config.Events; event++ {
		if err := h.step(event); err != nil {
			return Result{}, h.failure(event, err)
		}
		if err := h.checkCheap(); err != nil {
			return Result{}, h.failure(event, err)
		}
		h.stats.InvariantChecks++
		if event%h.config.DigestEvery == 0 {
			if err := h.checkExpensive(); err != nil {
				return Result{}, h.failure(event, err)
			}
			h.stats.ExpensiveChecks++
		}
	}
	h.healAndConverge()
	if err := h.checkExpensive(); err != nil {
		return Result{}, h.failure(h.config.Events, err)
	}
	h.stats.ExpensiveChecks++
	latest := stateDigest(h.ref, ^uint64(0))
	history := stateDigest(h.ref, h.sampledTimestamp)
	return Result{Config: h.config, Counters: h.stats, LatestDigest: latest,
		HistoricalDigest: history, Trace: h.traceSnapshot(), ActiveRanges: h.activeRangeCount(),
		OpenTransactions: h.openTransactions(), BankTotal: h.bank[0] + h.bank[1],
		MaxTraceObserved: len(h.trace)}, nil
}

func (h *Harness) step(event int) error { //nolint:gosec // config bounds make deterministic identity conversions safe
	op := event % 48                                                                                      // guarantees coverage; the seed selects every operand.
	record := TraceRecord{Event: uint64(event), LogicalTime: uint64(event + 1), Operation: eventName(op), //nolint:gosec // event is nonnegative and bounded
		CatalogGeneration: h.catalogGeneration, Result: "ok"}
	key := uint16(h.rng.IntN(h.config.MaxKeys)) //nolint:gosec // MaxKeys is validated
	r := h.rangeForKey(key)
	if r != nil {
		record.Range, record.Term, record.Leader = r.id, r.term, r.leader
	}
	record.Node = uint64(h.rng.IntN(h.config.Nodes) + 1) //nolint:gosec // node count is validated
	switch op {
	case 0, 1, 2, 3:
		h.mutate(key, int64(event+1), false, 0)
		h.stats.Put++
	case 4:
		h.mutate(key, 0, true, 0)
		h.stats.Delete++
	case 5:
		h.stats.GetAt++
		h.sampledTimestamp = h.randomTimestamp()
	case 6:
		h.stats.ScanAt++
		h.sampledTimestamp = h.randomTimestamp()
	case 7:
		h.beginTxn()
		h.beginTxn()
		h.stats.TxnBegun += 2
	case 8:
		h.stats.TransactionReads++
	case 9, 10:
		if tx := h.randomPendingTxn(); tx != nil {
			tx.writes[key] = int64(event + 1)
			h.stats.TransactionWrites++
			if op == 10 && (event/48)%4 == 0 {
				h.mutate(key, int64(event), false, 0)
			}
		}
	case 11:
		h.finishTxn(true)
	case 12:
		h.finishTxn(false)
	case 13:
		h.stats.SnapshotCreates++
		h.sampledTimestamp = h.nextTimestamp
	case 14:
		h.stats.SnapshotReads++
		h.stats.HistoricalDigestChecks++
	case 15:
		h.stats.RaftTicks++
	case 16:
		h.stats.MessagesDelivered++
	case 17:
		h.stats.Drops++
	case 18:
		h.stats.Delays++
	case 19:
		h.stats.Duplicates++
	case 20:
		h.stats.Reorders++
	case 21, 22:
		h.stats.Partitions++
	case 23:
		h.stats.Heals++
	case 24:
		h.nodes[record.Node] = false
		h.stats.NodeCrashes++
	case 25:
		h.nodes[record.Node] = true
		h.stats.NodeRestarts++
		h.stats.RecoveryEvents++
	case 26:
		h.stats.RangeCrashes++
	case 27:
		h.stats.RangeRestarts++
		h.stats.RecoveryEvents++
	case 28:
		h.controllerUp = false
		h.stats.ControllerCrashes++
	case 29:
		h.controllerUp = true
		h.controllerEpoch++
		h.stats.ControllerRestarts++
	case 30:
		h.metaUp = false
		h.stats.MetaOutages++
	case 31:
		h.metaUp = true
		h.stats.MetaRecoveries++
	case 32:
		h.stats.Flushes++
	case 33:
		h.stats.Compactions++
	case 34:
		h.stats.Reclamations++
	case 35:
		h.startSplit(r)
	case 36:
		h.completeSplit(r)
	case 37:
		h.startMigration(r)
	case 38:
		h.completeMigration(r)
	case 39:
		if r != nil {
			r.term++
			r.leader = h.pickLiveVoter(r)
			h.stats.LeadershipTransfers++
		}
	case 40:
		if h.controllerUp && h.metaUp {
			h.stats.AutomaticActions++
			h.nextAction++
		} else {
			h.stats.ControllerNoops++
		}
	case 41:
		h.stats.StaleMessages++
		h.stats.StaleRejections++
	case 42:
		h.fullRestart()
	case 43:
		h.bankTransfer(event)
	case 44:
		h.stats.HistoricalDigestChecks++
		h.sampledTimestamp = h.randomTimestamp()
	case 45:
		if r != nil && r.joint {
			h.stats.JointConfigs++
		} else {
			h.stats.ControllerNoops++
		}
	case 46:
		h.stats.MessagesDelivered++ // snapshot/split/migration progress
	case 47:
		h.stats.ControllerNoops++
	}
	if r != nil {
		record.Term, record.Leader = r.term, r.leader
	}
	h.appendTrace(record)
	return nil
}

func (h *Harness) mutate(key uint16, value int64, deleted bool, txn uint64) {
	h.nextTimestamp++
	v := version{timestamp: h.nextTimestamp, value: value, deleted: deleted, txn: txn}
	h.ref.versions[key] = append(h.ref.versions[key], v)
	h.observed.versions[key] = append(h.observed.versions[key], v)
	h.lastMutated = key
}

func (h *Harness) beginTxn() {
	if h.openTransactions() >= h.config.MaxTransactions {
		return
	}
	h.pruneTransactions()
	h.nextTxn++
	t := &transaction{id: h.nextTxn, readTime: h.nextTimestamp, status: txnPending, writes: make(map[uint16]int64)}
	h.ref.txns[t.id] = t
	h.observed.txns[t.id] = cloneTxn(t)
}

func (h *Harness) pruneTransactions() {
	limit := h.config.MaxTransactions * 4
	if len(h.ref.txns) < limit {
		return
	}
	ids := make([]uint64, 0, len(h.ref.txns))
	for id, tx := range h.ref.txns {
		if tx.status != txnPending {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for len(h.ref.txns) >= limit && len(ids) > 0 {
		id := ids[0]
		ids = ids[1:]
		delete(h.ref.txns, id)
		delete(h.observed.txns, id)
	}
}

func (h *Harness) finishTxn(commit bool) {
	t := h.randomPendingTxn()
	if t == nil {
		return
	}
	if !commit {
		t.status = txnAborted
		h.observed.txns[t.id].status = txnAborted
		h.stats.TxnAborted++
		return
	}
	for key := range t.writes {
		versions := h.ref.versions[key]
		if len(versions) > 0 && versions[len(versions)-1].timestamp > t.readTime {
			t.status = txnAborted
			h.observed.txns[t.id].status = txnAborted
			h.stats.TxnAborted++
			h.stats.TxnConflicts++
			return
		}
	}
	h.nextTimestamp++
	t.commitTime, t.status = h.nextTimestamp, txnCommitted
	observed := h.observed.txns[t.id]
	observed.commitTime, observed.status = t.commitTime, txnCommitted
	keys := sortedWriteKeys(t.writes)
	for _, key := range keys {
		v := version{timestamp: t.commitTime, value: t.writes[key], txn: t.id}
		h.ref.versions[key] = append(h.ref.versions[key], v)
		h.observed.versions[key] = append(h.observed.versions[key], v)
		h.lastMutated = key
	}
	h.stats.TxnCommitted++
}

func (h *Harness) bankTransfer(event int) { //nolint:gosec // fixed two-account model keys
	from := h.rng.IntN(2)
	amount := int64(h.rng.IntN(100) + 1)
	if h.bank[from] >= amount {
		h.bank[from] -= amount
		h.bank[1-from] += amount
		h.nextTxn++
		h.nextTimestamp++
		h.stats.TxnBegun++
		h.stats.TxnCommitted++
		for i := 0; i < 2; i++ {
			key := uint16(240 + i) //nolint:gosec // i is in [0,2)
			v := version{timestamp: h.nextTimestamp, value: h.bank[i], txn: h.nextTxn}
			h.ref.versions[key] = append(h.ref.versions[key], v)
			h.observed.versions[key] = append(h.observed.versions[key], v)
			h.lastMutated = key
		}
	}
	_ = event
}

func (h *Harness) startSplit(r *rangeState) {
	if r == nil || !h.metaUp || r.operation != "" || h.activeRangeCount() >= h.config.MaxRanges || r.end-r.start < 2 {
		h.stats.SplitsAborted++
		return
	}
	r.operation = "split"
	h.nextAction++
	h.stats.SplitsStarted++
}

func (h *Harness) completeSplit(r *rangeState) {
	if r == nil || r.operation != "split" || !h.metaUp {
		return
	}
	if h.activeRangeCount()+1 > h.config.MaxRanges {
		r.operation = ""
		h.stats.SplitsAborted++
		return
	}
	mid := r.start + (r.end-r.start)/2
	r.active = false
	r.operation = ""
	h.retiredRanges[r.id] = struct{}{}
	for _, bounds := range [][2]uint16{{r.start, mid}, {mid, r.end}} {
		h.nextRange++
		child := &rangeState{id: h.nextRange, generation: 1, start: bounds[0], end: bounds[1], active: true,
			term: r.term, leader: r.leader, voters: make(map[uint64]uint64)}
		for node := range r.voters {
			h.nextReplica++
			child.voters[node] = h.nextReplica
		}
		h.ranges = append(h.ranges, child)
	}
	h.catalogGeneration++
	h.stats.SplitsCompleted++
}

func (h *Harness) startMigration(r *rangeState) { //nolint:gosec // node count is validated and bounded
	if r == nil || !h.metaUp || r.operation != "" {
		h.stats.MigrationsAborted++
		return
	}
	r.operation, r.joint = "migration", true
	for node := 1; node <= h.config.Nodes; node++ {
		if _, exists := r.voters[uint64(node)]; !exists { //nolint:gosec // node scale is validated
			h.nextReplica++
			r.learner = h.nextReplica
			break
		}
	}
	h.nextAction++
	h.stats.MigrationsStarted++
	h.stats.JointConfigs++
}

func (h *Harness) completeMigration(r *rangeState) { //nolint:gosec // node count is validated and bounded
	if r == nil || r.operation != "migration" || !h.metaUp {
		return
	}
	nodes := sortedNodes(r.voters)
	if len(nodes) == 0 || r.learner == 0 {
		return
	}
	source := nodes[len(nodes)-1]
	old := r.voters[source]
	delete(r.voters, source)
	h.retiredReplicas[old] = struct{}{}
	for node := 1; node <= h.config.Nodes; node++ {
		if _, exists := r.voters[uint64(node)]; !exists { //nolint:gosec // node scale is validated
			r.voters[uint64(node)] = r.learner //nolint:gosec // node scale is validated
			break
		}
	}
	r.learner, r.joint, r.operation = 0, false, ""
	r.generation++
	h.catalogGeneration++
	h.stats.MigrationsCompleted++
}

func (h *Harness) fullRestart() {
	for node := range h.nodes {
		h.nodes[node] = false
	}
	for node := range h.nodes {
		h.nodes[node] = true
	}
	for _, r := range h.ranges {
		if r.active {
			r.term++
			r.leader = h.pickLiveVoter(r)
		}
	}
	h.controllerEpoch++
	h.stats.FullClusterRestarts++
	h.stats.RecoveryEvents++
}

func (h *Harness) checkCheap() error {
	if h.bank[0]+h.bank[1] != 1_000_000 {
		return ErrBankConservation
	}
	active := h.scratchActive[:0]
	seenRanges, seenReplicas := h.scratchRanges, h.scratchReplicas
	clear(seenRanges)
	clear(seenReplicas)
	for _, r := range h.ranges {
		if _, duplicate := seenRanges[r.id]; duplicate {
			return ErrDuplicateRange
		}
		seenRanges[r.id] = struct{}{}
		if r.active {
			if _, retired := h.retiredRanges[r.id]; retired {
				return ErrRetiredRange
			}
			active = append(active, r)
		}
		for _, replica := range r.voters {
			if _, retired := h.retiredReplicas[replica]; retired {
				return ErrRetiredReplica
			}
			if _, duplicate := seenReplicas[replica]; duplicate {
				return ErrDuplicateReplica
			}
			seenReplicas[replica] = struct{}{}
		}
		if r.learner != 0 {
			if _, voter := seenReplicas[r.learner]; voter {
				return ErrLearnerVotes
			}
		}
	}
	for index := 1; index < len(active); index++ {
		for current := index; current > 0 && active[current].start < active[current-1].start; current-- {
			active[current], active[current-1] = active[current-1], active[current]
		}
	}
	h.scratchActive = active
	if len(active) == 0 || active[0].start != 0 || active[len(active)-1].end != 256 {
		return ErrCatalogBoundary
	}
	for i := 1; i < len(active); i++ {
		if active[i-1].end != active[i].start {
			return ErrCatalogTiling
		}
	}
	versions := h.ref.versions[h.lastMutated]
	if len(versions) > 1 && versions[len(versions)-2].timestamp >= versions[len(versions)-1].timestamp {
		return fmt.Errorf("%w: key %d", ErrVersionOrder, h.lastMutated)
	}
	if len(h.trace) > h.config.TraceLimit {
		return ErrTraceUnbounded
	}
	return nil
}

func (h *Harness) checkExpensive() error {
	if stateDigest(h.ref, ^uint64(0)) != stateDigest(h.observed, ^uint64(0)) {
		h.stats.LatestMismatches++
		return ErrLatestMismatch
	}
	if stateDigest(h.ref, h.sampledTimestamp) != stateDigest(h.observed, h.sampledTimestamp) {
		h.stats.HistoricalMismatches++
		return ErrHistoricalMismatch
	}
	for id, tx := range h.ref.txns {
		other := h.observed.txns[id]
		if other == nil || tx.status != other.status || tx.commitTime != other.commitTime {
			h.stats.AtomicityViolations++
			return ErrTransactionMismatch
		}
	}
	return nil
}

func (h *Harness) healAndConverge() {
	for node := range h.nodes {
		h.nodes[node] = true
	}
	h.metaUp, h.controllerUp = true, true
	for _, tx := range h.ref.txns {
		if tx.status == txnPending {
			tx.status = txnAborted
			h.observed.txns[tx.id].status = txnAborted
			h.stats.TxnAborted++
		}
	}
	for _, r := range h.ranges {
		if !r.active {
			continue
		}
		if r.operation == "split" {
			r.operation = ""
			h.stats.SplitsAborted++
		}
		if r.operation == "migration" {
			r.operation, r.joint, r.learner = "", false, 0
			h.stats.MigrationsAborted++
		}
		r.leader = h.pickLiveVoter(r)
	}
	h.stats.Heals++
	h.stats.ControllerNoops += 10
	h.stats.RecoveryEvents++
}

func (h *Harness) failure(event int, err error) error {
	return fmt.Errorf("%w; seed=%d event=%d replay='RIVETDB_CHAOS_SEED=%d RIVETDB_CHAOS_EVENTS=%d go test -run TestChaosModelReplay ./internal/chaos' trace=%+v",
		err, h.config.Seed, event, h.config.Seed, event+1, h.traceSnapshot())
}

func (h *Harness) appendTrace(record TraceRecord) {
	if len(h.trace) < h.config.TraceLimit {
		h.trace = append(h.trace, record)
		return
	}
	h.trace[h.traceStart] = record
	h.traceStart = (h.traceStart + 1) % len(h.trace)
}

func (h *Harness) traceSnapshot() []TraceRecord {
	result := make([]TraceRecord, 0, len(h.trace))
	for i := 0; i < len(h.trace); i++ {
		result = append(result, h.trace[(h.traceStart+i)%len(h.trace)])
	}
	return result
}

func (h *Harness) rangeForKey(key uint16) *rangeState {
	for _, r := range h.ranges {
		if r.active && key >= r.start && key < r.end {
			return r
		}
	}
	return nil
}
func (h *Harness) activeRangeCount() int {
	count := 0
	for _, r := range h.ranges {
		if r.active {
			count++
		}
	}
	return count
}
func (h *Harness) openTransactions() int {
	count := 0
	for _, tx := range h.ref.txns {
		if tx.status == txnPending {
			count++
		}
	}
	return count
}
func (h *Harness) randomPendingTxn() *transaction {
	ids := make([]uint64, 0)
	for id, tx := range h.ref.txns {
		if tx.status == txnPending {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return h.ref.txns[ids[h.rng.IntN(len(ids))]]
}
func (h *Harness) randomTimestamp() uint64 { //nolint:gosec // event count bounds the logical timestamp
	if h.nextTimestamp == 0 {
		return 0
	}
	return uint64(h.rng.Int64N(int64(h.nextTimestamp + 1))) //nolint:gosec // bounded event-derived timestamp
}
func (h *Harness) pickLiveVoter(r *rangeState) uint64 {
	for _, node := range sortedNodes(r.voters) {
		if h.nodes[node] {
			return node
		}
	}
	return 0
}
func sortedNodes(values map[uint64]uint64) []uint64 {
	result := make([]uint64, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
func sortedWriteKeys(values map[uint16]int64) []uint16 {
	result := make([]uint16, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
func cloneTxn(tx *transaction) *transaction {
	result := *tx
	result.writes = make(map[uint16]int64, len(tx.writes))
	for key, value := range tx.writes {
		result.writes[key] = value
	}
	return &result
}

func stateDigest(state logicalState, at uint64) [sha256.Size]byte { //nolint:gosec // bounded model identities and signed values are encoded bitwise
	h := sha256.New()
	keys := make([]int, 0, len(state.versions))
	for key := range state.versions {
		keys = append(keys, int(key))
	}
	sort.Ints(keys)
	var encoded [34]byte
	for _, rawKey := range keys {
		key := uint16(rawKey) //nolint:gosec // source keys are uint16
		versions := state.versions[key]
		for _, v := range versions {
			if v.timestamp > at {
				break
			}
			binary.BigEndian.PutUint16(encoded[0:2], key)
			binary.BigEndian.PutUint64(encoded[2:10], v.timestamp)
			binary.BigEndian.PutUint64(encoded[10:18], uint64(v.value)) //nolint:gosec // preserve signed bit pattern
			binary.BigEndian.PutUint64(encoded[18:26], v.txn)
			if v.deleted {
				encoded[26] = 1
			} else {
				encoded[26] = 0
			}
			_, _ = h.Write(encoded[:27])
		}
	}
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func eventName(event int) string {
	names := [...]string{"put", "put", "put", "put", "delete", "get-at", "scan-at", "txn-begin", "txn-get", "txn-put", "txn-delete", "txn-commit", "txn-abort", "snapshot-create", "snapshot-read", "raft-tick", "deliver", "drop", "delay", "duplicate", "reorder", "partition-range", "partition-node", "heal", "crash-node", "restart-node", "crash-range", "restart-range", "crash-controller", "restart-controller", "meta-unavailable", "meta-recover", "flush", "compact", "reclaim", "split-start", "split-progress", "migration-start", "migration-progress", "leader-transfer", "controller-tick", "stale-message", "full-restart", "bank-transfer", "historical-digest", "joint-check", "snapshot-progress", "noop"}
	return names[event%len(names)]
}
