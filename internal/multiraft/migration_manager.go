package multiraft

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/rivetdb/rivetdb/internal/raft"
)

type MigrationHookStage uint8

const (
	MigrationRecordDurable MigrationHookStage = iota
	TargetDirectoryCreated
	SnapshotExportStarted
	SnapshotTransferPartial
	SnapshotTargetDurable
	LearnerAdded
	CatchupProgress
	PromotionBarrierCommitted
	TargetReady
	JointConfigAppended
	JointConfigCommitted
	LeadershipTransferStarted
	LeadershipTransferComplete
	FinalConfigAppended
	FinalConfigCommitted
	MetaPlacementCommitted
	SourceRetired
	SourceDeleteStarted
	SourceDeleteComplete
)

func (s MigrationHookStage) String() string {
	names := []string{"MIGRATION_RECORD_DURABLE", "TARGET_DIRECTORY_CREATED", "SNAPSHOT_EXPORT_STARTED", "SNAPSHOT_TRANSFER_PARTIAL", "SNAPSHOT_TARGET_DURABLE", "LEARNER_ADDED", "CATCHUP_PROGRESS", "PROMOTION_BARRIER_COMMITTED", "TARGET_READY", "JOINT_CONFIG_APPENDED", "JOINT_CONFIG_COMMITTED", "LEADERSHIP_TRANSFER_STARTED", "LEADERSHIP_TRANSFER_COMPLETE", "FINAL_CONFIG_APPENDED", "FINAL_CONFIG_COMMITTED", "META_PLACEMENT_COMMITTED", "SOURCE_RETIRED", "SOURCE_DELETE_STARTED", "SOURCE_DELETE_COMPLETE"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("MIGRATION_STAGE_%d", s)
}

type MigrationHook func(MigrationHookStage, MigrationRecord)
type MigrationManagerOptions struct {
	Meta   *MetaRange
	Router *Router
	Hook   MigrationHook
}
type MigrationManager struct {
	meta   *MetaRange
	router *Router
	hook   MigrationHook
}

type MigrationStatus struct {
	MigrationID                                                                                 MigrationID
	RangeID                                                                                     RangeID
	SourceReplica                                                                               ReplicaID
	SourceNode                                                                                  raft.NodeID
	TargetReplica                                                                               ReplicaID
	TargetNode                                                                                  raft.NodeID
	State                                                                                       MigrationState
	Epoch, SnapshotIndex, LearnerMatchIndex, PromotionBarrier, ConfigVersion, CatalogGeneration uint64
	CurrentLeader                                                                               raft.NodeID
	LastError                                                                                   string
}
type PlacementView struct {
	Published     RangeDescriptor
	CurrentVoters []raft.NodeID
	Learners      []raft.NodeID
	DesiredTarget ReplicaDescriptor
}

func (m *MigrationManager) Status(id MigrationID) (MigrationStatus, PlacementView, error) {
	var record MigrationRecord
	for _, candidate := range m.meta.Snapshot().Migrations {
		if candidate.MigrationID == id {
			record = candidate
			break
		}
	}
	if record.MigrationID == 0 {
		return MigrationStatus{}, PlacementView{}, ErrRangeNotFound
	}
	published, err := m.meta.Snapshot().Catalog.LookupByID(record.RangeID)
	if err != nil {
		return MigrationStatus{}, PlacementView{}, err
	}
	status := MigrationStatus{MigrationID: id, RangeID: record.RangeID, SourceReplica: record.SourceReplicaID, SourceNode: record.SourceNodeID, TargetReplica: record.TargetReplicaID, TargetNode: record.TargetNodeID, State: record.State, Epoch: record.Epoch, SnapshotIndex: record.BootstrapIndex, PromotionBarrier: record.PromotionBarrier, ConfigVersion: record.ConfigVersion, CatalogGeneration: record.CatalogGeneration, LastError: record.LastError}
	view := PlacementView{Published: published, DesiredTarget: ReplicaDescriptor{ReplicaID: record.TargetReplicaID, NodeID: record.TargetNodeID}}
	for nodeID, node := range m.router.scheduler.Nodes() {
		replica, e := node.Replica(record.RangeID)
		if e != nil {
			continue
		}
		raftStatus := replica.Status().Raft
		if raftStatus.Role == raft.Leader {
			status.CurrentLeader = nodeID
			status.LearnerMatchIndex = raftStatus.MatchIndex[record.TargetNodeID]
		}
		if len(view.CurrentVoters) == 0 && raftStatus.Config.Version != 0 {
			view.CurrentVoters = append([]raft.NodeID(nil), raftStatus.Config.OldVoters...)
			if raftStatus.Config.Joint() {
				view.CurrentVoters = append(view.CurrentVoters, raftStatus.Config.NewVoters...)
			}
			view.Learners = append([]raft.NodeID(nil), raftStatus.Config.Learners...)
		}
	}
	return status, view, nil
}

func NewMigrationManager(o MigrationManagerOptions) (*MigrationManager, error) {
	if o.Meta == nil || o.Router == nil {
		return nil, ErrInvalidCatalog
	}
	o.Router.AttachMetaRange(o.Meta)
	return &MigrationManager{meta: o.Meta, router: o.Router, hook: o.Hook}, nil
}
func (m *MigrationManager) observe(s MigrationHookStage, r MigrationRecord) {
	if m.hook != nil {
		m.hook(s, r)
	}
}

func (m *MigrationManager) MoveReplica(ctx context.Context, rangeID RangeID, source ReplicaID, target raft.NodeID) (MigrationRecord, error) {
	if ctx == nil {
		return MigrationRecord{}, ErrInvalidCatalog
	}
	catalog := m.meta.Snapshot().Catalog
	descriptor, err := catalog.LookupByID(rangeID)
	if err != nil {
		return MigrationRecord{}, err
	}
	record, err := m.meta.BeginMigration(ctx, rangeID, source, target, descriptor.Generation, catalog.Generation())
	if err != nil {
		return MigrationRecord{}, err
	}
	m.observe(MigrationRecordDurable, record)
	return m.run(ctx, record)
}

func (m *MigrationManager) RecoverMigration(ctx context.Context, id MigrationID) (MigrationRecord, error) {
	var record MigrationRecord
	for _, r := range m.meta.Snapshot().Migrations {
		if r.MigrationID == id {
			record = r
		}
	}
	if record.MigrationID == 0 {
		return record, ErrRangeNotFound
	}
	if record.State == MigrationSourceRetired {
		return record, m.reconcileRetired(ctx, record)
	}
	if record.State == MigrationAborted {
		return record, nil
	}
	next, err := m.meta.TakeoverMigration(ctx, id, record.Epoch)
	if err != nil {
		return record, err
	}
	return m.run(ctx, next)
}

func (m *MigrationManager) reconcileRetired(ctx context.Context, r MigrationRecord) error {
	catalog := m.meta.Snapshot().Catalog
	published, err := catalog.LookupByID(r.RangeID)
	if err != nil {
		return err
	}
	nodes := m.router.scheduler.Nodes()
	for _, node := range nodes {
		if err := node.InstallDynamicCatalog(catalog); err != nil {
			return err
		}
	}
	if err := m.router.InstallCatalog(catalog); err != nil {
		return err
	}
	if _, err := nodes[r.TargetNodeID].Replica(r.RangeID); err != nil {
		final := raft.Configuration{Version: r.ConfigVersion}
		for _, member := range published.Replicas {
			final.OldVoters = append(final.OldVoters, member.NodeID)
		}
		if err := nodes[r.TargetNodeID].AddLearnerRange(ctx, published, final, r.TargetReplicaID); err != nil {
			return err
		}
		m.router.scheduler.Refresh()
	}
	target, targetErr := nodes[r.TargetNodeID].Replica(r.RangeID)
	if targetErr != nil {
		return targetErr
	}
	target.ActivateMigration()
	if source, e := nodes[r.SourceNodeID].Replica(r.RangeID); e == nil && source.Status().ReplicaID == r.SourceReplicaID {
		if e := nodes[r.SourceNodeID].RetireReplica(ctx, r.RangeID, r.SourceReplicaID); e != nil {
			return e
		}
	}
	return nil
}

//nolint:contextcheck,govet // forward-only state machine calls contextless open/digest APIs and retains one outer error
func (m *MigrationManager) run(ctx context.Context, r MigrationRecord) (MigrationRecord, error) {
	descriptor, err := m.currentDescriptor(r.RangeID)
	if err != nil {
		return r, err
	}
	transition := cloneDescriptor(descriptor)
	transition.Replicas = append(transition.Replicas, ReplicaDescriptor{ReplicaID: r.TargetReplicaID, NodeID: r.TargetNodeID})
	nodes := m.router.scheduler.Nodes()
	for _, node := range nodes {
		node.InstallTransitionDescriptor(transition)
	}
	old := make([]raft.NodeID, 0, len(descriptor.Replicas))
	for _, rep := range descriptor.Replicas {
		old = append(old, rep.NodeID)
	}
	if r.State == MigrationSourceRemoving {
		final := raft.Configuration{Version: r.ConfigVersion}
		for _, id := range old {
			if id != r.SourceNodeID {
				final.OldVoters = append(final.OldVoters, id)
			}
		}
		final.OldVoters = append(final.OldVoters, r.TargetNodeID)
		if _, openErr := nodes[r.TargetNodeID].Replica(r.RangeID); openErr != nil {
			if err := nodes[r.TargetNodeID].AddLearnerRange(ctx, transition, final, r.TargetReplicaID); err != nil {
				return r, err
			}
			m.router.scheduler.Refresh()
		}
	}
	learnerConfig := raft.Configuration{Version: 1, OldVoters: old}
	leader, err := m.leader(ctx, r.RangeID)
	if err != nil {
		return r, err
	}
	leaderReplica, replicaErr := nodes[leader].Replica(r.RangeID)
	if replicaErr != nil {
		return r, replicaErr
	}
	base := leaderReplica.Status().Raft.Config
	baseVersion := base.Version
	switch {
	case r.State >= MigrationSourceRemoving:
		baseVersion = r.ConfigVersion - 2
	case r.State >= MigrationJoint:
		baseVersion = r.ConfigVersion - 1
	case r.State >= MigrationCatchingUp:
		baseVersion = r.ConfigVersion
	}
	learnerConfig = raft.Configuration{Version: baseVersion, OldVoters: old, Learners: []raft.NodeID{r.TargetNodeID}}
	if r.State < MigrationCatchingUp {
		learnerConfig.Version = base.Version + 1
		if base.Learner(r.TargetNodeID) {
			learnerConfig = base
		}
	}
	baseConfiguration := raft.Configuration{Version: learnerConfig.Version - 1, OldVoters: old}
	if _, openErr := nodes[r.TargetNodeID].Replica(r.RangeID); openErr != nil && r.State > MigrationBootstrapping && r.State < MigrationCommitted {
		if err := nodes[r.TargetNodeID].AddLearnerRange(ctx, transition, baseConfiguration, r.TargetReplicaID); err != nil {
			return r, err
		}
		m.router.scheduler.Refresh()
	}
	if r.State == MigrationPlanned {
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationBootstrapping, MigrationRecord{})
		if err != nil {
			return r, err
		}
	}
	if r.State == MigrationBootstrapping {
		if err := nodes[r.TargetNodeID].AddLearnerRange(ctx, transition, baseConfiguration, r.TargetReplicaID); err != nil {
			return r, err
		}
		m.router.scheduler.Refresh()
		m.observe(TargetDirectoryCreated, r)
		m.observe(SnapshotExportStarted, r)
		snapshot, err := leaderReplica.CreateRaftSnapshot()
		if err != nil {
			return r, fmt.Errorf("create migration Raft snapshot: %w", err)
		}
		header := migrationSnapshotHeaderFor(r, snapshot)
		staging := filepath.Join(nodes[r.TargetNodeID].directory, "ranges", fmt.Sprint(uint64(r.RangeID)), fmt.Sprintf("replica-%d", r.TargetReplicaID), "staging")
		for offset := 0; offset < len(snapshot.Data); offset += migrationSnapshotChunkBytes {
			end := min(offset+migrationSnapshotChunkBytes, len(snapshot.Data))
			chunk := snapshot.Data[offset:end]
			if err := stageSnapshotChunk(staging, header, uint64(offset), chunk, crc32.Checksum(chunk, migrationSnapshotCRC)); err != nil { //nolint:gosec // offset is bounded by snapshot slice length
				if offset == 0 && errors.Is(err, ErrMigrationConflict) {
					partial := filepath.Join(staging, fmt.Sprintf("migration-%d.snapshot.partial", r.MigrationID))
					if removeErr := os.Remove(partial); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
						return r, fmt.Errorf("discard abandoned migration snapshot: %w", removeErr)
					}
					if retryErr := stageSnapshotChunk(staging, header, 0, chunk, crc32.Checksum(chunk, migrationSnapshotCRC)); retryErr != nil {
						return r, retryErr
					}
				} else {
					return r, err
				}
			}
			if offset == 0 {
				m.observe(SnapshotTransferPartial, r)
			}
		}
		if err := finalizeStagedSnapshot(staging, header); err != nil {
			return r, err
		}
		digest := sha256.Sum256(snapshot.Data)
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationLearner, MigrationRecord{BootstrapIndex: snapshot.Index, StateDigest: digest})
		if err != nil {
			return r, err
		}
	}
	if r.State == MigrationLearner {
		if err := m.proposeConfig(ctx, r.RangeID, learnerConfig); err != nil && !errors.Is(err, raft.ErrConfigTransition) {
			return r, err
		}
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationCatchingUp, MigrationRecord{ConfigVersion: learnerConfig.Version})
		if err != nil {
			return r, err
		}
		m.observe(LearnerAdded, r)
	}
	if r.State == MigrationCatchingUp {
		if err := m.waitCaughtUp(ctx, r.RangeID, r.TargetNodeID, r.BootstrapIndex); err != nil {
			return r, err
		}
		m.observe(SnapshotTargetDurable, r)
		m.observe(CatchupProgress, r)
		barrier, err := m.proposeBarrier(ctx, r.RangeID)
		if err != nil {
			return r, err
		}
		if err := m.waitCaughtUp(ctx, r.RangeID, r.TargetNodeID, barrier); err != nil {
			return r, err
		}
		m.observe(PromotionBarrierCommitted, r)
		sourceDigest, err := m.rangeDigest(r.RangeID, leader)
		if err != nil {
			return r, err
		}
		targetDigest, err := m.rangeDigest(r.RangeID, r.TargetNodeID)
		if err != nil || sourceDigest != targetDigest {
			return r, errors.Join(ErrMigrationConflict, err)
		}
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationReady, MigrationRecord{CatchUpIndex: barrier, PromotionBarrier: barrier, StateDigest: sourceDigest})
		if err != nil {
			return r, err
		}
		m.observe(TargetReady, r)
	}
	newVoters := make([]raft.NodeID, 0, len(old))
	for _, id := range old {
		if id != r.SourceNodeID {
			newVoters = append(newVoters, id)
		}
	}
	newVoters = append(newVoters, r.TargetNodeID)
	joint := raft.Configuration{Version: learnerConfig.Version + 1, OldVoters: old, NewVoters: newVoters}
	if r.State == MigrationReady {
		m.observe(JointConfigAppended, r)
		if err := m.proposeConfig(ctx, r.RangeID, joint); err != nil {
			return r, err
		}
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationJoint, MigrationRecord{ConfigVersion: joint.Version})
		if err != nil {
			return r, err
		}
		m.observe(JointConfigCommitted, r)
	}
	if r.State == MigrationJoint {
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationPromoted, MigrationRecord{})
		if err != nil {
			return r, err
		}
	}
	if r.State == MigrationPromoted {
		leader, err = m.leader(ctx, r.RangeID)
		if err != nil {
			return r, err
		}
		if leader == r.SourceNodeID {
			var targetLeader raft.NodeID
			for _, id := range newVoters {
				if id != r.SourceNodeID {
					targetLeader = id
					break
				}
			}
			m.observe(LeadershipTransferStarted, r)
			leaderState, lookupErr := nodes[leader].Replica(r.RangeID)
			if lookupErr != nil {
				return r, lookupErr
			}
			if err := m.waitCaughtUp(ctx, r.RangeID, targetLeader, leaderState.Status().Raft.LastIndex); err != nil {
				return r, err
			}
			out, transferErr := nodes[leader].TransferRangeLeadership(r.RangeID, targetLeader)
			if transferErr != nil {
				return r, transferErr
			}
			for _, e := range out {
				if err := m.router.transport.Send(e); err != nil {
					return r, err
				}
			}
			if _, err := m.waitLeaderNot(ctx, r.RangeID, r.SourceNodeID); err != nil {
				return r, err
			}
			m.observe(LeadershipTransferComplete, r)
		}
		final := raft.Configuration{Version: joint.Version + 1, OldVoters: newVoters}
		m.observe(FinalConfigAppended, r)
		if err := m.proposeConfig(ctx, r.RangeID, final); err != nil {
			return r, err
		}
		if err := m.waitConfiguration(ctx, r.RangeID, r.TargetNodeID, final.Version); err != nil {
			return r, err
		}
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationSourceRemoving, MigrationRecord{ConfigVersion: final.Version})
		if err != nil {
			return r, err
		}
		m.observe(FinalConfigCommitted, r)
	}
	if r.State == MigrationSourceRemoving {
		r, err = m.meta.CommitMigration(ctx, r.MigrationID, r.Epoch, m.meta.Snapshot().Catalog.Generation(), r.ConfigVersion)
		if err != nil {
			return r, err
		}
		m.observe(MetaPlacementCommitted, r)
	}
	if r.State == MigrationCommitted {
		catalog := m.meta.Snapshot().Catalog
		for _, node := range nodes {
			if err := node.InstallDynamicCatalog(catalog); err != nil {
				return r, err
			}
		}
		if err := m.router.InstallCatalog(catalog); err != nil {
			return r, err
		}
		published, lookupErr := catalog.LookupByID(r.RangeID)
		if lookupErr != nil {
			return r, lookupErr
		}
		if _, openErr := nodes[r.TargetNodeID].Replica(r.RangeID); openErr != nil {
			finalConfig := raft.Configuration{Version: r.ConfigVersion, OldVoters: make([]raft.NodeID, 0, len(published.Replicas))}
			for _, member := range published.Replicas {
				finalConfig.OldVoters = append(finalConfig.OldVoters, member.NodeID)
			}
			if err := nodes[r.TargetNodeID].AddLearnerRange(ctx, published, finalConfig, r.TargetReplicaID); err != nil {
				return r, err
			}
			m.router.scheduler.Refresh()
		}
		for _, member := range published.Replicas {
			if replica, e := nodes[member.NodeID].Replica(r.RangeID); e == nil {
				if e := replica.InstallDescriptorGeneration(published.Generation); e != nil {
					return r, fmt.Errorf("install migrated descriptor generation: %w", e)
				}
			}
		}
		targetReplica, targetErr := nodes[r.TargetNodeID].Replica(r.RangeID)
		if targetErr != nil {
			return r, targetErr
		}
		targetReplica.ActivateMigration()
		if err := nodes[r.SourceNodeID].RetireReplica(ctx, r.RangeID, r.SourceReplicaID); err != nil {
			return r, err
		}
		r, err = m.meta.AdvanceMigration(ctx, r.MigrationID, r.Epoch, MigrationSourceRetired, MigrationRecord{})
		if err != nil {
			return r, err
		}
		m.observe(SourceRetired, r)
	}
	return r, nil
}

func (m *MigrationManager) currentDescriptor(id RangeID) (RangeDescriptor, error) {
	if d, e := m.meta.Snapshot().Catalog.LookupByID(id); e == nil {
		return d, nil
	}
	return RangeDescriptor{}, ErrRangeNotFound
}
func (m *MigrationManager) leader(ctx context.Context, id RangeID) (raft.NodeID, error) {
	for range m.router.maxWork {
		for nid, node := range m.router.scheduler.Nodes() {
			if rep, e := node.Replica(id); e == nil && rep.Status().Raft.Role == raft.Leader {
				return nid, nil
			}
		}
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("wait for migration leader: %w", err)
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return 0, err
		}
	}
	return 0, ErrLeaderUnknown
}
func (m *MigrationManager) waitLeaderNot(ctx context.Context, id RangeID, old raft.NodeID) (raft.NodeID, error) {
	for range m.router.maxWork {
		leader, e := m.leader(ctx, id)
		if e == nil && leader != old {
			return leader, nil
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return 0, err
		}
	}
	return 0, ErrLeaderUnknown
}
func (m *MigrationManager) proposeConfig(ctx context.Context, id RangeID, c raft.Configuration) error {
	leader, e := m.leader(ctx, id)
	if e != nil {
		return e
	}
	pending, out, e := m.router.scheduler.Nodes()[leader].ProposeConfiguration(ctx, id, c)
	if e != nil {
		return e
	}
	return m.drivePending(ctx, pending, out)
}
func (m *MigrationManager) proposeBarrier(ctx context.Context, id RangeID) (uint64, error) {
	leader, e := m.leader(ctx, id)
	if e != nil {
		return 0, e
	}
	pending, out, e := m.router.scheduler.Nodes()[leader].ProposeBarrier(ctx, id)
	if e != nil {
		return 0, e
	}
	return pending.index, m.drivePending(ctx, pending, out)
}
func (m *MigrationManager) drivePending(ctx context.Context, p *Pending, out []Envelope) error {
	for _, e := range out {
		if err := m.router.transport.Send(e); err != nil {
			return err
		}
	}
	for range m.router.maxWork {
		if done, e := p.poll(); done {
			return e
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("drive migration proposal: %w", err)
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return err
		}
	}
	return ErrLeaderUnknown
}
func (m *MigrationManager) waitCaughtUp(ctx context.Context, id RangeID, target raft.NodeID, index uint64) error {
	for range m.router.maxWork {
		node := m.router.scheduler.Nodes()[target]
		if node != nil {
			if rep, e := node.Replica(id); e == nil && rep.Status().Raft.LastApplied >= index {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for learner catch-up: %w", err)
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return err
		}
	}
	return ErrLeaderUnknown
}
func (m *MigrationManager) waitConfiguration(ctx context.Context, id RangeID, target raft.NodeID, version uint64) error {
	for range m.router.maxWork {
		node := m.router.scheduler.Nodes()[target]
		if node != nil {
			if rep, e := node.Replica(id); e == nil && rep.Status().Raft.Config.Version >= version {
				return nil
			}
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for target configuration: %w", err)
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return err
		}
	}
	return ErrLeaderUnknown
}

//nolint:contextcheck // logical digest is a locked snapshot API without a context parameter
func (m *MigrationManager) rangeDigest(id RangeID, nodeID raft.NodeID) ([32]byte, error) {
	node := m.router.scheduler.Nodes()[nodeID]
	if node == nil {
		return [32]byte{}, ErrNodeStopped
	}
	rep, e := node.Replica(id)
	if e != nil {
		return [32]byte{}, e
	}
	digest, err := rep.LogicalStateDigest()
	if err != nil {
		return [32]byte{}, fmt.Errorf("compute replica logical digest: %w", err)
	}
	return digest, nil
}
func (m *MigrationManager) DeleteSource(r MigrationRecord) error {
	if r.State != MigrationSourceRetired {
		return ErrMigrationConflict
	}
	published, err := m.meta.Snapshot().Catalog.LookupByID(r.RangeID)
	if err != nil {
		return err
	}
	if local, ok := published.ReplicaOn(r.SourceNodeID); ok {
		// A later migration may legitimately place a new incarnation on the old
		// node. Never let cleanup for the retired identity touch that replica.
		if local.ReplicaID != r.SourceReplicaID {
			return ErrMigrationConflict
		}
		return ErrMigrationConflict
	}
	m.observe(SourceDeleteStarted, r)
	node := m.router.scheduler.Nodes()[r.SourceNodeID]
	if node == nil {
		return ErrNodeStopped
	}
	if err := node.DeleteRetiredReplica(r.RangeID, r.SourceReplicaID); err != nil {
		return err
	}
	m.observe(SourceDeleteComplete, r)
	return nil
}
