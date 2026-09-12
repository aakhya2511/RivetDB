package multiraft

import (
	"fmt"
	"slices"

	"github.com/rivetdb/rivetdb/internal/raft"
)

func legalMigrationTransition(from, to MigrationState) bool {
	if to == MigrationAborted {
		return from >= MigrationPlanned && from < MigrationJoint
	}
	return to == from+1 && from >= MigrationPlanned && from < MigrationSourceRetired
}

func (s *metadataState) beginMigration(rangeID RangeID, source ReplicaID, target raft.NodeID, generation, expectedCatalog uint64) (MigrationRecord, error) {
	if s.catalog.Generation() != expectedCatalog {
		return MigrationRecord{}, ErrStaleRange
	}
	descriptor, err := s.catalog.LookupByID(rangeID)
	if err != nil || descriptor.Generation != generation {
		return MigrationRecord{}, ErrStaleRange
	}
	var sourceDescriptor ReplicaDescriptor
	for _, r := range descriptor.Replicas {
		if r.ReplicaID == source {
			sourceDescriptor = r
		}
		if r.NodeID == target {
			return MigrationRecord{}, ErrInvalidDescriptor
		}
	}
	if sourceDescriptor.ReplicaID == 0 || target == 0 || !slices.Contains(s.catalog.Snapshot().Nodes, target) {
		return MigrationRecord{}, ErrInvalidDescriptor
	}
	for _, split := range s.splits {
		if split.Parent.RangeID == rangeID && split.State != SplitCommitted && split.State != SplitAborted {
			return MigrationRecord{}, ErrSplitInProgress
		}
	}
	for _, existing := range s.migrations {
		if existing.RangeID == rangeID && existing.State != MigrationAborted && existing.State != MigrationSourceRetired {
			if existing.SourceReplicaID == source && existing.TargetNodeID == target {
				return existing, nil
			}
			return MigrationRecord{}, ErrMigrationInProgress
		}
	}
	if s.nextReplicaID == 0 || s.nextMigrationID == 0 {
		return MigrationRecord{}, ErrResourceLimit
	}
	record := MigrationRecord{MigrationID: s.nextMigrationID, RangeID: rangeID, RangeGeneration: generation, SourceReplicaID: source, SourceNodeID: sourceDescriptor.NodeID, TargetReplicaID: s.nextReplicaID, TargetNodeID: target, Epoch: 1, State: MigrationPlanned, CatalogGeneration: expectedCatalog}
	s.nextMigrationID++
	s.nextReplicaID++
	s.migrations[record.MigrationID] = record
	return record, nil
}

func (s *metadataState) advanceMigration(id MigrationID, epoch uint64, to MigrationState, update MigrationRecord) (MigrationRecord, error) {
	r, ok := s.migrations[id]
	if !ok || r.Epoch != epoch {
		return MigrationRecord{}, ErrMigrationConflict
	}
	if r.State == to {
		return r, nil
	}
	if !legalMigrationTransition(r.State, to) {
		return MigrationRecord{}, ErrMigrationConflict
	}
	r.State = to
	r.BootstrapIndex = max(r.BootstrapIndex, update.BootstrapIndex)
	r.CatchUpIndex = max(r.CatchUpIndex, update.CatchUpIndex)
	r.PromotionBarrier = max(r.PromotionBarrier, update.PromotionBarrier)
	r.ConfigVersion = max(r.ConfigVersion, update.ConfigVersion)
	if update.StateDigest != [32]byte{} {
		r.StateDigest = update.StateDigest
	}
	r.LastError = update.LastError
	s.migrations[id] = r
	return r, nil
}

func (s *metadataState) takeoverMigration(id MigrationID, epoch uint64) (MigrationRecord, error) {
	r, ok := s.migrations[id]
	if !ok || r.Epoch != epoch || r.State == MigrationSourceRetired || r.State == MigrationAborted || r.Epoch == ^uint64(0) {
		return MigrationRecord{}, ErrMigrationConflict
	}
	r.Epoch++
	s.migrations[id] = r
	return r, nil
}

func (s *metadataState) commitMigration(id MigrationID, epoch, expectedCatalog, configVersion uint64) (MigrationRecord, error) {
	r, ok := s.migrations[id]
	if !ok || r.Epoch != epoch || r.State != MigrationSourceRemoving || s.catalog.Generation() != expectedCatalog || r.PromotionBarrier == 0 || r.ConfigVersion != configVersion || r.StateDigest == [32]byte{} {
		return MigrationRecord{}, ErrMigrationConflict
	}
	bootstrap := s.catalog.Snapshot()
	bootstrap.Generation++
	found := false
	for di, d := range bootstrap.Ranges {
		if d.RangeID != r.RangeID {
			continue
		}
		if d.Generation != r.RangeGeneration {
			return MigrationRecord{}, ErrStaleRange
		}
		for ri, replica := range d.Replicas {
			if replica.ReplicaID == r.SourceReplicaID {
				d.Replicas[ri] = ReplicaDescriptor{ReplicaID: r.TargetReplicaID, NodeID: r.TargetNodeID}
				found = true
			}
		}
		d.Generation++
		bootstrap.Ranges[di] = d
	}
	if !found {
		return MigrationRecord{}, ErrMigrationConflict
	}
	candidate, err := NewCatalog(bootstrap)
	if err != nil {
		return MigrationRecord{}, fmt.Errorf("validate migration catalog: %w", err)
	}
	s.catalog = candidate
	r.State = MigrationCommitted
	r.CatalogGeneration = candidate.Generation()
	s.migrations[id] = r
	return r, nil
}
