package multiraft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/storage/engine"
)

type SplitManagerOptions struct {
	Meta   *MetaRange
	Router *Router
	Hook   SplitHook
}

type SplitManager struct {
	meta   *MetaRange
	router *Router
	hook   SplitHook
}

func NewSplitManager(options SplitManagerOptions) (*SplitManager, error) {
	if options.Meta == nil || options.Router == nil {
		return nil, ErrInvalidCatalog
	}
	options.Router.AttachMetaRange(options.Meta)
	return &SplitManager{meta: options.Meta, router: options.Router, hook: options.Hook}, nil
}

func (m *SplitManager) observe(stage SplitHookStage, record SplitRecord) {
	if m.hook != nil {
		m.hook(stage, cloneSplit(record))
	}
}

// SplitRange executes the bounded logical Phase 7 protocol. The replicated
// records make every individual step idempotent; callers may RecoverSplit after
// an interruption.
func (m *SplitManager) SplitRange(ctx context.Context, parentID RangeID, splitKey []byte) (SplitRecord, error) {
	if ctx == nil {
		return SplitRecord{}, ErrInvalidCatalog
	}
	catalog := m.meta.Snapshot().Catalog
	parent, err := catalog.LookupByID(parentID)
	if err != nil {
		return SplitRecord{}, err
	}
	record, err := m.meta.BeginSplit(ctx, RangeRef{RangeID: parent.RangeID, Generation: parent.Generation}, splitKey, catalog.Generation())
	if err != nil {
		return SplitRecord{}, err
	}
	m.observe(MetaSplitRecordDurable, record)
	return m.run(ctx, record)
}

func (m *SplitManager) RecoverSplit(ctx context.Context, id SplitID) (SplitRecord, error) {
	var record SplitRecord
	for _, candidate := range m.meta.Snapshot().Splits {
		if candidate.SplitID == id {
			record = candidate
			break
		}
	}
	if record.SplitID == 0 {
		return SplitRecord{}, ErrRangeNotFound
	}
	if record.State == SplitCommitted || record.State == SplitAborted {
		if record.State == SplitCommitted {
			return m.finishCommitted(ctx, record)
		}
		return record, nil
	}
	next, err := m.meta.TakeoverSplit(ctx, id, record.Epoch)
	if err != nil {
		return SplitRecord{}, err
	}
	return m.run(ctx, next)
}

func (m *SplitManager) AbortSplit(ctx context.Context, id SplitID, reason string) (SplitRecord, error) {
	var record SplitRecord
	for _, candidate := range m.meta.Snapshot().Splits {
		if candidate.SplitID == id {
			record = candidate
			break
		}
	}
	if record.SplitID == 0 {
		return SplitRecord{}, ErrRangeNotFound
	}
	if record.State == SplitFenced || record.State == SplitCommitted {
		return SplitRecord{}, ErrSplitConflict
	}
	if record.State == SplitAborted {
		return record, nil
	}
	parent, err := m.parentDescriptor(record)
	if err != nil {
		return SplitRecord{}, err
	}
	if _, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpAbort, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
		ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation}, descriptorAnchor(parent)); err != nil {
		return SplitRecord{}, err
	}
	return m.meta.AbortSplit(ctx, id, record.Epoch, reason)
}

//nolint:govet // stage-local error names keep the protocol transitions readable
func (m *SplitManager) run(ctx context.Context, record SplitRecord) (SplitRecord, error) {
	parent, err := m.parentDescriptor(record)
	if err != nil {
		return SplitRecord{}, err
	}
	if record.State != SplitPreparing && record.State != SplitCommitted && record.State != SplitAborted {
		if _, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBegin, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation}, descriptorAnchor(parent)); err != nil {
			return record, err
		}
	}
	if record.State == SplitPreparing {
		if _, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBegin, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation}, descriptorAnchor(parent)); err != nil {
			return record, err
		}
		m.observe(ParentSplitFenceActive, record)
		if err := m.drainTransactions(ctx, parent); err != nil {
			return record, err
		}
		m.observe(TxnDrainComplete, record)
		index, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBootstrapBarrier, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation}, descriptorAnchor(parent))
		if err != nil {
			return record, err
		}
		record, err = m.meta.AdvanceSplit(ctx, record.SplitID, record.Epoch, SplitCopying, SplitRecord{BootstrapIndex: index})
		if err != nil {
			return record, err
		}
		m.observe(BootstrapBarrierDurable, record)
	}
	if record.State == SplitCopying {
		if err := m.openChildren(record); err != nil { //nolint:contextcheck // opening a durable replica has its own cleanup context
			return record, err
		}
		versions, inheritedTimestamp, digest, err := m.parentImage(ctx, parent, record.BootstrapIndex)
		if err != nil {
			return record, err
		}
		for _, child := range []RangeDescriptor{record.Left, record.Right} {
			childVersions := versionsForDescriptor(versions, child)
			childDigest := digestVersions(childVersions)
			if _, err := m.proposeOperation(ctx, child, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBootstrapBarrier, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
				ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: record.BootstrapIndex, Timestamp: inheritedTimestamp, ImageDigest: childDigest}, descriptorAnchor(child)); err != nil {
				return record, err
			}
			for _, version := range childVersions {
				op := replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBootstrapVersion, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
					ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: record.BootstrapIndex,
					Timestamp: version.Timestamp, Kind: version.Kind, Value: version.Value, ImageDigest: childDigest}
				if _, err := m.proposeOperation(ctx, child, op, version.Key); err != nil {
					return record, err
				}
			}
			if err := m.waitChildImage(ctx, record, child, childVersions, childDigest); err != nil {
				return record, err
			}
			if err := m.flushReplicas(ctx, child); err != nil {
				return record, err
			}
			if err := m.checkChildImage(ctx, record, child, childVersions, childDigest); err != nil {
				return record, err
			}
			if child.RangeID == record.Left.RangeID {
				m.observe(LeftImageDurable, record)
			} else {
				m.observe(RightImageDurable, record)
			}
		}
		m.observe(ChildQuorumReady, record)
		record, err = m.meta.AdvanceSplit(ctx, record.SplitID, record.Epoch, SplitCatchingUp, SplitRecord{ImageDigest: digest,
			LeftReplayThrough: record.BootstrapIndex, RightReplayThrough: record.BootstrapIndex})
		if err != nil {
			return record, err
		}
	}
	if record.State == SplitCatchingUp {
		head, err := m.parentApplied(ctx, parent)
		if err != nil {
			return record, err
		}
		if err := m.replay(ctx, record, parent, head); err != nil {
			return record, err
		}
		record, err = m.meta.AdvanceSplit(ctx, record.SplitID, record.Epoch, SplitReady, SplitRecord{LeftReplayThrough: head, RightReplayThrough: head})
		if err != nil {
			return record, err
		}
	}
	if record.State == SplitReady {
		fence, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpFinalFence, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation}, descriptorAnchor(parent))
		if err != nil {
			return record, err
		}
		fencedRecord := cloneSplit(record)
		fencedRecord.FenceIndex = fence
		m.observe(FinalFenceDurable, fencedRecord)
		if err := m.replay(ctx, record, parent, fence); err != nil {
			return record, err
		}
		if err := m.verifyPartition(ctx, record, parent); err != nil {
			return record, err
		}
		fencedRecord.LeftReplayThrough = fence
		fencedRecord.RightReplayThrough = fence
		m.observe(ChildrenReplayedThroughFence, fencedRecord)
		record, err = m.meta.AdvanceSplit(ctx, record.SplitID, record.Epoch, SplitFenced, SplitRecord{FenceIndex: fence, LeftReplayThrough: fence, RightReplayThrough: fence})
		if err != nil {
			return record, err
		}
	}
	if record.State == SplitFenced {
		catalogGeneration := m.meta.Snapshot().Catalog.Generation()
		record, err = m.meta.CommitSplit(ctx, record.SplitID, record.Epoch, catalogGeneration)
		if err != nil {
			return record, err
		}
		m.observe(MetaCutoverDurable, record)
		return m.finishCommitted(ctx, record)
	}
	return record, nil
}

func (m *SplitManager) verifyPartition(ctx context.Context, record SplitRecord, parent RangeDescriptor) error {
	parentReplica, err := m.router.leaderReplica(ctx, parent)
	if err != nil {
		return err
	}
	parentVersions, err := parentReplica.ExportMVCCVersions(ctx)
	if err != nil {
		return fmt.Errorf("export parent partition proof: %w", err)
	}
	filtered := parentVersions[:0]
	for _, version := range parentVersions {
		if version.Kind != storage.KindIntent {
			filtered = append(filtered, version)
		}
	}
	for _, child := range []RangeDescriptor{record.Left, record.Right} {
		childReplica, leaderErr := m.router.leaderReplica(ctx, child)
		if leaderErr != nil {
			return leaderErr
		}
		actual, exportErr := childReplica.ExportMVCCVersions(ctx)
		if exportErr != nil {
			return fmt.Errorf("export child partition proof: %w", exportErr)
		}
		var expected []engine.MVCCVersion
		for _, version := range filtered {
			if child.Contains(version.Key) {
				expected = append(expected, version)
			}
		}
		if !equalVersions(expected, actual) {
			return errors.Join(ErrSplitConflict, engine.ErrCorruption)
		}
		if childReplica.Status().HLCFloor < parentReplica.Status().MaxAppliedMVCC {
			return replicatedrange.ErrMVCCRegression
		}
	}
	return nil
}

func equalVersions(left, right []engine.MVCCVersion) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Timestamp != right[index].Timestamp || left[index].Kind != right[index].Kind || !bytes.Equal(left[index].Key, right[index].Key) || !bytes.Equal(left[index].Value, right[index].Value) {
			return false
		}
	}
	return true
}

func versionsForDescriptor(versions []engine.MVCCVersion, descriptor RangeDescriptor) []engine.MVCCVersion {
	result := make([]engine.MVCCVersion, 0, len(versions)/2)
	for _, version := range versions {
		if descriptor.Contains(version.Key) {
			result = append(result, version)
		}
	}
	return result
}

func (m *SplitManager) waitChildImage(ctx context.Context, record SplitRecord, child RangeDescriptor, expected []engine.MVCCVersion, digest [32]byte) error {
	for range m.router.maxWork {
		if err := m.checkChildImage(ctx, record, child, expected, digest); err == nil {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for child image: %w", err)
		}
		if err := m.router.scheduler.Round(); err != nil && !errors.Is(err, ErrUnknownRange) && !errors.Is(err, ErrNodeStopped) {
			return err
		}
	}
	return ErrLeaderUnknown
}

func (m *SplitManager) checkChildImage(ctx context.Context, record SplitRecord, child RangeDescriptor, expected []engine.MVCCVersion, digest [32]byte) error {
	for _, assigned := range child.Replicas {
		node := m.router.scheduler.Nodes()[assigned.NodeID]
		if node == nil {
			return ErrNodeStopped
		}
		replica, err := node.Replica(child.RangeID)
		if err != nil {
			return err
		}
		status := replica.Status()
		if status.BootstrapSplitID != uint64(record.SplitID) || status.BootstrapParentRangeID != uint64(record.Parent.RangeID) ||
			status.BootstrapParentGen != record.Parent.Generation || status.BootstrapParentIndex != record.BootstrapIndex || status.BootstrapImageDigest != digest {
			return ErrSplitConflict
		}
		actual, err := replica.ExportMVCCVersions(ctx)
		if err != nil || !equalVersions(expected, actual) || digestVersions(actual) != digest {
			return errors.Join(ErrSplitConflict, err)
		}
	}
	return nil
}

func (m *SplitManager) finishCommitted(ctx context.Context, record SplitRecord) (SplitRecord, error) {
	parent := RangeDescriptor{RangeID: record.Parent.RangeID, Generation: record.Parent.Generation,
		StartKey: record.Left.StartKey, EndKey: record.Right.EndKey, Replicas: record.Left.Replicas}
	catalog := m.meta.Snapshot().Catalog
	for _, node := range m.router.scheduler.Nodes() {
		if err := node.InstallDynamicCatalog(catalog); err != nil {
			return record, err
		}
		node.InstallRetiredRedirect(parent.RangeID, []RangeDescriptor{record.Left, record.Right})
	}
	if err := m.router.InstallCatalog(catalog); err != nil {
		return record, err
	}
	m.router.InstallRetiredArchive(parent)
	for _, child := range []RangeDescriptor{record.Left, record.Right} {
		if _, err := m.proposeOperation(ctx, child, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpActivate, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: record.FenceIndex}, descriptorAnchor(child)); err != nil {
			return record, err
		}
	}
	m.observe(ChildActivated, record)
	if _, err := m.proposeOperation(ctx, parent, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpRetire, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
		ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: record.FenceIndex}, descriptorAnchor(parent)); err != nil {
		return record, err
	}
	m.observe(ParentRetired, record)
	return record, nil
}

func (m *SplitManager) parentDescriptor(record SplitRecord) (RangeDescriptor, error) {
	if descriptor, err := m.router.currentCatalog().LookupByID(record.Parent.RangeID); err == nil {
		return descriptor, nil
	}
	for _, node := range m.router.scheduler.Nodes() {
		if descriptor, err := node.descriptorFor(record.Parent.RangeID); err == nil {
			return descriptor, nil
		}
	}
	return RangeDescriptor{}, ErrRangeNotFound
}

func (m *SplitManager) drainTransactions(ctx context.Context, parent RangeDescriptor) error {
	var recoveryErr error
	for attempt := 0; attempt < m.router.maxAttempts; attempt++ {
		recoveryErr = m.router.RecoverTransactions(ctx)
		if recoveryErr == nil {
			break
		}
		if !errors.Is(recoveryErr, replicatedrange.ErrLeadershipLost) && !errors.Is(recoveryErr, ErrLeaderUnknown) && !errors.Is(recoveryErr, raft.ErrNotLeader) {
			break
		}
	}
	if recoveryErr != nil {
		return fmt.Errorf("recover transactions for split drain: %w", recoveryErr)
	}
	replica, err := m.router.leaderReplica(ctx, parent)
	if err != nil {
		return err
	}
	if !replica.TransactionDrainReady() {
		return ErrRangeSplitting
	}
	return nil
}

func (m *SplitManager) openChildren(record SplitRecord) error {
	for _, node := range m.router.scheduler.Nodes() {
		for _, child := range []RangeDescriptor{record.Left, record.Right} {
			if _, assigned := child.ReplicaOn(node.ID()); !assigned {
				continue
			}
			if err := node.AddShadowRange(child); err != nil {
				return fmt.Errorf("open shadow child %d: %w", child.RangeID, err)
			}
		}
	}
	m.router.scheduler.Refresh()
	return nil
}

func (m *SplitManager) parentImage(ctx context.Context, parent RangeDescriptor, bootstrap uint64) ([]engine.MVCCVersion, uint64, [32]byte, error) {
	replica, err := m.router.leaderReplica(ctx, parent)
	if err != nil {
		return nil, 0, [32]byte{}, err
	}
	if replica.Status().Raft.LastApplied < bootstrap {
		return nil, 0, [32]byte{}, ErrLeaderUnknown
	}
	entries, err := replica.CommittedEntries(bootstrap, bootstrap)
	if err != nil || len(entries) != 1 || entries[0].Type != raft.EntryCommand {
		return nil, 0, [32]byte{}, errors.Join(ErrCorruptMetadata, err)
	}
	barrierCommand, err := replicatedrange.DecodeCommand(entries[0].Command)
	if err != nil {
		return nil, 0, [32]byte{}, fmt.Errorf("decode parent bootstrap barrier: %w", err)
	}
	barrier, err := replicatedrange.DecodeSplitOperation(barrierCommand.Value)
	if err != nil || barrier.Type != replicatedrange.SplitOpBootstrapBarrier || barrier.Timestamp == 0 {
		return nil, 0, [32]byte{}, errors.Join(ErrCorruptMetadata, err)
	}
	maximum := barrier.Timestamp
	versions, err := replica.ExportMVCCVersions(ctx)
	if err != nil {
		return nil, 0, [32]byte{}, fmt.Errorf("export parent bootstrap image: %w", err)
	}
	filtered := versions[:0]
	for _, version := range versions {
		if version.Timestamp <= maximum {
			if version.Kind == storage.KindIntent {
				// Drained intents have a committed value/delete or abort marker at
				// the same timestamp. Intent protocol bytes are system state and
				// are deliberately excluded from the child user-state image.
				continue
			}
			filtered = append(filtered, version)
		}
	}
	return filtered, maximum, digestVersions(filtered), nil
}

func digestVersions(versions []engine.MVCCVersion) [32]byte {
	hash := sha256.New()
	var field [8]byte
	for _, version := range versions {
		binary.LittleEndian.PutUint64(field[:], uint64(len(version.Key)))
		_, _ = hash.Write(field[:])
		_, _ = hash.Write(version.Key)
		binary.LittleEndian.PutUint64(field[:], version.Timestamp)
		_, _ = hash.Write(field[:])
		_, _ = hash.Write([]byte{byte(version.Kind)})
		binary.LittleEndian.PutUint64(field[:], uint64(len(version.Value)))
		_, _ = hash.Write(field[:])
		_, _ = hash.Write(version.Value)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func (m *SplitManager) parentApplied(ctx context.Context, parent RangeDescriptor) (uint64, error) {
	replica, err := m.router.leaderReplica(ctx, parent)
	if err != nil {
		return 0, err
	}
	return replica.Status().Raft.LastApplied, nil
}

//nolint:govet // replay keeps proposal errors scoped to the exact child/index operation
func (m *SplitManager) replay(ctx context.Context, record SplitRecord, parent RangeDescriptor, through uint64) error {
	parentReplica, err := m.router.leaderReplica(ctx, parent)
	if err != nil {
		return err
	}
	for _, child := range []RangeDescriptor{record.Left, record.Right} {
		if _, err := m.proposeOperation(ctx, child, replicatedrange.SplitOperation{Type: replicatedrange.SplitOpBootstrapBarrier, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
			ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: record.BootstrapIndex}, descriptorAnchor(child)); err != nil {
			return err
		}
	}
	left, err := m.router.leaderReplica(ctx, record.Left)
	if err != nil {
		return err
	}
	right, err := m.router.leaderReplica(ctx, record.Right)
	if err != nil {
		return err
	}
	from := min(left.Status().ParentReplayThrough, right.Status().ParentReplayThrough) + 1
	if from > through {
		return nil
	}
	entries, err := parentReplica.CommittedEntries(from, through)
	if err != nil {
		return fmt.Errorf("load committed parent delta: %w", err)
	}
	byIndex := make(map[uint64]raft.Entry, len(entries))
	for _, entry := range entries {
		byIndex[entry.Index] = entry
	}
	for index := from; index <= through; index++ {
		entry, exists := byIndex[index]
		for _, child := range []RangeDescriptor{record.Left, record.Right} {
			op := replicatedrange.SplitOperation{Type: replicatedrange.SplitOpReplayAdvance, SplitID: uint64(record.SplitID), Epoch: record.Epoch,
				ParentRangeID: uint64(parent.RangeID), ParentGeneration: parent.Generation, ParentIndex: index}
			key := descriptorAnchor(child)
			if exists && entry.Type == raft.EntryCommand {
				command, decodeErr := replicatedrange.DecodeCommand(entry.Command)
				if decodeErr != nil {
					return fmt.Errorf("decode parent delta at %d: %w", index, decodeErr)
				}
				if (command.Type == replicatedrange.CommandPut || command.Type == replicatedrange.CommandDelete) && child.Contains(command.Key) {
					op.Type, op.Timestamp, op.Value = replicatedrange.SplitOpReplayVersion, uint64(command.Timestamp), command.Value
					if command.Type == replicatedrange.CommandPut {
						op.Kind = storage.KindValue
					} else {
						op.Kind = storage.KindDelete
					}
					op.CommandDigest, key = sha256.Sum256(entry.Command), command.Key
				} else if command.Type == replicatedrange.CommandTxnCreate || command.Type == replicatedrange.CommandTxnPrepare {
					return replicatedrange.ErrSplitFenced
				}
				if op.Type == replicatedrange.SplitOpReplayAdvance {
					op.Timestamp = uint64(command.Timestamp)
				}
			}
			if _, err := m.proposeOperation(ctx, child, op, key); err != nil {
				return err
			}
		}
		m.observe(DeltaReplayProgress, record)
	}
	return nil
}

func (m *SplitManager) proposeOperation(ctx context.Context, descriptor RangeDescriptor, operation replicatedrange.SplitOperation, key []byte) (uint64, error) {
	encoded, err := replicatedrange.EncodeSplitOperation(operation)
	if err != nil {
		return 0, fmt.Errorf("encode split operation: %w", err)
	}
	return m.router.proposeSplit(ctx, descriptor, replicatedrange.Command{Type: replicatedrange.CommandSplit, Key: bytes.Clone(key), Value: encoded, Timestamp: mvcc.Timestamp(operation.Timestamp)})
}

func (m *SplitManager) flushReplicas(ctx context.Context, descriptor RangeDescriptor) error {
	for _, replicaDescriptor := range descriptor.Replicas {
		node := m.router.scheduler.Nodes()[replicaDescriptor.NodeID]
		if node == nil {
			return ErrNodeStopped
		}
		replica, err := node.Replica(descriptor.RangeID)
		if err != nil {
			return err
		}
		if err := replica.Flush(ctx); err != nil {
			return fmt.Errorf("flush child range %d replica %d: %w", descriptor.RangeID, replicaDescriptor.ReplicaID, err)
		}
	}
	return nil
}
