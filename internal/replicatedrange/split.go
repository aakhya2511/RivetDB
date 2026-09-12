package replicatedrange

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/storage"
)

type Lifecycle uint8

const (
	LifecycleActive Lifecycle = iota + 1
	LifecycleShadow
	LifecycleRetired
	LifecycleLearner
)

type SplitOperationType uint8

const (
	SplitOpBegin SplitOperationType = iota + 1
	SplitOpBootstrapBarrier
	SplitOpBootstrapVersion
	SplitOpReplayVersion
	SplitOpReplayAdvance
	SplitOpFinalFence
	SplitOpActivate
	SplitOpRetire
	SplitOpAbort
)

type SplitOperation struct {
	Type             SplitOperationType
	SplitID          uint64
	Epoch            uint64
	ParentRangeID    uint64
	ParentGeneration uint64
	ParentIndex      uint64
	Timestamp        uint64
	Kind             storage.ValueKind
	CommandDigest    [sha256.Size]byte
	ImageDigest      [sha256.Size]byte
	Value            []byte
}

var splitOperationMagic = [4]byte{'R', 'V', 'S', 'P'}

func EncodeSplitOperation(op SplitOperation) ([]byte, error) {
	if err := validateSplitOperation(op); err != nil {
		return nil, err
	}
	result := make([]byte, 0, 104+len(op.Value))
	result = append(result, splitOperationMagic[:]...)
	result = append(result, 1, byte(op.Type), byte(op.Kind), 0)
	for _, value := range []uint64{op.SplitID, op.Epoch, op.ParentRangeID, op.ParentGeneration, op.ParentIndex, op.Timestamp} {
		result = binary.LittleEndian.AppendUint64(result, value)
	}
	result = append(result, op.CommandDigest[:]...)
	result = append(result, op.ImageDigest[:]...)
	result = binary.LittleEndian.AppendUint32(result, uint32(len(op.Value))) //nolint:gosec // command bound
	return append(result, op.Value...), nil
}

func DecodeSplitOperation(data []byte) (SplitOperation, error) {
	const header = 124
	if len(data) < header || !bytes.Equal(data[:4], splitOperationMagic[:]) || data[4] != 1 || data[7] != 0 {
		return SplitOperation{}, ErrInvalidCommand
	}
	op := SplitOperation{Type: SplitOperationType(data[5]), Kind: storage.ValueKind(data[6])}
	values := []*uint64{&op.SplitID, &op.Epoch, &op.ParentRangeID, &op.ParentGeneration, &op.ParentIndex, &op.Timestamp}
	offset := 8
	for _, value := range values {
		*value = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}
	copy(op.CommandDigest[:], data[offset:offset+32])
	offset += 32
	copy(op.ImageDigest[:], data[offset:offset+32])
	offset += 32
	length := uint64(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	if length != uint64(len(data)-offset) { //nolint:gosec // offset was bounded by the fixed header check
		return SplitOperation{}, ErrInvalidCommand
	}
	op.Value = bytes.Clone(data[offset:])
	if err := validateSplitOperation(op); err != nil {
		return SplitOperation{}, err
	}
	return op, nil
}

func validateSplitOperation(op SplitOperation) error {
	if op.Type < SplitOpBegin || op.Type > SplitOpAbort || op.SplitID == 0 || op.Epoch == 0 || op.ParentRangeID == 0 || op.ParentGeneration == 0 {
		return ErrInvalidCommand
	}
	version := op.Type == SplitOpBootstrapVersion || op.Type == SplitOpReplayVersion
	if version {
		if op.Timestamp == 0 || op.Kind > storage.KindIntent {
			return errors.Join(ErrInvalidCommand, storage.ErrInvalidValueKind)
		}
		if op.Type == SplitOpReplayVersion && (op.ParentIndex == 0 || op.CommandDigest == [32]byte{}) {
			return ErrInvalidCommand
		}
	} else if len(op.Value) != 0 || op.Kind != 0 || op.Timestamp != 0 && op.Type != SplitOpBootstrapBarrier && op.Type != SplitOpReplayAdvance {
		return fmt.Errorf("%w: metadata split operation contains MVCC value", ErrInvalidCommand)
	}
	return nil
}

func SplitDigest(versions []SplitOperation) [sha256.Size]byte {
	hash := sha256.New()
	var field [8]byte
	for _, version := range versions {
		binary.LittleEndian.PutUint64(field[:], uint64(len(version.Value)))
		_, _ = hash.Write(field[:])
		binary.LittleEndian.PutUint64(field[:], version.Timestamp)
		_, _ = hash.Write(field[:])
		_, _ = hash.Write([]byte{byte(version.Kind)})
		_, _ = hash.Write(version.Value)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
