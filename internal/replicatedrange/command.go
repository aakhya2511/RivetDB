// Package replicatedrange integrates one static Raft group with one local LSM
// without importing standalone storage ordering or WAL semantics.
package replicatedrange

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
	"github.com/rivetdb/rivetdb/internal/txn"
)

const (
	commandHeaderSize     = 16
	mvccCommandHeaderSize = 24
	commandVersion        = uint8(1)
	mvccCommandVersion    = uint8(2)
)

var commandMagic = [4]byte{'R', 'V', 'C', 'M'}

type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
	CommandTxnBarrier
	CommandTxnCreate
	CommandTxnPrepare
	CommandTxnCommit
	CommandTxnAbort
	CommandTxnTakeover
	CommandTxnResolveCommit
	CommandTxnResolveAbort
	CommandSplit
)

type Command struct {
	Type      CommandType
	Key       []byte
	Value     []byte
	Timestamp mvcc.Timestamp
}

var (
	ErrInvalidCommand     = errors.New("replicated range: invalid command")
	ErrUnsupportedCommand = errors.New("replicated range: unsupported command version")
	ErrCommandTooLarge    = errors.New("replicated range: command exceeds storage bounds")
)

func EncodeCommand(command Command) ([]byte, error) {
	if err := validateCommand(command); err != nil {
		return nil, err
	}
	headerSize := commandHeaderSize
	version := commandVersion
	if command.Timestamp != 0 {
		headerSize, version = mvccCommandHeaderSize, mvccCommandVersion
	}
	result := make([]byte, headerSize+len(command.Key)+len(command.Value))
	copy(result[:4], commandMagic[:])
	result[4], result[5] = version, byte(command.Type)
	lengthOffset := 8
	if version == mvccCommandVersion {
		binary.BigEndian.PutUint64(result[8:16], uint64(command.Timestamp))
		lengthOffset = 16
	}
	binary.LittleEndian.PutUint32(result[lengthOffset:lengthOffset+4], uint32(len(command.Key)))     //nolint:gosec // storage bound fits u32
	binary.LittleEndian.PutUint32(result[lengthOffset+4:lengthOffset+8], uint32(len(command.Value))) //nolint:gosec // storage bound fits u32
	copy(result[headerSize:], command.Key)
	copy(result[headerSize+len(command.Key):], command.Value)
	return result, nil
}

func DecodeCommand(encoded []byte) (Command, error) {
	if len(encoded) < commandHeaderSize || !bytes.Equal(encoded[:4], commandMagic[:]) {
		return Command{}, ErrInvalidCommand
	}
	version := encoded[4]
	if version != commandVersion && version != mvccCommandVersion {
		return Command{}, ErrUnsupportedCommand
	}
	if encoded[6] != 0 || encoded[7] != 0 {
		return Command{}, ErrInvalidCommand
	}
	headerSize, lengthOffset := commandHeaderSize, 8
	var timestamp mvcc.Timestamp
	if version == mvccCommandVersion {
		if len(encoded) < mvccCommandHeaderSize {
			return Command{}, ErrInvalidCommand
		}
		headerSize, lengthOffset = mvccCommandHeaderSize, 16
		timestamp = mvcc.Timestamp(binary.BigEndian.Uint64(encoded[8:16]))
		if timestamp == 0 {
			return Command{}, ErrInvalidCommand
		}
	}
	keyLength := uint64(binary.LittleEndian.Uint32(encoded[lengthOffset : lengthOffset+4]))
	valueLength := uint64(binary.LittleEndian.Uint32(encoded[lengthOffset+4 : lengthOffset+8]))
	if keyLength > sstable.MaxUserKeySize || valueLength > sstable.MaxValueSize {
		return Command{}, ErrCommandTooLarge
	}
	if keyLength+valueLength > uint64(raft.MaxCommandBytes-headerSize) { //nolint:gosec // positive compile-time resource bound
		return Command{}, ErrCommandTooLarge
	}
	if keyLength+valueLength != uint64(len(encoded)-headerSize) { //nolint:gosec // header length checked
		return Command{}, ErrInvalidCommand
	}
	keyEnd := headerSize + int(keyLength) //nolint:gosec // bounded by exact encoded length
	command := Command{Type: CommandType(encoded[5]), Timestamp: timestamp, Key: bytes.Clone(encoded[headerSize:keyEnd]), Value: bytes.Clone(encoded[keyEnd:])}
	if err := validateCommand(command); err != nil {
		return Command{}, err
	}
	return command, nil
}

func validateCommand(command Command) error {
	if command.Type < CommandPut || command.Type > CommandSplit {
		return fmt.Errorf("%w: type %d", ErrInvalidCommand, command.Type)
	}
	if len(command.Key) > sstable.MaxUserKeySize || len(command.Value) > sstable.MaxValueSize {
		return ErrCommandTooLarge
	}
	headerSize := commandHeaderSize
	if command.Timestamp != 0 {
		headerSize = mvccCommandHeaderSize
	}
	if len(command.Key) > raft.MaxCommandBytes-headerSize || len(command.Value) > raft.MaxCommandBytes-headerSize-len(command.Key) {
		return ErrCommandTooLarge
	}
	if (command.Type == CommandDelete || command.Type == CommandTxnBarrier) && len(command.Value) != 0 {
		return fmt.Errorf("%w: DELETE has value", ErrInvalidCommand)
	}
	if command.Type >= CommandTxnBarrier && command.Type < CommandSplit && command.Timestamp == 0 {
		return fmt.Errorf("%w: transaction command has zero timestamp", ErrInvalidCommand)
	}
	if command.Type > CommandTxnBarrier && command.Type < CommandSplit {
		operation, err := txn.DecodeOperation(command.Value)
		if err != nil {
			return fmt.Errorf("%w: transaction operation: %w", ErrInvalidCommand, err)
		}
		want := map[CommandType]txn.OperationType{
			CommandTxnCreate: txn.OpCreate, CommandTxnPrepare: txn.OpPrepare,
			CommandTxnCommit: txn.OpCommit, CommandTxnAbort: txn.OpAbort,
			CommandTxnTakeover: txn.OpTakeover, CommandTxnResolveCommit: txn.OpResolveCommit,
			CommandTxnResolveAbort: txn.OpResolveAbort,
		}[command.Type]
		if operation.Type != want || operation.CommitTime != uint64(command.Timestamp) {
			return fmt.Errorf("%w: transaction operation mismatch", ErrInvalidCommand)
		}
	}
	if command.Type == CommandSplit {
		operation, err := DecodeSplitOperation(command.Value)
		if err != nil || operation.Timestamp != uint64(command.Timestamp) {
			return fmt.Errorf("%w: split operation mismatch", ErrInvalidCommand)
		}
	}
	return nil
}
