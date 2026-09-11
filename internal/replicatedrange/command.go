// Package replicatedrange integrates one static Raft group with one local LSM
// without importing standalone storage ordering or WAL semantics.
package replicatedrange

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/storage/sstable"
)

const (
	commandHeaderSize = 16
	commandVersion    = uint8(1)
)

var commandMagic = [4]byte{'R', 'V', 'C', 'M'}

type CommandType uint8

const (
	CommandPut CommandType = iota + 1
	CommandDelete
)

type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
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
	result := make([]byte, commandHeaderSize+len(command.Key)+len(command.Value))
	copy(result[:4], commandMagic[:])
	result[4], result[5] = commandVersion, byte(command.Type)
	binary.LittleEndian.PutUint32(result[8:12], uint32(len(command.Key)))    //nolint:gosec // storage bound fits u32
	binary.LittleEndian.PutUint32(result[12:16], uint32(len(command.Value))) //nolint:gosec // storage bound fits u32
	copy(result[commandHeaderSize:], command.Key)
	copy(result[commandHeaderSize+len(command.Key):], command.Value)
	return result, nil
}

func DecodeCommand(encoded []byte) (Command, error) {
	if len(encoded) < commandHeaderSize || !bytes.Equal(encoded[:4], commandMagic[:]) {
		return Command{}, ErrInvalidCommand
	}
	if encoded[4] != commandVersion {
		return Command{}, ErrUnsupportedCommand
	}
	if encoded[6] != 0 || encoded[7] != 0 {
		return Command{}, ErrInvalidCommand
	}
	keyLength := uint64(binary.LittleEndian.Uint32(encoded[8:12]))
	valueLength := uint64(binary.LittleEndian.Uint32(encoded[12:16]))
	if keyLength > sstable.MaxUserKeySize || valueLength > sstable.MaxValueSize {
		return Command{}, ErrCommandTooLarge
	}
	if keyLength+valueLength > raft.MaxCommandBytes-commandHeaderSize {
		return Command{}, ErrCommandTooLarge
	}
	if keyLength+valueLength != uint64(len(encoded)-commandHeaderSize) { //nolint:gosec // header length checked
		return Command{}, ErrInvalidCommand
	}
	keyEnd := commandHeaderSize + int(keyLength) //nolint:gosec // bounded by exact encoded length
	command := Command{Type: CommandType(encoded[5]), Key: bytes.Clone(encoded[commandHeaderSize:keyEnd]), Value: bytes.Clone(encoded[keyEnd:])}
	if err := validateCommand(command); err != nil {
		return Command{}, err
	}
	return command, nil
}

func validateCommand(command Command) error {
	if command.Type != CommandPut && command.Type != CommandDelete {
		return fmt.Errorf("%w: type %d", ErrInvalidCommand, command.Type)
	}
	if len(command.Key) > sstable.MaxUserKeySize || len(command.Value) > sstable.MaxValueSize {
		return ErrCommandTooLarge
	}
	if len(command.Key) > raft.MaxCommandBytes-commandHeaderSize || len(command.Value) > raft.MaxCommandBytes-commandHeaderSize-len(command.Key) {
		return ErrCommandTooLarge
	}
	if command.Type == CommandDelete && len(command.Value) != 0 {
		return fmt.Errorf("%w: DELETE has value", ErrInvalidCommand)
	}
	return nil
}
