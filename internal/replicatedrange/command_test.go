package replicatedrange

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/mvcc"
)

func TestCommandRoundTripCanonicalAndOwned(t *testing.T) {
	for _, command := range []Command{
		{Type: CommandPut, Key: []byte{0, 0xff}, Value: []byte{}},
		{Type: CommandPut, Key: []byte("key"), Value: []byte("value")},
		{Type: CommandDelete, Key: []byte("gone")},
		{Type: CommandPut, Timestamp: mvcc.Timestamp(^uint64(0)), Key: []byte{0, 0xff}, Value: []byte("mvcc")},
	} {
		encoded, err := EncodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeCommand(encoded)
		if err != nil || decoded.Type != command.Type || decoded.Timestamp != command.Timestamp || !bytes.Equal(decoded.Key, command.Key) || !bytes.Equal(decoded.Value, command.Value) {
			t.Fatalf("decoded=%+v want=%+v err=%v", decoded, command, err)
		}
		wantDecoded := Command{Type: command.Type, Timestamp: command.Timestamp, Key: bytes.Clone(command.Key), Value: bytes.Clone(command.Value)}
		reencoded, err := EncodeCommand(decoded)
		if err != nil || !bytes.Equal(reencoded, encoded) {
			t.Fatalf("noncanonical re-encode: %x != %x err=%v", reencoded, encoded, err)
		}
		if len(command.Key)+len(command.Value) != 0 {
			encoded[len(encoded)-1] ^= 0xff
			if decoded.Type != wantDecoded.Type || !bytes.Equal(decoded.Key, wantDecoded.Key) || !bytes.Equal(decoded.Value, wantDecoded.Value) {
				t.Fatalf("decoded command aliases encoded bytes: got=%+v want=%+v", decoded, wantDecoded)
			}
		}
	}
}

func TestCommandRejectsMalformedInputAtEveryTruncation(t *testing.T) {
	valid, err := EncodeCommand(Command{Type: CommandPut, Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	for offset := range len(valid) {
		if _, err := DecodeCommand(valid[:offset]); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("truncation %d error=%v", offset, err)
		}
	}
	for _, mutate := range []func([]byte){
		func(data []byte) { data[0] ^= 1 },
		func(data []byte) { data[4]++ },
		func(data []byte) { data[5] = 99 },
		func(data []byte) { data[6] = 1 },
		func(data []byte) { binary.LittleEndian.PutUint32(data[8:12], ^uint32(0)) },
		func(data []byte) { binary.LittleEndian.PutUint32(data[12:16], ^uint32(0)) },
	} {
		corrupt := bytes.Clone(valid)
		mutate(corrupt)
		if _, err := DecodeCommand(corrupt); err == nil {
			t.Fatalf("accepted malformed command: %x", corrupt)
		}
	}
	deleteWithValue := Command{Type: CommandDelete, Key: []byte("key"), Value: []byte("bad")}
	if _, err := EncodeCommand(deleteWithValue); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("DELETE value error=%v", err)
	}
}

func TestMVCCCommandRejectsEveryTruncationAndMalformedTimestamp(t *testing.T) {
	valid, err := EncodeCommand(Command{Type: CommandPut, Timestamp: 123, Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	for offset := range len(valid) {
		if _, err := DecodeCommand(valid[:offset]); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("truncation %d=%v", offset, err)
		}
	}
	zero := bytes.Clone(valid)
	for i := 8; i < 16; i++ {
		zero[i] = 0
	}
	if _, err := DecodeCommand(zero); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("zero=%v", err)
	}
}
