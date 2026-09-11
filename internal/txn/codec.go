package txn

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	operationMagic = [4]byte{'R', 'V', 'T', 'X'}
	intentMagic    = [4]byte{'R', 'V', 'I', 'N'}
)

const (
	codecVersion     = byte(1)
	opHeaderSize     = 72
	intentHeaderSize = 64
)

func EncodeOperation(operation Operation) ([]byte, error) {
	if err := ValidateOperation(operation); err != nil {
		return nil, err
	}
	size := opHeaderSize + len(operation.Participants)*16
	for _, write := range operation.Writes {
		size += 9 + len(write.Key) + len(write.Value)
	}
	if size > MaxWriteBytes {
		return nil, ErrTooLarge
	}
	encoded := make([]byte, opHeaderSize, size)
	copy(encoded[:4], operationMagic[:])
	encoded[4], encoded[5] = codecVersion, byte(operation.Type)
	copy(encoded[8:24], operation.ID[:])
	binary.BigEndian.PutUint64(encoded[24:32], operation.ReadTime)
	binary.BigEndian.PutUint64(encoded[32:40], operation.CommitTime)
	binary.BigEndian.PutUint64(encoded[40:48], operation.Epoch)
	binary.BigEndian.PutUint64(encoded[48:56], operation.Home.RangeID)
	binary.BigEndian.PutUint64(encoded[56:64], operation.Home.Generation)
	binary.BigEndian.PutUint32(encoded[64:68], uint32(len(operation.Participants))) //nolint:gosec // bounded
	binary.BigEndian.PutUint32(encoded[68:72], uint32(len(operation.Writes)))       //nolint:gosec // bounded
	for _, participant := range operation.Participants {
		encoded = binary.BigEndian.AppendUint64(encoded, participant.RangeID)
		encoded = binary.BigEndian.AppendUint64(encoded, participant.Generation)
	}
	for _, write := range operation.Writes {
		var flags byte
		if write.Delete {
			flags = 1
		}
		encoded = append(encoded, flags)
		encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(write.Key)))   //nolint:gosec // bounded
		encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(write.Value))) //nolint:gosec // bounded
		encoded = append(encoded, write.Key...)
		encoded = append(encoded, write.Value...)
	}
	return encoded, nil
}

func DecodeOperation(encoded []byte) (Operation, error) {
	if len(encoded) < opHeaderSize || len(encoded) > MaxWriteBytes || !bytes.Equal(encoded[:4], operationMagic[:]) || encoded[4] != codecVersion || encoded[6] != 0 || encoded[7] != 0 {
		return Operation{}, ErrInvalid
	}
	operation := Operation{Type: OperationType(encoded[5]), ReadTime: binary.BigEndian.Uint64(encoded[24:32]), CommitTime: binary.BigEndian.Uint64(encoded[32:40]), Epoch: binary.BigEndian.Uint64(encoded[40:48]), Home: Participant{RangeID: binary.BigEndian.Uint64(encoded[48:56]), Generation: binary.BigEndian.Uint64(encoded[56:64])}}
	copy(operation.ID[:], encoded[8:24])
	participantCount, writeCount := binary.BigEndian.Uint32(encoded[64:68]), binary.BigEndian.Uint32(encoded[68:72])
	if participantCount > MaxParticipants || writeCount > MaxWrites {
		return Operation{}, ErrTooLarge
	}
	offset := opHeaderSize
	if int(participantCount) > (len(encoded)-offset)/16 { //nolint:gosec // participantCount is bounded by MaxParticipants
		return Operation{}, ErrInvalid
	}
	operation.Participants = make([]Participant, 0, participantCount)
	for range participantCount {
		operation.Participants = append(operation.Participants, Participant{RangeID: binary.BigEndian.Uint64(encoded[offset : offset+8]), Generation: binary.BigEndian.Uint64(encoded[offset+8 : offset+16])})
		offset += 16
	}
	operation.Writes = make([]Write, 0, writeCount)
	for range writeCount {
		if len(encoded)-offset < 9 {
			return Operation{}, ErrInvalid
		}
		flags := encoded[offset]
		if flags > 1 {
			return Operation{}, ErrInvalid
		}
		keyLength, valueLength := binary.BigEndian.Uint32(encoded[offset+1:offset+5]), binary.BigEndian.Uint32(encoded[offset+5:offset+9])
		offset += 9
		remaining := len(encoded) - offset
		if int(keyLength) > remaining || int(valueLength) > remaining-int(keyLength) { //nolint:gosec // lengths are bounded by MaxWriteBytes
			return Operation{}, ErrInvalid
		}
		keyEnd := offset + int(keyLength)     //nolint:gosec // bounded by remaining
		valueEnd := keyEnd + int(valueLength) //nolint:gosec // bounded by remaining
		operation.Writes = append(operation.Writes, Write{Key: bytes.Clone(encoded[offset:keyEnd]), Value: bytes.Clone(encoded[keyEnd:valueEnd]), Delete: flags == 1})
		offset = valueEnd
	}
	if offset != len(encoded) {
		return Operation{}, ErrInvalid
	}
	if err := ValidateOperation(operation); err != nil {
		return Operation{}, err
	}
	return operation, nil
}

func ValidateOperation(operation Operation) error {
	if operation.ID.IsZero() || operation.Epoch == 0 || operation.Home.RangeID == 0 || operation.Home.Generation == 0 || operation.ReadTime == 0 || operation.CommitTime <= operation.ReadTime {
		return ErrInvalid
	}
	switch operation.Type {
	case OpCreate:
		if len(operation.Participants) == 0 || len(operation.Writes) != 0 {
			return ErrInvalid
		}
	case OpPrepare:
		if len(operation.Participants) != 0 || len(operation.Writes) == 0 {
			return ErrInvalid
		}
	case OpCommit, OpAbort, OpTakeover, OpResolveCommit, OpResolveAbort:
		if len(operation.Participants) != 0 || len(operation.Writes) != 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	if len(operation.Participants) > MaxParticipants || len(operation.Writes) > MaxWrites {
		return ErrTooLarge
	}
	for index, participant := range operation.Participants {
		if participant.RangeID == 0 || participant.Generation == 0 || index > 0 && operation.Participants[index-1].RangeID >= participant.RangeID {
			return ErrInvalid
		}
	}
	total := 0
	for index, write := range operation.Writes {
		if write.Delete && len(write.Value) != 0 || index > 0 && bytes.Compare(operation.Writes[index-1].Key, write.Key) >= 0 {
			return ErrInvalid
		}
		total += len(write.Key) + len(write.Value)
		if total > MaxWriteBytes {
			return ErrTooLarge
		}
	}
	return nil
}

func EncodeIntent(intent Intent) ([]byte, error) {
	if intent.ID.IsZero() || intent.ReadTime == 0 || intent.CommitTime <= intent.ReadTime || intent.Epoch == 0 || intent.Home.RangeID == 0 || intent.Home.Generation == 0 || intent.Delete && len(intent.Value) != 0 || len(intent.Value) > MaxWriteBytes {
		return nil, ErrInvalid
	}
	encoded := make([]byte, intentHeaderSize+len(intent.Value))
	copy(encoded[:4], intentMagic[:])
	encoded[4] = codecVersion
	if intent.Delete {
		encoded[5] = 1
	}
	copy(encoded[8:24], intent.ID[:])
	binary.BigEndian.PutUint64(encoded[24:32], intent.ReadTime)
	binary.BigEndian.PutUint64(encoded[32:40], intent.CommitTime)
	binary.BigEndian.PutUint64(encoded[40:48], intent.Epoch)
	binary.BigEndian.PutUint64(encoded[48:56], intent.Home.RangeID)
	binary.BigEndian.PutUint64(encoded[56:64], intent.Home.Generation)
	copy(encoded[intentHeaderSize:], intent.Value)
	return encoded, nil
}

func DecodeIntent(encoded []byte) (Intent, error) {
	if len(encoded) < intentHeaderSize || len(encoded) > intentHeaderSize+MaxWriteBytes || !bytes.Equal(encoded[:4], intentMagic[:]) || encoded[4] != codecVersion || encoded[5] > 1 || encoded[6] != 0 || encoded[7] != 0 {
		return Intent{}, ErrInvalid
	}
	var intent Intent
	copy(intent.ID[:], encoded[8:24])
	intent.Delete = encoded[5] == 1
	intent.ReadTime = binary.BigEndian.Uint64(encoded[24:32])
	intent.CommitTime = binary.BigEndian.Uint64(encoded[32:40])
	intent.Epoch = binary.BigEndian.Uint64(encoded[40:48])
	intent.Home = Participant{RangeID: binary.BigEndian.Uint64(encoded[48:56]), Generation: binary.BigEndian.Uint64(encoded[56:64])}
	intent.Value = bytes.Clone(encoded[intentHeaderSize:])
	if intent.ID.IsZero() || intent.ReadTime == 0 || intent.CommitTime <= intent.ReadTime || intent.Epoch == 0 || intent.Home.RangeID == 0 || intent.Home.Generation == 0 || intent.Delete && len(intent.Value) != 0 {
		return Intent{}, fmt.Errorf("%w: intent fields", ErrInvalid)
	}
	return intent, nil
}
