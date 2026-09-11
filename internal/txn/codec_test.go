package txn_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rivetdb/rivetdb/internal/txn"
)

func testID(last byte) txn.ID {
	var id txn.ID
	id[len(id)-1] = last
	return id
}

func TestOperationAndIntentCanonicalRoundTrip(t *testing.T) {
	t.Parallel()
	operation := txn.Operation{Type: txn.OpPrepare, ID: testID(1), ReadTime: 10, CommitTime: 20, Epoch: 3,
		Home: txn.Participant{RangeID: 7, Generation: 2}, Writes: []txn.Write{
			{Key: []byte{0, 1}, Value: []byte("value")}, {Key: []byte{0xff}, Delete: true},
		}}
	encoded, err := txn.EncodeOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := txn.DecodeOperation(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != operation.Type || decoded.ID != operation.ID || !txn.EqualWrites(decoded.Writes, operation.Writes) {
		t.Fatalf("round trip = %+v", decoded)
	}
	reencoded, err := txn.EncodeOperation(decoded)
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("noncanonical round trip: %v", err)
	}
	intent := txn.Intent{ID: operation.ID, ReadTime: 10, CommitTime: 20, Epoch: 3, Home: operation.Home, Value: []byte("v")}
	intentBytes, err := txn.EncodeIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	got, err := txn.DecodeIntent(intentBytes)
	if err != nil || got.ID != intent.ID || !bytes.Equal(got.Value, intent.Value) {
		t.Fatalf("intent = %+v err=%v", got, err)
	}
}

func TestHostileTransactionCodecsRejectWithoutPanic(t *testing.T) {
	t.Parallel()
	operation := txn.Operation{Type: txn.OpCreate, ID: testID(1), ReadTime: 1, CommitTime: 2, Epoch: 1,
		Home: txn.Participant{RangeID: 1, Generation: 1}, Participants: []txn.Participant{{RangeID: 1, Generation: 1}}}
	encoded, err := txn.EncodeOperation(operation)
	if err != nil {
		t.Fatal(err)
	}
	for offset := range encoded {
		if _, err := txn.DecodeOperation(encoded[:offset]); err == nil {
			t.Fatalf("truncation %d accepted", offset)
		}
	}
	bad := append([]byte(nil), encoded...)
	bad[4]++
	if _, err := txn.DecodeOperation(bad); !errors.Is(err, txn.ErrInvalid) {
		t.Fatalf("version error=%v", err)
	}
	bad = append([]byte(nil), encoded...)
	bad[64], bad[65], bad[66], bad[67] = 0xff, 0xff, 0xff, 0xff
	if _, err := txn.DecodeOperation(bad); !errors.Is(err, txn.ErrTooLarge) {
		t.Fatalf("count error=%v", err)
	}
}

func TestOperationRejectsDuplicateOrUnsortedFields(t *testing.T) {
	t.Parallel()
	base := txn.Operation{Type: txn.OpCreate, ID: testID(1), ReadTime: 1, CommitTime: 2, Epoch: 1,
		Home: txn.Participant{RangeID: 1, Generation: 1}}
	base.Participants = []txn.Participant{{RangeID: 2, Generation: 1}, {RangeID: 2, Generation: 1}}
	if _, err := txn.EncodeOperation(base); !errors.Is(err, txn.ErrInvalid) {
		t.Fatalf("duplicate participants=%v", err)
	}
	base.Type, base.Participants = txn.OpPrepare, nil
	base.Writes = []txn.Write{{Key: []byte("b")}, {Key: []byte("a")}}
	if _, err := txn.EncodeOperation(base); !errors.Is(err, txn.ErrInvalid) {
		t.Fatalf("unsorted writes=%v", err)
	}
}
