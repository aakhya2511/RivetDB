package mvcc

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

func TestTimestampEncodingOrderingAndBounds(t *testing.T) {
	values := []Timestamp{0, 1, Timestamp(1000 << LogicalBits), Timestamp(1000<<LogicalBits | 9), Timestamp(math.MaxUint64)}
	for _, value := range values {
		decoded, err := DecodeTimestamp(value.Encode(nil))
		if err != nil || decoded != value {
			t.Fatalf("round trip %d=%d err=%v", value, decoded, err)
		}
	}
	if _, err := NewTimestamp(MaxPhysical+1, 0); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("physical overflow=%v", err)
	}
	if _, err := NextAfter(Timestamp(math.MaxUint64)); !errors.Is(err, ErrTimestampExhausted) {
		t.Fatalf("next max=%v", err)
	}
}

func TestClockTieObserveRegressionAndLogicalOverflow(t *testing.T) {
	physical := clock.NewMockAt(time.UnixMilli(1000))
	hlc, err := NewClock(physical, 0)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := hlc.Now()
	second, _ := hlc.Now()
	if first.Physical() != 1000 || first.Logical() != 0 || second != first+1 {
		t.Fatalf("tie %d %d", first, second)
	}
	hlc.Observe(Timestamp(1001<<LogicalBits | 17))
	third, _ := hlc.Now()
	if third != Timestamp(1001<<LogicalBits|18) {
		t.Fatalf("observe=%d", third)
	}
	hlc.Observe(Timestamp(2000<<LogicalBits | uint64(MaxLogical)))
	overflow, _ := hlc.Now()
	if overflow != Timestamp(2001<<LogicalBits) {
		t.Fatalf("logical overflow=%d", overflow)
	}
	hlc.Observe(Timestamp(math.MaxUint64))
	if _, err := hlc.Now(); !errors.Is(err, ErrTimestampExhausted) {
		t.Fatalf("max=%v", err)
	}
}

type scriptedClock struct {
	clock.Clock
	values []time.Time
	offset int
}

func (s *scriptedClock) Now() time.Time {
	value := s.values[s.offset]
	if s.offset+1 < len(s.values) {
		s.offset++
	}
	return value
}

func TestClockPhysicalRegressionSequence(t *testing.T) {
	base := clock.NewMockAt(time.UnixMilli(1000))
	physical := &scriptedClock{Clock: base, values: []time.Time{time.UnixMilli(1000), time.UnixMilli(1001), time.UnixMilli(995), time.UnixMilli(996), time.UnixMilli(1002)}}
	hlc, err := NewClock(physical, 0)
	if err != nil {
		t.Fatal(err)
	}
	var previous Timestamp
	for range physical.values {
		current, nowErr := hlc.Now()
		if nowErr != nil {
			t.Fatal(nowErr)
		}
		if current <= previous {
			t.Fatalf("regression %d <= %d", current, previous)
		}
		previous = current
	}
}

func TestClockLargePhysicalJump(t *testing.T) {
	physical := clock.NewMockAt(time.UnixMilli(1000))
	hlc, err := NewClock(physical, 0)
	if err != nil {
		t.Fatal(err)
	}
	before, err := hlc.Now()
	if err != nil {
		t.Fatal(err)
	}
	physical.Advance((24*time.Hour + 37*time.Millisecond))
	after, err := hlc.Now()
	if err != nil {
		t.Fatal(err)
	}
	if after <= before || after.Physical() != uint64(1000+(24*time.Hour+37*time.Millisecond)/time.Millisecond) || after.Logical() != 0 {
		t.Fatalf("large jump before=%d after=%d", before, after)
	}
}
