// Package mvcc defines replicated version timestamps and historical-read
// primitives. Transaction intents and isolation protocols are deliberately
// outside this package's Phase 5 boundary.
package mvcc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rivetdb/rivetdb/internal/clock"
)

const (
	LogicalBits = 16
	MaxLogical  = uint16(1<<LogicalBits - 1)
	MaxPhysical = uint64(1<<48 - 1)
)

var (
	ErrInvalidTimestamp   = errors.New("mvcc: invalid timestamp")
	ErrTimestampExhausted = errors.New("mvcc: timestamp exhausted")
)

// Timestamp is a totally ordered HLC value: 48 physical-millisecond bits and
// 16 logical bits. Unsigned integer order is timestamp order.
type Timestamp uint64

func NewTimestamp(physical uint64, logical uint16) (Timestamp, error) {
	if physical > MaxPhysical {
		return 0, ErrInvalidTimestamp
	}
	return Timestamp(physical<<LogicalBits | uint64(logical)), nil
}

func (t Timestamp) Physical() uint64 { return uint64(t) >> LogicalBits }
func (t Timestamp) Logical() uint16  { return uint16(t) } //nolint:gosec // intentional low 16-bit field
func (t Timestamp) Encode(dst []byte) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(t))
}

func DecodeTimestamp(encoded []byte) (Timestamp, error) {
	if len(encoded) != 8 {
		return 0, ErrInvalidTimestamp
	}
	return Timestamp(binary.BigEndian.Uint64(encoded)), nil
}

// Clock is a range-scoped hybrid logical clock. Observe advances its floor
// without generating an event; Now returns a new timestamp strictly above the
// floor and the current physical reading.
type Clock struct {
	mu       sync.Mutex
	physical clock.Clock
	last     Timestamp
}

func NewClock(physical clock.Clock, floor Timestamp) (*Clock, error) {
	if physical == nil {
		return nil, ErrInvalidTimestamp
	}
	return &Clock{physical: physical, last: floor}, nil
}

func (c *Clock) Observe(value Timestamp) {
	c.mu.Lock()
	if value > c.last {
		c.last = value
	}
	c.mu.Unlock()
}

func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *Clock) Now() (Timestamp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.physical.Now()
	millis := now.UnixMilli()
	if millis < 0 || uint64(millis) > MaxPhysical {
		return 0, fmt.Errorf("%w: physical time %s", ErrTimestampExhausted, now.Format(time.RFC3339Nano))
	}
	physical := uint64(millis)
	lastPhysical, lastLogical := c.last.Physical(), c.last.Logical()
	if physical > lastPhysical {
		c.last = Timestamp(physical << LogicalBits)
		if c.last == 0 {
			c.last = 1
		}
		return c.last, nil
	}
	if lastLogical != MaxLogical {
		c.last++
		return c.last, nil
	}
	if lastPhysical == MaxPhysical {
		return 0, ErrTimestampExhausted
	}
	c.last = Timestamp((lastPhysical + 1) << LogicalBits)
	return c.last, nil
}

func NextAfter(value Timestamp) (Timestamp, error) {
	if value == Timestamp(^uint64(0)) {
		return 0, ErrTimestampExhausted
	}
	return value + 1, nil
}
