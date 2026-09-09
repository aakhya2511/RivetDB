package clock

import "time"

// System returns the Clock backed by the operating system clock. It is the
// implementation used by running nodes.
func System() Clock { return systemClock{} }

// systemClock is a stateless adapter over the time package.
type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Since(t time.Time) time.Duration { return time.Since(t) }

func (systemClock) NewTimer(d time.Duration) Timer { return systemTimer{time.NewTimer(d)} }

func (systemClock) NewTicker(d time.Duration) Ticker { return systemTicker{time.NewTicker(d)} }

func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

type systemTimer struct{ t *time.Timer }

func (t systemTimer) C() <-chan time.Time        { return t.t.C }
func (t systemTimer) Stop() bool                 { return t.t.Stop() }
func (t systemTimer) Reset(d time.Duration) bool { return t.t.Reset(d) }

type systemTicker struct{ t *time.Ticker }

func (t systemTicker) C() <-chan time.Time   { return t.t.C }
func (t systemTicker) Stop()                 { t.t.Stop() }
func (t systemTicker) Reset(d time.Duration) { t.t.Reset(d) }
