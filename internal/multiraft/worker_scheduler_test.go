package multiraft

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

type blockingWorkHandler struct {
	started   chan struct{}
	release   chan struct{}
	coldDone  chan struct{}
	startOnce sync.Once
	coldOnce  sync.Once
}

func (h *blockingWorkHandler) Tick(rangeID RangeID) ([]Envelope, error) {
	if rangeID == 10 {
		h.startOnce.Do(func() { close(h.started) })
		<-h.release
	} else {
		h.coldOnce.Do(func() { close(h.coldDone) })
	}
	return nil, nil
}

func (h *blockingWorkHandler) Step(Envelope) ([]Envelope, error) { return nil, nil }

func TestFixedWorkerSchedulerSlowRangeDoesNotStarveColdRange(t *testing.T) {
	transport, err := NewTransport(32)
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := NewWorkerScheduler(transport, 2, 128)
	if err != nil {
		t.Fatal(err)
	}
	handler := &blockingWorkHandler{started: make(chan struct{}), release: make(chan struct{}), coldDone: make(chan struct{})}
	if addErr := scheduler.addHandler(1, handler); addErr != nil {
		t.Fatal(addErr)
	}
	hotDone, err := scheduler.SubmitTick(1, 10)
	if err != nil {
		t.Fatal(err)
	}
	<-handler.started
	for range 50 {
		if _, submitErr := scheduler.SubmitTick(1, 10); submitErr != nil {
			t.Fatal(submitErr)
		}
	}
	coldDone, err := scheduler.SubmitTick(1, 11)
	if err != nil {
		t.Fatal(err)
	}
	<-handler.coldDone
	if result := <-coldDone; result != nil {
		t.Fatal(result)
	}
	if scheduler.WorkerCount() != 2 || scheduler.Pending() != 51 {
		t.Fatalf("workers=%d pending=%d", scheduler.WorkerCount(), scheduler.Pending())
	}
	close(handler.release)
	if result := <-hotDone; result != nil {
		t.Fatal(result)
	}
	scheduler.Close()
	if stats := scheduler.Stats(); stats.Completed != 52 || stats.Failed != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestFixedWorkerSchedulerBoundsAndStopsAdmission(t *testing.T) {
	transport, _ := NewTransport(8)
	scheduler, _ := NewWorkerScheduler(transport, 1, 1)
	handler := &blockingWorkHandler{started: make(chan struct{}), release: make(chan struct{}), coldDone: make(chan struct{})}
	if err := scheduler.addHandler(1, handler); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.SubmitTick(1, 10); err != nil {
		t.Fatal(err)
	}
	<-handler.started
	if _, err := scheduler.SubmitTick(1, 11); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("bound=%v", err)
	}
	close(handler.release)
	scheduler.Close()
	if _, err := scheduler.SubmitTick(1, 11); !errors.Is(err, ErrSchedulerStopped) {
		t.Fatalf("stopped=%v", err)
	}
}

func TestNodeRuntimeUsesFixedSharedWorkers(t *testing.T) {
	defer testutil.NoLeaks(t)()
	bootstrap := Bootstrap{Generation: 1, Nodes: []raft.NodeID{1}, ReplicationFactor: 1, Ranges: []RangeDescriptor{
		{RangeID: 10, Generation: 1, StartKey: KeyBound{Unbounded: true}, EndKey: KeyBound{Unbounded: true},
			Replicas: []ReplicaDescriptor{{ReplicaID: 101, NodeID: 1}}},
	}}
	runtime, err := OpenRuntime(NodeOptions{NodeID: 1, Directory: filepath.Join(t.TempDir(), "node"), Bootstrap: &bootstrap}, 32)
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		done, submitErr := runtime.Scheduler.SubmitTick(1, 10)
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		if resultErr := <-done; resultErr != nil {
			t.Fatal(resultErr)
		}
	}
	if runtime.Scheduler.WorkerCount() != 4 || rangeStatus(t, runtime.Node, 10).Raft.Role != raft.Leader {
		t.Fatalf("workers=%d status=%+v", runtime.Scheduler.WorkerCount(), runtime.Node.Status())
	}
	if closeErr := runtime.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
}

var _ workHandler = (*blockingWorkHandler)(nil)
