package multiraft

import (
	"context"
	"errors"
)

// Runtime is the shared-service container for one physical Node. Production
// transport I/O remains external; the deterministic Transport holds outbound
// and delivered envelopes while Scheduler drives every local group fairly.
type Runtime struct {
	Node      *Node
	Transport *Transport
	Scheduler *WorkerScheduler
}

func OpenRuntime(options NodeOptions, maxPendingMessages int) (*Runtime, error) {
	node, err := OpenNode(options)
	if err != nil {
		return nil, err
	}
	transport, err := NewTransport(maxPendingMessages)
	if err != nil {
		return nil, errors.Join(err, node.Close(context.Background()))
	}
	scheduler, err := NewWorkerScheduler(transport, 4, maxPendingMessages)
	if err != nil {
		transport.Stop()
		return nil, errors.Join(err, node.Close(context.Background()))
	}
	if err := scheduler.AddNode(node); err != nil {
		scheduler.Close()
		transport.Stop()
		return nil, errors.Join(err, node.Close(context.Background()))
	}
	return &Runtime{Node: node, Transport: transport, Scheduler: scheduler}, nil
}

func (r *Runtime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.Scheduler.Close()
	r.Transport.Stop()
	return r.Node.Close(ctx)
}
