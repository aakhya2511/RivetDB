package multiraft

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rivetdb/rivetdb/internal/mvcc"
	"github.com/rivetdb/rivetdb/internal/raft"
	"github.com/rivetdb/rivetdb/internal/replicatedrange"
)

type RouterOptions struct {
	Catalog     *Catalog
	Scheduler   *Scheduler
	Transport   *Transport
	MaxAttempts int
	MaxWork     int
}

type Router struct {
	catalog     *Catalog
	scheduler   *Scheduler
	transport   *Transport
	maxAttempts int
	maxWork     int
	mu          sync.Mutex
	leaders     map[RangeID]raft.NodeID
}

func NewRouter(options RouterOptions) (*Router, error) {
	if options.Catalog == nil || options.Scheduler == nil || options.Transport == nil {
		return nil, ErrInvalidCatalog
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 8
	}
	if options.MaxWork == 0 {
		options.MaxWork = 100_000
	}
	if options.MaxAttempts < 1 || options.MaxWork < 1 {
		return nil, ErrResourceLimit
	}
	return &Router{catalog: options.Catalog, scheduler: options.Scheduler, transport: options.Transport,
		maxAttempts: options.MaxAttempts, maxWork: options.MaxWork, leaders: make(map[RangeID]raft.NodeID)}, nil
}

func (r *Router) Route(key []byte) (Route, error) {
	descriptor, err := r.catalog.Lookup(key)
	if err != nil {
		return Route{}, err
	}
	return Route{RangeID: descriptor.RangeID, Generation: descriptor.Generation, Key: append([]byte(nil), key...)}, nil
}

func (r *Router) Put(ctx context.Context, key, value []byte) error {
	return r.mutate(ctx, key, replicatedrange.Command{Type: replicatedrange.CommandPut, Key: key, Value: value})
}

func (r *Router) Delete(ctx context.Context, key []byte) error {
	return r.mutate(ctx, key, replicatedrange.Command{Type: replicatedrange.CommandDelete, Key: key})
}

func (r *Router) PutMVCC(ctx context.Context, key, value []byte) (mvcc.Timestamp, error) {
	return r.mutateMVCC(ctx, key, replicatedrange.Command{Type: replicatedrange.CommandPut, Key: key, Value: value})
}

func (r *Router) DeleteMVCC(ctx context.Context, key []byte) (mvcc.Timestamp, error) {
	return r.mutateMVCC(ctx, key, replicatedrange.Command{Type: replicatedrange.CommandDelete, Key: key})
}

func (r *Router) mutateMVCC(ctx context.Context, key []byte, command replicatedrange.Command) (mvcc.Timestamp, error) {
	route, err := r.Route(key)
	if err != nil {
		return 0, err
	}
	descriptor, err := r.catalog.LookupByID(route.RangeID)
	if err != nil {
		return 0, err
	}
	candidates := r.candidates(descriptor)
	nodes := r.scheduler.Nodes()
	attempts := 0
	for _, nodeID := range candidates {
		if attempts >= r.maxAttempts {
			break
		}
		if err := ctx.Err(); err != nil {
			return 0, fmt.Errorf("during routed MVCC proposal: %w", err)
		}
		node := nodes[nodeID]
		if node == nil {
			continue
		}
		attempts++
		timestamp, pending, outbound, proposeErr := node.ProposeMVCC(ctx, route, command)
		if proposeErr != nil {
			var notLeader *NotLeaderError
			if errors.As(proposeErr, &notLeader) {
				if notLeader.Leader != 0 {
					r.RecordLeader(route.RangeID, notLeader.Leader)
				}
				continue
			}
			return 0, proposeErr
		}
		for _, envelope := range outbound {
			if err := r.transport.Send(envelope); err != nil {
				return 0, err
			}
		}
		for work := 0; work < r.maxWork; work++ {
			done, resultErr := pending.poll()
			if done {
				if resultErr == nil {
					r.RecordLeader(route.RangeID, nodeID)
				}
				return timestamp, resultErr
			}
			delivered, schedulerErr := r.scheduler.DeliverNext()
			if schedulerErr == nil && !delivered {
				_, schedulerErr = r.scheduler.TickNext()
			}
			if schedulerErr != nil && !errors.Is(schedulerErr, ErrUnknownRange) && !errors.Is(schedulerErr, ErrNodeStopped) {
				return 0, schedulerErr
			}
		}
		return 0, fmt.Errorf("%w: MVCC proposal exceeded bounded scheduler work", ErrLeaderUnknown)
	}
	return 0, ErrLeaderUnknown
}

func (r *Router) mutate(ctx context.Context, key []byte, command replicatedrange.Command) error {
	route, err := r.Route(key)
	if err != nil {
		return err
	}
	encoded, err := replicatedrange.EncodeCommand(command)
	if err != nil {
		return fmt.Errorf("encode routed command: %w", err)
	}
	return r.Propose(ctx, route, encoded)
}

func (r *Router) Propose(ctx context.Context, route Route, encoded []byte) error {
	if ctx == nil {
		return ErrInvalidCatalog
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("before routed proposal: %w", err)
	}
	descriptor, err := r.catalog.LookupByID(route.RangeID)
	if err != nil {
		return err
	}
	if route.Generation != descriptor.Generation {
		return &StaleRangeError{Current: descriptor}
	}
	if !descriptor.Contains(route.Key) {
		return ErrWrongRangeKey
	}
	candidates := r.candidates(descriptor)
	nodes := r.scheduler.Nodes()
	attempts := 0
	for _, nodeID := range candidates {
		if attempts >= r.maxAttempts {
			break
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("during routed proposal: %w", err)
		}
		node := nodes[nodeID]
		if node == nil {
			continue
		}
		attempts++
		pending, outbound, proposeErr := node.Propose(ctx, route, encoded)
		if proposeErr != nil {
			var notLeader *NotLeaderError
			if errors.As(proposeErr, &notLeader) {
				if notLeader.Leader != 0 {
					r.RecordLeader(route.RangeID, notLeader.Leader)
				}
				continue
			}
			return proposeErr
		}
		for _, envelope := range outbound {
			if err := r.transport.Send(envelope); err != nil {
				return err
			}
		}
		for work := 0; work < r.maxWork; work++ {
			done, resultErr := pending.poll()
			if done {
				if resultErr == nil {
					r.RecordLeader(route.RangeID, nodeID)
				}
				return resultErr
			}
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("wait for routed proposal: %w", err)
			}
			delivered, schedulerErr := r.scheduler.DeliverNext()
			if schedulerErr == nil && !delivered {
				_, schedulerErr = r.scheduler.TickNext()
			}
			if schedulerErr != nil {
				if errors.Is(schedulerErr, ErrUnknownRange) || errors.Is(schedulerErr, ErrNodeStopped) {
					continue
				}
				return schedulerErr
			}
		}
		return fmt.Errorf("%w: proposal exceeded bounded scheduler work", ErrLeaderUnknown)
	}
	return ErrLeaderUnknown
}

func (r *Router) candidates(descriptor RangeDescriptor) []raft.NodeID {
	r.mu.Lock()
	hint := r.leaders[descriptor.RangeID]
	r.mu.Unlock()
	result := make([]raft.NodeID, 0, len(descriptor.Replicas))
	if hint != 0 {
		result = append(result, hint)
	}
	for _, replica := range descriptor.Replicas {
		if replica.NodeID != hint {
			result = append(result, replica.NodeID)
		}
	}
	return result
}

func (r *Router) RecordLeader(rangeID RangeID, nodeID raft.NodeID) {
	r.mu.Lock()
	r.leaders[rangeID] = nodeID
	r.mu.Unlock()
}

func (r *Router) LeaderHint(rangeID RangeID) raft.NodeID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaders[rangeID]
}
