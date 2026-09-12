package advisor

import (
	"context"
	"fmt"
	"sync"
)

type MockModel struct {
	mu       sync.Mutex
	Response []byte
	Err      error
	Block    bool
	Panic    bool
	Calls    int
	Requests [][]byte
	Started  chan struct{}
}

func (m *MockModel) Analyze(ctx context.Context, request []byte) ([]byte, error) {
	m.mu.Lock()
	m.Calls++
	m.Requests = append(m.Requests, append([]byte(nil), request...))
	block, doPanic, response, err := m.Block, m.Panic, append([]byte(nil), m.Response...), m.Err
	m.mu.Unlock()
	if m.Started != nil {
		select {
		case m.Started <- struct{}{}:
		default:
		}
	}
	if doPanic {
		panic("mock advisor failure")
	}
	if block {
		<-ctx.Done()
		return nil, fmt.Errorf("mock advisor wait: %w", ctx.Err())
	}
	return response, err
}
