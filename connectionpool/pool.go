package connectionpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrTooManyWaiters is returned when all connections are busy and the
	// configured waiter limit has already been reached.
	ErrTooManyWaiters = errors.New("too many waiting goroutines")

	// ErrNilConnection is returned when a nil connection is released.
	ErrNilConnection = errors.New("connection is nil")

	// ErrPoolFull is returned when releasing would exceed the pool capacity.
	ErrPoolFull = errors.New("connection pool is full")
)

type waiter struct {
	ready chan *Connection
}

// Pool is a fixed-size, FIFO connection pool.
//
// It lets callers acquire a reusable connection, wait with context cancellation
// or timeout when the pool is empty, and cap the number of goroutines allowed to
// wait for a connection at the same time.
type Pool struct {
	mu         sync.Mutex
	idle       []*Connection
	waiters    []*waiter
	capacity   int
	maxWaiters int
}

// Stats is a snapshot of pool state intended for examples, tests, and demos.
type Stats struct {
	Capacity   int
	Idle       int
	Waiting    int
	MaxWaiters int
}

// New creates a pool with size reusable connections and at most maxWaiters
// goroutines waiting for a busy pool.
func New(size int, maxWaiters int) (*Pool, error) {
	if size <= 0 {
		return nil, fmt.Errorf("size must be positive: %d", size)
	}
	if maxWaiters < 0 {
		return nil, fmt.Errorf("max waiters must be non-negative: %d", maxWaiters)
	}

	idle := make([]*Connection, 0, size)
	for id := 0; id < size; id++ {
		idle = append(idle, &Connection{id: id})
	}

	return &Pool{
		idle:       idle,
		capacity:   size,
		maxWaiters: maxWaiters,
	}, nil
}

// Acquire returns an idle connection or waits until one is released.
//
// If the context is canceled before a connection is available, Acquire returns
// the context error. If too many goroutines are already waiting, it returns
// ErrTooManyWaiters.
func (p *Pool) Acquire(ctx context.Context) (*Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	p.mu.Lock()
	if len(p.idle) > 0 && len(p.waiters) == 0 {
		conn := p.popIdleLocked()
		p.mu.Unlock()
		return conn, nil
	}
	if len(p.waiters) >= p.maxWaiters {
		p.mu.Unlock()
		return nil, ErrTooManyWaiters
	}

	w := &waiter{ready: make(chan *Connection, 1)}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()

	select {
	case conn := <-w.ready:
		return conn, nil
	case <-ctx.Done():
		p.mu.Lock()
		removed := p.removeWaiterLocked(w)
		p.mu.Unlock()
		if removed {
			return nil, ctx.Err()
		}

		return <-w.ready, nil
	}
}

// AcquireTimeout waits up to timeout for a connection.
func (p *Pool) AcquireTimeout(timeout time.Duration) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return p.Acquire(ctx)
}

// Release returns conn to the pool and wakes the oldest waiter, if any.
func (p *Pool) Release(conn *Connection) error {
	if conn == nil {
		return ErrNilConnection
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.waiters) > 0 {
		w := p.waiters[0]
		p.waiters[0] = nil
		p.waiters = p.waiters[1:]
		w.ready <- conn
		return nil
	}

	if len(p.idle) >= p.capacity {
		return ErrPoolFull
	}

	p.idle = append(p.idle, conn)
	return nil
}

// Stats returns a consistent snapshot of the pool state.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return Stats{
		Capacity:   p.capacity,
		Idle:       len(p.idle),
		Waiting:    len(p.waiters),
		MaxWaiters: p.maxWaiters,
	}
}

func (p *Pool) popIdleLocked() *Connection {
	conn := p.idle[0]
	p.idle[0] = nil
	p.idle = p.idle[1:]
	return conn
}

func (p *Pool) removeWaiterLocked(target *waiter) bool {
	for i, w := range p.waiters {
		if w != target {
			continue
		}

		copy(p.waiters[i:], p.waiters[i+1:])
		p.waiters[len(p.waiters)-1] = nil
		p.waiters = p.waiters[:len(p.waiters)-1]
		return true
	}

	return false
}
