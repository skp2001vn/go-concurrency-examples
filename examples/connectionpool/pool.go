package connectionpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrTooManyWaiters means too many callers are already waiting for a
	// connection.
	ErrTooManyWaiters = errors.New("too many waiting goroutines")

	// ErrNilConnection means a caller tried to return no connection.
	ErrNilConnection = errors.New("connection is nil")

	// ErrPoolFull means the pool already has all of its connections back.
	ErrPoolFull = errors.New("connection pool is full")
)

type waiter struct {
	ready chan *Connection
}

// Pool lets callers borrow and return a limited number of connections.
//
// Use a Pool to protect a database, external service, or other limited resource
// from too many simultaneous users. When all connections are borrowed, callers
// can wait their turn or receive a clear error if the wait list is already full.
//
// A Pool is safe for concurrent use by multiple goroutines.
type Pool struct {
	mu         sync.Mutex
	idle       []*Connection
	waiters    []*waiter
	capacity   int
	maxWaiters int
}

// Stats reports current connection availability and demand.
type Stats struct {
	// Capacity is the total number of connections callers can borrow.
	Capacity int

	// Idle is the number of connections available right now.
	Idle int

	// Waiting is the number of callers waiting for a connection.
	Waiting int

	// MaxWaiters is the maximum number of callers allowed to wait.
	MaxWaiters int
}

// New creates a pool with size connections and room for maxWaiters waiting
// callers.
//
// New returns an error when size is not positive or maxWaiters is negative.
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

// Acquire borrows a connection from the pool.
//
// Acquire returns right away when a connection is available. If all connections
// are already borrowed, it waits until one is returned, ctx is canceled, or the
// wait list is full. A full wait list returns ErrTooManyWaiters. A nil context
// is treated as context.Background.
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

// AcquireTimeout borrows a connection but gives up after timeout.
//
// If no connection becomes available before timeout, AcquireTimeout returns
// context.DeadlineExceeded.
func (p *Pool) AcquireTimeout(timeout time.Duration) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return p.Acquire(ctx)
}

// Release returns a borrowed connection so another caller can use it.
//
// Callers should release only connections acquired from the same Pool and should
// release each acquired connection at most once.
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

// Stats reports pool availability and demand at this moment.
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
