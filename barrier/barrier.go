package barrier

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUninitialized means the barrier was used before New created it.
	ErrUninitialized = errors.New("barrier: barrier is not initialized")

	// ErrClosed means the barrier was closed before a full group arrived.
	ErrClosed = errors.New("barrier: barrier is closed")
)

// Barrier waits until a fixed number of callers reach the same phase.
//
// Wait blocks until parties callers have arrived. The last caller to arrive
// releases the group and resets the barrier so it can be reused for the next
// phase.
//
// A Barrier is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Barrier.
type Barrier struct {
	mu         sync.Mutex
	cond       *sync.Cond
	parties    int
	arrived    int
	generation int
	closed     bool
}

// New creates a reusable barrier for parties callers.
//
// New returns an error when parties is not positive.
func New(parties int) (*Barrier, error) {
	if parties <= 0 {
		return nil, fmt.Errorf("parties must be positive: %d", parties)
	}

	b := &Barrier{
		parties: parties,
	}
	b.cond = sync.NewCond(&b.mu)

	return b, nil
}

// Wait blocks until all parties callers reach the barrier.
//
// Wait returns nil after the current phase is complete. It returns ErrClosed
// when Close wakes the caller before enough parties arrive.
func (b *Barrier) Wait() error {
	if err := b.ready(); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrClosed
	}

	generation := b.generation
	b.arrived++
	if b.arrived == b.parties {
		b.arrived = 0
		b.generation++
		b.cond.Broadcast()
		return nil
	}

	for generation == b.generation && !b.closed {
		b.cond.Wait()
	}
	if generation != b.generation {
		return nil
	}

	return ErrClosed
}

// Close wakes callers waiting at the barrier and prevents future waits.
//
// Close is safe to call more than once. Callers already released by a completed
// phase still return nil from Wait.
func (b *Barrier) Close() {
	if b == nil || b.cond == nil {
		return
	}

	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.cond.Broadcast()
	}
	b.mu.Unlock()
}

func (b *Barrier) ready() error {
	if b == nil || b.cond == nil || b.parties <= 0 {
		return ErrUninitialized
	}

	return nil
}
