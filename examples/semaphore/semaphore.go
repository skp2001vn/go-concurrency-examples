package semaphore

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrUninitialized means the semaphore was used before New created it.
	ErrUninitialized = errors.New("semaphore is not initialized")

	// ErrReleaseWithoutAcquire means a caller released more slots than it
	// acquired.
	ErrReleaseWithoutAcquire = errors.New("release without matching acquire")
)

// Semaphore limits how many callers may hold a slot at the same time.
//
// Use a Semaphore when work must stay below a fixed concurrency limit, such as
// outbound API requests, file processing, or database activity. Acquire blocks
// until capacity is available or the caller's context is canceled.
//
// A Semaphore is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Semaphore.
type Semaphore struct {
	permits chan struct{}
}

// New creates a semaphore with room for limit concurrent holders.
//
// New returns an error when limit is not positive.
func New(limit int) (*Semaphore, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive: %d", limit)
	}

	return &Semaphore{
		permits: make(chan struct{}, limit),
	}, nil
}

// Acquire waits until one slot is available for the caller.
//
// Acquire returns right away when the semaphore has free capacity. Otherwise,
// it waits until another caller releases a slot or ctx is canceled. A nil
// context is treated as context.Background.
func (s *Semaphore) Acquire(ctx context.Context) error {
	permits := s.channel()
	if permits == nil {
		return ErrUninitialized
	}
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case permits <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire takes one slot only if capacity is available right now.
//
// TryAcquire returns false when the semaphore is full or not initialized.
func (s *Semaphore) TryAcquire() bool {
	permits := s.channel()
	if permits == nil {
		return false
	}

	select {
	case permits <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release returns one slot so another caller can proceed.
//
// Release returns ErrReleaseWithoutAcquire when no caller currently holds a
// slot. Callers should release each successful acquisition at most once.
func (s *Semaphore) Release() error {
	permits := s.channel()
	if permits == nil {
		return ErrUninitialized
	}

	select {
	case <-permits:
		return nil
	default:
		return ErrReleaseWithoutAcquire
	}
}

// Capacity reports the maximum number of callers that may hold a slot at once.
func (s *Semaphore) Capacity() int {
	return cap(s.channel())
}

// InUse reports how many slots are currently held.
func (s *Semaphore) InUse() int {
	return len(s.channel())
}

func (s *Semaphore) channel() chan struct{} {
	if s == nil {
		return nil
	}

	return s.permits
}
