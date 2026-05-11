package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrUninitialized means the limiter was used before New created it.
	ErrUninitialized = errors.New("ratelimiter: limiter is not initialized")

	// ErrStopped means the limiter has been stopped and will not allow more work.
	ErrStopped = errors.New("ratelimiter: limiter is stopped")
)

// Limiter controls how often callers may proceed.
//
// A Limiter starts with burst available permits. After callers consume those
// permits, a new permit becomes available every interval configured with New.
//
// A Limiter is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Limiter.
type Limiter struct {
	tokens  chan struct{}
	stop    chan struct{}
	stopped chan struct{}

	once sync.Once
	mu   sync.Mutex
	done bool
}

// New creates a limiter that refills one permit every interval.
//
// New returns an error when interval is not positive or burst is not positive.
func New(interval time.Duration, burst int) (*Limiter, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive: %s", interval)
	}
	if burst <= 0 {
		return nil, fmt.Errorf("burst must be positive: %d", burst)
	}

	l := &Limiter{
		tokens:  make(chan struct{}, burst),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	for i := 0; i < burst; i++ {
		l.tokens <- struct{}{}
	}

	go l.refill(interval)

	return l, nil
}

// Wait blocks until one permit is available for the caller.
//
// Wait returns nil after consuming one permit. If no permit is available, it
// waits until the limiter refills, ctx is canceled, or Stop is called. A nil
// context is treated as context.Background.
func (l *Limiter) Wait(ctx context.Context) error {
	if err := l.ready(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if l.isStopped() {
		return ErrStopped
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.stopped:
		return ErrStopped
	case <-l.tokens:
		if l.isStopped() {
			return ErrStopped
		}
		return nil
	}
}

// TryAllow consumes one permit only if capacity is available right now.
//
// TryAllow returns false when no permit is available, the limiter is stopped,
// or the limiter is not initialized.
func (l *Limiter) TryAllow() bool {
	if l == nil || l.tokens == nil || l.stop == nil || l.stopped == nil {
		return false
	}
	if l.isStopped() {
		return false
	}

	select {
	case <-l.tokens:
		return !l.isStopped()
	default:
		return false
	}
}

// Stop releases limiter resources and wakes callers waiting in Wait.
//
// Stop is safe to call more than once. Calling Stop on a nil or uninitialized
// limiter has no effect.
func (l *Limiter) Stop() {
	if l == nil || l.stop == nil || l.stopped == nil {
		return
	}

	l.once.Do(func() {
		l.mu.Lock()
		l.done = true
		l.mu.Unlock()

		close(l.stopped)
		close(l.stop)
	})
}

func (l *Limiter) refill(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case l.tokens <- struct{}{}:
			default:
			}
		case <-l.stop:
			return
		}
	}
}

func (l *Limiter) ready() error {
	if l == nil || l.tokens == nil || l.stop == nil || l.stopped == nil {
		return ErrUninitialized
	}

	return nil
}

func (l *Limiter) isStopped() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.done
}
