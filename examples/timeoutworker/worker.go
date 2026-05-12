package timeoutworker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNilWork means Run was called without an operation to execute.
var ErrNilWork = errors.New("timeoutworker: work is nil")

// Work represents one operation guarded by a deadline.
//
// Work should return nil after a successful operation. If the deadline expires
// or the caller cancels the operation, Work should stop early when ctx is
// canceled.
type Work[T any] func(ctx context.Context) (T, error)

type result[T any] struct {
	value T
	err   error
}

// Run executes work and returns its result or the deadline error.
//
// Run starts work in its own goroutine, waits for either the work result or ctx
// cancellation, and returns whichever happens first. timeout must be positive.
// A nil context is treated as context.Background.
func Run[T any](ctx context.Context, timeout time.Duration, work Work[T]) (T, error) {
	var zero T
	if work == nil {
		return zero, ErrNilWork
	}
	if timeout <= 0 {
		return zero, fmt.Errorf("timeout must be positive: %s", timeout)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan result[T], 1)
	go func() {
		value, err := work(ctx)
		done <- result[T]{value: value, err: err}
	}()

	select {
	case result := <-done:
		return result.value, result.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}
