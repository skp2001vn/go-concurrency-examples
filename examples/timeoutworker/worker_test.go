package timeoutworker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRunReturnsWorkResult verifies that a fast operation returns its value.
func TestRunReturnsWorkResult(t *testing.T) {
	value, err := Run(context.Background(), time.Second, func(context.Context) (string, error) {
		return "profile", nil
	})
	if err != nil {
		t.Fatalf("run work: %v", err)
	}
	if value != "profile" {
		t.Fatalf("value = %q, want %q", value, "profile")
	}
}

// TestRunReturnsWorkError verifies that a fast operation can return its own
// failure.
func TestRunReturnsWorkError(t *testing.T) {
	workErr := errors.New("fetch failed")

	_, err := Run(context.Background(), time.Second, func(context.Context) (string, error) {
		return "", workErr
	})
	if !errors.Is(err, workErr) {
		t.Fatalf("run error = %v, want %v", err, workErr)
	}
}

// TestRunReturnsDeadlineExceeded verifies that callers receive a timeout when
// work does not finish before the deadline.
func TestRunReturnsDeadlineExceeded(t *testing.T) {
	_, err := Run(context.Background(), 10*time.Millisecond, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run error = %v, want %v", err, context.DeadlineExceeded)
	}
}

// TestRunReturnsWhenWorkIgnoresCancellation verifies that a timed-out caller
// does not wait for work that has not returned yet.
func TestRunReturnsWhenWorkIgnoresCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan runResult[string], 1)

	go func() {
		value, err := Run(context.Background(), 10*time.Millisecond, func(context.Context) (string, error) {
			close(started)
			<-release
			return "late", nil
		})
		done <- runResult[string]{value: value, err: err}
	}()

	<-started
	result := receiveRunResult(t, done)
	close(release)

	if !errors.Is(result.err, context.DeadlineExceeded) {
		t.Fatalf("run error = %v, want %v", result.err, context.DeadlineExceeded)
	}
	if result.value != "" {
		t.Fatalf("value = %q, want zero value", result.value)
	}
}

// TestRunReturnsCallerCancellation verifies that parent cancellation wins when
// it happens before the timeout.
func TestRunReturnsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan runResult[int], 1)

	go func() {
		value, err := Run(ctx, time.Second, func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		})
		done <- runResult[int]{value: value, err: err}
	}()

	<-started
	cancel()

	result := receiveRunResult(t, done)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("run error = %v, want %v", result.err, context.Canceled)
	}
}

// TestRunTreatsNilContextAsBackground verifies that callers can omit a parent
// context for simple work.
func TestRunTreatsNilContextAsBackground(t *testing.T) {
	value, err := Run(nil, time.Second, func(context.Context) (int, error) {
		return 7, nil
	})
	if err != nil {
		t.Fatalf("run work: %v", err)
	}
	if value != 7 {
		t.Fatalf("value = %d, want 7", value)
	}
}

// TestRunRejectsNilWork verifies that callers get a clear error for a missing
// operation.
func TestRunRejectsNilWork(t *testing.T) {
	_, err := Run[int](context.Background(), time.Second, nil)
	if !errors.Is(err, ErrNilWork) {
		t.Fatalf("run error = %v, want %v", err, ErrNilWork)
	}
}

// TestRunRejectsInvalidTimeout verifies that callers cannot create a deadline
// guard with no usable time budget.
func TestRunRejectsInvalidTimeout(t *testing.T) {
	_, err := Run(context.Background(), 0, func(context.Context) (int, error) {
		return 0, nil
	})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

type runResult[T any] struct {
	value T
	err   error
}

func receiveRunResult[T any](t *testing.T, done <-chan runResult[T]) runResult[T] {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run result")
		return runResult[T]{}
	}
}
