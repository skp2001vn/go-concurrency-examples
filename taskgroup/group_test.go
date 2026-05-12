package taskgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunCompletesSuccessfulTasks verifies that a group succeeds after every
// related task finishes without error.
func TestRunCompletesSuccessfulTasks(t *testing.T) {
	var completed atomic.Int32

	err := Run(context.Background(),
		func(context.Context) error {
			completed.Add(1)
			return nil
		},
		func(context.Context) error {
			completed.Add(1)
			return nil
		},
	)

	if err != nil {
		t.Fatalf("run tasks: %v", err)
	}
	if got := completed.Load(); got != 2 {
		t.Fatalf("completed tasks = %d, want 2", got)
	}
}

// TestRunReturnsFirstFailure verifies that the first task failure is returned
// to the caller.
func TestRunReturnsFirstFailure(t *testing.T) {
	taskErr := errors.New("load permissions")

	err := Run(context.Background(),
		func(context.Context) error {
			return taskErr
		},
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)

	if !errors.Is(err, taskErr) {
		t.Fatalf("run error = %v, want %v", err, taskErr)
	}
}

// TestRunCancelsRemainingTasks verifies that one failed task asks related work
// to stop early.
func TestRunCancelsRemainingTasks(t *testing.T) {
	taskErr := errors.New("load profile")
	canceled := make(chan error, 1)

	err := Run(context.Background(),
		func(context.Context) error {
			return taskErr
		},
		func(ctx context.Context) error {
			<-ctx.Done()
			canceled <- ctx.Err()
			return ctx.Err()
		},
	)

	if !errors.Is(err, taskErr) {
		t.Fatalf("run error = %v, want %v", err, taskErr)
	}

	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("task context error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task cancellation")
	}
}

// TestRunReturnsCallerCancellation verifies that caller cancellation is
// returned when no task fails first.
func TestRunReturnsCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	<-started
	cancel()

	err := receiveError(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want %v", err, context.Canceled)
	}
}

// TestRunRecordsNilTask verifies that an empty task fails the group instead of
// crashing the caller.
func TestRunRecordsNilTask(t *testing.T) {
	err := Run(context.Background(), nil)
	if !errors.Is(err, ErrNilTask) {
		t.Fatalf("run error = %v, want %v", err, ErrNilTask)
	}
}

// TestRunHandlesNoTasks verifies that an empty group succeeds unless the caller
// context has already been canceled.
func TestRunHandlesNoTasks(t *testing.T) {
	if err := Run(context.Background()); err != nil {
		t.Fatalf("run empty group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run canceled empty group error = %v, want %v", err, context.Canceled)
	}
}

// TestRunWaitsForStartedTasks verifies that Run does not return until active
// tasks have observed cancellation and finished.
func TestRunWaitsForStartedTasks(t *testing.T) {
	taskErr := errors.New("request failed")
	canceled := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- Run(context.Background(),
			func(context.Context) error {
				return taskErr
			},
			func(ctx context.Context) error {
				<-ctx.Done()
				close(canceled)
				<-release
				close(finished)
				return ctx.Err()
			},
		)
	}()

	select {
	case err := <-done:
		t.Fatalf("run returned before active task finished: %v", err)
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for active task cancellation")
	}

	select {
	case err := <-done:
		t.Fatalf("run returned before active task was released: %v", err)
	default:
	}

	close(release)

	err := receiveError(t, done)
	if !errors.Is(err, taskErr) {
		t.Fatalf("run error = %v, want %v", err, taskErr)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for active task to finish")
	}
}

func receiveError(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}
