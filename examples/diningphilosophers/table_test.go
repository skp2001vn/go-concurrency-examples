package diningphilosophers

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRunCompletesAllRounds verifies that every worker can finish all rounds
// without deadlocking.
func TestRunCompletesAllRounds(t *testing.T) {
	const (
		philosophers = 5
		rounds       = 20
	)

	results, err := Run(context.Background(), philosophers, rounds)
	if err != nil {
		t.Fatalf("run philosophers: %v", err)
	}
	assertResults(t, results, philosophers, rounds)
}

// TestRunHandlesZeroRounds verifies that callers can request no work and still
// receive one result per worker.
func TestRunHandlesZeroRounds(t *testing.T) {
	results, err := Run(context.Background(), 3, 0)
	if err != nil {
		t.Fatalf("run zero rounds: %v", err)
	}
	assertResults(t, results, 3, 0)
}

// TestRunReturnsCancellation verifies that callers can stop waiting when the
// context is already canceled.
func TestRunReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := Run(ctx, 5, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want %v", err, context.Canceled)
	}
	if len(results) != 5 {
		t.Fatalf("results length = %d, want 5", len(results))
	}
}

// TestRunRejectsInvalidInputs verifies that unusable table settings fail before
// any work starts.
func TestRunRejectsInvalidInputs(t *testing.T) {
	if results, err := Run(context.Background(), 1, 1); err == nil {
		t.Fatal("expected philosopher count error, got nil")
	} else if results != nil {
		t.Fatalf("results = %v, want nil", results)
	}

	if results, err := Run(context.Background(), 2, -1); err == nil {
		t.Fatal("expected rounds error, got nil")
	} else if results != nil {
		t.Fatalf("results = %v, want nil", results)
	}
}

// TestRunDoesNotHangUnderContention verifies that heavy resource contention
// completes within a short guard timeout.
func TestRunDoesNotHangUnderContention(t *testing.T) {
	done := make(chan runResult, 1)
	go func() {
		results, err := Run(context.Background(), 10, 50)
		done <- runResult{results: results, err: err}
	}()

	select {
	case result := <-done:
		if result.err != nil {
			t.Fatalf("run philosophers: %v", result.err)
		}
		assertResults(t, result.results, 10, 50)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for philosophers to finish")
	}
}

type runResult struct {
	results []Result
	err     error
}

func assertResults(t *testing.T, results []Result, philosophers int, rounds int) {
	t.Helper()

	if len(results) != philosophers {
		t.Fatalf("results length = %d, want %d", len(results), philosophers)
	}
	for i, result := range results {
		if result.Philosopher != i {
			t.Fatalf("result philosopher at index %d = %d, want %d", i, result.Philosopher, i)
		}
		if result.Rounds != rounds {
			t.Fatalf("philosopher %d rounds = %d, want %d", i, result.Rounds, rounds)
		}
	}
}
