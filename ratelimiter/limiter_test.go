package ratelimiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTryAllowConsumesBurst verifies that callers can use the initial burst
// immediately and fail fast once it is exhausted.
func TestTryAllowConsumesBurst(t *testing.T) {
	limiter := mustLimiter(t, time.Hour, 2)
	defer limiter.Stop()

	if ok := limiter.TryAllow(); !ok {
		t.Fatal("expected first permit to be available")
	}
	if ok := limiter.TryAllow(); !ok {
		t.Fatal("expected second permit to be available")
	}
	if ok := limiter.TryAllow(); ok {
		t.Fatal("expected burst to be exhausted")
	}
}

// TestWaitAllowsWorkAfterRefill verifies that a waiting caller proceeds after
// the limiter replenishes capacity.
func TestWaitAllowsWorkAfterRefill(t *testing.T) {
	limiter := mustLimiter(t, 10*time.Millisecond, 1)
	defer limiter.Stop()

	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("consume initial permit: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- limiter.Wait(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait after refill: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for refill")
	}
}

// TestWaitReturnsContextError verifies that callers can stop waiting when their
// request is canceled before another permit is available.
func TestWaitReturnsContextError(t *testing.T) {
	limiter := mustLimiter(t, time.Hour, 1)
	defer limiter.Stop()

	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("consume initial permit: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := limiter.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want %v", err, context.Canceled)
	}
}

// TestStopWakesWaitingCaller verifies that stopped limiters do not leave
// callers blocked waiting for future capacity.
func TestStopWakesWaitingCaller(t *testing.T) {
	limiter := mustLimiter(t, time.Hour, 1)
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatalf("consume initial permit: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- limiter.Wait(context.Background())
	}()

	limiter.Stop()

	err := receiveError(t, done)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("wait error = %v, want %v", err, ErrStopped)
	}
}

// TestStopPreventsNewPermits verifies that callers cannot use a limiter after
// it has been stopped.
func TestStopPreventsNewPermits(t *testing.T) {
	limiter := mustLimiter(t, time.Hour, 1)
	limiter.Stop()
	limiter.Stop()

	if ok := limiter.TryAllow(); ok {
		t.Fatal("expected stopped limiter to reject try allow")
	}

	err := limiter.Wait(context.Background())
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("wait error = %v, want %v", err, ErrStopped)
	}
}

// TestWaitRejectsUninitializedLimiter verifies that callers get a clear error
// instead of blocking forever on an uninitialized limiter.
func TestWaitRejectsUninitializedLimiter(t *testing.T) {
	var limiter Limiter

	err := limiter.Wait(context.Background())
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("wait error = %v, want %v", err, ErrUninitialized)
	}
	if ok := limiter.TryAllow(); ok {
		t.Fatal("expected try allow to fail for uninitialized limiter")
	}
	limiter.Stop()
}

// TestNewRejectsInvalidConfig verifies that callers cannot create a limiter
// with an unusable rate or burst.
func TestNewRejectsInvalidConfig(t *testing.T) {
	if limiter, err := New(0, 1); err == nil {
		limiter.Stop()
		t.Fatal("expected interval error, got nil")
	}

	if limiter, err := New(time.Second, 0); err == nil {
		limiter.Stop()
		t.Fatal("expected burst error, got nil")
	}
}

func mustLimiter(t *testing.T, interval time.Duration, burst int) *Limiter {
	t.Helper()

	limiter, err := New(interval, burst)
	if err != nil {
		t.Fatalf("new limiter: %v", err)
	}

	return limiter
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
