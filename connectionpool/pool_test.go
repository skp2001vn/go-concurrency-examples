package connectionpool

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestAcquireAndReleaseConnection verifies that a released connection can be
// acquired again by a later caller.
func TestAcquireAndReleaseConnection(t *testing.T) {
	pool := mustPool(t, 1, 1)

	first, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire first connection: %v", err)
	}

	if err := pool.Release(first); err != nil {
		t.Fatalf("release first connection: %v", err)
	}

	second, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire released connection: %v", err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("connection ID mismatch: first=%d second=%d", first.ID(), second.ID())
	}
}

// TestAcquireReturnsContextErrorWhenTimedOut verifies that acquiring from an
// empty pool returns the context deadline error when no connection is released.
func TestAcquireReturnsContextErrorWhenTimedOut(t *testing.T) {
	pool := mustPool(t, 1, 1)
	held, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire held connection: %v", err)
	}
	defer func() {
		if releaseErr := pool.Release(held); releaseErr != nil {
			t.Fatalf("release held connection: %v", releaseErr)
		}
	}()

	conn, err := pool.AcquireTimeout(20 * time.Millisecond)
	if conn != nil {
		t.Fatalf("expected no connection, got %v", conn)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

// TestWaitingAcquireSucceedsAfterConnectionIsReleased verifies that a blocked
// acquire receives the released connection instead of timing out.
func TestWaitingAcquireSucceedsAfterConnectionIsReleased(t *testing.T) {
	pool := mustPool(t, 1, 1)
	held, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire held connection: %v", err)
	}

	acquired := make(chan *Connection, 1)
	errs := make(chan error, 1)
	go func() {
		conn, acquireErr := pool.AcquireTimeout(time.Second)
		if acquireErr != nil {
			errs <- acquireErr
			return
		}
		acquired <- conn
	}()

	waitUntil(t, func() bool {
		return pool.Stats().Waiting == 1
	})

	if err := pool.Release(held); err != nil {
		t.Fatalf("release held connection: %v", err)
	}

	select {
	case err := <-errs:
		t.Fatalf("waiting acquire failed: %v", err)
	case conn := <-acquired:
		if conn.ID() != held.ID() {
			t.Fatalf("connection ID mismatch: held=%d acquired=%d", held.ID(), conn.ID())
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire result")
	}
}

// TestRejectsAcquireWhenTooManyGoroutinesAreWaiting verifies that the pool
// rejects new waiters once maxWaiters has been reached.
func TestRejectsAcquireWhenTooManyGoroutinesAreWaiting(t *testing.T) {
	pool := mustPool(t, 1, 1)
	held, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire held connection: %v", err)
	}
	defer func() {
		if releaseErr := pool.Release(held); releaseErr != nil {
			t.Fatalf("release held connection: %v", releaseErr)
		}
	}()

	errs := make(chan error, 1)
	go func() {
		_, acquireErr := pool.AcquireTimeout(time.Second)
		errs <- acquireErr
	}()

	waitUntil(t, func() bool {
		return pool.Stats().Waiting == 1
	})

	conn, err := pool.AcquireTimeout(100 * time.Millisecond)
	if conn != nil {
		t.Fatalf("expected no connection, got %v", conn)
	}
	if !errors.Is(err, ErrTooManyWaiters) {
		t.Fatalf("expected ErrTooManyWaiters, got %v", err)
	}
}

// TestReleaseWakesWaitersInFIFOOrder verifies that released connections are
// handed to waiting goroutines in the order they started waiting.
func TestReleaseWakesWaitersInFIFOOrder(t *testing.T) {
	pool := mustPool(t, 1, 2)
	held, err := pool.AcquireTimeout(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("acquire held connection: %v", err)
	}

	results := make(chan acquireResult, 2)
	startWaiter := func(name string) {
		go func() {
			conn, acquireErr := pool.AcquireTimeout(time.Second)
			results <- acquireResult{name: name, conn: conn, err: acquireErr}
		}()
	}

	startWaiter("first")
	waitUntil(t, func() bool {
		return pool.Stats().Waiting == 1
	})
	startWaiter("second")
	waitUntil(t, func() bool {
		return pool.Stats().Waiting == 2
	})

	if err := pool.Release(held); err != nil {
		t.Fatalf("release held connection: %v", err)
	}

	first := receiveResult(t, results)
	if first.err != nil {
		t.Fatalf("first waiter failed: %v", first.err)
	}
	if first.name != "first" {
		t.Fatalf("expected first waiter to wake first, got %q", first.name)
	}

	if err := pool.Release(first.conn); err != nil {
		t.Fatalf("release first waiter connection: %v", err)
	}

	second := receiveResult(t, results)
	if second.err != nil {
		t.Fatalf("second waiter failed: %v", second.err)
	}
	if second.name != "second" {
		t.Fatalf("expected second waiter to wake second, got %q", second.name)
	}
	if err := pool.Release(second.conn); err != nil {
		t.Fatalf("release second waiter connection: %v", err)
	}
}

type acquireResult struct {
	name string
	conn *Connection
	err  error
}

func mustPool(t *testing.T, size int, maxWaiters int) *Pool {
	t.Helper()

	pool, err := New(size, maxWaiters)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	return pool
}

func receiveResult(t *testing.T, results <-chan acquireResult) acquireResult {
	t.Helper()

	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire result")
		return acquireResult{}
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}
