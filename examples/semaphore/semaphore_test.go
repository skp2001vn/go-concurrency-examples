package semaphore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAcquireAndRelease verifies that a released slot becomes available to a
// later caller.
func TestAcquireAndRelease(t *testing.T) {
	sem := mustSemaphore(t, 1)

	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire first slot: %v", err)
	}
	if got := sem.InUse(); got != 1 {
		t.Fatalf("in use after first acquire = %d, want 1", got)
	}

	if err := sem.Release(); err != nil {
		t.Fatalf("release first slot: %v", err)
	}
	if got := sem.InUse(); got != 0 {
		t.Fatalf("in use after release = %d, want 0", got)
	}

	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire released slot: %v", err)
	}
	if err := sem.Release(); err != nil {
		t.Fatalf("release reacquired slot: %v", err)
	}
}

// TestAcquireWaitsUntilRelease verifies that a full semaphore wakes a waiting
// caller after another caller returns a slot.
func TestAcquireWaitsUntilRelease(t *testing.T) {
	sem := mustSemaphore(t, 1)
	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire held slot: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- sem.Acquire(context.Background())
	}()

	<-started

	select {
	case err := <-done:
		t.Fatalf("waiting acquire finished early: %v", err)
	default:
	}

	if err := sem.Release(); err != nil {
		t.Fatalf("release held slot: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiting acquire failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for acquire")
	}

	if err := sem.Release(); err != nil {
		t.Fatalf("release waiter slot: %v", err)
	}
}

// TestAcquireReturnsContextError verifies that a caller can stop waiting when
// its deadline expires.
func TestAcquireReturnsContextError(t *testing.T) {
	sem := mustSemaphore(t, 1)
	if err := sem.Acquire(context.Background()); err != nil {
		t.Fatalf("acquire held slot: %v", err)
	}
	defer func() {
		if err := sem.Release(); err != nil {
			t.Fatalf("release held slot: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := sem.Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire error = %v, want %v", err, context.DeadlineExceeded)
	}
}

// TestTryAcquireReportsFull verifies that callers can fail fast instead of
// waiting when no slot is available.
func TestTryAcquireReportsFull(t *testing.T) {
	sem := mustSemaphore(t, 1)

	if ok := sem.TryAcquire(); !ok {
		t.Fatal("expected first try acquire to succeed")
	}
	if ok := sem.TryAcquire(); ok {
		t.Fatal("expected second try acquire to fail")
	}
	if err := sem.Release(); err != nil {
		t.Fatalf("release slot: %v", err)
	}
}

// TestReleaseRejectsUnbalancedCalls verifies that callers cannot release more
// slots than they successfully acquired.
func TestReleaseRejectsUnbalancedCalls(t *testing.T) {
	sem := mustSemaphore(t, 1)

	err := sem.Release()
	if !errors.Is(err, ErrReleaseWithoutAcquire) {
		t.Fatalf("release error = %v, want %v", err, ErrReleaseWithoutAcquire)
	}
}

// TestAcquireRejectsUninitializedSemaphore verifies that callers get a clear
// error instead of blocking forever on an uninitialized semaphore.
func TestAcquireRejectsUninitializedSemaphore(t *testing.T) {
	var sem Semaphore

	err := sem.Acquire(context.Background())
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("acquire error = %v, want %v", err, ErrUninitialized)
	}
	if ok := sem.TryAcquire(); ok {
		t.Fatal("expected try acquire to fail for uninitialized semaphore")
	}
	if got := sem.Capacity(); got != 0 {
		t.Fatalf("capacity = %d, want 0", got)
	}
	if got := sem.InUse(); got != 0 {
		t.Fatalf("in use = %d, want 0", got)
	}
}

// TestSemaphoreLimitsConcurrentCallers verifies that the configured limit is
// never exceeded even when many goroutines compete for slots.
func TestSemaphoreLimitsConcurrentCallers(t *testing.T) {
	const (
		limit   = 2
		callers = 6
	)

	sem := mustSemaphore(t, limit)
	release := make(chan struct{})
	started := make(chan struct{}, callers)
	errs := make(chan error, callers)

	var wg sync.WaitGroup
	var running atomic.Int32
	var maxRunning atomic.Int32

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			if err := sem.Acquire(ctx); err != nil {
				errs <- err
				return
			}
			defer func() {
				if err := sem.Release(); err != nil {
					errs <- err
				}
			}()

			now := running.Add(1)
			recordMax(&maxRunning, now)
			started <- struct{}{}

			<-release
			running.Add(-1)
		}()
	}

	waitForStarted(t, started, limit)
	if got := maxRunning.Load(); got > limit {
		t.Fatalf("max running callers = %d, want at most %d", got, limit)
	}

	close(release)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("worker error: %v", err)
		}
	}
	if got := maxRunning.Load(); got > limit {
		t.Fatalf("max running callers = %d, want at most %d", got, limit)
	}
	if got := sem.InUse(); got != 0 {
		t.Fatalf("in use after workers = %d, want 0", got)
	}
}

// TestNewRejectsInvalidLimit verifies that callers cannot create a semaphore
// with a non-positive capacity.
func TestNewRejectsInvalidLimit(t *testing.T) {
	sem, err := New(0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if sem != nil {
		t.Fatalf("semaphore = %v, want nil", sem)
	}
}

func mustSemaphore(t *testing.T, limit int) *Semaphore {
	t.Helper()

	sem, err := New(limit)
	if err != nil {
		t.Fatalf("new semaphore: %v", err)
	}

	return sem
}

func recordMax(maxRunning *atomic.Int32, value int32) {
	for {
		current := maxRunning.Load()
		if value <= current {
			return
		}
		if maxRunning.CompareAndSwap(current, value) {
			return
		}
	}
}

func waitForStarted(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for caller %d to start", i+1)
		}
	}
}
