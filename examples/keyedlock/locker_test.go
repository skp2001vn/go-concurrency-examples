package keyedlock

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLockSerializesSameKey verifies that work for the same business key does
// not overlap.
func TestLockSerializesSameKey(t *testing.T) {
	locker := New()
	firstUnlock := mustLock(t, locker, "order-1")

	started := make(chan struct{})
	acquired := make(chan Unlock, 1)
	go func() {
		close(started)
		unlock, err := locker.Lock("order-1")
		if err != nil {
			t.Errorf("lock same key: %v", err)
			return
		}
		acquired <- unlock
	}()
	<-started

	select {
	case unlock := <-acquired:
		unlock()
		t.Fatal("same-key lock was acquired before first lock was released")
	default:
	}

	firstUnlock()

	secondUnlock := receiveUnlock(t, acquired)
	secondUnlock()
	if got := locker.ActiveKeys(); got != 0 {
		t.Fatalf("active keys = %d, want 0", got)
	}
}

// TestLockAllowsDifferentKeysConcurrently verifies that unrelated business
// keys can proceed at the same time.
func TestLockAllowsDifferentKeysConcurrently(t *testing.T) {
	locker := New()
	firstUnlock := mustLock(t, locker, "order-1")
	defer firstUnlock()

	acquired := make(chan Unlock, 1)
	go func() {
		unlock, err := locker.Lock("order-2")
		if err != nil {
			t.Errorf("lock different key: %v", err)
			return
		}
		acquired <- unlock
	}()

	secondUnlock := receiveUnlock(t, acquired)
	secondUnlock()
}

// TestConcurrentSameKeyUpdates verifies that many callers updating one key
// observe serialized access.
func TestConcurrentSameKeyUpdates(t *testing.T) {
	const callers = 50

	locker := New()
	var running atomic.Int32
	var maxRunning atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	started := make(chan struct{}, callers)
	release := make(chan struct{})

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			unlock, err := locker.Lock("sku-1")
			if err != nil {
				errs <- err
				return
			}
			defer unlock()

			now := running.Add(1)
			recordMax(&maxRunning, now)
			started <- struct{}{}
			<-release
			running.Add(-1)
		}()
	}

	for i := 0; i < callers; i++ {
		waitForStarted(t, started)
		if got := running.Load(); got != 1 {
			t.Fatalf("running same-key sections = %d, want 1", got)
		}
		release <- struct{}{}
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("lock error: %v", err)
		}
	}
	if got := maxRunning.Load(); got != 1 {
		t.Fatalf("max running for same key = %d, want 1", got)
	}
	if got := locker.ActiveKeys(); got != 0 {
		t.Fatalf("active keys = %d, want 0", got)
	}
}

// TestUnlockIsIdempotent verifies that callers can safely defer or repeat the
// returned release function without corrupting the registry.
func TestUnlockIsIdempotent(t *testing.T) {
	locker := New()
	unlock := mustLock(t, locker, "cart-1")

	unlock()
	unlock()

	if got := locker.ActiveKeys(); got != 0 {
		t.Fatalf("active keys = %d, want 0", got)
	}
}

// TestLockRejectsInvalidUse verifies that callers get clear errors for missing
// keys or uninitialized lockers.
func TestLockRejectsInvalidUse(t *testing.T) {
	locker := New()
	if unlock, err := locker.Lock(""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key error = %v, want %v", err, ErrEmptyKey)
	} else if unlock != nil {
		t.Fatalf("unlock = %v, want nil", unlock)
	}

	var zero Locker
	if unlock, err := zero.Lock("sku-1"); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("zero locker error = %v, want %v", err, ErrUninitialized)
	} else if unlock != nil {
		t.Fatalf("unlock = %v, want nil", unlock)
	}

	var missing *Locker
	if unlock, err := missing.Lock("sku-1"); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("nil locker error = %v, want %v", err, ErrUninitialized)
	} else if unlock != nil {
		t.Fatalf("unlock = %v, want nil", unlock)
	}
}

func mustLock(t *testing.T, locker *Locker, key string) Unlock {
	t.Helper()

	unlock, err := locker.Lock(key)
	if err != nil {
		t.Fatalf("lock %q: %v", key, err)
	}

	return unlock
}

func receiveUnlock(t *testing.T, acquired <-chan Unlock) Unlock {
	t.Helper()

	select {
	case unlock := <-acquired:
		return unlock
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for lock")
		return nil
	}
}

func waitForStarted(t *testing.T, started <-chan struct{}) {
	t.Helper()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for critical section")
	}
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
