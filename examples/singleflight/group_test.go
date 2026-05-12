package singleflight

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestDoRunsFunctionOnceForConcurrentDuplicateKey verifies that duplicate
// callers share one in-flight result.
func TestDoRunsFunctionOnceForConcurrentDuplicateKey(t *testing.T) {
	group := NewGroup()
	started := make(chan struct{})
	release := make(chan struct{})
	results := make(chan doResult, 3)

	var runs atomic.Int32
	fn := func() (any, error) {
		runs.Add(1)
		close(started)
		<-release
		return "profile", nil
	}

	go func() {
		value, err, shared := group.Do("user:1", fn)
		results <- doResult{value: value, err: err, shared: shared}
	}()
	waitForClosed(t, started)

	ready := make(chan struct{}, 2)
	group.mu.Lock()
	for i := 0; i < 2; i++ {
		go func() {
			ready <- struct{}{}
			value, err, shared := group.Do("user:1", fn)
			results <- doResult{value: value, err: err, shared: shared}
		}()
	}
	waitForSignals(t, ready, 2)
	group.mu.Unlock()
	waitForWaiters(t, group, "user:1", 2)
	close(release)

	var sharedCount int
	for i := 0; i < 3; i++ {
		result := receiveResult(t, results)
		if result.err != nil {
			t.Fatalf("do error: %v", result.err)
		}
		if result.value != "profile" {
			t.Fatalf("value = %v, want profile", result.value)
		}
		if result.shared {
			sharedCount++
		}
	}

	if got := runs.Load(); got != 1 {
		t.Fatalf("function runs = %d, want 1", got)
	}
	if sharedCount != 2 {
		t.Fatalf("shared results = %d, want 2", sharedCount)
	}
}

// TestDoSharesErrors verifies that duplicate callers receive the same failure
// from one in-flight call.
func TestDoSharesErrors(t *testing.T) {
	group := NewGroup()
	callErr := errors.New("load failed")
	started := make(chan struct{})
	release := make(chan struct{})
	results := make(chan doResult, 2)

	var runs atomic.Int32
	fn := func() (any, error) {
		runs.Add(1)
		close(started)
		<-release
		return nil, callErr
	}

	go func() {
		value, err, shared := group.Do("config", fn)
		results <- doResult{value: value, err: err, shared: shared}
	}()
	waitForClosed(t, started)

	ready := make(chan struct{}, 1)
	group.mu.Lock()
	go func() {
		ready <- struct{}{}
		value, err, shared := group.Do("config", fn)
		results <- doResult{value: value, err: err, shared: shared}
	}()
	waitForSignals(t, ready, 1)
	group.mu.Unlock()
	waitForWaiters(t, group, "config", 1)

	close(release)

	for i := 0; i < 2; i++ {
		result := receiveResult(t, results)
		if result.value != nil {
			t.Fatalf("value = %v, want nil", result.value)
		}
		if !errors.Is(result.err, callErr) {
			t.Fatalf("error = %v, want %v", result.err, callErr)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("function runs = %d, want 1", got)
	}
}

// TestDoRunsDifferentKeysIndependently verifies that calls for different keys do
// not block behind one another.
func TestDoRunsDifferentKeysIndependently(t *testing.T) {
	group := NewGroup()
	started := make(chan string, 2)
	release := make(chan struct{})
	results := make(chan doResult, 2)

	run := func(key string) {
		value, err, shared := group.Do(key, func() (any, error) {
			started <- key
			<-release
			return key, nil
		})
		results <- doResult{value: value, err: err, shared: shared}
	}

	go run("user:1")
	go run("user:2")

	waitForStartedKeys(t, started, "user:1", "user:2")
	close(release)

	for i := 0; i < 2; i++ {
		result := receiveResult(t, results)
		if result.err != nil {
			t.Fatalf("do error: %v", result.err)
		}
		if result.shared {
			t.Fatal("different keys should not share results")
		}
	}
}

// TestDoRunsAgainAfterPreviousCallFinishes verifies that finished calls are not
// cached.
func TestDoRunsAgainAfterPreviousCallFinishes(t *testing.T) {
	group := NewGroup()
	var runs atomic.Int32

	for i := 0; i < 2; i++ {
		value, err, shared := group.Do("user:1", func() (any, error) {
			return runs.Add(1), nil
		})
		if err != nil {
			t.Fatalf("do call %d: %v", i+1, err)
		}
		if shared {
			t.Fatalf("call %d shared result, want fresh work", i+1)
		}
		if value != int32(i+1) {
			t.Fatalf("value = %v, want %d", value, i+1)
		}
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("function runs = %d, want 2", got)
	}
}

// TestDoRejectsInvalidInputs verifies that bad caller input does not run work.
func TestDoRejectsInvalidInputs(t *testing.T) {
	group := NewGroup()
	var zero Group
	var nilGroup *Group

	tests := []struct {
		name string
		run  func() (any, error, bool)
		want error
	}{
		{name: "nil group", run: func() (any, error, bool) {
			return nilGroup.Do("key", func() (any, error) { return "value", nil })
		}, want: ErrUninitialized},
		{name: "zero group", run: func() (any, error, bool) {
			return zero.Do("key", func() (any, error) { return "value", nil })
		}, want: ErrUninitialized},
		{name: "empty key", run: func() (any, error, bool) {
			return group.Do("", func() (any, error) { return "value", nil })
		}, want: ErrEmptyKey},
		{name: "nil function", run: func() (any, error, bool) {
			return group.Do("key", nil)
		}, want: ErrNilFunc},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err, shared := tt.run()
			if value != nil {
				t.Fatalf("value = %v, want nil", value)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if shared {
				t.Fatal("shared = true, want false")
			}
		})
	}
}

type doResult struct {
	value  any
	err    error
	shared bool
}

func receiveResult(t *testing.T, results <-chan doResult) doResult {
	t.Helper()

	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for result")
		return doResult{}
	}
}

func waitForClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for call to start")
	}
}

func waitForSignals(t *testing.T, signals <-chan struct{}, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-signals:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for signal %d", i+1)
		}
	}
}

func waitForWaiters(t *testing.T, group *Group, key string, count int) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		group.mu.Lock()
		c := group.calls[key]
		var waiters int
		if c != nil {
			waiters = c.waiters
		}
		group.mu.Unlock()

		if waiters == count {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d waiters on key %q", count, key)
}

func waitForStartedKeys(t *testing.T, started <-chan string, want ...string) {
	t.Helper()

	seen := make(map[string]bool)
	for len(seen) < len(want) {
		select {
		case key := <-started:
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for started keys: seen=%v", seen)
		}
	}

	for _, key := range want {
		if !seen[key] {
			t.Fatalf("key %q did not start: seen=%v", key, seen)
		}
	}
}
