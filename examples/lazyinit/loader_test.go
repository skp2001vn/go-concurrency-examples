package lazyinit

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGetRunsLoadOnce verifies that repeated callers share one initialized
// resource.
func TestGetRunsLoadOnce(t *testing.T) {
	var calls atomic.Int32
	loader := mustLoader(t, func() (string, error) {
		calls.Add(1)
		return "config", nil
	})

	for i := 0; i < 3; i++ {
		value, err := loader.Get()
		if err != nil {
			t.Fatalf("get value: %v", err)
		}
		if value != "config" {
			t.Fatalf("value = %q, want %q", value, "config")
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("load calls = %d, want 1", got)
	}
}

// TestConcurrentGetRunsLoadOnce verifies that concurrent callers wait for the
// same initialization attempt instead of duplicating work.
func TestConcurrentGetRunsLoadOnce(t *testing.T) {
	const callers = 20

	start := make(chan struct{})
	var calls atomic.Int32
	loader := mustLoader(t, func() (int, error) {
		calls.Add(1)
		<-start
		return 42, nil
	})

	var wg sync.WaitGroup
	values := make(chan int, callers)
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			value, err := loader.Get()
			values <- value
			errs <- err
		}()
	}

	waitForLoadCall(t, &calls)
	close(start)
	wg.Wait()
	close(values)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("get error: %v", err)
		}
	}
	for value := range values {
		if value != 42 {
			t.Fatalf("value = %d, want 42", value)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load calls = %d, want 1", got)
	}
}

// TestGetCachesLoadError verifies that initialization errors are returned to
// later callers without retrying the setup.
func TestGetCachesLoadError(t *testing.T) {
	loadErr := errors.New("open config")
	var calls atomic.Int32
	loader := mustLoader(t, func() (int, error) {
		calls.Add(1)
		return 0, loadErr
	})

	for i := 0; i < 2; i++ {
		value, err := loader.Get()
		if !errors.Is(err, loadErr) {
			t.Fatalf("get error = %v, want %v", err, loadErr)
		}
		if value != 0 {
			t.Fatalf("value = %d, want zero value", value)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("load calls = %d, want 1", got)
	}
}

// TestNewRejectsNilLoad verifies that callers cannot create a loader without
// initialization work.
func TestNewRejectsNilLoad(t *testing.T) {
	loader, err := New[int](nil)
	if !errors.Is(err, ErrNilLoad) {
		t.Fatalf("new error = %v, want %v", err, ErrNilLoad)
	}
	if loader != nil {
		t.Fatalf("loader = %v, want nil", loader)
	}
}

// TestGetRejectsUninitializedLoader verifies that callers get a clear error
// instead of a panic from a zero-value loader.
func TestGetRejectsUninitializedLoader(t *testing.T) {
	var loader Loader[int]

	value, err := loader.Get()
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("get error = %v, want %v", err, ErrUninitialized)
	}
	if value != 0 {
		t.Fatalf("value = %d, want zero value", value)
	}
}

// TestGetRejectsNilLoader verifies that callers get a clear error for a
// missing loader.
func TestGetRejectsNilLoader(t *testing.T) {
	var loader *Loader[int]

	value, err := loader.Get()
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("get error = %v, want %v", err, ErrUninitialized)
	}
	if value != 0 {
		t.Fatalf("value = %d, want zero value", value)
	}
}

func mustLoader[T any](t *testing.T, load func() (T, error)) *Loader[T] {
	t.Helper()

	loader, err := New(load)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}

	return loader
}

func waitForLoadCall(t *testing.T, calls *atomic.Int32) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if calls.Load() > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for load call")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
