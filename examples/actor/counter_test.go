package actor

import (
	"errors"
	"sync"
	"testing"
)

// TestCounterAddsAndReportsValue verifies that callers can update and read the
// actor-owned counter value.
func TestCounterAddsAndReportsValue(t *testing.T) {
	counter := NewCounter(10)
	defer counter.Close()

	got, err := counter.Add(5)
	if err != nil {
		t.Fatalf("add value: %v", err)
	}
	if got != 15 {
		t.Fatalf("updated value = %d, want 15", got)
	}

	got, err = counter.Value()
	if err != nil {
		t.Fatalf("read value: %v", err)
	}
	if got != 15 {
		t.Fatalf("current value = %d, want 15", got)
	}
}

// TestCounterSerializesConcurrentAdds verifies that concurrent callers do not
// lose updates.
func TestCounterSerializesConcurrentAdds(t *testing.T) {
	const callers = 100
	counter := NewCounter(0)
	defer counter.Close()

	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, err := counter.Add(1)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("add error: %v", err)
		}
	}

	got, err := counter.Value()
	if err != nil {
		t.Fatalf("read value: %v", err)
	}
	if got != callers {
		t.Fatalf("current value = %d, want %d", got, callers)
	}
}

// TestCounterHandlesNegativeDelta verifies that commands can represent normal
// business changes in either direction.
func TestCounterHandlesNegativeDelta(t *testing.T) {
	counter := NewCounter(10)
	defer counter.Close()

	got, err := counter.Add(-3)
	if err != nil {
		t.Fatalf("add negative delta: %v", err)
	}
	if got != 7 {
		t.Fatalf("updated value = %d, want 7", got)
	}
}

// TestCloseRejectsFutureCommands verifies that the actor stops accepting work
// after it is closed.
func TestCloseRejectsFutureCommands(t *testing.T) {
	counter := NewCounter(0)
	counter.Close()
	counter.Close()

	if _, err := counter.Add(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("add error = %v, want %v", err, ErrClosed)
	}
	if _, err := counter.Value(); !errors.Is(err, ErrClosed) {
		t.Fatalf("value error = %v, want %v", err, ErrClosed)
	}
}

// TestNilCounterReturnsUninitialized verifies that callers get a clear error
// for a missing actor.
func TestNilCounterReturnsUninitialized(t *testing.T) {
	var counter *Counter

	if _, err := counter.Add(1); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("add error = %v, want %v", err, ErrUninitialized)
	}
	if _, err := counter.Value(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("value error = %v, want %v", err, ErrUninitialized)
	}
	counter.Close()
}

// TestZeroValueCounterReturnsUninitialized verifies that callers get a clear
// error instead of blocking forever on an uninitialized actor.
func TestZeroValueCounterReturnsUninitialized(t *testing.T) {
	var counter Counter

	if _, err := counter.Add(1); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("add error = %v, want %v", err, ErrUninitialized)
	}
	if _, err := counter.Value(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("value error = %v, want %v", err, ErrUninitialized)
	}
	counter.Close()
}
