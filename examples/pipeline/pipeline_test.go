package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestRunProcessesPositiveValues verifies that a full pipeline accepts valid
// values, transforms them, and preserves their order.
func TestRunProcessesPositiveValues(t *testing.T) {
	got, err := Run(context.Background(), []int{2, -1, 0, 3, 4})
	if err != nil {
		t.Fatalf("run pipeline: %v", err)
	}

	want := []int{4, 9, 16}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("results = %v, want %v", got, want)
	}
}

// TestRunHandlesEmptyBatch verifies that callers can process an empty batch
// without special-case setup.
func TestRunHandlesEmptyBatch(t *testing.T) {
	got, err := Run(context.Background(), nil)
	if err != nil {
		t.Fatalf("run empty pipeline: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("results = %v, want empty", got)
	}
}

// TestCollectReturnsCancellation verifies that a waiting caller can stop
// collection without requiring the input channel to close first.
func TestCollectReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan int)
	done := make(chan collectResult, 1)

	go func() {
		values, err := Collect(ctx, in)
		done <- collectResult{values: values, err: err}
	}()

	cancel()

	result := receiveCollectResult(t, done)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("collect error = %v, want %v", result.err, context.Canceled)
	}
	if len(result.values) != 0 {
		t.Fatalf("collected values = %v, want empty", result.values)
	}
}

// TestSourceStopsAfterCancellation verifies that cancellation closes the output
// channel even when downstream work stops reading.
func TestSourceStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := Source(ctx, []int{1, 2, 3})

	if got := receiveValue(t, out); got != 1 {
		t.Fatalf("first value = %d, want 1", got)
	}

	cancel()
	expectClosed(t, out)
}

// TestFilterClosesOutputWithInput verifies that a stage closes its output after
// all upstream values have been handled.
func TestFilterClosesOutputWithInput(t *testing.T) {
	in := make(chan int)
	close(in)

	out := Filter(context.Background(), in, func(value int) bool {
		return value > 0
	})

	expectClosed(t, out)
}

// TestMapPreservesOrder verifies that transformations are emitted in the same
// order as accepted input values.
func TestMapPreservesOrder(t *testing.T) {
	in := make(chan int)
	go func() {
		defer close(in)
		in <- 3
		in <- 5
	}()

	out := Map(context.Background(), in, func(value int) int {
		return value * 10
	})

	got, err := Collect(context.Background(), out)
	if err != nil {
		t.Fatalf("collect mapped values: %v", err)
	}

	want := []int{30, 50}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mapped values = %v, want %v", got, want)
	}
}

// TestRunReturnsCanceledContext verifies that a canceled caller receives a
// cancellation error before batch work starts.
func TestRunReturnsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := Run(ctx, []int{1, 2, 3})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v, want %v", err, context.Canceled)
	}
	if len(got) != 0 {
		t.Fatalf("results = %v, want empty", got)
	}
}

type collectResult struct {
	values []int
	err    error
}

func receiveCollectResult(t *testing.T, done <-chan collectResult) collectResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for collect result")
		return collectResult{}
	}
}

func receiveValue(t *testing.T, values <-chan int) int {
	t.Helper()

	select {
	case value, ok := <-values:
		if !ok {
			t.Fatal("channel closed before value was received")
		}
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for value")
		return 0
	}
}

func expectClosed(t *testing.T, values <-chan int) {
	t.Helper()

	select {
	case value, ok := <-values:
		if ok {
			t.Fatalf("received value %d, want closed channel", value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}
