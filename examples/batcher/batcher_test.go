package batcher

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestBatchFlushesWhenSizeLimitIsReached verifies that callers receive a batch
// as soon as enough items arrive.
func TestBatchFlushesWhenSizeLimitIsReached(t *testing.T) {
	input := make(chan int)
	output := mustBatch(t, context.Background(), input, 3, time.Hour)

	input <- 1
	input <- 2
	input <- 3

	got := receiveBatch(t, output)
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch = %v, want %v", got, want)
	}
}

// TestBatchFlushesWhenDelayExpires verifies that callers receive a partial
// batch when the flush delay expires.
func TestBatchFlushesWhenDelayExpires(t *testing.T) {
	input := make(chan string)
	output := mustBatch(t, context.Background(), input, 3, 10*time.Millisecond)

	input <- "a"
	input <- "b"

	got := receiveBatch(t, output)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch = %v, want %v", got, want)
	}
}

// TestBatchFlushesRemainingItemsWhenInputCloses verifies that no pending items
// are lost when the producer finishes.
func TestBatchFlushesRemainingItemsWhenInputCloses(t *testing.T) {
	input := make(chan int)
	output := mustBatch(t, context.Background(), input, 3, time.Hour)

	input <- 1
	input <- 2
	close(input)

	got := receiveBatch(t, output)
	want := []int{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch = %v, want %v", got, want)
	}
	expectClosed(t, output)
}

// TestBatchEmitsMultipleBatches verifies that a stream can produce more than
// one output batch.
func TestBatchEmitsMultipleBatches(t *testing.T) {
	input := make(chan int)
	output := mustBatch(t, context.Background(), input, 2, time.Hour)

	input <- 1
	input <- 2
	if got, want := receiveBatch(t, output), []int{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first batch = %v, want %v", got, want)
	}

	input <- 3
	input <- 4
	if got, want := receiveBatch(t, output), []int{3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second batch = %v, want %v", got, want)
	}
}

// TestBatchStopsWhenContextIsCanceled verifies that cancellation closes the
// output stream without waiting for a full batch.
func TestBatchStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	input := make(chan int)
	output := mustBatch(t, ctx, input, 3, time.Hour)

	input <- 1
	cancel()

	expectClosed(t, output)
}

// TestBatchRejectsInvalidConfig verifies that callers receive clear errors for
// unusable batching settings.
func TestBatchRejectsInvalidConfig(t *testing.T) {
	if output, err := Batch[int](context.Background(), nil, 1, time.Second); !errors.Is(err, ErrNilInput) {
		t.Fatalf("nil input error = %v, want %v", err, ErrNilInput)
	} else if output != nil {
		t.Fatalf("output = %v, want nil", output)
	}

	input := make(chan int)
	if output, err := Batch(context.Background(), input, 0, time.Second); err == nil {
		t.Fatal("expected max size error, got nil")
	} else if output != nil {
		t.Fatalf("output = %v, want nil", output)
	}

	if output, err := Batch(context.Background(), input, 1, 0); err == nil {
		t.Fatal("expected max delay error, got nil")
	} else if output != nil {
		t.Fatalf("output = %v, want nil", output)
	}
}

func mustBatch[T any](t *testing.T, ctx context.Context, input <-chan T, maxSize int, maxDelay time.Duration) <-chan []T {
	t.Helper()

	output, err := Batch(ctx, input, maxSize, maxDelay)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}

	return output
}

func receiveBatch[T any](t *testing.T, output <-chan []T) []T {
	t.Helper()

	select {
	case batch, ok := <-output:
		if !ok {
			t.Fatal("output closed before batch was received")
		}
		return batch
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch")
		return nil
	}
}

func expectClosed[T any](t *testing.T, output <-chan []T) {
	t.Helper()

	select {
	case batch, ok := <-output:
		if ok {
			t.Fatalf("received batch %v, want closed output", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for output to close")
	}
}
