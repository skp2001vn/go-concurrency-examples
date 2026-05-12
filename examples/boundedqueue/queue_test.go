package boundedqueue

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestEnqueueDequeuePreservesFIFO verifies that callers receive items in the
// same order they were added.
func TestEnqueueDequeuePreservesFIFO(t *testing.T) {
	q := mustQueue[int](t, 3)

	if err := q.Enqueue(1); err != nil {
		t.Fatalf("enqueue first item: %v", err)
	}
	if err := q.Enqueue(2); err != nil {
		t.Fatalf("enqueue second item: %v", err)
	}
	if err := q.Enqueue(3); err != nil {
		t.Fatalf("enqueue third item: %v", err)
	}

	got := []int{
		mustDequeue(t, q),
		mustDequeue(t, q),
		mustDequeue(t, q),
	}
	want := []int{1, 2, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dequeued items = %v, want %v", got, want)
	}
}

// TestEnqueueWaitsUntilSpaceIsAvailable verifies that a full queue lets a
// producer continue after a consumer removes one item.
func TestEnqueueWaitsUntilSpaceIsAvailable(t *testing.T) {
	q := mustQueue[int](t, 1)
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("enqueue initial item: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- q.Enqueue(2)
	}()
	<-started

	select {
	case err := <-done:
		t.Fatalf("enqueue finished while queue was full: %v", err)
	default:
	}

	if got := mustDequeue(t, q); got != 1 {
		t.Fatalf("dequeued item = %d, want 1", got)
	}
	if err := receiveError(t, done); err != nil {
		t.Fatalf("enqueue after space was available: %v", err)
	}
	if got := mustDequeue(t, q); got != 2 {
		t.Fatalf("dequeued item = %d, want 2", got)
	}
}

// TestDequeueWaitsUntilItemIsAvailable verifies that an empty queue lets a
// consumer continue after a producer adds one item.
func TestDequeueWaitsUntilItemIsAvailable(t *testing.T) {
	q := mustQueue[string](t, 1)

	started := make(chan struct{})
	done := make(chan dequeueResult[string], 1)
	go func() {
		close(started)
		value, err := q.Dequeue()
		done <- dequeueResult[string]{value: value, err: err}
	}()
	<-started

	select {
	case result := <-done:
		t.Fatalf("dequeue finished while queue was empty: value=%q err=%v", result.value, result.err)
	default:
	}

	if err := q.Enqueue("job"); err != nil {
		t.Fatalf("enqueue item: %v", err)
	}
	result := receiveDequeueResult(t, done)
	if result.err != nil {
		t.Fatalf("dequeue after item was available: %v", result.err)
	}
	if result.value != "job" {
		t.Fatalf("dequeued item = %q, want %q", result.value, "job")
	}
}

// TestCloseWakesBlockedProducer verifies that closing a full queue releases a
// producer waiting for space.
func TestCloseWakesBlockedProducer(t *testing.T) {
	q := mustQueue[int](t, 1)
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("enqueue initial item: %v", err)
	}

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- q.Enqueue(2)
	}()
	<-started

	q.Close()

	err := receiveError(t, done)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue error = %v, want %v", err, ErrClosed)
	}
}

// TestCloseWakesBlockedConsumer verifies that closing an empty queue releases a
// consumer waiting for work.
func TestCloseWakesBlockedConsumer(t *testing.T) {
	q := mustQueue[int](t, 1)

	started := make(chan struct{})
	done := make(chan dequeueResult[int], 1)
	go func() {
		close(started)
		value, err := q.Dequeue()
		done <- dequeueResult[int]{value: value, err: err}
	}()
	<-started

	q.Close()

	result := receiveDequeueResult(t, done)
	if !errors.Is(result.err, ErrClosed) {
		t.Fatalf("dequeue error = %v, want %v", result.err, ErrClosed)
	}
	if result.value != 0 {
		t.Fatalf("dequeued value = %d, want zero value", result.value)
	}
}

// TestCloseAllowsBufferedItemsToDrain verifies that already queued work remains
// available after the queue is closed.
func TestCloseAllowsBufferedItemsToDrain(t *testing.T) {
	q := mustQueue[int](t, 2)
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("enqueue first item: %v", err)
	}
	if err := q.Enqueue(2); err != nil {
		t.Fatalf("enqueue second item: %v", err)
	}

	q.Close()

	if got := mustDequeue(t, q); got != 1 {
		t.Fatalf("first item = %d, want 1", got)
	}
	if got := mustDequeue(t, q); got != 2 {
		t.Fatalf("second item = %d, want 2", got)
	}

	_, err := q.Dequeue()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("dequeue error = %v, want %v", err, ErrClosed)
	}
}

// TestEnqueueAfterCloseReturnsError verifies that closed queues reject new
// work.
func TestEnqueueAfterCloseReturnsError(t *testing.T) {
	q := mustQueue[int](t, 1)
	q.Close()
	q.Close()

	err := q.Enqueue(1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("enqueue error = %v, want %v", err, ErrClosed)
	}
}

// TestLenReportsBufferedItems verifies that callers can inspect the current
// queue depth.
func TestLenReportsBufferedItems(t *testing.T) {
	q := mustQueue[int](t, 2)
	if got := q.Len(); got != 0 {
		t.Fatalf("initial len = %d, want 0", got)
	}
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("enqueue item: %v", err)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("len after enqueue = %d, want 1", got)
	}
	_ = mustDequeue(t, q)
	if got := q.Len(); got != 0 {
		t.Fatalf("len after dequeue = %d, want 0", got)
	}
}

// TestQueueRejectsUninitializedUse verifies that callers get a clear error
// instead of blocking forever on an uninitialized queue.
func TestQueueRejectsUninitializedUse(t *testing.T) {
	var q Queue[int]

	if err := q.Enqueue(1); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("enqueue error = %v, want %v", err, ErrUninitialized)
	}
	if _, err := q.Dequeue(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("dequeue error = %v, want %v", err, ErrUninitialized)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("len = %d, want 0", got)
	}
	q.Close()
}

// TestNewRejectsInvalidCapacity verifies that callers cannot create a queue
// with no room for work.
func TestNewRejectsInvalidCapacity(t *testing.T) {
	q, err := New[int](0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if q != nil {
		t.Fatalf("queue = %v, want nil", q)
	}
}

type dequeueResult[T any] struct {
	value T
	err   error
}

func mustQueue[T any](t *testing.T, capacity int) *Queue[T] {
	t.Helper()

	q, err := New[T](capacity)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	return q
}

func mustDequeue[T any](t *testing.T, q *Queue[T]) T {
	t.Helper()

	value, err := q.Dequeue()
	if err != nil {
		t.Fatalf("dequeue item: %v", err)
	}

	return value
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

func receiveDequeueResult[T any](t *testing.T, done <-chan dequeueResult[T]) dequeueResult[T] {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dequeue result")
		return dequeueResult[T]{}
	}
}
