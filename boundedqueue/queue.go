package boundedqueue

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUninitialized means the queue was used before New created it.
	ErrUninitialized = errors.New("boundedqueue: queue is not initialized")

	// ErrClosed means the queue was closed and no more items can be exchanged.
	ErrClosed = errors.New("boundedqueue: queue is closed")
)

// Queue is a fixed-size FIFO buffer for coordinating producers and consumers.
//
// Enqueue blocks while the queue is full. Dequeue blocks while the queue is
// empty. Close wakes blocked callers and prevents future enqueue or dequeue
// operations once buffered items have been drained.
//
// A Queue is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Queue.
type Queue[T any] struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond
	items    []T
	capacity int
	closed   bool
}

// New creates a queue that can hold capacity items.
//
// New returns an error when capacity is not positive.
func New[T any](capacity int) (*Queue[T], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive: %d", capacity)
	}

	q := &Queue[T]{
		items:    make([]T, 0, capacity),
		capacity: capacity,
	}
	q.notFull = sync.NewCond(&q.mu)
	q.notEmpty = sync.NewCond(&q.mu)

	return q, nil
}

// Enqueue adds item to the back of the queue.
//
// Enqueue blocks while the queue is full. It returns ErrClosed when the queue
// is closed before the item can be added.
func (q *Queue[T]) Enqueue(item T) error {
	if err := q.ready(); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == q.capacity && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		return ErrClosed
	}

	q.items = append(q.items, item)
	q.notEmpty.Signal()
	return nil
}

// Dequeue removes and returns the oldest item from the queue.
//
// Dequeue blocks while the queue is empty. It returns ErrClosed when the queue
// is closed and no buffered items remain.
func (q *Queue[T]) Dequeue() (T, error) {
	var zero T
	if err := q.ready(); err != nil {
		return zero, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.items) == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if len(q.items) == 0 {
		return zero, ErrClosed
	}

	item := q.items[0]
	q.items[0] = zero
	q.items = q.items[1:]
	q.notFull.Signal()
	return item, nil
}

// Close prevents new items and wakes callers waiting on the queue.
//
// Close is safe to call more than once. Consumers may continue to dequeue items
// that were already buffered before Close was called.
func (q *Queue[T]) Close() {
	if q == nil || q.notFull == nil || q.notEmpty == nil {
		return
	}

	q.mu.Lock()
	alreadyClosed := q.closed
	q.closed = true
	q.mu.Unlock()

	if alreadyClosed {
		return
	}

	q.notFull.Broadcast()
	q.notEmpty.Broadcast()
}

// Len reports the number of items currently buffered.
func (q *Queue[T]) Len() int {
	if q == nil || q.notFull == nil || q.notEmpty == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.items)
}

func (q *Queue[T]) ready() error {
	if q == nil || q.notFull == nil || q.notEmpty == nil || q.capacity <= 0 {
		return ErrUninitialized
	}

	return nil
}
