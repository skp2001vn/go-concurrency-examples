// Package boundedqueue coordinates producers and consumers through a fixed-size
// queue.
//
// The business logic is a small job buffer: producers enqueue work until the
// queue is full, consumers dequeue work until the queue is empty, and closing
// the queue wakes callers that are waiting for space or work.
//
// The example uses sync.Cond because producers and consumers wait for shared
// state conditions to become true: not full for producers and not empty for
// consumers. A mutex protects the queue slice while condition variables put
// callers to sleep without busy waiting. These techniques are useful when a
// program owns shared state directly and needs precise wakeups when that state
// changes.
package boundedqueue
