// Package batcher groups incoming items into size- or time-based batches.
//
// The business logic is a small batching worker: callers stream individual
// items, receive full batches when enough items arrive, receive partial batches
// when the flush delay expires, and receive the final partial batch when input
// closes.
//
// The example uses channels because callers hand off items as a stream and
// receive grouped output batches from a separate goroutine. A time.Timer starts
// when the first pending item arrives and is reset after each flush, while
// context cancellation lets callers stop the batching loop early. These
// techniques are useful for grouping logs, database writes, API calls, or
// analytics events without waiting forever for a full batch.
package batcher
