// Package semaphore limits how many callers may use a shared resource at the
// same time.
//
// The business logic is a small capacity guard: callers acquire a slot before
// using a limited dependency, release the slot afterward, try to acquire without
// waiting, or stop waiting when their context is canceled.
//
// The example uses a buffered channel because the channel capacity naturally
// represents the number of available slots. Sending into the channel acquires a
// slot, receiving from it releases a slot, and select enables both non-blocking
// and cancellable acquisition. This technique is compact, standard-library
// only, and keeps bounded-concurrency behavior visible in the API.
package semaphore
