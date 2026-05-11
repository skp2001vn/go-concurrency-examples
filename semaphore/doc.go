// Package semaphore limits how many callers may use a shared resource at once.
//
// Use it to protect a downstream service, CPU-heavy work, or any other limited
// dependency from too many simultaneous users. Callers can wait for capacity,
// fail fast, or stop waiting when their context is canceled. The example
// teaches using a buffered channel as a counting semaphore with blocking,
// non-blocking, and cancellable acquisition paths.
package semaphore
