// Package workerpool helps process large batches without overwhelming a system.
//
// Use it for work such as sending notifications, calling external APIs, or
// updating many records when each job is independent but the application still
// needs a clear limit on how much work is active at once. The example teaches
// fan-out with worker goroutines, job channels, result collection, and context
// cancellation.
package workerpool
