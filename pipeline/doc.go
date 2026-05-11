// Package pipeline shows how to process a batch by passing values through
// cancellable channel stages instead of coordinating access to shared memory.
//
// Use it for work such as validating records, transforming accepted items, and
// collecting the final results while allowing the caller to stop the whole
// pipeline with a context. The example teaches channel ownership, stage
// composition, output-channel closure, and cancellation propagation.
package pipeline
