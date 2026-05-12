// Package pipeline validates, filters, transforms, and collects a batch with
// cancellation support.
//
// The business logic is a small batch processor: callers provide input values,
// invalid values are filtered out, accepted values are transformed, and final
// results are collected in order while the caller can cancel the work.
//
// The example uses channels because each stage can communicate by passing
// values to the next stage instead of coordinating access to shared memory. Each
// stage owns and closes its outbound channel, which makes data flow and channel
// lifetime clear. Context cancellation is checked at each stage so the whole
// pipeline can stop without leaking blocked goroutines. These techniques make
// staged processing composable and easy to reason about.
package pipeline
