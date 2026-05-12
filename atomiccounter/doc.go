// Package atomiccounter tracks high-frequency metrics from many goroutines.
//
// The business logic is a small request metrics collector: callers record
// started requests, successful completions, failed completions, and the highest
// number of requests that were in flight at the same time.
//
// The example uses sync/atomic because each metric is a single integer that can
// be updated safely without protecting a larger invariant with a mutex.
// Add and Load handle simple counters, while CompareAndSwap records the peak
// in-flight value only when a caller observes a new maximum. These techniques
// are useful for lightweight counters where every update should be safe under
// concurrent access but no multi-field transaction is required.
package atomiccounter
