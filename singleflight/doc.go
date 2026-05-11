// Package singleflight suppresses duplicate concurrent requests by key.
//
// The business logic is duplicate request suppression: when many callers ask
// for the same expensive value at the same time, only the first caller performs
// the work and duplicate callers receive the same result.
//
// The example uses a mutex-protected map because active work must be tracked by
// key without races. Each in-flight call uses a sync.WaitGroup so duplicate
// callers can wait for the original caller to finish and then share its value
// and error. This technique avoids wasted work while still allowing different
// keys to run independently.
package singleflight
