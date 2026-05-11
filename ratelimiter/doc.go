// Package ratelimiter limits how often callers may perform work over time.
//
// The business logic is a small traffic guard: callers can perform a burst of
// work immediately, wait for later capacity when the burst is exhausted, fail
// fast when no capacity is available, or stop waiting when their context is
// canceled.
//
// The example uses a token channel because available capacity can be modeled as
// discrete permits. A time.Ticker replenishes permits at a fixed interval, and
// context cancellation lets callers stop waiting when the work is no longer
// useful. These techniques protect downstream systems from too much activity
// over time while keeping the caller-facing API small.
package ratelimiter
