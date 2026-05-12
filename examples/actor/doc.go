// Package actor manages shared state through a single owner goroutine.
//
// The business logic is a small counter service: many callers can add to a
// counter, read its current value, and close the service when no more commands
// should be accepted.
//
// The example uses the actor pattern because one goroutine owns the mutable
// counter value and serializes every command sent through a channel. Callers use
// per-command reply channels to receive results without reading the state
// directly. These techniques avoid shared-memory locking in the caller-facing
// API and make state changes happen in one predictable event loop.
package actor
