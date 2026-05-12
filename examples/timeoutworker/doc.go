// Package timeoutworker runs one operation with a deadline.
//
// The business logic is a small deadline guard: callers start one operation,
// receive its result if it finishes in time, or receive a timeout/cancellation
// error if the operation takes too long.
//
// The example uses a goroutine because the operation must run while the caller
// waits on either the result or the deadline. A buffered result channel lets the
// worker finish after the caller has timed out without blocking forever, and
// select chooses whichever event happens first. context.WithTimeout gives the
// worker a cancellation signal it should observe when possible.
package timeoutworker
