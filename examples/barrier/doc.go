// Package barrier coordinates workers that must move through phases together.
//
// The business logic is a small phase gate: a fixed number of workers each
// finish one phase of work, wait until every worker reaches the same point, and
// then all continue to the next phase together.
//
// The example uses sync.Cond because callers wait for a shared phase condition:
// all parties for the current generation have arrived. A mutex protects the
// arrival count and generation number, while Broadcast wakes the waiting group
// when the last party arrives or the barrier is closed. These techniques are
// useful for staged worker workflows where no participant should start the next
// phase early.
package barrier
