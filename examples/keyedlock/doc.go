// Package keyedlock serializes work for the same business key while allowing
// different keys to proceed concurrently.
//
// The business logic is a small keyed update guard: callers can protect updates
// for one SKU, order ID, cart ID, file path, or session ID without blocking
// unrelated keys.
//
// The example uses a short global mutex to protect the lock registry and a
// separate mutex for each active key. The global lock is held only while finding
// or creating the per-key lock, so the real business work is serialized only
// with other work for the same key. This improves throughput compared with one
// global lock because unrelated keys can proceed concurrently instead of waiting
// behind each other. Reference counting removes unused key locks from the
// registry. This technique is useful when the rule is same key: serialize for
// correctness, different keys: parallelize for better performance.
package keyedlock
