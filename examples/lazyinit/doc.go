// Package lazyinit initializes an expensive shared resource only once.
//
// The business logic is a small resource loader: many callers need the same
// shared value, such as a configuration, client, or model, and only the first
// caller should perform the expensive initialization.
//
// The example uses sync.Once because concurrent callers should wait for the
// same initialization attempt instead of running duplicate work. The loader
// caches both the initialized value and the initialization error, so later
// callers receive the same result without repeating the setup. This technique is
// useful for safe, one-time initialization of shared process resources.
package lazyinit
