// Package singleflight collapses duplicate concurrent work by key.
//
// Use it when many callers may request the same expensive value at the same
// time, such as loading a cache entry or fetching one remote resource, and only
// one caller should perform the work. The example teaches tracking in-flight
// calls with a mutex-protected map and sharing completion with a sync.WaitGroup.
package singleflight
