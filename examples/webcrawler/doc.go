// Package webcrawler crawls linked pages with dynamic task discovery.
//
// The business logic is a small in-memory crawler: callers start from one URL,
// fetch links for each page, visit each discovered page at most once, respect a
// depth limit, and stop cleanly when the crawl is canceled.
//
// The example uses worker goroutines because page fetches can run concurrently.
// A job channel carries URLs to workers, a mutex-protected scheduled set
// prevents duplicate jobs before pages are fetched, and a sync.WaitGroup tracks
// jobs discovered while the crawl is already running. context cancellation lets
// callers stop the traversal early. This technique is useful for graph-shaped
// work where tasks discover more tasks while executing.
package webcrawler
