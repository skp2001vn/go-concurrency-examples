package webcrawler

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrEmptyStart means Crawl was called without a starting URL.
	ErrEmptyStart = errors.New("webcrawler: start URL is empty")

	// ErrNilFetcher means Crawl was called without a page fetcher.
	ErrNilFetcher = errors.New("webcrawler: fetcher is nil")
)

// Fetcher loads one page and returns the links discovered on that page.
//
// Fetch should return promptly when ctx is canceled.
type Fetcher interface {
	Fetch(ctx context.Context, url string) ([]string, error)
}

// Result reports the pages fetched by a crawl.
type Result struct {
	// Visited contains each URL that workers attempted to fetch, at most once.
	Visited []string

	// Errors maps URLs to fetch errors. A page with an error is still considered
	// fetched, but its links are not followed.
	Errors map[string]error
}

type job struct {
	url   string
	depth int
}

type crawler struct {
	ctx      context.Context
	fetcher  Fetcher
	maxDepth int
	jobs     chan job
	pending  sync.WaitGroup

	mu        sync.Mutex
	scheduled map[string]bool
	fetched   []string
	errors    map[string]error
}

// Crawl visits pages reachable from start up to maxDepth.
//
// Crawl runs at most workers fetches at the same time. It records fetch errors
// and continues crawling other pages. If ctx is canceled, Crawl stops scheduling
// useful work, waits for started workers to finish, and returns the cancellation
// error. A nil context is treated as context.Background.
func Crawl(ctx context.Context, start string, maxDepth int, workers int, fetcher Fetcher) (Result, error) {
	result := Result{Errors: map[string]error{}}
	if start == "" {
		return result, ErrEmptyStart
	}
	if maxDepth < 0 {
		return result, fmt.Errorf("max depth must be non-negative: %d", maxDepth)
	}
	if workers <= 0 {
		return result, fmt.Errorf("workers must be positive: %d", workers)
	}
	if fetcher == nil {
		return result, ErrNilFetcher
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	c := &crawler{
		ctx:       ctx,
		fetcher:   fetcher,
		maxDepth:  maxDepth,
		jobs:      make(chan job),
		scheduled: make(map[string]bool),
		errors:    make(map[string]error),
	}

	var workerWG sync.WaitGroup
	workerWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer workerWG.Done()
			c.work()
		}()
	}

	c.enqueue(start, 0)
	c.pending.Wait()
	close(c.jobs)
	workerWG.Wait()

	result = c.result()
	if err := ctx.Err(); err != nil {
		return result, err
	}

	return result, nil
}

func (c *crawler) work() {
	for j := range c.jobs {
		c.fetch(j)
		c.pending.Done()
	}
}

func (c *crawler) fetch(j job) {
	if err := c.ctx.Err(); err != nil {
		return
	}

	c.recordFetched(j.url)
	links, err := c.fetcher.Fetch(c.ctx, j.url)
	if err != nil {
		if c.ctx.Err() == nil {
			c.recordError(j.url, err)
		}
		return
	}
	if j.depth >= c.maxDepth {
		return
	}

	for _, link := range links {
		c.enqueue(link, j.depth+1)
	}
}

func (c *crawler) enqueue(url string, depth int) {
	if url == "" || depth > c.maxDepth {
		return
	}

	c.mu.Lock()
	if c.scheduled[url] {
		c.mu.Unlock()
		return
	}
	c.scheduled[url] = true
	c.mu.Unlock()

	c.pending.Add(1)
	go func() {
		select {
		case c.jobs <- job{url: url, depth: depth}:
		case <-c.ctx.Done():
			c.pending.Done()
		}
	}()
}

func (c *crawler) recordFetched(url string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fetched = append(c.fetched, url)
}

func (c *crawler) recordError(url string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.errors[url] = err
}

func (c *crawler) result() Result {
	// Workers have stopped before result is called, so no goroutine can still
	// write fetched or errors.
	fetched := append([]string(nil), c.fetched...)
	sort.Strings(fetched)

	errs := make(map[string]error, len(c.errors))
	for url, err := range c.errors {
		errs[url] = err
	}

	return Result{
		Visited: fetched,
		Errors:  errs,
	}
}
