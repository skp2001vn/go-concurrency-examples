package webcrawler

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestCrawlVisitsReachablePagesOnce verifies that the crawler follows links
// and suppresses duplicate visits.
func TestCrawlVisitsReachablePagesOnce(t *testing.T) {
	fetcher := newMapFetcher(map[string][]string{
		"/":         {"/about", "/products", "/about"},
		"/about":    {"/"},
		"/products": {"/pricing"},
		"/pricing":  nil,
	}, nil)

	result, err := Crawl(context.Background(), "/", 3, 2, fetcher)
	if err != nil {
		t.Fatalf("crawl graph: %v", err)
	}

	want := []string{"/", "/about", "/pricing", "/products"}
	if !reflect.DeepEqual(result.Visited, want) {
		t.Fatalf("visited = %v, want %v", result.Visited, want)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("errors = %v, want none", result.Errors)
	}
	for _, url := range want {
		if got := fetcher.callsFor(url); got != 1 {
			t.Fatalf("fetch calls for %q = %d, want 1", url, got)
		}
	}
}

// TestCrawlRespectsMaxDepth verifies that links beyond the caller's depth
// limit are not visited.
func TestCrawlRespectsMaxDepth(t *testing.T) {
	fetcher := newMapFetcher(map[string][]string{
		"/":        {"/level-1"},
		"/level-1": {"/level-2"},
		"/level-2": nil,
	}, nil)

	result, err := Crawl(context.Background(), "/", 1, 2, fetcher)
	if err != nil {
		t.Fatalf("crawl graph: %v", err)
	}

	want := []string{"/", "/level-1"}
	if !reflect.DeepEqual(result.Visited, want) {
		t.Fatalf("visited = %v, want %v", result.Visited, want)
	}
	if got := fetcher.callsFor("/level-2"); got != 0 {
		t.Fatalf("fetch calls for level 2 = %d, want 0", got)
	}
}

// TestCrawlRecordsFetchErrors verifies that a failed page is reported without
// stopping unrelated pages.
func TestCrawlRecordsFetchErrors(t *testing.T) {
	pageErr := errors.New("not found")
	fetcher := newMapFetcher(map[string][]string{
		"/":   {"/ok", "/bad"},
		"/ok": nil,
	}, map[string]error{
		"/bad": pageErr,
	})

	result, err := Crawl(context.Background(), "/", 2, 2, fetcher)
	if err != nil {
		t.Fatalf("crawl graph: %v", err)
	}

	want := []string{"/", "/bad", "/ok"}
	if !reflect.DeepEqual(result.Visited, want) {
		t.Fatalf("visited = %v, want %v", result.Visited, want)
	}
	if !errors.Is(result.Errors["/bad"], pageErr) {
		t.Fatalf("bad page error = %v, want %v", result.Errors["/bad"], pageErr)
	}
}

// TestCrawlReturnsCancellation verifies that callers can stop an in-flight
// crawl through context cancellation.
func TestCrawlReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	fetcher := FetcherFunc(func(ctx context.Context, url string) ([]string, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	done := make(chan crawlResult, 1)
	go func() {
		result, err := Crawl(ctx, "/", 1, 1, fetcher)
		done <- crawlResult{result: result, err: err}
	}()

	<-started
	cancel()

	outcome := receiveCrawlResult(t, done)
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("crawl error = %v, want %v", outcome.err, context.Canceled)
	}
	if !reflect.DeepEqual(outcome.result.Visited, []string{"/"}) {
		t.Fatalf("visited = %v, want start page", outcome.result.Visited)
	}
}

// TestCrawlRejectsInvalidInputs verifies that unusable crawl settings fail
// before any workers start.
func TestCrawlRejectsInvalidInputs(t *testing.T) {
	fetcher := newMapFetcher(nil, nil)

	if _, err := Crawl(context.Background(), "", 1, 1, fetcher); !errors.Is(err, ErrEmptyStart) {
		t.Fatalf("empty start error = %v, want %v", err, ErrEmptyStart)
	}
	if _, err := Crawl(context.Background(), "/", -1, 1, fetcher); err == nil {
		t.Fatal("expected max depth error, got nil")
	}
	if _, err := Crawl(context.Background(), "/", 1, 0, fetcher); err == nil {
		t.Fatal("expected workers error, got nil")
	}
	if _, err := Crawl(context.Background(), "/", 1, 1, nil); !errors.Is(err, ErrNilFetcher) {
		t.Fatalf("nil fetcher error = %v, want %v", err, ErrNilFetcher)
	}
}

// FetcherFunc adapts a function to Fetcher.
type FetcherFunc func(ctx context.Context, url string) ([]string, error)

// Fetch loads links for url.
func (f FetcherFunc) Fetch(ctx context.Context, url string) ([]string, error) {
	return f(ctx, url)
}

type crawlResult struct {
	result Result
	err    error
}

type mapFetcher struct {
	mu    sync.Mutex
	links map[string][]string
	errs  map[string]error
	calls map[string]int
}

func newMapFetcher(links map[string][]string, errs map[string]error) *mapFetcher {
	if links == nil {
		links = map[string][]string{}
	}
	if errs == nil {
		errs = map[string]error{}
	}

	return &mapFetcher{
		links: links,
		errs:  errs,
		calls: make(map[string]int),
	}
}

func (f *mapFetcher) Fetch(_ context.Context, url string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls[url]++
	if err := f.errs[url]; err != nil {
		return nil, err
	}

	return append([]string(nil), f.links[url]...), nil
}

func (f *mapFetcher) callsFor(url string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[url]
}

func receiveCrawlResult(t *testing.T, done <-chan crawlResult) crawlResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for crawl result")
		return crawlResult{}
	}
}
