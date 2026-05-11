package singleflight

import (
	"errors"
	"sync"
)

var (
	// ErrUninitialized means a group was used before NewGroup created it.
	ErrUninitialized = errors.New("group is not initialized")

	// ErrEmptyKey means a caller requested work without a deduplication key.
	ErrEmptyKey = errors.New("key is empty")

	// ErrNilFunc means a caller requested work without a function to run.
	ErrNilFunc = errors.New("function is nil")
)

// Group deduplicates in-flight work by key.
//
// Use a Group to ensure that concurrent callers asking for the same key share
// one running function and receive the same result. Calls for different keys may
// run at the same time.
//
// A Group is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call NewGroup before using a Group.
type Group struct {
	mu    sync.Mutex
	calls map[string]*call
}

type call struct {
	wg      sync.WaitGroup
	value   any
	err     error
	waiters int
}

// NewGroup creates an empty duplicate-suppression group.
func NewGroup() *Group {
	return &Group{
		calls: make(map[string]*call),
	}
}

// Do runs fn for key or waits for an already-running call with the same key.
//
// Do returns shared as false to the caller that ran fn. Duplicate callers return
// shared as true and receive the same value and error as the original caller.
// Once a call finishes, a later Do with the same key starts fresh work.
func (g *Group) Do(key string, fn func() (any, error)) (value any, err error, shared bool) {
	if g == nil || g.calls == nil {
		return nil, ErrUninitialized, false
	}
	if key == "" {
		return nil, ErrEmptyKey, false
	}
	if fn == nil {
		return nil, ErrNilFunc, false
	}

	g.mu.Lock()
	if existing := g.calls[key]; existing != nil {
		existing.waiters++
		g.mu.Unlock()

		existing.wg.Wait()
		return existing.value, existing.err, true
	}

	c := &call{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	c.value, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.value, c.err, false
}
