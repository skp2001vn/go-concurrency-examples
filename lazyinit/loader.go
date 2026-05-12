package lazyinit

import (
	"errors"
	"sync"
)

var (
	// ErrNilLoad means a loader was created without an initialization function.
	ErrNilLoad = errors.New("lazyinit: load function is nil")

	// ErrUninitialized means the loader was used before New created it.
	ErrUninitialized = errors.New("lazyinit: loader is not initialized")
)

// Loader initializes and caches one shared value.
//
// The first Get call runs the load function. Concurrent callers wait for that
// same load attempt to finish, and later callers receive the cached value and
// error.
//
// A Loader is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Loader.
type Loader[T any] struct {
	once  sync.Once
	load  func() (T, error)
	value T
	err   error
}

// New creates a loader that runs load at most once.
//
// New returns an error when load is nil.
func New[T any](load func() (T, error)) (*Loader[T], error) {
	if load == nil {
		return nil, ErrNilLoad
	}

	return &Loader[T]{
		load: load,
	}, nil
}

// Get returns the initialized value.
//
// The first Get call runs the load function. If loading fails, Get returns the
// cached error and later calls return the same error without retrying. Get
// returns ErrUninitialized when the loader was not created with New.
func (l *Loader[T]) Get() (T, error) {
	var zero T
	if l == nil || l.load == nil {
		return zero, ErrUninitialized
	}

	l.once.Do(func() {
		l.value, l.err = l.load()
	})

	return l.value, l.err
}
