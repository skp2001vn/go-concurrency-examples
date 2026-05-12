package actor

import (
	"errors"
	"sync"
)

var (
	// ErrUninitialized means the counter was used before NewCounter created it.
	ErrUninitialized = errors.New("actor: counter is not initialized")

	// ErrClosed means the counter actor has stopped accepting commands.
	ErrClosed = errors.New("actor: counter is closed")
)

type operation int

const (
	add operation = iota
	value
)

type command struct {
	op    operation
	delta int
	reply chan result
}

type result struct {
	value int
}

// Counter owns an integer value in a private goroutine.
//
// Add and Value send commands to the owner goroutine and wait for replies.
// Close stops the owner goroutine and causes future commands to return
// ErrClosed.
//
// A Counter is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call NewCounter before calling methods
// on a Counter.
type Counter struct {
	commands chan command
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

// NewCounter creates a counter with initial as its starting value.
func NewCounter(initial int) *Counter {
	c := &Counter{
		commands: make(chan command),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go c.run(initial)

	return c
}

// Add applies delta to the counter and returns the updated value.
//
// Add returns ErrUninitialized when the counter was not created with
// NewCounter and ErrClosed when the counter actor has been closed.
func (c *Counter) Add(delta int) (int, error) {
	return c.call(command{
		op:    add,
		delta: delta,
		reply: make(chan result, 1),
	})
}

// Value reports the current counter value.
//
// Value returns ErrUninitialized when the counter was not created with
// NewCounter and ErrClosed when the counter actor has been closed.
func (c *Counter) Value() (int, error) {
	return c.call(command{
		op:    value,
		reply: make(chan result, 1),
	})
}

// Close stops the counter actor.
//
// Close is safe to call more than once.
func (c *Counter) Close() {
	if c == nil || c.stop == nil || c.done == nil {
		return
	}

	c.once.Do(func() {
		close(c.stop)
		<-c.done
	})
}

func (c *Counter) call(cmd command) (int, error) {
	if c == nil || c.commands == nil || c.done == nil {
		return 0, ErrUninitialized
	}

	select {
	case <-c.done:
		return 0, ErrClosed
	case c.commands <- cmd:
	}

	select {
	case <-c.done:
		return 0, ErrClosed
	case result := <-cmd.reply:
		return result.value, nil
	}
}

func (c *Counter) run(current int) {
	defer close(c.done)

	for {
		select {
		case <-c.stop:
			return
		case cmd := <-c.commands:
			switch cmd.op {
			case add:
				current += cmd.delta
				cmd.reply <- result{value: current}
			case value:
				cmd.reply <- result{value: current}
			}
		}
	}
}
