package taskgroup

import (
	"context"
	"errors"
	"sync"
)

// ErrNilTask means a submitted task was empty and could not be run.
var ErrNilTask = errors.New("taskgroup: task is nil")

// Task represents one related unit of work in a group.
//
// A Task returns nil after successfully finishing its work. If another task
// fails or the caller cancels the group, the task should stop early when ctx is
// canceled.
type Task func(ctx context.Context) error

// Run runs tasks concurrently and returns the first failure.
//
// Run starts every task, waits for all started tasks to finish, and returns nil
// only when every task succeeds. When one task fails, Run cancels the group
// context so other tasks can stop early. A nil context is treated as
// context.Background.
func Run(ctx context.Context, tasks ...Task) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error

	recordError := func(err error) {
		if err == nil {
			return
		}
		once.Do(func() {
			firstErr = err
			cancel()
		})
	}

	for _, task := range tasks {
		task := task
		wg.Add(1)
		go func() {
			defer wg.Done()

			if task == nil {
				recordError(ErrNilTask)
				return
			}
			recordError(task(ctx))
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	return ctx.Err()
}
