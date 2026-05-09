package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNilTask is recorded in a Result when Run receives a nil Task.
var ErrNilTask = errors.New("workerpool: task is nil")

// Task is a unit of work executed by Run.
//
// A Task should return nil on success. If ctx is canceled, a Task should stop
// promptly and return ctx.Err when possible.
type Task func(ctx context.Context) error

// Result describes the outcome of one task submitted to Run.
type Result struct {
	// Index is the task's position in the slice passed to Run.
	Index int

	// Err is the error returned by the task, ErrNilTask for a nil task, or the
	// context error for a task that was not run because ctx was canceled.
	Err error
}

// Run executes tasks with at most workers tasks running at the same time.
//
// Run returns one Result for each task, preserving the original task indexes.
// If ctx is canceled before all tasks are scheduled, Run stops scheduling new
// tasks, marks unscheduled tasks with ctx.Err, waits for already-started tasks,
// and returns the context error. Individual task failures are recorded in the
// corresponding Result.
func Run(ctx context.Context, workers int, tasks []Task) ([]Result, error) {
	if workers <= 0 {
		return nil, fmt.Errorf("workers must be positive: %d", workers)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	results := make([]Result, len(tasks))
	for i := range results {
		results[i].Index = i
	}
	if len(tasks) == 0 {
		return results, nil
	}
	if workers > len(tasks) {
		workers = len(tasks)
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index].Err = runTask(ctx, tasks[index])
			}
		}()
	}

	var runErr error
	for i := range tasks {
		if err := ctx.Err(); err != nil {
			runErr = err
			markUnscheduled(results, i, err)
			break
		}

		select {
		case jobs <- i:
		case <-ctx.Done():
			runErr = ctx.Err()
			markUnscheduled(results, i, runErr)
			close(jobs)
			wg.Wait()
			return results, runErr
		}
	}

	close(jobs)
	wg.Wait()
	return results, runErr
}

func runTask(ctx context.Context, task Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if task == nil {
		return ErrNilTask
	}

	return task(ctx)
}

func markUnscheduled(results []Result, start int, err error) {
	for i := start; i < len(results); i++ {
		results[i].Err = err
	}
}
