package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrNilTask means a submitted job was empty and could not be processed.
var ErrNilTask = errors.New("workerpool: task is nil")

// Task represents one job in a batch.
//
// A Task returns nil after successfully finishing the job. If the caller cancels
// the batch, the task should stop early when it can.
type Task func(ctx context.Context) error

// Result tells the caller whether one job succeeded or failed.
type Result struct {
	// Index identifies the job in the original batch.
	Index int

	// Err is nil when the job succeeds. Otherwise, it explains why that job did
	// not complete.
	Err error
}

// Run completes a batch of jobs without letting too many run at once.
//
// Use Run when a batch may contain many independent jobs, but the application
// needs to protect a database, API, CPU budget, or other limited resource. The
// returned results let callers match each job to its success or failure.
//
// If the caller cancels the batch, Run stops starting new jobs, waits for active
// jobs to finish, and returns the cancellation error.
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
