package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunCompletesAllTasks verifies that Run executes every task and returns a
// result for each original task index.
func TestRunCompletesAllTasks(t *testing.T) {
	var completed atomic.Int32
	tasks := []Task{
		func(context.Context) error {
			completed.Add(1)
			return nil
		},
		func(context.Context) error {
			completed.Add(1)
			return nil
		},
		func(context.Context) error {
			completed.Add(1)
			return nil
		},
	}

	results, err := Run(context.Background(), 2, tasks)
	if err != nil {
		t.Fatalf("run tasks: %v", err)
	}
	if completed.Load() != int32(len(tasks)) {
		t.Fatalf("completed tasks = %d, want %d", completed.Load(), len(tasks))
	}
	assertResults(t, results, len(tasks))
	for _, result := range results {
		if result.Err != nil {
			t.Fatalf("result %d error = %v, want nil", result.Index, result.Err)
		}
	}
}

// TestRunLimitsConcurrentTasks verifies that Run never executes more than the
// configured number of tasks at the same time.
func TestRunLimitsConcurrentTasks(t *testing.T) {
	const workers = 2
	var running atomic.Int32
	var maxRunning atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, 4)

	task := func(context.Context) error {
		now := running.Add(1)
		recordMax(&maxRunning, now)
		started <- struct{}{}
		<-release
		running.Add(-1)
		return nil
	}
	tasks := []Task{task, task, task, task}

	done := make(chan runResult, 1)
	go func() {
		results, err := Run(context.Background(), workers, tasks)
		done <- runResult{results: results, err: err}
	}()

	waitForStarted(t, started, workers)
	if got := maxRunning.Load(); got > workers {
		t.Fatalf("max running tasks = %d, want at most %d", got, workers)
	}

	close(release)
	result := receiveRunResult(t, done)
	if result.err != nil {
		t.Fatalf("run tasks: %v", result.err)
	}
	assertResults(t, result.results, len(tasks))
	if got := maxRunning.Load(); got > workers {
		t.Fatalf("max running tasks = %d, want at most %d", got, workers)
	}
}

// TestRunRecordsTaskErrors verifies that task failures are stored on the
// matching Result instead of stopping unrelated tasks.
func TestRunRecordsTaskErrors(t *testing.T) {
	taskErr := errors.New("task failed")
	tasks := []Task{
		func(context.Context) error {
			return nil
		},
		func(context.Context) error {
			return taskErr
		},
	}

	results, err := Run(context.Background(), 2, tasks)
	if err != nil {
		t.Fatalf("run tasks: %v", err)
	}
	assertResults(t, results, len(tasks))
	if results[0].Err != nil {
		t.Fatalf("result 0 error = %v, want nil", results[0].Err)
	}
	if !errors.Is(results[1].Err, taskErr) {
		t.Fatalf("result 1 error = %v, want %v", results[1].Err, taskErr)
	}
}

// TestRunStopsSchedulingWhenContextIsCanceled verifies that cancellation stops
// new tasks from being scheduled while already-started tasks are allowed to end.
func TestRunStopsSchedulingWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	var taskStarts atomic.Int32

	tasks := []Task{
		func(ctx context.Context) error {
			taskStarts.Add(1)
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
		func(context.Context) error {
			taskStarts.Add(1)
			return nil
		},
		func(context.Context) error {
			taskStarts.Add(1)
			return nil
		},
	}

	done := make(chan runResult, 1)
	go func() {
		results, err := Run(ctx, 1, tasks)
		done <- runResult{results: results, err: err}
	}()

	waitForStarted(t, started, 1)
	cancel()

	result := receiveRunResult(t, done)
	if !errors.Is(result.err, context.Canceled) {
		t.Fatalf("run error = %v, want %v", result.err, context.Canceled)
	}
	assertResults(t, result.results, len(tasks))
	if taskStarts.Load() != 1 {
		t.Fatalf("task starts = %d, want 1", taskStarts.Load())
	}
	for _, taskResult := range result.results {
		if !errors.Is(taskResult.Err, context.Canceled) {
			t.Fatalf("result %d error = %v, want %v", taskResult.Index, taskResult.Err, context.Canceled)
		}
	}
}

// TestRunRecordsNilTask verifies that nil tasks are reported as task-level
// errors without panicking.
func TestRunRecordsNilTask(t *testing.T) {
	results, err := Run(context.Background(), 1, []Task{nil})
	if err != nil {
		t.Fatalf("run nil task: %v", err)
	}
	assertResults(t, results, 1)
	if !errors.Is(results[0].Err, ErrNilTask) {
		t.Fatalf("result error = %v, want %v", results[0].Err, ErrNilTask)
	}
}

// TestRunRejectsInvalidWorkerCount verifies that Run returns an error when the
// worker count is not positive.
func TestRunRejectsInvalidWorkerCount(t *testing.T) {
	results, err := Run(context.Background(), 0, []Task{
		func(context.Context) error {
			return nil
		},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if results != nil {
		t.Fatalf("results = %v, want nil", results)
	}
}

type runResult struct {
	results []Result
	err     error
}

func assertResults(t *testing.T, results []Result, want int) {
	t.Helper()

	if len(results) != want {
		t.Fatalf("results length = %d, want %d", len(results), want)
	}
	for i, result := range results {
		if result.Index != i {
			t.Fatalf("result index at position %d = %d, want %d", i, result.Index, i)
		}
	}
}

func recordMax(maxRunning *atomic.Int32, value int32) {
	for {
		current := maxRunning.Load()
		if value <= current {
			return
		}
		if maxRunning.CompareAndSwap(current, value) {
			return
		}
	}
}

func receiveRunResult(t *testing.T, done <-chan runResult) runResult {
	t.Helper()

	select {
	case result := <-done:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run result")
		return runResult{}
	}
}

func waitForStarted(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for task %d to start", i+1)
		}
	}
}
