// Package taskgroup runs related tasks concurrently and cancels the group on
// first failure.
//
// The business logic is a small request workflow: callers start several related
// tasks, such as loading profile data, permissions, and recommendations. If one
// task fails, the remaining tasks are asked to stop and the first failure is
// returned.
//
// The example uses goroutines because the tasks can run independently for the
// same operation. A sync.WaitGroup waits for all started tasks to finish,
// context.WithCancel signals early stop after the first failure, and sync.Once
// records exactly one first error. These techniques model structured
// concurrency with only the standard library.
package taskgroup
