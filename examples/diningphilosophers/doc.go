// Package diningphilosophers demonstrates deadlock avoidance for workers that
// need two shared resources.
//
// The business logic is the classic dining philosophers problem: each worker
// must acquire two neighboring resources before doing work, and all workers may
// try to work concurrently.
//
// The example uses a counting semaphore with capacity one less than the number
// of workers. That prevents every worker from holding one resource while waiting
// for another, which breaks the circular-wait condition that causes deadlock.
// Each shared resource is still protected by its own mutex. This technique is
// useful whenever work needs multiple resources and the system must avoid a
// deadlock-prone "everyone holds one thing and waits" state.
package diningphilosophers
