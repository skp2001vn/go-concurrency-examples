// Package workerpool processes batches of independent jobs without
// overwhelming a system.
//
// The business logic is fixed-concurrency batch work: callers submit many jobs,
// choose how many may run at once, receive one result per job, and can cancel
// the batch before all jobs have started.
//
// The example uses worker goroutines because the jobs are independent and can
// run in parallel up to a caller-chosen limit. A job channel distributes work to
// available workers, a wait group lets Run wait for active workers to finish,
// and context cancellation stops scheduling unnecessary work. These techniques
// increase throughput while protecting downstream resources from too much
// concurrent activity.
package workerpool
