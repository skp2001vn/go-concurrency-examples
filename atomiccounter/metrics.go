package atomiccounter

import "sync/atomic"

// Metrics tracks request counts with atomic integer operations.
//
// Metrics is useful for high-frequency observations where each counter can be
// updated independently. It is safe for concurrent use by multiple goroutines.
//
// The zero value is ready for use.
type Metrics struct {
	requests atomic.Int64
	success  atomic.Int64
	failure  atomic.Int64
	inFlight atomic.Int64
	peak     atomic.Int64
}

// Snapshot reports the metric values at a moment in time.
type Snapshot struct {
	// Requests is the number of requests that have started.
	Requests int64

	// Success is the number of requests that completed successfully.
	Success int64

	// Failure is the number of requests that completed with failure.
	Failure int64

	// InFlight is the number of requests currently active.
	InFlight int64

	// PeakInFlight is the highest observed number of active requests.
	PeakInFlight int64
}

// Start records that one request has begun and returns the active request
// count.
func (m *Metrics) Start() int64 {
	m.requests.Add(1)
	current := m.inFlight.Add(1)
	m.recordPeak(current)
	return current
}

// Succeed records one successful request completion and returns the remaining
// active request count.
func (m *Metrics) Succeed() int64 {
	m.success.Add(1)
	return m.inFlight.Add(-1)
}

// Fail records one failed request completion and returns the remaining active
// request count.
func (m *Metrics) Fail() int64 {
	m.failure.Add(1)
	return m.inFlight.Add(-1)
}

// Snapshot returns the current metrics.
func (m *Metrics) Snapshot() Snapshot {
	if m == nil {
		return Snapshot{}
	}

	return Snapshot{
		Requests:     m.requests.Load(),
		Success:      m.success.Load(),
		Failure:      m.failure.Load(),
		InFlight:     m.inFlight.Load(),
		PeakInFlight: m.peak.Load(),
	}
}

// Reset clears all counters.
func (m *Metrics) Reset() {
	if m == nil {
		return
	}

	m.requests.Store(0)
	m.success.Store(0)
	m.failure.Store(0)
	m.inFlight.Store(0)
	m.peak.Store(0)
}

func (m *Metrics) recordPeak(value int64) {
	for {
		current := m.peak.Load()
		if value <= current {
			return
		}
		if m.peak.CompareAndSwap(current, value) {
			return
		}
	}
}
