package atomiccounter

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMetricsRecordsRequestOutcomes verifies that callers can count started,
// successful, and failed requests.
func TestMetricsRecordsRequestOutcomes(t *testing.T) {
	var metrics Metrics

	if got := metrics.Start(); got != 1 {
		t.Fatalf("in flight after start = %d, want 1", got)
	}
	if got := metrics.Start(); got != 2 {
		t.Fatalf("in flight after second start = %d, want 2", got)
	}
	if got := metrics.Succeed(); got != 1 {
		t.Fatalf("in flight after success = %d, want 1", got)
	}
	if got := metrics.Fail(); got != 0 {
		t.Fatalf("in flight after failure = %d, want 0", got)
	}

	got := metrics.Snapshot()
	want := Snapshot{
		Requests:     2,
		Success:      1,
		Failure:      1,
		InFlight:     0,
		PeakInFlight: 2,
	}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

// TestMetricsHandlesConcurrentUpdates verifies that many goroutines can update
// counters without lost increments.
func TestMetricsHandlesConcurrentUpdates(t *testing.T) {
	const callers = 100
	var metrics Metrics

	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			metrics.Start()
			<-release
			if index%2 == 0 {
				metrics.Succeed()
				return
			}
			metrics.Fail()
		}(i)
	}

	waitForInFlight(t, &metrics, callers)
	close(release)
	wg.Wait()

	got := metrics.Snapshot()
	if got.Requests != callers {
		t.Fatalf("requests = %d, want %d", got.Requests, callers)
	}
	if got.Success != callers/2 {
		t.Fatalf("success = %d, want %d", got.Success, callers/2)
	}
	if got.Failure != callers/2 {
		t.Fatalf("failure = %d, want %d", got.Failure, callers/2)
	}
	if got.InFlight != 0 {
		t.Fatalf("in flight = %d, want 0", got.InFlight)
	}
	if got.PeakInFlight != callers {
		t.Fatalf("peak in flight = %d, want %d", got.PeakInFlight, callers)
	}
}

// TestResetClearsCounters verifies that callers can clear the metric state.
func TestResetClearsCounters(t *testing.T) {
	var metrics Metrics
	metrics.Start()
	metrics.Succeed()

	metrics.Reset()

	if got := metrics.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("snapshot after reset = %+v, want zero snapshot", got)
	}
}

// TestNilMetricsSnapshotAndResetAreSafe verifies that read-only helper methods
// tolerate a missing metrics collector.
func TestNilMetricsSnapshotAndResetAreSafe(t *testing.T) {
	var metrics *Metrics

	if got := metrics.Snapshot(); got != (Snapshot{}) {
		t.Fatalf("nil snapshot = %+v, want zero snapshot", got)
	}
	metrics.Reset()
}

func waitForInFlight(t *testing.T, metrics *Metrics, want int64) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		if got := metrics.Snapshot().InFlight; got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for in-flight count %d", want)
		default:
			runtime.Gosched()
		}
	}
}
