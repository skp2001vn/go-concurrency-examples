package barrier

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestWaitReleasesOnlyAfterAllPartiesArrive verifies that workers cannot pass
// the phase gate until the full group has reached it.
func TestWaitReleasesOnlyAfterAllPartiesArrive(t *testing.T) {
	b := mustBarrier(t, 3)

	started := make(chan struct{}, 2)
	done := make(chan error, 3)
	for i := 0; i < 2; i++ {
		go func() {
			started <- struct{}{}
			done <- b.Wait()
		}()
	}
	waitForStarted(t, started, 2)

	select {
	case err := <-done:
		t.Fatalf("wait finished before all parties arrived: %v", err)
	default:
	}

	done <- b.Wait()
	for i := 0; i < 3; i++ {
		if err := receiveError(t, done); err != nil {
			t.Fatalf("wait error = %v, want nil", err)
		}
	}
}

// TestWaitCanBeReusedForMultiplePhases verifies that the barrier resets after
// each complete group arrives.
func TestWaitCanBeReusedForMultiplePhases(t *testing.T) {
	b := mustBarrier(t, 2)

	for phase := 0; phase < 3; phase++ {
		started := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			close(started)
			done <- b.Wait()
		}()
		<-started

		select {
		case err := <-done:
			t.Fatalf("phase %d finished before second party arrived: %v", phase, err)
		default:
		}

		done <- b.Wait()
		for i := 0; i < 2; i++ {
			if err := receiveError(t, done); err != nil {
				t.Fatalf("phase %d wait error = %v, want nil", phase, err)
			}
		}
	}
}

// TestBarrierCoordinatesPhasedWorkers verifies that no worker starts the next
// phase until every worker has completed the current one.
func TestBarrierCoordinatesPhasedWorkers(t *testing.T) {
	const (
		parties = 4
		phases  = 3
	)

	b := mustBarrier(t, parties)
	var mu sync.Mutex
	completed := make([]int, phases)
	errs := make(chan error, parties)

	for worker := 0; worker < parties; worker++ {
		go func() {
			for phase := 0; phase < phases; phase++ {
				mu.Lock()
				for previous := 0; previous < phase; previous++ {
					if completed[previous] != parties {
						mu.Unlock()
						errs <- errors.New("worker started next phase too early")
						return
					}
				}
				completed[phase]++
				mu.Unlock()

				if err := b.Wait(); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}

	for worker := 0; worker < parties; worker++ {
		if err := receiveError(t, errs); err != nil {
			t.Fatalf("worker error: %v", err)
		}
	}
}

// TestCloseWakesWaitingCallers verifies that closing the barrier releases
// callers waiting for a group that will not arrive.
func TestCloseWakesWaitingCallers(t *testing.T) {
	b := mustBarrier(t, 2)

	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- b.Wait()
	}()
	<-started

	b.Close()

	err := receiveError(t, done)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("wait error = %v, want %v", err, ErrClosed)
	}
}

// TestWaitAfterCloseReturnsError verifies that a closed barrier rejects new
// phase waits.
func TestWaitAfterCloseReturnsError(t *testing.T) {
	b := mustBarrier(t, 1)
	b.Close()
	b.Close()

	err := b.Wait()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("wait error = %v, want %v", err, ErrClosed)
	}
}

// TestWaitRejectsUninitializedBarrier verifies that callers get a clear error
// instead of blocking forever on an uninitialized barrier.
func TestWaitRejectsUninitializedBarrier(t *testing.T) {
	var b Barrier

	err := b.Wait()
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("wait error = %v, want %v", err, ErrUninitialized)
	}
	b.Close()
}

// TestNewRejectsInvalidPartyCount verifies that callers cannot create a
// barrier with no participants.
func TestNewRejectsInvalidPartyCount(t *testing.T) {
	b, err := New(0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if b != nil {
		t.Fatalf("barrier = %v, want nil", b)
	}
}

func mustBarrier(t *testing.T, parties int) *Barrier {
	t.Helper()

	b, err := New(parties)
	if err != nil {
		t.Fatalf("new barrier: %v", err)
	}

	return b
}

func receiveError(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

func waitForStarted(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for caller %d to start", i+1)
		}
	}
}
