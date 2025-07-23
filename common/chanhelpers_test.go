package common

import (
	"testing"
	"time"
)

func TestBroadcastNonBlocking(t *testing.T) {
	ch1 := make(chan struct{}, 1)
	ch2 := make(chan struct{}, 1)
	ch3 := make(chan struct{}, 1)
	triggerChans := []chan struct{}{ch1, ch2, ch3}

	// All channels are empty, should receive a value after broadcast
	BroadcastNonBlocking(triggerChans)

	for i, ch := range triggerChans {
		select {
		case <-ch:
			// ok
		default:
			t.Errorf("channel %d did not receive broadcast", i)
		}
	}

	// Fill channels to capacity, broadcast should not block or send
	for _, ch := range triggerChans {
		ch <- struct{}{}
	}
	BroadcastNonBlocking(triggerChans)
	// Should not panic or block, and channels should not have more than 1 value
	for i, ch := range triggerChans {
		select {
		case <-ch:
			// ok, one value
		default:
			t.Errorf("channel %d should have one value", i)
		}
		select {
		case <-ch:
			t.Errorf("channel %d should not have more than one value", i)
		default:
			// ok
		}
	}
}

func TestDrainChannel(t *testing.T) {
	ch := make(chan struct{}, 5)
	// Fill channel with 3 values
	ch <- struct{}{}
	ch <- struct{}{}
	ch <- struct{}{}

	DrainChannel(ch)

	select {
	case <-ch:
		t.Errorf("channel should be drained but still has value")
	default:
		// ok, channel is empty
	}

	// Test draining an already empty channel (should not block or panic)
	done := make(chan struct{})
	go func() {
		DrainChannel(ch)
		close(done)
	}()
	select {
	case <-done:
		// ok
	case <-time.After(100 * time.Millisecond):
		t.Errorf("DrainChannel blocked on empty channel")
	}
}
