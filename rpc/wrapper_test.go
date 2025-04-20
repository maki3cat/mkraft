package rpc

import (
	"testing"
	"time"
)

func TestWriteNilChannel(t *testing.T) {
	// Create a nil channel
	var nilChan chan int
	go func() {
		nilChan <- 42 // This will cause a panic
	}()
	time.Sleep(10 * time.Second) // Wait for the goroutine to finish

}
