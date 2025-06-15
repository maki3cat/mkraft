package plugs

import (
	"context"
	"testing"
)

func TestStateMachineAppendOnly(t *testing.T) {
	// Create temp dir for test
	tempDir := t.TempDir()

	// Create new state machine
	sm := NewStateMachineAppendOnlyImpl(tempDir)
	defer sm.Close()

	// Test single command
	cmd := []byte("test command")
	_, err := sm.ApplyCommand(context.Background(), cmd)
	if err != nil {
		t.Errorf("ApplyCommand failed: %v", err)
	}

	// Test batch commands
	cmds := [][]byte{
		[]byte("cmd1"),
		[]byte("cmd2"),
		[]byte("cmd3"),
	}
	resps, err := sm.BatchApplyCommand(context.Background(), cmds)
	if err != nil {
		t.Errorf("BatchApplyCommand failed: %v", err)
	}
	if resps != nil {
		t.Errorf("Expected nil responses, got %v", resps)
	}

	// Test file persistence
	sm2 := NewStateMachineAppendOnlyImpl(tempDir)
	defer sm2.Close()

}
