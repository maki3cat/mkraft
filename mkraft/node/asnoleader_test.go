package node

import (
	"context"
	"testing"
)

func TestAsyncSendElection(t *testing.T) {
	t.Run("update term error", func(t *testing.T) {
		testCtx := context.Background()
		n := newMockNode(t)
		defer cleanUpTmpDir()

		// Mock error case for updateCurrentTermAndVotedForAsCandidate
		n.stateRWLock.Lock()
		n.CurrentTerm = 1
		n.stateRWLock.Unlock()

		// Call asyncSendElection which should fail due to term update error
		consensusChan := n.asyncSendElection(testCtx)

		// Channel should be closed due to error
		_, ok := <-consensusChan
		if ok {
			t.Error("expected consensusChan to be closed due to term update error")
		}
	})

}
