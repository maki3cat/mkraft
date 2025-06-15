package common
import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContextDoneErr(t *testing.T) {
	err := ContextDoneErr()
	assert.Equal(t, "context done", err.Error())
}

func TestErrorMessages(t *testing.T) {
	// Test raft log errors
	assert.Equal(t, "prelog not match", ErrPreLogNotMatch.Error())
	assert.Equal(t, "not leader", ErrNotLeader.Error())
	assert.Equal(t, "server busy", ErrServerBusy.Error())

	// Test consensus errors
	assert.Equal(t, "not enough peers for consensus", ErrNotEnoughPeersForConsensus.Error())
	assert.Equal(t, "majority not met", ErrMajorityNotMet.Error())

	// Test invariant errors
	assert.Equal(t, "invariants broken", ErrInvariantsBroken.Error())
	assert.Equal(t, "corrupt persistent file", ErrCorruptPersistentFile.Error())

	// Test context related errors
	assert.Equal(t, "deadline not set", ErrDeadlineNotSet.Error())
	assert.Equal(t, "context done", ErrContextDone.Error())
	assert.Equal(t, "deadline in the past", ErrDeadlineInThePast.Error())
}
