package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCurrentState(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()
	state := node.GetNodeState()
	assert.Equal(t, StateFollower, state)
}

func TestSetCurrentState(t *testing.T) {

	node := newMockNode(t)
	defer cleanUpTmpDir()

	// Test setting to Leader
	node.SetNodeState(StateLeader)
	assert.Equal(t, StateLeader, node.GetNodeState())

	// Test setting to Candidate
	node.SetNodeState(StateCandidate)
	assert.Equal(t, StateCandidate, node.GetNodeState())

	// Test setting to Follower
	node.SetNodeState(StateFollower)
	assert.Equal(t, StateFollower, node.GetNodeState())
}

func TestIsLeader(t *testing.T) {

	node := newMockNode(t)
	defer cleanUpTmpDir()
	// Test when node is leader
	node.SetNodeState(StateLeader)
	assert.True(t, node.GetNodeState() == StateLeader)

	// Test when node is not leader
	node.SetNodeState(StateFollower)
	assert.False(t, node.GetNodeState() == StateLeader)
}

func TestIsFollower(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()
	// Test when node is follower
	node.SetNodeState(StateFollower)
	assert.True(t, node.GetNodeState() == StateFollower)

	// Test when node is not follower
	node.SetNodeState(StateLeader)
	assert.False(t, node.GetNodeState() == StateFollower)
}

func TestIsCandidate(t *testing.T) {
	node := newMockNode(t)
	defer cleanUpTmpDir()
	// Test when node is candidate
	node.SetNodeState(StateCandidate)
	assert.True(t, node.GetNodeState() == StateCandidate)

	// Test when node is not candidate
	node.SetNodeState(StateLeader)
	assert.False(t, node.GetNodeState() == StateCandidate)
}
