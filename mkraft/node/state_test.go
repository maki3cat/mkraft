package node

import (
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestGetCurrentState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaftLog := log.NewMockRaftLogs(ctrl)
	membership := peers.NewMockMembership(ctrl)
	config := common.NewMockConfigIface(ctrl)
	statemachine := plugs.NewMockStateMachineIface(ctrl)
	config.EXPECT().GetRaftNodeRequestBufferSize().Return(10)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)

	node.SetNodeState(StateFollower)

	state := node.GetNodeState()
	assert.Equal(t, StateFollower, state)
}

func TestSetCurrentState(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftLog := log.NewMockRaftLogs(ctrl)
	membership := peers.NewMockMembership(ctrl)
	config := common.NewMockConfigIface(ctrl)
	statemachine := plugs.NewMockStateMachineIface(ctrl)
	config.EXPECT().GetRaftNodeRequestBufferSize().Return(10)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaftLog := log.NewMockRaftLogs(ctrl)
	membership := peers.NewMockMembership(ctrl)
	config := common.NewMockConfigIface(ctrl)
	statemachine := plugs.NewMockStateMachineIface(ctrl)
	config.EXPECT().GetRaftNodeRequestBufferSize().Return(10)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)

	// Test when node is leader
	node.SetNodeState(StateLeader)
	assert.True(t, node.GetNodeState() == StateLeader)

	// Test when node is not leader
	node.SetNodeState(StateFollower)
	assert.False(t, node.GetNodeState() == StateLeader)
}

func TestIsFollower(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaftLog := log.NewMockRaftLogs(ctrl)
	membership := peers.NewMockMembership(ctrl)
	config := common.NewMockConfigIface(ctrl)
	statemachine := plugs.NewMockStateMachineIface(ctrl)
	config.EXPECT().GetRaftNodeRequestBufferSize().Return(10)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)

	// Test when node is follower
	node.SetNodeState(StateFollower)
	assert.True(t, node.GetNodeState() == StateFollower)

	// Test when node is not follower
	node.SetNodeState(StateLeader)
	assert.False(t, node.GetNodeState() == StateFollower)
}

func TestIsCandidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRaftLog := log.NewMockRaftLogs(ctrl)
	membership := peers.NewMockMembership(ctrl)
	config := common.NewMockConfigIface(ctrl)
	statemachine := plugs.NewMockStateMachineIface(ctrl)
	config.EXPECT().GetRaftNodeRequestBufferSize().Return(10)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)

	// Test when node is candidate
	node.SetNodeState(StateCandidate)
	assert.True(t, node.GetNodeState() == StateCandidate)

	// Test when node is not candidate
	node.SetNodeState(StateLeader)
	assert.False(t, node.GetNodeState() == StateCandidate)
}
