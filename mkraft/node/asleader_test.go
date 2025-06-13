package node

import (
	"context"
	"errors"
	"testing"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// --------syncDoHeartbeat--------

func TestNode_syncDoHeartbeat_HappyPath(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	// Setup mock expectations
	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return([]string{"peer1", "peer2"}, nil)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{}, nil).Times(2)
	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil).Times(2)
	raftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()

	// set the current term and voted for
	n.CurrentTerm = 1
	n.VotedFor = "node1"

	consensus := n.consensus.(*MockConsensus)
	consensus.EXPECT().ConsensusAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&AppendEntriesConsensusResp{Success: true, Term: 1}, nil)

	// Execute
	ctx := context.Background()
	result, err := n.syncDoHeartbeat(ctx)

	// Verify
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result.ShallDegrade {
		t.Error("Expected ShallDegrade to be false")
	}
}

func TestNode_syncDoHeartbeat_Degrade(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	// Setup mock expectations
	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return([]string{"peer1", "peer2"}, nil)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{}, nil).Times(2)
	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil).Times(2)
	raftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()

	// set the current term and voted for
	n.CurrentTerm = 1
	n.VotedFor = "node1"

	consensus := n.consensus.(*MockConsensus)
	consensus.EXPECT().ConsensusAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&AppendEntriesConsensusResp{Success: false, Term: 2}, nil)

	// Execute
	ctx := context.Background()
	result, err := n.syncDoHeartbeat(ctx)
	assert.NoError(t, err)

	// Verify
	assert.Equal(t, true, result.ShallDegrade)
	assert.Equal(t, TermRank(2), result.Term)
}

func TestNode_syncDoHeartbeat_Fail_Case1(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	// Setup mock expectations
	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return([]string{"peer1", "peer2"}, nil)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{}, nil).Times(2)
	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil).Times(2)
	raftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()

	// set the current term and voted for
	n.CurrentTerm = 1
	n.VotedFor = "node1"

	consensus := n.consensus.(*MockConsensus)
	consensus.EXPECT().ConsensusAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&AppendEntriesConsensusResp{Success: false, Term: 1}, nil)

	// Execute
	ctx := context.Background()
	result, err := n.syncDoHeartbeat(ctx)

	// Verify: when majority fail
	assert.Error(t, err)
	assert.Equal(t, false, result.ShallDegrade)

}

func TestNode_syncDoHeartbeat_Fail_Case2(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	// Setup mock expectations
	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return([]string{"peer1", "peer2"}, nil)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{}, nil).Times(2)
	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil).Times(2)
	raftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()

	// set the current term and voted for
	n.CurrentTerm = 1
	n.VotedFor = "node1"

	consensus := n.consensus.(*MockConsensus)
	consensus.EXPECT().ConsensusAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&AppendEntriesConsensusResp{Success: false, Term: 2}, errors.New("test error"))

	// Execute
	ctx := context.Background()
	result, err := n.syncDoHeartbeat(ctx)

	// Verify: when majority fail
	assert.Error(t, err)
	assert.Equal(t, false, result.ShallDegrade)
}
