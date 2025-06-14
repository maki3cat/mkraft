package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// ---------------------------------getLogsToCatchupForPeers------------------------------------
func TestNode_getLogsToCatchupForPeers_HappyPath(t *testing.T) {
	n, ctrl := newMockNodeWithNoExpectations(t)
	defer cleanUpTmpDir(ctrl)

	// set different nextIndex for each peer
	n.nextIndex = map[string]uint64{
		"peer1": 1,
		"peer2": 3,
	}

	// peer2 is the same with the leader, while peer1 is behind
	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{
		{Commands: []byte("test1")},
		{Commands: []byte("test2")},
	}, nil).Times(1)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(3)).Return([]*log.RaftLogEntry{}, nil).Times(1)

	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil).Times(1)
	raftLog.EXPECT().GetTermByIndex(uint64(2)).Return(uint32(2), nil).Times(1)

	peerNodeIDs := []string{"peer1", "peer2"}
	result, err := n.getLogsToCatchupForPeers(peerNodeIDs)

	// peer one
	assert.NoError(t, err)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, uint64(0), result["peer1"].LastLogIndex)
	assert.Equal(t, uint32(1), result["peer1"].LastLogTerm)
	assert.Equal(t, 2, len(result["peer1"].Entries))
	assert.Equal(t, []byte("test1"), result["peer1"].Entries[0].Commands)
	assert.Equal(t, []byte("test2"), result["peer1"].Entries[1].Commands)

	// peer two
	assert.Equal(t, uint64(2), result["peer2"].LastLogIndex)
	assert.Equal(t, uint32(2), result["peer2"].LastLogTerm)
	assert.Equal(t, 0, len(result["peer2"].Entries))
}

func TestNode_getLogsToCatchupForPeers_ReadLogsError(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return(nil, errors.New("read error"))

	peerNodeIDs := []string{"peer1"}
	result, err := n.getLogsToCatchupForPeers(peerNodeIDs)

	assert.Error(t, err)
	assert.Nil(t, result)
}

// ---------------------------------handlerAppendEntriesAsLeader---------------------------------
func TestNode_handlerAppendEntriesAsLeader_HigherTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.CurrentTerm = 1
	req := &utils.AppendEntriesInternalReq{
		Req: &rpc.AppendEntriesRequest{
			Term: 2,
		},
		RespChan: make(chan *utils.RPCRespWrapper[*rpc.AppendEntriesResponse], 1),
	}

	result, err := n.handlerAppendEntriesAsLeader(req)

	assert.NoError(t, err)
	assert.True(t, result.ShallDegrade)
	assert.Equal(t, TermRank(2), result.Term)

	resp := <-req.RespChan
	assert.Equal(t, uint32(1), resp.Resp.Term)
	assert.False(t, resp.Resp.Success)
}

// ---------------------------------handleRequestVoteAsLeader---------------------------------
func TestNode_handleRequestVoteAsLeader_VoteGranted(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.CurrentTerm = 1
	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(0), uint32(0))

	req := &utils.RequestVoteInternalReq{
		Req: &rpc.RequestVoteRequest{
			Term:         2,
			CandidateId:  "candidate1",
			LastLogIndex: 1,
			LastLogTerm:  1,
		},
		RespChan: make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1),
	}

	result, err := n.handleRequestVoteAsLeader(req)

	assert.NoError(t, err)
	assert.True(t, result.ShallDegrade)
	assert.Equal(t, TermRank(2), result.Term)

	resp := <-req.RespChan
	assert.True(t, resp.Resp.VoteGranted)
	assert.Equal(t, uint32(2), resp.Resp.Term)
}

func TestNode_handleRequestVoteAsLeader_SameTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.storeCurrentTermAndVotedFor(2, "node1", true)
	req := &utils.RequestVoteInternalReq{
		Req: &rpc.RequestVoteRequest{
			Term:         2,
			CandidateId:  "candidate1",
			LastLogIndex: 1,
			LastLogTerm:  2,
		},
		RespChan: make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1),
	}

	result, err := n.handleRequestVoteAsLeader(req)

	assert.NoError(t, err)
	assert.False(t, result.ShallDegrade)

	resp := <-req.RespChan
	assert.False(t, resp.Resp.VoteGranted)
	assert.Equal(t, uint32(2), resp.Resp.Term)
}

func TestNode_handleRequestVoteAsLeader_LowerTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.storeCurrentTermAndVotedFor(3, "node1", true)

	req := &utils.RequestVoteInternalReq{
		Req: &rpc.RequestVoteRequest{
			Term:         2,
			CandidateId:  "candidate1",
			LastLogIndex: 1,
			LastLogTerm:  2,
		},
		RespChan: make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1),
	}

	result, err := n.handleRequestVoteAsLeader(req)

	assert.NoError(t, err)
	assert.False(t, result.ShallDegrade)

	resp := <-req.RespChan
	assert.False(t, resp.Resp.VoteGranted)
	assert.Equal(t, uint32(3), resp.Resp.Term)
}

// ---------------------------------recordLeaderState---------------------------------
func TestNode_recordLeaderState(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	defer func() {
		err := os.Remove("leader.tmp")
		assert.NoError(t, err)
	}()

	n.NodeId = "test-node-1"
	n.recordLeaderState()

	// Verify file exists and contains node ID
	data, err := os.ReadFile("leader.tmp")
	assert.NoError(t, err)
	assert.Contains(t, string(data), "test-node")

	time.Sleep(10 * time.Millisecond)

	n.NodeId = "test-node-2"
	n.recordLeaderState()

	data, err = os.ReadFile("leader.tmp")
	assert.NoError(t, err)
	// split data by \n
	dataStr := string(data)
	lines := strings.Split(dataStr, "\n")
	fmt.Println(lines)
	assert.Contains(t, lines[0], "test-node-1")
	assert.Contains(t, lines[1], "test-node-2")
}

// --------------------------------- syncDoLogReplication ---------------------------------
// todo: add more test cases for this method
func TestNode_syncDoLogReplication_HappyPath(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return([]string{"peer1"}, nil)

	raftLog := n.raftLog.(*log.MockRaftLogs)
	raftLog.EXPECT().ReadLogsInBatchFromIdx(uint64(1)).Return([]*log.RaftLogEntry{}, nil)
	raftLog.EXPECT().GetTermByIndex(uint64(0)).Return(uint32(1), nil)
	raftLog.EXPECT().AppendLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	consensus := n.consensus.(*MockConsensus)
	consensus.EXPECT().ConsensusAppendEntries(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		&AppendEntriesConsensusResp{Success: true, Term: 1}, nil)

	n.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, 1)
	clientCmd := &utils.ClientCommandInternalReq{
		Req: &rpc.ClientCommandRequest{
			Command: []byte("test"),
		},
	}

	result, err := n.syncDoLogReplication(context.Background(), []*utils.ClientCommandInternalReq{clientCmd})

	assert.NoError(t, err)
	assert.False(t, result.ShallDegrade)
}

// --------------------------------- syncDoHeartbeat ---------------------------------
func TestNode_syncDoHeartbeat_GetPeerIDsError(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	membership := n.membership.(*peers.MockMembership)
	membership.EXPECT().GetAllPeerNodeIDs().Return(nil, errors.New("peer error"))

	result, err := n.syncDoHeartbeat(context.Background())

	assert.Error(t, err)
	assert.False(t, result.ShallDegrade)
}

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
