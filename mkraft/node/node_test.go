package node

import (
	"fmt"
	"os"
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------grantVote---------------------------------
func TestNode_grantVote_basicsRules(t *testing.T) {
	n, ctrl := newMockNodeWithNoExpectations(t)
	defer cleanUpTmpDir(ctrl)
	t.Run("test case 0: current term is larger than new term", func(t *testing.T) {
		n.CurrentTerm = 2
		granted := n.grantVote(10, 0, 1, n.NodeId)
		assert.False(t, granted)
	})

	t.Run("test case 1: current term is same with the new term, but voteFor is not the candidate", func(t *testing.T) {
		n.CurrentTerm = 0
		n.VotedFor = ""
		granted := n.grantVote(10, 0, 0, n.NodeId)
		assert.False(t, granted)
	})
	t.Run("test case 2: current term is same with the new term, and voteFor is the candidate", func(t *testing.T) {
		n.CurrentTerm = 0
		n.VotedFor = "node2"
		granted := n.grantVote(10, 0, 0, "node2")
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(0))
	})

}

func TestNode_grantVote_voteRestrictions(t *testing.T) {
	n, ctrl := newMockNodeWithNoExpectations(t)
	defer cleanUpTmpDir(ctrl)
	t.Run("test case 1: current term is lower than the new term, leader raftlog last term is higher but length lower", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(8, 2, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
	t.Run("test case 2: current term is lower than the new term, leader raftlog last term is same but length lower", func(t *testing.T) {
		n.CurrentTerm = 0
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(9, 0, 1, n.NodeId)
		assert.False(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(0))
	})
	t.Run("test case 3: current term is lower than the new term, leader raftlog last term is same but length same", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(10, 0, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
	t.Run("test case 4: current term is lower than the new term, leader raftlog last term is same but length higher", func(t *testing.T) {
		n.CurrentTerm = 0
		assert.NotEqual(t, n.CurrentTerm, uint32(1))
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))
		granted := n.grantVote(11, 0, 1, n.NodeId)
		assert.True(t, granted)
		assert.Equal(t, n.CurrentTerm, uint32(1))
	})
}

// ---------------------------------recordNodeState---------------------------------
func TestNode_recordNodeState(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.NodeId = "test-node-1"
	n.state = StateLeader
	n.CurrentTerm = 5
	n.VotedFor = "test-node-2"

	n.recordNodeState(n.CurrentTerm, StateLeader, n.VotedFor)

	// Verify file exists and contains node ID
	filePath := getStateFilePath(n.cfg.GetDataDir())
	data, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Contains(t, string(data), n.NodeId)
	assert.Contains(t, string(data), "leader")                           // State is stored as lowercase
	assert.Contains(t, string(data), fmt.Sprintf("#%d#", n.CurrentTerm)) // Verify term is included
}

// ---------------------------------handleVoteRequest---------------------------------
func TestNode_handleVoteRequest(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("vote granted", func(t *testing.T) {
		n.CurrentTerm = 0
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))

		req := &rpc.RequestVoteRequest{
			Term:         1,
			CandidateId:  "test-node-1",
			LastLogIndex: 11,
			LastLogTerm:  0,
		}

		resp := n.handleVoteRequest(req)
		assert.True(t, resp.VoteGranted)
		assert.Equal(t, uint32(1), resp.Term)
	})

	t.Run("vote not granted", func(t *testing.T) {
		n.CurrentTerm = 0
		raftLog := n.raftLog.(*log.MockRaftLogs)
		raftLog.EXPECT().GetLastLogIdxAndTerm().Return(uint64(10), uint32(0))

		req := &rpc.RequestVoteRequest{
			Term:         1,
			CandidateId:  "test-node-1",
			LastLogIndex: 9,
			LastLogTerm:  0,
		}

		resp := n.handleVoteRequest(req)
		assert.False(t, resp.VoteGranted)
		assert.Equal(t, uint32(0), resp.Term)
	})
}

// ---------------------------------VoteRequest---------------------------------
func TestNode_VoteRequest(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("request accepted", func(t *testing.T) {
		respChan := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
		req := &utils.RequestVoteInternalReq{
			RespChan: respChan,
		}
		n.VoteRequest(req)
		// Verify request was added to channel
		select {
		case <-n.requestVoteCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		// Fill up channel
		for i := 0; i < cap(n.requestVoteCh); i++ {
			n.requestVoteCh <- &utils.RequestVoteInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
		req := &utils.RequestVoteInternalReq{
			RespChan: respChan,
		}
		n.VoteRequest(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}

// ---------------------------------AppendEntryRequest---------------------------------
func TestNode_AppendEntryRequest(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("request accepted", func(t *testing.T) {
		respChan := make(chan *utils.RPCRespWrapper[*rpc.AppendEntriesResponse], 1)
		req := &utils.AppendEntriesInternalReq{
			RespChan: respChan,
		}
		n.AppendEntryRequest(req)
		// Verify request was added to channel
		select {
		case <-n.appendEntryCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		// Fill up channel
		for i := 0; i < cap(n.appendEntryCh); i++ {
			n.appendEntryCh <- &utils.AppendEntriesInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.AppendEntriesResponse], 1)
		req := &utils.AppendEntriesInternalReq{
			RespChan: respChan,
		}
		n.AppendEntryRequest(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}

// ---------------------------------ClientCommand---------------------------------
func TestNode_ClientCommand(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("not leader", func(t *testing.T) {
		n.state = StateFollower
		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrNotLeader, resp.Err)
		default:
			t.Error("No error response received")
		}
	})

	t.Run("request accepted", func(t *testing.T) {
		n.state = StateLeader
		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)
		// Verify request was added to channel
		select {
		case <-n.clientCommandCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		n.state = StateLeader
		// Fill up channel
		for i := 0; i < cap(n.clientCommandCh); i++ {
			n.clientCommandCh <- &utils.ClientCommandInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}
