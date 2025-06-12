package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"

	"go.uber.org/mock/gomock"
)

// --------asyncSendElection--------

func TestNode_asyncSendElection_HappyPath(t *testing.T) {

	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(&MajorityRequestVoteResp{
		Term:        1,
		VoteGranted: true,
	}, nil)

	consensusChan := n.asyncSendElection(context.Background())

	select {
	case resp, ok := <-consensusChan:
		assert.True(t, ok)
		assert.Equal(t, uint32(1), resp.Term)
		assert.True(t, resp.VoteGranted)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}

func TestNode_asyncSendElection_Err(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(nil, errors.New("test mock error"))

	consensusChan := n.asyncSendElection(context.Background())

	select {
	case resp, ok := <-consensusChan:
		assert.False(t, ok)
		assert.Nil(t, resp)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}

func TestNode_asyncSendElection_ContextCancelled(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(nil, errors.New("test mock error"))

	ctx, cancel := context.WithCancel(context.Background())
	consensusChan := n.asyncSendElection(ctx)
	cancel()

	select {
	case resp, ok := <-consensusChan:
		assert.False(t, ok)
		assert.Nil(t, resp)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}

// --------receiveAppendEntriesAsNoLeader--------
func TestNode_receiveAppendEntriesAsNoLeader_LowerTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	currentTerm := n.getCurrentTerm()
	assert.Equal(t, term, currentTerm)

	// req with term 1
	req := &rpc.AppendEntriesRequest{
		Term: 1,
	}

	response := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.Equal(t, term, response.Term)
	assert.False(t, response.Success)
}

func TestNode_receiveAppendEntriesAsNoLeader_HigherTerm_NoLogs(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	currentTerm := n.getCurrentTerm()
	assert.Equal(t, term, currentTerm)

	// req with term 3
	newTerm := uint32(3)
	req := &rpc.AppendEntriesRequest{
		Term: newTerm,
	}

	response := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.Equal(t, uint32(3), response.Term)
	assert.True(t, response.Success)
}

func TestNode_receiveAppendEntriesAsNoLeader_SameTerm_NoLogs(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	// set the nextIndex to 0
	n.nextIndex = make(map[string]uint64)
	n.nextIndex["node1"] = 0

	// req with term 2
	req := &rpc.AppendEntriesRequest{
		Term: term,
	}

	response := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.Equal(t, term, response.Term)
	assert.True(t, response.Success)
}

func TestNode_receiveAppendEntriesAsNoLeader_WithLogs_PreLogNotMatch(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	mockedRaftLog := n.raftLog.(*log.MockRaftLogs)
	mockedRaftLog.EXPECT().UpdateLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(common.ErrPreLogNotMatch)

	// req with term 2
	req := &rpc.AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*rpc.LogEntry{
			{
				Data: []byte("test command"),
			},
		},
	}

	response := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.Equal(t, term, response.Term)
	assert.False(t, response.Success)

}

func TestNode_receiveAppendEntriesAsNoLeader_WithLogs_UnknownError(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	mockedRaftLog := n.raftLog.(*log.MockRaftLogs)
	mockedRaftLog.EXPECT().UpdateLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("test mock error"))

	// req with term 2
	req := &rpc.AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*rpc.LogEntry{
			{
				Data: []byte("test command"),
			},
		},
	}
	assert.Panics(t, func() {
		n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	})
}

func TestNode_receiveAppendEntriesAsNoLeader_WithLogs_HappyPath(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	// set the current term to 1
	term := uint32(2)
	n.storeCurrentTermAndVotedFor(term, "", false)

	mockedRaftLog := n.raftLog.(*log.MockRaftLogs)
	mockedRaftLog.EXPECT().UpdateLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// req with term 2
	req := &rpc.AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []*rpc.LogEntry{
			{
				Data: []byte("test command"),
			},
		},
	}
	response := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.Equal(t, term, response.Term)
	assert.True(t, response.Success)
}
