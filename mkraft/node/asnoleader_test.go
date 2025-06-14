package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"

	"go.uber.org/mock/gomock"
)

// ----------noleaderWorkerForClientCommand------
func TestNode_noleaderWorkerForClientCommand_ContextCancelled(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	n.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, 10)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	bg := context.Background()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go n.noleaderWorkerForClientCommand(ctx, wg)
	cancel()
	time.Sleep(100 * time.Millisecond)
	wg.Wait()
}

func TestNode_noleaderWorkerForClientCommand_HappyPath(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)
	n.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, 10)
	req := &utils.ClientCommandInternalReq{
		Req: &rpc.ClientCommandRequest{
			Command: []byte("test command"),
		},
		RespChan: make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1),
	}
	n.leaderApplyCh <- req
	wg := &sync.WaitGroup{}
	wg.Add(1)
	bg := context.Background()
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	go n.noleaderWorkerForClientCommand(ctx, wg)
	time.Sleep(100 * time.Millisecond)
	select {
	case resp, ok := <-req.RespChan:
		assert.True(t, ok)
		assert.Equal(t, common.ErrNotLeader, resp.Err)
		assert.Nil(t, resp.Resp.Result)
	default:
		assert.Fail(t, "should not happen")
	}
	cancel()
	wg.Wait()
}

// ---------------------------------receiveAppendEntriesAsNoLeader---------------------------------
func TestNode_receiveAppendEntriesAsNoLeader_HappyPath(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.noleaderApplySignalCh = make(chan bool, 10)
	mockedLog := n.raftLog.(*log.MockRaftLogs)
	mockedLog.EXPECT().CheckPreLog(gomock.Any(), gomock.Any()).Return(true)
	mockedLog.EXPECT().UpdateLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	req := &rpc.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*rpc.LogEntry{
			{Data: []byte("test1")},
			{Data: []byte("test2")},
		},
		LeaderCommit: 1,
	}
	fmt.Println("req", req, "currentTerm", n.getCurrentTerm())

	resp := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.True(t, resp.Success)
	assert.Equal(t, uint32(1), resp.Term)

	// Verify apply signal was sent
	select {
	case <-n.noleaderApplySignalCh:
	default:
		assert.Fail(t, "apply signal not sent")
	}
}

func TestNode_receiveAppendEntriesAsNoLeader_StaleTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.storeCurrentTermAndVotedFor(2, "", false)

	req := &rpc.AppendEntriesRequest{
		Term:         1, // Stale term
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*rpc.LogEntry{},
		LeaderCommit: 0,
	}

	resp := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.False(t, resp.Success)
	assert.Equal(t, uint32(2), resp.Term)
}

func TestNode_receiveAppendEntriesAsNoLeader_PreLogMismatch(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedLog := n.raftLog.(*log.MockRaftLogs)
	mockedLog.EXPECT().CheckPreLog(gomock.Any(), gomock.Any()).Return(false)

	req := &rpc.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader1",
		PrevLogIndex: 5, // Mismatched index
		PrevLogTerm:  2,
		Entries:      []*rpc.LogEntry{},
		LeaderCommit: 0,
	}

	resp := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.False(t, resp.Success)
	assert.Equal(t, uint32(0), resp.Term)
}

func TestNode_receiveAppendEntriesAsNoLeader_HigherTerm(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.noleaderApplySignalCh = make(chan bool, 10)
	mockedLog := n.raftLog.(*log.MockRaftLogs)
	mockedLog.EXPECT().CheckPreLog(gomock.Any(), gomock.Any()).Return(true)

	req := &rpc.AppendEntriesRequest{
		Term:         2, // Higher term
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*rpc.LogEntry{},
		LeaderCommit: 0,
	}

	resp := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.True(t, resp.Success)
	assert.Equal(t, uint32(2), resp.Term)
}

func TestNode_receiveAppendEntriesAsNoLeader_UpdateLogError(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	n.noleaderApplySignalCh = make(chan bool, 10)
	mockedLog := n.raftLog.(*log.MockRaftLogs)
	mockedLog.EXPECT().CheckPreLog(gomock.Any(), gomock.Any()).Return(true)
	mockedLog.EXPECT().UpdateLogsInBatch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(common.ErrPreLogNotMatch)

	req := &rpc.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*rpc.LogEntry{
			{Data: []byte("test")},
		},
		LeaderCommit: 0,
	}

	resp := n.receiveAppendEntriesAsNoLeader(context.Background(), req)
	assert.False(t, resp.Success)
	assert.Equal(t, uint32(0), resp.Term)
}

// ---------------------------------asyncSendElection---------------------------------

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
