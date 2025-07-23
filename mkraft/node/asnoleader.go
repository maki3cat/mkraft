package node

import (
	"context"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

func (n *nodeImpl) RunAsNoLeader(ctx context.Context) {

	state := n.getNodeState()
	n.logger.Info("node is running as ", zap.String("state", state.String()))
	if state != StateFollower {
		panic("node is not running as noleader")
	}

	n.runLock.Lock()
	defer n.runLock.Unlock()

	// two workers to apply logs, receive commands
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(1)
	workerCtx, workerCancel := context.WithCancel(ctx)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)
	defer func() {
		n.logger.Info("exiting the noleader state")
		workerCancel()
		workerWaitGroup.Wait()
		n.logger.Info("exited the noleader state successfully")
	}()

	// election related, both candidate/follower has the timeout to elect mechanism
	var electionChan chan *MajorityRequestVoteResp
	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())
	defer electionTicker.Stop()

	for {
		n.logger.Debug("as noleader starts another loop")
		select {
		case <-ctx.Done():
			n.logger.Warn("context done, exiting")
			return
		default:
			{
				select {

				case <-ctx.Done():
					n.logger.Warn("context done, exiting")
					return

				case <-electionTicker.C:
					n.logger.Debug("election timeout, sending election request")
					nextTimeout := n.cfg.GetElectionTimeout()
					electionChan = n.asyncSendElection(ctx, nextTimeout)
					electionTicker.Reset(nextTimeout)
					n.logger.Debug("[election timeout reset] at sending election request", zap.Duration("timeout", nextTimeout))

				case response := <-electionChan:
					// the vote granted can change the candidate
					//  to leader or follower or stay the same
					if response.Err != nil {
						n.logger.Error(
							"candidate has error in election, try to re-elect after another election timeout",
							zap.Error(response.Err))
						continue
					}

					// to trigger reset the election timer, it at least,should not be a error like timeout
					timeout := n.cfg.GetElectionTimeout()
					electionTicker.Reset(timeout)
					n.logger.Debug("[election timeout reset] at handling election response", zap.Duration("timeout", timeout))

					if n.handleElectionResp(response) {
						n.logger.Info("STATE CHANGE: candidate is upgraded to leader")
						go n.RunAsLeader(ctx)
						return
					}
				// if we separate these from the main loop, we need to test asyncSendElection checks the state
				case req := <-n.requestVoteCh: // commonRule: handling voteRequest from another candidate
					if req.IsTimeout.Load() {
						n.logger.Warn("received a request vote from peers but it is timeout")
						continue
					}
					timeout := n.cfg.GetElectionTimeout()
					electionTicker.Reset(timeout)
					n.logger.Debug("[election timeout reset] at handling vote request", zap.Duration("timeout", timeout))

					resp := n.receiveVoteRequestAsNoLeader(req.Req) // state changed inside
					req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}

				case req := <-n.appendEntryCh: // commonRule: handling appendEntry from a leader which can be stale or new
					if req.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
					timeout := n.cfg.GetElectionTimeout()
					electionTicker.Reset(timeout)
					n.logger.Debug("[election timeout reset] at handling append entry", zap.Duration("timeout", timeout))

					resp := n.receiveAppendEntriesAsNoLeader(ctx, req.Req)
					n.logger.Debug("append entry response", zap.Any("resp", resp))
					req.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					n.logger.Debug("append entry response sent")
				}
			}
		}
	}
}

// ---------------------------------------basic units for the noleader-------------------------------------

// SHARED BY FOLLOWER AND CANDIDATE
func (n *nodeImpl) receiveVoteRequestAsNoLeader(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	newTerm := req.Term
	candidateId := req.CandidateId
	candidateLastLogTerm := req.LastLogTerm
	candidateLastLogIdx := req.LastLogIndex

	voteGranted := false
	currentTerm, voteFor := n.CurrentTerm, n.VotedFor

	if currentTerm < newTerm {
		lastLogIdx, lastLogTerm := n.raftLog.GetLastLogIdxAndTerm()
		if (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLastLogIdx >= lastLogIdx) {
			n.logger.Info("handleVoteRequestAsNoLeader: update term",
				zap.Int("currentTerm", int(currentTerm)),
				zap.Int("newTerm", int(newTerm)),
				zap.String("candidateId", candidateId))
			err := n.ToFollower(candidateId, newTerm, true)
			if err != nil {
				n.logger.Error("error in ToFollower", zap.Error(err))
				panic(err)
			}
			voteGranted = true
			currentTerm = newTerm
		} else {
			n.logger.Info("handleVoteRequestAsNoLeader: not update term",
				zap.String("candidateId", candidateId))
			voteGranted = false
		}
	}

	// empty voteFor should not be granted, because it may be learned from the new leader without voting for it
	if currentTerm == newTerm && voteFor == candidateId {
		voteGranted = true
	}
	return &rpc.RequestVoteResponse{
		Term: currentTerm,
		// implementation gap: I think there is no need to differentiate the updated currentTerm or the previous currentTerm
		VoteGranted: voteGranted,
	}
}

// SHARED BY FOLLOWER AND CANDIDATE
// whole function is lock-protected and should be short, the only IO is local file updates of meta state, no network

// maki: lastLogIndex, commitIndex, lastApplied can be totally different from each other
// shall be called when the node is not a leader
// the raft server is generally single-threaded, so there is no other thread to change the commitIdx
func (n *nodeImpl) receiveAppendEntriesAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	requestID := common.GetRequestID(ctx)
	n.logger.Debug("receiveAppendEntriesAsNoLeader: received an append entries request", zap.Any("req", req), zap.String("requestID", requestID))

	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)

	// return FALSE CASES:
	// (1) fast track for the stale term
	// (2) check the prevLogIndex and prevLogTerm
	if reqTerm < n.CurrentTerm || !n.raftLog.CheckPreLog(req.PrevLogIndex, req.PrevLogTerm) {
		response = rpc.AppendEntriesResponse{
			Term:    n.CurrentTerm,
			Success: false,
		}
		return &response
	}

	// return TRUE CASES:
	// 1. udpate commitIdx, and trigger the apply
	defer func() {
		// the updateCommitIdx will find the min(leaderCommit, index of last new entry in the log), so the update
		// doesn't require result of appendLogs
		n.incrementCommitIdx(uint64(len(req.Entries)), true)
	}()

	// 2. update the term
	// if current term is same, and
	if reqTerm > n.CurrentTerm ||
		(reqTerm == n.CurrentTerm && (n.state == StateCandidate || n.VotedFor != req.LeaderId)) {
		n.logger.Debug("receiveAppendEntriesAsNoLeader: update meta state")
		err := n.ToFollower(req.LeaderId, reqTerm, true)
		if err != nil {
			n.logger.Error("key error: in ToFollower", zap.Error(err))
			panic(err)
		}
	}

	// 3. append logs
	if len(req.Entries) > 0 {
		err := n.updateLogsAsNoLeader(ctx, req)
		if err != nil {
			if err == common.ErrPreLogNotMatch {
				n.logger.Error("pre log does not match")
				response = rpc.AppendEntriesResponse{
					Term:    n.CurrentTerm,
					Success: false,
				}
				return &response
			}
			n.logger.Error("haven't designed the error handling for UpdateLogsInBatch", zap.Error(err))
			panic(err)
		}
	}

	resp := &rpc.AppendEntriesResponse{
		Term:    n.CurrentTerm,
		Success: true,
	}
	n.logger.Debug("receiveAppendEntriesAsNoLeader: returned an append entries response", zap.Any("resp", resp), zap.String("requestID", requestID))
	return resp
}

// SHARED BY FOLLOWER AND CANDIDATE
// helper to append logs to the raft log
func (n *nodeImpl) updateLogsAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) error {
	if n.getNodeState() == StateLeader {
		panic("violation of Leader Append-only property: leader cannot call UpdateLogsInBatch")
	}
	logs := make([][]byte, len(req.Entries))
	for idx, entry := range req.Entries {
		logs[idx] = entry.Data
	}
	return n.raftLog.UpdateLogsInBatch(ctx, req.PrevLogIndex, logs, req.Term)
}

func (n *nodeImpl) noleaderWorkerForClientCommand(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			n.logger.Info("client-command-worker, exiting on context done")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Info("client-command-worker, exiting on context done")
				return
			case cmd := <-n.clientCommandCh:
				cmd.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
					Resp: &rpc.ClientCommandResponse{
						Result: nil,
					},
					Err: common.ErrNotLeader,
				}
			}
		}
	}
}

// this is an async function so it doesn't wait for the network IO to complete
// it is safe to lock the whole function
// but definitely, when the response comes back
// the state may be changed and we will compare that in @handleElectionResp

// Specifical Rule for Candidate Election:
// (1) increment currentTerm
// (2) vote for self
// (3) send RequestVote RPCs to all other servers
// if votes received from majority of servers: become leader
func (n *nodeImpl) asyncSendElection(ctx context.Context, timeout time.Duration) chan *MajorityRequestVoteResp {
	n.logger.Debug("asyncSendElection: sending election request", zap.Duration("timeout", timeout))
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)
	err := n.ChangeStateForElection(true)
	if err != nil {
		n.logger.Error("state change for election failed", zap.String("requestID", requestID), zap.Error(err))
		consensusChan <- &MajorityRequestVoteResp{
			Err: err,
		}
		return consensusChan
	}

	go func(term uint32) {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		req := &rpc.RequestVoteRequest{
			Term:        term,
			CandidateId: n.NodeId,
			// todo: get the last log index and term from the raft log
			// LastLogIndex: n.raftLog.GetLastLogIndex(),
			// LastLogTerm:  n.raftLog.GetLastLogTerm(),
		}
		resp := n.consensus.ConsensusRequestVote(ctxWithTimeout, req)
		err := resp.Err
		if err != nil {
			n.logger.Error("asyncSendElection: error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			consensusChan <- &MajorityRequestVoteResp{
				Err: err,
			}
		} else {
			consensusChan <- resp
		}
	}(n.CurrentTerm)
	return consensusChan
}

// return true if the node should be upgraded to leader
func (n *nodeImpl) handleElectionResp(resp *MajorityRequestVoteResp) bool {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	n.logger.Info("receiveRequestVoteResponse: received a request vote response, with current term", zap.Any("resp", resp), zap.Int("currentTerm", int(n.CurrentTerm)))
	// state check
	if n.state == StateFollower {
		n.logger.Warn("receiveRequestVoteResponse: the node is a follower, the voteRequestResult is stale")
		return false
	}
	if n.state == StateLeader {
		panic("a node cannot be a leader when the current requestVote is not handled")
	}
	// if the node is a candidate, we need to check the term
	if resp.VoteGranted {
		// what if the term is higher
		if resp.Term == n.CurrentTerm {
			n.ToLeader(true)
			n.logger.Info("STATE CHANGE: candidate is upgraded to leader")
			return true
		} else if resp.Term > n.CurrentTerm {
			panic("cannot get the vote and a higher term")
		} else {
			n.logger.Warn("the term is lower, the voteRequestResult is stale")
			return false
		}
	}
	// vote not granted
	if resp.Term > n.CurrentTerm {
		// keypoint: here the vote for shall be the one that sends the higher term,
		// or when the one comes to ask for vote, it will get true
		// if we save empty, we will not be able to get who wins the vote
		err := n.ToFollower(resp.PeerNodeIDWithHigherTerm, resp.Term, false)
		if err != nil {
			n.logger.Error("key error: in ToFollower", zap.Error(err))
			panic(err)
		}
		n.logger.Info("election failed, still running as a follower")
		return false
	}
	return false
}
