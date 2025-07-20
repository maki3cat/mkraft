package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// SHARED BY FOLLOWER AND CANDIDATE
func (n *nodeImpl) handleVoteRequestAsNoLeader(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
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
// maki: lastLogIndex, commitIndex, lastApplied can be totally different from each other
// shall be called when the node is not a leader
// the raft server is generally single-threaded, so there is no other thread to change the commitIdx
func (n *nodeImpl) receiveAppendEntriesAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	requestID := common.GetRequestID(ctx)
	n.logger.Debug("receiveAppendEntriesAsNoLeader: received an append entries request", zap.Any("req", req), zap.String("requestID", requestID))

	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm, _, _ := n.getKeyState()

	// return FALSE CASES:
	// (1) fast track for the stale term
	// (2) check the prevLogIndex and prevLogTerm
	if reqTerm < currentTerm || !n.raftLog.CheckPreLog(req.PrevLogIndex, req.PrevLogTerm) {
		response = rpc.AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
		return &response
	}

	// return TRUE CASES:
	// 1. udpate commitIdx, and trigger the apply
	defer func() {
		// the updateCommitIdx will find the min(leaderCommit, index of last new entry in the log), so the update
		// doesn't require result of appendLogs
		n.incrementCommitIdx(uint64(len(req.Entries)))
		n.noleaderApplySignalCh <- true
	}()

	// 2. update the term
	// is the node is not a leader, we don't need the term to be larger, we only need it to be no-less
	returnedTerm := currentTerm
	if (reqTerm > currentTerm) || (reqTerm == currentTerm && n.state == StateCandidate) {
		err := n.ToFollower(req.LeaderId, reqTerm, false)
		if err != nil {
			n.logger.Error("key error: in ToFollower", zap.Error(err))
			panic(err)
		}
		returnedTerm = reqTerm
	}

	// 3. append logs
	if len(req.Entries) > 0 {
		err := n.updateLogsAsNoLeader(ctx, req)
		if err != nil {
			if err == common.ErrPreLogNotMatch {
				response = rpc.AppendEntriesResponse{
					Term:    returnedTerm,
					Success: false,
				}
				return &response
			}
			n.logger.Error("error in UpdateLogsInBatch", zap.Error(err))
			panic(err)
		}
	}

	resp := &rpc.AppendEntriesResponse{
		Term:    returnedTerm,
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

// SHARED BY FOLLOWER AND CANDIDATE
// noleader-WORKER-2 aside from the apply worker-1
// this worker is forever looping to handle client commands, should be called in a separate goroutine
// quits on the context done, and set the waitGroup before return
// maki: the tricky part is that the client command needs NOT to be drained but the apply signal needs to be drained
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
			case cmd := <-n.leaderApplyCh:
				n.logger.Info("client-command-worker, received client command")
				// todo: add delegation to the leader
				// easy trivial work, can be done in parallel with the main logic, in case this dirty messages interfere with the main logicj
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


// Specifical Rule for Candidate Election:
// (1) increment currentTerm
// (2) vote for self
// (3) send RequestVote RPCs to all other servers
// if votes received from majority of servers: become leader
// if AppendEntries RPC received from new leader: convert to follower
// if election timeout elapses: start new election
// error handling:
// This function closes the channel when there is and error, and should be returned
// LOCKED
func (n *nodeImpl) asyncSendElection(ctx context.Context, updateCandidateTerm bool) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	// check if the node is degraded to follower
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	if n.state == StateFollower {
		consensusChan <- &MajorityRequestVoteResp{
			Err: common.ErrNotCandidate, // todo: test case for this one; and the caller should consume this err
		}
		return consensusChan
	} else {
		if updateCandidateTerm {
			n.logger.Debug("asyncSendElection: updating candidate term", zap.String("requestID", requestID))
			err := n.ToCandidate(true)
			if err != nil {
				n.logger.Error("asyncSendElection: error in ToCandidate", zap.String("requestID", requestID), zap.Error(err))
				consensusChan <- &MajorityRequestVoteResp{
					Err: err,
				}
				return consensusChan
			}
		} else {
			n.logger.Debug("asyncSendElection: not updating candidate term", zap.String("requestID", requestID))
		}
	}

	// todo: testing should be called cancelled 1) the node degrade to folower; 2) the node starts a new election;
	timeout := n.cfg.GetElectionTimeout()
	n.logger.Debug("async election timeout", zap.Duration("timeout", timeout))
	electionCtx, _ := context.WithTimeout(ctx, timeout)
	// defer electionCancel() // this is not needed, because the ConsensusRequestVote is a shortcut method
	// and the ctx is called cancelled when the candidate shall degrade to follower

	// the term may be updated previously
	if n.state != StateCandidate {
		n.logger.Warn("asyncSendElection: node is not in candidate state, returning")
		consensusChan <- &MajorityRequestVoteResp{
			Err: common.ErrNotCandidate,
		}
		return consensusChan
	}
	go func(term uint32) {
		req := &rpc.RequestVoteRequest{
			Term:        term,
			CandidateId: n.NodeId,
			// LastLogIndex: n.raftLog.GetLastLogIndex(),
			// LastLogTerm:  n.raftLog.GetLastLogTerm(),
		}
		n.logger.Debug("asyncSendElection: sending a request vote to the consensus", zap.String("requestID", requestID), zap.Any("req", req))
		resp, err := n.consensus.ConsensusRequestVote(electionCtx, req)
		if err != nil {
			n.logger.Error("asyncSendElection: error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			consensusChan <- &MajorityRequestVoteResp{
				Err: err,
			}
		} else {
			n.logger.Debug("asyncSendElection: received a response from the consensus", zap.String("requestID", requestID), zap.Any("resp", resp))
			consensusChan <- resp
		}
	}(n.CurrentTerm)
	return consensusChan
}