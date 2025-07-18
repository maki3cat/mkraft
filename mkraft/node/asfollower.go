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

/*
PAPER:

Follower are passive and they don't initiate any requests,
and only respond to requests from candidates and leaders.
If times out, the follower will convert to candidate state.

implementation gap:
the control flow of the FOLLOWER is :
- (RECEIVE: main thread) the main flow to receive requests sent by the peers(leader/candidate), heartbeating is related with this flow;
- (SENDER: not applicable) follower doesn't send any requests;
- (APPLY: worker thread) the worker to apply logs to the state machine, so the application doesn't block the receiver;
- (CLEANER: worker thread) the cleaner to reject client commands, so this dirty messages don't interfere with the main flow;
*/
func (n *nodeImpl) RunAsFollower(ctx context.Context) {
	n.logger.Info("node starts as follower...")
	if n.getNodeState() != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	n.logger.Info("STATE CHANGE: node acquires to run in FOLLOWER state")
	n.runLock.Lock()
	defer n.runLock.Unlock()
	n.logger.Info("STATE CHANGE: acquired semaphore in FOLLOWER state")

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(2)

	electionTimeout := n.cfg.GetElectionTimeout()
	n.logger.Info("election timeout", zap.Duration("electionTimeout", electionTimeout))
	electionTicker := time.NewTicker(electionTimeout)
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())

	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)

	defer func() { // gracefully exit for follower state is easy
		n.logger.Info("node is exiting the follower state")
		electionTicker.Stop()
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("node has exited the follower state successfully")
	}()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("raft node's main context done, exiting")
					return

				case <-electionTicker.C:
					n.logger.Info("election timeout, converting to candidate")
					err := n.ToCandidate(false)
					if err != nil {
						n.logger.Error("key error: in fromFollowerToCandidate", zap.Error(err))
						continue // wait for the next election timeout
					}
					go n.RunAsCandidate(ctx)
					return

				case requestVoteInternal := <-n.requestVoteCh:
					electionTimeout = n.cfg.GetElectionTimeout()
					n.logger.Info("election timeout", zap.Duration("electionTimeout", electionTimeout))
					electionTicker.Reset(electionTimeout)

					resp := n.handleVoteRequestAsNoLeader(requestVoteInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespChan <- &wrappedResp

				case appendEntryInternal := <-n.appendEntryCh:
					electionTimeout = n.cfg.GetElectionTimeout()
					n.logger.Info("election timeout", zap.Duration("electionTimeout", electionTimeout))
					electionTicker.Reset(electionTimeout)

					resp := n.receiveAppendEntriesAsNoLeader(ctx, appendEntryInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					appendEntryInternal.RespChan <- &wrappedResp
				}
			}
		}
	}
}

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
