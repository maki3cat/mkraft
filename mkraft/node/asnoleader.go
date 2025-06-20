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

	if n.GetNodeState() != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")
	go n.recordNodeState() // trivial-path

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(2)
	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)
	defer func() { // gracefully exit for follower state is easy
		electionTicker.Stop()
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("follower worker exited successfully")
		n.sem.Release(1)
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
					n.SetNodeState(StateCandidate)
					go n.RunAsCandidate(ctx)
					return
				case requestVoteInternal := <-n.requestVoteCh:
					electionTicker.Reset(n.cfg.GetElectionTimeout())
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					resp := n.handleVoteRequest(requestVoteInternal.Req)
					wrappedResp := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespChan <- &wrappedResp
				case appendEntryInternal := <-n.appendEntryCh:
					if appendEntryInternal.Req.Term >= n.getCurrentTerm() {
						electionTicker.Reset(n.cfg.GetElectionTimeout())
					}
					if appendEntryInternal.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
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

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower

implementation gap:
the control flow of the FOLLOWER is :
- (SENDER: main thread) do election in an async way;
- (RECEIVER: main thread) since votes are collected asynchronously, it will be simpler and easier if we combine sender/receiver;
- (APPLY: worker thread) the worker to apply logs to the state machine, so the application doesn't block the receiver;
- (CLEANER: worker thread) the cleaner to reject client commands, so this dirty messages don't interfere with the main flow;
*/
func (n *nodeImpl) RunAsCandidate(ctx context.Context) {

	if n.GetNodeState() != StateCandidate {
		panic("node is not in CANDIDATE state")
	}
	go n.recordNodeState() // trivial-path

	n.logger.Info("node starts to acquiring CANDIDATE state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("node has acquired semaphore in CANDIDATE state")

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(2)
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)

	defer func() {
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("candidate worker exited successfully")
		n.sem.Release(1)
	}()

	// there is no tikcer in this cancdiate state, and we use this election return as a de facto ticker
	consensusChan := n.asyncSendElection(ctx)

	for {
		currentTerm := n.getCurrentTerm()
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
				case response, ok := <-consensusChan: // some response from last election

					if !ok || response == nil {
						// implementaiton gap: add timeout here on error of a node
						n.logger.Error("error in consensusChan, try to re-elect after another election timeout")
						time.Sleep(n.cfg.GetElectionTimeout())
						continue
					}

					if response.VoteGranted {
						n.SetNodeState(StateLeader)
						n.cleanupApplyLogsBeforeToLeader()
						go n.RunAsLeader(ctx)
						return
					} else {
						if response.Term > currentTerm {
							err := n.storeCurrentTermAndVotedFor(response.Term, "", false) // did not vote for anyone for this new term
							if err != nil {
								n.logger.Error(
									"error in storeCurrentTermAndVotedFor", zap.Error(err),
									zap.String("nId", n.NodeId))
								panic(err) // todo: error handling
							}

							n.SetNodeState(StateFollower)
							go n.RunAsFollower(ctx)
							return
						} else {
							n.logger.Warn(
								"not enough votes, re-elect again",
								zap.Int("term", int(currentTerm)), zap.String("nId", n.NodeId))
							consensusChan = n.asyncSendElection(ctx)
						}
					}
				case req := <-n.requestVoteCh: // commonRule: handling voteRequest from another candidate
					if !req.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: n.handleVoteRequest(req.Req),
						Err:  nil,
					}

					// for voteRequest, only when the term is higher,
					// the node will convert to follower
					if req.Req.Term > currentTerm {
						n.SetNodeState(StateFollower)
						go n.RunAsFollower(ctx)
						return
					}
				case req := <-n.appendEntryCh: // commonRule: handling appendEntry from a leader which can be stale or new
					if req.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
					req.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: n.receiveAppendEntriesAsNoLeader(ctx, req.Req),
						Err:  nil,
					}
					// maki: here is a bit tricky compared with voteRequest
					// but for appendEntry, only the term can be equal or higher
					if req.Req.Term >= currentTerm {
						n.SetNodeState(StateFollower)
						go n.RunAsFollower(ctx)
						return
					}
				}
			}
		}
	}
}

// ---------------------------------------small unit functions for noleader -------------------------------------
// noleader-WORKER-2 aside from the apply worker-1
// this worker is forever looping to handle client commands, should be called in a separate goroutine
// quits on the context done, and set the waitGroup before return
// maki: the tricky part is that the client command needs NOT to be drained but the apply signal needs to be drained
func (n *nodeImpl) noleaderWorkerForClientCommand(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("client-command-worker, exiting leader's worker for client commands")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("client-command-worker, exiting leader's worker for client commands")
				return
			case cmd := <-n.leaderApplyCh:
				n.logger.Warn("client-command-worker, received client command")
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

// maki: lastLogIndex, commitIndex, lastApplied can be totally different from each other
// shall be called when the node is not a leader
// the raft server is generally single-threaded, so there is no other thread to change the commitIdx
func (n *nodeImpl) receiveAppendEntriesAsNoLeader(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {

	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	currentTerm := n.getCurrentTerm()

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
	returnedTerm := currentTerm
	if reqTerm > currentTerm {
		returnedTerm = reqTerm
		err := n.storeCurrentTermAndVotedFor(reqTerm, "", false) // did not vote for anyone
		if err != nil {
			n.logger.Error(
				"error in storeCurrentTermAndVotedFor", zap.Error(err),
				zap.String("nId", n.NodeId))
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	// 3. append logs
	if len(req.Entries) > 0 {
		err := n.wrappedUpdateLogsInBatch(ctx, req)
		if err != nil {
			if err == common.ErrPreLogNotMatch {
				response = rpc.AppendEntriesResponse{
					Term:    returnedTerm,
					Success: false,
				}
				return &response
			}

			// todo: replace panic with returning error to the caller
			// this error cannot be not match,
			// because the prevLogIndex and prevLogTerm has been checked
			n.logger.Error("error in UpdateLogsInBatch", zap.Error(err))
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	response = rpc.AppendEntriesResponse{
		Term:    returnedTerm,
		Success: true,
	}
	return &response
}

// Property: Leader Append-only
func (n *nodeImpl) wrappedUpdateLogsInBatch(ctx context.Context, req *rpc.AppendEntriesRequest) error {
	if n.GetNodeState() == StateLeader {
		panic("violation of Leader Append-only property: leader cannot call UpdateLogsInBatch")
	}
	logs := make([][]byte, len(req.Entries))
	for idx, entry := range req.Entries {
		logs[idx] = entry.Data
	}
	return n.raftLog.UpdateLogsInBatch(ctx, req.PrevLogIndex, logs, req.Term)
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
func (n *nodeImpl) asyncSendElection(ctx context.Context) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	err := n.updateCurrentTermAndVotedForAsCandidate(false)
	if err != nil {
		n.logger.Error(
			"this shouldn't happen, bugs in updateCurrentTermAndVotedForAsCandidate",
			zap.String("requestID", requestID), zap.Error(err))
		close(consensusChan)
	}

	// todo: testing should be called cancelled 1) the node degrade to folower; 2) the node starts a new election;
	timeout := n.cfg.GetElectionTimeout()
	electionCtx, _ := context.WithTimeout(ctx, timeout)
	// defer electionCancel() // this is not needed, because the ConsensusRequestVote is a shortcut method
	// and the ctx is called cancelled when the candidate shall degrade to follower

	go func() {
		req := &rpc.RequestVoteRequest{
			Term:        n.getCurrentTerm(),
			CandidateId: n.NodeId,
		}
		resp, err := n.consensus.ConsensusRequestVote(electionCtx, req)
		if err != nil {
			n.logger.Error("error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			close(consensusChan)
		} else {
			consensusChan <- resp
		}
	}()
	return consensusChan
}
