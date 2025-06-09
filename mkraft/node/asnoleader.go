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
*/
func (n *Node) RunAsFollower(ctx context.Context) {

	if n.GetNodeState() != StateFollower {
		panic("node is not in FOLLOWER state")
	}
	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")

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
					resp := n.noleaderHandleAppendEntries(ctx, appendEntryInternal.Req)
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
How the Shared Rule works for Candidates:
(1) handle the response of RequestVoteRPC initiated by itself
(2) handle request of AppendEntriesRPC initiated by another server
(3) handle reuqest of RequestVoteRPC initiated by another server
*/
func (n *Node) RunAsCandidate(ctx context.Context) {

	if n.GetNodeState() != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

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
	consensusChan := n.asyncReElect(ctx)

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
							consensusChan = n.asyncReElect(ctx)
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
						Resp: n.noleaderHandleAppendEntries(ctx, req.Req),
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
// this worker is used to handle client commands
// which can be run independently of the main logic
// maki: the tricky part is that the client command needs NOT to be drained but the apply signal needs to be drained
func (n *Node) noleaderWorkerForClientCommand(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
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
				// easy trivial work, can be done in parallel with the main logic
				// feature: add delegation to the leader
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
func (n *Node) noleaderHandleAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {

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
	if reqTerm > currentTerm {
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
		logs := make([][]byte, len(req.Entries))
		for idx, entry := range req.Entries {
			logs[idx] = entry.Data
		}
		err := n.raftLog.UpdateLogsInBatch(ctx, req.PrevLogIndex, logs, req.Term)
		if err != nil {
			// this error cannot be not match,
			// because the prevLogIndex and prevLogTerm has been checked
			panic(err) // todo: critical error, cannot continue, not sure how to handle this
		}
	}

	response = rpc.AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
	return &response
}

// Specifical Rule for Candidate Election:
// (1) increment currentTerm
// (2) vote for self
// (3) send RequestVote RPCs to all other servers
// if votes received from majority of servers: become leader
// if AppendEntries RPC received from new leader: convert to follower
// if election timeout elapses: start new election

// the implementation:
// triggers peerCnt + 1 goroutines for fan-out rpc and fan-in the responses
func (n *Node) asyncReElect(ctx context.Context) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)
	timeout := n.cfg.GetElectionTimeout()
	electionCtx, electionCancel := context.WithTimeout(ctx, timeout)
	defer electionCancel()

	err := n.updateCurrentTermAndVotedForAsCandidate(false)
	if err != nil {
		n.logger.Error(
			"this shouldn't happen, bugs in updateCurrentTermAndVotedForAsCandidate",
			zap.String("requestID", requestID), zap.Error(err))
		close(consensusChan)
	}

	// todo: testing should be called cancelled 1) the node degrade to folower; 2) the node starts a new election;
	// maki: is this a good pattern? this consensus actually has a timeout, so can be combined
	go func() {
		req := &rpc.RequestVoteRequest{
			Term:        n.getCurrentTerm(),
			CandidateId: n.NodeId,
		}
		resp, err := n.ConsensusRequestVote(electionCtx, req)
		if err != nil {
			n.logger.Error("error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			close(consensusChan)
		} else {
			consensusChan <- resp
		}
	}()
	return consensusChan
}
