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

	if n.getNodeState() != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	n.logger.Info("STATE CHANGE: node starts to acquiring CANDIDATE state")
	n.runLock.Lock()
	defer n.runLock.Unlock()
	n.logger.Info("STATE CHANGE: node has acquired semaphore in CANDIDATE state")

	// there is no tikcer in this cancdiate state, and we use this election return as a de facto ticker
	// the candidate replies on the election to trigger recording of state
	consensusChan := n.asyncSendElection(ctx, false)

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(2)
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)

	defer func() {
		n.logger.Info("worker is exiting the candidate state")
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("candidate worker exited successfully")
	}()

	reElectionTimer := time.NewTimer(n.cfg.GetElectionTimeout())
	defer reElectionTimer.Stop()

	for {
		currentTerm, _, _ := n.getKeyState()
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {

				case <-reElectionTimer.C:
					// the elect maintains the candidate state and vote for, but changes the term
					n.logger.Debug("ELECTION: candidate timer fires, starts a new election")
					consensusChan = n.asyncSendElection(ctx, true)
					reElectionTimer.Reset(n.cfg.GetElectionTimeout())

				case <-ctx.Done():
					n.logger.Warn("raft node's main context done, exiting")
					return

				case response, ok := <-consensusChan:
					// the vote granted can change the candidate to leader or follower or stay the same
					if !ok {
						panic("consensusChan should never be closed")
					}
					if response.Err == common.ErrNotCandidate {
						n.logger.Warn("the node has is exiting the candidate state")
						// pass current select
						continue
					}
					reElectionTimer.Reset(n.cfg.GetElectionTimeout())
					if response.Err != nil {
						n.logger.Error(
							"candidate has error in election, try to re-elect after another election timeout",
							zap.Error(response.Err))
						continue
					}
					if response.VoteGranted {
						n.ToLeader()
						n.cleanupApplyLogsBeforeToLeader()
						n.logger.Info("STATE CHANGE: candidate is upgraded to leader")
						go n.RunAsLeader(ctx)
						return
					} else {
						if response.Term > currentTerm {
							// keypoint: here the vote for shall be the one that sends the higher term,
							// or when the one comes to ask for vote, it will get true
							// if we save empty, we will not be able to get who wins the vote
							err := n.ToFollower(response.PeerNodeIDWithHigherTerm, response.Term, false)
							if err != nil {
								n.logger.Error("key error: in ToFollower", zap.Error(err))
								panic(err)
							}
							go n.RunAsFollower(ctx)
							return
						} else {
							n.logger.Warn(
								"not enough votes, re-elect again",
								zap.Int("term", int(currentTerm)), zap.String("nId", n.NodeId))
							consensusChan = n.asyncSendElection(ctx, true)
						}
					}

				// if we separate these from the main loop, we need to test asyncSendElection checks the state
				case req := <-n.requestVoteCh: // commonRule: handling voteRequest from another candidate
					if req.IsTimeout.Load() {
						n.logger.Warn("received a request vote from peers but it is timeout")
						continue
					}
					resp := n.handleVoteRequestAsNoLeader(req.Req) // state changed inside
					req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					if resp.VoteGranted {
						go n.RunAsFollower(ctx)
						return
					}

				case req := <-n.appendEntryCh: // commonRule: handling appendEntry from a leader which can be stale or new
					if req.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
					resp := n.receiveAppendEntriesAsNoLeader(ctx, req.Req)
					req.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					// if the state if follower just degrade
					if n.getNodeState() == StateFollower {
						go n.RunAsFollower(ctx)
						return
					}
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
func (n *nodeImpl) asyncSendElection(ctx context.Context, updateCandidateTerm bool) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	// check if the node is degraded to follower
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	if n.getNodeState() == StateFollower {
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
	term, state, _ := n.getKeyState()
	if state != StateCandidate {
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
	}(term)
	return consensusChan
}
