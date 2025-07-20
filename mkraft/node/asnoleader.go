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
	if state != StateCandidate && state != StateFollower {
		panic("node is not running as noleader")
	}

	n.runLock.Lock()
	defer n.runLock.Unlock()

	// two workers to apply logs, receive commands
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(2)
	workerCtx, workerCancel := context.WithCancel(ctx)
	// todo: why do I design something like this?
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)
	defer func() {
		n.logger.Info("exiting the noleader state")
		workerCancel()
		workerWaitGroup.Wait()
		n.logger.Info("exited the noleader state successfully")
	}()

	// election related
	var electionChan chan *MajorityRequestVoteResp
	electionTimer := time.NewTimer(n.cfg.GetElectionTimeout())
	defer electionTimer.Stop()

	for {
		currentTerm, _, _ := n.getKeyState()
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

				case <-electionTimer.C:
					n.logger.Debug("ELECTION: election timer fires, starts a new election")
					electionChan = n.asyncSendElection(ctx, true)
					electionTimer.Reset(n.cfg.GetElectionTimeout())

				case response, ok := <-electionChan:
					// the vote granted can change the candidate
					//  to leader or follower or stay the same
					if !ok {
						panic("consensusChan is not designed to be closed")
					}
					if response.Err == common.ErrNotCandidate {
						continue
					}

					electionTimer.Reset(n.cfg.GetElectionTimeout())
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
							go n.RunAsNoLeader(ctx)
							return
						} else {
							n.logger.Warn(
								"not enough votes, re-elect again",
								zap.Int("term", int(currentTerm)), zap.String("nId", n.NodeId))
							electionChan = n.asyncSendElection(ctx, true)
						}
					}

				// if we separate these from the main loop, we need to test asyncSendElection checks the state
				case req := <-n.requestVoteCh: // commonRule: handling voteRequest from another candidate
					if req.IsTimeout.Load() {
						n.logger.Warn("received a request vote from peers but it is timeout")
						continue
					}
					electionTimer.Reset(n.cfg.GetElectionTimeout())
					resp := n.handleVoteRequestAsNoLeader(req.Req) // state changed inside
					req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
				case req := <-n.appendEntryCh: // commonRule: handling appendEntry from a leader which can be stale or new
					if req.IsTimeout.Load() {
						n.logger.Warn("append entry is timeout")
						continue
					}
					electionTimer.Reset(n.cfg.GetElectionTimeout())
					resp := n.receiveAppendEntriesAsNoLeader(ctx, req.Req)
					req.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
				}
			}
		}
	}
}
