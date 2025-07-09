package node

import (
	"context"
	"sync"
	"time"

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
	n.sem.Acquire(ctx, 1)
	n.logger.Info("STATE CHANGE: node has acquired semaphore in CANDIDATE state")

	// there is no tikcer in this cancdiate state, and we use this election return as a de facto ticker
	// the candidate replies on the election to trigger recording of state
	consensusChan := n.asyncSendElection(ctx)
	degradeChan := make(chan struct{}) // if the node is degraded to follower, this channel will be closed

	workerCtx, workerCancel := context.WithCancel(ctx)
	workerWaitGroup := sync.WaitGroup{}
	workerWaitGroup.Add(3)
	n.noleaderApplySignalCh = make(chan bool, n.cfg.GetRaftNodeRequestBufferSize())
	go n.noleaderWorkerToApplyLogs(workerCtx, &workerWaitGroup)
	go n.noleaderWorkerForClientCommand(workerCtx, &workerWaitGroup)
	go n.candidateHandlePeerRequest(workerCtx, &workerWaitGroup, degradeChan)

	defer func() {
		n.logger.Info("worker is exiting the candidate state")
		workerCancel()
		workerWaitGroup.Wait() // cancel only closes the Done channel, it doesn't wait for the worker to exit
		n.logger.Info("candidate worker exited successfully")
		n.sem.Release(1)
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
					n.logger.Debug("ELECTION: candidate timer fires, starts a new election")
					consensusChan = n.asyncSendElection(ctx)
					reElectionTimer.Reset(n.cfg.GetElectionTimeout())
				case <-ctx.Done():
					n.logger.Warn("raft node's main context done, exiting")
					return
				case response, ok := <-consensusChan:
					if !ok {
						panic("consensusChan should never be closed")
					}
					reElectionTimer.Reset(n.cfg.GetElectionTimeout())
					if response.Err != nil {
						n.logger.Error("error in consensusChan, try to re-elect after another election timeout", zap.Error(response.Err))
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
							err := n.ToFollower("", response.Term, false)
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
							consensusChan = n.asyncSendElection(ctx)
						}
					}
				case <-degradeChan:
					n.logger.Info("STATE CHANGE: candidate is degraded to follower")
					go n.RunAsFollower(ctx)
					return
				}
			}
		}
	}
}
