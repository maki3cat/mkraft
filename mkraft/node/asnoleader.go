package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

func (n *nodeImpl) candidateHandlePeerRequest(ctx context.Context, workerWaitGroup *sync.WaitGroup, degradeChan chan struct{}) {
	defer workerWaitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			n.logger.Info("peer-request-worker, exiting leader's worker for peer requests")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Info("peer-request-worker, exiting leader's worker for peer requests")
				return
			case req := <-n.requestVoteCh: // commonRule: handling voteRequest from another candidate
				if req.IsTimeout.Load() {
					n.logger.Warn("received a request vote from peers but it is timeout")
					continue
				}
				req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
					Resp: n.handleVoteRequest(req.Req),
					Err:  nil,
				}
				// for voteRequest, only when the term is higher,
				// the node will convert to follower
				currentTerm := n.getCurrentTerm()
				if req.Req.Term > currentTerm {
					n.setNodeState(StateFollower)
					currentTerm, state, votedFor := n.getKeyState()
					n.recordNodeState(currentTerm, state, votedFor)
					close(degradeChan)
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
				currentTerm := n.getCurrentTerm()
				if req.Req.Term >= currentTerm {
					n.setNodeState(StateFollower)
					currentTerm, state, votedFor := n.getKeyState()
					n.recordNodeState(currentTerm, state, votedFor)
					close(degradeChan)
					return
				}
			}
		}
	}
}

// noleader-WORKER-2 aside from the apply worker-1
// this worker is forever looping to handle client commands, should be called in a separate goroutine
// quits on the context done, and set the waitGroup before return
// maki: the tricky part is that the client command needs NOT to be drained but the apply signal needs to be drained
func (n *nodeImpl) noleaderWorkerForClientCommand(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
	for {
		select {
		case <-ctx.Done():
			n.logger.Info("client-command-worker, exiting leader's worker for client commands")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Info("client-command-worker, exiting leader's worker for client commands")
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
func (n *nodeImpl) asyncSendElection(ctx context.Context) chan *MajorityRequestVoteResp {

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)

	n.logger.Debug("asyncSendElection: vote for oneself")
	err := n.updateCurrentTermAndVotedForAsCandidate(false)
	if err != nil {
		n.logger.Error(
			"this shouldn't happen, bugs in updateCurrentTermAndVotedForAsCandidate",
			zap.String("requestID", requestID), zap.Error(err))
		consensusChan <- &MajorityRequestVoteResp{
			Err: err,
		}
		return consensusChan
	}

	// todo: testing should be called cancelled 1) the node degrade to folower; 2) the node starts a new election;
	timeout := n.cfg.GetElectionTimeout()
	n.logger.Debug("async election timeout", zap.Duration("timeout", timeout))
	electionCtx, _ := context.WithTimeout(ctx, timeout)
	// defer electionCancel() // this is not needed, because the ConsensusRequestVote is a shortcut method
	// and the ctx is called cancelled when the candidate shall degrade to follower

	go func() {
		req := &rpc.RequestVoteRequest{
			Term:        n.getCurrentTerm(),
			CandidateId: n.NodeId,
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
	}()
	return consensusChan
}
