package node

import (
	"context"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"

	"go.uber.org/zap"
)

type DegradeSignal struct {
	ShallDegrade bool
	VotedFor     string
	ToTerm       uint32
}

func (n *nodeImpl) initializePeerIndex() {
	peerIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		panic(err)
	}
	n.initPeerIndex(peerIDs)
}

func (n *nodeImpl) RunAsLeader(ctx context.Context) {
	n.runAsLeaderImpl(ctx)
}

func (n *nodeImpl) runAsLeaderImpl(ctx context.Context) {
	if n.getNodeState() != StateLeader {
		panic("node is not in LEADER state")
	}
	n.runLock.Lock()
	defer n.runLock.Unlock()

	// important initialization, should be before the leader really starts to work
	n.initializePeerIndex()

	// at least one slot is needed so that the sender will not block, we don't need to buffer more than one signal
	n.leaderDegradeCh = make(chan DegradeSignal, 1)
	subWorkerCtx, subWorkerCancel := context.WithCancel(ctx)

	defer subWorkerCancel()
	go n.runLeaderReceiver(subWorkerCtx)
	go n.runLogReplicaitonPipeline(subWorkerCtx)

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node main context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("raft node main context done, exiting")
				return
			case <-n.leaderDegradeCh:
				n.logger.Info("leader degrade to follower, exiting")
				subWorkerCancel()
				go n.RunAsNoLeader(ctx)
				return
			}
		}
	}
}

// handle the requestVote and appendEntries from other nodes
func (n *nodeImpl) runLeaderReceiver(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // give ctx higher priority
			n.logger.Warn("raft node main context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("raft node main context done, exiting")
				return
			case internalReq := <-n.requestVoteCh:
				unitResult, err := n.recvRequestVoteAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling request vote", zap.Error(err))
					panic(err)
				}
				if unitResult.ShallDegrade {
					// todo: change this to send a signal to the pipeline
					close(n.leaderDegradeCh)
					return
				}
			case internalReq := <-n.appendEntryCh:
				unitResult, err := n.recvAppendEntriesAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling append entries", zap.Error(err))
					panic(err)
				}
				if unitResult.ShallDegrade {
					// todo: change this to send a signal to the pipeline
					close(n.leaderDegradeCh)
					return
				}
			}
		}
	}
}

// ---------------------------------------the receiver units for the leader-------------------------------------

// critical section: readwrite the meta-state, protected by the stateRWLock
// fast, no IO
func (n *nodeImpl) recvAppendEntriesAsLeader(internalReq *utils.AppendEntriesInternalReq) (DegradeSignal, error) {
	requestID := common.GetRequestID(internalReq.Ctx)
	n.logger.Debug("recvAppendEntriesAsLeader: received an append entries request", zap.Any("req", internalReq.Req), zap.String("requestID", requestID))
	req := internalReq.Req
	reqTerm := req.Term
	resp := new(rpc.AppendEntriesResponse)
	defer func() {
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: resp,
			Err:  nil,
		}
		n.logger.Debug("recvAppendEntriesAsLeader: returned an append entries response", zap.Any("resp", resp), zap.String("requestID", requestID))
	}()

	// protect the state read/write
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	if reqTerm > n.CurrentTerm {
		n.ToFollower(req.LeaderId, reqTerm, true)
		// implementation gap:
		// maki: this may be a very tricky design in implementation,
		// but this simplifies the logic here
		// 3rd reply the response, we directly reject, and fix the log after
		// the leader degrade to follower

		// todo: tricky case
		// so this gives rise to a case the leader gets false in appendEntries but the term is same
		// the leader should retry the appendEntries for 3 times, if still false, should fail the clientCommands?
		resp.Success = false
		resp.Term = reqTerm
		return DegradeSignal{ShallDegrade: true}, nil
	} else if reqTerm < n.CurrentTerm {
		resp.Term = n.CurrentTerm
		resp.Success = false
		return DegradeSignal{ShallDegrade: false}, nil
	} else {
		panic("shouldn't happen, break the property of Election Safety")
	}
}

// critical section: readwrite the meta-state, protected by the stateRWLock
// fast, no IO
func (n *nodeImpl) recvRequestVoteAsLeader(internalReq *utils.RequestVoteInternalReq) (DegradeSignal, error) {

	req := internalReq.Req
	resp := new(rpc.RequestVoteResponse)
	defer func() {
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
			Resp: resp,
			Err:  nil,
		}
	}()

	// synchronization
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()

	if n.CurrentTerm < req.Term {
		lastLogIdx, lastLogTerm := n.raftLog.GetLastLogIdxAndTerm()
		if (req.LastLogTerm > lastLogTerm) || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIdx) {
			err := n.ToFollower(req.CandidateId, req.Term, true) // requestVote comes from the candidate, so we use the candidateId
			if err != nil {
				n.logger.Error("error in ToFollower", zap.Error(err))
				panic(err)
			}
			resp.Term = n.CurrentTerm
			resp.VoteGranted = true
			return DegradeSignal{ShallDegrade: true}, nil
		}
	}

	// the leader should reject the vote in all other cases
	resp.Term = n.CurrentTerm
	resp.VoteGranted = false
	return DegradeSignal{ShallDegrade: false}, nil
}
