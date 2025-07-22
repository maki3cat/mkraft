package node

import (
	"context"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"

	"go.uber.org/zap"
)

// ---------------------------------------CONTROL FLOW: THE LEADER-------------------------------------
/*
SECTION1: THE COMMON RULE (paper)
If any RPC request or response is received from a server with a higher term,
convert to follower

How the Shared Rule works for Leaders with 3 scenarios:
(MAKI comments)
(1) response of AppendEntries RPC sent by itself (OK)
(2) receive request of AppendEntries RPC from a server with a higher term (OK)
(3) receive request of RequestVote RPC from a server with a higher term (OK)

// SECTION2: SPECIFICAL RULE FOR LEADERS (paper)
(1) Upon election:

	send initial empty AppendEntries (heartbeat) RPCs to each reserver;
	repeat during idle periods to prevent election timeouts; (5.2) (OK)

(2) If command received from client:

	append entry to local log, respond after entry applied to state machine; (5.3) (OK)

(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
todo: this paper doesn't mention how a stale leader catches up and becomes a follower
*/
func (n *nodeImpl) RunAsLeader(ctx context.Context) {
	n.runAsLeaderImpl(ctx)
}

// SINGLE GOROUTINE WITH BATCHING PATTERN
// maki: take this simple version as the baseline, and add more gorouintes if this has performance issues
// analysis of why this pattern can be used:
// task2, task4 have data race if using different goroutine -> they both change raftlog/state machine in a serial order
// so one way is to use the same goroutine for task2 and task4 to handle log replication
// normally, the leader only has task4, and only in unstable conditions, it has to handle task2 and task4 at the same time
// if task4 accepts the log replication, the leader shall degrade to follower with graceful shutdown

// task3 also happens in unstable conditions, but it is less frequent than task2 so can also be in the same goroutine
// task1 has lower priority than task3

// the only worker thread needed is the log applicaiton thread
func (n *nodeImpl) runAsLeaderImpl(ctx context.Context) {

	if n.getNodeState() != StateLeader {
		panic("node is not in LEADER state")
	}
	n.logger.Info("acquiring the Semaphore as the LEADER state")
	n.runLock.Lock()
	defer n.runLock.Unlock()
	n.logger.Info("acquired the Semaphore as the LEADER state")

	degradeChan := make(chan struct{})
	subWorkerCtx, subWorkerCancel := context.WithCancel(ctx)
	defer subWorkerCancel()

	// one for handling requestVote and appendEntries from peers
	go n.leaderReceiverWorker(subWorkerCtx, degradeChan)
	// one for handling log replication
	n.startLogReplicaitonPipeline(subWorkerCtx)

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
			case <-degradeChan:
				n.logger.Info("leader degrade to follower, exiting")
				subWorkerCancel()
				go n.RunAsNoLeader(ctx)
				return
			}
		}
	}
}

// handle the requestVote and appendEntries from other nodes
func (n *nodeImpl) leaderReceiverWorker(ctx context.Context, degradeChan chan struct{}) {
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
					close(degradeChan)
					return
				}
			case internalReq := <-n.appendEntryCh:
				unitResult, err := n.recvAppendEntriesAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling append entries", zap.Error(err))
					panic(err)
				}
				if unitResult.ShallDegrade {
					close(degradeChan)
					return
				}
			}
		}
	}
}

// ---------------------------------------the receiver units for the leader-------------------------------------

// critical section: readwrite the meta-state, protected by the stateRWLock
// fast, no IO
func (n *nodeImpl) recvAppendEntriesAsLeader(internalReq *utils.AppendEntriesInternalReq) (UnitResult, error) {
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
		return UnitResult{ShallDegrade: true}, nil
	} else if reqTerm < n.CurrentTerm {
		resp.Term = n.CurrentTerm
		resp.Success = false
		return UnitResult{ShallDegrade: false}, nil
	} else {
		panic("shouldn't happen, break the property of Election Safety")
	}
}

// critical section: readwrite the meta-state, protected by the stateRWLock
// fast, no IO
func (n *nodeImpl) recvRequestVoteAsLeader(internalReq *utils.RequestVoteInternalReq) (UnitResult, error) {

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
			return UnitResult{ShallDegrade: true}, nil
		}
	}

	// the leader should reject the vote in all other cases
	resp.Term = n.CurrentTerm
	resp.VoteGranted = false
	return UnitResult{ShallDegrade: false}, nil
}
