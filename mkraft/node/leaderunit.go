package node

import (
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// critical section: readwrite the meta-state, protected by the stateRWLock
func (n *nodeImpl) handlerAppendEntriesAsLeader(internalReq *utils.AppendEntriesInternalReq) (JobResult, error) {
	req := internalReq.Req
	reqTerm := req.Term
	resp := new(rpc.AppendEntriesResponse)
	defer func() {
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: resp,
			Err:  nil,
		}
	}()

	// protect the state read/write
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	fromState := n.CurrentTerm

	if reqTerm > n.CurrentTerm {
		n.ToFollower(req.LeaderId, reqTerm, true)
		resp.Term = reqTerm
		// implementation gap:
		// maki: this may be a very tricky design in implementation,
		// but this simplifies the logic here
		// 3rd reply the response, we directly reject, and fix the log after
		// the leader degrade to follower
		resp.Success = false
		return JobResult{ShallDegrade: true, FromTerm: TermRank(fromState), ToTerm: TermRank(reqTerm), VotedFor: req.LeaderId}, nil
	} else if reqTerm < n.CurrentTerm {
		resp.Term = n.CurrentTerm
		resp.Success = false
		return JobResult{ShallDegrade: false}, nil
	} else {
		panic("shouldn't happen, break the property of Election Safety")
	}
}

// critical section: readwrite the meta-state, protected by the stateRWLock
func (n *nodeImpl) handleRequestVoteAsLeader(internalReq *utils.RequestVoteInternalReq) (JobResult, error) {

	req := internalReq.Req
	candidateLastLogIdx := req.LastLogIndex
	candidateLastLogTerm := req.LastLogTerm
	candidateId := req.CandidateId
	peerTerm := req.Term
	nodeID := req.NodeId

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

	fromTerm := TermRank(n.CurrentTerm)
	if n.CurrentTerm < req.Term {
		lastLogIdx, lastLogTerm := n.raftLog.GetLastLogIdxAndTerm()
		if (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLastLogIdx >= lastLogIdx) {
			err := n.ToFollower(candidateId, peerTerm, true)
			if err != nil {
				n.logger.Error("error in ToFollower", zap.Error(err))
				panic(err)
			}
			resp.Term = n.CurrentTerm
			resp.VoteGranted = true
			return JobResult{ShallDegrade: true, FromTerm: fromTerm, ToTerm: TermRank(req.Term), VotedFor: nodeID}, nil
		}
	}

	// the leader should reject the vote in all other cases
	resp.Term = n.CurrentTerm
	resp.VoteGranted = false
	return JobResult{ShallDegrade: false}, nil
}
