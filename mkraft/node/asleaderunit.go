package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type UnitResult struct {
	ShallDegrade bool
	// if the shallDegrade is true, these must be filled in
	VotedFor string
	FromTerm uint32
	ToTerm   uint32
}

// ---------------------------------------the sender units for the leader-------------------------------------
// happy path:
// 1) the leader is alive and the followers are alive (done)
// problem-1: the leader is alive but minority followers are dead -> can be handled by the retry mechanism
// problem-2: the leader is alive but majority followers are dead
// problem-3: the leader is stale
// @return: shall degrade to follower or not, and the error
// todo: warning: this function doesn't change the state inside it right now
// todo: errors that should panic; maybe we should use panic freely and even a lot in the first version
// todo: so that the debugging process is easier
func (n *nodeImpl) syncSendAppendEntries(ctx context.Context, clientCommands []*utils.ClientCommandInternalReq) (UnitResult, error) {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)

	// prep logs
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		panic(err)
	}
	cathupLogsForPeers, err := n.helperForCatchupLogs(peerNodeIDs)
	if err != nil {
		panic(err)
	}

	// if the state is not leader, we save us one rpc call to all peers
	currentTerm, state, _ := n.getKeyState()
	if state != StateLeader {
		n.stateRWLock.RUnlock()
		return UnitResult{ShallDegrade: false}, common.ErrStateChangedDuringIO
	}

	// task1: appends the command to the local as a new entry
	// if there is no command, we don't need to append the logs
	errorChanTask1 := make(chan error, 1)
	if len(clientCommands) > 0 {
		go func(ctx context.Context) {
			defer subTasksToWait.Done()
			commands := make([][]byte, len(clientCommands))
			for i, clientCommand := range clientCommands {
				commands[i] = clientCommand.Req.Command
			}
			errorChanTask1 <- n.raftLog.AppendLogsInBatch(ctx, commands, currentTerm)
		}(ctx)
	} else {
		subTasksToWait.Done()
		errorChanTask1 <- nil
	}

	// task2 sends the command of appendEntries to all the followers in parallel to replicate the entry
	// granular lock -2: check the state again before sending the request
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	errorChanTask2 := make(chan error, 1)
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout()*3) // 3 times the deadline
	defer timeoutCancel()
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		newCommands := make([]*rpc.LogEntry, len(clientCommands))
		if len(clientCommands) > 0 {
			for i, clientCommand := range clientCommands {
				newCommands[i] = &rpc.LogEntry{
					Data: clientCommand.Req.Command,
				}
			}
		}
		reqs := make(map[string]*rpc.AppendEntriesRequest, len(peerNodeIDs))
		for nodeID, catchup := range cathupLogsForPeers {
			catchupCommands := make([]*rpc.LogEntry, len(catchup.Entries))
			for i, log := range catchup.Entries {
				catchupCommands[i] = &rpc.LogEntry{
					Data: log.Commands,
				}
			}
			reqs[nodeID] = &rpc.AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     n.NodeId,
				PrevLogIndex: catchup.LastLogIndex,
				PrevLogTerm:  catchup.LastLogTerm,
				Entries:      append(catchupCommands, newCommands...),
			}
		}
		resp, err := n.consensus.ConsensusAppendEntries(ctx, reqs, currentTerm)
		respChan <- resp
		errorChanTask2 <- err
	}(timeoutCtx)

	subTasksToWait.Wait()
	if err := <-errorChanTask1; err != nil {
		n.logger.Error("error in appending logs to raft log", zap.Error(err))
		panic("not sure how to handle task-1 error")
	}
	if err := <-errorChanTask2; err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		panic("not sure how to handle task-2 error")
	}

	resp := <-respChan
	if !resp.Success {
		if resp.Term > currentTerm {
			n.stateRWLock.RLock()
			if n.CurrentTerm > resp.Term {
				// this response is stale, but the consensus is true
				n.logger.Warn("the response is stale, but the consensus is true", zap.String("requestID", requestID))
				panic("not sure how to handle the stale response")
			}
			n.ToFollower(resp.PeerNodeIDWithHigherTerm, resp.Term, true)
			return UnitResult{ShallDegrade: true, ToTerm: resp.Term, FromTerm: currentTerm, VotedFor: resp.PeerNodeIDWithHigherTerm}, nil
		} else {
			panic("failed append entries, but without not a higher term")
		}
	} else {

		// (4) the leader applies the command, and responds to the client
		n.incrementCommitIdx(uint64(len(clientCommands)))

		// (5) send to the apply command channel
		for _, clientCommand := range clientCommands {
			n.leaderApplyCh <- clientCommand
		}
		return UnitResult{ShallDegrade: false}, nil
	}
}

func (n *nodeImpl) syncSendHeartbeat(ctx context.Context) (UnitResult, error) {
	return n.syncSendAppendEntries(ctx, nil)
}

// ---------------------------------------the receiver units for the leader-------------------------------------

// critical section: readwrite the meta-state, protected by the stateRWLock
// fast, no IO
func (n *nodeImpl) recvAppendEntriesAsLeader(internalReq *utils.AppendEntriesInternalReq) (UnitResult, error) {
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
		return UnitResult{ShallDegrade: true, FromTerm: fromState, ToTerm: reqTerm, VotedFor: req.LeaderId}, nil
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
	candidateLastLogIdx := req.LastLogIndex
	candidateLastLogTerm := req.LastLogTerm
	peerTerm := req.Term

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

	fromTerm := n.CurrentTerm
	if n.CurrentTerm < req.Term {
		lastLogIdx, lastLogTerm := n.raftLog.GetLastLogIdxAndTerm()
		if (candidateLastLogTerm > lastLogTerm) || (candidateLastLogTerm == lastLogTerm && candidateLastLogIdx >= lastLogIdx) {
			err := n.ToFollower(req.CandidateId, peerTerm, true) // requestVote comes from the candidate, so we use the candidateId
			if err != nil {
				n.logger.Error("error in ToFollower", zap.Error(err))
				panic(err)
			}
			resp.Term = n.CurrentTerm
			resp.VoteGranted = true
			return UnitResult{ShallDegrade: true, FromTerm: fromTerm, ToTerm: req.Term, VotedFor: req.CandidateId}, nil
		}
	}

	// the leader should reject the vote in all other cases
	resp.Term = n.CurrentTerm
	resp.VoteGranted = false
	return UnitResult{ShallDegrade: false}, nil
}

// ---------------------------------------the helper functions for the unit-------------------------------------
func (n *nodeImpl) helperForCatchupLogs(peerNodeIDs []string) (map[string]log.CatchupLogs, error) {
	n.logger.Debug("helperForCatchupLogs enters", zap.Strings("peerNodeIDs", peerNodeIDs))
	result := make(map[string]log.CatchupLogs)
	for _, peerNodeID := range peerNodeIDs {
		nextID := n.getPeersNextIndex(peerNodeID)
		logs, err := n.raftLog.ReadLogsInBatchFromIdx(nextID)
		if err != nil {
			panic(err)
		}
		prevLogIndex := nextID - 1
		prevTerm, err := n.raftLog.GetTermByIndex(prevLogIndex)
		if err != nil {
			panic(err)
		}
		result[peerNodeID] = log.CatchupLogs{
			LastLogIndex: prevLogIndex,
			LastLogTerm:  prevTerm,
			Entries:      logs,
		}
	}
	return result, nil
}
