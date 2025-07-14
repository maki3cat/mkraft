package node

import (
	"context"
	"errors"
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
func (n *nodeImpl) syncDoLogReplication(ctx context.Context, clientCommands []*utils.ClientCommandInternalReq) (UnitResult, error) {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)
	currentTerm, _, _ := n.getKeyState()

	// prep:
	// get logs from the raft logs for each client
	// before the task-1 trying to change the logs and task-2 reading the logs in parallel and we don't know who is faster
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return UnitResult{ShallDegrade: false}, err
	}
	cathupLogsForPeers, err := n.helperForCatchupLogs(peerNodeIDs)
	if err != nil {
		return UnitResult{ShallDegrade: false}, err
	}

	// task1: appends the command to the local as a new entry
	errorChanTask1 := make(chan error, 1)
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		commands := make([][]byte, len(clientCommands))
		for i, clientCommand := range clientCommands {
			commands[i] = clientCommand.Req.Command
		}
		errorChanTask1 <- n.raftLog.AppendLogsInBatch(ctx, commands, currentTerm)
	}(ctx)

	// task2 sends the command of appendEntries to all the followers in parallel to replicate the entry
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	errorChanTask2 := make(chan error, 1)
	go func(ctx context.Context) {
		defer subTasksToWait.Done()
		newCommands := make([]*rpc.LogEntry, len(clientCommands))
		for i, clientCommand := range clientCommands {
			newCommands[i] = &rpc.LogEntry{
				Data: clientCommand.Req.Command,
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
	}(ctx)

	// todo: shall retry forever?
	// todo: what if task1 fails and task2 succeeds?
	// todo: what if task1 succeeds and task2 fails?
	// task3 when the entry has been safely replicated, the leader applies the entry to the state machine
	subTasksToWait.Wait()
	// maki: not sure how to handle the error?
	if err := <-errorChanTask1; err != nil {
		n.logger.Error("error in appending logs to raft log", zap.Error(err))
		panic("not sure how to handle the error")
	}
	if err := <-errorChanTask2; err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		panic("not sure how to handle the error")
	}
	resp := <-respChan
	if !resp.Success { // no consensus
		if resp.Term > currentTerm {
			return UnitResult{ShallDegrade: true, ToTerm: resp.Term, FromTerm: currentTerm, VotedFor: resp.PeerNodeIDWithHigherTerm}, nil
		} else {
			// todo: the unsafe panic is temporarily used for debugging
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

// todo:
// could we use the syncDoLogReplication to send heartbeat as empty command?
// critical section: granular lock to separate the function into 3 parts
// if the state changed during the IO,
// @return ErrStateChangedDuringIO, which should not retry
// @return: if err is not nil and not ErrStateChangedDuringIO, the caller shall retry
// todo: errors that should panic; maybe we should use panic freely and even a lot in the first version
// todo: so that the debugging process is easier
func (n *nodeImpl) syncSendHeartbeat(ctx context.Context) (UnitResult, error) {

	// granular lock -1: check the state before preparing the main logic
	n.stateRWLock.RLock()
	currentTerm, state, _ := n.getKeyState()
	if state != StateLeader {
		return UnitResult{ShallDegrade: false}, common.ErrNotLeader
	}
	n.stateRWLock.RUnlock()

	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		panic(err)
	}
	// catch up logs for peers
	cathupLogsForPeers, err := n.helperForCatchupLogs(peerNodeIDs)
	if err != nil {
		panic(err)
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
			Entries:      catchupCommands,
			LeaderCommit: n.getCommitIdx(),
		}
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
	defer cancel()

	// granular lock -2: check the state again before sending the request
	n.stateRWLock.RLock()
	currentTerm2, state, _ := n.getKeyState()
	if state != StateLeader || currentTerm2 != currentTerm {
		return UnitResult{ShallDegrade: false}, common.ErrStateChangedDuringIO
	}
	n.stateRWLock.RUnlock()

	resp, err := n.consensus.ConsensusAppendEntries(ctxTimeout, reqs, n.CurrentTerm)
	if err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		return UnitResult{ShallDegrade: false}, err
	}
	if resp.Success {
		n.logger.Debug("append entries success", zap.String("requestID", requestID))
		return UnitResult{ShallDegrade: false}, nil
	} else {
		// granular lock -3: check the state again after receiving the response
		// but at this point, the IO is already done, so we can just lock to the end of the function
		n.stateRWLock.RLock()
		defer n.stateRWLock.RUnlock()
		currentTerm3, state, _ := n.getKeyState()
		if state != StateLeader || currentTerm3 != currentTerm {
			return UnitResult{ShallDegrade: false}, common.ErrStateChangedDuringIO
		}
		if resp.Term > currentTerm {
			// key line: state change
			n.logger.Info("peer's term is greater than current term, degrade to follower", zap.String("requestID", requestID))
			err := n.ToFollower(resp.PeerNodeIDWithHigherTerm, resp.Term, true)
			if err != nil {
				n.logger.Error("error in ToFollower", zap.Error(err))
				panic(err)
			}
			return UnitResult{
				ShallDegrade: true,
				VotedFor:     resp.PeerNodeIDWithHigherTerm,
				FromTerm:     currentTerm,
				ToTerm:       resp.Term,
			}, nil
		} else {
			n.logger.Error("append entries failed probably because of the index mismatch", zap.String("requestID", requestID))
			return UnitResult{ShallDegrade: false}, errors.New("append entries failed, shall retry")
		}
	}
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

	fromTerm := n.CurrentTerm
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
			return UnitResult{ShallDegrade: true, FromTerm: fromTerm, ToTerm: req.Term, VotedFor: nodeID}, nil
		}
	}

	// the leader should reject the vote in all other cases
	resp.Term = n.CurrentTerm
	resp.VoteGranted = false
	return UnitResult{ShallDegrade: false}, nil
}

// ---------------------------------------the helper functions for the unit-------------------------------------
func (n *nodeImpl) helperForCatchupLogs(peerNodeIDs []string) (map[string]log.CatchupLogs, error) {
	result := make(map[string]log.CatchupLogs)
	for _, peerNodeID := range peerNodeIDs {
		nextID := n.getPeersNextIndex(peerNodeID)
		logs, err := n.raftLog.ReadLogsInBatchFromIdx(nextID)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			return nil, err
		}
		prevLogIndex := nextID - 1
		prevTerm, error := n.raftLog.GetTermByIndex(prevLogIndex)
		if error != nil {
			n.logger.Error("failed to get term by index", zap.Error(error))
			return nil, error
		}
		result[peerNodeID] = log.CatchupLogs{
			LastLogIndex: prevLogIndex,
			LastLogTerm:  prevTerm,
			Entries:      logs,
		}
	}
	return result, nil
}
