package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type UnitResult struct {
	ShallDegrade bool
	// if the shallDegrade is true, these must be filled in
	// VotedFor string
	// FromTerm uint32
	// ToTerm   uint32
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
			return UnitResult{ShallDegrade: true}, nil
		} else {
			panic("failed append entries, but without not a higher term")
		}
	} else {

		// (4) the leader applies the command, and responds to the client
		n.incrementCommitIdx(uint64(len(clientCommands)), false)

		// (5) send to the apply command channel
		for _, clientCommand := range clientCommands {
			n.leaderApplyCh <- clientCommand
		}
		return UnitResult{ShallDegrade: false}, nil
	}
}
