package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
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
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired the Semaphore as the LEADER state")
	defer n.sem.Release(1)

	// maki: this is a tricky design (the whole design of the log/client command application is tricky)
	// todo: catch up the log application to make sure lastApplied == commitIndex for the leader
	n.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, n.cfg.GetRaftNodeRequestBufferSize())

	degradeChan := make(chan struct{})
	subWorkerCtx, subWorkerCancel := context.WithCancel(ctx)
	defer subWorkerCancel()

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(3)
	go n.leaderSendWorker(subWorkerCtx, degradeChan, &waitGroup)
	go n.leaderReceiverWorker(subWorkerCtx, degradeChan, &waitGroup)
	go n.leaderLogApplyWorker(subWorkerCtx, &waitGroup)

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
				subWorkerCancel()
				waitGroup.Wait()
				go n.RunAsFollower(ctx)
				return
			}
		}
	}
}

// sender is named from the perspective of sending request to other nodes
// receiving the client commands triggers sending so put in the same worker
func (n *nodeImpl) leaderSendWorker(ctx context.Context, degradeChan chan struct{}, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
	defer n.cleanupApplyLogsBeforeToFollower()

	tickerForHeartbeat := time.NewTicker(n.cfg.GetLeaderHeartbeatPeriod())
	defer tickerForHeartbeat.Stop()
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
			// task1: send the heartbeat -> as leader, may degrade to follower
			case <-tickerForHeartbeat.C:
				tickerForHeartbeat.Reset(n.cfg.GetLeaderHeartbeatPeriod())
				singleJobResult, err := n.syncDoHeartbeat(ctx)
				if err != nil {
					n.logger.Error("error in sending heartbeat, omit it for next retry", zap.Error(err))
				}
				if singleJobResult.ShallDegrade {
					close(degradeChan)
					return
				}
			// task2: handle the client command, need to change raftlog/state machine -> as leader, may degrade to follower
			case clientCmd := <-n.clientCommandCh:
				tickerForHeartbeat.Reset(n.cfg.GetElectionTimeout())
				batchingSize := n.cfg.GetRaftNodeRequestBufferSize() - 1
				clientCommands := utils.ReadMultipleFromChannel(n.clientCommandCh, batchingSize)
				clientCommands = append(clientCommands, clientCmd)
				singleJobResult, err := n.syncDoLogReplication(clientCmd.Ctx, clientCommands)
				if err != nil {
					// todo: as is same with most other panics, temporary solution, shall handle the error properly
					panic(err)
				}
				if singleJobResult.ShallDegrade {
					close(degradeChan)
					return
				}
			}
		}
	}
}

// receiver is named from the perspective of receiving request from other nodes
// not receiving from clients
func (n *nodeImpl) leaderReceiverWorker(ctx context.Context, degradeChan chan struct{}, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()
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
				singleJobResult, err := n.handleRequestVoteAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling request vote", zap.Error(err))
					panic(err)
				}
				if singleJobResult.ShallDegrade {
					close(degradeChan)
					return
				}
			case internalReq := <-n.appendEntryCh:
				singleJobResult, err := n.handlerAppendEntriesAsLeader(internalReq)
				if err != nil {
					n.logger.Error("error in handling append entries", zap.Error(err))
					panic(err)
				}
				if singleJobResult.ShallDegrade {
					close(degradeChan)
					return
				}
			}
		}
	}
}

// ---------------------------------------small unit helper functions for the leader-------------------------------------
type JobResult struct {
	ShallDegrade bool
	// if the shallDegrade is false, there is no need to fill in the term, and voteFor
	Term TermRank
	// if the current node doesn't vote for anyone when term changes, the voteFor is empty
	VotedFor string
}

// @return: shall degrade to follower or not,
// @return: if err is not nil, the caller shall retry
func (n *nodeImpl) syncDoHeartbeat(ctx context.Context) (JobResult, error) {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	currentTerm, _, _ := n.getKeyState()
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return JobResult{ShallDegrade: false}, err
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

	// the handling of success/failure is not aligned with the consensus method
	resp, err := n.consensus.ConsensusAppendEntries(ctxTimeout, reqs, n.CurrentTerm)
	if err != nil {
		n.logger.Error("error in sending append entries to one node", zap.Error(err))
		return JobResult{ShallDegrade: false}, err
	}

	// shouldn't have this case, fatal error -> it won't happen
	if resp.Term > currentTerm && resp.Success {
		panic("should not happen, break the property of Election Safety")
	}

	if resp.Success {
		n.logger.Info("append entries success", zap.String("requestID", requestID))
		return JobResult{ShallDegrade: false}, nil
	} else {
		// another failed reason is the prelog is not correct
		if resp.Term > currentTerm {
			n.logger.Warn("peer's term is greater than current term", zap.String("requestID", requestID))
			return JobResult{ShallDegrade: true, Term: TermRank(resp.Term), VotedFor: "Na"}, nil
		} else {
			// should retry: may be from the network error and the majority failed
			return JobResult{ShallDegrade: false}, errors.New("append entries failed, shall retry")
		}
	}
}

// happy path: 1) the leader is alive and the followers are alive (done)
// problem-1: the leader is alive but minority followers are dead -> can be handled by the retry mechanism
// problem-2: the leader is alive but majority followers are dead
// problem-3: the leader is stale
// @return: shall degrade to follower or not, and the error
func (n *nodeImpl) syncDoLogReplication(ctx context.Context, clientCommands []*utils.ClientCommandInternalReq) (JobResult, error) {

	var subTasksToWait sync.WaitGroup
	subTasksToWait.Add(2)
	currentTerm, _, _ := n.getKeyState()

	// prep:
	// get logs from the raft logs for each client
	// before the task-1 trying to change the logs and task-2 reading the logs in parallel and we don't know who is faster
	peerNodeIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		return JobResult{ShallDegrade: false}, err
	}
	cathupLogsForPeers, err := n.getLogsToCatchupForPeers(peerNodeIDs)
	if err != nil {
		return JobResult{ShallDegrade: false}, err
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
			return JobResult{ShallDegrade: true, Term: TermRank(resp.Term), VotedFor: "Na"}, nil
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
		return JobResult{ShallDegrade: false, Term: TermRank(currentTerm)}, nil
	}
}

func (n *nodeImpl) handlerAppendEntriesAsLeader(internalReq *utils.AppendEntriesInternalReq) (JobResult, error) {
	req := internalReq.Req
	reqTerm := uint32(req.Term)
	currentTerm, _, _ := n.getKeyState()

	if reqTerm > currentTerm {
		result := JobResult{ShallDegrade: true, Term: TermRank(reqTerm), VotedFor: "Na"}
		// implementation gap:
		// maki: this may be a very tricky design in implementation,
		// but this simplifies the logic here
		// 3rd reply the response, we directly reject this reject, and fix the log after
		// the leader degrade to follower
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: &rpc.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
			Err: nil,
		}
		return result, nil
	} else if reqTerm < currentTerm {
		internalReq.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Resp: &rpc.AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			},
			Err: nil,
		}
		return JobResult{ShallDegrade: false}, nil
	} else {
		panic("shouldn't happen, break the property of Election Safety")
	}
}

func (n *nodeImpl) handleRequestVoteAsLeader(internalReq *utils.RequestVoteInternalReq) (JobResult, error) {
	resp := n.handleVoteRequest(internalReq.Req)
	wrapper := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
		Resp: resp,
		Err:  nil,
	}
	internalReq.RespChan <- &wrapper
	if resp.VoteGranted {
		return JobResult{ShallDegrade: true, Term: TermRank(internalReq.Req.Term), VotedFor: "Na"}, nil
	} else {
		return JobResult{ShallDegrade: false}, nil
	}
}

func (n *nodeImpl) getLogsToCatchupForPeers(peerNodeIDs []string) (map[string]log.CatchupLogs, error) {
	// todo: can be batch reading
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
