package mkraft

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

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

	--todo: not implemented yet

(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
todo: this paper doesn't mention how a stale leader catches up and becomes a follower
*/
func (n *Node) RunAsLeader(ctx context.Context) {

	if n.State != StateLeader {
		panic("node is not in LEADER state")
	}
	n.logger.Info("acquiring the Semaphore as the LEADER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired the Semaphore as the LEADER state")
	defer n.sem.Release(1)

	// Append the current NodeID to a file called "state"
	stateFilePath := "state.tmp"
	file, err := os.OpenFile(stateFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		n.logger.Error("failed to open state file", zap.Error(err))
		return
	}
	defer file.Close()

	currentTime := time.Now().Format(time.RFC3339)
	entry := currentTime + " " + n.NodeId + "\n"

	_, writeErr := file.WriteString(entry)
	if writeErr != nil {
		n.logger.Error("failed to append NodeID to state file", zap.Error(writeErr))
		return
	}

	// leader:
	// (1) major task-1: handle client commands and send append entries
	// (2) major task-2: send heartbeats to all the followers
	// (3) common task-1: handle voting requests from other candidates
	// (4) common task-2: handle append entries requests from other candidates

	n.clientCommandChan = make(chan *ClientCommandInternalReq, n.cfg.GetRaftNodeRequestBufferSize())
	n.leaderDegradationChan = make(chan TermRank, n.cfg.GetRaftNodeRequestBufferSize())

	// these channels cannot be closed because they are created in the main thread but used in the worker
	// we need to clean the channel when the leader quits,
	// and reset the channels when the leader comes back

	heartbeatDuration := n.cfg.GetLeaderHeartbeatPeriod()
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()

	ctxForWorker, cancelWorker := context.WithCancel(ctx)
	go n.leaderCommonTaskWorker(ctxForWorker)
	defer cancelWorker()

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
			case newTerm := <-n.leaderDegradationChan:
				n.State = StateFollower
				n.SetVoteForAndTerm(n.VotedFor, uint32(newTerm))
				// shall first change the state, so that new client commands won't flood in when we close the channel
				n.closeClientCommandChan()
				go n.RunAsFollower(ctx)
				return
			case <-tickerForHeartbeat.C:
				heartbeatReq := &rpc.AppendEntriesRequest{
					Term:     n.CurrentTerm,
					LeaderId: n.NodeId,
				}
				go n.callAppendEntries(ctx, heartbeatReq)
			case internalReq := <-n.clientCommandChan:
				tickerForHeartbeat.Stop()
				log := &rpc.LogEntry{
					Data: internalReq.Req.Command,
				}
				appendEntryReq := &rpc.AppendEntriesRequest{
					Term:     n.CurrentTerm,
					LeaderId: n.NodeId,
					Entries:  []*rpc.LogEntry{log},
				}
				go n.callAppendEntries(ctx, appendEntryReq)
				tickerForHeartbeat.Reset(heartbeatDuration)
			}
		}
	}
}

func (n *Node) leaderCommonTaskWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("leader's worker context done, exiting")
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("leader's worker context done, exiting")
				return
			case internalReq := <-n.requestVoteChan: // commonRule: same with candidate
				if !internalReq.IsTimeout.Load() {
					// no-IO operation
					req := internalReq.Req
					resp := n.receiveVoteRequest(req)
					wrappedResp := &RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					internalReq.RespChan <- wrappedResp
					if resp.VoteGranted {
						n.leaderDegradationChan <- TermRank(resp.Term)
						return
					}
				}
			case internalReq := <-n.appendEntryChan: // commonRule: same with candidate
				// todo: shall add appendEntry operations which shall be a goroutine
				resp := n.receiveAppendEntires(internalReq.Req)
				wrapper := RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
					Err:  nil,
				}
				internalReq.RespChan <- &wrapper
				if resp.Success {
					n.leaderDegradationChan <- TermRank(resp.Term)
					return
				}
			}
		}
	}
}

// todo: shall add batching
func (n *Node) handleClientCommand(ctx context.Context, internalReq *ClientCommandInternalReq) error {

	if n.State != StateLeader {
		return errors.New("node is not in LEADER state")
	}

	// (1) appends the command to the local as a new entry
	// todo: how to get the response
	go n.raftLog.AppendLog(internalReq.Req.Command, int(n.CurrentTerm))

	// (2) sends the command of appendEntries to all the followers in parallel to replicate the entry
	index, term := n.raftLog.GetPrevLogIndexAndTerm()
	entries := make([]*rpc.LogEntry, 1)
	entries[0] = &rpc.LogEntry{
		Data: internalReq.Req.Command,
	}

	req := &rpc.AppendEntriesRequest{
		Term:         n.CurrentTerm,
		LeaderId:     n.NodeId,
		PrevLogIndex: index,
		PrevLogTerm:  term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	// todo: how to get the response
	go n.callAppendEntries(ctx, req)

	// (3) when the entry has been safely replicated, the leader applies the entry to the state machine
	applyResp, err := n.statemachine.ApplyCommand(internalReq.Req.Command)
	// todo: how to get the response

	// (4) the leader responds to the client
	if err != nil {
		n.logger.Error("error in applying command to state machine", zap.Error(err))
		internalReq.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
			Resp: nil,
			Err:  err,
		}
	} else {
		clientResp := &rpc.ClientCommandResponse{
			Result: applyResp,
		}
		internalReq.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
			Resp: clientResp,
			Err:  nil,
		}
	}

	// (5) if the follower run slowly or crash, the leader will retry to send the appendEntries indefinitely
	// todo: how to do get the slower follower and resend the req?

	// (6)
	return nil
}

// synchronous, should be called in a goroutine
// todo: suppose we don't need response now
func (n *Node) callAppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	errChan := make(chan error, 1)
	respChan := make(chan *AppendEntriesConsensusResp, 1)
	go func(ctx context.Context) {
		ctxTimeout, _ := context.WithTimeout(ctx, n.cfg.GetRPCRequestTimeout())
		// since normally we don't wait for the stragglers,
		// if we call cancel, there will be errors in the clients to the stragglers
		consensusResp, err := n.consensus.AppendEntriesSendForConsensus(ctxTimeout, req)
		if err != nil {
			errChan <- err
		} else {
			respChan <- consensusResp
		}
	}(ctx)

	select {
	case <-ctx.Done():
		n.logger.Warn("raft node main context done, exiting", zap.String("requestID", requestID))
	case err := <-errChan:
		n.logger.Error(
			"error in sending append entries to one node", zap.Error(err), zap.String("requestID", requestID))
	case resp := <-respChan:
		if resp.Success {
			n.logger.Info("append entries success", zap.String("requestID", requestID))
			// todo: shall deliver the result
		} else {
			n.leaderDegradationChan <- TermRank(resp.Term)
			n.logger.Warn("append entries failed", zap.String("requestID", requestID))
		}
	}
}

func (n *Node) closeClientCommandChan() {
	close(n.clientCommandChan)
	for request := range n.clientCommandChan {
		if request != nil {
			request.RespChan <- &RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: nil,
				Err:  errors.New("raft node is not in leader state"),
			}
		}
	}
}

func (n *Node) ClientCommand(req *ClientCommandInternalReq) {
	n.clientCommandChan <- req
}
