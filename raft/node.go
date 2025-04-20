package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"golang.org/x/sync/semaphore"
)

var nodeInstance *Node
var nodeInitOnce = &sync.Once{}

func StartRaftNode(ctx context.Context) {
	nodeInitOnce.Do(func() {
		nodeInstance = NewNode(memberMgr.GetCurrentNodeID())
		nodeInstance.Start(ctx)
	})
}

func GetRaftNode() *Node {
	if nodeInstance == nil {
		panic("raft node is not initialized")
	}
	return nodeInstance
}

type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	// should never reach here
	return "Unknown State"
}

// the data structure used by the Raft-server
// todo: shall I add a timeout ctx for this internal request as well?
type RequestVoteInternal struct {
	Request    *rpc.RequestVoteRequest
	RespWraper chan *rpc.RPCRespWrapper[*rpc.RequestVoteResponse]
	IsTimeout  atomic.Bool
}

type AppendEntriesInternal struct {
	Request      *rpc.AppendEntriesRequest
	ResponseChan chan *rpc.AppendEntriesResponse
}

type ClientCommandInternal struct {
	request []byte
	// resChan chan []byte
}

type TermRank int

// the Raft Server Node
// maki: go gymnastics for sync values
// todo: add sync for these values?
type Node struct {
	sem *semaphore.Weighted

	LeaderId string
	NodeId   string // maki: nodeID uuid or number or something else?
	State    NodeState

	clientCommandChan chan *ClientCommandInternal
	requestVoteChan   chan *RequestVoteInternal
	appendEntryChan   chan *AppendEntriesInternal

	// Persistent state on all servers
	// todo: how/why to make it persistent? (embedded db?)
	CurrentTerm int32
	VotedFor    string // candidateID
	// LogEntries

	// Volatile state on all servers
	// todo: in the logging part
	// commitIndex int
	// lastApplied int

	// Volatile state on leaders only
	// todo: in the logging part
	// nextIndex  []int
	// matchIndex []int
}

// maki: go gymnastics for sync values
// todo: add sync for these values?
func (node *Node) SetVoteForAndTerm(voteFor string, term int32) {
	node.VotedFor = voteFor
	node.CurrentTerm = term
}

func (node *Node) ResetVoteFor() {
	node.VotedFor = ""
}

func (node *Node) VoteRequest(req *RequestVoteInternal) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req *AppendEntriesInternal) {
	node.appendEntryChan <- req
}

func NewNode(nodeId string) *Node {
	return &Node{
		State:             StateFollower, // servers start up as followers
		NodeId:            nodeId,
		sem:               semaphore.NewWeighted(1),
		CurrentTerm:       0,
		VotedFor:          "",
		clientCommandChan: make(chan *ClientCommandInternal),
		requestVoteChan:   make(chan *RequestVoteInternal),
		appendEntryChan:   make(chan *AppendEntriesInternal),
		LeaderId:          "",
	}
}

// servers start up as followers
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop() {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

// todo, maki: shall check if we need priority of a speicifc channel in our select
/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Followers:
(1) handle request of AppendEntriesRPC initiated by another server
(2) handle reuqest of RequestVoteRPC initiated by another server

Speicial Rules for Followers
Respond to RPCs from candidates and leaders
If election timeout elapses without receiving AppendEntries RPC from current leader
or granting vote to candidate: convert to candidate
*/
func (node *Node) RunAsFollower(ctx context.Context) {
	if node.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}

	sugarLogger.Info("node acquires to run in FOLLOWER state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired semaphore in FOLLOWER state")
	defer node.sem.Release(1)

	electionTicker := time.NewTicker(util.GetConfig().GetRPCRequestTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("context done")
			memberMgr.GracefulShutdown()
			return
		case <-electionTicker.C:
			node.State = StateCandidate
			go node.RunAsCandidate(ctx)
			return
		case requestVoteInternal := <-node.requestVoteChan:
			if requestVoteInternal.IsTimeout.Load() {
				sugarLogger.Warn("request vote is timeout")
				continue
			}
			electionTicker.Stop()

			// for the follower, the node state has no reason to change because of the request
			resp := node.voting(requestVoteInternal.Request)
			wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp: resp,
				Err:  nil,
			}
			requestVoteInternal.RespWraper <- wrapper
			electionTicker.Reset(util.GetConfig().GetElectionTimeout())

		case appendEntriesInternal := <-node.appendEntryChan:
			electionTicker.Stop()

			// for the follower, the node state has no reason to change because of the request
			req := appendEntriesInternal.Request
			resChan := appendEntriesInternal.ResponseChan
			resChan <- node.appendEntries(req)

			electionTicker.Reset(util.GetConfig().GetElectionTimeout())
		}
	}
}

/*
PAPER (quote):
Shared Rule: if any RPC request or response is received from a server with a higher term,
convert to follower
How the Shared Rule works for Candidates:
(1) handle the response of RequestVoteRPC initiated by itself
(2) handle request of AppendEntriesRPC initiated by another server
(3) handle reuqest of RequestVoteRPC initiated by another server

Specifical Rule for Candidates:
On conversion to a candidate, start election:
(1) increment currentTerm
(2) vote for self
(3) send RequestVote RPCs to all other servers
if votes received from majority of servers: become leader
if AppendEntries RPC received from new leader: convert to follower
if election timeout elapses: start new election
*/
func (node *Node) RunAsCandidate(ctx context.Context) {
	if node.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	sugarLogger.Info("node starts to acquiring CANDIDATE state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("node has acquired semaphore in CANDIDATE state")
	defer node.sem.Release(1)

	var consensusChan chan *MajorityRequestVoteResp
	var voteCancel context.CancelFunc
	defer voteCancel()
	var ctxTimeout context.Context

	// leverage closure
	tryElection := func() {
		// buffer == 1 so that the goroutine can return immediately
		// we only handle one out-going election at a time
		consensusChan = make(chan *MajorityRequestVoteResp, 1)
		node.CurrentTerm++
		node.VotedFor = node.NodeId
		req := &rpc.RequestVoteRequest{
			Term:        node.CurrentTerm,
			CandidateId: node.NodeId,
		}
		ctxTimeout, voteCancel = context.WithTimeout(
			ctx, time.Duration(util.GetConfig().GetElectionTimeout())*time.Millisecond)
		go RequestVoteSendForConsensus(ctxTimeout, req, consensusChan)
	}

	// the first trial of election
	tryElection()

	ticker := time.NewTicker(util.GetConfig().GetElectionTimeout())
	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("raft node main context done")
			memberMgr.GracefulShutdown()
			return
		case response := <-consensusChan: // some response from last election
			// I don't think we need to reset the ticker here
			voteCancel() // cancel the rest
			if response.VoteGranted {
				node.State = StateLeader
				go node.RunAsLeader(ctx)
				return
			} else {
				if response.Term > node.CurrentTerm {
					// some one has become a leader
					voteCancel()
					node.CurrentTerm = response.Term
					node.ResetVoteFor()
					node.State = StateFollower
					go node.RunAsFollower(ctx)
					return
				} else {
					sugarLogger.Infof(
						"not enough votes, re-elect again, current term: %d, candidateID: %d",
						node.CurrentTerm, node.NodeId,
					)
				}
			}

		case <-ticker.C: // last election timeout withno response
			voteCancel()
			tryElection()

		case requestVoteInternal := <-node.requestVoteChan: // commonRule: handling voteRequest from another candidate
			if requestVoteInternal.IsTimeout.Load() {
				sugarLogger.Warn("request vote is timeout")
				continue
			}
			req := requestVoteInternal.Request
			resChan := requestVoteInternal.RespWraper
			resp := node.voting(req)
			wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp: resp,
				Err:  nil,
			}
			resChan <- wrapper
			// this means other candiate has a higher term
			if resp.VoteGranted {
				node.State = StateFollower
				go node.RunAsFollower(ctx)
				return
			}
		case request := <-node.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
			req := request.Request
			resChan := request.ResponseChan

			resp := node.appendEntries(req)
			resChan <- resp

			if resp.Success {
				// this means there is a leader there
				node.State = StateFollower
				node.CurrentTerm = req.Term
				go node.RunAsFollower(ctx)
				return
			}
		}
	}
}

/*
PAPER (quote):
COMMON RULE
If any RPC request or response is received from a server with a higher term,
convert to follower
MAKI comprehension-How the Shared Rule works for Leaders with 3 scenarios:
(1) response of AppendEntries RPC sent by itself (OK)
(2) receive request of AppendEntries RPC from a server with a higher term (OK)
(3) receive request of RequestVote RPC from a server with a higher term (OK)

SPECIFICAL RULE FOR LEADERS:
(1) Upon election:

	send initial empty AppendEntries (heartbeat) RPCs to each reserver; repeat during idle periods to prevent election timeouts; (5.2) (OK)

(2) If command received from client:

	append entry to local log, respond after entry applied to state machine; (5.3) (OK)

	--todo: not implemented yet

(3) If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex for the follower;
If successful: update nextIndex and matchIndex for follower
If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
(4) If there exists and N such that N > committedIndex, a majority of matchIndex[i] ≥ N, ... (5.3/5.4)
todo: this paper doesn't mention how a stale leader catches up and becomes a follower
*/
func (node *Node) RunAsLeader(ctx context.Context) {

	if node.State != StateLeader {
		panic("node is not in LEADER state")
	}

	sugarLogger.Info("node acquires to run in LEADER state")
	node.sem.Acquire(ctx, 1)
	sugarLogger.Info("acquired semaphore in LEADER state")
	defer node.sem.Release(1)

	node.clientCommandChan = make(chan *ClientCommandInternal, util.GetConfig().ClientCommandBufferSize)
	defer close(node.clientCommandChan) // todo: not sure if this is the best way to cleanup

	// THE COMPLEXITY OF THE LEADER IS MUCH HIGHER THAN THE CANDIDATE
	// unlike the candidate which issues one request at a time
	// the leader is more complex
	// (1)
	// because it shall send multiple AppendEntries requests at the same time
	// and if any of them gets a response with a higher term
	// the leader shall become a follower
	// (2) (3)
	// at the same time, it shall send heartbeats to all the followers
	// at the same time, it shall handle the voting requests from other candidates

	// RESPONSE READER FOR THE APPENDENTRIES RESPONSES
	leaderRecedeToFollowerChan := make(chan TermRank) // the term

	respReaderCtx, respCancel := context.WithCancel(ctx)
	defer respCancel()
	appendEntriesRespChan := make(chan *MajorityAppendEntriesResp, util.GetConfig().LeaderBufferSize)
	defer close(appendEntriesRespChan) // todo: gorouting writing to it may panic the entire program

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				sugarLogger.Warn("raft node main context done")
				memberMgr.GracefulShutdown()
				return
			case response := <-appendEntriesRespChan:
				// BATCHING, BATCHING takes all in buffer now, we'll see if we need a batch-size config
				remainingSize := len(appendEntriesRespChan)
				responseBatch := make([]*MajorityAppendEntriesResp, remainingSize+1)
				responseBatch[0] = response
				for i := range remainingSize {
					responseBatch[i] = <-appendEntriesRespChan
				}

				highestTerm := node.CurrentTerm
				for _, resp := range responseBatch {
					if resp.Term > node.CurrentTerm {
						highestTerm = max(resp.Term, highestTerm)
					}
				}
				if highestTerm > node.CurrentTerm {
					sugarLogger.Warn("term is greater than current term")
					leaderRecedeToFollowerChan <- TermRank(highestTerm)
					return
				} else {
					// calculate the committed index
					// todo: not impletmented yet

				}
			}

		}
	}(respReaderCtx)

	heartbeatDuration := time.Duration(util.GetConfig().LeaderHeartbeatPeriodInMs) * time.Millisecond
	tickerForHeartbeat := time.NewTicker(heartbeatDuration)
	defer tickerForHeartbeat.Stop()

	// this timeout is one consensus timeout, the internal should be one rpc request timeout
	callAppendEntries := func(req *rpc.AppendEntriesRequest) {
		ctxTimeout, cancel := context.WithTimeout(ctx, util.GetConfig().GetRPCRequestTimeout())
		defer cancel()
		AppendEntriesSendForConsensus(ctxTimeout, req, appendEntriesRespChan)
	}

	for {
		select {
		case <-ctx.Done():
			sugarLogger.Warn("raft node main context done")
			memberMgr.GracefulShutdown()
			return
		case newTerm := <-leaderRecedeToFollowerChan:
			// shall clean up other channels, specially the channel of client request
			// paper: if a leader receives a heartbeat from a node with a higher term,
			// it becomes a follower

			// todo: we can make one place for all term and vote changes and add lock if needed
			node.State = StateFollower
			node.SetVoteForAndTerm(node.VotedFor, int32(newTerm))
			go node.RunAsFollower(ctx)
			return

		case <-tickerForHeartbeat.C:
			heartbeatReq := &rpc.AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderId: node.NodeId,
			}
			// this timeout is one consensus timeout, the internal should be one rpc request timeout
			go callAppendEntries(heartbeatReq)

		case clientCmdReq := <-node.clientCommandChan:
			// todo: should add rate limit the client command
			// todo: add batching to appendEntries when we do logging
			// todo: we don't handle single client command
			tickerForHeartbeat.Stop()
			log := &rpc.LogEntry{
				Data: clientCmdReq.request,
			}
			appendEntryReq := &rpc.AppendEntriesRequest{
				Term:     node.CurrentTerm,
				LeaderId: node.NodeId,
				Entries:  []*rpc.LogEntry{log},
			}
			go callAppendEntries(appendEntryReq)
			tickerForHeartbeat.Reset(heartbeatDuration)

		case requestVote := <-node.requestVoteChan: // commonRule: same with candidate
			if !requestVote.IsTimeout.Load() {
				req := requestVote.Request
				resChan := requestVote.RespWraper
				resp := node.voting(req)
				wrapper := &rpc.RPCRespWrapper[*rpc.RequestVoteResponse]{
					Resp: resp,
					Err:  nil,
				}
				resChan <- wrapper
				// this means other candiate has a higher term
				if resp.VoteGranted {
					node.State = StateFollower
					go node.RunAsFollower(ctx)
					return
				}
			}
		case request := <-node.appendEntryChan: // commonRule: same with candidate
			req := request.Request
			resChan := request.ResponseChan

			resp := node.appendEntries(req)
			resChan <- resp

			if resp.Success {
				// this means there is a leader there
				node.State = StateFollower
				node.CurrentTerm = req.Term
				go node.RunAsFollower(ctx)
				return
			}
		}
	}
}
