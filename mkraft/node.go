package mkraft

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/pluggable"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

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

type TermRank int

var _ NodeIface = (*Node)(nil)

type NodeIface interface {
	VoteRequest(req *RequestVoteInternalReq)
	AppendEntryRequest(req *AppendEntriesInternalReq)
	ClientCommand(req *ClientCommandInternalReq)

	Start(ctx context.Context)
	Stop(ctx context.Context)
}

func NewNode(
	nodeId string,
	cfg common.ConfigIface,
	logger *zap.Logger,
	membership MembershipMgrIface,
	raftlog RaftLogsIface,
	statemachine pluggable.StateMachineIface,
) NodeIface {
	bufferSize := cfg.GetRaftNodeRequestBufferSize()
	consensus := NewConsensus(logger, membership, cfg)
	return &Node{
		statemachine: statemachine,
		raftLog:      raftlog,
		cfg:          cfg,
		membership:   membership,
		consensus:    consensus,
		logger:       logger,

		State:             StateFollower, // servers start up as followers
		NodeId:            nodeId,
		sem:               semaphore.NewWeighted(1),
		CurrentTerm:       0,
		VotedFor:          "",
		clientCommandChan: make(chan *ClientCommandInternalReq, bufferSize),
		requestVoteChan:   make(chan *RequestVoteInternalReq, bufferSize),
		appendEntryChan:   make(chan *AppendEntriesInternalReq, bufferSize),
		LeaderId:          "",
	}
}

// the Raft Server Node
// maki: go gymnastics for sync values
// todo: add sync for these values?
type Node struct {
	raftLog      RaftLogsIface
	consensus    ConsensusIface
	membership   MembershipMgrIface
	cfg          common.ConfigIface
	logger       *zap.Logger
	statemachine pluggable.StateMachineIface

	sem *semaphore.Weighted

	LeaderId string
	NodeId   string // maki: nodeID uuid or number or something else?
	State    NodeState

	// leader only channels
	// gracefully clean every time a leader degrades to a follower
	// reset these 2 data structures everytime a new leader is elected
	clientCommandChan     chan *ClientCommandInternalReq
	leaderDegradationChan chan TermRank

	// shared by all states
	requestVoteChan chan *RequestVoteInternalReq
	appendEntryChan chan *AppendEntriesInternalReq

	// Persistent state on all servers
	// todo: how/why to make it persistent? (embedded db?)
	// todo: change to concurrency safe
	CurrentTerm uint32
	VotedFor    string // candidateID
	// LogEntries

	// Paper page 4:
	//	Volatile state on all servers (both initialized to 0, increase monotonically)
	//  index of the highest log entry known to be committed
	commitIndex uint32
	// index of the highest log entry applied to state machine
	lastApplied uint32

	// Volatile state on leaders only, reinitialized after election, initialized to leader last log index+1
	nextIndex  map[string]uint32 // map[peerID]nextIndex, index of the next log entry to send to that server
	matchIndex map[string]uint32 // map[peerID]matchIndex, index of highest log entry known to be replicated on that server

}

// maki: go gymnastics for sync values
// todo: add sync for these values?
func (node *Node) SetVoteForAndTerm(voteFor string, term uint32) {
	node.VotedFor = voteFor
	node.CurrentTerm = term
}

func (node *Node) ResetVoteFor() {
	node.VotedFor = ""
}

func (node *Node) VoteRequest(req *RequestVoteInternalReq) {
	node.requestVoteChan <- req
}

func (node *Node) AppendEntryRequest(req *AppendEntriesInternalReq) {
	node.appendEntryChan <- req
}

// servers start up as followers
func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) Stop(ctx context.Context) {
	close(node.appendEntryChan)
	close(node.clientCommandChan)
}

func (node *Node) runOneElection(ctx context.Context) chan *MajorityRequestVoteResp {
	ctx, requestID := common.GetOrGenerateRequestID(ctx)
	consensusChan := make(chan *MajorityRequestVoteResp, 1)
	node.CurrentTerm++
	node.VotedFor = node.NodeId
	req := &rpc.RequestVoteRequest{
		Term:        node.CurrentTerm,
		CandidateId: node.NodeId,
	}
	ctxTimeout, _ := context.WithTimeout(
		ctx, time.Duration(node.cfg.GetElectionTimeout()))
	go func() {
		resp, err := node.consensus.RequestVoteSendForConsensus(ctxTimeout, req)
		if err != nil {
			node.logger.Error(
				"error in RequestVoteSendForConsensus", zap.String("requestID", requestID), zap.Error(err))
			return
		} else {
			consensusChan <- resp
		}
	}()
	return consensusChan
}

// TODO: THE WHOLE MODULE SHALL BE REFACTORED TO BE AN INTEGRAL OF THE CONSENSUS ALGORITHM
// The decision of consensus upon receiving a request
// can be independent of the current state of the node

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: checks the receiveVoteRequest works correctly for any state of the node, candidate or leader or follower
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) receiveVoteRequest(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	var response rpc.RequestVoteResponse
	if req.Term > node.CurrentTerm {
		node.VotedFor = req.CandidateId
		node.CurrentTerm = req.Term
		response = rpc.RequestVoteResponse{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else if req.Term < node.CurrentTerm {
		response = rpc.RequestVoteResponse{
			Term:        node.CurrentTerm,
			VoteGranted: false,
		}
	} else {
		if node.VotedFor == "" || node.VotedFor == req.CandidateId {
			node.VotedFor = req.CandidateId
			response = rpc.RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: true,
			}
		} else {
			response = rpc.RequestVoteResponse{
				Term:        node.CurrentTerm,
				VoteGranted: false,
			}
		}
	}
	return &response
}

// maki: jthis method should be a part of the consensus algorithm
// todo: right now this method doesn't check the current state of the node
// todo: not sure what state shall be changed inside or outside in the caller
func (node *Node) receiveAppendEntires(req *rpc.AppendEntriesRequest) *rpc.AppendEntriesResponse {
	var response rpc.AppendEntriesResponse
	reqTerm := uint32(req.Term)
	if reqTerm > node.CurrentTerm {
		// todo: tell the leader/candidate to change the state to follower
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	} else if reqTerm < node.CurrentTerm {
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: false,
		}
	} else {
		// should accecpet it directly?
		response = rpc.AppendEntriesResponse{
			Term:    node.CurrentTerm,
			Success: true,
		}
	}
	return &response
}
