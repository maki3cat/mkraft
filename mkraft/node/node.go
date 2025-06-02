package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/maki3cat/mkraft/mkraft/utils"
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
	// todo: can refer to hashicorp raft for ideas
	// todo: lost requestID and other values in the context
	VoteRequest(req *utils.RequestVoteInternalReq)
	// todo: lost requestID and other values in the context
	AppendEntryRequest(req *utils.AppendEntriesInternalReq)
	// todo: lost requestID and other values in the context
	ClientCommand(req *utils.ClientCommandInternalReq)

	Start(ctx context.Context)
	GracefulStop()
}

// not only new a class but also catch up statemachine, so it may cost time
func NewNode(
	nodeId string,
	cfg common.ConfigIface,
	logger *zap.Logger,
	membership peers.MembershipMgrIface,
) NodeIface {
	bufferSize := cfg.GetRaftNodeRequestBufferSize()

	// todo: can be a problem of these two intializations
	// zero can be problematic, but these can also be a problem
	// we can fix with happy path first like start from a new cluster, and never fails
	// lastAppliedIdx := statemachine.GetLatestAppliedIndex()
	// lastCommitIdx, _ := raftlog.GetLastLogIdxAndTerm()

	node := &Node{
		membership: membership,
		cfg:        cfg,
		logger:     logger,

		stateRWLock: &sync.RWMutex{},
		sem:         semaphore.NewWeighted(1),

		NodeId: nodeId,
		state:  StateFollower,

		// leader only channels
		clientCommandCh:       make(chan *utils.ClientCommandInternalReq, bufferSize),
		leaderApplyCh:         make(chan *utils.ClientCommandInternalReq, bufferSize),
		noleaderApplySignalCh: make(chan bool, bufferSize),

		requestVoteCh: make(chan *utils.RequestVoteInternalReq, bufferSize),
		appendEntryCh: make(chan *utils.AppendEntriesInternalReq, bufferSize),

		// persistent state on all servers
		CurrentTerm: 0, // as the logical clock in Raft to allow detection of stale messages
		VotedFor:    "",

		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64, 6),
		matchIndex:  make(map[string]uint64, 6),
	}

	// initialize the raft log and statemachine
	raftLogIface := plugs.NewRaftLogsImplAndLoad(cfg.GetRaftLogFilePath(), logger)
	statemachine := plugs.NewStateMachineNoOpImpl()
	node.raftLog = raftLogIface
	node.statemachine = statemachine

	// load persistent state
	err := node.loadCurrentTermAndVotedFor()
	if err != nil {
		node.logger.Error("error loading current term and voted for", zap.Error(err))
		panic(err)
	}
	// load logs
	err = node.unsafeLoadIdx()
	if err != nil {
		node.logger.Error("error loading index", zap.Error(err))
		panic(err)
	}
	return node
}

// the Raft Server Node
type Node struct {
	membership peers.MembershipMgrIface // managed by the outside overarching server

	raftLog      plugs.RaftLogsIface // required, persistent
	cfg          common.ConfigIface
	logger       *zap.Logger
	statemachine plugs.StateMachineIface

	// for the node state
	sem *semaphore.Weighted
	// a RW mutex for all the internal states in this node
	stateRWLock *sync.RWMutex

	NodeId string // maki: nodeID uuid or number or something else?
	state  NodeState

	// leader only channels
	// gracefully clean every time a leader degrades to a follower
	// reset these 2 data structures everytime a new leader is elected
	clientCommandCh chan *utils.ClientCommandInternalReq

	leaderApplyCh         chan *utils.ClientCommandInternalReq
	noleaderApplySignalCh chan bool

	// shared by all states
	requestVoteCh chan *utils.RequestVoteInternalReq
	appendEntryCh chan *utils.AppendEntriesInternalReq

	// Persistent state on all servers
	CurrentTerm uint32 // required, persistent
	VotedFor    string // required, persistent
	// LogEntries

	// Paper page 4:
	commitIndex uint64 // required, volatile on all servers
	lastApplied uint64 // required, volatile on all servers

	// required, volatile, on leaders only, reinitialized after election, initialized to leader last log index+1
	nextIndex  map[string]uint64 // map[peerID]nextIndex, index of the next log entry to send to that server
	matchIndex map[string]uint64 // map[peerID]matchIndex, index of highest log entry known to be replicated on that server
}

func (node *Node) GetNodeState() NodeState {
	node.stateRWLock.RLock()
	defer node.stateRWLock.RUnlock()
	return node.state
}

func (node *Node) SetNodeState(state NodeState) {
	node.stateRWLock.Lock()
	defer node.stateRWLock.Unlock()
	if node.state == state {
		return // no change
	}
	node.logger.Info("Node state changed",
		zap.String("nodeID", node.NodeId),
		zap.String("oldState", node.state.String()),
		zap.String("newState", state.String()))

	node.state = state
}

func (node *Node) Start(ctx context.Context) {
	go node.RunAsFollower(ctx)
}

// gracefully stop the node and cleanup
func (node *Node) GracefulStop() {
	// feature: need to check the graceful stop the node itself,
	// membership graceful stop is handled by the outside overarching server
	node.logger.Info("graceful stop of node")
	// (1) internal dependencies: raftLog, statemachine,
	// (2) shall the memebrship be internalized in the node ? decide after checking the dynamic membership protocol
	// (3) others defer functions are suitable and enough for the graceful stop as different states?
}

// todo: reconstruction of the requets-receiving apis,
// 1) wrap context in; 2) add the return default to reject using the leakage bucket
func (node *Node) VoteRequest(req *utils.RequestVoteInternalReq) {
	select {
	case node.requestVoteCh <- req:
	default:
		node.logger.Warn("request vote channel is full, dropping request", zap.String("nodeID", node.NodeId))
		req.RespChan <- &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
			Err: common.ErrServerBusy,
		}
	}
}

func (node *Node) AppendEntryRequest(req *utils.AppendEntriesInternalReq) {
	select {
	case node.appendEntryCh <- req:
	default:
		node.logger.Warn("append entry channel is full, dropping request", zap.String("nodeID", node.NodeId))
		req.RespChan <- &utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
			Err: common.ErrServerBusy,
		}
	}
}

// todo: shall add the feature of "redirection to the leader"
func (n *Node) ClientCommand(req *utils.ClientCommandInternalReq) {
	if n.GetNodeState() != StateLeader {
		n.logger.Warn("Client command received but node is not a leader, dropping request",
			zap.String("nodeID", n.NodeId))
		req.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
			Err: common.ErrNotLeader,
		}
		return
	}
	defer func() {
		if r := recover(); r != nil {
			n.logger.Error("panic in ClientCommand, shall have bugs", zap.Any("panic", r))
			req.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Err: common.ErrNotLeader,
			}
		}
	}()
	select {
	case n.clientCommandCh <- req:
	default:
		n.logger.Warn("client command channel is full, dropping request", zap.String("nodeID", n.NodeId))
		req.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
			Err: common.ErrServerBusy,
		}
	}
}

// paper: $5.4.1, property & mechanism
// This method is shared by leader/follower/candidate
// Related Property: Leader Completeness, in any leader-based consensus protocol, the leader should eventually store all COMMITTED log entries.
// Restriction: Raft implements this by the election mechanism, i.e., the leader selected shall have all the committed log entries of previous leaders;
// Impementation: a node cannot vote for a candidate that has 1) lower term of last log entry, or 2) same term of last log entry but lower index of last log entry.
// return: (voteGranted, shouldUpdateCurrentTermAndVoteFor)
func (node *Node) grantVote(candidateLastLogIdx uint64, candidateLastLogTerm, newTerm uint32, candidateId string) bool {
	node.stateRWLock.RLock()
	defer node.stateRWLock.RUnlock()

	currentTerm, voteFor := node.CurrentTerm, node.VotedFor
	if currentTerm < newTerm {
		lastLogIdx, lastLogTerm := node.raftLog.GetLastLogIdxAndTerm()
		if candidateLastLogTerm >= lastLogTerm && candidateLastLogIdx >= lastLogIdx {
			err := node.storeCurrentTermAndVotedFor(newTerm, candidateId, true)
			if err != nil {
				node.logger.Error("error in storeCurrentTermAndVotedFor", zap.Error(err))
				panic(err)
			}
			return true
		}
	}
	if currentTerm == newTerm && (voteFor == "" || voteFor == candidateId) {
		return true
	}
	return false
}

func (node *Node) handleVoteRequest(req *rpc.RequestVoteRequest) *rpc.RequestVoteResponse {
	voteGranted := node.grantVote(req.LastLogIndex, req.LastLogTerm, req.Term, req.CandidateId)
	// implementation gap: I think there is no need to differentiate the updated currentTerm or the previous currentTerm
	currentTerm := node.getCurrentTerm()
	return &rpc.RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}
}
