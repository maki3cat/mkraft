package node

import (
	"context"
	"errors"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type Consensus interface {

	// synchronous call to until the a consensus is reached or is failed
	// it contains forever retry mechanism, so
	// !!! the ctx shall be Done within the election timeout
	// !!! This is shortcut method, it returns when the consensus is reached or failed without waiting for all the responses
	ConsensusRequestVote(ctx context.Context, request *rpc.RequestVoteRequest) *MajorityRequestVoteResp

	// goroutine management:this method expands goroutines to the number of peers,
	// but since this system handles appendEnrines once a time in a serial way
	// I don't think we need to worry about the explosion of goroutines
	ConsensusAppendEntries(ctx context.Context, peerReq map[string]*rpc.AppendEntriesRequest, currentTerm uint32) (*AppendEntriesConsensusResp, error)

	SetNodeToUpdateOn(node Node)
}

type consensus struct {
	node       Node
	logger     *zap.Logger
	membership peers.Membership
}

func NewConsensus(logger *zap.Logger, membership peers.Membership) Consensus {
	return &consensus{
		logger:     logger,
		membership: membership,
	}
}

func (c *consensus) SetNodeToUpdateOn(node Node) {
	c.node = node
}

// if the majority pper-rpc fail, the err is nil and the resp.Success is false because can be multiple errors; the caller shall recall;
// if other parts fail, the err is not nil and the resp.Success is false; the caller shall retry;
func (c *consensus) ConsensusAppendEntries(ctx context.Context, peerReq map[string]*rpc.AppendEntriesRequest, currentTerm uint32) (*AppendEntriesConsensusResp, error) {
	requestID := common.GetRequestID(ctx)
	total := c.membership.GetTotalMemberCount()
	peerClients, err := c.membership.GetAllPeerClientsV2()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return nil, err
	}

	// check if registered peers are enough to get the consensus
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("not enough peer clients registered for consensus", zap.String("requestID", requestID))
		return nil, common.ErrNotEnoughPeersForConsensus
	}

	// in paralelel, send append entries to all peers
	fanInChan := make(chan utils.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))

	// check deadline and create rpc timeout
	deadline, deadlineSet := ctx.Deadline()
	if !deadlineSet {
		return nil, common.ErrDeadlineNotSet
	}
	rpcTimtout := time.Until(deadline)
	if rpcTimtout <= 0 {
		return nil, common.ErrDeadlineInThePast
	}

	for nodeID, member := range peerClients {
		// FAN-OUT
		go func(nodeID string, client peers.PeerClient) {

			rpcCtx, rpcCancel := context.WithTimeout(ctx, rpcTimtout)
			defer rpcCancel()

			req := peerReq[nodeID]
			resp, err := client.AppendEntriesWithRetry(rpcCtx, req)
			// FAN-IN
			if err != nil {
				c.logger.Error("error in sending append entries to one node",
					zap.Error(err),
					zap.String("requestID", requestID))
				fanInChan <- utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Err: err,
				}
				return
			} else {
				if resp.Success {
					// todo: the index part should be checked and reorganized
					// update the peers' index
					c.node.IncrPeerIdx(nodeID, uint64(len(req.Entries)))
				} else {
					c.node.DecrPeerIdx(nodeID)
				}
				fanInChan <- utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp:       resp,
					Err:        nil,
					PeerNodeID: nodeID,
				}
			}
		}(nodeID, member)
	}

	// STOPPING SHORT
	peerVoteAccumulated := 0
	failAccumulated := 0

	peersCount := len(peerClients)
	for range peersCount {
		select {
		case wrappedResp := <-fanInChan:
			if err := wrappedResp.Err; err != nil {
				c.logger.Warn("error returned from appendEntries",
					zap.Error(err),
					zap.String("requestID", requestID))
				failAccumulated++
				continue
			} else {
				resp := wrappedResp.Resp
				if resp.Term > currentTerm {
					c.logger.Info("peer's term is greater than current term",
						zap.String("requestID", requestID))
					return &AppendEntriesConsensusResp{
						Term:                     resp.Term,
						Success:                  false,
						PeerNodeIDWithHigherTerm: wrappedResp.PeerNodeID,
					}, nil
				}
				if resp.Term == currentTerm {
					if resp.Success {
						peerVoteAccumulated++
						if calculateIfMajorityMet(total, peerVoteAccumulated) {
							return &AppendEntriesConsensusResp{
								Term:    resp.Term,
								Success: true,
							}, nil
						}
					} else {
						c.logger.Error("append entries failed probably because of the prelog is not correct",
							zap.String("requestID", requestID))
						return &AppendEntriesConsensusResp{
							Term:    resp.Term,
							Success: false,
						}, nil
					}
				}
				if resp.Term < currentTerm {
					panic("this should not happen, the append entries should not get a smaller term than the current term")
				}
			}
		case <-ctx.Done():
			c.logger.Info("context canceled",
				zap.String("requestID", requestID))
			return nil, common.ErrContextDone
		}
	}
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}

// algorithm about consensus
func calculateIfMajorityMet(total, peerVoteAccumulated int) bool {
	return (peerVoteAccumulated + 1) >= total/2+1
}

func calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed int) bool {
	majority := total/2 + 1
	majorityNeeded := majority - 1
	needed := majorityNeeded - peerVoteAccumulated
	possibleRespondant := peersCount - voteFailed - peerVoteAccumulated
	return possibleRespondant < needed
}

// peers communication to get the consensus
type AppendEntriesConsensusResp struct {
	Term                     uint32
	Success                  bool
	PeerNodeIDWithHigherTerm string // critical if the term is won by a node with a higher term
	// Err                      error
}

type MajorityRequestVoteResp struct {
	Term                     uint32
	VoteGranted              bool
	PeerNodeIDWithHigherTerm string // critical if the term is won by a node with a higher term
	Err                      error
}
