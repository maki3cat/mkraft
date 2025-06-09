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
	Term    uint32
	Success bool
}

type MajorityRequestVoteResp struct {
	Term        uint32
	VoteGranted bool
	Err         error
}

// synchronous call to until the a consensus is reached or is failed
// it contains forever retry mechanism, so
// !!! the ctx shall be Done within the election timeout
func (c *Node) ConsensusRequestVote(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error) {

	requestID := common.GetRequestID(ctx)
	total := c.membership.GetMemberCount()
	peerClients, err := c.membership.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return nil, err
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("Not enough peers for majority",
			zap.Int("total", total),
			zap.Int("peerCount", len(peerClients)),
			zap.String("requestID", requestID))
		return nil, errors.New("no member clients found")
	}

	peersCount := len(peerClients)
	resChan := make(chan utils.RPCRespWrapper[*rpc.RequestVoteResponse], peersCount) // buffered with len(members) to prevent goroutine leak

	// check deadline and create rpc timeout
	deadline, deadlineSet := ctx.Deadline()
	if !deadlineSet {
		return nil, errors.New("deadline is not set")
	}
	rpcTimtout := time.Until(deadline)
	if rpcTimtout <= 0 {
		return nil, errors.New("deadline is in the past")
	}

	for _, member := range peerClients {
		// FAN-OUT
		go func() {
			memberHandle := member
			rpcCtx, rpcCancel := context.WithTimeout(ctx, rpcTimtout)
			defer rpcCancel()
			// FAN-IN
			resp, err := memberHandle.RequestVoteWithRetry(rpcCtx, request)
			res := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp: resp,
				Err:  err,
			}
			resChan <- res
		}()
	}

	// FAN-IN WITH STOPPING SHORT
	peerVoteAccumulated := 0 // the node itself is counted as a vote
	voteFailed := 0
	for range peersCount {
		select {
		case res := <-resChan:
			if err := res.Err; err != nil {
				voteFailed++
				c.logger.Error("error in sending request vote to one node",
					zap.Error(err),
					zap.Int("voteFailed", voteFailed),
					zap.String("requestID", requestID))
				if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
					return nil, common.ErrMajorityNotMet
				} else {
					continue
				}
			} else {
				resp := res.Resp
				// if someone responds with a term greater than the current term
				if resp.Term > request.Term {
					return &MajorityRequestVoteResp{
						Term:        resp.Term,
						VoteGranted: false,
					}, nil
				}
				if resp.Term == request.Term {
					if resp.VoteGranted {
						// won the election
						peerVoteAccumulated++
						if calculateIfMajorityMet(total, peerVoteAccumulated) {
							return &MajorityRequestVoteResp{
								Term:        request.Term,
								VoteGranted: true,
							}, nil
						}
					} else {
						voteFailed++
						if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
							return nil, common.ErrMajorityNotMet
						}
					}
				}
				if resp.Term < request.Term {
					return nil, common.ErrInvariantsBroken
				}
			}
		case <-ctx.Done():
			return nil, common.ErrContextDone
		}
	}
	return nil, common.ErrInvariantsBroken
}

// goroutine management:this method expands goroutines to the number of peers,
// but since this system handles appendEnrines once a time in a serial way
// I don't think we need to worry about the explosion of goroutines
func (n *Node) ConsensusAppendEntries(
	ctx context.Context, peerReq map[string]*rpc.AppendEntriesRequest, currentTerm uint32) (*AppendEntriesConsensusResp, error) {

	requestID := common.GetRequestID(ctx)
	total := n.membership.GetMemberCount()
	peerClients, err := n.membership.GetAllPeerClientsV2()
	if err != nil {
		n.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return nil, err
	}

	// check if registered peers are enough to get the consensus
	if !calculateIfMajorityMet(total, len(peerClients)) {
		n.logger.Error("not enough peer clients registered for consensus", zap.String("requestID", requestID))
		return nil, common.ErrNotEnoughPeersForConsensus
	}

	// in paralelel, send append entries to all peers
	allRespChan := make(chan utils.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))

	// check deadline and create rpc timeout
	deadline, deadlineSet := ctx.Deadline()
	if !deadlineSet {
		return nil, errors.New("deadline is not set")
	}
	rpcTimtout := time.Until(deadline)
	if rpcTimtout <= 0 {
		return nil, errors.New("deadline is in the past")
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
				n.logger.Error("error in sending append entries to one node",
					zap.Error(err),
					zap.String("requestID", requestID))
				allRespChan <- utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Err: err,
				}
				return
			} else {
				if resp.Success {
					// update the peers' index
					n.incrPeerIdxAfterLogRepli(nodeID, uint64(len(req.Entries)))
				} else {
					n.decrPeerIdxAfterLogRepli(nodeID)
				}
				allRespChan <- utils.RPCRespWrapper[*rpc.AppendEntriesResponse]{
					Resp: resp,
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
		case res := <-allRespChan:
			if err := res.Err; err != nil {
				n.logger.Warn("error returned from appendEntries",
					zap.Error(err),
					zap.String("requestID", requestID))
				failAccumulated++
				continue
			} else {
				resp := res.Resp
				if resp.Term > currentTerm {
					n.logger.Info("peer's term is greater than current term",
						zap.String("requestID", requestID))
					return &AppendEntriesConsensusResp{
						Term:    resp.Term,
						Success: false,
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
						failAccumulated++
						if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, failAccumulated) {
							n.logger.Warn("another node with same term becomes the leader",
								zap.String("requestID", requestID))
							return &AppendEntriesConsensusResp{
								Term:    resp.Term,
								Success: false,
							}, nil
						}
					}
				}
				if resp.Term < currentTerm {
					n.logger.Error(
						"invairant failed, smaller term is not overwritten by larger term",
						zap.String("response", resp.String()),
						zap.String("requestID", requestID))
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			n.logger.Info("context canceled",
				zap.String("requestID", requestID))
			return nil, errors.New("context canceled")
		}
	}
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}
