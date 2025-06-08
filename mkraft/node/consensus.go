package node

import (
	"context"
	"errors"

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
}

// synchronous call to wait until the consensus is reached or is failed
// each internal rpc call is retried if a RPC call times out with no response
// todo: make sure there is someone calling the ctx.Done() to stop the infinite retry
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
	for _, member := range peerClients {
		// FAN-OUT
		// maki: todo topic for go gynastics
		go func() {
			memberHandle := member
			timeout := c.cfg.GetElectionTimeout()
			ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			// FAN-IN
			resChan <- <-memberHandle.SendRequestVoteWithRetries(ctxWithTimeout, request)
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
	for nodeID, member := range peerClients {
		// FAN-OUT
		go func(nodeID string, client peers.InternalClientIface) {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, n.cfg.GetElectionTimeout())
			defer cancel()
			req := peerReq[nodeID]
			resp := client.SendAppendEntries(ctxWithTimeout, req)
			if resp.Err == nil {
				if resp.Resp.Success {
					// update the peers' index
					n.incrPeerIdxAfterLogRepli(nodeID, uint64(len(req.Entries)))
				} else {
					n.decrPeerIdxAfterLogRepli(nodeID)
				}
			}
			// FAN-IN
			allRespChan <- resp
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
