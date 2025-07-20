package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// we wait for all the response from the peers, and then return the result
// we don't cancel the context with majority

type consensus2 struct {
	node       Node
	logger     *zap.Logger
	membership peers.Membership
}

// send one rpc to all the peers and wait for the rpc timeout to calculate the result
// should handle ErrMajorityNotMet
func (c *consensus2) ConsensusRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error) {

	// check membership
	requestID := common.GetRequestID(ctx)
	total := c.membership.GetTotalMemberCount()
	peerClients, err := c.membership.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return nil, common.ErrMembershipErr
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("Not enough peers for majority",
			zap.Int("total", total),
			zap.Int("peerCount", len(peerClients)),
			zap.String("requestID", requestID))
		return nil, common.ErrMembershipErr
	}

	peersCount := len(peerClients)
	fanInChan := make(chan utils.RPCRespWrapper[*rpc.RequestVoteResponse], peersCount) // buffered with len(members) to prevent goroutine leak
	wg := sync.WaitGroup{}
	wg.Add(peersCount)

	for _, member := range peerClients {
		// FAN-OUT
		go func() {
			defer wg.Done()
			memberHandle := member
			// FAN-IN
			resp, err := memberHandle.RequestVote(ctx, req)
			res := utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
				Resp:       resp,
				Err:        err,
				PeerNodeID: memberHandle.GetNodeID(),
			}
			fanInChan <- res
		}()
	}

	// blocking point:wait for the response or the context is done
	select {
	case <-ctx.Done():
		return nil, common.ErrContextDone
	case <-fanInChan:
		wg.Wait()
		close(fanInChan)
	}

	// FAN-IN WITH STOPPING SHORT
	peerVoteAccumulated := 0 // the node itself is counted as a vote
	voteFailed := 0
	for wrappedRes := range fanInChan {
		if err := wrappedRes.Err; err != nil {
			voteFailed++
			c.logger.Error("error in sending request vote to one node",
				zap.Error(err),
				zap.Int("voteFailed", voteFailed),
				zap.String("requestID", requestID))
			if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
				return nil, common.ErrMajorityNotMet
			}
			continue
		}

		resp := wrappedRes.Resp
		if resp.Term > req.Term {
			return &MajorityRequestVoteResp{
				Term:                     resp.Term,
				VoteGranted:              false,
				PeerNodeIDWithHigherTerm: wrappedRes.PeerNodeID,
			}, nil
		}
		if resp.Term == req.Term {
			if resp.VoteGranted {
				peerVoteAccumulated++
				if calculateIfMajorityMet(total, peerVoteAccumulated) {
					return &MajorityRequestVoteResp{
						Term:        req.Term,
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
		if resp.Term < req.Term {
			c.logger.Warn("current node doesn't have the latest commit index, so it fails to get the vote from a peer",
				zap.String("requestID", requestID))
			return nil, common.ErrMajorityNotMet
		}
	}
	panic("consensus of request vote should not reach here")
}
