package node

import (
	"context"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// synchronous call to until the a consensus is reached or the timeout failed
// should be timeout within the election timeout
func (c *consensus) ConsensusRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) *MajorityRequestVoteResp {

	if !common.HasDeadline(ctx) {
		panic("consensus context should have a deadline")
	}

	resChan := make(chan *MajorityRequestVoteResp, 1)
	oneRound := func(ctx context.Context, resChan chan<- *MajorityRequestVoteResp) {
		res := c.requestVoteOnce(ctx, req)
		if res.Err != nil {
			c.logger.Error("request vote one round failed",
				zap.Error(res.Err),
				zap.String("requestID", common.GetRequestID(ctx)))
		}
		resChan <- res
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		go oneRound(ctxWithCancel, resChan)
		select {
		case <-ctx.Done():
			return &MajorityRequestVoteResp{
				Err: common.ErrContextDone,
			}
		case res := <-resChan:
			if res.Err != nil {
				// Retry on error, but respect context cancellation
				continue
			}
			return res
		}
	}
}

// send one rpc to all the peers and wait for the rpc timeout to calculate the result
// @return: handle ErrMembershipErr, ErrMajorityNotMet
// all rpc are timeoutbounded out of the box, so the function takes as long as the rpc timeout
func (c *consensus) requestVoteOnce(ctx context.Context, req *rpc.RequestVoteRequest) *MajorityRequestVoteResp {

	// check membership
	requestID := common.GetRequestID(ctx)
	total := c.membership.GetTotalMemberCount()
	peerClients, err := c.membership.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return &MajorityRequestVoteResp{
			Err: common.ErrMembershipErr,
		}
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("Not enough peers for majority",
			zap.Int("total", total),
			zap.Int("peerCount", len(peerClients)),
			zap.String("requestID", requestID))
		return &MajorityRequestVoteResp{
			Err: common.ErrMembershipErr,
		}
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
		return &MajorityRequestVoteResp{
			Err: common.ErrContextDone,
		}
	case <-fanInChan:
		wg.Wait()
		close(fanInChan)
	}

	// FAN-IN WITH STOPPING SHORT
	// filter out the errors
	voteFailed := 0
	correctRes := make(chan utils.RPCRespWrapper[*rpc.RequestVoteResponse], len(peerClients))
	for wrappedRes := range fanInChan {
		if err := wrappedRes.Err; err != nil {
			voteFailed++
			c.logger.Error("error in response of request vote to one node",
				zap.String("requestID", requestID))
		} else {
			resp := wrappedRes.Resp
			if resp.Term < req.Term {
				panic("the peer should at least have the same term as the request")
			}
			correctRes <- wrappedRes
		}
	}
	if calculateIfAlreadyFail(total, peersCount, 0, voteFailed) {
		return &MajorityRequestVoteResp{
			Err: common.ErrMajorityNotMet,
		}
	}
	close(correctRes)

	// analyze the responses
	peerVoteAccumulated := 0 // the node itself is counted as a vote
	for wrappedRes := range correctRes {
		resp := wrappedRes.Resp
		if resp.Term > req.Term {
			return &MajorityRequestVoteResp{
				Term:                     resp.Term,
				VoteGranted:              false,
				PeerNodeIDWithHigherTerm: wrappedRes.PeerNodeID,
			}
		}
		// equal case
		if resp.VoteGranted {
			peerVoteAccumulated++
			if calculateIfMajorityMet(total, peerVoteAccumulated) {
				return &MajorityRequestVoteResp{
					Term:        req.Term,
					VoteGranted: true,
				}
			}
		} else {
			voteFailed++ // probably the current node has not the latest commit index
			c.logger.Warn("current node probablyhas not the latest commit index, so it fails to get the vote from a peer",
				zap.String("requestID", requestID))
			if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
				return &MajorityRequestVoteResp{
					Err: common.ErrMajorityNotMet,
				}
			}
		}
	}
	panic("consensus of request vote should not reach here")
}
