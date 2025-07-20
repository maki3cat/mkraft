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

// THIS FILE IS ABOUT THE CONSENSUS OF THE REQUEST VOTE

// ConsensusRequestVote attempts to reach consensus for a vote request within the given context deadline.
// It retries on transient errors until either a majority is reached or the context is done.
func (c *consensus) ConsensusRequestVote(ctx context.Context, req *rpc.RequestVoteRequest) *MajorityRequestVoteResp {
	if !common.HasDeadline(ctx) {
		panic("consensus context should have a deadline")
	}

	for {
		// Check if context is already done before starting a round
		select {
		case <-ctx.Done():
			return &MajorityRequestVoteResp{
				Err: common.ErrContextDone,
			}
		default:
		}

		res := c.requestVoteOnce(ctx, req)
		if res.Err != nil {
			c.logger.Error("request vote round failed",
				zap.Error(res.Err),
				zap.String("requestID", common.GetRequestID(ctx)))
			// Retry on error, but respect context cancellation
			continue
		}
		return res
	}
}

// send one rpc to all the peers and wait for the rpc timeout to calculate the result
// @return: handle ErrMembershipErr, ErrMajorityNotMet
// all rpc are timeoutbounded out of the box, so the function takes as long as the rpc timeout
func (c *consensus) requestVoteOnce(ctx context.Context, req *rpc.RequestVoteRequest) *MajorityRequestVoteResp {

	// CHECK MEMBERSHIP
	requestID := common.GetRequestID(ctx)
	total := c.membership.GetTotalMemberCount()
	peerClients, err := c.membership.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return &MajorityRequestVoteResp{Err: common.ErrMembershipErr}
	}
	if !calculateIfMajorityMet(total, len(peerClients)) {
		c.logger.Error("Not enough peers for majority",
			zap.Int("total", total),
			zap.Int("peerCount", len(peerClients)),
			zap.String("requestID", requestID))
		return &MajorityRequestVoteResp{Err: common.ErrMembershipErr}
	}

	// FAN-OUT, FAN-IN
	fanOutFunc := func(c peers.PeerClient, fanInChan chan<- *utils.RPCRespWrapper[*rpc.RequestVoteResponse], wg *sync.WaitGroup) {
		defer wg.Done()
		resp, err := c.RequestVote(ctx, req)
		res := &utils.RPCRespWrapper[*rpc.RequestVoteResponse]{
			Resp:       resp,
			Err:        err,
			PeerNodeID: c.GetNodeID(),
		}
		fanInChan <- res
	}

	peersCount := len(peerClients)
	fanInChan := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], peersCount) // buffered with len(members) to prevent goroutine leak
	fanInChanDone := make(chan struct{})
	wg := sync.WaitGroup{}
	for _, pc := range peerClients {
		c := pc
		wg.Add(1) // add cannot be concurrently with wait, so we cannot move it into another goroutine
		go fanOutFunc(c, fanInChan, &wg)
	}
	go func() {
		wg.Wait()
		close(fanInChan)
		close(fanInChanDone)
	}()

	// wait until the fan-in is done or the context is done
	select {
	case <-ctx.Done():
		return &MajorityRequestVoteResp{
			Err: common.ErrContextDone,
		}
	case <-fanInChanDone:
	}

	// filter out the errors first
	voteFailed := 0
	correctRes := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], len(peerClients))
	for wrappedRes := range fanInChan {
		if err := wrappedRes.Err; err != nil {
			voteFailed++
		} else {
			resp := wrappedRes.Resp
			if resp.Term < req.Term {
				panic("invariant broken: the peer should at least have the same term as the request")
			}
			correctRes <- wrappedRes
		}
	}
	close(correctRes)
	if calculateIfAlreadyFail(total, peersCount, 0, voteFailed) {
		return &MajorityRequestVoteResp{
			Err: common.ErrMajorityNotMet,
		}
	}

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
		} else {
			voteFailed++ // probably the current node has not the latest commit index
			c.logger.Warn("current node probablyhas not the latest commit index, so it fails to get the vote from a peer",
				zap.String("requestID", requestID))
		}
	}
	if calculateIfMajorityMet(total, peerVoteAccumulated) {
		return &MajorityRequestVoteResp{
			Term:        req.Term,
			VoteGranted: true,
		}
	} else {
		return &MajorityRequestVoteResp{
			Err: common.ErrMajorityNotMet,
		}
	}
}
