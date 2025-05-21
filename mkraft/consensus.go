package mkraft

import (
	"context"
	"errors"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

type AppendEntriesConsensusResp struct {
	Term    uint32
	Success bool
}

type MajorityRequestVoteResp struct {
	Term        uint32
	VoteGranted bool
}

func (c *Node) ConsensusRequestVote(ctx context.Context, request *rpc.RequestVoteRequest) (*MajorityRequestVoteResp, error) {

	requestID := common.GetRequestID(ctx)
	c.logger.Debug("Starting RequestVoteSendForConsensus",
		zap.Uint32("term", request.Term),
		zap.String("candidateId", request.CandidateId),
		zap.String("requestID", requestID))

	total := c.membership.GetMemberCount()
	c.logger.Debug("Got member count",
		zap.Int("total", total),
		zap.String("requestID", requestID))

	peerClients, err := c.membership.GetAllPeerClients()
	if err != nil {
		c.logger.Error("error in getting all peer clients",
			zap.Error(err),
			zap.String("requestID", requestID))
		return nil, err
	}
	c.logger.Debug("Got peer clients",
		zap.Int("peerCount", len(peerClients)),
		zap.String("requestID", requestID))

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
					return nil, errors.New("majority of nodes failed to respond")
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
						c.logger.Debug("Vote granted",
							zap.Int("votesAccumulated", peerVoteAccumulated),
							zap.String("requestID", requestID))
						if calculateIfMajorityMet(total, peerVoteAccumulated) {
							c.logger.Info("Majority achieved",
								zap.Int("votesNeeded", total/2+1),
								zap.Int("votesReceived", peerVoteAccumulated),
								zap.String("requestID", requestID))
							return &MajorityRequestVoteResp{
								Term:        request.Term,
								VoteGranted: true,
							}, nil
						}
					} else {
						voteFailed++
						c.logger.Debug("Vote denied",
							zap.Int("votesFailed", voteFailed),
							zap.String("requestID", requestID))
						if calculateIfAlreadyFail(total, peersCount, peerVoteAccumulated, voteFailed) {
							c.logger.Info("Failed to get majority",
								zap.Int("votesNeeded", total/2+1),
								zap.Int("votesReceived", peerVoteAccumulated),
								zap.Int("votesFailed", voteFailed),
								zap.String("requestID", requestID))
							return nil, errors.New("majority of nodes failed to respond")
						}
					}
				}
				if resp.Term < request.Term {
					panic("this should not happen, the consensus algorithm is not implmented correctly")
				}
			}
		case <-ctx.Done():
			return nil, errors.New("context done")
		}
	}
	c.logger.Error("Unexpected exit from vote collection loop",
		zap.Int("votesAccumulated", peerVoteAccumulated),
		zap.Int("votesFailed", voteFailed),
		zap.String("requestID", requestID))
	return nil, errors.New("this should not happen, the consensus algorithm is not implmented correctly")
}

// also update the peer index in this method
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
	if !calculateIfMajorityMet(total, len(peerClients)) {
		n.logger.Error("not enough peer clients found",
			zap.String("requestID", requestID))
		return nil, errors.New("not enough peer clients found")
	}

	allRespChan := make(chan utils.RPCRespWrapper[*rpc.AppendEntriesResponse], len(peerClients))
	for nodeID, member := range peerClients {
		// FAN-OUT
		go func(nodeID string, client peers.InternalClientIface) {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, n.cfg.GetElectionTimeout())
			defer cancel()
			req := peerReq[nodeID]
			// FAN-IN
			resp := client.SendAppendEntries(ctxWithTimeout, req)
			if resp.Err == nil && resp.Resp.Success {
				n.updatePeerIndexAfterAppendEntries(nodeID, req)
			}
			// update the peers' index
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
