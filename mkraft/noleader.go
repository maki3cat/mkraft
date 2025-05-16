package mkraft

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

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
func (n *Node) RunAsFollower(ctx context.Context) {

	if n.State != StateFollower {
		panic("node is not in FOLLOWER state")
	}

	n.logger.Info("node acquires to run in FOLLOWER state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("acquired semaphore in FOLLOWER state")
	defer n.sem.Release(1)

	electionTicker := time.NewTicker(n.cfg.GetElectionTimeout())
	defer electionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("context done")
					n.membership.GracefulShutdown()
					return
				case <-electionTicker.C:
					n.State = StateCandidate
					go n.RunAsCandidate(ctx)
					return
				case requestVoteInternal := <-n.requestVoteChan:
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := n.receiveVoteRequest(requestVoteInternal.Req)
					wrappedResp := RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					requestVoteInternal.RespChan <- &wrappedResp
					electionTicker.Reset(n.cfg.GetElectionTimeout())
				case req := <-n.appendEntryChan:
					electionTicker.Stop()
					// for the follower, the node state has no reason to change because of the request
					resp := n.receiveAppendEntires(req.Req)
					wrappedResp := RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					electionTicker.Reset(n.cfg.GetElectionTimeout())
				}
			}
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
func (n *Node) RunAsCandidate(ctx context.Context) {
	if n.State != StateCandidate {
		panic("node is not in CANDIDATE state")
	}

	n.logger.Info("node starts to acquiring CANDIDATE state")
	n.sem.Acquire(ctx, 1)
	n.logger.Info("node has acquired semaphore in CANDIDATE state")
	defer n.sem.Release(1)

	consensusChan := n.runOneElection(ctx)

	ticker := time.NewTicker(n.cfg.GetElectionTimeout())
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("raft node's main context done, exiting")
			return
		default:
			{
				select {
				case <-ctx.Done():
					n.logger.Warn("raft node main context done")
					n.membership.GracefulShutdown()
					return
				case response := <-consensusChan: // some response from last election
					// I don't think we need to reset the ticker here
					// voteCancel() // cancel the rest
					if response.VoteGranted {
						n.State = StateLeader
						go n.RunAsLeader(ctx)
						return
					} else {
						if response.Term > n.CurrentTerm {
							// some one has become a leader
							// voteCancel()
							n.CurrentTerm = response.Term
							n.ResetVoteFor()
							n.State = StateFollower
							go n.RunAsFollower(ctx)
							return
						} else {
							n.logger.Info(
								"not enough votes, re-elect again",
								zap.Int("term", int(n.CurrentTerm)), zap.String("nId", n.NodeId))
						}
					}
				case <-ticker.C: // last election timeout withno response
					// voteCancel()
					consensusChan = n.runOneElection(ctx)
				case requestVoteInternal := <-n.requestVoteChan: // commonRule: handling voteRequest from another candidate
					if requestVoteInternal.IsTimeout.Load() {
						n.logger.Warn("request vote is timeout")
						continue
					}
					req := requestVoteInternal.Req
					resChan := requestVoteInternal.RespChan
					resp := n.receiveVoteRequest(req)
					wrappedResp := RPCRespWrapper[*rpc.RequestVoteResponse]{
						Resp: resp,
						Err:  nil,
					}
					resChan <- &wrappedResp
					// this means other candiate has a higher term
					if resp.VoteGranted {
						n.State = StateFollower
						go n.RunAsFollower(ctx)
						return
					}
				case req := <-n.appendEntryChan: // commonRule: handling appendEntry from a leader which can be stale or new
					resp := n.receiveAppendEntires(req.Req)
					wrappedResp := RPCRespWrapper[*rpc.AppendEntriesResponse]{
						Resp: resp,
						Err:  nil,
					}
					req.RespChan <- &wrappedResp
					if resp.Success {
						// this means there is a leader there
						n.State = StateFollower
						n.CurrentTerm = req.Req.Term
						go n.RunAsFollower(ctx)
						return
					}
				}
			}
		}
	}
}
