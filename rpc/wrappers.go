// This file defines the RPC client interface and its implementation
// the RPC client is responsible for sending the RPC requests to only one other node and try to get a response
// it doesn't manage the consensus between different nodes
// TODO: maki: need to learn and revist the future and retry pattern here
// golang gymnastics
// summarize the golang patterns here:
// 1) async call with future; 2) retry pattern; 3) context with timeout/cancel; 4)template and generics

// paper: the 2 RPCs defined by the Raft paper
// membership module shall be responsible for the client to each member

package rpc

import (
	"context"
	"fmt"
	"time"

	util "github.com/maki3cat/mkraft/util"
)

type RPCResWrapper[T RPCResponse] struct {
	Err  error
	Resp T
}

type RPCResponse interface {
	*AppendEntriesResponse | *RequestVoteResponse
}

// the real RPC wrapper used directly for the server
type InternalClientIface interface {
	SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCResWrapper[*RequestVoteResponse]
	SendAppendEntries(ctx context.Context, req *AppendEntriesRequest) RPCResWrapper[*AppendEntriesResponse]
}

type InternalClientImpl struct {
	rawClient RaftServiceClient
}

func NewInternalClient(raftServiceClient RaftServiceClient) InternalClientIface {
	return &InternalClientImpl{
		rawClient: raftServiceClient,
	}
}

// should call this with goroutine
// the parent shall control the timeout of the election
func (rc *InternalClientImpl) SendRequestVote(ctx context.Context, req *RequestVoteRequest) chan RPCResWrapper[*RequestVoteResponse] {
	out := make(chan RPCResWrapper[*RequestVoteResponse], 1)
	func() {
		retryTicker := time.NewTicker(time.Millisecond * util.RPC_REUQEST_TIMEOUT_IN_MS)
		defer retryTicker.Stop()

		var singleResChan chan RPCResWrapper[*RequestVoteResponse]
		callRPC := func() {
			singleCallCtx, singleCallCancel := context.WithTimeout(ctx, time.Millisecond*(util.RPC_REUQEST_TIMEOUT_IN_MS-10))
			defer singleCallCancel()
			// todo: make sure the synchronous call will consume the ctx timeout in someway
			response, err := rc.rawClient.RequestVote(singleCallCtx, req)
			wrapper := RPCResWrapper[*RequestVoteResponse]{
				Resp: response,
				Err:  err,
			}
			singleResChan <- wrapper
		}
		go callRPC()

		for {
			select {
			case <-ctx.Done():
				// will propagate to the child context as well
				out <- RPCResWrapper[*RequestVoteResponse]{Err: fmt.Errorf("context done without a response")}
				return
			case <-retryTicker.C:
				callRPC()
			case response := <-singleResChan:
				out <- response
				return
			}
		}
	}()
	return out
}

// the generator pattern
func (rc *InternalClientImpl) SendAppendEntries(
	ctx context.Context, req *AppendEntriesRequest) RPCResWrapper[*AppendEntriesResponse] {
	response, err := rc.rawClient.AppendEntries(ctx, req)
	wrapper := RPCResWrapper[*AppendEntriesResponse]{
		Resp: response,
		Err:  err,
	}
	return wrapper
}
