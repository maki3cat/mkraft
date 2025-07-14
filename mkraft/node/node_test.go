package node

import (
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------VoteRequest---------------------------------
func TestNode_VoteRequest(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("request accepted", func(t *testing.T) {
		respChan := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
		req := &utils.RequestVoteInternalReq{
			RespChan: respChan,
		}
		n.VoteRequest(req)
		// Verify request was added to channel
		select {
		case <-n.requestVoteCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		// Fill up channel
		for i := 0; i < cap(n.requestVoteCh); i++ {
			n.requestVoteCh <- &utils.RequestVoteInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.RequestVoteResponse], 1)
		req := &utils.RequestVoteInternalReq{
			RespChan: respChan,
		}
		n.VoteRequest(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}

// ---------------------------------AppendEntryRequest---------------------------------
func TestNode_AppendEntryRequest(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("request accepted", func(t *testing.T) {
		respChan := make(chan *utils.RPCRespWrapper[*rpc.AppendEntriesResponse], 1)
		req := &utils.AppendEntriesInternalReq{
			RespChan: respChan,
		}
		n.AppendEntryRequest(req)
		// Verify request was added to channel
		select {
		case <-n.appendEntryCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		// Fill up channel
		for i := 0; i < cap(n.appendEntryCh); i++ {
			n.appendEntryCh <- &utils.AppendEntriesInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.AppendEntriesResponse], 1)
		req := &utils.AppendEntriesInternalReq{
			RespChan: respChan,
		}
		n.AppendEntryRequest(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}

// ---------------------------------ClientCommand---------------------------------
func TestNode_ClientCommand(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	t.Run("not leader", func(t *testing.T) {
		n.state = StateFollower
		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrNotLeader, resp.Err)
		default:
			t.Error("No error response received")
		}
	})

	t.Run("request accepted", func(t *testing.T) {
		n.state = StateLeader
		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)
		// Verify request was added to channel
		select {
		case <-n.clientCommandCh:
			// Success
		default:
			t.Error("Request was not added to channel")
		}
	})

	t.Run("channel full", func(t *testing.T) {
		n.state = StateLeader
		// Fill up channel
		for i := 0; i < cap(n.clientCommandCh); i++ {
			n.clientCommandCh <- &utils.ClientCommandInternalReq{}
		}

		respChan := make(chan *utils.RPCRespWrapper[*rpc.ClientCommandResponse], 1)
		req := &utils.ClientCommandInternalReq{
			RespChan: respChan,
		}
		n.ClientCommand(req)

		// Verify error response
		select {
		case resp := <-respChan:
			assert.Equal(t, common.ErrServerBusy, resp.Err)
		default:
			t.Error("No error response received")
		}
	})
}
