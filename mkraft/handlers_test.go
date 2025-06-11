package mkraft

import (
	"context"
	"errors"
	"testing"

	"github.com/maki3cat/mkraft/mkraft/node"
	"github.com/maki3cat/mkraft/mkraft/utils"
	pb "github.com/maki3cat/mkraft/rpc"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

// Example for TestRequestVote_Success
func TestRequestVote_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node1",
	}
	expectedResp := &pb.RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	node.EXPECT().VoteRequest(gomock.Any()).DoAndReturn(
		func(req *utils.RequestVoteInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.RequestVoteResponse]{
				Resp: expectedResp,
				Err:  nil,
			}
		})

	resp, err := handlers.RequestVote(context.Background(), expectedReq)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp != expectedResp {
		t.Errorf("unexpected response: got %v want %v", resp, expectedResp)
	}
}

func TestRequestVote_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node1",
	}
	expectedErr := errors.New("vote request failed")

	node.EXPECT().VoteRequest(gomock.Any()).DoAndReturn(
		func(req *utils.RequestVoteInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.RequestVoteResponse]{
				Resp: nil,
				Err:  expectedErr,
			}
		})

	resp, err := handlers.RequestVote(context.Background(), expectedReq)
	if err != expectedErr {
		t.Errorf("unexpected error: got %v want %v", err, expectedErr)
	}
	if resp != nil {
		t.Errorf("unexpected response: got %v want nil", resp)
	}
}

func TestAppendEntries_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*pb.LogEntry{},
		LeaderCommit: 0,
	}
	expectedResp := &pb.AppendEntriesResponse{
		Term:    1,
		Success: true,
	}

	node.EXPECT().AppendEntryRequest(gomock.Any()).DoAndReturn(
		func(req *utils.AppendEntriesInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.AppendEntriesResponse]{
				Resp: expectedResp,
				Err:  nil,
			}
		})

	resp, err := handlers.AppendEntries(context.Background(), expectedReq)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp != expectedResp {
		t.Errorf("unexpected response: got %v want %v", resp, expectedResp)
	}
}

func TestAppendEntries_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*pb.LogEntry{},
		LeaderCommit: 0,
	}
	expectedErr := errors.New("append entries failed")

	node.EXPECT().AppendEntryRequest(gomock.Any()).DoAndReturn(
		func(req *utils.AppendEntriesInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.AppendEntriesResponse]{
				Resp: nil,
				Err:  expectedErr,
			}
		})

	resp, err := handlers.AppendEntries(context.Background(), expectedReq)
	if err != expectedErr {
		t.Errorf("unexpected error: got %v want %v", err, expectedErr)
	}
	if resp != nil {
		t.Errorf("unexpected response: got %v want nil", resp)
	}
}

func TestClientCommand_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.ClientCommandRequest{
		Command: []byte("test command"),
	}
	expectedResp := &pb.ClientCommandResponse{
		Result: []byte("test result"),
	}

	node.EXPECT().ClientCommand(gomock.Any()).DoAndReturn(
		func(req *utils.ClientCommandInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.ClientCommandResponse]{
				Resp: expectedResp,
				Err:  nil,
			}
		})

	resp, err := handlers.ClientCommand(context.Background(), expectedReq)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if resp != expectedResp {
		t.Errorf("unexpected response: got %v want %v", resp, expectedResp)
	}
}

func TestClientCommand_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	node := node.NewMockNode(ctrl)
	defer ctrl.Finish()

	handlers := &Handlers{
		node:   node,
		logger: zap.NewNop(),
	}

	expectedReq := &pb.ClientCommandRequest{
		Command: []byte("test command"),
	}
	expectedErr := errors.New("client command failed")

	node.EXPECT().ClientCommand(gomock.Any()).DoAndReturn(
		func(req *utils.ClientCommandInternalReq) {
			if req.Req != expectedReq {
				t.Errorf("unexpected request: got %v want %v", req.Req, expectedReq)
			}
			req.RespChan <- &utils.RPCRespWrapper[*pb.ClientCommandResponse]{
				Resp: nil,
				Err:  expectedErr,
			}
		})

	resp, err := handlers.ClientCommand(context.Background(), expectedReq)
	if err != expectedErr {
		t.Errorf("unexpected error: got %v want %v", err, expectedErr)
	}
	if resp != nil {
		t.Errorf("unexpected response: got %v want nil", resp)
	}
}
