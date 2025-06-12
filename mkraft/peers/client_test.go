package peers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
)

func TestNewRobustClientImpl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	logger := zap.NewNop()

	client, err := NewPeerClientImpl("test-node", "localhost:8080", logger, mockConfig)
	require.NoError(t, err)
	require.NotNil(t, client)

	assert.Equal(t, "test-node", client.nodeId)
	assert.Equal(t, "localhost:8080", client.nodeAddr)
}

func setupAppendEntriesTest(t *testing.T) (*peerClient, *rpc.MockRaftServiceClient, *rpc.AppendEntriesRequest) {
	ctrl := gomock.NewController(t)
	t.Cleanup(func() { ctrl.Finish() })

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	client := &peerClient{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.AppendEntriesRequest{
		Term:     1,
		LeaderId: "leader",
	}

	return client, mockClient, req
}

func TestAppendEntriesWithRetry_Success(t *testing.T) {
	client, mockClient, req := setupAppendEntriesTest(t)

	expectedResp := &rpc.AppendEntriesResponse{
		Term:    1,
		Success: true,
	}

	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).
		Return(expectedResp, nil)

	resp, err := client.AppendEntriesWithRetry(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestAppendEntriesWithRetry_ContextCancellation(t *testing.T) {
	client, mockClient, req := setupAppendEntriesTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).
		Return(nil, assert.AnError)
	cancel() // Cancel context before retry

	resp, err := client.AppendEntriesWithRetry(ctx, req)
	require.Error(t, err)
	assert.Equal(t, common.ErrContextDone, err)
	assert.Nil(t, resp)
}

func TestAppendEntriesWithRetry_FailAfterThreeRetries(t *testing.T) {
	client, mockClient, req := setupAppendEntriesTest(t)

	// Expect 3 failed attempts
	mockClient.EXPECT().
		AppendEntries(gomock.Any(), req).
		Return(nil, assert.AnError).
		Times(3)

	resp, err := client.AppendEntriesWithRetry(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, assert.AnError, err)
	assert.Nil(t, resp)
}

func TestRobustClientImpl_RequestVoteWithRetry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	client := &peerClient{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	expectedResp := &rpc.RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.RequestVoteWithRetry(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestRobustClientImpl_RequestVoteWithRetry_ErrorWithRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	client := &peerClient{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	expectedResp := &rpc.RequestVoteResponse{
		Term:        1,
		VoteGranted: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, assert.AnError).
		Times(1)
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(expectedResp, nil).
		Times(1)

	resp, err := client.RequestVoteWithRetry(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestRobustClientImpl_RequestVoteWithRetry_ContextCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	client := &peerClient{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, assert.AnError).
		Times(0)

	cancel() // Cancel context before retry
	resp, err := client.RequestVoteWithRetry(ctx, req)
	require.Error(t, err)
	assert.Equal(t, common.ErrContextDone, err)
	assert.Nil(t, resp)
}

func TestRobustClientImpl_RequestVoteWithRetry_ContextCancellationAfterRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := rpc.NewMockRaftServiceClient(ctrl)
	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")

	client := &peerClient{
		nodeId:    "test-node",
		nodeAddr:  "localhost:8080",
		rawClient: mockClient,
		logger:    zap.NewNop(),
		cfg:       mockConfig,
	}

	req := &rpc.RequestVoteRequest{
		Term:         1,
		CandidateId:  "candidate",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	mockClient.EXPECT().
		RequestVote(gomock.Any(), req).
		Return(nil, assert.AnError).
		MinTimes(1)

	// Start the request in a goroutine
	respChan := make(chan *rpc.RequestVoteResponse)
	errChan := make(chan error)
	go func() {
		resp, err := client.RequestVoteWithRetry(ctx, req)
		respChan <- resp
		errChan <- err
	}()

	// Wait a bit to allow 3 retries
	time.Sleep(time.Millisecond * 100)
	cancel() // Cancel after 3 retries

	resp := <-respChan
	err := <-errChan
	require.Error(t, err)
	assert.Equal(t, common.ErrContextDone, err)
	assert.Nil(t, resp)
}

func TestRobustClientImpl_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConfig := common.GetDefaultConfig()
	mockConfig.SetDataDir("./tmp/")
	client := &peerClient{
		nodeId:   "test-node",
		nodeAddr: "localhost:8080",
		logger:   zap.NewNop(),
		cfg:      mockConfig,
	}

	// Test nil connection
	err := client.Close()
	require.NoError(t, err)

	// Test with connection
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())
	require.NoError(t, err)
	client.conn = conn

	err = client.Close()
	require.NoError(t, err)
}
