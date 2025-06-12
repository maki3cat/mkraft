package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.uber.org/mock/gomock"
)

func TestNode_asyncSendElection_HappyPath(t *testing.T) {

	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(&MajorityRequestVoteResp{
		Term:        1,
		VoteGranted: true,
	}, nil)

	consensusChan := n.asyncSendElection(context.Background())

	select {
	case resp, ok := <-consensusChan:
		assert.True(t, ok)
		assert.Equal(t, uint32(1), resp.Term)
		assert.True(t, resp.VoteGranted)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}

func TestNode_asyncSendElection_Err(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(nil, errors.New("test mock error"))

	consensusChan := n.asyncSendElection(context.Background())

	select {
	case resp, ok := <-consensusChan:
		assert.False(t, ok)
		assert.Nil(t, resp)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}

func TestNode_asyncSendElection_ContextCancelled(t *testing.T) {
	n, ctrl := newMockNode(t)
	defer cleanUpTmpDir(ctrl)

	mockedConsensus := n.consensus.(*MockConsensus)
	mockedConsensus.EXPECT().ConsensusRequestVote(gomock.Any(), gomock.Any()).Return(nil, errors.New("test mock error"))

	ctx, cancel := context.WithCancel(context.Background())
	consensusChan := n.asyncSendElection(ctx)
	cancel()

	select {
	case resp, ok := <-consensusChan:
		assert.False(t, ok)
		assert.Nil(t, resp)
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "timeout")
	}
}
