package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

// ---------------------------------------basic method to apply logs: applyAllLaggedCommitedLogs---------------------------------------

func TestApplyAllLaggedCommitedLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMembership := peers.NewMockMembership(ctrl)
	node := getMockNode(mockMembership)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	t.Run("correct case", func(t *testing.T) {
		node.applyAllLaggedCommitedLogs(context.Background())
	})

	t.Run("panic when leaderApplyCh not empty", func(t *testing.T) {
		node.leaderApplyCh = make(chan *utils.ClientCommandInternalReq, 10)
		node.leaderApplyCh <- &utils.ClientCommandInternalReq{}
		fmt.Println("leaderApplyCh length", len(node.leaderApplyCh))
		assert.Panics(t, func() {
			node.applyAllLaggedCommitedLogs(ctx)
		})
	})
}

func TestApplyAllLaggedCommitedLogs_CommitIdxEqualsLastApplied(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMembership := peers.NewMockMembership(ctrl)
	node := getMockNode(mockMembership)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	node.stateRWLock.Lock()
	node.commitIndex = 5
	node.lastApplied = 5
	node.stateRWLock.Unlock()

	err := node.applyAllLaggedCommitedLogs(ctx)
	assert.NoError(t, err)
	// Should still be equal since no logs needed to be applied
	commitIdx, lastApplied := node.getCommitIdxAndLastApplied()
	assert.Equal(t, uint64(5), commitIdx)
	assert.Equal(t, uint64(5), lastApplied)
}

func TestApplyAllLaggedCommitedLogs_CommitIdxGreaterThanLastApplied(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockMembership := peers.NewMockMembership(ctrl)
	node := getMockNode(mockMembership)
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	stateMachine := plugs.NewMockStateMachineIface(ctrl)
	mockLogs := log.NewMockRaftLogsIface(ctrl)
	node.raftLog = mockLogs
	node.statemachine = stateMachine
	node.commitIndex = 10
	node.lastApplied = 5
	expectedLogs := make([]*log.RaftLogEntry, node.commitIndex-node.lastApplied)
	for i := range node.commitIndex - node.lastApplied {
		expectedLogs[i] = &log.RaftLogEntry{
			Term:     1,
			Commands: fmt.Appendf(nil, "test command %d", i),
		}
	}
	t.Run("success case - correct number of logs", func(t *testing.T) {
		fmt.Println("expectedLogs", len(expectedLogs))
		mockLogs.EXPECT().ReadLogsInBatchFromIdx(gomock.Any()).Return(expectedLogs, nil)
		stateMachine.EXPECT().BatchApplyCommand(gomock.Any(), gomock.Any()).Return(nil, nil)

		err := node.applyAllLaggedCommitedLogs(ctx)
		assert.NoError(t, err)

		commitIdx, lastApplied := node.getCommitIdxAndLastApplied()
		fmt.Println("commitIdx", commitIdx)
		fmt.Println("lastApplied", lastApplied)
		assert.Equal(t, uint64(10), commitIdx)
		assert.Equal(t, uint64(10), lastApplied)
	})

	t.Run("error case - incorrect number of logs", func(t *testing.T) {
		// Add an extra log entry that shouldn't be there
		extraLogs := append(expectedLogs, &log.RaftLogEntry{
			Term:     1,
			Commands: []byte("extra log that shouldn't exist"),
		})
		node.commitIndex = 10
		node.lastApplied = 5
		mockLogs.EXPECT().ReadLogsInBatchFromIdx(gomock.Any()).Return(extraLogs, nil)
		stateMachine.EXPECT().BatchApplyCommand(gomock.Any(), gomock.Any()).Return(nil, nil)

		assert.Panics(t, func() { node.applyAllLaggedCommitedLogs(ctx) })

	})
}
