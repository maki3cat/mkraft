package node

import (
	"os"
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newMockNode(t *testing.T) *nodeImpl {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRaftLog := log.NewMockRaftLogs(ctrl)
	mockRaftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()

	config := common.GetDefaultConfig()
	config.SetDataDir("./tmp/")

	membership := peers.NewMockMembership(ctrl)
	statemachine := plugs.NewMockStateMachine(ctrl)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog)
	node := n.(*nodeImpl)
	return node
}

func cleanUpTmpDir() {
	os.RemoveAll("./tmp/")
}
