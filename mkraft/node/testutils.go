package node

import (
	"os"
	"testing"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/peers"
	"github.com/maki3cat/mkraft/mkraft/persister"
	"github.com/maki3cat/mkraft/mkraft/plugs"
	gomock "go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func newMockNode(t *testing.T) (*nodeImpl, *gomock.Controller) {
	ctrl := gomock.NewController(t)

	mockRaftLog := persister.NewMockRaftLogs(ctrl)
	mockRaftLog.EXPECT().GetLastLogIdx().Return(uint64(0)).AnyTimes()
	mockRaftLog.EXPECT().CheckPreLog(gomock.Any(), gomock.Any()).Return(true).AnyTimes()

	config := common.GetDefaultConfig()
	config.SetDataDir("./tmp/")
	err := os.MkdirAll(config.GetDataDir(), 0755)
	if err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	membership := peers.NewMockMembership(ctrl)
	statemachine := plugs.NewMockStateMachine(ctrl)
	consensus := NewMockConsensus(ctrl)

	n := NewNode("1", config, zap.NewNop(), membership, statemachine, mockRaftLog, consensus)
	node := n.(*nodeImpl)
	return node, ctrl
}

func cleanUpTmpDir(ctrl *gomock.Controller) {
	if ctrl != nil {
		ctrl.Finish()
	}
	os.RemoveAll("./tmp/")
}