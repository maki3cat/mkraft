package node

import (
	"fmt"
	"path/filepath"
)

const (
	LeaderStateFileName = "leader_%s.mk"
)

func getLeaderStateFileName(nodeID string) string {
	return fmt.Sprintf(LeaderStateFileName, nodeID)
}

func getLeaderStateFilePath(nodeID string, dateDir string) string {
	stateFileName := getLeaderStateFileName(nodeID)
	return filepath.Join(dateDir, stateFileName)
}
