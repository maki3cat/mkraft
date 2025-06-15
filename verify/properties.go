package verify

import (
	"fmt"
	"os"
	"strings"

	"github.com/maki3cat/mkraft/mkraft/node"
	"go.uber.org/zap"
)

var logger, _ = zap.NewDevelopment()

func VerifyLeaderSafetyFromFiles(nodeToStateFilePath map[string]string) (bool, error) {
	nodeToStateEntries := make(map[string]string)
	for nodeId, stateFilePath := range nodeToStateFilePath {
		entries, err := os.ReadFile(stateFilePath)
		if err != nil {
			logger.Error("failed to read state file", zap.String("nodeId", nodeId), zap.String("stateFilePath", stateFilePath), zap.Error(err))
			return false, err
		}
		nodeToStateEntries[nodeId] = string(entries)
	}
	return verifyLeaderSafety(nodeToStateEntries)
}

func verifyLeaderSafety(nodeToStateEntries map[string]string) (bool, error) {
	termToStates := make(map[uint32]map[node.NodeState][]string)
	for _, stateEntries := range nodeToStateEntries {
		stateEntries = strings.TrimSpace(stateEntries)
		if stateEntries == "" {
			continue
		}
		// Parse each line in the state file
		entries := strings.SplitN(strings.TrimSpace(stateEntries), "\n", -1)
		for _, entry := range entries {
			term, nodeId, state, err := node.DeserializeNodeStateEntry(entry)
			if err != nil {
				return false, err
			}

			// Initialize map for this term if needed
			if _, exists := termToStates[term]; !exists {
				termToStates[term] = make(map[node.NodeState][]string)
				termToStates[term][node.StateLeader] = []string{}
				termToStates[term][node.StateFollower] = []string{}
				termToStates[term][node.StateCandidate] = []string{}
			}

			// Add node to appropriate state list for this term
			termToStates[term][state] = append(termToStates[term][state], nodeId)
		}
	}
	// check if there is a leader in each term
	for term, states := range termToStates {
		if len(states[node.StateLeader]) != 1 {
			fmt.Println("term", term, "has", len(states[node.StateLeader]), "leaders")
			return false, nil
		}
	}
	return true, nil
}
