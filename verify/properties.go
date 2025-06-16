package verify

import (
	"fmt"
	"os"
	"strings"

	"github.com/maki3cat/mkraft/mkraft/log"
	"github.com/maki3cat/mkraft/mkraft/node"
	"go.uber.org/zap"
)

var logger, _ = zap.NewDevelopment()

// -------------------property of leader election safety-------------------

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

// -------------------property of log mathcing-------------------
// suppose with log compaction, the logs are sure to be in the memory altogether
func VerifyLogMatching(nodeToStateEntries map[string][]*log.RaftLogEntry) (bool, error) {
	size := 0
	for nodeId, logs := range nodeToStateEntries {
		if size == 0 {
			size = len(logs)
		} else if size != len(logs) {
			return false, fmt.Errorf("node %s has %d logs, but expected %d", nodeId, len(logs), size)
		}
	}
	if size == 0 {
		return false, fmt.Errorf("no logs")
	}

	earliestDiscrepancyIdx := -1
	for i := range size {
		entries := make([]*log.RaftLogEntry, 0)
		for _, logs := range nodeToStateEntries {
			entries = append(entries, logs[i])
		}
		// check if all term of the entries are the same
		termEqual := true
		dataEqual := true
		term := entries[0].Term
		data := entries[0].Commands
		for _, entry := range entries {
			if entry.Term != term {
				termEqual = false
				break
			}
			if string(entry.Commands) != string(data) {
				dataEqual = false
				break
			}
		}
		if !(termEqual && dataEqual) {
			earliestDiscrepancyIdx = i
		}
		if termEqual && dataEqual && earliestDiscrepancyIdx != -1 {
			return false, nil
		}
	}
	return true, nil
}

// -------------------property of state machine safety-------------------
// we assume the state machien apply the command log by just appned the command with \n separated the file

func VerifyStateMachineSafety(nodeToStateMachineData [][]string) (bool, error) {
	nodeCount := len(nodeToStateMachineData)
	lineCount := len(nodeToStateMachineData[0])
	for _, data := range nodeToStateMachineData {
		if len(data) != lineCount {
			return false, fmt.Errorf("node %s has %d lines, but expected %d", data[0], len(data), lineCount)
		}
	}
	// compare each line of each node's data to see if they are the same
	for i := range lineCount {
		for j := range nodeCount {
			if nodeToStateMachineData[j][i] != nodeToStateMachineData[0][i] {
				return false, nil
			}
		}
	}
	return true, nil
}
