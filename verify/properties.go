// package main

// import (
// 	"fmt"
// 	"os"
// 	"strings"

// 	"github.com/maki3cat/mkraft/mkraft/log"
// )

// // -------------------property of leader election safety-------------------

// func VerifyLeaderSafetyFromFiles(nodeToStateFilePath map[string]string) (bool, error) {
// 	// Map to track leaders per term
// 	termToLeaders := make(map[uint32][]string)

// 	// Process each node's state history file
// 	for nodeId, filePath := range nodeToStateFilePath {
// 		entries, err := os.ReadFile(filePath)
// 		if err != nil {
// 			return false, fmt.Errorf("failed to read state file for node %s: %v", nodeId, err)
// 		}

// 		// Split into lines and process each state entry
// 		lines := strings.Split(strings.TrimSpace(string(entries)), "\n")
// 		for _, line := range lines {
// 			// Parse state entry format: #<timestamp>, term <term>, nodeID <id>, state <state>, vote for <vote>#
// 			if !strings.HasPrefix(line, "#") || !strings.HasSuffix(line, "#") {
// 				continue
// 			}

// 			// Extract term and state
// 			parts := strings.Split(line[1:len(line)-1], ", ")
// 			if len(parts) < 4 {
// 				continue
// 			}

// 			var term uint32
// 			var state string
// 			for _, part := range parts {
// 				if strings.HasPrefix(part, "term ") {
// 					fmt.Sscanf(part, "term %d", &term)
// 				} else if strings.HasPrefix(part, "state ") {
// 					state = strings.TrimPrefix(part, "state ")
// 				}
// 			}

// 			// Record if node was a leader for this term
// 			if state == "Leader" {
// 				termToLeaders[term] = append(termToLeaders[term], nodeId)
// 			}
// 		}
// 	}

// 	// Check for terms with multiple leaders
// 	for term, leaders := range termToLeaders {
// 		if len(leaders) > 1 {
// 			return false, fmt.Errorf("term %d had multiple leaders: %v", term, leaders)
// 		}
// 	}

// 	return true, nil
// }

// // -------------------property of log mathcing-------------------
// // suppose with log compaction, the logs are sure to be in the memory altogether
// func VerifyLogMatching(nodeToStateEntries map[string][]*log.RaftLogEntry) (bool, error) {
// 	size := 0
// 	for nodeId, logs := range nodeToStateEntries {
// 		if size == 0 {
// 			size = len(logs)
// 		} else if size != len(logs) {
// 			return false, fmt.Errorf("node %s has %d logs, but expected %d", nodeId, len(logs), size)
// 		}
// 	}
// 	if size == 0 {
// 		return false, fmt.Errorf("no logs")
// 	}

// 	earliestDiscrepancyIdx := -1
// 	for i := range size {
// 		entries := make([]*log.RaftLogEntry, 0)
// 		for _, logs := range nodeToStateEntries {
// 			entries = append(entries, logs[i])
// 		}
// 		// check if all term of the entries are the same
// 		termEqual := true
// 		dataEqual := true
// 		term := entries[0].Term
// 		data := entries[0].Commands
// 		for _, entry := range entries {
// 			if entry.Term != term {
// 				termEqual = false
// 				break
// 			}
// 			if string(entry.Commands) != string(data) {
// 				dataEqual = false
// 				break
// 			}
// 		}
// 		if !(termEqual && dataEqual) {
// 			earliestDiscrepancyIdx = i
// 		}
// 		if termEqual && dataEqual && earliestDiscrepancyIdx != -1 {
// 			return false, nil
// 		}
// 	}
// 	return true, nil
// }

// // -------------------property of state machine safety-------------------
// // we assume the state machien apply the command log by just appned the command with \n separated the file

// func VerifyStateMachineSafety(nodeToStateMachineData [][]string) (bool, error) {
// 	nodeCount := len(nodeToStateMachineData)
// 	lineCount := len(nodeToStateMachineData[0])
// 	for _, data := range nodeToStateMachineData {
// 		if len(data) != lineCount {
// 			return false, fmt.Errorf("node %s has %d lines, but expected %d", data[0], len(data), lineCount)
// 		}
// 	}
// 	// compare each line of each node's data to see if they are the same
// 	for i := range lineCount {
// 		for j := range nodeCount {
// 			if nodeToStateMachineData[j][i] != nodeToStateMachineData[0][i] {
// 				return false, nil
// 			}
// 		}
// 	}
// 	return true, nil
// }