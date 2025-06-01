package node

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
)

func (n *Node) getIdxFileName() string {
	return "index.rft"
}

// implementation gap: the commitIdx and lastApplied shall be persisted in implementation
// if not, if all nodes shutdown, the commitIdx and lastApplied will be lost
// maki: this is a tricky part, discuss with the prof
func (n *Node) unsafeSaveIdx() error {
	// use create and rename to avoid data corruption
	// create index_timestamp.rft
	idxFileName := fmt.Sprintf("index_%s.rft", time.Now().Format("20060102150405"))
	err := os.WriteFile(idxFileName, []byte(fmt.Sprintf("%d,%d\n", n.commitIndex, n.lastApplied)), 0644)
	if err != nil {
		return err
	}
	// rename the file to index.rft
	return os.Rename(idxFileName, n.getIdxFileName())
}

func (n *Node) unsafeLoadIdx() error {
	file, err := os.Open(n.getIdxFileName())
	if err != nil {
		return err
	}
	defer file.Close()
	var commitIdx, lastApplied uint64
	_, err = fmt.Fscanf(file, "%d,%d\n", &commitIdx, &lastApplied)
	// if the last line is not formatted correctly, load the last but one line
	if err != nil {
		return err
	}
	n.commitIndex = commitIdx
	n.lastApplied = lastApplied
	return nil
}

// section1: for indices of commidID and lastApplied which are owned by all the nodes
func (n *Node) getCommitIdxAndLastApplied() (uint64, uint64) {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex, n.lastApplied
}

func (n *Node) getLastApplied() uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.lastApplied
}

func (n *Node) getCommitIdx() uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex
}

func (n *Node) incrementCommitIdx(numberOfCommand uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.commitIndex = n.commitIndex + numberOfCommand
	n.unsafeSaveIdx()
}

func (n *Node) incrementLastApplied(numberOfCommand uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.lastApplied = n.lastApplied + numberOfCommand
	n.unsafeSaveIdx()
}

// section2: for indices of leaders, nextIndex/matchIndex
// maki: Updating a follower’s match/next index is independent of whether consensus is reached.
// Updating matchIndex/nextIndex is a per-follower operation.
// Reaching consensus (a majority of nodes having the same entry) is a cluster-wide operation.
func (n *Node) incrementPeersNextIndexOnSuccess(nodeID string, logCnt uint64) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// important
	// According to the Log Matching Property in Raft:
	// once appendEntries is successful,
	// the follower's matchIndex should be equal to the index of the last entry appended,
	// and the nextIndex should be matchIndex + 1

	if _, exists := n.nextIndex[nodeID]; exists {
		n.nextIndex[nodeID] = n.nextIndex[nodeID] + logCnt
	} else {
		n.nextIndex[nodeID] = logCnt + 1
	}
	n.matchIndex[nodeID] = n.nextIndex[nodeID] - 1
}

// important invariant: matchIndex[follower] ≤ nextIndex[follower] - 1
// if the matchIndex is less than nextIndex-1, appendEntries will fail and the follower's nextIndex will be decremented
func (n *Node) decrementPeersNextIndexOnFailure(nodeID string) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// todo: there can be improvement of efficiency here, refer to the paper page 8 first paragraph
	n.logger.Warn("fixing inconsistent logs for peer",
		zap.String("nodeID", nodeID))

	if n.nextIndex[nodeID] > 1 {
		n.nextIndex[nodeID] -= 1
	} else {
		n.logger.Error("next index is already at 1, cannot decrement", zap.String("nodeID", nodeID))
	}
}

func (n *Node) getPeersNextIndex(nodeID string) uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	if index, ok := n.nextIndex[nodeID]; ok {
		return index
	} else {
		n.nextIndex[nodeID], n.matchIndex[nodeID] = n.getInitDefaultValuesForPeer()
		return n.nextIndex[nodeID]
	}
}

// returns nextIndex, matchIndex
func (n *Node) getInitDefaultValuesForPeer() (uint64, uint64) {
	return n.raftLog.GetLastLogIdx() + 1, 0
}
