package node

import (
	"fmt"
	"os"
	"time"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

// implementation gap: the commitIdx and lastApplied shall be persisted in implementation
// maki: this gap is a tricky part, discuss with the prof
// if not, if all nodes shutdown, the commitIdx and lastApplied will be lost
func (n *nodeImpl) getIdxFileName() string {
	dataDir := n.cfg.GetDataDir()
	return fmt.Sprintf("%s/index.mk", dataDir)
}

func (n *nodeImpl) unsafeSaveIdx() error {
	dir := n.cfg.GetDataDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	idxFileName := fmt.Sprintf("%s/index_%s.mk", dir, time.Now().Format("20060102150405"))
	n.logger.Debug("saving index", zap.String("idxFileName", idxFileName))

	buf := make([]byte, 0, 32)
	buf = fmt.Appendf(buf, "%d,%d\n", n.commitIndex, n.lastApplied)
	err := os.WriteFile(idxFileName, buf, 0644)
	if err != nil {
		return err
	}
	// using rename to ensure atomicity of file writing
	return os.Rename(idxFileName, n.getIdxFileName())
}

func (n *nodeImpl) unsafeLoadIdx() error {
	if _, err := os.Stat(n.getIdxFileName()); os.IsNotExist(err) {
		// default values
		n.commitIndex = 0
		n.lastApplied = 0
		return nil
	}

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
func (n *nodeImpl) getCommitIdxAndLastApplied() (uint64, uint64) {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex, n.lastApplied
}

func (n *nodeImpl) getCommitIdx() uint64 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.commitIndex
}

// From Paper:
// • If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
// implementation gap:
// we currently wait for every appendEntries to reach consensus, and then update the commitIdx
// so we don't reply on matchIndex to update the commitIdx
// not sure this is a good idea, but this design makes the implementation SIMPLE
// todo: important
// so we can prove this 1) test cases; 2) comparison with Hashicorp Raft implementation
func (n *nodeImpl) incrementCommitIdx(numberOfCommand uint64) error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.commitIndex = n.commitIndex + numberOfCommand
	if err := n.unsafeCheckIndexIntegrity(); err != nil {
		return err
	}
	return n.unsafeSaveIdx()
}

func (n *nodeImpl) incrementLastApplied(numberOfCommand uint64) error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.lastApplied = n.lastApplied + numberOfCommand
	err := n.unsafeCheckIndexIntegrity()
	if err != nil {
		return err
	}
	return n.unsafeSaveIdx()
}

func (n *nodeImpl) unsafeCheckIndexIntegrity() error {
	if n.commitIndex < n.lastApplied {
		return common.ErrInvariantsBroken
	}
	return nil
}

// section2: for indices of leaders, nextIndex/matchIndex
// maki: Updating a follower's match/next index is independent of whether consensus is reached.
// Updating matchIndex/nextIndex is a per-follower operation.
// Reaching consensus (a majority of nodes having the same entry) is a cluster-wide operation.
func (n *nodeImpl) incrPeerIdxAfterLogRepli(nodeID string, logCnt uint64) {
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
	// directly equal the matchIndex to the nextIndex - 1,
	// so that it can be updated to a correct value even from 0 in the first place
	n.matchIndex[nodeID] = n.nextIndex[nodeID] - 1
}

// important invariant: matchIndex[follower] ≤ nextIndex[follower] - 1
// if the matchIndex is less than nextIndex-1, appendEntries will fail and the follower's nextIndex will be decremented
func (n *nodeImpl) decrPeerIdxAfterLogRepli(nodeID string) {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// todo: feature mentioned by the paper,
	// there can be improvement of efficiency here, refer to the paper page 8 first paragraph
	n.logger.Warn("fixing inconsistent logs for peer",
		zap.String("nodeID", nodeID))

	if n.nextIndex[nodeID] > 1 {
		n.nextIndex[nodeID] -= 1
	} else {
		n.logger.Error("next index is already at 1, cannot decrement", zap.String("nodeID", nodeID))
	}
}

func (n *nodeImpl) getPeersNextIndex(nodeID string) uint64 {
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
func (n *nodeImpl) getInitDefaultValuesForPeer() (uint64, uint64) {
	return n.raftLog.GetLastLogIdx() + 1, 0
}

func (n *nodeImpl) IncrPeerIdx(nodeID string, idx uint64) {
	n.incrPeerIdxAfterLogRepli(nodeID, idx)
}

func (n *nodeImpl) DecrPeerIdx(nodeID string) {
	n.decrPeerIdxAfterLogRepli(nodeID)
}
