package node

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

const (
	TermAndVoteForFileName = "keystate.mk"
)

// only serialize the term and voteFor for now
func serializeTermAndVoteFor(term uint32, voteFor string) string {
	return fmt.Sprintf("#%d,%s#", term, voteFor)
}

func deserializeTermAndVoteFor(entry string) (uint32, string, error) {
	// Remove leading/trailing #
	entry = strings.Trim(entry, "#")

	parts := strings.Split(entry, ",")
	if len(parts) != 2 {
		return 0, "", common.ErrCorruptPersistentFile
	}
	term64, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil {
		return 0, "", common.ErrCorruptPersistentFile
	}
	return uint32(term64), strings.TrimSpace(parts[1]), nil
}

func (n *nodeImpl) getStateFilePath() string {
	dir := n.cfg.GetDataDir()
	return filepath.Join(dir, TermAndVoteForFileName)
}

func (n *nodeImpl) getTmpStateFilePath() string {
	dir := n.cfg.GetDataDir()
	formatted := time.Now().Format("20060102150405")
	numericTimestamp := formatted[:len(formatted)-4] + formatted[len(formatted)-3:]
	fileName := fmt.Sprintf("%s_%s", TermAndVoteForFileName, numericTimestamp)
	return filepath.Join(dir, fileName)
}

// load from file system, shall be called at the beginning of the node
func (n *nodeImpl) loadCurrentTermAndVotedFor() error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	path := n.getStateFilePath()

	// if not exists, initialize to default values
	if _, err := os.Stat(path); os.IsNotExist(err) {
		n.logger.Info("raft state file does not exist, initializing to default values")
		n.CurrentTerm = 0
		n.VotedFor = ""
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			n.logger.Info("raft state file does not exist, initializing to default values")
			n.CurrentTerm = 0
			n.VotedFor = ""
			return nil
		}
		return err
	}
	defer file.Close()

	// can also be "1," which is valid for not voting for anyone
	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("error reading raft state file: %w", err)
	}
	term, votedFor, err := deserializeTermAndVoteFor(string(content))
	if err != nil {
		return err
	}
	n.CurrentTerm = term
	n.VotedFor = votedFor
	return nil
}

func (n *nodeImpl) unsafePersistTermAndVoteFor(term uint32, voteFor string) error {
	path := n.getTmpStateFilePath()
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	entry := serializeTermAndVoteFor(term, voteFor)
	_, err = file.WriteString(entry)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		n.logger.Error("error syncing file", zap.String("fileName", path), zap.Error(err))
		return err
	}
	err = file.Close()
	if err != nil {
		n.logger.Error("error closing file", zap.String("fileName", path), zap.Error(err))
		return err
	}

	err = os.Rename(path, n.getStateFilePath())
	if err != nil {
		return err
	}

	n.VotedFor = voteFor
	return nil
}

// normal read
func (n *nodeImpl) getCurrentTerm() uint32 {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.CurrentTerm
}

func (n *nodeImpl) getNodeState() NodeState {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.state
}

func (n *nodeImpl) getKeyState() (uint32, NodeState, string) {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.CurrentTerm, n.state, n.VotedFor
}

func (n *nodeImpl) updateCurrentTermAndVotedForAsCandidate(reEntrant bool) error {
	if !reEntrant {
		n.stateRWLock.Lock()
		defer n.stateRWLock.Unlock()
	}
	term := n.CurrentTerm + 1
	voteFor := n.NodeId
	err := n.unsafePersistTermAndVoteFor(term, voteFor)
	if err != nil {
		return err
	}
	n.CurrentTerm = term
	return nil
}

func (n *nodeImpl) setKeyState(term uint32, state NodeState, voteFor string) error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	n.CurrentTerm = term
	n.state = state
	n.VotedFor = voteFor
	err := n.unsafePersistTermAndVoteFor(term, voteFor)
	if err != nil {
		return err
	}
	// todo: record to be sent to a channel to be sent to perist them
	// in a batch
	n.recordNodeState(term, state, voteFor)
	return nil
}

func (n *nodeImpl) setNodeState(state NodeState) {
	n.logger.Debug("entering SetNodeState")
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	if n.state == state {
		return // no change
	}
	n.logger.Info("Node state changed",
		zap.String("nodeID", n.NodeId),
		zap.String("oldState", n.state.String()),
		zap.String("newState", state.String()))
	n.state = state
	n.logger.Debug("exiting SetNodeState")
}
