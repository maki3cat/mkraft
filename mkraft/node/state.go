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
	MetaStateFileName = "metastate.mk"
)

// this is called when the node is a candidate and receives enough votes
// vote for and term are the same as the candidate's
func (n *nodeImpl) ToLeader() error {
	n.logger.Debug("STATE CHANGE: to leader enters")
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()
	n.state = StateLeader
	n.tracer.add(n.CurrentTerm, n.NodeId, n.state, n.VotedFor)
	n.logger.Debug("STATE CHANGE: to leader exits")
	return nil
}

func (n *nodeImpl) ToCandidate(reEntrant bool) error {
	n.logger.Debug("STATE CHANGE: to candidate", zap.Bool("reEntrant", reEntrant))
	if !reEntrant {
		n.stateRWLock.Lock()
		defer n.stateRWLock.Unlock()
	}

	prevTerm := n.CurrentTerm
	term := n.CurrentTerm + 1
	voteFor := n.NodeId

	err := n.unsafePersistTermAndVoteFor(term, voteFor)
	if err != nil {
		panic(err)
	}

	n.state = StateCandidate
	n.CurrentTerm = term
	n.VotedFor = voteFor
	n.tracer.add(n.CurrentTerm, n.NodeId, n.state, n.VotedFor)
	n.logger.Debug("STATE CHANGE: to candidate exits", zap.Uint32("prevTerm", prevTerm), zap.Uint32("newTerm", term), zap.String("voteFor", n.NodeId))
	return nil
}

func (n *nodeImpl) ToFollower(voteFor string, newTerm uint32, reEntrant bool) error {
	n.logger.Debug("STATE CHANGE: to follower enters.", zap.String("voteFor", voteFor), zap.Uint32("newTerm", newTerm), zap.Bool("reEntrant", reEntrant))
	if !reEntrant {
		n.stateRWLock.Lock()
		defer n.stateRWLock.Unlock()
	}
	err := n.unsafePersistTermAndVoteFor(newTerm, voteFor)
	if err != nil {
		panic(err)
	}
	n.CurrentTerm = newTerm
	n.VotedFor = voteFor
	n.state = StateFollower
	n.tracer.add(n.CurrentTerm, n.NodeId, n.state, n.VotedFor)
	n.logger.Debug("STATE CHANGE: to follower exits")
	return nil
}

func serializeKeyState(term uint32, voteFor string) string {
	return fmt.Sprintf("#%d,%s#", term, voteFor)
}

func deserializeKeyState(entry string) (uint32, string, error) {
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
	return filepath.Join(dir, MetaStateFileName)
}

func (n *nodeImpl) getTmpStateFilePath() string {
	dir := n.cfg.GetDataDir()
	formatted := time.Now().Format("20060102150405")
	numericTimestamp := formatted[:len(formatted)-4] + formatted[len(formatted)-3:]
	fileName := fmt.Sprintf("%s_%s", MetaStateFileName, numericTimestamp)
	return filepath.Join(dir, fileName)
}

// load from file system, shall be called at the beginning of the node
// but we don't need to load the state from the file system
func (n *nodeImpl) LoadMetaState() error {
	n.logger.Debug("STATE CHANGE: load meta state enters")
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
	term, votedFor, err := deserializeKeyState(string(content))
	if err != nil {
		return err
	}
	n.CurrentTerm = term
	n.VotedFor = votedFor
	n.logger.Debug("STATE CHANGE: load meta state exits")
	return nil
}

func (n *nodeImpl) unsafePersistTermAndVoteFor(term uint32, voteFor string) error {
	n.logger.Debug("STATE CHANGE: persist term and vote for enters")
	path := n.getTmpStateFilePath()
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	entry := serializeKeyState(term, voteFor)
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
	n.logger.Debug("STATE CHANGE: persist term and vote for exits")
	return nil
}

func (n *nodeImpl) getKeyState() (uint32, NodeState, string) {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.CurrentTerm, n.state, n.VotedFor
}

func (n *nodeImpl) getNodeState() NodeState {
	n.stateRWLock.RLock()
	defer n.stateRWLock.RUnlock()
	return n.state
}
