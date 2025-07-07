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

// the Persistent state on all servers: currentTerm, votedFor
// the logs are managed by RaftLogImpl, which is a separate file
func (n *nodeImpl) getTermAndVoteForFileName() string {
	return "term_votefor.mk"
}

func (n *nodeImpl) getStateFilePath() string {
	dir := n.cfg.GetDataDir()
	return filepath.Join(dir, "term_votefor.mk")
}

func (n *nodeImpl) getTmpStateFilePath() string {
	dir := n.cfg.GetDataDir()
	formatted := time.Now().Format("20060102150405")
	numericTimestamp := formatted[:len(formatted)-4] + formatted[len(formatted)-3:]
	fileName := fmt.Sprintf("%s_%s", n.getTermAndVoteForFileName(), numericTimestamp)
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
	term, votedFor, err := n.deserializeTermAndVoteFor(string(content))
	if err != nil {
		return err
	}
	n.CurrentTerm = term
	n.VotedFor = votedFor
	return nil
}

// store to file system, shall be called when the term or votedFor changes
func (n *nodeImpl) storeCurrentTermAndVotedFor(term uint32, voteFor string, reEntrant bool) error {
	if !reEntrant {
		n.stateRWLock.Lock()
		defer n.stateRWLock.Unlock()
	}
	err := n.unsafePersistTermAndVoteFor(term, voteFor)
	if err != nil {
		return err
	}
	n.CurrentTerm = term
	return nil
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

func (n *nodeImpl) serializeTermAndVoteFor(term uint32, voteFor string) string {
	return fmt.Sprintf("#%d,%s#", term, voteFor)
}

func (n *nodeImpl) deserializeTermAndVoteFor(entry string) (uint32, string, error) {
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

func (n *nodeImpl) unsafePersistTermAndVoteFor(term uint32, voteFor string) error {
	path := n.getTmpStateFilePath()
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	entry := n.serializeTermAndVoteFor(term, voteFor)
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
