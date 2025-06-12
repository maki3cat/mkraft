package node

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

// the Persistent state on all servers: currentTerm, votedFor
// the logs are managed by RaftLogImpl, which is a separate file
func (n *nodeImpl) getStateFileName() string {
	return "state.rft"
}

func (n *nodeImpl) getStateFilePath() string {
	dir := n.cfg.GetDataDir()
	return filepath.Join(dir, "state.rft")
}

func (n *nodeImpl) getTmpStateFilePath() string {
	dir := n.cfg.GetDataDir()
	formatted := time.Now().Format("20060102150405.000")
	numericTimestamp := formatted[:len(formatted)-4] + formatted[len(formatted)-3:]
	fileName := fmt.Sprintf("%s_%s.tmp", n.getStateFileName(), numericTimestamp)
	return filepath.Join(dir, fileName)
}

// load from file system, shall be called at the beginning of the node
func (n *nodeImpl) loadCurrentTermAndVotedFor() error {
	n.stateRWLock.Lock()
	defer n.stateRWLock.Unlock()

	// if not exists, initialize to default values
	if _, err := os.Stat(n.getStateFileName()); os.IsNotExist(err) {
		n.logger.Info("raft state file does not exist, initializing to default values")
		n.CurrentTerm = 0
		n.VotedFor = ""
		return nil
	}

	file, err := os.Open(n.getStateFileName())
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

	var term uint32
	var voteFor string
	cnt, err := fmt.Fscanf(file, "%d,%s", &term, &voteFor)
	if err != nil {
		return fmt.Errorf("error reading raft state file: %w", err)
	}

	if cnt != 2 {
		return fmt.Errorf("expected 2 values in raft state file, got %d", cnt)
	}

	n.CurrentTerm = term
	n.VotedFor = voteFor

	n.logger.Debug("loadCurrentTermAndVotedFor",
		zap.String("fileName", n.getStateFileName()),
		zap.Int("bytesRead", cnt),
		zap.Uint32("term", n.CurrentTerm),
		zap.String("voteFor", n.VotedFor),
	)
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

func (n *nodeImpl) unsafePersistTermAndVoteFor(term uint32, voteFor string) error {
	path := n.getTmpStateFilePath()
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	_, err = file.WriteString(fmt.Sprintf("%d,%s", term, voteFor))
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
