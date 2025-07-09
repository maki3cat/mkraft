package node

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

func NewStateTrace(logger *zap.Logger, dataDir string) *stateTrace {
	trace := &stateTrace{
		logger:        logger,
		dataDir:       dataDir,
		stateFilePath: filepath.Join(dataDir, "state_history.mk"),
		queue:         make(chan string, 100),
	}
	return trace
}

type stateTrace struct {
	logger        *zap.Logger
	dataDir       string
	stateFilePath string
	queue         chan string
}

func (s *stateTrace) start(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	default:
		select {
		case <-ctx.Done():
			return
		case entry := <-s.queue:
			entries := make([]string, 0, 100)
			entries = append(entries, entry)
		DRAIN:
			for {
				select {
				case entry := <-s.queue:
					// take all
					entries = append(entries, entry)
				default:
					break DRAIN
				}
			}
			s.save(entries)
			s.logger.Info("saved state trace", zap.Int("count", len(entries)))
		}
	}
}

func (s *stateTrace) add(currentTerm uint32, nodeId string, state NodeState, voteFor string) {
	entry := s.serialize(currentTerm, nodeId, state, voteFor)
	s.queue <- entry
}

func (s *stateTrace) serialize(term uint32, nodeId string, state NodeState, voteFor string) string {
	currentTime := time.Now().Format(time.RFC3339)
	return fmt.Sprintf("#%s, term %d, nodeID %s, state %s, vote for %s#\n", currentTime, term, nodeId, state, voteFor)
}

func (s *stateTrace) save(lines []string) {
	file, err := os.OpenFile(s.stateFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		s.logger.Error("open recordNodeState file", zap.Error(err))
		return
	}
	defer file.Close()

	for _, line := range lines {
		if _, err := file.WriteString(line); err != nil {
			s.logger.Error("write recordNodeState", zap.Error(err))
		}
	}

	if err := file.Sync(); err != nil {
		s.logger.Warn("fsync recordNodeState", zap.Error(err))
	}
	s.logger.Debug("exiting recordNodeState")
}
