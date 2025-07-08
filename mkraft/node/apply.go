package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// Implementation gap: Raft log application behavior across different node roles and transitions
// Implementation gap: is complicated and not well documented by the paper; thus, I dedicate a file to this.

// ---------------------------------------As a Leader: -----------------------------------------------------
// Case-1: AS A Leader, newly elected
// When a follower becomes the leader,
// it needs to apply previous committed logs (which doesn't require replies to the clients)
// to the state machine before it can reply to new client commands

// Case-2: AS A Leader, while serving
// When a leader stays as the leader,
// it needs to apply new committed logs to the state machine
// and reply to the clients

// For this worker, it is actually a variate pipeline pattern:
// first step is log replication, second step is apply to the state machine
// and this is the 2nd step in the pipeline of log (1-replication, 2-apply)
// shall NOT reset the chan inside this function because the main thread may write to it simultaneously
func (n *nodeImpl) leaderWorkerForLogApplication(ctx context.Context) {
	// Case-1: apply lagged commited logs
	err := n.applyAllLaggedCommitedLogs(ctx)
	if err != nil {
		n.logger.Error("failed to apply all lagged commited logs", zap.Error(err))
		panic(err)
	}
	// Case-2: apply new committed logs
	for {
		select {
		case <-ctx.Done():
			return
		case clientCommand := <-n.leaderApplyCh:
			result, err := n.statemachine.ApplyCommand(ctx, clientCommand.Req.Command)
			if err != nil {
				n.logger.Error("failed to apply command", zap.Error(err))
				panic(err) // todo: shall not panic, shall handle the error properly
			}
			clientCommand.RespChan <- &utils.RPCRespWrapper[*rpc.ClientCommandResponse]{
				Resp: &rpc.ClientCommandResponse{
					Result: result,
				},
				Err: nil,
			}
		}
	}
}

// Case-3: AS A Leader, but stale
// implementation gap:
// it can give up serving clients, just drain/return no leader leaderApplyCh, and leave the work to follower
func (n *nodeImpl) cleanupApplyLogsBeforeToFollower() {
	utils.DrainChannel(n.leaderApplyCh, n.cfg.GetRaftNodeRequestBufferSize())
}

// ---------------------------------------As a Follower/Candidate: -------------------------------------
// Case-4: AS A Follower/Candidate
// When a follower/candidate commits a new log
// no-leader-WORKER-1
func (n *nodeImpl) noleaderWorkerToApplyLogs(ctx context.Context, workerWaitGroup *sync.WaitGroup) {
	defer workerWaitGroup.Done()

	tickerTriggered := time.NewTicker(time.Duration(100 * time.Microsecond)) // empirical number
	defer tickerTriggered.Stop()

	for {
		select {
		case <-ctx.Done():
			n.logger.Info("apply-worker, exiting leader's worker for applying commands")
			return
		default:
			select {
			case <-tickerTriggered.C:
				err := n.applyAllLaggedCommitedLogs(ctx)
				if err != nil {
					n.logger.Error("failed to apply all lagged commited logs", zap.Error(err))
				}
			case <-n.noleaderApplySignalCh:
				utils.DrainChannel(n.noleaderApplySignalCh, n.cfg.GetRaftNodeRequestBufferSize())
				err := n.applyAllLaggedCommitedLogs(ctx)
				if err != nil {
					n.logger.Error("failed to apply all lagged commited logs", zap.Error(err))
				}
			case <-ctx.Done():
				n.logger.Info("apply-worker, exiting leader's worker for applying commands")
				return
			}
		}
	}
}

// Case-5: As a candidate, and then it becomes a leader
// it doesn't needs to apply the logs to the state machine which may be slow and the new leader need to start heartbeat soon
// so it just drains the noleaderApplySignalCh
func (n *nodeImpl) cleanupApplyLogsBeforeToLeader() {
	utils.DrainChannel(n.noleaderApplySignalCh, n.cfg.GetRaftNodeRequestBufferSize())
}

// ------------------- BASIC OPERATIONS --------------------------------
// apply the committed yet not applied logs to the state machine
func (n *nodeImpl) applyAllLaggedCommitedLogs(ctx context.Context) error {
	if len(n.leaderApplyCh) > 0 {
		n.logger.Error("leaderApplyCh is not empty, this should not happen")
		// maki: panic seems to be a bad idea, but this is a fatal error
		// todo: don't know other ways to handle this
		panic("leaderApplyCh is not empty, critial bug")
	}

	commitIdx, lastApplied := n.getCommitIdxAndLastApplied()
	if commitIdx > lastApplied {
		logs, err := n.raftLog.ReadLogsInBatchFromIdx(lastApplied + 1)
		if err != nil {
			n.logger.Error("failed to get logs from index", zap.Error(err))
			return err
		}
		cmds := make([][]byte, len(logs))
		for i, cmd := range logs {
			cmds[i] = cmd.Commands
		}
		_, err = n.statemachine.BatchApplyCommand(ctx, cmds)
		if err != nil {
			n.logger.Error("failed to apply command to state machine", zap.Error(err))
			return err
		}
		err = n.incrementLastApplied(uint64(len(logs)))
		if err != nil {
			if errors.Is(err, common.ErrInvariantsBroken) {
				panic(err) // fatal error
			}
			return err
		}
	}
	return nil
}
