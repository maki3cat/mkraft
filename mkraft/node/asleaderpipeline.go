package node

import (
	"context"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"go.uber.org/zap"
)

var (
	pipelineBuffer = 100
)

func (n *nodeImpl) startLogReplicaitonPipeline(ctx context.Context) {

	// stage 1: append logs
	stage1OutChan := make(chan struct{}, 1)
	go n.stageAppendLocalLogs(ctx, stage1OutChan)


	// stage 2: send append entries/heartbeat

	appendEntriesTriggerChan := n.stageSendAppendEntriesToPeers(ctx, stage1OutChan)
	consensusLogsTriggerChan := n.sendAppendEntriesToPeers(ctx, appendEntriesTriggerChan)
	commitIdxTriggerChan := n.updateCommitIdx(ctx, consensusLogsTriggerChan)
	n.applyLogsAndRespond(ctx, commitIdxTriggerChan)
}

// stage 1: append logs
// in-channel: client commands
// out-channel: send append entries trigger
func (n *nodeImpl) stageAppendLocalLogs(ctx context.Context, outChan chan<- struct{}) {
	name := "leader-pipe-s1: appendLocalLogs"
	n.logger.Info("starting: ", zap.String("step", name))
	tickerForHeartbeat := time.NewTicker(n.cfg.GetLeaderHeartbeatPeriod())
	defer tickerForHeartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			n.logger.Warn("context done, exiting", zap.String("step", name))
			return
		default:
			select {
			case <-ctx.Done():
				n.logger.Warn("context done, exiting", zap.String("step", name))
				return
			case <-tickerForHeartbeat.C:
				n.logger.Debug("heartbeat triggered", zap.String("step", name))
				common.NonBlockingSend(outChan)
			case clientCmd := <-n.clientCommandCh:
				n.logger.Debug("client commands triggered", zap.String("step", name))

				batchingSize := n.cfg.GetRaftNodeRequestBufferSize()
				clientCommands := utils.ReadMultipleFromChannel(n.clientCommandCh, batchingSize-1)
				clientCommands = append(clientCommands, clientCmd)

				// leader only append logs
				currentTerm, _, _ := n.getKeyState()
				commands := make([][]byte, len(clientCommands))
				for i, clientCommand := range clientCommands {
					commands[i] = clientCommand.Req.Command
				}
				err := n.raftLog.AppendLogsInBatch(ctx, commands, currentTerm)
				if err != nil {
					n.logger.Error("append logs in batch failed", zap.String("step", name), zap.Error(err))
				}

				// non-blocking trigger the next stage
				tickerForHeartbeat.Reset(n.cfg.GetLeaderHeartbeatPeriod())
				common.NonBlockingSend(outChan)
			}
		}
	}
}

// step5: with new commit idx, apply the logs to the state machine
func (n *nodeImpl) applyLogsAndRespond(ctx context.Context, inChan <-chan struct{}) {
	go func(ctx context.Context) {
		name := "leader-pipeline-stage4: applyLogsAndRespond"
		n.logger.Info("starting: ", zap.String("step", name))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-inChan:
					// todo:
					// apply the logs to the state machine, update the last applied idx
				}
			}
		}
	}(ctx)
}

// step3: after matchIdx is updated, update commit idx
func (n *nodeImpl) updateCommitIdx(ctx context.Context, triggerChan <-chan struct{}) <-chan struct{} {
	name := "leader-pipeline-stage3: updateCommitIdx"
	outChan := make(chan struct{}, pipelineBuffer)
	go func(ctx context.Context) {
		n.logger.Info("starting: ", zap.String("step", name))
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-triggerChan:
					common.DrainChannel(outChan)
					n.logger.Info("update commit idx")
					// todo
				}
			}
		}
	}(ctx)
	return outChan
}

// stage 2: send append entries to the peers, help peers to catch up with the leader, update nextIdx and matchIdx
func (n *nodeImpl) sendAppendEntriesToPeers(ctx context.Context, triggerChan <-chan struct{}) <-chan struct{} {
	name := "leader-pipeline-stage2: sendAppendEntriesToPeers"
	outChan := make(chan struct{}, pipelineBuffer) // todo: seems here it needs a buffered channel

	// start each goroutine for each peer to catch up with the leader
	peerWorker := func(ctx context.Context, peerID string, inChan <-chan struct{}, outChan chan struct{}) {
		n.logger.Debug("starting: ", zap.String("step", name), zap.String("peer", peerID))
		defer n.logger.Debug("exiting: ", zap.String("step", name), zap.String("peer", peerID))
		// todo: implement the logic to send append entries to the peer
		select {
		case <-ctx.Done():
			return
		default:
			select {
			case <-ctx.Done():
				return
			case <-inChan:
				// for loop:
				// get nextIdx from the peer
				// send append entries to the peer
				// receive response from the peer
				// update nextIdx and matchIdx
				// if nextIdx needs to rewind back, rewind and continue the for loop
				// if nextIdx is updated, send append entries to the peer
			}
		}
	}

	// triggerWorker
	triggerWorker := func(ctx context.Context, inTriggerChan <-chan struct{}, peerChans []chan struct{}) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-inTriggerChan:
					common.BroadcastNonBlocking(peerChans)
				}
			}
		}
	}

	peerIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		panic(err)
	}
	peerTriggerChans := make([]chan struct{}, len(peerIDs))
	for idx, peerID := range peerIDs {
		peerTriggerChans[idx] = make(chan struct{}, 1)
		go peerWorker(ctx, peerID, peerTriggerChans[idx], outChan)
	}
	go triggerWorker(ctx, triggerChan, peerTriggerChans)

	return outChan
}
