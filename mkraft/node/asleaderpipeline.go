package node

import (
	"context"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/mkraft/utils"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

var (
	signalBuffer           = 1000 // if the buffer is added, the consumer should drain the channel at each iteration
	appendEntriesBatchSize = 5000
)

// --------------- pipeline ---------------
func (n *nodeImpl) runLogReplicaitonPipeline(ctx context.Context) {
	wg := sync.WaitGroup{}

	// stage 1: append logs
	stage1OutChan := make(chan struct{}, 1)
	wg.Add(1)
	go n.stageAppendLocalLogs(ctx, stage1OutChan, &wg)

	// stage 2: send append entries/heartbeat
	// though the stage 3 is a short one, stage2 have K-goroutins sharingthe outChan
	stage2OutChan := make(chan struct{}, signalBuffer)
	wg.Add(1)
	go n.stage2AppendEntries(ctx, stage1OutChan, stage2OutChan, &wg)

	stage3OutChan := make(chan struct{}, signalBuffer)
	wg.Add(1)
	go n.stage3UpdateCommitIdx(ctx, stage2OutChan, stage3OutChan, &wg)

	stage4OutChan := make(chan struct{}, signalBuffer)
	wg.Add(1)
	go n.stage4ApplyLogsAndRespond(ctx, stage3OutChan, stage4OutChan, &wg)

	wg.Wait()
}

// ------------------- stage 1: append logs -------------------
// stage 1: append logs
// in-channel: client commands
// out-channel: send append entries trigger
func (n *nodeImpl) stageAppendLocalLogs(ctx context.Context, outChan chan<- struct{}, wg *sync.WaitGroup) {
	name := "leader-pipe-s1: appendLocalLogs"
	n.logger.Info("starting: ", zap.String("step", name))
	tickerForHeartbeat := time.NewTicker(n.cfg.GetLeaderHeartbeatPeriod())
	defer tickerForHeartbeat.Stop()
	defer wg.Done()
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

// --------------- stage2: append entries to peers ---------------
// stage 2: send append entries to the peers, help peers to catch up with the leader, update nextIdx and matchIdx
func (n *nodeImpl) stage2AppendEntries(
	ctx context.Context, inChan <-chan struct{}, outChan chan<- struct{}, wg *sync.WaitGroup) {
	broadcast := func(ctx context.Context, inTriggerChan <-chan struct{}, peerChans []chan struct{}) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				select {
				case <-ctx.Done():
					return
				case <-inTriggerChan:
					n.logger.Debug("append entries triggered")
					common.BroadcastNonBlocking(peerChans)
				}
			}
		}
	}
	peerWg := sync.WaitGroup{}
	peerIDs, err := n.membership.GetAllPeerNodeIDs()
	if err != nil {
		panic(err)
	}
	peerTriggerChans := make([]chan struct{}, len(peerIDs))
	for idx, nodeID := range peerIDs {
		peerTriggerChans[idx] = make(chan struct{}, 1)
		peerWg.Add(1)
		go n.appendEntriesPeerWorker(ctx, nodeID, &peerWg, peerTriggerChans[idx], outChan)
	}
	go broadcast(ctx, inChan, peerTriggerChans)
	wg.Wait()
}

func (n *nodeImpl) appendEntriesPeerWorker(
	ctx context.Context,
	nodeID string, wg *sync.WaitGroup,
	inChan <-chan struct{}, outChan chan<- struct{}) {
	n.logger.Debug("appendEntriesPeerWorker starts", zap.String("nodeID", nodeID))
	defer wg.Done()
	defer n.logger.Debug("appendEntriesPeerWorker exits", zap.String("nodeID", nodeID))
	for {
		select {
		case <-ctx.Done():
			return
		default:
			select {
			case <-ctx.Done():
				return
			case <-inChan:
				common.DrainChannel(inChan)
				// this can be a long blocking call, but the ctx.Done() will be checked in the loop
				// then it will return and this goroutine will be exited on context done as well
				updated := n.sendAppendEntriesToPeerLoop(ctx, nodeID)
				if updated {
					common.NonBlockingSend(outChan)
				}
			}
		}
	}
}

// this should be called synchronously
// send a batch of logs the peer, if the peer lags behind in logs, the loop will continue to help it catch up
// if the peer is up to date, the loop will terminate
func (n *nodeImpl) sendAppendEntriesToPeerLoop(ctx context.Context, nodeID string) bool {
	n.logger.Debug("sendAppendEntriesToPeerLoop enters", zap.String("nodeID", nodeID))
	defer n.logger.Debug("sendAppendEntriesToPeerLoop exits", zap.String("nodeID", nodeID))
	respChan := make(chan *rpc.AppendEntriesResponse, 1)
	errChan := make(chan error, 1)
	callPeer := func() {
		n.logger.Debug("sendAppendEntriesToPeer", zap.String("nodeID", nodeID))
		resp, err := n.sendAppendEntriesToPeer(ctx, nodeID)
		if err != nil {
			n.logger.Error("send append entries to peer returns error", zap.String("nodeID", nodeID), zap.Error(err))
			errChan <- err
			return
		}
		respChan <- resp
	}
	for {
		callPeer()
		select {
		case <-ctx.Done():
			return false
		case <-errChan:
			return false
		case resp := <-respChan:
			if resp == nil { // no logs to send
				n.logger.Error("send append entries to peer returns nil", zap.String("nodeID", nodeID))
				return false
			}
			if resp.Success {
				return true
			} else {
				n.logger.Error("log mismatch, decrement nextIdx and retry", zap.String("nodeID", nodeID))
				continue
			}
		}
	}
}

// basic unit: get the nextIdx and a batch of raft logs, send append entries to the peer
// wait for the response from the peer
// if the response is success, update the nextIdx and matchIdx
// if the response is not success, decrement the nextIdx and retry
func (n *nodeImpl) sendAppendEntriesToPeer(
	ctx context.Context, nodeID string) (resp *rpc.AppendEntriesResponse, err error) {
	client, err := n.membership.GetPeerClient(nodeID)
	if err != nil {
		return nil, err
	}
	nextIdx := n.getPeersNextIndex(nodeID)
	logs, err := n.raftLog.GetLogs(nextIdx, appendEntriesBatchSize)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		n.logger.Debug("no logs appended, only heartbeat", zap.String("nodeID", nodeID), zap.Uint64("nextIdx", nextIdx))
	}
	prevLogIndex := nextIdx - 1
	prevLogTerm, err := n.raftLog.GetTermByIndex(prevLogIndex)
	if err != nil {
		return nil, err
	}
	currentTerm, _, _ := n.getKeyState()
	entries := make([]*rpc.LogEntry, len(logs))
	for i, log := range logs {
		entries[i] = &rpc.LogEntry{
			Data: log.Commands,
		}
	}
	req := &rpc.AppendEntriesRequest{
		Term:         currentTerm,
		LeaderId:     n.NodeId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}
	resp, err = client.AppendEntries(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Success {
		n.setPeerIndex(nodeID, nextIdx+uint64(len(logs)), nextIdx+uint64(len(logs))-1)
	} else {
		if resp.Term > currentTerm {
			// the peer is in a different term, so we need to degrade to follower
			// this will stop the whole pipeline
			n.logger.Warn("peer is in a different term, degrading to follower", zap.String("nodeID", nodeID), zap.Uint32("term", resp.Term))
			n.leaderDegradeCh <- DegradeSignal{
				ShallDegrade: true,
				VotedFor:     n.NodeId,
				ToTerm:       resp.Term,
			}
			return nil, common.ErrStateChangeFromLeaderToFollower
		} else {
			n.setPeerNextIndex(nodeID, nextIdx-uint64(1))
		}
	}
	return resp, nil
}

// step3: after matchIdx is updated, update commit idx
func (n *nodeImpl) stage3UpdateCommitIdx(ctx context.Context, inChan <-chan struct{}, outChan chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			select {
			case <-ctx.Done():
				return
			case <-inChan:
				common.DrainChannel(inChan)
				n.logger.Info("update commit idx")
				updated := n.UpdateCommit()
				if updated {
					common.NonBlockingSend(outChan)
				}
			}
		}
	}
}

// step4: with new commit idx, apply the logs to the state machine
func (n *nodeImpl) stage4ApplyLogsAndRespond(ctx context.Context, inChan <-chan struct{}, outChan chan<- struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			select {
			case <-ctx.Done():
				return
			case <-inChan:
				common.DrainChannel(inChan)
				n.logger.Info("mocking apply logs and respond")
			}
		}
	}
}
