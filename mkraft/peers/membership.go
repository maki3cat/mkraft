package peers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

var _ Membership = (*staticMembership)(nil)

func NewMembershipWithStaticConfig(logger *zap.Logger, cfg *common.Config) (Membership, error) {
	membership := cfg.GetMembership()
	if len(membership.AllMembers) < 3 {
		return nil, errors.New("smallest cluster size is 3")
	}
	if len(membership.AllMembers)%2 == 0 {
		return nil, errors.New("the member count should be odd")
	}
	for _, node := range membership.AllMembers {
		if node.NodeID == "" || node.NodeURI == "" {
			return nil, errors.New("node id and uri should not be empty")
		}
	}
	// init
	logger.Info("Initializing static membership manager")
	staticMembership := &staticMembership{
		clients: &sync.Map{},
		// conns:         &sync.Map{},
		peerAddrs:     make(map[string]string),
		peerInitLocks: make(map[string]*sync.Mutex),
		logger:        logger,
		cfg:           cfg,
	}
	for _, node := range membership.AllMembers {
		staticMembership.peerAddrs[node.NodeID] = node.NodeURI
		staticMembership.peerInitLocks[node.NodeID] = &sync.Mutex{}
	}
	return staticMembership, nil
}

// What is the functions of the membership manager
// invariants total > peersCount
// maki should make sure this is guaranteed somewhere else
type Membership interface {
	// member count, and peers may diverge if the membership is dynamic
	GetTotalMemberCount() int // the cluster size, not just the active ones
	GetAllPeerClients() ([]PeerClient, error)
	GetAllPeerNodeIDs() ([]string, error)
	GetPeerClient(nodeID string) (PeerClient, error)

	// start the membership manager
	Start(ctx context.Context)
	GracefulStop()
}

type staticMembership struct {
	peerAddrs     map[string]string
	peerInitLocks map[string]*sync.Mutex
	clients       *sync.Map
	// conns         *sync.Map
	logger *zap.Logger
	cfg    *common.Config
}

func (mgr *staticMembership) Start(ctx context.Context) {
	_, err := mgr.getAllPeerClientsV2()
	if err != nil {
		mgr.logger.Error("failed to get initialize all peer clients at start time, will retry later", zap.Error(err))
	}
	go mgr.connCheckWorker(ctx)
}

func (mgr *staticMembership) GracefulStop() {
	mgr.logger.Info("graceful stop of membership manager")
	mgr.clients.Range(func(key, value any) bool {
		value.(PeerClient).Close()
		return true
	})
}

func (mgr *staticMembership) GetAllPeerNodeIDs() ([]string, error) {
	membership := mgr.cfg.GetMembership()
	peers := make([]string, 0)
	for _, nodeInfo := range membership.AllMembers {
		if nodeInfo.NodeID != membership.CurrentNodeID {
			peers = append(peers, nodeInfo.NodeID)
		}
	}
	if len(peers) == 0 {
		mgr.logger.Error("no peers found without errors")
		return nil, errors.New("no peers found without errors")
	}
	return peers, nil
}

func (mgr *staticMembership) GetPeerClient(nodeID string) (PeerClient, error) {
	client, ok := mgr.clients.Load(nodeID)
	if ok {
		return client.(PeerClient), nil
	}

	mgr.peerInitLocks[nodeID].Lock()
	defer mgr.peerInitLocks[nodeID].Unlock()

	client, ok = mgr.clients.Load(nodeID)
	if ok {
		return client.(PeerClient), nil
	}

	addr := mgr.peerAddrs[nodeID]
	newClient, err := NewPeerClientImpl(nodeID, addr, mgr.logger, mgr.cfg)
	if err != nil {
		mgr.logger.Error("failed to create new client", zap.String("nodeID", nodeID), zap.Error(err))
		return nil, err
	}
	mgr.clients.Store(nodeID, newClient)
	return newClient, nil
}

func (mgr *staticMembership) GetTotalMemberCount() int {
	return mgr.cfg.GetClusterSize()
}

func (mgr *staticMembership) GetAllPeerClients() ([]PeerClient, error) {
	clientsMap, err := mgr.getAllPeerClientsV2()
	if err != nil {
		return nil, err
	}
	clients := make([]PeerClient, 0, len(clientsMap))
	for _, client := range clientsMap {
		clients = append(clients, client)
	}
	return clients, nil
}

func (mgr *staticMembership) getAllPeerClientsV2() (map[string]PeerClient, error) {
	membership := mgr.cfg.GetMembership()
	peers := make(map[string]PeerClient)
	for _, nodeInfo := range membership.AllMembers {
		if nodeInfo.NodeID != membership.CurrentNodeID {
			client, err := mgr.GetPeerClient(nodeInfo.NodeID)
			if err != nil {
				mgr.logger.Error("failed to create new client", zap.String("nodeID", membership.CurrentNodeID), zap.Error(err))
				continue
			}
			peers[nodeInfo.NodeID] = client
		}
	}
	if len(peers) == 0 {
		return peers, errors.New("no peers found without errors")
	}
	return peers, nil
}

func (mgr *staticMembership) refreshPeerClient(nodeID string) error {
	mgr.peerInitLocks[nodeID].Lock()
	defer mgr.peerInitLocks[nodeID].Unlock()
	// directlyrebuild the connection
	addr := mgr.peerAddrs[nodeID]
	newClient, err := NewPeerClientImpl(nodeID, addr, mgr.logger, mgr.cfg)
	if err != nil {
		mgr.logger.Error("failed to refresh new client", zap.String("nodeID", nodeID), zap.Error(err))
		return err
	}
	mgr.clients.Store(nodeID, newClient)
	return nil
}

func (mgr *staticMembership) connCheckWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mgr.logger.Info("conn check: exits because of context done")
			return
		case <-ticker.C:
			clients, err := mgr.getAllPeerClientsV2()
			if err != nil {
				mgr.logger.Error("conn check: failed to get all peer clients", zap.Error(err))
				continue
			}
			for nodeID, client := range clients {
				if !client.PeerConnCheck(ctx) {
					mgr.logger.Error("conn check: peer is unavailable, should be refreshed", zap.String("nodeID", nodeID), zap.Error(err))
					err = mgr.refreshPeerClient(nodeID)
					if err != nil {
						mgr.logger.Error("conn check: failed to refresh peer client, will retry later", zap.String("nodeID", nodeID), zap.Error(err))
						continue
					}
				} else {
					mgr.logger.Debug("conn check: peer is healthy", zap.String("nodeID", nodeID))
				}
			}
		}
	}
}
