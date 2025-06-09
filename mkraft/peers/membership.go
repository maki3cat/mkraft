package peers

import (
	"errors"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"go.uber.org/zap"
)

var _ Membership = (*staticMembership)(nil)

func NewMembershipWithStaticConfig(logger *zap.Logger, cfg common.ConfigIface) (Membership, error) {
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
	// todo: GetMemberCount, GetAllPeerClients may diverge
	// todo: may need to be re-constructed when dynamic membership is added
	GetMemberCount() int // current in use or set up ? setup shall be in the conf ?
	GetAllPeerClients() ([]PeerClient, error)
	GetAllPeerClientsV2() (map[string]PeerClient, error)
	GetAllPeerNodeIDs() ([]string, error)
	GracefulStop()
}

type staticMembership struct {
	peerAddrs     map[string]string
	peerInitLocks map[string]*sync.Mutex
	clients       *sync.Map
	// conns         *sync.Map
	logger *zap.Logger
	cfg    common.ConfigIface
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

func (mgr *staticMembership) getPeerClient(nodeID string) (PeerClient, error) {
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

func (mgr *staticMembership) GetMemberCount() int {
	return mgr.cfg.GetClusterSize()
}

func (mgr *staticMembership) GetAllPeerClients() ([]PeerClient, error) {
	membership := mgr.cfg.GetMembership()
	peers := make([]PeerClient, 0)
	for _, nodeInfo := range membership.AllMembers {
		if nodeInfo.NodeID != membership.CurrentNodeID {
			client, err := mgr.getPeerClient(nodeInfo.NodeID)
			if err != nil {
				mgr.logger.Error("failed to create new client", zap.String("nodeID", membership.CurrentNodeID), zap.Error(err))
				continue
			}
			peers = append(peers, client)
		}
	}
	if len(peers) == 0 {
		return peers, errors.New("no peers found without errors")
	}
	return peers, nil
}

func (mgr *staticMembership) GetAllPeerClientsV2() (map[string]PeerClient, error) {
	membership := mgr.cfg.GetMembership()
	peers := make(map[string]PeerClient)
	for _, nodeInfo := range membership.AllMembers {
		if nodeInfo.NodeID != membership.CurrentNodeID {
			client, err := mgr.getPeerClient(nodeInfo.NodeID)
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
