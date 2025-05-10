package raft

import (
	"errors"
	"sync"

	"github.com/maki3cat/mkraft/common"
	"github.com/maki3cat/mkraft/rpc"
	"go.uber.org/zap"
)

// What is the functions of the membership manager
type MembershipMgrIface interface {
	// todo: GetMemberCount, GetAllPeerClients may diverge
	// todo: may need to be re-constructed when dynamic membership is added
	GetMemberCount() int // current in use or set up ? setup shall be in the conf ?
	GetAllPeerClients() ([]rpc.InternalClientIface, error)
	GracefulShutdown()
}

// using the a static
func NewStatisMembership(logger *zap.Logger, cfg common.ConfigIface) (MembershipMgrIface, error) {
	staticMembership := cfg.GetMembership()
	if len(staticMembership.AllMembers) < 3 {
		return nil, errors.New("smallest cluster size is 3")
	}
	if len(staticMembership.AllMembers)%2 == 0 {
		return nil, errors.New("the member count should be odd")
	}
	for _, node := range staticMembership.AllMembers {
		if node.NodeID == "" || node.NodeURI == "" {
			return nil, errors.New("node id and uri should not be empty")
		}
	}

	// init
	logger.Info("Initializing static membership manager")
	staticMembershipMgr := &StaticMembershipMgr{
		clients: &sync.Map{},
		// conns:         &sync.Map{},
		peerAddrs:     make(map[string]string),
		peerInitLocks: make(map[string]*sync.Mutex),
		logger:        logger,
		cfg:           cfg,
	}
	for _, node := range staticMembership.AllMembers {
		staticMembershipMgr.peerAddrs[node.NodeID] = node.NodeURI
		staticMembershipMgr.peerInitLocks[node.NodeID] = &sync.Mutex{}
	}
	return staticMembershipMgr, nil
}

type StaticMembershipMgr struct {
	peerAddrs     map[string]string
	peerInitLocks map[string]*sync.Mutex
	clients       *sync.Map
	// conns         *sync.Map
	logger *zap.Logger
	cfg    common.ConfigIface
}

func (mgr *StaticMembershipMgr) GracefulShutdown() {
	mgr.logger.Info("graceful shutdown of membership manager")
	mgr.clients.Range(func(key, value interface{}) bool {
		value.(rpc.InternalClientIface).Close()
		return true
	})
}

func (mgr *StaticMembershipMgr) getPeerClient(nodeID string) (rpc.InternalClientIface, error) {
	client, ok := mgr.clients.Load(nodeID)
	if ok {
		return client.(rpc.InternalClientIface), nil
	}

	mgr.peerInitLocks[nodeID].Lock()
	defer mgr.peerInitLocks[nodeID].Unlock()

	client, ok = mgr.clients.Load(nodeID)
	if ok {
		return client.(rpc.InternalClientIface), nil
	}

	addr := mgr.peerAddrs[nodeID]
	newClient, err := rpc.NewInternalClient(nodeID, addr, mgr.logger, mgr.cfg)
	if err != nil {
		mgr.logger.Error("failed to create new client", zap.String("nodeID", nodeID), zap.Error(err))
		return nil, err
	}
	mgr.clients.Store(nodeID, newClient)
	return newClient, nil
}

func (mgr *StaticMembershipMgr) GetMemberCount() int {
	return mgr.cfg.GetClusterSize()
}

func (mgr *StaticMembershipMgr) GetAllPeerClients() ([]rpc.InternalClientIface, error) {
	membership := mgr.cfg.GetMembership()
	peers := make([]rpc.InternalClientIface, 0)
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
