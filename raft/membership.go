package raft

import (
	"sync"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
)

var (
	memberMgr MembershipMgrIface
	once      sync.Once
)

func InitGlobalMembershipManager(staticMembership *Membership) {
	once.Do(func() {
		util.GetSugarLogger().Info("Initializing static membership manager")
		staticMembershipMgr := &StaticMembershipMgr{
			membership:    staticMembership,
			connections:   &sync.Map{},
			peerAddrs:     make(map[string]string),
			peerInitLocks: make(map[string]*sync.Mutex),
		}
		for _, node := range staticMembership.AllMembers {
			staticMembershipMgr.peerAddrs[node.NodeID] = node.NodeURI
			staticMembershipMgr.peerInitLocks[node.NodeID] = &sync.Mutex{}
		}
		memberMgr = staticMembershipMgr
	})
}

type MembershipMgrIface interface {
	GetCurrentNodeID() string
	GetPeerClient(nodeID string) (rpc.InternalClientIface, error)
	// if the memebrship is dynamic, the count and peer change and may not be consistent
	GetMemberCount() int
	GetAllPeerClients() ([]rpc.InternalClientIface, error)
	Warmup()
}

type Membership struct {
	CurrentNodeID   string     `json:"current_node_id" yaml:"current_node_id"`
	CurrentPort     int        `json:"current_port" yaml:"current_port"`
	CurrentNodeAddr string     `json:"current_node_addr" yaml:"current_node_addr"`
	AllMembers      []NodeAddr `json:"all_members" yaml:"all_members"`
}

type NodeAddr struct {
	NodeID  string `json:"node_id"`
	NodeURI string `json:"node_uri"`
}

type StaticMembershipMgr struct {
	membership *Membership

	peerAddrs     map[string]string
	peerInitLocks map[string]*sync.Mutex
	connections   *sync.Map
}

func (mgr *StaticMembershipMgr) GetCurrentNodeID() string {
	return mgr.membership.CurrentNodeID
}

func (mgr *StaticMembershipMgr) Warmup() {
	_, err := mgr.GetAllPeerClients()
	if err != nil {
		util.GetSugarLogger().Errorw("failed to warmup peer clients", "error", err)
	} else {
		util.GetSugarLogger().Info("warmup peer clients done")

	}
}

func (mgr *StaticMembershipMgr) GetPeerClient(nodeID string) (rpc.InternalClientIface, error) {
	client, ok := mgr.connections.Load(nodeID)
	if ok {
		return client.(rpc.InternalClientIface), nil
	}

	mgr.peerInitLocks[nodeID].Lock()
	defer mgr.peerInitLocks[nodeID].Unlock()

	client, ok = mgr.connections.Load(nodeID)
	if ok {
		return client.(rpc.InternalClientIface), nil
	}

	conn, err := grpc.NewClient(mgr.peerAddrs[nodeID])
	if err != nil {
		util.GetSugarLogger().Errorw("failed to connect to server", "nodeID", nodeID, "error", err)
		return nil, err
	}
	newClient := rpc.NewInternalClient(rpc.NewRaftServiceClient(conn))
	return newClient, nil
}

func (mgr *StaticMembershipMgr) GetMemberCount() int {
	return len(mgr.membership.AllMembers)
}

// synchronous, can pre-warm
func (mgr *StaticMembershipMgr) GetAllPeerClients() ([]rpc.InternalClientIface, error) {
	peers := make([]rpc.InternalClientIface, 0)
	for _, nodeInfo := range mgr.membership.AllMembers {
		if nodeInfo.NodeID == mgr.membership.CurrentNodeID {
			// self
			continue
		}
		client, err := mgr.GetPeerClient(nodeInfo.NodeID)
		if err != nil {
			util.GetSugarLogger().Errorw("failed to get peer client", "nodeID", nodeInfo.NodeID, "error", err)
			return nil, err
		}
		peers = append(peers, client)
	}
	return peers, nil
}
