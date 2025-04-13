package raft

import (
	"sync"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
)

var (
	memberMgr MembershipMgrIface
)

type Membership struct {
	CurrentNodeID   string     `json:"current_node_id"`
	CurrentPort     int        `json:"current_port"`
	CurrentNodeAddr string     `json:"current_node_addr"`
	AllMembers      []NodeAddr `json:"all_members"`
}

type NodeAddr struct {
	NodeID  string `json:"node_id"`
	NodeURI string `json:"node_uri"`
}

func InitMembershipWithStaticConfig(staticMembership *Membership) {
	staticMembershipMgr := &StaticMembershipMgr{
		membership: staticMembership,
		clients:    make(map[string]rpc.InternalClientIface),
		cliRWLock:  sync.RWMutex{},
		peerAddrs:  make(map[string]string),
	}
	for _, node := range staticMembership.AllMembers {
		staticMembershipMgr.peerAddrs[node.NodeID] = node.NodeURI
	}
	memberMgr = staticMembershipMgr
}

type MembershipMgrIface interface {
	GetClientWithLazyInit(nodeID string) rpc.InternalClientIface

	// if the memebrship is dynamic, the count and peer change and may not be consistent
	GetMemberCount() int
	GetAllPeers() chan rpc.InternalClientIface
}

// maki
// todo: right now we suppose membership list doesn't change after first set up
// they may be alive or dead, but the list is fixed
type StaticMembershipMgr struct {

	// todo: in the near future, we need update membership dynamically;
	// todo: in the remote future, we need to update the config dynamically
	membership *Membership
	peerAddrs  map[string]string

	// maki: here is a topic
	// todo: should be one connection to one client?
	// todo: what is the best pattern of maintaing busy connections and clients
	// todo: now these connections/clients are stored in map which is not thread safe
	// todo: need to check problem of concurrency here

	// maki: here is a topic for go gynastics
	// interface cannot use pointer
	cliRWLock sync.RWMutex
	clients   map[string]rpc.InternalClientIface
}

func (mgr *StaticMembershipMgr) initClient(nodeID string) {
	mgr.cliRWLock.Lock()
	defer mgr.cliRWLock.Lock()

	if _, ok := mgr.clients[nodeID]; ok {
		util.GetSugarLogger().Debugw("client already exists", "nodeID", nodeID)
		return
	}

	serverAddr := mgr.peerAddrs[nodeID]
	conn, err := grpc.NewClient(serverAddr)
	if err != nil {
		util.GetSugarLogger().Errorw("failed to connect to server", "nodeID", nodeID, "error", err)
	}
	client := rpc.NewRaftServiceClient(conn)
	internalClient := rpc.NewInternalClient(client)
	mgr.clients[nodeID] = internalClient
}

func (mgr *StaticMembershipMgr) getClient(nodeID string) rpc.InternalClientIface {
	mgr.cliRWLock.RLock()
	defer mgr.cliRWLock.RUnlock()
	client := mgr.clients[nodeID]
	return client
}

func (mgr *StaticMembershipMgr) GetClientWithLazyInit(nodeID string) rpc.InternalClientIface {
	client := mgr.getClient(nodeID)
	if client == nil {
		mgr.initClient(nodeID)
		client = mgr.getClient(nodeID)
	}
	return client
}

func (mgr *StaticMembershipMgr) GetMemberCount() int {
	return len(mgr.membership.AllMembers)
}

func (mgr *StaticMembershipMgr) GetAllPeers() chan rpc.InternalClientIface {
	peers := make(chan rpc.InternalClientIface)
	go func() {
		for _, nodeInfo := range mgr.membership.AllMembers {
			nodeID := nodeInfo.NodeID
			if nodeID == util.GetConfig().NodeID {
				continue
			}
			client := mgr.GetClientWithLazyInit(nodeID)
			peers <- client
		}
		close(peers)
	}()
	return peers
}
