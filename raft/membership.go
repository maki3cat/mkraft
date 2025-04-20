package raft

import (
	"errors"
	"sync"

	"github.com/maki3cat/mkraft/rpc"
	"github.com/maki3cat/mkraft/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	memberMgr MembershipMgrIface
	once      sync.Once
)

// EXPOSED APIS
func GetAllPeerClients() ([]rpc.InternalClientIface, error) {
	if memberMgr == nil {
		return nil, errors.New("membership manager is not initialized")
	}
	return memberMgr.GetAllPeerClients()
}

func GetPeerClient(nodeID string) (rpc.InternalClientIface, error) {
	if memberMgr == nil {
		return nil, errors.New("membership manager is not initialized")
	}
	return memberMgr.GetPeerClient(nodeID)
}

func GetCurrentNodeID() string {
	if memberMgr == nil {
		return ""
	}
	return memberMgr.GetCurrentNodeID()
}

func InitGlobalMembershipWithStaticConfig(staticMembership *Membership) {
	once.Do(func() {
		// check sanity
		if staticMembership.AllMembers == nil || len(staticMembership.AllMembers) == 0 {
			util.GetSugarLogger().Fatal("static membership is empty")
		}
		for _, node := range staticMembership.AllMembers {
			if node.NodeID == "" || node.NodeURI == "" {
				util.GetSugarLogger().Fatal("static membership is invalid")
			}
		}

		// init
		util.GetSugarLogger().Info("Initializing static membership manager")
		staticMembershipMgr := &StaticMembershipMgr{
			membership:    staticMembership,
			clients:       &sync.Map{},
			conns:         &sync.Map{},
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
	GracefulShutdown()
}

type Membership struct {
	CurrentNodeID   string     `json:"current_node_id" yaml:"current_node_id"`
	CurrentPort     int        `json:"current_port" yaml:"current_port"`
	CurrentNodeAddr string     `json:"current_node_addr" yaml:"current_node_addr"`
	AllMembers      []NodeAddr `json:"all_members" yaml:"all_members"`
}

type NodeAddr struct {
	NodeID  string `json:"node_id" yaml:"node_id"`
	NodeURI string `json:"node_uri" yaml:"node_uri"`
}

type StaticMembershipMgr struct {
	membership *Membership

	peerAddrs     map[string]string
	peerInitLocks map[string]*sync.Mutex
	clients       *sync.Map
	conns         *sync.Map
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

func (mgr *StaticMembershipMgr) GracefulShutdown() {
	logger := util.GetSugarLogger()
	logger.Info("graceful shutdown of membership manager")
	// close all connections
	for _, nodeInfo := range mgr.membership.AllMembers {
		if nodeInfo.NodeID == mgr.membership.CurrentNodeID {
			// self
			continue
		}
		conn, ok := mgr.conns.Load(nodeInfo.NodeID)
		if ok {
			logger.Infof("closing connection to %s", nodeInfo.NodeID)
			clientConn, _ := conn.(*grpc.ClientConn)
			clientConn.Close()
		}
	}
}

func (mgr *StaticMembershipMgr) GetPeerClient(nodeID string) (rpc.InternalClientIface, error) {
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

	// todo: insecure credentials now
	addr, _ := mgr.peerAddrs[nodeID]
	util.GetSugarLogger().Debugw("creating new connection to", "nodeID", nodeID, "peerAddr", addr)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.GetSugarLogger().Errorw("failed to connect to server", "nodeID", nodeID, "error", err)
	}
	mgr.conns.Store(nodeID, conn)

	rpcClient := rpc.NewRaftServiceClient(conn)
	newClient := rpc.NewInternalClient(rpcClient)
	mgr.clients.Store(nodeID, newClient)
	return newClient, nil
}

func (mgr *StaticMembershipMgr) GetMemberCount() int {
	return len(mgr.membership.AllMembers)
}

func (mgr *StaticMembershipMgr) GetAllPeerClients() ([]rpc.InternalClientIface, error) {
	peers := make([]rpc.InternalClientIface, 0)
	for _, nodeInfo := range mgr.membership.AllMembers {
		if nodeInfo.NodeID != mgr.membership.CurrentNodeID {
			client, err := mgr.GetPeerClient(nodeInfo.NodeID)
			// todo: here the get client doesn't stop the process
			if err != nil {
				util.GetSugarLogger().Errorw(
					"failed to get peer client", "nodeID", nodeInfo.NodeID, "error", err)
				continue
			}
			peers = append(peers, client)
		}
	}
	if len(peers) == 0 {
		return peers, errors.New("no peers found")
	}
	return peers, nil
}
