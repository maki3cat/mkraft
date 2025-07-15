package main

// need a thin layer of client-sdk to work with server's redirection policy
// also need the configuration of the server address for the client

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/maki3cat/mkraft/rpc"
	"google.golang.org/grpc"
)

func main() {
	// Accept node addresses from command line as a comma-separated list
	addrsFlag := flag.String("addrs", "", "comma-separated list of node addresses (e.g. localhost:18081,localhost:18082)")
	flag.Parse()
	defaultAddrs := []string{
		"localhost:18081",
		"localhost:18082",
		"localhost:18083",
	}
	var nodeAddrs []string

	if *addrsFlag != "" {
		// using the default config file
		nodeAddrs = strings.Split(*addrsFlag, ",")
	} else {
		fmt.Println("using default config file")
		nodeAddrs = defaultAddrs
	}

	for i := range nodeAddrs {
		nodeAddrs[i] = strings.TrimSpace(nodeAddrs[i])
	}

	rand.Seed(time.Now().UnixNano())
	clientID := fmt.Sprintf("client-%d", rand.Intn(1000000))
	fmt.Println("ClientID:", clientID)

	clients := make([]rpc.RaftServiceClient, len(nodeAddrs))
	conns := make([]*grpc.ClientConn, len(nodeAddrs))
	for i, addr := range nodeAddrs {
		conn, err := grpc.NewClient(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect to %s: %v\n", addr, err)
			os.Exit(1)
		}
		conns[i] = conn
		clients[i] = rpc.NewRaftServiceClient(conn)
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	round := 0
	for {
		idx := round % len(clients)
		client := clients[idx]
		addr := nodeAddrs[idx]

		// Example command: just a counter as bytes
		cmd := []byte(fmt.Sprintf("cmd-%d", round))
		req := &rpc.ClientCommandRequest{
			Metadata: []byte(fmt.Sprintf("meta-%d", round)),
			Command:  cmd,
			ClientId: clientID,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		resp, err := client.ClientCommand(ctx, req)
		cancel()
		if err != nil {
			panic(err)
			// fmt.Printf("[Node %s] Error sending command: %v\n", addr, err)
		} else {
			fmt.Printf("[Node %s] Sent command: %s, Got result: %s\n", addr, string(cmd), string(resp.Result))
		}

		round++
		time.Sleep(1 * time.Second)
	}
}
