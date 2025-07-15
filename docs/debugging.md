## Debuggging Journal


#### (No.2) Panic on State not Follower

previously, the trace users Follower instead of n.state,
and n.state is not updated,
we should use the fields in the struct to record trace
refined `trace.go`
```
ToFollower in state.go
	n.CurrentTerm = newTerm
	n.VotedFor = voteFor
	n.state = StateFollower
	n.tracer.add(n.CurrentTerm, n.NodeId, n.state, n.VotedFor)
```


### (No.1) Cascading Errors
- a) first, a Node1 panics after entering the Leader state; -> a bug in handling the index; then other nodes will have this RPC error;
- b) but the thing is the leaving 2 nodes should select a new leader instead of hanging around;
    - b1) One Node2: was doing forever retries, vote for the Other but doesn't degrade to follower;
    - b2) One Node3: was doing retry while running for election, but failed to become leader;

(a)
When a node crashes (e.g. panic happens), the error of "unavailable/connection refused error" happens and which corresponds to the RST packet found on Wireshark. This is expected.

```
**(a)**

2025-07-14T19:20:12.941-0400│   ERROR│  peers/client.go:200│TimeoutClientInterceptor: RPC call error│   {"method": "/RaftService/RequestVote", "error": "rpc error: code = Unavailable desc = connection error: desc  = \"transport: Error while dialing: dial tcp [::1]:18081: connect: connection refused\"", "request": "term:2 candidate_id:\"node3\"", "requestID": "fcf00378-f799-48e2-8870-256f443ef467"}
github.com/maki3cat/mkraft/mkraft/peers.(*peerClient).timeoutClientInterceptor
│   /Users/maki/projects/mkraft/mkraft/peers/client.go:200
google.golang.org/grpc.(*ClientConn).Invoke
│   /Users/maki/go/pkg/mod/google.golang.org/grpc@v1.71.1/call.go:35
github.com/maki3cat/mkraft/rpc.(*raftServiceClient).RequestVote
│   /Users/maki/projects/mkraft/rpc/service_grpc.pb.go:61
github.com/maki3cat/mkraft/mkraft/peers.(*peerClient).asyncCallRequestVote.func1
│   /Users/maki/projects/mkraft/mkraft/peers/client.go:124
2025-07-14T19:20:12.941-0400│   ERROR│  peers/client.go:127│single RPC error in rawClient.RequestVote:│ {"error": "rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing:    dial tcp [::1]:18081: connect: connection refused\"", "requestID": "3ef83300-a86f-4142-b685-8f80b5a7faf1"}
github.com/maki3cat/mkraft/mkraft/peers.(*peerClient).asyncCallRequestVote.func1
│   /Users/maki/projects/mkraft/mkraft/peers/client.go:127
```

