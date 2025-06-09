
[![Test and Coverage](https://github.com/maki3cat/mkraft/actions/workflows/test-coverage.yml/badge.svg?branch=main)](https://github.com/maki3cat/mkraft/actions/workflows/test-coverage.yml)

## Paper and Learning Web

<img src="img/logo.jpg" alt="My Image" align="right" width="250">

https://raft.github.io/


## Implementation MileStones

### V1 without Testing

Scope: Basic Engineering Framework, Leader Election, Log Replicaiton, Safety
Design:

<img src="img/impl_design_v1.jpg" alt="design-v1" align="right">


<!-- ### Basic Raft Properties and Mechanisms that guarantee them
Raft guarantees that each of these properties is true at all the time
#### Leader Election ($5.2)-> DONE
- Election Safety: at most one leader can be elected in a given term (not time but term) -> ($5.2 Leader Election)
#### Log Replication ($5.3)-> DONE
- Leader Append-only: a leader never overwrites or deletes entries in its logs; it only appends new entries; -> ($5.3 Log Replication)
(but followers may overwrite or truncate their logs)
- Log Matching: if two logs contain an entry with the same index and term, then logs are identical in all entries up through the given index; -> ($5.3 Log Replication)
#### Safety ($5.4)->DONE
- Leader Completeness: if a log entry is commited in a given term, then that entry will be present in the logs of the leaders for all higher numbered terms; ($5.4)
- State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index ($5.4.3)
#### Testing of the Cluster
#### Automatic Deployment of the Cluster


### From Lab to Industry

#### Dynamic Membership
#### Log Compaction
#### An implementation of The StateMachine


## Backlog

### Raft Paper
- [x] AppendEntries
- [x] Log Replication
- [x] Persistence
- [ ] Membership Changes
- [ ] Log Compaction
- [ ] Example of Pluggable State Machine

### Infra Framework
- [ ] rate limiter
- [ ] pressure testing
- [ ] otel: metrics

grpc part
https://grpc.io/docs/guides/performance/
- [ ] try to change to streaming RPCs (with keep alive) or keep both and do a benchmarking
- [ ] channel optimization
- [ ] error handling
- [ ] authentication
- [ ] flow control
      
| Number | Feature Topic                                                        |
| ------ | -------------------------------------------------------------------- |
| 1      | deadline/timeout                                                     |
| 2      | graceful shutdown (https://grpc.io/docs/guides/server-graceful-stop) |
| 3      | server/client interceptors                                           |
| 4      | compression                                                          |
| 5      | retry (partially used)                                               |



<img src="img/logo1.jpg" alt="My Image" align="left" width="300">

## Patterns for GoGymnastics
- [ ] async all with future pattern
- [ ] retry call with timeout
- [ ] fan-in/fan-out in RAFT
- [ ] short stopping in RAFT
- [ ] ticker, timer
- [ ] context examples summary
- [ ] plummer and channels

## Implementation Decisions and Extreme Engineering
- [ ] add batching to handling ClientCommands, and send AppendLogEntries
- [ ] check prof Yang's improvements of paxos for ideas
- [ ] check architecture classes of caching and memory hierarchy for ideas -->