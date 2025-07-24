

motto: testing, debugging are harder than implementation of this system

## Learning and Principles
- so far, the more "panics" the better for errors not clear how to handle, for broken invariants;
  if the process stops, it is easy to debug and find solutions;



## TESTING Cases and Operations

About Leader Election


### BASIC CASES
(1) after the majority of the cluster nodes are up, the cluster should be able to elect out a leader ASAP;

how to do it:
- `make serverstart`
- `make verification` the term-1 should have a leader

(2) when the leader is down, another leader is elected in the next term;

- `kill {leaderPid}`
- `make verification` to find term-2 with a new leader

(3) when the original leader node is added back, it becomes a follower;
- restart the node
- `make verification` to find it a follower to catchup with term-2

(4) check the logs
- the trace of metastate change `node1/state_history.mk` should be decent; no weird messed-up lines;
- the 