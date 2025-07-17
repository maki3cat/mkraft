
## Correctness

1. ELECTION SAFETY AND EFFICIENCY

efficiency here can be viewed as kind of correctness that a leader should be elected ASAP

| #   | Invariant / Property                                                                 | Verification                      |
| --- | ------------------------------------------------------------------------------------ | --------------------------------- |
| 1   | Only one leader exists for one term (paper)                                          | by logs                           |
| 2   | When leader is elected, others become followers                                      | Partly by logs; partly by manual; |
| 3   | When election happens, it usually gets the new leader with next term                 | by logs                           |
| 4   | When leader is elected, it usually doesn't change if nothing unstable happens        | Partly by logs                    |
| 5   | The elected leader should be the one with all the committed logs (paper)             | Haven't been auto-checked         |
| 6   | We don't want more than 1 node to become Candidate; this will slow down the election | todo by logs                      |


**Key Progress**



- currently working on
1. when only one node, it keeps re-election; and it becomes a leader as soon as another node joins;

- first without client commands
- 1/2 works currectly with continuous client commands 

testing for systems is no simplier than systems themselves.

## Journey of Testing
```
make integration-test
check the logs
make verification
```

- My radical statement **"Panic" is the best**: anti-intuition decision, use panic freely whenever there is no explicit way of handling errors; debugging is extremely hard for this kind of project;

- **Unit-testing is Trivial and should be all AI generated**: 
I feel unit-testing for this kind of system engineering project is useless for most of the part. It is important when I calculate the majority or doing some math; but for the complex systems, they are far from enough; I am not saying we should 



### Design Invariants/Properties Verification

#### (1) Election Safety (VerificationScript-Covered)
In each term change, each node stores a tuple of (timestamp, term, nodeID, the State of Leader/Candidate/Follower),
and we continuously compare logs of all nodes to check if only one leader is elected;

#### (2) Leader append-only (UT-Covered)
By condition checking in method and unit-testing, which means the method to overwrite/delete logs cannot be called
by a node which is a leader;

#### (3) Log Matching (VerificationScript-Covered)
We compare the tuples of (index, term and log) of all nodes to check if this is matched.

#### (4) Leader Completeness (UT-Covered)
This one is a bit hard to verify directly, but this property is guaranteed by Log Matching and Vote Restriction,
so we check this property by checking the 2 property/restriction indirectly.
For the voting restriction, we validate it by unit-testing to guarantee the vote cannot be true if the restriction is broken.
(marked with invariant)

#### (5) StateMachine Safety
We implement an statemachine which will just record ths raft log with index and term for each node.
The use case is after a testing round is done, we run comparing
the raft logs to check if at any index a different log is applied for the state machine.

### Adversal Environment
- happy env, no injected errors

### Engineering, the testing framework/infra
todo

## Invariants/Properties to hold in any condition

1. Election Safety:
at most one leader can be elected at a given term; ($5.2)

2. Leader Append-only: ($5.3)
    - a leader never deletes OR overwrites its log entries;
    - it only appends new log entries;

3. Log Matching: ($5.3)
if two logs contain an entry with the same index + same term, 
all logs are identical in all entries through the given index;

4. Leader Completeness: ($5.4)
IF a log entry is commited in a given term,
then that entry will be present in the logs of the LEADERS for all higher-numbered terms;
(The leader has all pre-committed logs. The log entries only flow from leader to followers.)

5. State Machine Safety: ($5.4)
if a server has applied a log entry at a given index to the state machine, 
no other server will ever apply a different log entry for the same index; 

### Key Mechanisms to maintain the invariants/properties

(1) Voting Restriction ($5.4)
The voter denies its vote if its log is more up-to-date than that of the candidate.
What is more up-to-date mean:
- compare the index(length) and term of the last log entry
- first, the larger term is more updated
- second, if the last terms are the same, the longer log/larger index is the more up-to-date

## Resilience of Engineerings 

1) How the errors are managed?