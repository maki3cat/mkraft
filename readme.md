[![Test and Coverage](https://github.com/maki3cat/mkraft/actions/workflows/test-coverage.yml/badge.svg?branch=main)](https://github.com/maki3cat/mkraft/actions/workflows/test-coverage.yml)

This is a Work-In-Progress Project.

## What this Project is About

A Raft implementation:
- to be built frirst Toy then Industry level;
- to systematically explores and presents what the **GAPS Paper/Theoretical Modeling and Implementation Modeling/Coding(toy/industry)**;
- to investigate the **TESTING** for a complicated system project; the torturing moments are all about the testing yet I still feel unconfident;
- to find some new **INNOVATION** possiblity for this old protocol;
- **EDUCATION** purpose to help navigate colledge students from small labs to real project; and also show what college students can do;

<img src="img/logo.jpg" alt="My Image" align="right" width="250">

## What I am currently Doing

Testing and design testing the following key features

<b> v0.2.0-alpha </b>
- $5.2 Leader Election
- $5.3 Log Replication
- $5.4 Safety

testing for systems is no simplier than systems themselves.

## Some Key Decisions
- anti-intuition decision, use panic freely whenever there is no explicit way of handling errors; debugging is extremely hard for this kind of project;

## The Architecture

<a href="img/impl_design_v1.jpg">
  <img src="img/impl_design_v1.jpg" alt="design-v1" align="right">
</a>


## Key Deisgns
### Invariants/Properties to hold in any condition

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

### Meta-State Machine
<a href="img/impl_design_state_v2.jpg">
  <img src="img/impl_design_state_v2.jpg" alt="design-v1" align="right">
</a>


### Key Mechanisms to maintain the invariants/properties

(1) Voting Restriction ($5.4)
The voter denies its vote if its log is more up-to-date than that of the candidate.
What is more up-to-date mean:
- compare the index(length) and term of the last log entry
- first, the larger term is more updated
- second, if the last terms are the same, the longer log/larger index is the more up-to-date


## The Testing 

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


## Resilience of Engineerings 

1) How the errors are managed?
