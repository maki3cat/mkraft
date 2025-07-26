# Coding and Implementation Guide

## Table of Contents

- [Coding and Implementation Guide](#coding-and-implementation-guide)
  - [Table of Contents](#table-of-contents)
  - [Concurrency Programming](#concurrency-programming)
    - [Rules and Principles](#rules-and-principles)
      - [GENERAL RULEs](#general-rules)
      - [LOCKS RULEs](#locks-rules)
      - [CONDITIONAL VARIABLE RUles](#conditional-variable-rules)
    - [Current Key Use-cases](#current-key-use-cases)
    - [Testing and Verification](#testing-and-verification)
  - [Code Structure](#code-structure)
    - [from MIT-Moris](#from-mit-moris)



## Concurrency Programming

maybe the hardest part

### Rules and Principles

#### GENERAL RULEs
(from Yang)
- " mutiple goroutines + rw/ww " requires synchronization -> read only doesn't need that, sometimes when only write at init time and rr afterwards, in this case we don't need runtime synchronization;
- "the types of problems" : (1) atomicity problem; (2) data racing problem; (3) the order problem;

#### LOCKS RULEs
(from Yang)
 - p1: " protect the shared data "
 - P2: " lock all or nothing " 
 - p3: coarse-grained locking OR fine-grained locking; " correctness first, then performance "; but actually in system engineering, we need all;
    (additional from MIT-Moris; summarized from http://nil.csail.mit.edu/6.824/2022/labs/raft-locking.txt)
   - p3.a) better protect the whole critical section accessing (read/write), or, states may change in-btw
   - p3.4) don't hold the lock for time-consuming operations (IO/long-running operations)

#### CONDITIONAL VARIABLE RUles
(from Yang)
- can handle the order problem just like semaphore
- always use together with lock
- always use other conditions because CONDITIONAL variable has no memory as in Sempaphore
- use `while` instead of if
- use `broadcast` instead of signal : 1) not sure which one to wake up; 2) linux spurious wakeup;

### Current Key Use-cases

to be summarized

### Testing and Verification

USE RACE DETECTOR in testing
- `go build -race -o bin/mkraft cmd/main.go`
- check in logs keywords of RACE

Exhaust the definition of invariants, and verifiy the invariants defined using trace and logs. (need to flesh this idea out in practical ways)


## Code Structure




### from MIT-Moris

Original words are from here, 
http://nil.csail.mit.edu/6.824/2022/labs/raft-structure.txt
and I have put the lines here
that are important and not out-of-dated in new Golang Versions go1.24.

A Raft instance has to deal with **the arrival of external events**
(Start() calls, AppendEntries and RequestVote RPCs, and RPC replies),
and it has to execute **periodic/time-driven tasks (elections and heart-beats)**.
There are **many ways to structure** your Raft code to manage these
activities; this document outlines a few ideas.

- Each Raft instance has a bunch of state (the log, the current index,
&c) which must be updated in response to events arising in concurrent
goroutines. Experience suggests that for Raft
it is most straightforward to use **shared data and locks**.

- You'll want to have a separate **long-running goroutine** that sends
committed log entries in order on the applyCh. It must be **separate**,
since sending on the applyCh **can block**; 
   - and it must be **a single goroutine**, since otherwise it may be hard to ensure that you send log
entries in **log order**. 
   - The code that advances **commitIndex will need to
kick the apply goroutine; it's probably easiest to use a condition
variable (Go's sync.Cond) for this** (wait for sth can be chan or cond)

- Each RPC should probably be sent (and its reply processed) in its own
goroutine, for two reasons: so that unreachable peers don't delay the collection of a majority of replies; so that the heartbeat and election timers can continue to tick at all times. It's easiest to do the RPC reply processing in the same goroutine, rather than sending reply information over a channel;