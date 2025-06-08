package common

import "errors"

func ContextDoneErr() error {
	return errors.New("context done")
}

// raft log errors
var ErrPreLogNotMatch = errors.New("prelog not match")
var ErrNotLeader = errors.New("not leader")
var ErrServerBusy = errors.New("server busy")

// consensus errors
var ErrNotEnoughPeersForConsensus = errors.New("not enough peers for consensus")
var ErrMajorityNotMet = errors.New("majority not met")

// invariants broken
var ErrInvariantsBroken = errors.New("invariants broken")

var ErrContextDone = errors.New("context done")
