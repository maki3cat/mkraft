package common

import (
	"context"
	"time"
)

// HasDeadline returns true if the context has a deadline set, false otherwise.
func HasDeadline(ctx context.Context) bool {
	_, ok := ctx.Deadline()
	return ok
}

// DeadlinePassed returns true if the context has a deadline and it has already passed.
func DeadlinePassed(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return deadline.Before(time.Now())
}
