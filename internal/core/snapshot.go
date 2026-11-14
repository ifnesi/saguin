package core

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/store"
)

type snapshotState struct {
	every               time.Duration
	at, began           time.Time
	source, destination string
	dirty, pending      bool
	final, unhealthy    bool
	finalTaken          bool
	ok, failed          *metrics.Counter
	lastSuccess         *metrics.SecondsGauge
	duration            *metrics.SecondsGauge
}

func (r *storeRequest) snapshot() bool {
	return r.kind == foldSnapshot || r.kind == copySnapshot
}

func (c *Core) startSnapshot(now time.Time, final bool) {
	s := &c.snapshot
	s.at, s.began, s.pending, s.final = time.Time{}, now, true, final
	c.enqueue(&storeRequest{kind: foldSnapshot, op: &store.SnapshotFold{}})
}

// finalSnapshot follows every accepted request and its answer. It is also
// reached directly by the answer which ends a snapshot already running at stop.
// A latched store error skips the final snapshot and retains the RAM copy.
// Other failures, such as a failed accept, drain through this same final copy.
func (c *Core) finalSnapshot(acts []Action, now time.Time) []Action {
	s := &c.snapshot
	if c.phase >= stopping && c.phase < stopped && c.failure == nil && len(c.connections) == 0 && c.dialing == 0 &&
		s.every != 0 && s.dirty && !s.pending && c.storeIdle() {
		c.startSnapshot(now, true)
		acts = c.dispatchRequests(acts, now)
	}
	return acts
}

func (c *Core) endSnapshot(acts []Action, err error, now time.Time) []Action {
	s := &c.snapshot
	s.pending, s.at = false, now.Add(s.every)
	s.duration.Set(int64(now.Sub(s.began)))
	s.unhealthy = err != nil
	if err == nil {
		if s.final {
			s.finalTaken = true
		}
		s.lastSuccess.Set(now.UnixNano())
		s.ok.Inc()
	} else {
		s.dirty = true
		s.failed.Inc()
		if c.failure == nil {
			if s.final || !store.SnapshotFailed(err) {
				acts = c.fail(acts, err, now)
			} else {
				acts = c.logFailure(acts, logger.Error, fmt.Errorf("snapshot of RAM copy %s: %w", s.source, err), now)
			}
		}
	}
	c.updateReady()
	return acts
}

// SnapshotSafe reports whether a clean stop may discard its RAM copy. Main
// calls this only after Run has ended and ownership has returned to main.
func (c *Core) SnapshotSafe() bool {
	return c.snapshot.every != 0 && c.failure == nil && !c.snapshot.pending && !c.snapshot.dirty
}
