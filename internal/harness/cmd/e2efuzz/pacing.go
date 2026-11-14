package main

import (
	"fmt"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/testkit"
)

// Publish limits (design.md, Flows, PUBLISH, Rate limits: "publish_rate and
// publish_bytes slow a publisher down. They never refuse a publish. The
// reader counts them in buckets holding one second's worth, full at
// CONNECT. Every completely received publish is charged at its receipt
// time, including one refused for another policy reason. A message larger
// than a second's bytes passes and leaves the byte bucket in debt. When a
// charge leaves the message bucket below one message, or the byte bucket at
// or below zero, the reader reads nothing more from its connection until
// the message bucket holds one message again and the byte bucket is above
// zero"; "Nothing is refused or lost, at any QoS or MQTT version"). The
// run's acl_file gives pacedUser's entry both limits (writeACL), which
// apply to each of its connections separately (Access and reload), so its
// windows and floods outrun them. Nothing it sends is predicted otherwise:
// every answer is checked as any client's, and its deliveries keep its
// order as any publisher's. On a broker the run starts, which reads that
// file, each answer to one of its PUBLISHes is also checked to come no
// sooner than the limits let the reader read that PUBLISH (pacing.charge),
// and at the end the broker must have waited for an allowance (throttled):
// a run whose readers never waited did not test the limits.

// pacedUser is the user whose entry in the run's acl_file sets publish
// limits, pacedRate its publish_rate and pacedBytes its publish_bytes:
// above what one client's ordinary steps send in a second, below what its
// windows and floods send, so those wait for the allowance, a second or
// two at most, well within the run's answer timeout.
const (
	pacedUser  = "ann"
	pacedRate  = 100
	pacedBytes = 512 << 10
)

// pacedLimits is pacedUser's limits as its acl_file entry writes them.
var pacedLimits = fmt.Sprintf("{publish_rate: %d, publish_bytes: %dKiB}", pacedRate, pacedBytes>>10)

// pacing is what the model knows of one connection's publish allowance,
// from the PUBLISHes it wrote through send, each written at or before its
// receipt: the PUBLISHes counted, the payload bytes they carried, and,
// over every PUBLISH j counted so far, the greatest of j's writing less
// j's count over pacedRate (byRate), and of j's writing less the payload
// bytes before it over pacedBytes (byBytes). The zero value is a new
// connection's.
//
// Its buckets hold at most a second's worth when j is charged, are charged
// at least one message and j's payload for each PUBLISH from j on (the
// broker may count more bytes, and PUBLISHes the run sent otherwise, such
// as a QoS 2 PUBLISH sent again), and refill at pacedRate and pacedBytes a
// second. So the reader reads the m-th PUBLISH no sooner than j's writing
// plus (m-j+1-pacedRate)/pacedRate seconds, nor than j's writing plus the
// payload bytes of j to m-1, less pacedBytes, over pacedBytes. Each bound
// keeps paceRoom of the wait, clear of the broker's rounding.
type pacing struct {
	count           int
	bytes           int
	byRate, byBytes time.Time
}

// paceRoom is the part of each wait the limits demand that the run holds
// the broker to.
const paceRoom = 0.9

// charge counts a PUBLISH of size payload bytes written at at, and returns
// the earliest the reader can read it: its answer comes no sooner.
func (p *pacing) charge(at time.Time, size int) time.Time {
	// seconds is a duration of x seconds, its room kept.
	seconds := func(x float64) time.Duration { return time.Duration(paceRoom * x * float64(time.Second)) }
	p.count++
	p.byRate = later(p.byRate, at.Add(-seconds(float64(p.count)/pacedRate)))
	p.byBytes = later(p.byBytes, at.Add(-seconds(float64(p.bytes)/pacedBytes)))
	earliest := later(p.byRate.Add(seconds(float64(p.count+1-pacedRate)/pacedRate)),
		p.byBytes.Add(seconds(float64(p.bytes-pacedBytes)/pacedBytes)))
	p.bytes += size
	return earliest
}

// later is the later of a and b.
func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// paced reports whether the model holds s's connection to pacedUser's
// limits: it logged in as pacedUser on a broker the run started.
func (s *slot) paced() bool {
	return s.f.paces && s.user == pacedUser
}

// earlyAnswer returns an error where ack, read now, answers o before the
// earliest the reader could read o (pacing.charge), nil otherwise.
func earlyAnswer(ack *harness.Ack, o *out, now time.Time) error {
	if o.earliest.IsZero() || !now.Before(o.earliest) {
		return nil
	}
	return fmt.Errorf("%s 0x%02X to %s %v before %s's publish limits %s let the reader read it (design.md, PUBLISH, Rate limits: \"the reader reads nothing more from its connection until the message bucket holds one message again and the byte bucket is above zero\")",
		harness.Name(ack.Kind), ack.Reason, o, o.earliest.Sub(now), pacedUser, pacedLimits)
}

// throttledSeries is metrics.md, Publishing,
// saguin_publish_throttled_seconds_total: "time readers spent waiting for
// their connection's publish allowance to refill".
const throttledSeries = "saguin_publish_throttled_seconds_total"

// throttled reads b's /metrics at the end of a run of a broker it started
// and returns an error unless throttledSeries rose above 0: a run in which
// no reader waited for its allowance did not test the limits (design.md,
// Testing: "A run that tested nothing never passes").
func throttled(b *harness.Broker) error {
	return risen(b, testkit.SeriesKey(throttledSeries), "no reader ever waited for its publish allowance, so the publish limits were not tested (design.md, PUBLISH, Rate limits)")
}
