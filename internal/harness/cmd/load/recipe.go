package main

import "time"

// The recipe of the end-to-end baseline, agreed with the maintainer: every
// run measures the same thing, so these are constants and nothing on the
// command line changes them. The command line can only run fewer of the
// cases (-sizes, -modes) or fewer runs of each (-runs), and a results file
// says when it did. Changing a value here is a recipe change, recorded in
// its commit. The fixed mode requires a rate supplied outside the recipe.
const (
	// payloadSize is every publication's payload, in bytes.
	payloadSize = 256
	// groupSize is how many publishers share a group: publisher p of group
	// g publishes on load/<g>/<p>, and a group's subscribers take
	// load/<g>/+, so every message reaches every subscriber of its group.
	groupSize = 10
	// sessionExpiry is each subscriber's Session Expiry Interval, in
	// seconds: a durable session (Clean Start 0). Publishers have clean
	// sessions: Clean Start 1, expiry 0.
	sessionExpiry = 3600
	// seed decides which half of the publishers publish only at QoS 0 and
	// which half only at QoS 1.
	seed = 20261024
	// warmUp is how long the clients publish before the measurement
	// starts; measured is how long the measurement lasts.
	warmUp   = 10 * time.Second
	measured = 30 * time.Second
	// observeLimit is how long after the measured window ends the run
	// keeps observing: the publishers settle their QoS 1 publications and
	// the subscribers drain, both within this one cutoff. A publisher never
	// waits for consumers (design.md, R5), so subscribers can end a run
	// behind: the wire counts gaps and unresolved tails alike for both
	// brokers, with Sagüin's database evidence beside them.
	observeLimit = 10 * time.Second
	// answerTimeout bounds a socket write (publication writes also obey
	// the run's end), and the read of what a peer
	// still sends after the run's DISCONNECT, before the run fails. A
	// subscriber waits for deliveries without limit: under overload a
	// quiet subscriber is measured (coverage, latency), not failed.
	answerTimeout = time.Minute
	// runsPerCase is how many runs each case takes; a case reports the
	// median and the spread (lowest to highest) over them.
	runsPerCase = 5
)

// A size is how many publishers and subscribers a case runs, subscribers
// spread evenly over the publishers' groups.
type size struct {
	name        string
	publishers  int
	subscribers int
	// qos1Only leaves the QoS 0 publishers connected but idle: for the
	// publish-only experiment (experiment.go), never the recipe.
	qos1Only bool
}

// sizes are the recipe's three sizes, in the order they run.
var sizes = []size{
	{"small", 10, 10, false},
	{"medium", 100, 100, false},
	{"large", 1000, 1000, false},
}

// A mode is how the publishers pace themselves.
type mode int

const (
	// maximum: each publisher publishes as fast as the broker accepts,
	// a QoS 1 publisher with at most its Receive Maximum unacknowledged.
	maximum mode = iota
	// fixed: the publishers together send at -rate, each
	// publication due at its own time, spread evenly over the publishers.
	fixed
)

// String names the mode in the results.
func (m mode) String() string {
	if m == maximum {
		return "maximum"
	}
	return "fixed"
}

// brokerMemory is the broker's own memory bound, its own systemd scope's
// MemoryMax, swap off; the load tool runs under make's bound, in another
// scope. -broker-memory sets it outside the recipe.
var brokerMemory = "4G"

// The test matrix, outside the recipe, keeps Sagüin from being tuned to the
// recipe's one load: -payload, -qos1-share and -fanout change these from
// the recipe's values, and the results then say so.
var (
	// payloads is how big each publication's payload is (payload.go).
	payloads = payloadDist{fixed: payloadSize}
	// qos1Share is the share of the publishers that publish only at QoS 1,
	// the rest only at QoS 0, as the seed decides (newRun).
	qos1Share = 0.5
	// fanout is each group's subscribers, so how many receive each
	// message: the recipe's sizes have groupSize, one subscriber per
	// publisher. A group's publishers stay groupSize.
	fanout = groupSize
	// rateOverride is -rate: the publishers' total publications per second
	// in the fixed mode, required to run it, for every size.
	rateOverride float64
)
