//go:build race

package broker_test

// underRace says the tests were built with the race detector, which the
// suite's own run uses. It multiplies what a hold of the broker's lock is
// allowed to take: the detector instruments every memory access, so the
// same work takes an order of magnitude longer, and a bound set from a
// plain run would fail for the tooling rather than for the broker.
const underRace = true
