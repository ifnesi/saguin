//go:build race

package mqtt

// underRace says the tests were built with the race detector, under which
// sync.Pool drops a quarter of what it is given back, so a count of
// allocations taken there measures the detector rather than the code.
const underRace = true
