//go:build !race

package mqtt

// underRace says the tests were built with the race detector. See the
// build-tagged file beside this one.
const underRace = false
