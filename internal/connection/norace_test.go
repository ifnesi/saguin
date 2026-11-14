//go:build !race

package connection_test

// raceEnabled reports whether the test binary is built with the race
// detector, whose memory profile TestIdleConnectionsHoldNoBuffer reads.
const raceEnabled = false
