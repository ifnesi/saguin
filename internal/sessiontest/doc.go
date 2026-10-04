// Package sessiontest holds the end-to-end tests about sessions: what a
// session keeps between its client's connections, who owns a client id, a
// takeover, an expiry, a Will, and what a start does with what it finds.
//
// **It is a package of its own so that it runs beside the rest rather than
// after it.** Go runs test packages in parallel and the tests inside one
// package one at a time, so `internal/broker`'s single test binary was the
// whole suite's critical path. The tests here are unchanged - they drive
// the same broker through the same harness in internal/brokertest - and
// only where they run has moved.
package sessiontest
