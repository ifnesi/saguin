// Package testkit holds the helpers the repository's tests share, whatever
// package they test: allocation counts, waits with a backstop, goroutine
// walks, metric scrapes, command runs, a test run again on its own, test
// files, TLS certificates, the MQTT string oracles and the log line's. It
// is test-support code, as internal/harness is: never linked into the
// broker, whose code may not import it, and outside the R1 and R3 scans
// (internal/rules), so a helper may start a goroutine of its own. It
// imports the standard library alone, so any test package can import it
// without a cycle, and its oracles stay independent of the code they judge.
package testkit

import (
	"testing"
	"time"
)

// NoAllocs fails the test if f allocates, on average over many runs.
func NoAllocs(t testing.TB, what string, f func()) {
	t.Helper()
	if n := testing.AllocsPerRun(100, f); n != 0 {
		t.Errorf("%s allocates %v times a call; want 0", what, n)
	}
}

// Within polls ok until it holds or limit passes, and reports whether it
// held. It checks at once, then after a millisecond, the wait doubling up
// to 20 milliseconds, so a quick condition is seen quickly and a long wait
// costs little. In a synctest bubble the clock is the bubble's.
func Within(limit time.Duration, ok func() bool) bool {
	pause := time.Millisecond
	for deadline := time.Now().Add(limit); ; {
		if ok() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(pause)
		pause = min(2*pause, 20*time.Millisecond)
	}
}

// ReturnsWithin runs f on a goroutine of its own and reports whether it
// returned within limit: a stop that hangs fails its test instead of
// hanging it. A hung f is left running; what it writes is the caller's to
// read only when ReturnsWithin reports true.
func ReturnsWithin(limit time.Duration, f func()) bool {
	done := make(chan struct{}, 1)
	go func() {
		f()
		done <- struct{}{}
	}()
	_, ok := RecvUntil(done, limit, nil)
	return ok
}

// RecvUntil takes values from ch, handing each to keep, until keep keeps
// one, and returns it; a nil keep keeps the first. A closed channel gives
// its zero value, as a receive does, which signals by close. It reports
// false where none is kept within limit, counted from the call (in a
// synctest bubble, on the bubble's clock).
func RecvUntil[T any](ch <-chan T, limit time.Duration, keep func(T) bool) (T, bool) {
	deadline := time.After(limit)
	for {
		select {
		case v := <-ch:
			if keep == nil || keep(v) {
				return v, true
			}
		case <-deadline:
			var zero T
			return zero, false
		}
	}
}

// Recv takes a value from ch, failing the test where none comes within
// limit; what names the value for the message.
func Recv[T any](t testing.TB, ch <-chan T, limit time.Duration, what string) T {
	t.Helper()
	v, ok := RecvUntil(ch, limit, nil)
	if !ok {
		t.Fatalf("waited %v for %s", limit, what)
	}
	return v
}

// Send puts v on ch, failing the test where ch does not take it within
// limit.
func Send[T any](t testing.TB, ch chan<- T, v T, limit time.Duration) {
	t.Helper()
	select {
	case ch <- v:
	case <-time.After(limit):
		t.Fatalf("nothing took %T within %v", v, limit)
	}
}
