// Package timestamp handles deadline rounding in Unix milliseconds.
package timestamp

import "time"

// CeilMilli returns Unix milliseconds rounded up, leaving exact milliseconds
// unchanged. A deadline derived from it cannot precede the full interval.
// Due-time comparisons should use Time.UnixMilli instead, so a future
// millisecond is never treated as already reached.
func CeilMilli(at time.Time) int64 {
	millis := at.UnixMilli()
	if at.Nanosecond()%int(time.Millisecond) != 0 {
		millis++
	}
	return millis
}

// AfterMilli returns a retention deadline for an interval in milliseconds
// measured from a floored Unix millisecond timestamp. The discarded fraction
// may be almost a millisecond, so allow a full millisecond: late, never early.
// An origin of zero returns the adjusted interval for SQL deadline and due-time
// arithmetic. Origins already rounded up with CeilMilli need ordinary addition.
// Message Expiry must never be late, so it keeps its unadjusted deadline.
func AfterMilli(origin, interval int64) int64 {
	return origin + interval + 1
}
