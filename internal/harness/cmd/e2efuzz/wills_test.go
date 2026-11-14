package main

import "testing"

// design.md, DISCONNECT: "The Will goes after its Will Delay, or at once
// for MQTT 3.1.1"; "A reconnect in time cancels it. If the session ends
// first, the Will goes early." Restart: "A Will whose client was connected
// at the stop is due its Will Delay Interval after the start".
//
// The summary counts publications already observed, immediate Wills and
// delayed Wills known due at a checkpoint. A delay still running, a possible
// cancellation and a Will kept for restart are not missing publications.
// A due Will without a stored record still counts, so counting does not
// conceal a missing record. The broker checks remain independent.
func TestWillCountsOnlyDuePublications(t *testing.T) {
	for _, c := range []struct {
		name                                    string
		state                                   willState
		checked                                 bool
		uncertain                               bool
		dropped                                 bool
		stored                                  bool
		delivered                               bool
		mayDrop                                 bool
		keptAfterStop                           bool
		published, found, notDue, possibleDrops int
	}{
		{name: "immediate", state: willFired, published: 1},
		{name: "possibly full channel", state: willFired, mayDrop: true, possibleDrops: 1},
		{name: "possibly full but stored", state: willFired, mayDrop: true, stored: true, published: 1, found: 1},
		{name: "possibly full but delivered", state: willFired, mayDrop: true, delivered: true, published: 1},
		{name: "immediate kept at stop", state: willFired, keptAfterStop: true, notDue: 1},
		{name: "due delay kept at stop", state: willWaiting, checked: true, keptAfterStop: true, notDue: 1},
		{name: "immediate stored", state: willFired, stored: true, published: 1, found: 1},
		{name: "delay still running", state: willWaiting, notDue: 1},
		{name: "delay due at checkpoint", state: willWaiting, checked: true, published: 1},
		{name: "delayed stored", state: willWaiting, stored: true, published: 1, found: 1},
		{name: "delayed delivered", state: willWaiting, delivered: true, published: 1},
		{name: "reconnect near due time", state: willWaiting, checked: true, uncertain: true, notDue: 1},
		{name: "uncertain but stored", state: willWaiting, uncertain: true, stored: true, published: 1, found: 1},
		{name: "kept for restart", state: willDeferred, notDue: 1},
		{name: "published before stop", state: willDeferred, stored: true, published: 1, found: 1},
		{name: "cancelled", state: willCancelled, checked: true},
		{name: "discarded", state: willDiscarded},
		{name: "refused", state: willRefused},
		{name: "connected", state: willHeld},
		{name: "unanswered CONNECT", state: willSent},
		{name: "dropped", state: willFired, dropped: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := &will{state: c.state, checked: c.checked, uncertain: c.uncertain, dropped: c.dropped, keptAfterStop: c.keptAfterStop, mayDrop: c.mayDrop}
			if c.stored {
				w.found = "channel 1 offset 1"
			}
			w.delivered.Store(c.delivered)
			f := &fuzzer{wills: map[string]*will{"w1": w}}
			if published, found, notDue, possibleDrops := f.willCounts(); published != c.published || found != c.found || notDue != c.notDue || possibleDrops != c.possibleDrops {
				t.Fatalf("%d Wills published, %d found stored, %d not yet due, %d possibly dropped; want %d, %d, %d, %d", published, found, notDue, possibleDrops, c.published, c.found, c.notDue, c.possibleDrops)
			}
		})
	}
}
