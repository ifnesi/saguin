package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

const liveOperatorWill fault = "Will before the operator's disconnect"

// design.md, $saguin topics: "An operator disconnects the client whose id
// is the payload (0x98). Its session is kept." DISCONNECT: "The Will goes
// after its Will Delay, or at once for MQTT 3.1.1"; "A reconnect in time
// cancels it." MQTT-3.1.2-8 requires the Will when the connection closes
// without the client's DISCONNECT. The requester also subscribes to the
// Will, which may arrive among its request's answers. The planted fault
// publishes it while the target is still connected, before any request.
func TestOperatorDisconnectPublishesItsTargetsWill(t *testing.T) {
	for _, delay := range []uint32{0, 1} {
		t.Run(fmt.Sprintf("delay %d", delay), func(t *testing.T) {
			caughtInScene(t, liveOperatorWill, sceneQueue, "its connection still open", func(sc *scene) error {
				requester, err := sc.client(0, "ann", false, false)
				if err != nil {
					return err
				}
				if err := sc.subscribe(requester, harness.Subscription{Filter: "#", QoS: 0, RetainHandling: 2}); err != nil {
					return err
				}
				target := sc.f.slots[1]
				giveWill := withWill(sc.f.step + 1)
				if err := sc.connect(target, "ann", func(p *harness.Connect) {
					durable(p)
					giveWill(p)
					p.Will.QoS, p.Will.Retain = 2, true
					p.Will.Props = harness.Properties{{ID: harness.WillDelayInterval, Int: delay}}
				}); err != nil {
					return err
				}
				w := target.will
				if sc.b.fault == liveOperatorWill {
					sc.b.mu.Lock()
					sc.b.publishWillOf(target.id, sc.b.sessions[target.id].conn.will)
					sc.b.mu.Unlock()
				}
				if err := sc.read(requester); err != nil {
					return err
				}
				if err := sc.step(); err != nil {
					return err
				}
				p, a := requester.newRequest(disconnectTopic, target.id, false, "")
				p.QoS, a.want = 1, "hung-up"
				if err := requester.disconnectOther(p, a, target); err != nil {
					return err
				}
				// Read the delayed publication through the same delivery oracle.
				// An immediate one may already have accompanied the answer.
				if !w.delivered.Load() {
					if _, err := requester.read(); err != nil {
						return err
					}
				}
				if !w.delivered.Load() {
					return errors.New("the operator's hang-up left its target's Will undelivered")
				}
				return sc.check()
			})
		})
	}
}
