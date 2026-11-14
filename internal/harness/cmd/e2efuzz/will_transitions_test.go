package main

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

// The Will transitions a run's reloads and reconnects drive (wills.go,
// reload.go), each checked through the oracles a run uses: the record check
// (checkRecords), the delivery check and its bound (read), the PUBACK
// prediction (placeAll, collect), the checkpoint (checkDeliveries) and the
// counts (willCounts). The Will's client holds no socket: its CONNECT, the
// end of its connection, its clean reconnect and the reloads are the model's
// own entry points (newWill, fire, reconnectWill, restrictedWills and
// unsettleWills). The fake broker publishes, stores and delivers what a
// broker would, or forges what a broker must not.
//
// Whether a Will may have been dropped for its topic depends on whether the
// core may have checked it, at its earliest due time or later (at its
// release for one without a Will Delay), while the acl_file denied that
// topic: a Will whose check can fall only while the topic is allowed is
// published, one whose check can fall only while it is denied is dropped,
// and only an overlap leaves it either.

// willPayload is the payload of every Will willSlot gives, and forged the
// payload of a record of it a broker altered.
const (
	willPayload = "w"
	forged      = "forged"
)

// payloadMismatch is what the record check says of a Will's record whose
// payload alone is forged (checkWillRecord).
var payloadMismatch = fmt.Sprintf("payload %q, want %q", forged, willPayload)

// runRights are restricted's rights as the run's acl_file gives them,
// before any reload (reloads.restore).
var runRights = slices.Clone(rights)

// playWills is playScene for script, rights the run's from its start and
// given back at its end, so that no scene, a subtest's or one of several a
// test plays, starts from another's reloads. Every other state a scene
// changes is its own fuzzer's, fake's or connections'.
func playWills(t *testing.T, script func(*scene) error) error {
	t.Helper()
	copy(rights, runRights)
	defer copy(rights, runRights)
	return playScene(t, "", sceneQueue, script)
}

// smallGrant is the index in rights of restricted's write on small, a
// channel with a max_bytes.
var smallGrant = slices.IndexFunc(rights, func(g grant) bool { return g.channel == "small" })

// willSlot gives slot 1, the Will's client's in every scene here (slot 0
// being its subscriber's or another client's), a connection as restricted,
// whose rights the reloads change, whose accepted CONNECT carried a Will on
// topic, a format holding the Will's tag (willTag), at qos, retained where
// retain, with a Will Delay of delay seconds and its session kept where
// delay is set: the model's state once try has taken the CONNECT (newWill,
// willHeld). It holds no socket.
func (sc *scene) willSlot(topic string, qos byte, retain bool, delay uint32) (*slot, *will, error) {
	if err := sc.step(); err != nil {
		return nil, nil, err
	}
	s := sc.f.slots[1]
	s.user, s.clientID, s.durable = restricted, s.id, delay > 0
	hw := &harness.Will{QoS: qos, Retain: retain, Topic: fmt.Sprintf(topic, willTag(sc.f.step)), Payload: []byte(willPayload)}
	if delay > 0 {
		hw.Props = harness.Properties{{ID: harness.WillDelayInterval, Int: delay}}
	}
	w := sc.f.newWill(s, &attempt{connect: &harness.Connect{ClientID: s.id, Username: restricted, Will: hw}})
	w.state, s.will = willHeld, w
	return s, w, nil
}

// endWithWill ends s's connection any way but a normal DISCONNECT, its end
// not seen taken (fire): the core may take it later.
func (sc *scene) endWithWill(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	s.fire("its connection ended")
	return nil
}

// reloadWrite is an applied reload giving restricted write on the topics of
// rights[i], or taking it away, as reload follows one: the Wills that may
// still go read before the swap (restrictedWills), the rules swapped, and
// those Wills unsettled after (unsettleWills).
func (sc *scene) reloadWrite(i int, write bool) error {
	if err := sc.step(); err != nil {
		return err
	}
	writable := sc.f.restrictedWills()
	rights[i].write = write
	sc.f.unsettleWills(writable)
	return nil
}

// cleanStart is an accepted CONNECT with Clean Start 1 for s's client id,
// its CONNACK read (try, reconnectWill).
func (sc *scene) cleanStart(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	now := time.Now()
	sc.f.reconnectWill(s, true, true, now, now)
	return nil
}

// forgeWill has the fake publish w as a record of s's client id, stored and
// delivered, as the broker publishes a Will (publishWillOf): the id, not
// the connection's, which a later connection may have ended.
func (sc *scene) forgeWill(s *slot, w *harness.Will) error {
	if err := sc.step(); err != nil {
		return err
	}
	sc.control(func(b *fake) { b.publishWillOf(s.id, w) })
	return nil
}

// waitDue waits until w's earliest due time, from which it may go: a lower
// bound, which no machine's speed breaks.
func waitDue(w *will) func() error {
	return func() error {
		time.Sleep(time.Until(w.notBefore))
		return nil
	}
}

// premiseBroken marks the error of a test whose steps did not end in time.
const premiseBroken = "test premise broken"

// beforeDue fails, as premiseBroken, unless w's earliest due time is still
// to come once what precedes it, what, has ended: a test whose steps must
// end before it fails on a machine too slow for them, never passing
// silently.
func beforeDue(w *will, what string) func() error {
	return func() error {
		if !time.Now().Before(w.notBefore) {
			return fmt.Errorf("%s: %s ended at or after the Will's earliest due time, %s: give it a longer Will Delay",
				premiseBroken, what, w.notBefore.Format(time.RFC3339Nano))
		}
		return nil
	}
}

// premise stops t where err says the test's premise is broken.
func premise(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), premiseBroken) {
		t.Fatal(err)
	}
}

// alteredWill is w as a broker altering its payload alone would store it.
func alteredWill(w *will) *harness.Will {
	stored := *w.w
	stored.Payload = []byte(forged)
	return &stored
}

// ownWill is w as its CONNECT gave it.
func ownWill(w *will) *harness.Will { return w.w }

// deliver has s read and check ps, one after another, as its connection
// would read them from a broker sending them (read).
func deliver(s *slot, ps ...*harness.Publish) error {
	defer func() { s.readPacket = nil }()
	for _, p := range ps {
		s.readPacket = func() (harness.Packet, error) { return p, nil }
		if _, err := s.read(); err != nil {
			return err
		}
	}
	return nil
}

// v311 makes a CONNECT an MQTT 3.1.1 one, whose deliveries of retained
// values carry RETAIN 1 (MQTT-3.3.1-8).
func v311(p *harness.Connect) { p.Version = harness.V311 }

// steps are a history's steps for the Will w of slot s.
type steps func(sc *scene, s *slot, w *will) []func() error

// willHistory is a scene's history of a QoS 1 Will of restricted's on a
// topic of three levels, held by slot 1, with a Will Delay of delay
// seconds, and of slot 0, subscribed to its topics before its connection
// ends: before; then, where record is set, the fake publishing the Will as
// record gives it; then after; then final, an oracle's verdict, slot 0's
// waits bounded by timeout where it is set.
type willHistory struct {
	delay         uint32
	before, after steps
	record        func(w *will) *harness.Will
	timeout       time.Duration
	final         func(sc *scene) error
}

// play plays h, returning its Will and the first error.
func (h willHistory) play(t *testing.T) (w *will, err error) {
	t.Helper()
	err = playWills(t, func(sc *scene) error {
		var s *slot
		var err error
		if s, w, err = sc.willSlot("%s/a/b", 1, false, h.delay); err != nil {
			return err
		}
		sub, err := sc.client(0, "ann", false, false)
		if err != nil {
			return err
		}
		if err := sc.subscribe(sub, harness.Subscription{Filter: w.tag + "/#", QoS: 1, RetainHandling: 2}); err != nil {
			return err
		}
		run := h.before(sc, s, w)
		if h.record != nil {
			run = append(run, func() error { return sc.forgeWill(s, h.record(w)) })
		}
		if h.after != nil {
			run = append(run, h.after(sc, s, w)...)
		}
		if err := do(run...); err != nil {
			return err
		}
		if h.timeout != 0 {
			sub.conn.Timeout = h.timeout
		}
		return h.final(sc)
	})
	return w, err
}

// design.md, DISCONNECT: "The Will is dropped and counted if the acl_file
// no longer allows its topic"; "The Will goes after its Will Delay";
// Access and reload: the acl_file reloads "for every check after the swap,
// including open connections and waiting Wills". A Will without a delay
// whose connection ends after a reload took its topic may be taken by the
// core after a reload gives the topic back; a delayed Will denied so, its
// topic given back, may go once its due time has come while it is allowed,
// before a reload takes the topic again and a clean reconnect follows. The
// record the fake then stores, the Will's own, is accepted once a reload
// gives the topic back, and one whose payload alone differs is refused for
// its payload.
func TestWillDenialAndRestorationKeepItsRecord(t *testing.T) {
	for _, c := range []struct {
		name          string
		delay         uint32
		before, after steps
	}{
		{"at its release", 0, func(sc *scene, s *slot, _ *will) []func() error {
			return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.endWithWill(s) },
				func() error { return sc.reloadWrite(toggled, true) }}
		}, nil},
		{"repeated", 1, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.endWithWill(s) },
				func() error { return sc.reloadWrite(toggled, true) }, waitDue(w)}
		}, func(sc *scene, s *slot, _ *will) []func() error {
			return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.cleanStart(s) },
				func() error { return sc.reloadWrite(toggled, true) }}
		}},
	} {
		for _, altered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, altered %v", c.name, altered), func(t *testing.T) {
				record := ownWill
				if altered {
					record = alteredWill
				}
				_, err := willHistory{delay: c.delay, before: c.before, after: c.after, record: record, final: (*scene).check}.play(t)
				switch {
				case !altered && err != nil:
					t.Errorf("the Will's own record, its topic given back: %v; want it accepted", err)
				case altered && (err == nil || !strings.Contains(err.Error(), payloadMismatch)):
					t.Errorf("a record of the Will whose payload alone differs, its topic given back: %v; want it refused, the record check saying %s", err, payloadMismatch)
				}
			})
		}
	}
}

// design.md, DISCONNECT: "The Will is dropped and counted if the acl_file
// no longer allows its topic or the will feature, or if its channel or the
// retained values' limit is full." CONNECT, 6: "Clean start ends an
// existing session and publishes its delayed Will"; 7: "The core stores
// step 6's changes, the CONNECT's Will and the session record in one
// transaction"; 8: "Once the changes are stored, the broker sends
// CONNACK". A delayed Will is dropped for certain where its channel holds
// its max_bytes when its connection ends, or where a reload took its topic
// before a clean reconnect whose CONNACK the client read, its Will Delay of
// a minute still running: a reload giving the topic back changes nothing. A
// record or a delivery of it is refused, and it is counted neither
// published nor possibly dropped.
func TestWillCertainlyDroppedStaysDropped(t *testing.T) {
	for _, c := range []struct {
		name, topic, filter string
		steps               steps
	}{
		{"its channel full", "ca/s/%s", "ca/s/#", func(sc *scene, s *slot, _ *will) []func() error {
			return []func() error{func() error {
				if err := sc.step(); err != nil {
					return err
				}
				// The bytes small held committed when the step began: its
				// max_bytes (beginStep reads them from the database).
				sc.f.lowerAt = map[string]int64{"small": smallMaxBytes}
				s.fire("its connection ended")
				return nil
			}, func() error { return sc.reloadWrite(smallGrant, false) }, func() error { return sc.cleanStart(s) },
				func() error { return sc.reloadWrite(smallGrant, true) }}
		}},
		{"its topic denied at a clean reconnect", "%s/a/b", "+/a/b", func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.endWithWill(s) }, func() error { return sc.reloadWrite(toggled, false) },
				beforeDue(w, "the reload taking its topic"), func() error { return sc.cleanStart(s) },
				func() error { return sc.reloadWrite(toggled, true) }}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := playWills(t, func(sc *scene) error {
				s, w, err := sc.willSlot(c.topic, 1, false, 60)
				if err != nil {
					return err
				}
				sub, err := sc.client(0, "ann", false, false)
				if err != nil {
					return err
				}
				if err := sc.subscribe(sub, harness.Subscription{Filter: c.filter, QoS: 1, RetainHandling: 2}); err != nil {
					return err
				}
				if err := do(append(c.steps(sc, s, w), func() error { return sc.forgeWill(s, w.w) })...); err != nil {
					return err
				}
				want := fmt.Sprintf("%q", w.w.Topic)
				if err := sc.check(); err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("a record of the Will dropped: %v; want it refused, naming %s", err, want)
				}
				if err := sc.read(sub); err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("a delivery of the Will dropped: %v; want it refused, naming %s", err, want)
				}
				if published, found, notDue, possibleDrops := sc.f.willCounts(); published != 0 || found != 0 || notDue != 0 || possibleDrops != 0 {
					t.Errorf("%d Wills published, %d found stored, %d not yet due, %d possibly dropped; want none: the Will was dropped", published, found, notDue, possibleDrops)
				}
				return nil
			})
			premise(t, err)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// design.md, DISCONNECT: "The Will goes after its Will Delay"; "The Will is
// dropped and counted if the acl_file no longer allows its topic"; Access
// and reload: "Every check after the swap uses the new acl_file"; CONNECT,
// 8: "Once the changes are stored, the broker sends CONNACK". A delayed
// Will whose due time comes while its topic is allowed may go then; a
// reload taking the topic and a clean reconnect after it drop it only if it
// is still there. Its own record, published while the topic was allowed, is
// accepted; one whose payload alone differs is refused for its payload; one
// first published after the clean reconnect's CONNACK, which settled it, is
// refused; and no subscriber is owed it, so a checkpoint at which it never
// comes passes.
func TestWillPossiblyDueBeforeAReloadIsNotCertainlyDropped(t *testing.T) {
	allowed := func(sc *scene, s *slot, w *will) []func() error {
		return []func() error{func() error { return sc.endWithWill(s) }, waitDue(w)}
	}
	denied := func(sc *scene, s *slot, _ *will) []func() error {
		return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.cleanStart(s) }}
	}
	both := func(sc *scene, s *slot, w *will) []func() error {
		// A record stored after the CONNACK carries a later time.
		return append(append(allowed(sc, s, w), denied(sc, s, w)...), nextMillisecond)
	}
	for _, c := range []struct {
		name          string
		before, after steps
		record        func(w *will) *harness.Will
		final         func(sc *scene) error
		want          string // what the refusal says; "" for none, "topic" for one naming the Will's topic
	}{
		{"its record, published while allowed", allowed, denied, ownWill, (*scene).check, ""},
		{"an altered record, published while allowed", allowed, denied, alteredWill, (*scene).check, payloadMismatch},
		{"its record, published after the clean reconnect", both, nil, ownWill, (*scene).check, "topic"},
		{"never published", allowed, denied, nil, (*scene).checkpoint, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			w, err := willHistory{delay: 1, before: c.before, after: c.after, record: c.record, final: c.final}.play(t)
			want := c.want
			if want == "topic" && w != nil {
				want = fmt.Sprintf("%q", w.w.Topic)
			}
			switch {
			case want == "" && err != nil:
				t.Errorf("%s: %v; want it passed", c.name, err)
			case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
				t.Errorf("%s: %v; want it refused, saying %s", c.name, err, want)
			}
		})
	}
}

// design.md, CONNECT, 6: "Clean start ends an existing session and
// publishes its delayed Will"; DISCONNECT: "The Will goes after its Will
// Delay"; Access and reload: "Every check after the swap uses the new
// acl_file". A delayed Will whose topic a reload took over a time at which
// it may have come due, and another gave back, may have been dropped then:
// the clean reconnect that follows publishes it only if it is still there,
// so no subscriber is owed it, and a checkpoint at which it never comes
// passes. Where the topic was taken and given back before its earliest due
// time, or never taken, the clean reconnect publishes it, and a checkpoint
// at which it never comes fails.
func TestWillUncertainAfterAReloadIsNotOwedAtACleanStart(t *testing.T) {
	for _, c := range []struct {
		name  string
		delay uint32
		steps steps
		owed  bool
	}{
		{"its topic denied over a possible due time", 1, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.endWithWill(s) }, func() error { return sc.reloadWrite(toggled, false) },
				waitDue(w), func() error { return sc.reloadWrite(toggled, true) }, func() error { return sc.cleanStart(s) }}
		}, false},
		{"its topic denied before its earliest due time", 60, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.endWithWill(s) }, func() error { return sc.reloadWrite(toggled, false) },
				func() error { return sc.reloadWrite(toggled, true) }, beforeDue(w, "the reload giving its topic back"),
				func() error { return sc.cleanStart(s) }}
		}, true},
		{"no reload", 1, func(sc *scene, s *slot, _ *will) []func() error {
			return []func() error{func() error { return sc.endWithWill(s) }, func() error { return sc.cleanStart(s) }}
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := willHistory{delay: c.delay, before: c.steps, final: (*scene).checkpoint}
			if c.owed {
				h.timeout = 2 * time.Second // the fake never publishes the Will
			}
			_, err := h.play(t)
			premise(t, err)
			switch {
			case !c.owed && err != nil:
				t.Errorf("a checkpoint after the clean reconnect, the Will possibly dropped: %v; want it passed", err)
			case c.owed && (err == nil || !strings.Contains(err.Error(), "still owed at checkpoint")):
				t.Errorf("a checkpoint after the clean reconnect, the Will published: %v; want it failed, the Will still owed", err)
			}
		})
	}
}

// design.md, DISCONNECT: "A Will is published and removed in one
// transaction." A delayed Will whose record the record check has found, or
// a delivery of which a subscriber has had, is published: a reload taking
// its topic, a clean reconnect and a reload giving the topic back cannot
// drop it, and the counts report it published, and found where its record
// was.
func TestWillObservedStaysPublished(t *testing.T) {
	for _, c := range []struct {
		name    string
		observe func(sc *scene, sub *slot) error
		found   int
	}{
		{"its record found", func(sc *scene, _ *slot) error { return sc.check() }, 1},
		{"delivered", func(sc *scene, sub *slot) error { return sc.read(sub) }, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := playWills(t, func(sc *scene) error {
				s, w, err := sc.willSlot("%s/a/b", 1, false, 1)
				if err != nil {
					return err
				}
				sub, err := sc.client(0, "ann", false, false)
				if err != nil {
					return err
				}
				if err := do(func() error {
					return sc.subscribe(sub, harness.Subscription{Filter: w.tag + "/#", QoS: 1, RetainHandling: 2})
				}, func() error { return sc.endWithWill(s) }); err != nil {
					return err
				}
				time.Sleep(time.Until(w.notBefore)) // its Will Delay
				if err := do(func() error { return sc.forgeWill(s, w.w) }, func() error { return c.observe(sc, sub) },
					func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.cleanStart(s) },
					func() error { return sc.reloadWrite(toggled, true) }); err != nil {
					return err
				}
				if published, found, notDue, possibleDrops := sc.f.willCounts(); published != 1 || found != c.found || notDue != 0 || possibleDrops != 0 {
					t.Errorf("%d Wills published, %d found stored, %d not yet due, %d possibly dropped; want 1, %d, 0, 0", published, found, notDue, possibleDrops, c.found)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// design.md, Channels, $broadcast: a record's "current retained value [...]
// applies to at most one record per topic"; SUBSCRIBE, One copy: "a record
// matching several of a session's non-shared subscriptions is sent once";
// Delivery: "While connected, the broker starts no resend". A retained Will
// whose due time comes while its topic is allowed, then denied at a clean
// reconnect after a reload took the topic, and given back by another, may
// have been published as one that was never denied: an MQTT 3.1.1
// subscriber holding one subscription with a retained pass may receive it
// as many times, and the delivery one past the never-denied history's bound
// fails in both.
func TestWillRestoredKeepsTheDeliveryBound(t *testing.T) {
	// accepted plays the history, denied and restored where restored, and
	// returns how many deliveries of the Will the subscriber accepts, the
	// Will live then its retained value again and again, and the refusal
	// that ends them.
	accepted := func(restored bool) (n int, err error) {
		err = playWills(t, func(sc *scene) error {
			s, w, err := sc.willSlot("%s/a/b", 0, true, 1)
			if err != nil {
				return err
			}
			sub := sc.f.slots[0]
			history := []func() error{func() error { return sc.connect(sub, "ann", v311) },
				func() error { return sc.subscribe(sub, harness.Subscription{Filter: w.tag + "/#", QoS: 1}) },
				func() error { return sc.endWithWill(s) }, waitDue(w)}
			if restored {
				history = append(history, func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.cleanStart(s) },
					func() error { return sc.reloadWrite(toggled, true) })
			} else {
				history = append(history, func() error { return sc.cleanStart(s) })
			}
			if err := do(history...); err != nil {
				return err
			}
			if err := deliver(sub, &harness.Publish{Topic: w.w.Topic, Payload: w.w.Payload}); err != nil {
				return err
			}
			value := &harness.Publish{Retain: true, Topic: w.w.Topic, Payload: w.w.Payload}
			for n = 1; n < 1000; n++ {
				if err := deliver(sub, value); err != nil {
					return err
				}
			}
			return fmt.Errorf("%d deliveries of the Will accepted, none refused", n)
		})
		return n, err
	}
	never, err := accepted(false)
	if err == nil || !strings.Contains(err.Error(), "never stops sending") {
		t.Fatalf("the Will never denied: %d deliveries accepted, then %v; want a delivery past the bound refused", never, err)
	}
	restored, err := accepted(true)
	if restored != never || err == nil || !strings.Contains(err.Error(), "never stops sending") {
		t.Errorf("the Will denied and restored: %d deliveries accepted, then %v; want %d, as never denied, then the next refused, the broker never stopping", restored, err, never)
	}
}

// design.md, DISCONNECT: "The Will is dropped and counted if the acl_file
// no longer allows its topic". A retained Will whose topic a reload took
// before its connection ended is never published: an MQTT 3.1.1
// subscriber's live delivery of it, RETAIN 0, and its retained value,
// RETAIN 1 in a retained pass, are each refused.
func TestWillDeniedIsRefusedRetained(t *testing.T) {
	for _, c := range []struct {
		name           string
		subscribeFirst bool
	}{
		{"live", true},
		{"retained", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			var want string
			err := playWills(t, func(sc *scene) error {
				s, w, err := sc.willSlot("%s/a/b", 0, true, 0)
				if err != nil {
					return err
				}
				want = fmt.Sprintf("%q", w.w.Topic)
				sub := sc.f.slots[0]
				subscribe := func() error { return sc.subscribe(sub, harness.Subscription{Filter: w.tag + "/#", QoS: 1}) }
				forge := func() error { return sc.forgeWill(s, w.w) }
				order := []func() error{subscribe, forge}
				if !c.subscribeFirst {
					order = []func() error{forge, subscribe}
				}
				steps := []func() error{func() error { return sc.connect(sub, "ann", v311) },
					func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.endWithWill(s) }}
				if err := do(steps...); err != nil {
					return fmt.Errorf("before the delivery: %w", err)
				}
				if err := do(order...); err != nil {
					return err
				}
				return sc.read(sub)
			})
			if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "before the delivery") {
				t.Errorf("a %s delivery of the Will after its topic was taken: %v; want it refused, naming %s", c.name, err, want)
			}
		})
	}
}

// design.md, Channels: "A retain flag on a configured channel topic is
// ignored (MQTT-3.3.1-5)"; "A hard byte limit counts committed charges and
// reservations for accepted writes"; DISCONNECT: "The Will is dropped and
// counted if the acl_file no longer allows its topic or the will feature,
// or if its channel or the retained values' limit is full". A retained Will
// on small, a channel with a max_bytes, whose connection ends after a
// reload took its topic, the core perhaps taking that end after another
// gave it back, is expected as one never denied: its record with RETAIN 0
// is accepted and one with RETAIN 1 refused, and a PUBLISH on small that
// fits alone but not beside it, predicted before the Will is stored and
// sent after, may be refused for room.
func TestWillRestoredOnAChannelIsExpectedAsAnAllowedOne(t *testing.T) {
	for _, restored := range []bool{false, true} {
		for _, retained := range []bool{false, true} {
			t.Run(fmt.Sprintf("restored %v, record retained %v", restored, retained), func(t *testing.T) {
				err := playWills(t, func(sc *scene) error {
					s, w, err := sc.willSlot("ca/s/%s", 1, true, 0)
					if err != nil {
						return err
					}
					pub, err := sc.client(0, "ann", false, false)
					if err != nil {
						return err
					}
					end := []func() error{func() error { return sc.endWithWill(s) }}
					if restored {
						end = []func() error{func() error { return sc.reloadWrite(smallGrant, false) }, end[0],
							func() error { return sc.reloadWrite(smallGrant, true) }}
					}
					if err := do(end...); err != nil {
						return err
					}
					if retained {
						// The broker's record, its published_retained 1.
						if err := sc.forgeWill(s, w.w); err != nil {
							return err
						}
						sc.control(func(b *fake) {
							_, err = b.db.Exec(`UPDATE records SET published_retained = 1 WHERE "offset" = ?`, b.offset)
						})
						if err != nil {
							return err
						}
						return sc.check()
					}
					if err := sc.step(); err != nil {
						return err
					}
					o, err := sc.newOut(pub, "ca/s/x", 1, false)
					if err != nil {
						return err
					}
					room := smallMaxBytes - len(o.p.Topic) - len(o.m.tag) - 1 // the payload's tag and space
					if err := sc.f.fill(o, sc.n, room, room, letters, harness.V5); err != nil {
						return err
					}
					return do(func() error { return sc.f.placeAll([]*out{o}) }, func() error { sc.f.expect(o); return sc.forgeWill(s, w.w) },
						func() error { return send(pub, o) }, func() error { return collect(pub) }, func() error { return pingPast(pub) }, sc.check)
				})
				switch {
				case !retained && err != nil:
					t.Errorf("the Will's record with RETAIN 0, and a PUBLISH refused for the room it took: %v; want both accepted", err)
				case retained && (err == nil || !strings.Contains(err.Error(), "published_retained true; want")):
					t.Errorf("the Will's record with RETAIN 1 on a configured channel: %v; want it refused for its published_retained", err)
				}
			})
		}
	}
}

// endSeen ends s's connection any way but a normal DISCONNECT, its end
// seen taken (fireTaken): the departure observed, bounding the latest due
// time of a delayed Will.
func (sc *scene) endSeen(s *slot) error {
	if err := sc.step(); err != nil {
		return err
	}
	s.fireTaken("its connection dropped")
	return nil
}

// waitLatest waits until w's latest due time, by which it has gone: a lower
// bound, which no machine's speed breaks.
func waitLatest(w *will) func() error {
	return func() error {
		time.Sleep(time.Until(w.owedAfter))
		return nil
	}
}

// nextMillisecond waits a few milliseconds, so that a record stored next carries a
// later time, to the millisecond, than what came before (storage.md,
// records: time).
func nextMillisecond() error {
	time.Sleep(10 * time.Millisecond)
	return nil
}

// stopHoldingNone has the fake stop as a broker whose wills table holds
// none of the run's Wills (storage.md, wills, as far as the run reads it),
// the broker having exited 0: the record checks then check the Wills
// against it (checkWillsAfterStop).
func (sc *scene) stopHoldingNone() error {
	if err := sc.step(); err != nil {
		return err
	}
	var err error
	sc.control(func(b *fake) { _, err = b.db.Exec(`CREATE TABLE wills (topic TEXT NOT NULL)`) })
	sc.f.stopped = true
	return err
}

// design.md, CONNECT, 6: "Clean start ends an existing session and
// publishes its delayed Will"; 7: "The core stores step 6's changes, the
// CONNECT's Will and the session record in one transaction"; 8: "Once the
// changes are stored, the broker sends CONNACK"; Channels, $broadcast:
// "Every message goes in, whatever its QoS"; MQTT 5 4.5: the server "MUST
// add it to the Session State for those Clients that have matching
// Subscriptions [MQTT-4.5.0-1]". A delayed QoS 1 Will, its departure not
// seen, goes once its due time has come, stored, its delivery to a
// subscriber of its topics withheld (the fake's lostDelivery, its next
// QoS 1 delivery lost); the record check finds it; a clean reconnect whose
// CONNACK the client reads then proves it published before. It is owed to
// the subscriber, so a checkpoint at which it never comes fails.
func TestWillObservedThenSettledIsOwed(t *testing.T) {
	lost := func(sc *scene, s *slot, w *will) func() error {
		return func() error {
			sc.control(func(b *fake) { b.fault, b.lost = lostDelivery, 3 })
			err := sc.forgeWill(s, w.w)
			sc.control(func(b *fake) { b.fault = "" })
			return err
		}
	}
	_, err := willHistory{delay: 1, before: func(sc *scene, s *slot, w *will) []func() error {
		return []func() error{func() error { return sc.endWithWill(s) }, waitDue(w), lost(sc, s, w), sc.check,
			func() error { return sc.cleanStart(s) }}
	}, timeout: 2 * time.Second, final: (*scene).checkpoint}.play(t)
	if err == nil || !strings.Contains(err.Error(), "still owed at checkpoint") {
		t.Errorf("a checkpoint after the clean reconnect, the Will found stored and never delivered: %v; want it failed, the Will still owed", err)
	}
}

// design.md, DISCONNECT: "The Will goes after its Will Delay"; "The Will is
// dropped and counted if the acl_file no longer allows its topic"; Restart:
// the broker restores "Wills with their due time"; Graceful stop: "Expired
// sessions and due Wills are handled at the next start". A delayed Will
// whose topic a reload took before its connection ended, or after but
// before its Will Delay ended, its departure seen, has been dropped once
// its latest due time has passed: a stopped broker holding neither it nor
// its record has lost nothing, and the final check passes. One stopped
// before its Will Delay ends is still owed to the restart: a stopped broker
// holding neither has lost it, and the final check fails.
func TestWillDeniedAfterItsDelayIsNoLostData(t *testing.T) {
	for _, c := range []struct {
		name  string
		delay uint32
		steps steps
		lost  bool
	}{
		{"its topic taken before its release", 1, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.endSeen(s) }, waitLatest(w)}
		}, false},
		{"its topic taken during its Will Delay", 1, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.endSeen(s) }, func() error { return sc.reloadWrite(toggled, false) },
				beforeDue(w, "the reload taking its topic"), waitLatest(w)}
		}, false},
		{"stopped during its Will Delay", 60, func(sc *scene, s *slot, w *will) []func() error {
			return []func() error{func() error { return sc.reloadWrite(toggled, false) }, func() error { return sc.endSeen(s) },
				beforeDue(w, "the stop")}
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := playWills(t, func(sc *scene) error {
				s, w, err := sc.willSlot("%s/a/b", 1, false, c.delay)
				if err != nil {
					return err
				}
				return do(append(c.steps(sc, s, w), sc.stopHoldingNone, sc.check)...)
			})
			premise(t, err)
			switch {
			case !c.lost && err != nil:
				t.Errorf("the final check, the Will dropped after its Will Delay: %v; want it passed", err)
			case c.lost && (err == nil || !strings.Contains(err.Error(), "neither published nor held")):
				t.Errorf("the final check, the Will owed to the restart and not held: %v; want it failed, the Will neither published nor held", err)
			}
		})
	}
}

// design.md, CONNECT, 5: "A delayed Will is cancelled only by reconnection
// to the still-existing session before its due time, judged when the core
// takes the CONNECT. A CONNECT taken at or after the due time is too late";
// 8: "Once the changes are stored, the broker sends CONNACK". A CONNECT
// keeping the session, sent after the delayed Will's latest due time, its
// departure seen, finds the Will gone or going: its CONNACK read, the Will
// has been published or dropped, and a record of it first stored after
// that CONNACK is refused.
func TestWillRecordAfterALateReconnectIsRefused(t *testing.T) {
	w, err := willHistory{delay: 1, before: func(sc *scene, s *slot, w *will) []func() error {
		return []func() error{func() error { return sc.endSeen(s) }, waitLatest(w),
			func() error { return sc.connect(s, restricted, resumed) }, nextMillisecond}
	}, record: ownWill, final: (*scene).check}.play(t)
	if w == nil || err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", w.w.Topic)) {
		t.Errorf("a record of the Will first stored after the CONNACK of a late reconnect: %v; want it refused, naming the Will", err)
	}
}

// design.md, CONNECT, 6: "Clean start ends an existing session and
// publishes its delayed Will"; 7: "The core stores step 6's changes, the
// CONNECT's Will and the session record in one transaction"; 8: "Once the
// changes are stored, the broker sends CONNACK"; DISCONNECT, Reasons Sagüin
// sends: "Sagüin uses 0x80 when it cannot build a packet; the packet it
// could not build is never sent". An accepted clean CONNECT whose CONNACK
// is over the client's Maximum Packet Size, the broker closing the
// connection, has stored its changes, the delayed Will's publication
// included, before the reply it could not send: a record of the Will first
// stored after that close is refused.
func TestWillRecordAfterAnUnsendableConnackIsRefused(t *testing.T) {
	tiny := func(p *harness.Connect) {
		p.Props = append(p.Props, harness.Property{ID: harness.MaximumPacketSize, Int: smallestConnack - 1})
	}
	w, err := willHistory{delay: 1, before: func(sc *scene, s *slot, _ *will) []func() error {
		return []func() error{func() error { return sc.endWithWill(s) }, func() error { return sc.connect(s, restricted, tiny) }, nextMillisecond}
	}, record: ownWill, final: (*scene).check}.play(t)
	if w == nil || err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", w.w.Topic)) {
		t.Errorf("a record of the Will first stored after the close of a clean CONNECT whose CONNACK could not be sent: %v; want it refused, naming the Will", err)
	}
}

// design.md, DISCONNECT: "A Will is published and removed in one
// transaction"; SUBSCRIBE, One copy: "Where a permitted eligible
// subscription owes the record, it can supply the one copy"; Channels,
// $broadcast: "Every message goes in, whatever its QoS"; MQTT 5 4.5: the
// server "MUST add it to the Session State for those Clients that have
// matching Subscriptions [MQTT-4.5.0-1]". Two subscribers to its topics
// precede a delayed QoS 1 Will's release, its departure seen; a reload
// takes its topic over its due times and another gives it back, so it may
// have been dropped. After a record check finds nothing, the broker
// publishes it during a checkpoint's reads, storing it and delivering it to
// one subscriber, or to both: one delivery proves it published, so it is
// owed to the other, and a checkpoint, or the record check and checkpoint
// after it, fails while that subscriber never receives it, whichever of the
// two the checkpoint visits first. Where both receive it, they pass.
func TestWillProvedPublishedAtACheckpointIsOwedToEach(t *testing.T) {
	for _, c := range []struct {
		name      string
		delivered []int // the slots the broker delivers the Will to
		withheld  int   // the slot it never reaches, -1 for none
	}{
		{"the delivered subscriber visited first", []int{0}, 2},
		{"the delivered subscriber visited second", []int{2}, 0},
		{"both delivered", []int{0, 2}, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			var want string
			err := playWills(t, func(sc *scene) error {
				s, w, err := sc.willSlot("%s/a/b", 1, false, 1)
				if err != nil {
					return err
				}
				subs := map[int]*slot{}
				for _, i := range []int{0, 2} {
					if subs[i], err = sc.client(i, "ann", false, false); err != nil {
						return err
					}
					if err := sc.subscribe(subs[i], harness.Subscription{Filter: w.tag + "/#", QoS: 1, RetainHandling: 2}); err != nil {
						return err
					}
				}
				if err := do(func() error { return sc.endSeen(s) }, func() error { return sc.reloadWrite(toggled, false) }, waitLatest(w),
					func() error { return sc.reloadWrite(toggled, true) }, sc.check); err != nil {
					return err
				}
				stored := false
				for _, i := range c.delivered {
					sub := subs[i]
					sent := false
					sub.readPacket = func() (harness.Packet, error) {
						if sent {
							return sub.conn.Recv()
						}
						sent = true
						if !stored {
							// The broker's Will publication, its record first.
							stored = true
							sc.control(func(b *fake) {
								b.store(s.id, w.w.Topic, &harness.Publish{QoS: w.w.QoS, Topic: w.w.Topic, Payload: w.w.Payload}, true)
							})
						}
						return &harness.Publish{QoS: w.w.QoS, ID: 1, Topic: w.w.Topic, Payload: w.w.Payload}, nil
					}
				}
				defer func() {
					for _, sub := range subs {
						sub.readPacket = nil
					}
				}()
				if c.withheld >= 0 {
					want = fmt.Sprintf("%s: publication %s on %q still owed", subs[c.withheld].id, w.tag, w.w.Topic)
					subs[c.withheld].conn.Timeout = 2 * time.Second // the broker never sends it
				}
				return do(sc.checkpoint, sc.check, sc.checkpoint)
			})
			switch {
			case want == "" && err != nil:
				t.Errorf("the checkpoints, both subscribers delivered the Will: %v; want them passed", err)
			case want != "" && (err == nil || !strings.Contains(err.Error(), want)):
				t.Errorf("the checkpoints, the Will proved published and one subscriber never delivered it: %v; want them failed, saying %s", err, want)
			}
		})
	}
}
