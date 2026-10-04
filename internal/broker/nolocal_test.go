package broker_test

import (
	"context"
	"net"
	"testing"
	"time"

	"path/filepath"

	pahopackets "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
)

// No Local [MQTT-3.8.3-3]: a record is not forwarded to a connection whose
// client id equals the publishing connection's.
//
// **Every one of these is about a channel**, which is where saguin answered
// the flag by ignoring it. Broadcast was always right - the substrate
// compares the packet's origin against the client it is writing to - and a
// channel delivery is a read from the store, minutes or a restart after the
// publishing connection has gone, so what it compares is the publisher the
// record carries.
//
// **Ignoring a flag is silent, which is why these assert what arrived rather
// than a code.** Nothing in MQTT reports a server that granted No Local and
// then did not honour it: the SUBACK said `0x01`, and the subscriber simply
// received its own records.

// subNoLocal subscribes with the flag and returns the granted code.
func subNoLocal(t *testing.T, c *client, filter string, qos byte) byte {
	t.Helper()
	ack, err := c.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos, NoLocal: true}},
	})
	if err != nil {
		t.Fatalf("subscribe %q with No Local: %v", filter, err)
	}
	return ack.Reasons[0]
}

// A publisher gets everybody else's records on the channel it subscribed to
// with No Local, and none of its own - live, and on replay.
func TestNoLocalWithholdsAPublishersOwnRecordsOnAnAppendChannel(t *testing.T) {
	h := start(t)

	// Published before anybody subscribes, so the withholding below is
	// decided on the replay path rather than on a live delivery.
	early := connect(t, h, "listener", true, false)
	early.Pub(t, "events/early", "listener-early")
	other := connect(t, h, "other", true, false)
	other.Pub(t, "events/other-early", "other-early")

	if code := subNoLocal(t, early, "events/#", 1); code != 1 {
		t.Fatalf("the subscription was granted 0x%02X, want 0x01 - No Local is not a "+
			"thing a server may refuse on an ordinary filter", code)
	}

	// And live, after the subscription exists.
	early.Pub(t, "events/late", "listener-late")
	other.Pub(t, "events/other-late", "other-late")

	awaitCount(t, early, 2, 5*time.Second)
	got := payloads(early.All())
	want := []string{"other-early", "other-late"}
	if len(got) != len(want) {
		t.Fatalf("the No Local subscriber holds %v, want exactly %v - its own records "+
			"are withheld on replay and live alike, and everybody else's are not", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("the No Local subscriber holds %v, want %v", got, want)
		}
	}
}

// The same client, two subscriptions, one of which did not ask: the record
// is owed to the one that did not.
//
// **This is why the flag is read off the filter that matched rather than off
// the consumer.** A channel consumer holds one position, so a check written
// per client rather than per subscription would take the record away from
// the subscription that never asked for anything.
func TestNoLocalIsReadOffTheFilterThatMatched(t *testing.T) {
	h := start(t)
	c := connect(t, h, "both-ways", true, false)
	if code := subNoLocal(t, c, "events/quiet/#", 1); code != 1 {
		t.Fatalf("granted 0x%02X", code)
	}
	c.Sub(t, "events/#", 1)

	c.Pub(t, "events/quiet/one", "mine")
	awaitCount(t, c, 1, 3*time.Second)
	if got := payloads(c.All()); len(got) != 1 || got[0] != "mine" {
		t.Errorf("the publisher holds %v, want one copy of its own record: one of its "+
			"two subscriptions reaches it and did not ask for No Local, and a record "+
			"withheld from that one is a record nothing reports missing", got)
	}
}

// A `latest` channel answers it on both of its halves: the live change and
// the current-state pass a subscriber is given when it subscribes.
//
// The two are different code - one pushes from the publisher's goroutine,
// the other reads the whole of current state from the store - so a test of
// one says nothing about the other.
func TestNoLocalWithholdsAPublishersOwnValuesOnALatestChannel(t *testing.T) {
	h := start(t)
	mine := connect(t, h, "thermostat", true, false)
	other := connect(t, h, "other", true, false)

	// Current state first, so the subscribe-time pass has something of each
	// to decide about.
	mine.Pub(t, "state/hallway", "mine-before")
	other.Pub(t, "state/kitchen", "other-before")
	settle(t, other)

	if code := subNoLocal(t, mine, "state/#", 1); code != 1 {
		t.Fatalf("granted 0x%02X", code)
	}
	awaitCount(t, mine, 1, 3*time.Second)
	if got := payloads(mine.All()); len(got) != 1 || got[0] != "other-before" {
		t.Fatalf("the current-state pass gave %v, want only other-before - a value this "+
			"subscriber published is one it asked not to be sent", got)
	}

	// Then the live half.
	mine.Pub(t, "state/hallway", "mine-after")
	other.Pub(t, "state/kitchen", "other-after")
	awaitCount(t, mine, 2, 3*time.Second)
	if got := payloads(mine.All()); len(got) != 2 || got[1] != "other-after" {
		t.Errorf("after the live changes the subscriber holds %v, want other-before then "+
			"other-after", got)
	}
}

// A retained value on a broadcast topic is served on every subscribe, so the
// publisher that set it would be handed its own value back once per
// reconnect for as long as the value stands.
func TestNoLocalWithholdsAPublishersOwnRetainedValue(t *testing.T) {
	// A broker with a retained store, which the default harness has not:
	// without one a retained publish to a broadcast topic is refused, and
	// there would be no retained pass to assert anything about.
	h := startRetaining(t, 0)
	mine := connect(t, h, "sensor", true, false)
	other := connect(t, h, "other", true, false)
	mine.PubRetained(t, "weather/hallway", "mine")
	other.PubRetained(t, "weather/roof", "theirs")
	settle(t, other)

	if code := subNoLocal(t, mine, "weather/#", 1); code != 1 {
		t.Fatalf("granted 0x%02X", code)
	}
	awaitCount(t, mine, 1, 3*time.Second)
	if got := payloads(mine.All()); len(got) != 1 || got[0] != "theirs" {
		t.Errorf("the retained pass gave %v, want only theirs - its own retained value "+
			"would come back on every reconnect for as long as it stands", got)
	}
}

// A record's publisher survives a restart, because a channel replays from
// the store and the connection that published is long gone by then.
func TestAPublishersIdentitySurvivesARestart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start func(t *testing.T, where string) *harness
		where func(t *testing.T) string
	}{
		{"snapshots", func(t *testing.T, d string) *harness { return startDurable(t, d) },
			func(t *testing.T) string { return t.TempDir() }},
		{"sqlite", func(t *testing.T, p string) *harness { return startDurableSQLite(t, p) },
			func(t *testing.T) string { return filepath.Join(t.TempDir(), "s.db") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			where := tc.where(t)
			h := tc.start(t, where)
			p := connect(t, h, "durable-publisher", true, false)
			p.Pub(t, "events/mine", "mine")
			other := connect(t, h, "other", true, false)
			other.Pub(t, "events/theirs", "theirs")
			settle(t, other)
			p.Close()
			other.Close()

			h.Stop()
			h2 := tc.start(t, where)

			back := connect(t, h2, "durable-publisher", true, false)
			if code := subNoLocal(t, back, "events/#", 1); code != 1 {
				t.Fatalf("granted 0x%02X", code)
			}
			awaitCount(t, back, 1, 5*time.Second)
			if got := payloads(back.All()); len(got) != 1 || got[0] != "theirs" {
				t.Errorf("after a restart the No Local subscriber holds %v, want only "+
					"theirs - the publishing connection is gone, so the record is the "+
					"only thing that can say who wrote it", got)
			}
		})
	}
}

// RFC 0003 "No Local": a Will is published as the client that armed it, so
// a No Local subscription of that client's is not sent its own Will - on a
// channel, on a broadcast topic, retained, and live to the connection that
// took its client id over. mosquitto attributes a Will to its client the
// same way.
//
// Measured before: every Will carried the in-process publisher's id, and
// every arm below delivered the device its own Will. The watcher, which
// did not ask for No Local, is what proves each Will was published at all.
func TestNoLocalWithholdsAClientsOwnWill(t *testing.T) {
	for _, tc := range []struct {
		name, topic, filter string
		retain, takeover    bool
	}{
		{"append", "events/dev", "events/#", false, false},
		{"latest", "state/dev", "state/#", false, false},
		{"broadcast", "loose/dev", "loose/#", false, false},
		{"broadcast, retained", "weather/dev", "weather/#", true, false},
		{"broadcast, live to the takeover", "loose/dev", "loose/#", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h *harness
			if tc.retain {
				h = startRetaining(t, 0)
			} else {
				h = start(t)
			}
			w := connect(t, h, "watcher", true, false)
			w.Sub(t, tc.filter, 1)

			raw, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { _ = raw.Close() })
			c := paho.NewClient(paho.ClientConfig{Conn: pahopackets.NewThreadSafeConn(raw)})
			expiry, delay := uint32(3600), uint32(0)
			if _, err := c.Connect(context.Background(), &paho.Connect{ClientID: "dev", CleanStart: true,
				KeepAlive: 60, Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
				WillMessage: &paho.WillMessage{Topic: tc.topic, QoS: 1, Retain: tc.retain,
					Payload: []byte("dev-died")},
				WillProperties: &paho.WillProperties{WillDelayInterval: &delay}}); err != nil {
				t.Fatalf("connect dev: %v", err)
			}
			ack, err := c.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{{Topic: tc.filter, QoS: 1, NoLocal: true}}})
			if err != nil || ack.Reasons[0] != 1 {
				t.Fatalf("dev's No Local subscription: %v, %v", ack, err)
			}

			// A resume taking the id over publishes the old connection's
			// Will at once [MQTT-3.1.3-9], with the new one connected; a cut
			// publishes it with nobody there, and the device resumes after.
			var back *client
			if tc.takeover {
				back = connect(t, h, "dev", false, false)
			} else {
				_ = raw.Close()
			}
			if r, ok := w.Await(t, 3*time.Second); !ok || r.Payload != "dev-died" {
				t.Fatalf("the watcher was sent %v (%v), want dev's Will - it never fired", r, ok)
			}
			if back == nil {
				back = connect(t, h, "dev", false, false)
			}
			// Subscribed again, so the retained and current-state passes are
			// asked as well as the delivery the session was owed.
			if code := subNoLocal(t, back, tc.filter, 1); code != 1 {
				t.Fatalf("granted 0x%02X", code)
			}
			if r, ok := back.Await(t, 2*time.Second); ok {
				t.Errorf("dev's No Local subscription was sent its own Will: %q on %s", r.Payload, r.Topic)
			}
		})
	}
}

// **No Local on a queue is refused rather than honoured**, and the refusal
// is the point: withholding a job from the worker whose client id published
// it leaves that job undeliverable while a worker holds the queue open -
// never acknowledged, never timed out, never dead-lettered, and nothing
// reporting it. That is [MQTT-3.8.3-4]'s own reasoning for making the flag a
// protocol error on a shared subscription.
func TestNoLocalOnAQueueIsRefused(t *testing.T) {
	h := start(t)
	w := connect(t, h, "worker", true, true)
	ack, err := w.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "$saguin/queue/jobs", QoS: 1, NoLocal: true}},
	})
	if err == nil && ack.Reasons[0] < 0x80 {
		t.Fatalf("a queue subscription asking for No Local was granted 0x%02X", ack.Reasons[0])
	}
	if ack == nil || ack.Reasons[0] != 0x8F {
		t.Errorf("refused with %v, want 0x8F naming the filter to change", ack)
	}

	// The connection stands, unlike the shared case below: this is saguin's
	// own form and its own rule, so the client is told which filter to fix
	// rather than being disconnected.
	w.Pub(t, "jobs/one", "work")

	// And the job is deliverable, which is the whole reason for refusing:
	// subscribed without the flag, the worker that published it is offered it.
	w.Sub(t, "$saguin/queue/jobs", 1)
	awaitCount(t, w, 1, 5*time.Second)
	if got := payloads(w.All()); len(got) != 1 || got[0] != "work" {
		t.Errorf("the worker holds %v, want the job it published - a job withheld here "+
			"is one nothing ever reports", got)
	}
}

// No Local on a shared subscription is a Protocol Error [MQTT-3.8.3-4], so
// the connection goes with 0x82 rather than the SUBACK carrying a code no
// SUBACK may carry (section 3.9.3).
func TestNoLocalOnASharedSubscriptionDisconnects(t *testing.T) {
	h := start(t)
	c := connect(t, h, "sharer", true, false)
	_, _ = c.C.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: "$share/g/events/#", QoS: 1, NoLocal: true}},
	})
	// The refusal is the disconnect, and it is what the client can act on:
	// answered in a SUBACK alone it was a bare failure with no readable
	// reason, on a connection that went on working.
	sessionGone(t, h, "sharer")
}
