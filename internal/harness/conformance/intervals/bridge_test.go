package intervals

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of bridge links over TCP whose waits run to seconds and more
// (design.md, Bridges): the redial waits, keep-alive, subscription retries,
// the unanswered SUBSCRIBE and the log lines of a link's life. Each starts
// a broker of its own with a bridge to a test peer, harness.Peer, which
// records what the bridge sends with its own receipt times and answers as
// the test scripts; each bound is read from those times, a lower and an
// upper bound with room.

// room is what a bound read from the peer's receipt times allows for: the
// loopback, and the broker taking the event that starts or ends the wait.
const room = time.Second

// early is what a lower bound allows for: the peer's clock reads a send
// before the broker can take it, and a receipt after the broker's write.
const early = 50 * time.Millisecond

// refusing is the CONNACK refusing a bridge, Not authorized: a failed
// establishment.
var refusing = &harness.Connack{Reason: 0x87}

// refuse answers l's CONNECT with refusing, then closes the link, as a
// server does after a refusing CONNACK, and returns when the CONNACK was
// sent.
func refuse(t *testing.T, l *harness.PeerLink) time.Time {
	t.Helper()
	_, _, sent := l.Accept(t, refusing)
	_ = l.Close() // the peer closes after its refusal
	return sent
}

// backoff is the most a redial or a subscription retry waits after failure
// n, from 1: design.md, Bridges, "min(60,000, 5,000 × 2^(n−1))"
// milliseconds.
func backoff(n int) time.Duration {
	return min(time.Minute, 5*time.Second<<(n-1))
}

// TestBridgeRedialWaits: design.md, Bridges: "The first redial after failed
// establishment or link loss is immediate. After redial failure n, from 1,
// the wait is a uniformly drawn whole number of milliseconds from 1,000
// through min(60,000, 5,000 × 2^(n−1)), inclusive. The calculation
// saturates before overflow. Successful CONNACK resets n."
//
// The peer refuses the bridge's first dial: the redial reaches it at once.
// It refuses that one too, redial failure 1: the next comes 1 to 5 seconds
// after the refusal. It refuses that, failure 2: the next comes 1 to 10
// seconds after. It accepts that one, then drops the link: the redial
// comes at once. It refuses that, failure 1 again since the CONNACK reset
// n: the next comes 1 to 5 seconds after.
func TestBridgeRedialWaits(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("redialling", peer, support.In("a/#", 1))}})

	refused := refuse(t, peer.Link(t))
	l := peer.Link(t)
	support.Between(t, "the first redial after a failed establishment", l.Accepted.Sub(refused), 0, room)
	for n := 1; n <= 2; n++ {
		refused = refuse(t, l)
		l = peer.LinkWithin(t, backoff(n)+harness.DefaultTimeout)
		support.Between(t, fmt.Sprintf("the redial after redial failure %d", n), l.Accepted.Sub(refused), time.Second-early, backoff(n)+room)
	}
	l.Accept(t, support.Quiet)
	harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	dropped := time.Now()
	_ = l.Close()
	l = peer.Link(t)
	support.Between(t, "the first redial after the link's loss", l.Accepted.Sub(dropped), 0, room)
	refused = refuse(t, l)
	l = peer.LinkWithin(t, backoff(1)+harness.DefaultTimeout)
	support.Between(t, "the redial after redial failure 1, n reset by the CONNACK", l.Accepted.Sub(refused), time.Second-early, backoff(1)+room)
}

// TestBridgeKeepAlive: design.md, Bridges: "A bridge advertises Keep Alive
// 30 seconds. A Server Keep Alive replaces it. For a nonzero effective
// interval, PINGREQ is due after half that interval without a completed
// outgoing control packet. Failure to receive PINGRESP within half the
// interval after the PINGREQ is written ends the link. An effective
// interval of zero disables these keep-alive timers."; MQTT 5 3.2.2.3.14:
// "If the Server sends a Server Keep Alive on the CONNACK packet, the Client
// MUST use this value instead of the Keep Alive value the Client sent on
// CONNECT [MQTT-3.2.2-21]."
func TestBridgeKeepAlive(t *testing.T) {
	t.Parallel()
	const serverKeepAlive = 2 * time.Second
	keepAlive := &harness.Connack{Props: harness.Properties{{ID: harness.ServerKeepAlive, Int: uint32(serverKeepAlive / time.Second)}}}
	half := serverKeepAlive / 2

	// The peer's CONNACK carries Server Keep Alive 2 and it answers every
	// PINGREQ: the first comes a second after the SUBSCRIBE, the bridge's
	// last outgoing packet, and the next a second after it.
	t.Run("a PINGREQ every half of the Server Keep Alive", func(t *testing.T) {
		t.Parallel()
		peer := harness.ListenedPeer(t)
		support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("pinging", peer, support.In("a/#", 1))}})
		l := peer.Link(t)
		l.Accept(t, keepAlive)
		l.AnswerPings(true)
		s, subscribed := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
		if _, err := l.Send(support.Grants(s)); err != nil {
			t.Fatalf("granting the SUBSCRIBE: %v", err)
		}
		_, first := harness.Arrive[*harness.Pingreq](t, l, serverKeepAlive+harness.DefaultTimeout)
		support.Between(t, "the first PINGREQ after the SUBSCRIBE", first.At.Sub(subscribed.At), half-early, half+room)
		_, second := harness.Arrive[*harness.Pingreq](t, l, serverKeepAlive+harness.DefaultTimeout)
		support.Between(t, "the next PINGREQ after the first", second.At.Sub(first.At), half-early, half+room)
	})

	// The same, but the peer never answers a PINGREQ: the link ends a
	// second after the first.
	t.Run("a PINGREQ unanswered ends the link", func(t *testing.T) {
		t.Parallel()
		peer := harness.ListenedPeer(t)
		support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("unanswered", peer, support.In("a/#", 1))}})
		l, _, _ := support.LinkUp(t, peer, keepAlive)
		_, pinged := harness.Arrive[*harness.Pingreq](t, l, serverKeepAlive+harness.DefaultTimeout)
		end, _ := support.Ending(t, l, serverKeepAlive+harness.DefaultTimeout)
		support.Between(t, "the link's end after an unanswered PINGREQ", end.At.Sub(pinged.At), half-early, half+room)
	})

	// A broker has two bridges. One peer's CONNACK carries no Server Keep
	// Alive, so the advertised 30 seconds is the interval: its first PINGREQ
	// comes 15 seconds after its SUBSCRIBE. The other's carries Server Keep
	// Alive 0: once the first has pinged, at SIGTERM, the packet that
	// follows its SUBSCRIBE is the stop's DISCONNECT, no PINGREQ before it.
	t.Run("the advertised 30 seconds, and zero", func(t *testing.T) {
		t.Parallel()
		const advertised = 30 * time.Second
		thirty, zero := harness.ListenedPeer(t), harness.ListenedPeer(t)
		b := support.Launch(t, &harness.Options{Bridges: []harness.Bridge{
			support.BridgeTo("thirty", thirty, support.In("a/#", 1)),
			support.BridgeTo("zero", zero, support.In("a/#", 1)),
		}})
		long := thirty.Link(t)
		long.Accept(t, &harness.Connack{})
		long.AnswerPings(true)
		s, subscribed := harness.Arrive[*harness.Subscribe](t, long, harness.DefaultTimeout)
		if _, err := long.Send(support.Grants(s)); err != nil {
			t.Fatalf("granting the SUBSCRIBE: %v", err)
		}
		off, _, _ := support.LinkUp(t, zero, &harness.Connack{Props: harness.Properties{{ID: harness.ServerKeepAlive, Int: 0}}})
		_, pinged := harness.Arrive[*harness.Pingreq](t, long, advertised)
		support.Between(t, "the first PINGREQ under the advertised Keep Alive", pinged.At.Sub(subscribed.At), advertised/2-early, advertised/2+room)
		if err := b.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("SIGTERM: %v", err)
		}
		if d, _ := harness.Arrive[*harness.Disconnect](t, off, support.StopLimit); d.Reason != 0x00 {
			t.Errorf("the zero link's DISCONNECT at the stop has reason 0x%02X; want 0x00", d.Reason)
		}
	})
}

// TestBridgeAsksRefusedFiltersAgainAlone: design.md, Bridges, In: "If the
// peer refuses filters, the bridge asks again for those filters alone, in
// as few SUBSCRIBEs as fit the peer's Maximum Packet Size, on the same link
// after the redial wait above. There, n counts the bridge's consecutive
// refusals (Bridges)."; "Subscription-refusal retries have a separate
// counter. It resets once every filter a link asks for is granted, after
// one SUBACK or after several."; "Every new
// link subscribes all its filters afresh."
//
// A bridge has in rules on a/#, b/+ and c. On its first link the peer
// grants a/# and refuses b/+ and c: 1 to 5 seconds after that SUBACK, the
// link's next packet is a SUBSCRIBE of b/+ and c alone, asked as the first
// asked them. The peer grants b/+ and refuses c, the second refusal in a
// row: 1 to 10 seconds after, a SUBSCRIBE of c alone. The peer grants it,
// every filter granted, and drops the link. The next link's SUBSCRIBE
// carries all three again; the peer refuses a/#, a first refusal since the
// grant: 1 to 5 seconds after, a SUBSCRIBE of a/# alone.
func TestBridgeAsksRefusedFiltersAgainAlone(t *testing.T) {
	t.Parallel()
	peer := harness.ListenedPeer(t)
	support.Launch(t, &harness.Options{Bridges: []harness.Bridge{
		support.BridgeTo("retrying", peer, support.In("a/#", 1), support.In("b/+", 1), support.In("c", 1)),
	}})

	l := peer.Link(t)
	l.Accept(t, support.Quiet)
	first, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	asked := make(map[string]harness.Subscription, len(first.Subs))
	for _, sub := range first.Subs {
		asked[sub.Filter] = sub
	}
	// answer refuses refused among s's filters with 0x87, grants the rest,
	// and returns when the SUBACK was sent.
	answer := func(l *harness.PeerLink, s *harness.Subscribe, refused ...string) time.Time {
		t.Helper()
		ack := support.Grants(s)
		for i, sub := range s.Subs {
			if slices.Contains(refused, sub.Filter) {
				ack.Reasons[i] = 0x87
			}
		}
		sent, err := l.Send(ack)
		if err != nil {
			t.Fatalf("answering the SUBSCRIBE: %v", err)
		}
		return sent
	}
	// again waits on l for the SUBSCRIBE asking for want alone after the
	// SUBACK sent at answered, consecutive refusal n, and returns it.
	again := func(l *harness.PeerLink, answered time.Time, n int, want ...string) *harness.Subscribe {
		t.Helper()
		s, a := harness.Arrive[*harness.Subscribe](t, l, backoff(n)+harness.DefaultTimeout)
		support.Between(t, fmt.Sprintf("the SUBSCRIBE after consecutive refusal %d", n), a.At.Sub(answered), time.Second-early, backoff(n)+room)
		if got := support.Filters(s); !slices.Equal(slices.Sorted(slices.Values(got)), want) {
			t.Fatalf("the SUBSCRIBE after refusal %d asks %v; want %v alone", n, got, want)
		}
		for _, sub := range s.Subs {
			if sub != asked[sub.Filter] {
				t.Errorf("%s is asked again as %+v; want %+v, as first asked", sub.Filter, sub, asked[sub.Filter])
			}
		}
		return s
	}

	s := again(l, answer(l, first, "b/+", "c"), 1, "b/+", "c")
	s = again(l, answer(l, s, "c"), 2, "c")
	answer(l, s)
	_ = l.Close()

	l = peer.Link(t)
	l.Accept(t, support.Quiet)
	fresh, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	if got := slices.Sorted(slices.Values(support.Filters(fresh))); !slices.Equal(got, []string{"a/#", "b/+", "c"}) {
		t.Fatalf("the new link's SUBSCRIBE asks %v; want every filter", got)
	}
	again(l, answer(l, fresh, "a/#"), 1, "a/#")
}

// TestBridgeEndsALinkWhoseSubscribeGoesUnanswered: design.md, Bridges, In:
// "A SUBSCRIBE left unanswered for 30 seconds ends the link, which is then
// redialled. Every new link subscribes all its filters afresh."
//
// The peer accepts the bridge and never answers its SUBSCRIBE: the link
// ends 30 seconds after the SUBSCRIBE reached the peer, and the bridge
// dials again, its new link subscribing again.
func TestBridgeEndsALinkWhoseSubscribeGoesUnanswered(t *testing.T) {
	t.Parallel()
	const unanswered = 30 * time.Second
	peer := harness.ListenedPeer(t)
	support.Launch(t, &harness.Options{Bridges: []harness.Bridge{support.BridgeTo("unanswered", peer, support.In("a/#", 1))}})

	l := peer.Link(t)
	l.Accept(t, support.Quiet)
	_, subscribed := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	end, _ := support.Ending(t, l, unanswered+harness.DefaultTimeout)
	support.Between(t, "the link's end after its unanswered SUBSCRIBE", end.At.Sub(subscribed.At), unanswered-early, unanswered+room)
	again := peer.Link(t)
	again.Accept(t, support.Quiet)
	if s, _ := harness.Arrive[*harness.Subscribe](t, again, harness.DefaultTimeout); !slices.Equal(support.Filters(s), []string{"a/#"}) {
		t.Errorf("the redialled link's SUBSCRIBE asks %v; want a/#", support.Filters(s))
	}
}

// TestBridgeLogsItsLinksLife: design.md, Bridges, Logging: "A link that comes
// up is one INFO line, saying how long it was down if it was up before
// [...]. A link that goes down is one WARN line. A link the core still
// counts as live when the stop begins is one INFO line instead,
// link closed: broker stopping. A failed dial is one WARN
// line when its reason differs from the previous attempt's. Otherwise it is
// one DEBUG line. [...] A CONNACK with Session Present 0 on a reconnect is
// one WARN line. A refused subscription is one ERROR line. Its repeats are
// DEBUG lines. One ERROR line says when every filter is granted again."; "Every bridge line names its
// bridge."; "saying how long it was down if it was up before, rounded to
// the second (1m30s)"; Flows, Logging: "Each log line is
// written on standard error."
//
// A bridge named logged-link, at log_level debug, its lines those naming
// it, lives through each event in turn: its first dial refused (one WARN);
// the redial refused alike (DEBUG, no more WARN); the next accepted with
// Session Present 0 (one INFO), b/+ refused (one ERROR); the retry refused
// again (DEBUG, no more ERROR); the next retry granted (a second ERROR);
// the link dropped (a second WARN); the redial accepted with Session
// Present 0 on a reconnect (a third WARN) and up again (a second INFO,
// naming how long the link was down, rounded to the second as 1m30s is:
// whole hours, minutes and seconds, at least the seconds). SIGTERM then
// closes the live link: exactly one INFO line naming the bridge and saying
// link closed: broker stopping, with no further WARN line. Counts are
// checked after exit, when the log is complete.
func TestBridgeLogsItsLinksLife(t *testing.T) {
	t.Parallel()
	const name = "logged-link"
	peer := harness.ListenedPeer(t)
	b := support.Launch(t, &harness.Options{LogLevel: "debug", Bridges: []harness.Bridge{
		support.BridgeTo(name, peer, support.In("a/#", 1), support.In("b/+", 1)),
	}})
	// lines returns the lines of log at level naming the bridge, its own
	// alone. The start's INFO line names each bridge too (design.md,
	// Logging, What the broker logs of its own life: "The start, once it
	// is ready: one INFO line naming its version, the configuration file,
	// [...] each bridge"); it alone names the configuration file, no
	// bridge sentence naming one, and is left out by it.
	lines := func(log, level string) []string {
		return slices.DeleteFunc(testkit.LinesNaming(log, level, name), func(line string) bool { return strings.Contains(line, b.Config) })
	}
	// await waits until the log holds want lines naming the bridge at each
	// level, at least the DEBUG count, other DEBUG lines being the broker's
	// own.
	await := func(stage string, want map[string]int) {
		t.Helper()
		got := map[string]int{}
		if !testkit.Within(harness.DefaultTimeout, func() bool {
			log := b.Log()
			for level := range want {
				got[level] = len(lines(log, level))
			}
			for level, n := range want {
				if got[level] != n && (level != "DEBUG" || got[level] < n) {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("%s: the lines naming %s count %v; want %v\n%s", stage, name, got, want, b.Log())
		}
	}
	// answer answers s granting every filter but refused, and returns when
	// the SUBACK was sent.
	answer := func(l *harness.PeerLink, s *harness.Subscribe, refused string) {
		t.Helper()
		ack := support.Grants(s)
		for i, sub := range s.Subs {
			if sub.Filter == refused {
				ack.Reasons[i] = 0x87
			}
		}
		if _, err := l.Send(ack); err != nil {
			t.Fatalf("answering the SUBSCRIBE: %v", err)
		}
	}

	refuse(t, peer.Link(t))
	l := peer.Link(t)
	await("the first dial refused", map[string]int{"WARN": 1, "INFO": 0, "ERROR": 0})
	refuse(t, l)
	l = peer.LinkWithin(t, backoff(1)+harness.DefaultTimeout)
	await("the redial refused alike", map[string]int{"WARN": 1, "DEBUG": 1, "INFO": 0, "ERROR": 0})
	l.Accept(t, support.Quiet)
	s, _ := harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	answer(l, s, "b/+")
	await("the link up, b/+ refused", map[string]int{"WARN": 1, "INFO": 1, "ERROR": 1})
	s, _ = harness.Arrive[*harness.Subscribe](t, l, backoff(1)+harness.DefaultTimeout)
	answer(l, s, "b/+")
	s, _ = harness.Arrive[*harness.Subscribe](t, l, backoff(2)+harness.DefaultTimeout)
	await("b/+ refused again", map[string]int{"WARN": 1, "DEBUG": 2, "INFO": 1, "ERROR": 1})
	answer(l, s, "")
	await("every filter granted again", map[string]int{"WARN": 1, "INFO": 1, "ERROR": 2})
	_ = l.Close()
	l = peer.Link(t)
	await("the link down", map[string]int{"WARN": 2, "INFO": 1, "ERROR": 2})
	l.Accept(t, support.Quiet)
	s, _ = harness.Arrive[*harness.Subscribe](t, l, harness.DefaultTimeout)
	answer(l, s, "")
	await("the link up again, Session Present 0 on a reconnect", map[string]int{"WARN": 3, "INFO": 2, "ERROR": 2})

	ups := lines(b.Log(), "INFO")
	// A whole number of seconds, after whole minutes and hours where any,
	// standing apart from the words around it: never 1.5s or 300ms.
	duration := regexp.MustCompile(`(^|[\s(])(\d+h)?(\d+m)?\d+s($|[\s),;.])`)
	if _, message, _ := strings.Cut(strings.SplitN(ups[1], " ", 3)[2], " "); !duration.MatchString(message) {
		t.Errorf("the link's second up line %q names no duration rounded to the second; want how long it was down, as 1m30s", ups[1])
	}

	logged := len(b.Log())
	if err := b.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if state := support.Exited(t, b); !state.Success() {
		t.Errorf("SIGTERM with the link up: the broker ended %v; want exit status 0", state)
	}
	since := b.Log()[logged:]
	if closed := lines(since, "INFO"); len(closed) != 1 || !strings.Contains(closed[0], "link closed: broker stopping") {
		t.Errorf("SIGTERM with the link up: INFO lines naming %s: %q; want exactly one saying link closed: broker stopping", name, closed)
	}
	if warned := lines(since, "WARN"); len(warned) != 0 {
		t.Errorf("SIGTERM with the link up: WARN lines naming %s: %q; want none", name, warned)
	}
}

// TestBridgeAcknowledgesAfterTheStoreAndAckInterval: design.md, Bridges:
// "ack_interval bounds intentional batching. The first store-confirmed
// acknowledgement sets its release deadline. Subsequent acknowledgements do
// not extend it. At release, acknowledgements enter the reply queue in
// order."; Bridges, In: "A stored record's acknowledgement follows the
// batching and write deadlines above (Bridges; R4)."; Flows, Replies: "a
// bridge's acknowledgement to its peer [is] sent only once the store
// confirms"; "Acknowledgements of stored changes preserve their packet
// order."; Store, Writes: "With a configured interval, every kind of
// eligible write joins the collecting group. Its first publication fixes the
// core's collection deadline".
//
// With a publish_commit_interval of 400 ms, a bridge with ack_interval 300
// ms has an in rule on a/# at QoS 1. Its peer forwards twenty records at
// QoS 1, one every 150 ms, so acknowledgements keep becoming ready while
// earlier ones wait. Each gets PUBACK 0x00, in packet order. The first comes
// no earlier than the commit interval and ack_interval after its PUBLISH's
// sending, the store's confirmation setting its release, and within room
// of that; and it comes while the peer is still forwarding, before the last
// PUBLISH is sent, though later acknowledgements were ready before it.
func TestBridgeAcknowledgesAfterTheStoreAndAckInterval(t *testing.T) {
	t.Parallel()
	const (
		commit  = 400 * time.Millisecond
		ack     = 300 * time.Millisecond
		every   = 150 * time.Millisecond
		records = 20
	)
	peer := harness.ListenedPeer(t)
	bridge := support.BridgeTo("batching", peer, support.In("a/#", 1))
	bridge.AckInterval = ack
	support.Launch(t, &harness.Options{PublishCommitInterval: commit, Bridges: []harness.Bridge{bridge}})
	l, _, _ := support.LinkUp(t, peer, support.Quiet)

	tick := time.NewTicker(every)
	defer tick.Stop()
	var first, last time.Time
	for id := uint16(1); id <= records; id++ {
		if id > 1 {
			<-tick.C // the peer's own pace: one record every 150 ms
		}
		last = support.PeerSends(t, l, &harness.Publish{QoS: 1, ID: id, Topic: "a/batched", Payload: []byte{byte(id)}})
		if id == 1 {
			first = last
		}
	}
	acked := support.PeerAck(t, l, harness.PUBACK, 1, 0x00)
	for id := uint16(2); id <= records; id++ {
		support.PeerAck(t, l, harness.PUBACK, id, 0x00)
	}
	support.Between(t, "the first PUBACK, from its PUBLISH's sending", acked.At.Sub(first), commit+ack-early, commit+ack+room)
	if !acked.At.Before(last) {
		t.Errorf("the first PUBACK came %v after the first PUBLISH, the last PUBLISH sent %v after it; want it before the last: later acknowledgements do not extend its release",
			acked.At.Sub(first), last.Sub(first))
	}
}
