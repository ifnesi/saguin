package bridge

// What a bridge decides about one record, with no broker at either end.
//
// The link itself is tested end to end against a real second broker in
// internal/broker; these are the decisions that are hard to provoke there -
// a channel that refuses and then stops refusing, a record that can never be
// accepted - and each of them is a way to lose or duplicate data quietly.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/ifnesi/saguin/internal/mqtt/packets"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/store"
)

// acked drives one record through the bridge's decision and reports whether
// the upstream may be told saguin is finished with it, which is the one
// question most of these tests are about.
func (b *Bridge) acked(ctx context.Context, upstream string, payload []byte,
	h []packets.UserProperty) bool {
	d := b.handle(ctx, upstream, payload, h, store.Props{})
	return d == finished
}

// fake is a Publisher that records what it was asked and answers what a test
// told it to.
type fake struct {
	mu   sync.Mutex
	got  []string // "topic payload" per accepted publish
	all  []string // every attempt, accepted or not
	fail []error  // answers for successive Publish calls; nil pads the rest
	stop error    // what Bounded answers, for every call
}

func (f *fake) Publish(topic string, payload []byte, h []packets.UserProperty, _ store.Props) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.all = append(f.all, topic+" "+string(payload))
	if len(f.fail) > 0 {
		err := f.fail[0]
		f.fail = f.fail[1:]
		if err != nil {
			return err
		}
	}
	f.got = append(f.got, topic+" "+string(payload))
	return nil
}

func (f *fake) Bounded(string, []byte, []packets.UserProperty, store.Props) error { return f.stop }

func (f *fake) accepted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.got...)
}

func (f *fake) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.all)
}

// rules compiles a bridge configuration by writing it and loading it, so
// that a test runs against the same rules the broker runs against rather
// than against a hand-built approximation of them. It also means a rule a
// test writes has to be one the configuration would actually accept.
func rules(t *testing.T, chans []string, inbound ...string) config.Bridge {
	t.Helper()

	var b strings.Builder
	b.WriteString("broker:\n  id: t\n  storage:\n    default: mem\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n" +
		"channels:\n")
	for _, n := range chans {
		fmt.Fprintf(&b, "  %s:\n    type: append\n", n)
	}
	b.WriteString("bridges:\n  head-office:\n    peer: tcp://example.invalid:1883\n" +
		"    client_id: vessel-07\n    topics:\n")
	for _, r := range inbound {
		b.WriteString(r)
	}

	path := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	set := f.BridgeSet()
	if len(set) != 1 {
		t.Fatalf("got %d bridges, want 1", len(set))
	}
	return set[0]
}

// rule writes one inbound rule as the file writes it.
func rule(filter, topic string) string {
	return fmt.Sprintf("      - filter: %q\n        topic: %q\n        direction: in\n", filter, topic)
}

// logging returns a bridge whose log lines a test can read back, because
// three of the things a bridge does are *only* a log line: a record no rule
// covers, a record a rule cannot place, and a record saguin will never take.
// Nothing else reports them, so a test that did not read the log would be
// asserting that the record vanished quietly.
func logging(t *testing.T, cfg config.Bridge, pub Publisher) (*Bridge, func() string) {
	t.Helper()
	return loggingAt(t, cfg, pub, slog.LevelWarn)
}

// loggingAt is the same with the level a test needs.
//
// **It used to exist because a clear was written below the line it
// retracted**: `bridge link down` was a Warn and `bridge link up again` an
// Info, so a test about the pair had to read at `info` to see both halves -
// and so did an operator, who had not been offered the choice. A retraction
// is now written at the level of the line it retracts, so a test about a
// pair reads at `warn` and that is the point of it. The knob stays for the
// tests that are about an Info line on its own.
func loggingAt(t *testing.T, cfg config.Bridge, pub Publisher, level slog.Level) (*Bridge, func() string) {
	t.Helper()
	var mu sync.Mutex
	var buf strings.Builder
	log := slog.New(slog.NewTextHandler(&writerFunc{func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}}, &slog.HandlerOptions{Level: level}))

	b := New(cfg, pub, config.Limits{}.Resolve(), log, nil)
	b.firstRetry = time.Millisecond // a retry a test can wait for
	return b, func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type writerFunc struct{ f func([]byte) (int, error) }

func (w *writerFunc) Write(p []byte) (int, error) { return w.f(p) }

func TestARecordTakesTheFirstRuleThatCoversIt(t *testing.T) {
	cfg := rules(t, []string{"readings", "jobs"},
		rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"),
		rule("fleet/#", "jobs/$#"),
	)
	f := &fake{}
	b, _ := logging(t, cfg, f)

	// Both rules cover this one. The order in the file is what decides, and
	// a merge that reordered two entries would change what the broker does
	// without changing what any line says.
	if !b.acked(t.Context(), "fleet/vessel-07/telemetry/hold/psi", []byte("42"), nil) {
		t.Fatal("the record was not finished with")
	}
	// And this one only the second covers.
	if !b.acked(t.Context(), "fleet/vessel-07/other", []byte("x"), nil) {
		t.Fatal("the record was not finished with")
	}

	want := []string{
		"readings/telemetry/vessel-07/hold/psi 42",
		"jobs/vessel-07/other x",
	}
	got := f.accepted()
	if len(got) != len(want) {
		t.Fatalf("published %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("record %d published to %q, want %q", i, got[i], want[i])
		}
	}
}

// RFC 0002 "Bridges": four things drop a record, and each is warned about
// and counted under its cause (RFC 0005 saguin_bridge_unstored_total). A
// drop with no line is a record an operator cannot find, and one with no
// count is a loss nobody can alert on: each was finished with at the peer.
func TestTheFourDropsAreEachWarnedAboutAndCounted(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rule     string
		chans    []string
		upstream string
		bounded  error
		want     string
		cause    string
	}{
		{
			name:     "no rule covers it",
			rule:     rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"),
			chans:    []string{"readings"},
			upstream: "weather/station-3/wind",
			want:     "no rule covers it",
			cause:    "no_rule",
		},
		{
			// `#` matches zero levels, so this reaches the bare `fleet` and
			// the tail is empty. The template is a `$#` and nothing else, so
			// there is no topic left at all - which is what makes it a drop
			// rather than a publish. A template spelling a topic out around
			// the tail still has that topic, and is published.
			name:     "a rule covers it and produces no topic",
			rule:     rule("fleet/#", "$#"),
			chans:    []string{"readings"},
			upstream: "fleet",
			want:     "produced no topic",
			cause:    "unmappable",
		},
		{
			// Only a rule naming no channel can reach the reserved space, and
			// only through a capture: a `$` written in a template is refused
			// at startup as a substitution that is neither `$#` nor a number.
			//
			// This case used to assert "produced no topic", which is the drop
			// above rather than this one - the two look identical to Match,
			// and the test was encoding the wrong answer as the right one.
			name:     "it would land in the reserved $ space",
			rule:     rule("relay/#", "$#"),
			chans:    []string{"readings"},
			upstream: "relay/$saguin/queue/jobs/response",
			want:     "reserved `$` space",
			cause:    "unmappable",
		},
		{
			name:     "saguin would never accept it",
			rule:     rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"),
			chans:    []string{"readings"},
			upstream: "fleet/vessel-07/telemetry/hold/psi",
			bounded:  packets.ErrPacketTooLarge,
			want:     "would never accept",
			cause:    "never_accepted",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fake{stop: tc.bounded}
			b, logged := logging(t, rules(t, tc.chans, tc.rule), f)

			// Acknowledged: the record is dropped, and holding it would stop
			// the link for ever behind something that cannot move.
			if !b.acked(t.Context(), tc.upstream, []byte("payload"), nil) {
				t.Error("the record was not acknowledged, so the link would stop behind it")
			}
			if n := len(f.accepted()); n != 0 {
				t.Errorf("it was published anyway (%d records)", n)
			}
			if got := logged(); !strings.Contains(got, tc.want) {
				t.Errorf("nothing said %q was dropped; the log was:\n%s", tc.want, got)
			}
			counted := map[string]uint64{
				"no_rule": b.UnstoredNoRule(), "unmappable": b.UnstoredUnmappable(),
				"never_accepted": b.UnstoredNeverAccepted(), "queue_full": b.UnstoredQueueFull(),
			}
			for cause, n := range counted {
				want := uint64(0)
				if cause == tc.cause {
					want = 1
				}
				if n != want {
					t.Errorf("unstored %s counted %d, want %d: the drop is counted under %s", cause, n,
						want, tc.cause)
				}
			}
		})
	}
}

// The rule the whole feature rests on: a refused record is not acknowledged,
// and it is published again until it gets in.
func TestARefusedRecordIsRetriedRatherThanAcknowledged(t *testing.T) {
	cfg := rules(t, []string{"readings"},
		rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"))

	// Full, full, full, then a consumer drains it.
	f := &fake{fail: []error{packets.ErrQuotaExceeded, packets.ErrQuotaExceeded, packets.ErrQuotaExceeded}}
	b, logged := logging(t, cfg, f)

	if !b.acked(t.Context(), "fleet/vessel-07/telemetry/hold/psi", []byte("42"), nil) {
		t.Fatal("the record was refused for good, but every refusal here can stop being true")
	}
	if n := f.attempts(); n != 4 {
		t.Errorf("it was published %d times, want 4 - three refusals and the one that got in", n)
	}
	if got := f.accepted(); len(got) != 1 || got[0] != "readings/telemetry/vessel-07/hold/psi 42" {
		t.Errorf("it was stored as %v", got)
	}
	if s := logged(); !strings.Contains(s, "backpressure") {
		t.Errorf("nothing said the subscription had become backpressure; the log was:\n%s", s)
	}
	// One line, not one per attempt: a channel that stays full stays full,
	// and a line per retry buries everything else that happened.
	if n := strings.Count(logged(), "refused a bridged record"); n != 1 {
		t.Errorf("the refusal was logged %d times, want 1", n)
	}
	// **And the line that takes it back is readable at the same level.**
	// `logging` reads at `warn`, which is what an operator may run at: a
	// clear written below the line it retracts leaves them holding a
	// warning about a channel that started accepting again three seconds
	// later, for the life of the process.
	if n := strings.Count(logged(), "took a bridged record that had been refused"); n != 1 {
		t.Errorf("the recovery was logged %d times at `warn`, want 1 - the refusal above "+
			"is visible at this level and its retraction must be too:\n%s", n, logged())
	}
}

// A shutdown must not turn a refused record into an acknowledged one. The
// upstream still holds it, and a bridge that acknowledged on the way out
// would have discarded a record the upstream believes it delivered.
func TestAShutdownLeavesARefusedRecordUnacknowledged(t *testing.T) {
	cfg := rules(t, []string{"readings"},
		rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"))

	// Refuses for ever, which is a channel nobody is draining.
	f := &fake{}
	for range 1000 {
		f.fail = append(f.fail, packets.ErrQuotaExceeded)
	}
	b, _ := logging(t, cfg, f)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- b.acked(ctx, "fleet/vessel-07/telemetry/hold/psi", []byte("42"), nil) }()

	// Let it refuse at least once, so the cancellation lands mid-retry
	// rather than before the first attempt.
	deadline := time.After(2 * time.Second)
	for f.attempts() == 0 {
		select {
		case <-deadline:
			t.Fatal("it never tried to publish")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()

	select {
	case acked := <-done:
		if acked {
			t.Fatal("a shutdown acknowledged a record saguin had refused; " +
				"the upstream would drop it and nothing here has it")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("it did not stop when the context was cancelled")
	}
}

// A header a foreign broker sets must not be able to impersonate saguin's
// own. The broker strips the reserved prefix on the way in, so what this
// checks is that a bridge hands the properties over rather than inventing
// its own - the filtering lives in one place, and this is not it.
func TestTheUpstreamsPropertiesAreCarriedAcross(t *testing.T) {
	cfg := rules(t, []string{"readings"},
		rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"))

	var seen []packets.UserProperty
	b, _ := logging(t, cfg, &capturing{onPublish: func(_ string, _ []byte, h []packets.UserProperty) error {
		seen = h
		return nil
	}})
	headers := []packets.UserProperty{{Key: "unit", Val: "bar"}, {Key: "saguin-offset", Val: "999"}}
	if !b.acked(t.Context(), "fleet/vessel-07/telemetry/hold/psi", []byte("42"), headers) {
		t.Fatal("the record was not finished with")
	}
	if len(seen) != 2 {
		t.Fatalf("the publish carried %d properties, want 2 - a bridge filters none of them", len(seen))
	}
}

type capturing struct {
	onPublish func(string, []byte, []packets.UserProperty) error
}

func (c *capturing) Publish(t string, p []byte, h []packets.UserProperty, _ store.Props) error {
	return c.onPublish(t, p, h)
}
func (c *capturing) Bounded(string, []byte, []packets.UserProperty, store.Props) error { return nil }

func TestJitterStaysInsideItsWindow(t *testing.T) {
	// A retry that could return zero would spin, and one that could exceed
	// the wait would drift past the ceiling store thinks it has.
	for range 1000 {
		got := jitter(100 * time.Millisecond)
		if got < 50*time.Millisecond || got > 100*time.Millisecond {
			t.Fatalf("jitter(100ms) gave %v, want between 50ms and 100ms", got)
		}
	}
}

func TestNamesReadsBackWhatIsRunning(t *testing.T) {
	cfg := rules(t, []string{"readings"},
		rule("fleet/#", "readings/$#"))
	b, _ := logging(t, cfg, &fake{})
	if got := Names([]*Bridge{b}); got != "head-office" {
		t.Errorf("Names gave %q, want %q", got, "head-office")
	}
}

var _ io.Writer = (*writerFunc)(nil)

// config.Load fills the three tuning values in for every bridge it
// validates, so a zero can only reach New from a config.Bridge somebody
// built by hand. New defaults them anyway, because of what a zero would
// mean rather than because it is expected: a Session Expiry Interval of 0
// tells the upstream to discard the session the moment the link drops, so
// the bridge would hold no backlog and lose every record published during
// an outage, without an error anywhere.
func TestNewDefaultsWhatAHandBuiltConfigLeftAtZero(t *testing.T) {
	b, _ := logging(t, config.Bridge{Name: "hand-built"}, &fake{})
	if b.cfg.SessionExpiry != config.DefaultSessionExpiry {
		t.Errorf("session expiry is %v, want %v - zero tells the upstream to discard "+
			"the session the moment the link drops", b.cfg.SessionExpiry, config.DefaultSessionExpiry)
	}
	if b.cfg.ReceiveMaximum != config.DefaultReceiveMaximum {
		t.Errorf("receive maximum is %d, want %d - zero is a subscriber that may receive nothing",
			b.cfg.ReceiveMaximum, config.DefaultReceiveMaximum)
	}
	if b.cfg.AckInterval != config.DefaultAckInterval {
		t.Errorf("acknowledgement interval is %v, want %v - zero is a ticker that never fires",
			b.cfg.AckInterval, config.DefaultAckInterval)
	}
}

// The failure this exists to prevent, found by running a bridge
// rather than reading it: one record the broker will never accept, held at the
// head of the link, and every record behind it waiting for ever. Measured
// then: **0 of 40** good records crossed after one poison record, with a
// single warning to show for it.
//
// A refusal is retried only when it is one that can stop being true. The
// list is of what may be retried, never of what may not - so a refusal
// nobody classified gives up and says so, rather than holding the link.
func TestARecordTheBrokerWillNeverAcceptIsDroppedRatherThanHeld(t *testing.T) {
	cfg := rules(t, []string{"readings"},
		rule("fleet/+/telemetry/#", "readings/telemetry/$1/$#"))

	for _, tc := range []struct {
		name    string
		err     error
		dropped bool
	}{
		// The two that clear themselves, and RFC 0002 names the reason for
		// both: a channel empties as consumers drain it, and a storage that
		// is not working can start working.
		{"a full channel", packets.ErrQuotaExceeded, false},
		{"a store that did not work", packets.ErrImplementationSpecificError, false},

		// The one that was missed. A produced topic carrying a wildcard is
		// refused by the substrate before the hook is even reached, and no
		// amount of waiting changes the topic.
		{"a topic the broker will not publish", packets.ErrProtocolViolationSurplusWildcard, true},
		// Any other refusal about this record's own shape.
		{"a topic name that is invalid", packets.ErrTopicNameInvalid, true},
		{"a payload the broker refuses", packets.ErrPayloadFormatInvalid, true},
		{"a refusal nobody has classified", packets.Code{Code: 0x9F, Reason: "invented"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Refuses for ever. A record that is retried will never finish.
			f := &fake{}
			for range 500 {
				f.fail = append(f.fail, tc.err)
			}
			b, logged := logging(t, cfg, f)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			done := make(chan bool, 1)
			go func() {
				done <- b.acked(ctx, "fleet/vessel-07/telemetry/hold/psi", []byte("42"), nil)
			}()

			if tc.dropped {
				select {
				case acked := <-done:
					if !acked {
						t.Fatal("the record was not acknowledged, so the link stops behind it")
					}
				case <-time.After(2 * time.Second):
					t.Fatal("it is still retrying a record that can never be accepted; " +
						"this link would carry nothing again, ever")
				}
				if n := f.attempts(); n != 1 {
					t.Errorf("it was published %d times, want 1 - a permanent refusal is "+
						"answered the first time", n)
				}
				if s := logged(); !strings.Contains(s, "will never accept") {
					t.Errorf("nothing said why it was dropped; the log was:\n%s", s)
				}
				return
			}

			// The transient ones must still be held: not acknowledging is
			// what turns a full channel into backpressure.
			select {
			case <-done:
				t.Fatal("a refusal that can stop being true was given up on, so the " +
					"record is gone and the upstream believes it was delivered")
			case <-time.After(200 * time.Millisecond):
			}
			cancel()
			<-done
		})
	}
}

// RFC 0002 lists two separate drops that Match answers the same way: a `#`
// that matched zero levels leaving nothing to publish, and a topic aiming at
// the reserved `$` space. Reported as the first, an operator goes looking
// for an empty tail and finds a healthy rule.
func TestTheReservedSpaceDropSaysWhatItActuallyWas(t *testing.T) {
	// `channel: none` is the only way to reach the reserved space at all,
	// and only through a capture: a `$` written in a template never survives
	// as literal text.
	cfg := rules(t, []string{"readings"}, rule("fwd/#", "$#"))
	b, logged := logging(t, cfg, &fake{})

	if !b.acked(t.Context(), "fwd/$saguin/queue/jobs/response", []byte("x"), nil) {
		t.Fatal("the record was not finished with")
	}
	got := logged()
	if !strings.Contains(got, "reserved") {
		t.Errorf("the drop does not name the reserved space; the log was:\n%s", got)
	}
	if strings.Contains(got, "produced no topic") {
		t.Errorf("it was reported as an empty tail, which sends an operator to a "+
			"healthy rule; the log was:\n%s", got)
	}

	// And the genuinely empty one still says so, or the fix would have
	// swapped one wrong message for another. That takes a template which is
	// a `$#` and nothing else: since a rule's topic became the whole topic
	// rather than a suffix inside a channel, `readings/$#` with an empty tail
	// leaves `readings`, which is a topic and is published.
	cfg2 := rules(t, []string{"readings"}, rule("fleet/#", "$#"))
	b2, logged2 := logging(t, cfg2, &fake{})
	if !b2.acked(t.Context(), "fleet", []byte("x"), nil) {
		t.Fatal("the record was not finished with")
	}
	if s := logged2(); !strings.Contains(s, "produced no topic") {
		t.Errorf("an empty tail is no longer reported as one; the log was:\n%s", s)
	}
}

// **A bridge that has never reached its upstream says so.**
//
// The connect error was Debug for everything, on the reasoning that a bad
// link writes one per attempt and the two lines that matter come from
// connected and disconnected. That holds for a link that was up and went
// away; for one that has never been up, connected has never fired, so
// disconnected never does either, and the broker's log ends at "bridges
// starting" while nothing is ever collected. A certificate the far end
// cannot be verified with is exactly that failure: it does not heal by
// retrying, and it has to be distinguishable from a cable somebody pulled.
//
// The helper's handler is set at Warn, so anything this test sees is a line
// an operator running at the ordinary level sees too.
func TestABridgeThatCannotConnectSaysSoOnceAtWarn(t *testing.T) {
	cfg := config.Bridge{
		Name: "up", ClientID: "b1",
		// A port nothing is listening on: every attempt fails the same way.
		Peer:           "tcp://127.0.0.1:1",
		SessionExpiry:  time.Minute,
		ReceiveMaximum: 10,
		AckInterval:    time.Millisecond,
		Topics:         []config.Rule{},
	}
	b, logged := logging(t, cfg, &fake{})

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx)

	out := logged()
	if !strings.Contains(out, "bridge cannot reach its peer") {
		t.Errorf("a bridge that never connected wrote nothing at Warn:\n%s", out)
	}
	if !strings.Contains(out, "127.0.0.1:1") {
		t.Errorf("the line does not name the peer:\n%s", out)
	}

	// Once each. The logger this bridge writes through is built with the
	// bridge and its peer on it, so a call naming them again prints
	// both twice - which is what the first version of this line did, and
	// what showed on the wire before anybody read the code.
	for _, key := range []string{"bridge=", "peer="} {
		if n := strings.Count(out, key); n != 1 {
			t.Errorf("%q appears %d times in one line, want 1:\n%s", key, n, out)
		}
	}

	// And it does not write one per attempt. autopaho retries several times
	// in the window above; the reason has not changed, so the line has not
	// been written again.
	if n := strings.Count(out, "bridge cannot reach its peer"); n != 1 {
		t.Errorf("%d lines for one unchanging reason, want 1 - a bad link would fill "+
			"the log it is supposed to be visible in:\n%s", n, out)
	}
}

// The same bridge, silent at `info` and speaking at `debug` - with the level
// moved while it runs, which is what `SIGUSR1` does to a broker nobody wants
// to restart.
//
// RFC 0002's level table promises that `debug` adds "every bridge connection
// attempt", and `broker.pid_file` exists so an operator can ask for that on a
// broker holding durable sessions. Both claims rest on this one line being
// gated by the level and on the gate moving at runtime, and neither was run
// by anything: the end-to-end suite fixes its handler at `LevelDebug`, so a
// change that promoted this line to `info` - filling the log a bad link is
// supposed to stay visible in - would have passed every test here.
//
// It asserts the before as well as the after. A test that only checks the
// line appears at `debug` passes just as well against a line that was never
// gated at all, which is the defect it is meant to catch.
func TestTheAttemptDetailIsGatedByTheLevelAndTheGateMoves(t *testing.T) {
	cfg := config.Bridge{
		Name: "up", ClientID: "b1",
		// A port nothing is listening on: every attempt fails the same way.
		Peer:           "tcp://127.0.0.1:1",
		SessionExpiry:  time.Minute,
		ReceiveMaximum: 10,
		AckInterval:    time.Millisecond,
		Topics:         []config.Rule{},
	}

	var mu sync.Mutex
	var buf strings.Builder
	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	log := slog.New(slog.NewTextHandler(&writerFunc{func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}}, &slog.HandlerOptions{Level: level}))
	read := func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}

	b := New(cfg, &fake{}, config.Limits{}.Resolve(), log, nil)
	b.firstRetry = time.Millisecond
	// The real backoff is five seconds between attempts, and this test is
	// about what the second one logs rather than about how long it waits.
	//
	// **It is also the count of attempts made.** autopaho asks it before
	// every attempt, and asks it for attempt n only once attempt n-1 has
	// failed and been reported, so a wait for an attempt number is a wait
	// for everything the attempts before it logged - where a sleep was a
	// guess at how many had happened.
	var asked atomic.Int64
	asked.Store(-1)
	b.reconnectBackoff = func(n int) time.Duration {
		asked.Store(int64(n))
		return 20 * time.Millisecond
	}
	reached := func(n int64, what string) {
		t.Helper()
		for deadline := time.Now().Add(10 * time.Second); asked.Load() < n; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: the bridge made %d attempts in 10s:\n%s", what, asked.Load(), read())
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = b.Run(ctx) }()

	const attempt = "bridge connection attempt failed"

	// At info: the fault is reported once at warn, and the per-attempt
	// detail is not written at all. Attempts 0 and 1 have failed and been
	// reported once attempt 2 is asked for: the first is the warning, the
	// second the first detail line that the level withholds.
	reached(2, "at info")
	at := read()
	if !strings.Contains(at, "bridge cannot reach its peer") {
		t.Fatalf("nothing was logged at info, so this test never drove the "+
			"bridge and the rest of it proves nothing:\n%s", at)
	}
	if n := strings.Count(at, attempt); n != 0 {
		t.Errorf("%d attempt lines at info, want 0 - a bad link would fill the "+
			"log it is supposed to stay visible in:\n%s", n, at)
	}

	// Then the level moves under a running bridge, the way the signal moves
	// it under a running broker. Every attempt numbered past the last one
	// asked for is begun after the move, so the first of them is reported
	// at debug once the one after it is asked for.
	level.Set(slog.LevelDebug)
	reached(asked.Load()+2, "at debug")

	after := strings.TrimPrefix(read(), at)
	if n := strings.Count(after, attempt); n == 0 {
		t.Errorf("no attempt line after the level moved to debug, so the detail "+
			"RFC 0002 promises there cannot be reached without a restart:\n%s", after)
	}
	cancel()
	<-done
}

// **A record at the source larger than this broker's `max_message_size`
// never reaches the record path at all**, so none of the rules above can
// see it. The bridge declares that bound as MQTT's Maximum Packet Size in
// its CONNECT, and the *source* refuses to deliver and disconnects the
// consumer - leaving the bridge to reconnect, resubscribe and be
// disconnected on the same record, for ever, with the cause logged only at
// the other end.
//
// Nothing is lost: the position never advances past it. What was missing is
// that the box whose copy is stalled said nothing at all.
func TestAnOversizedRecordAtTheSourceStopsTheBridgeAndSaysWhy(t *testing.T) {
	b, logged := logging(t, rules(t, []string{"events"},
		rule("events/#", "events/$#")), &fake{})

	// Every other reason is an ordinary link that will come back.
	b.serverGone(&paho.Disconnect{ReasonCode: byte(packets.ErrServerShuttingDown.Code)})
	if b.Stopped() {
		t.Fatal("an ordinary server disconnect stopped the bridge for good")
	}

	b.serverGone(&paho.Disconnect{ReasonCode: byte(packets.ErrPacketTooLarge.Code)})
	if !b.Stopped() {
		t.Error("a source disconnecting because a record is too large left the bridge " +
			"reconnecting for ever, with nothing here saying why")
	}
	out := logged()
	if !strings.Contains(out, "max_message_size") {
		t.Errorf("the log does not name the setting to change:\n%s", out)
	}
	// The remedy, not just the diagnosis: an operator reading this has to
	// know what to do next.
	if !strings.Contains(out, "Raise max_message_size") {
		t.Errorf("the log does not say what to do about it:\n%s", out)
	}
}

// **A bridge that halts itself reports its link down.**
//
// RFC 0005 gives an operator two gauges: `saguin_bridge_connected` for
// whether the link is up, and `saguin_bridge_stopped` for whether this
// bridge has given up on it. A stopped bridge is `connected 0` beside
// `stopped 1`, and the pair is the whole point - `connected 0` alone cannot
// say whether somebody should wait for a reconnection or come and look.
//
// It is asserted here because nothing did: the bridge metrics are otherwise
// tested through a fake that reports whatever it is told, so a document
// could say the opposite of the code and no test would notice. One did.
//
// Cancelling the context shuts autopaho down from the inside and
// OnConnectionDown does not fire for a shutdown it was asked for, so the
// state has to be set by stop() itself rather than left to the callback.
func TestAStoppedBridgeReportsItsLinkDown(t *testing.T) {
	b, _ := logging(t, config.Bridge{Name: "dr-link", ClientID: "dr-link"}, nil)

	b.linkUp.Store(true)
	if !b.Connected() || b.Stopped() {
		t.Fatalf("before stopping: connected=%v stopped=%v, want a live link that has "+
			"not halted - this test never reached the state it is about",
			b.Connected(), b.Stopped())
	}

	b.stop()

	if b.Connected() {
		t.Error("a bridge that halted itself still reports saguin_bridge_connected 1. An " +
			"operator watching the ordinary signal sees a healthy link that is dead, and " +
			"the one gauge that would have told them apart is the one they were not " +
			"watching")
	}
	if !b.Stopped() {
		t.Error("a bridge that halted itself reports saguin_bridge_stopped 0, so nothing " +
			"anywhere says a person is needed")
	}
}

// **The read loop never waits on a channel**. A record the channel refuses is retried by the bridge's worker,
// not by the handler paho calls on the connection's read loop - where it
// kept autopaho from noticing a dropped link for as long as the refusal
// lasted. So the handler returns at once while the channel refuses, and what
// the peer sent is still stored in the order it arrived once it takes it.
func TestARefusingChannelDoesNotHoldTheLinksReadLoop(t *testing.T) {
	cfg := rules(t, []string{"readings"}, rule("fleet/#", "readings/$#"))
	f := &fake{fail: []error{
		packets.ErrQuotaExceeded, packets.ErrQuotaExceeded, packets.ErrQuotaExceeded, packets.ErrQuotaExceeded}}
	b, _ := logging(t, cfg, f)
	b.firstRetry = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); b.work(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	for i, topic := range []string{"fleet/a", "fleet/b", "fleet/c"} {
		began := time.Now()
		b.enqueue(ctx, paho.PublishReceived{Packet: &paho.Publish{
			Topic: topic, QoS: 1, PacketID: uint16(i + 1), Payload: []byte(fmt.Sprintf("r%d", i+1))}})
		if took := time.Since(began); took > 50*time.Millisecond {
			t.Fatalf("the handler took %v to return while the channel refused: the read loop "+
				"waits on the channel, and a dropped link goes unnoticed for as long", took)
		}
	}
	want := []string{"readings/a r1", "readings/b r2", "readings/c r3"}
	deadline := time.Now().Add(5 * time.Second)
	for !slices.Equal(f.accepted(), want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := f.accepted(); !slices.Equal(got, want) {
		t.Fatalf("the channel took %v, want %v, in the order the peer sent them", got, want)
	}
}

// A QoS 0 record may take one Receive Maximum of the queue and is dropped and
// counted beyond it - at-most-once, as mosquitto drops QoS 0 over a full
// queue - while a QoS 1 record, bounded by the Receive Maximum the peer must
// respect, is never dropped.
func TestQoS0FromThePeerIsDroppedAndCountedWhenTheQueueIsFull(t *testing.T) {
	cfg := rules(t, []string{"readings"}, rule("fleet/#", "readings/$#"))
	b, logged := logging(t, cfg, &fake{})
	ctx := context.Background()
	qos0 := func(i int) paho.PublishReceived {
		return paho.PublishReceived{Packet: &paho.Publish{Topic: "fleet/x", QoS: 0, Payload: []byte(fmt.Sprint(i))}}
	}
	// No worker: the channel is not taking anything.
	for i := range b.cfg.ReceiveMaximum {
		b.enqueue(ctx, qos0(i))
	}
	if b.UnstoredQueueFull() != 0 {
		t.Fatalf("%d QoS 0 records dropped within their share of the queue", b.UnstoredQueueFull())
	}
	b.enqueue(ctx, qos0(-1))
	if b.UnstoredQueueFull() != 1 {
		t.Fatalf("a QoS 0 record past its share was counted %d times, want once", b.UnstoredQueueFull())
	}
	if !strings.Contains(logged(), "dropping QoS 0 records from the peer") {
		t.Errorf("the drop was not said:\n%s", logged())
	}
	b.enqueue(ctx, paho.PublishReceived{Packet: &paho.Publish{Topic: "fleet/x", QoS: 1, PacketID: 1, Payload: []byte("kept")}})
	if len(b.inbox) != b.cfg.ReceiveMaximum+1 || b.UnstoredQueueFull() != 1 {
		t.Errorf("a QoS 1 record behind a full QoS 0 share was not queued: %d queued, %d dropped",
			len(b.inbox), b.UnstoredQueueFull())
	}
}

// Invariant 13: the bound is applied before memory is spent on what it
// bounds. A peer that ignores the Maximum Packet Size this bridge sent can
// still send a record over max_message_size, which paho reads whole; it is
// dropped where it arrives, counted never_accepted and acknowledged as the
// worker would drop it, rather than queued - where up to three Receive
// Maximums of them waited, each as large as the peer chose, before the worker
// reached them. A record at the bound is queued as any other.
func TestARecordOverMaxMessageSizeIsDroppedWhereItArrives(t *testing.T) {
	cfg := rules(t, []string{"readings"}, rule("fleet/#", "readings/$#"))
	// The broker's Bounded refuses a payload over max_message_size first.
	b, logged := logging(t, cfg, &fake{stop: packets.ErrPacketTooLarge})
	ctx := context.Background()
	max := int(b.lim.MaxMessageSize)
	// Manual acknowledgement off: its Ack answers that, and ack logs it,
	// which is what shows the drop was acknowledged.
	cl := paho.NewClient(paho.ClientConfig{})
	n := b.cfg.ReceiveMaximum
	for i := range n {
		b.enqueue(ctx, paho.PublishReceived{Client: cl, Packet: &paho.Publish{
			Topic: "fleet/x", QoS: 1, PacketID: uint16(i + 1), Payload: make([]byte, max+1)}})
	}
	if got := len(b.inbox); got != 0 {
		t.Errorf("%d records over max_message_size were queued, want none", got)
	}
	if got := b.UnstoredNeverAccepted(); got != uint64(n) {
		t.Errorf("%d counted as never_accepted, want %d", got, n)
	}
	if got := b.Received(); got != uint64(n) {
		t.Errorf("%d counted as received, want %d", got, n)
	}
	if got := strings.Count(logged(), "cannot acknowledge a bridged record at its peer"); got != n {
		t.Errorf("%d of %d dropped records were acknowledged:\n%s", got, n, logged())
	}
	b.enqueue(ctx, paho.PublishReceived{Packet: &paho.Publish{
		Topic: "fleet/x", QoS: 1, PacketID: 999, Payload: make([]byte, max)}})
	if got := len(b.inbox); got != 1 {
		t.Errorf("a record at max_message_size left %d queued, want 1", got)
	}

	// A bridge that has stopped takes nothing and acknowledges nothing, here
	// as in the worker: the peer keeps what it holds.
	b.stopped.Store(true)
	b.enqueue(ctx, paho.PublishReceived{Client: cl, Packet: &paho.Publish{
		Topic: "fleet/x", QoS: 1, PacketID: 1000, Payload: make([]byte, max+1)}})
	if got := strings.Count(logged(), "cannot acknowledge a bridged record at its peer"); got != n {
		t.Errorf("a stopped bridge acknowledged a record over max_message_size: %d acknowledgements, want %d", got, n)
	}
}
