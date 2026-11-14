package intervals

import (
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// TestOutSkipsAndCountsWhatItCannotSend: design.md, Bridges: "Topic limits
// are checked after substitution as well as at configuration load."; "An
// invalid outbound result is skipped and advances that rule's cursor. It is
// counted and logged as topic_invalid. An outbound packet exceeding the
// peer's Maximum Packet Size is skipped in the same way as too_large.";
// Bridges, Out: "A record the peer refuses is skipped, counted and
// logged."; "One record per topic at the peer is in flight at a time.";
// "Every bridge line names its bridge."; Delivery, Message Expiry: "A
// bridge never sends an expired record, from any channel, and counts it.";
// "Expiry applies only to a message not yet sent." metrics.md,
// saguin_bridge_unsent_total{bridge,cause}: "refused (the peer refused it),
// expired, topic_invalid (its topic, after substitution, past a topic
// limit) or too_large (over the peer's Maximum Packet Size)". MQTT 5
// 3.2.2.3.6: "The Client MUST NOT send packets exceeding Maximum Packet Size
// to the Server [MQTT-3.2.2-15]."
//
// A broker with max_topic_length 20, logging at debug, has a bridge,
// skipping, with out rules taking s/# to a/$# and t/# to long-prefix/$#,
// each at QoS 1, whose peer's CONNACK has Maximum Packet Size 256. A client
// stores, and the peer receives, in turn:
//   - t/abcdefghijklmnopq, of 19 bytes, which would be long-prefix/... of 29
//     there, then t/n: the first PUBLISH is long-prefix/n, the rule's cursor
//     past the record it skipped;
//   - s/big, its payload 400 bytes, then s/n: the first is a/n;
//   - s/r, which the peer refuses with PUBACK 0x87, then s/m: a/m comes;
//   - s/e, which the peer holds unanswered, then on s/e again one with a
//     Message Expiry of one second, then a third: the peer acknowledges the
//     first once the second's expiry has passed, and the next on a/e is
//     the third.
//
// saguin_bridge_unsent_total then reads one under each cause, and the log
// holds lines naming the bridge and each of refused, too_large and
// topic_invalid.
func TestOutSkipsAndCountsWhatItCannotSend(t *testing.T) {
	t.Parallel()
	const name = "skipping"
	b, _, l := support.Outbound(t, &harness.Options{Operations: true, LogLevel: "debug", MaxTopicLength: 20}, name,
		support.QuietWith(false, harness.Property{ID: harness.MaximumPacketSize, Int: 256}),
		support.Out("s/#", "a/$#", 1), support.Out("t/#", "long-prefix/$#", 1))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: "skipping-pub", CleanStart: true})
	at := func(topic, payload string) *harness.Publish {
		return &harness.Publish{QoS: 1, Topic: topic, Payload: []byte(payload)}
	}

	after := at("t/n", "after the invalid topic")
	support.PublishAll(t, pub, []*harness.Publish{at("t/abcdefghijklmnopq", "invalid at the peer"), after})
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "long-prefix/n", 1, after.Payload)

	after = at("s/n", "after the large one")
	support.PublishAll(t, pub, []*harness.Publish{{QoS: 1, Topic: "s/big", Payload: bytes.Repeat([]byte("x"), 400)}, after})
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "a/n", 1, after.Payload)

	refused := at("s/r", "refused by the peer")
	support.Acknowledged(t, pub, refused)
	got, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	support.Crossed(t, got, "a/r", 1, refused.Payload)
	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: got.ID, Reason: 0x87})
	after = at("s/m", "after the refused one")
	support.Acknowledged(t, pub, after)
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "a/m", 1, after.Payload)

	first := at("s/e", "in flight")
	support.Acknowledged(t, pub, first)
	inFlight, _ := harness.Arrive[*harness.Publish](t, l, harness.DefaultTimeout)
	support.Crossed(t, inFlight, "a/e", 1, first.Payload)
	expiring := at("s/e", "expiring behind it")
	expiring.Props = harness.Properties{{ID: harness.MessageExpiryInterval, Int: 1}}
	after = at("s/e", "after the expired one")
	sent := time.Now()
	support.PublishAll(t, pub, []*harness.Publish{expiring, after})
	time.Sleep(time.Until(sent.Add(time.Second + support.Late))) // the expiry passing is the behaviour under test
	support.PeerSends(t, l, &harness.Ack{Kind: harness.PUBACK, ID: inFlight.ID})
	support.Crossed(t, support.PeerTakes(t, l, 1)[0], "a/e", 1, after.Payload)

	unsent := func(cause string) string {
		return testkit.SeriesKey("saguin_bridge_unsent_total", "bridge", name, "cause", cause)
	}
	support.AwaitSampled(t, b, "one record skipped under each cause", map[string]float64{
		unsent("topic_invalid"): 1, unsent("too_large"): 1, unsent("refused"): 1, unsent("expired"): 1,
	})
	for _, cause := range []string{"topic_invalid", "too_large", "refused"} {
		var log string
		if !testkit.Within(harness.DefaultTimeout, func() bool {
			log = b.Log()
			return slices.ContainsFunc([]string{"DEBUG", "INFO", "WARN", "ERROR"}, func(level string) bool {
				return slices.ContainsFunc(testkit.LinesNaming(log, level, name), func(line string) bool { return strings.Contains(line, cause) })
			})
		}) {
			t.Errorf("no log line names %s and %s; the log:\n%s", name, cause, strings.TrimSpace(log))
		}
	}
}
