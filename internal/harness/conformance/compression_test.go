package conformance

import (
	"bytes"
	"database/sql"
	"debug/buildinfo"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/s2"

	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/harness/conformance/support"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The tests of payload compression (design.md, Store, Compression; Flows,
// Message memory; storage.md, A message's bytes and A stored payload). An
// operator reads what is stored from the database file, as the operator's
// own SQLite tools would, and a client sees what is delivered.

// pageSize is the size over which a payload is compressed: design.md,
// Store, Compression, "A payload over 4 KiB, SQLite's page size".
const pageSize = 4 << 10

// sensorJSON is n bytes of a device's sensor readings in JSON, the kind of
// payload 0031a measured S2 on: realistic, and compressible. Its readings
// come from a fixed seed, so every run sends the same bytes; the last
// reading is cut where n ends.
func sensorJSON(n int) []byte {
	r := rand.New(rand.NewPCG(31, 1))
	b := make([]byte, 0, n+128)
	b = append(b, '[')
	for len(b) < n {
		b = fmt.Appendf(b, `{"device":"pump-%02d","ts":%d,"temperature":%.2f,"humidity":%.1f,"pressure":%.1f,"status":"ok"},`,
			r.IntN(16), 1760000000000+int64(len(b))*7, 18+4*r.Float64(), 40+20*r.Float64(), 990+30*r.Float64())
	}
	return b[:n]
}

// shrinks fails the test unless S2's block encoding (design.md, Store,
// Compression: "compressed with S2"; storage.md, A stored payload, "its S2
// block encoding (github.com/klauspost/compress/s2)") makes p shorter, the
// premise of a payload stored compressed, and returns that encoding's
// length.
func shrinks(t *testing.T, p []byte) int {
	t.Helper()
	n := len(s2.Encode(nil, p))
	if n >= len(p) {
		t.Fatalf("S2 encodes the %d-byte payload in %d bytes; the test needs one it makes shorter", len(p), n)
	}
	return n
}

// storedPayload is a payload as a table holding payloads stores it, with
// its payload_size (storage.md, A stored payload).
type storedPayload struct {
	payload []byte // the stored payload
	size    int    // payload_size: "the payload's size as received"
}

// storedOn reads from db the one payload table, records, wills or
// held_publishes, holds on topic, failing the test unless it holds exactly
// one.
func storedOn(t *testing.T, db *sql.DB, table, topic string) storedPayload {
	t.Helper()
	var ps []storedPayload
	support.Query(t, db, `SELECT payload, payload_size FROM `+table+` WHERE topic = ?`, func(rows *sql.Rows) error {
		var p storedPayload
		err := rows.Scan(&p.payload, &p.size)
		ps = append(ps, p)
		return err
	}, topic)
	if len(ps) != 1 {
		t.Fatalf("%s holds %d payloads on %s; want one", table, len(ps), topic)
	}
	return ps[0]
}

// expectCompressed fails the test unless s is want stored compressed:
// design.md, Store, Compression: "A payload over 4 KiB, SQLite's page size,
// is compressed with S2 and stored compressed only where that makes it
// shorter"; storage.md, A stored payload, payload_size, "the payload's size
// as received. A stored payload shorter than its payload_size is compressed".
// where says which row s is.
func expectCompressed(t *testing.T, s storedPayload, want []byte, where string) {
	t.Helper()
	switch {
	case s.size != len(want):
		t.Errorf("%s: payload_size %d; want %d, the payload's size as received", where, s.size, len(want))
	case len(s.payload) >= s.size:
		t.Errorf("%s: a stored payload of %d bytes, payload_size %d; want it stored compressed, shorter than its size as received",
			where, len(s.payload), s.size)
	default:
		if got, err := s2.Decode(nil, s.payload); err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: the stored payload of %d bytes does not decode as S2's block format to the payload as received (%v)",
				where, len(s.payload), err)
		}
	}
}

// expectAsReceived fails the test unless s is want stored as received:
// storage.md, A stored payload, "the payload as received [...] one of equal
// length is not [compressed]". where says which row s is.
func expectAsReceived(t *testing.T, s storedPayload, want []byte, where string) {
	t.Helper()
	if s.size != len(want) || !bytes.Equal(s.payload, want) {
		t.Errorf("%s: a stored payload of %d bytes, payload_size %d; want the %d bytes as received, and payload_size equal to them",
			where, len(s.payload), s.size, len(want))
	}
}

// design.md, Store, Compression: "A payload over 4 KiB, SQLite's page size,
// is compressed with S2 and stored compressed only where that makes it
// shorter"; "A store reader decompresses it once per read, whatever the
// number of recipients. The core interprets only the payload as received. It
// carries any prepared stored copy opaquely to the store. The connection
// writers handle only the payload as received." storage.md, A stored payload:
// "A stored payload is the payload as received or its S2 block encoding
// (github.com/klauspost/compress/s2)."; payload_size,
// "the payload's size as received. A stored payload shorter than its
// payload_size is compressed". design.md, Channels, append: "a Kafka-like
// topic"; Words: Consumer, "a session's subscription to a channel. It has a
// position".
//
// An append channel, sensors, holds sensors/#. A durable session
// subscribes to it at QoS 1 and goes away; a second client subscribes at
// QoS 1 and stays. A publisher stores two readings of sensor JSON, one of
// 4 KiB and a byte, one of 64 KiB, each answered PUBACK 0x00. The
// connected subscriber receives both, byte for byte; the durable session
// returns and catches up from the store, receiving both byte for byte. The
// database file holds each record's payload shorter than its payload_size,
// which is the payload's length, and S2's block encoding of it.
func TestCompressiblePayloadIsStoredCompressed(t *testing.T) {
	b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "sensors", Type: "append", Filter: "sensors/#"}}})
	catchUp := kept(harness.V5, clientID(t, "-catch-up"))
	support.Away(t, support.Subscribed(t, b.Addr, harness.V5, catchUp, support.At(1, "sensors/#")))
	live := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-live"), CleanStart: true}, support.At(1, "sensors/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	ps := []*harness.Publish{
		{QoS: 1, Topic: "sensors/pump-07", Payload: sensorJSON(pageSize + 1)},
		{QoS: 1, Topic: "sensors/pump-08", Payload: sensorJSON(64 << 10)},
	}
	for _, p := range ps {
		shrinks(t, p.Payload)
	}
	support.PublishAll(t, pub, ps)
	for _, p := range ps {
		support.DeliveredAt(t, live, 1, p.Topic, p.Payload)
	}
	back := resumed(t, b.Addr, harness.V5, catchUp)
	for _, p := range ps {
		support.DeliveredAt(t, back, 1, p.Topic, p.Payload)
	}
	db := support.OpenDB(t, b.DB)
	for _, p := range ps {
		expectCompressed(t, storedOn(t, db, "records", p.Topic), p.Payload, "the record of "+p.Topic)
	}
}

// design.md, Store, Compression: "A payload over 4 KiB, SQLite's page size,
// is compressed with S2 and stored compressed only where that makes it
// shorter". storage.md, A stored payload: "A stored payload is the payload as
// received or its S2 block encoding (github.com/klauspost/compress/s2)."; "A
// stored payload shorter than its payload_size is compressed; one of equal
// length is not".
//
// A subscriber holds plain/# at QoS 1. A publisher stores, each answered
// PUBACK 0x00, sensor JSON of exactly 4 KiB, which S2 would make shorter,
// and 64 KiB of random bytes, which it would not. The subscriber receives
// both byte for byte, and the database file holds each record's payload as
// received, its payload_size its length.
func TestPayloadIsStoredAsReceived(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, "plain/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	atLimit := &harness.Publish{QoS: 1, Topic: "plain/at-limit", Payload: sensorJSON(pageSize)}
	random := &harness.Publish{QoS: 1, Topic: "plain/random", Payload: support.Noise(64 << 10)}
	shrinks(t, atLimit.Payload)
	support.Unshrunk(t, random.Payload)
	ps := []*harness.Publish{atLimit, random}
	support.PublishAll(t, pub, ps)
	for _, p := range ps {
		support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	}
	db := support.OpenDB(t, b.DB)
	for _, p := range ps {
		expectAsReceived(t, storedOn(t, db, "records", p.Topic), p.Payload, "the record of "+p.Topic)
	}
}

// design.md, Store, Compression: "The connection's reader compresses the
// payload, including a Will's payload, before handing it to the core"; "A payload over 4 KiB
// [...] is compressed with S2 and stored compressed only where that makes
// it shorter". storage.md, A stored payload: "Every table holding a payload
// has payload_size"; wills: payload "the stored Will Payload", payload_size
// "the Will Payload's size as received"; held_publishes: payload and
// payload_size "as in records". design.md, Flows, PUBLISH: QoS 2 "enters
// on PUBREL, from a held copy stored and charged to the channel before
// PUBREC"; DISCONNECT, A lost connection: the Will "is read from the store
// when it goes".
//
// A subscriber holds will/# and held/# at QoS 1. A client connects with a
// Will of 10 KiB of sensor JSON on will/pump-07, and the database file holds
// its Will Payload compressed. The client drops its socket, and the
// subscriber receives the Will byte for byte. A publisher then sends 12 KiB
// of sensor JSON at QoS 2 on held/pump-08, PUBREC 0x00: the database
// file holds the held copy compressed. Its PUBREL gets PUBCOMP 0x00, and
// the subscriber receives it byte for byte.
func TestWillAndHeldQoS2AreStoredCompressed(t *testing.T) {
	b := support.Launch(t, &harness.Options{})
	db := support.OpenDB(t, b.DB)
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, "will/#", "held/#"))

	will := &harness.Will{Topic: "will/pump-07", Payload: sensorJSON(10 << 10)}
	shrinks(t, will.Payload)
	willer, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-will"), CleanStart: true, Will: will})
	expectCompressed(t, storedOn(t, db, "wills", will.Topic), will.Payload, "the Will on "+will.Topic)
	if err := willer.Close(); err != nil {
		t.Fatal(err)
	}
	live(t, sub, will.Topic, will.Payload)

	held := &harness.Publish{QoS: 2, Topic: "held/pump-08", Payload: sensorJSON(12 << 10)}
	shrinks(t, held.Payload)
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	support.Sent(t, pub, held)
	pubrecOf(t, pub, held, 0x00)
	expectCompressed(t, storedOn(t, db, "held_publishes", held.Topic), held.Payload, "the held copy on "+held.Topic)
	pubrel(t, pub, held.ID, 0x00)
	liveAt(t, sub, 1, held.Topic, held.Payload)
}

// design.md, Store, Compression: "Retention, the size limits and the
// Maximum Packet Size checks count the payload's size as received".
// storage.md, A message's bytes: "its topic's, payload's and properties'
// bytes, the payload counted at its size as received. Retention and the
// size limits count those bytes"; channels, bytes: "the bytes of its
// records". design.md, Flows, PUBLISH: "0x97 | over [...] the retained
// values' limit, a channel's max_bytes"; Channels, $broadcast: "Retained
// values have their own size limit. A value that does not fit is refused
// with 0x97"; Words: Retention, "retention deletes a channel's oldest
// records by age and size".
//
// Every publication carries 8 KiB of sensor JSON and no property, and each
// limit is 12 KiB: two such messages pass it, while two of their S2
// encodings would fit. Each case runs on a broker of its own, and in each
// the database file holds the record kept stored compressed, so what is
// refused or deleted is so at the size as received:
//   - a channel's max_bytes: the first publication on capped is taken,
//     0x00, and channels holds its bytes at the payload's size as received;
//     the second is refused, 0x97.
//   - the retained values' limit: a retained value on kept/a is taken, one
//     on kept/b refused, 0x97.
//   - retention: aged keeps 12 KiB. Once a second record is stored, the
//     size pass leaves aged/2 alone.
func TestSizeLimitsCountThePayloadAsReceived(t *testing.T) {
	const limit = 12 << 10
	payload := sensorJSON(8 << 10)
	premise := func(t *testing.T, topic string) {
		t.Helper()
		if n := 2 * (len(topic) + shrinks(t, payload)); n > limit {
			t.Fatalf("two S2 copies on %s make %d bytes; the test needs them within %d", topic, n, limit)
		}
	}
	t.Run("a channel's max_bytes", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{Channels: capped(limit)})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		premise(t, "capped/1")
		answered(t, c, &harness.Publish{QoS: 1, Topic: "capped/1", Payload: payload}, 0x00)
		if got, want := storedChannels(t, b.DB)["capped"].bytes, int64(len("capped/1")+len(payload)); got != want {
			t.Errorf("channels holds bytes %d for capped; want %d, its record's topic and payload as received", got, want)
		}
		answered(t, c, &harness.Publish{QoS: 1, Topic: "capped/2", Payload: payload}, 0x97)
		expectCompressed(t, storedOn(t, support.OpenDB(t, b.DB), "records", "capped/1"), payload, "the record of capped/1")
	})
	t.Run("the retained values' limit", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{RetainedMaxBytes: limit})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		premise(t, "kept/a")
		answered(t, c, retain("kept/a", payload), 0x00)
		answered(t, c, retain("kept/b", payload), 0x97)
		expectCompressed(t, storedOn(t, support.OpenDB(t, b.DB), "records", "kept/a"), payload, "the record of kept/a")
	})
	t.Run("retention", func(t *testing.T) {
		b := support.Launch(t, &harness.Options{Channels: []harness.Channel{{Name: "aged", Type: "append", Filter: "aged/#", RetentionBytes: limit}}})
		c, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, ""), CleanStart: true})
		premise(t, "aged/1")
		support.PublishAll(t, c, []*harness.Publish{{QoS: 1, Topic: "aged/1", Payload: payload}, {QoS: 1, Topic: "aged/2", Payload: payload}})
		db := support.OpenDB(t, b.DB)
		awaitHeld(t, db, "aged", []string{"aged/2"}, "once aged/2 has made two records of 8 KiB as received")
		expectCompressed(t, storedOn(t, db, "records", "aged/2"), payload, "the record of aged/2")
	})
}

// tooLargeSeries is saguin_missed_total of channel, cause too_large
// (metrics.md, Delivery).
func tooLargeSeries(channel string) string {
	return testkit.SeriesKey("saguin_missed_total", "channel", channel, "cause", "too_large")
}

// design.md, Store, Compression: "the Maximum Packet Size checks count the
// payload's size as received"; Delivery: "A message over the client's
// Maximum Packet Size is discarded for that subscriber as if sent, counted
// as missed and logged (MQTT-3.1.2-25)"; Words: Missed, "missed records are
// those a consumer was owed but never received because [...] they were too
// large for it (too_large)". metrics.md: saguin_missed_total{channel,cause}.
// MQTT 5 [MQTT-3.1.2-24]: "The Server MUST NOT send packets exceeding
// Maximum Packet Size to the Client".
//
// A subscriber whose CONNECT sent a Maximum Packet Size of 8 KiB, which the
// harness client holds the broker to, and another with none hold size/# at
// QoS 0. A publisher stores 10 KiB of sensor JSON on size/big, whose
// PUBLISH would fit within 8 KiB carrying S2's encoding of it, then a small
// message on size/next. The first subscriber receives size/next alone, the
// other both; /metrics counts one record missed on $broadcast as
// too_large, and the database file holds size/big stored compressed.
func TestMaximumPacketSizeCountsThePayloadAsReceived(t *testing.T) {
	const limit = 8 << 10
	big := &harness.Publish{QoS: 1, Topic: "size/big", Payload: sensorJSON(10 << 10)}
	if n := len(harness.Encoded(t, &harness.Publish{Topic: big.Topic, Payload: s2.Encode(nil, big.Payload)}, harness.V5)); n > limit {
		t.Fatalf("a PUBLISH carrying S2's encoding of the payload is %d bytes; the test needs it within %d", n, limit)
	}
	b := support.Launch(t, &harness.Options{Operations: true})
	small := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-small"), CleanStart: true,
		Props: harness.Properties{{ID: harness.MaximumPacketSize, Int: limit}}}, support.At(0, "size/#"))
	other := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-other"), CleanStart: true}, support.At(0, "size/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	next := &harness.Publish{QoS: 1, Topic: "size/next", Payload: []byte("next")}
	support.PublishAll(t, pub, []*harness.Publish{big, next})
	delivered(t, other, big.Topic, big.Payload)
	delivered(t, other, next.Topic, next.Payload)
	delivered(t, small, next.Topic, next.Payload)
	awaitSeries(t, b, map[string]float64{tooLargeSeries("$broadcast"): 1})
	expectCompressed(t, storedOn(t, support.OpenDB(t, b.DB), "records", big.Topic), big.Payload, "the record of "+big.Topic)
}

// design.md, Flows, Message memory, Asking: "A body over 4 KiB also asks
// for room for the largest copy S2 can make of it, so its payload can be
// compressed within the reservation (Compression)"; config.md,
// max_incoming_memory: "It must hold the largest body one reader can ask
// for: max_packet_size, plus the largest S2 copy of it where
// max_packet_size is over 4 KiB". The largest copy S2 can make of n bytes
// is s2.MaxEncodedLen(n), the library storage.md names.
//
// With max_packet_size 64 KiB and max_incoming_memory just large enough, 64
// KiB plus the largest S2 copy of it, a subscriber holds memory/# at QoS 1,
// and a publisher sends a QoS 1 PUBLISH of exactly max_packet_size carrying
// sensor JSON. It is answered PUBACK 0x00, the subscriber receives it byte
// for byte, and the database file holds its payload compressed.
func TestLeastIncomingMemoryHoldsTheLargestS2Copy(t *testing.T) {
	const packet = 64 << 10
	b := support.Launch(t, &harness.Options{MaxPacketSize: packet, MaxIncomingMemory: leastIncoming(packet)})
	sub := support.Subscribed(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-sub"), CleanStart: true}, support.At(1, "memory/#"))
	pub, _ := harness.Connected(t, b.Addr, harness.V5, &harness.Connect{ClientID: clientID(t, "-pub"), CleanStart: true})
	// A fixed header of 4 (a Remaining Length of three bytes), the Topic
	// Name's 2+11, the Packet Identifier's 2, an empty property section's 1,
	// and the payload.
	p := &harness.Publish{QoS: 1, ID: pub.NextID(), Topic: "memory/full", Payload: sensorJSON(packet - 4 - 13 - 2 - 1)}
	if n := len(harness.Encoded(t, p, harness.V5)); n != packet {
		t.Fatalf("the PUBLISH is %d bytes, want max_packet_size, %d", n, packet)
	}
	shrinks(t, p.Payload)
	support.Acknowledged(t, pub, p)
	support.DeliveredAt(t, sub, 1, p.Topic, p.Payload)
	expectCompressed(t, storedOn(t, support.OpenDB(t, b.DB), "records", p.Topic), p.Payload, "the record of "+p.Topic)
}

// compressionModule is the library design.md, Store, Compression
// compresses with (storage.md, A stored payload:
// "github.com/klauspost/compress/s2").
const compressionModule = "github.com/klauspost/compress"

// design.md, Operations: "saguin --licenses [...] | prints the licences of
// the code inside"; Store, Compression: a payload "is compressed with S2";
// storage.md, A stored payload: "(github.com/klauspost/compress/s2)". 0031a,
// 261031 070347: "Its LICENSE, with the Snappy BSD notice S2 derives from,
// goes into cmd/saguin/THIRD-PARTY-NOTICES.md (embedded for --licenses)".
//
// The shipped binary's build information records the compression module
// as linked into it, and its --licenses, exit 0, prints a section headed
// "## " and the module's path and version holding the module's LICENSE as
// it is in the module, Snappy's notice among it.
func TestLicensesCarryTheCompressionLibrary(t *testing.T) {
	info, err := buildinfo.ReadFile(support.BrokerBinary)
	if err != nil {
		t.Fatal(err)
	}
	var version string
	for _, m := range info.Deps {
		if m.Path == compressionModule {
			version = m.Version
		}
	}
	if version == "" {
		t.Fatalf("%s holds no %s: payloads over 4 KiB are compressed with its S2", support.BrokerBinary, compressionModule)
	}
	code, cache, stderr := execute(t, "go", "env", "GOMODCACHE")
	if code != 0 {
		t.Fatalf("go env GOMODCACHE: exit %d, %s", code, stderr)
	}
	licence, err := os.ReadFile(filepath.Join(strings.TrimSpace(cache), compressionModule+"@"+version, "LICENSE"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(licence, []byte("The Snappy-Go Authors")) {
		t.Fatalf("%s %s's LICENSE holds no Snappy notice", compressionModule, version)
	}
	code, stdout, stderr := execute(t, support.BrokerBinary, "--licenses")
	if code != 0 || stderr != "" {
		t.Fatalf("--licenses: exit %d, stderr %q; want 0 and nothing", code, stderr)
	}
	section, ok := noticeSection(stdout, func(heading string) bool { return heading == compressionModule+" "+version })
	if !ok || !strings.Contains(section, strings.TrimSpace(string(licence))) {
		t.Errorf("--licenses prints no section \"## %s %s\" holding its LICENSE", compressionModule, version)
	}
}
