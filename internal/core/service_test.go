package core_test

import (
	"bytes"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/payload"
	"github.com/ifnesi/saguin/internal/store"
	"github.com/ifnesi/saguin/internal/testkit"
)

// The core's side of a $saguin/kv/get whose value changes while the request
// runs (design.md, $saguin topics, A kv/get): when the value is replaced
// between its size read and its load is an ordering no client can force
// (Testing: "orderings no client can force"), so the test drives the core
// with scripted events, a real store behind it (liveScript).
//
// design.md, $saguin topics: "A kv/get reads the value's size first. The
// core reserves outgoing memory for it, then the value is loaded (R6)." The
// scripted events express that ordering as follows:
//
//   - the reader hands the core a kv/get as a *connection.RequestPublish of
//     its own kind, connection.KVGetRequest, the PUBLISH whose payload is
//     the topic asked for, Channel the routing index of the latest channel
//     that topic lands in (the reader refuses a topic on no latest channel
//     with 0x90, so it has resolved it), and the Response Topic and
//     Correlation Data as for a seek;
//   - the core reads the value's size with one store read, then loads it
//     with the answer's payload read (service.go's serviceBody.Load), a
//     load of a larger value reporting that value's size and being sent
//     again once the request has reserved for it. The test names neither
//     read's type: it counts the reads the core sends.

// valuesChannel is a latest channel holding values/#.
const valuesChannel = `channels:
  values:
    type: latest
    filter: "values/#"
`

// stored is the PUBLISH of value on topic, QoS 1 with id, as a reader hands
// it to the core: a value over SQLite's page size carried as its prepared S2
// copy alone, which is stored, its size as received kept (design.md, Store,
// Compression: "The connection's reader compresses the payload [...] before
// handing it to the core"); and the body's bytes the reader holds for it.
func stored(topic string, id uint16, value []byte) (p *packet.Publish, body int64) {
	p = &packet.Publish{QoS: 1, PacketID: id, Topic: []byte(topic), Payload: value}
	return p, compressed(p)
}

// compressed prepares p's payload as its reader does before handing it to
// the core, carrying a payload over SQLite's page size as its S2 copy alone
// where that is shorter, and returns the body's bytes the reader keeps for
// it (design.md, Message memory, Asking: "Once compressed, the body keeps
// only the room of the copy it stores, and the rest goes back").
func compressed(p *packet.Publish) int64 {
	if encoded := payload.Compress(p.Payload); encoded != nil {
		p.StoredPayload, p.PayloadSize, p.Payload = payload.Compact(encoded), len(p.Payload), nil
	}
	return bodyOf(p) + int64(len(p.StoredPayload))
}

// measuredAlone names the environment variable set in the run of the test
// binary TestKVGetAnswersTheNewestValue makes for itself, alone; and
// measuredAloneLimit bounds that run.
const (
	measuredAlone      = "SAGUIN_KV_GET_MEASURED_ALONE"
	measuredAloneLimit = 2 * time.Minute
)

// loadSlack is what a store read may allocate for its own work, its query
// and its answer, beside the value it hands back: far below the values the
// test loads, each a mebibyte or more.
const loadSlack = 64 << 10

// noise is n bytes no compression makes shorter, the same for every n
// (design.md, Store, Compression: "stored compressed only where that makes
// it shorter"), and halfNoise n bytes, half of them noise and half zeros,
// which S2 makes about half as long.
func noise(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(26111105, 0))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func halfNoise(n int) []byte {
	return append(noise(n/2), make([]byte, n-n/2)...)
}

// design.md, $saguin topics, A kv/get: "A kv/get reads the value's size
// first. The core reserves outgoing memory for it, then the value is loaded
// (R6). A value replaced in between by one that fits the reservation is
// answered at once. A larger one makes the load report its size, and the
// request reserves again for that size, so the answer is the newest value.
// A value deleted in between gets the empty answer." Message memory, Two
// queues: "A delivery of a compressed payload also reserves the stored
// copy's size. That reservation is given back once the store reader has
// decompressed it"; "A reservation is taken from one budget and given back
// to that same budget"; Bytes back: "A delivery reservation ends when the
// writer reports completion or discard". Store, Compression: a payload over
// 4 KiB "is compressed with S2 and stored compressed only where that makes
// it shorter"; "A store reader decompresses it once per read"; Testing: the
// busy path "avoids every copy or allocation it can".
//
// A value's reservation is its answer's size and, stored compressed, its
// stored copy's: a replacement fits where its own two are no more. Each
// case stores values/a, then a client sends a QoS 1 kv/get of values/a with
// a Response Topic; the core's first store read for it, the size read, is
// carried out, and before its result reaches the core a publisher's QoS 1
// PUBLISH replaces values/a and is committed. The values, of a few
// mebibytes, are zeros (stored compressed, its copy small), noise (stored
// as received) or half noise (stored compressed to about half):
//   - replacements that fit: a smaller compressed one; a larger value whose
//     stored copy is smaller, which fits by the copy (audit 261111, review:
//     12,288 bytes compressed to 6,156 replaced by 14,336 compressed to 10
//     stopped the core); compressed to stored as received and back; one
//     stored as received replacing a larger one;
//   - replacements that do not: larger each way between compressed and
//     stored as received, and a smaller value whose stored copy makes it
//     not fit;
//   - a deletion, of a value stored compressed and of one stored as
//     received.
//
// The answer on the Response Topic is then the replacement, empty for the
// deletion, the core having sent, after the size read, one load for a
// replacement that fits and for the deletion, and two for one that does
// not. The core does not stop. While the store carries out each of the
// kv/get's reads, the broker allocates no more than the value that read
// hands back, decompressed once, and no more than the memory reserved for
// it, each with loadSlack beside, the stored copy it reads besides the
// value: a read handing back no value (the size read, a load finding the
// value larger than its reservation) materialises none. That is checked in
// every case, whatever the representation of the value read: one stored
// compressed is its stored copy and its decompressed body, one stored as
// received is its body alone, read once and handed on without another copy
// (Store: "A store reader hands a stored payload on as the driver gives it,
// without copying it again"). Once the answer's writer has reported it,
// nothing is
// in transit in the outgoing budget (metrics.md, Store and memory:
// saguin_memory_in_transit_bytes{direction}, "outstanding reservations of
// each budget").
func TestKVGetAnswersTheNewestValue(t *testing.T) {
	// What a read allocates is measured over the whole process, so the
	// test runs alone in a run of the test binary of its own, where no
	// other test allocates meanwhile.
	if os.Getenv(measuredAlone) == "" {
		testkit.Rerun(t, "the run measuring the kv/get's reads alone", measuredAloneLimit, []string{measuredAlone + "=1"})
		return
	}
	const asked, answers = "values/a", "answers/kv"
	const mib = 1 << 20
	zeros := func(n int) []byte { return make([]byte, n) }
	for _, tc := range []struct {
		name                  string
		original, replacement []byte
		fits                  bool // false: does not fit; a deletion needs no room
	}{
		{"compressed, by a smaller compressed value", zeros(2 * mib), zeros(mib), true},
		{"compressed, by a larger value with a smaller stored copy", halfNoise(3 * mib), zeros(7 * mib / 2), true},
		{"stored as received, by a compressed value", noise(2 * mib), zeros(mib), true},
		{"compressed, by a larger value stored as received that fits", halfNoise(3 * mib), noise(4 * mib), true},
		{"stored as received, by a smaller one", noise(2 * mib), noise(mib), true},
		{"compressed, by a larger compressed value", zeros(2 * mib), zeros(4 * mib), false},
		{"stored as received, by a larger compressed value", noise(mib), halfNoise(2 * mib), false},
		{"compressed, by a larger value stored as received", zeros(mib), noise(2 * mib), false},
		{"stored as received, by a larger one", noise(mib), noise(2 * mib), false},
		{"compressed, by a smaller value with a larger stored copy", zeros(4 * mib), halfNoise(3 * mib), false},
		{"compressed, deleted", zeros(2 * mib), nil, false},
		{"stored as received, deleted", noise(2 * mib), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newLiveScript(t, valuesChannel)
			pub := s.connect("kv-pub", 0, 10)
			asker := s.connect("kv-asker", 0, 10)
			original, n := stored(asked, 1, tc.original)
			// The reservation each value needs beyond the answer's same
			// header: its payload and its stored copy.
			need := func(p *packet.Publish) int { return max(len(p.Payload), p.PayloadSize) + len(p.StoredPayload) }
			loads := 1 // the reads the core sends after the size read
			probe, _ := stored(asked, 2, tc.replacement)
			if tc.replacement != nil {
				if fits := need(probe) <= need(original); fits != tc.fits {
					t.Fatalf("rig: the replacement needs %d bytes against the original's %d, fitting %v; the case wants %v", need(probe), need(original), fits, tc.fits)
				}
				if !tc.fits {
					loads = 2
				}
			}
			s.hand(pub, n, original)
			s.serve()

			index := config.NewRouting(s.cfg.Channels).Resolve(asked)
			if index < 0 {
				t.Fatalf("%s lands in no configured channel", asked)
			}
			p := packet.Publish{QoS: 1, PacketID: 1, Topic: []byte("$saguin/kv/get"), Payload: []byte(asked)}
			correlation := []byte("kv")
			readerBody(t, s.cfg, &p, packet.Property{ID: packet.ResponseTopic, String: answers},
				packet.Property{ID: packet.CorrelationData, Binary: correlation})
			q := &connection.RequestPublish{Kind: connection.KVGetRequest, Publish: p, Channel: index, ResponseTopic: answers, Correlation: correlation}

			// The reads the core sends, and the answers it hands the asker's
			// writer, each kept as handed over: the core reuses a delivery's
			// slot once its writer reports it.
			var reads []store.Read
			var got [][]byte
			s.nudge = func(acts []core.Action) {
				for _, a := range acts {
					if a.Read != nil {
						reads = append(reads, a.Read)
					}
					if d, ok := a.Reply.(*connection.Delivery); ok && a.Conn == asker && string(d.Topic) == answers {
						got = append(got, bytes.Clone(d.Payload))
					}
				}
			}
			// Each of the kv/get's reads, carried out: what the broker
			// allocated meanwhile, against the value it hands back and the
			// memory reserved for it, read before the core takes the answer.
			s.measured = func(op store.Read, allocated uint64) {
				r, ok := op.(*store.ReadCurrentValue)
				if !ok {
					return
				}
				// A value handed back is the replacement, read with its
				// stored copy where it is stored compressed (Message memory:
				// "A delivery of a compressed payload also reserves the
				// stored copy's size"), alone where stored as received.
				var handed, copied int
				if r.Into != nil {
					handed = len(*r.Into)
				}
				if handed != 0 {
					copied = len(probe.StoredPayload)
				}
				if allocated > uint64(handed+copied)+loadSlack {
					t.Errorf("read %d of the kv/get allocated %d bytes and handed back a value of %d from a stored copy of %d: a value materialised beyond the one answered, copied again or decompressed twice",
						len(reads), allocated, handed, copied)
				}
				if r.Into != nil && allocated > uint64(r.Budget)+loadSlack {
					t.Errorf("load %d of the kv/get allocated %d bytes with %d reserved for it: the value loaded before its memory was reserved",
						len(reads)-1, allocated, r.Budget)
				}
			}
			s.answer = func(a store.Answer) []core.Action {
				if len(reads) == 0 || a.Op != reads[0] || a.View {
					return s.core.Answer(nil, a, s.now)
				}
				s.answer = nil // the size read's result: the value is replaced first
				replacement, n := stored(asked, 2, tc.replacement)
				s.hand(pub, n, replacement)
				i := slices.IndexFunc(s.queue, func(op any) bool { _, ok := op.(*core.Group); return ok })
				if i < 0 {
					t.Fatalf("the replacement of %s, the kv/get's size read viewed, sent no write", asked)
				}
				g := s.queue[i].(*core.Group)
				s.queue = slices.Delete(s.queue, i, i+1)
				s.commit(g)
				return s.core.Answer(nil, a, s.now)
			}
			defer func() {
				if failure := recover(); failure != nil {
					t.Fatalf("the core stopped answering the kv/get: %v", failure)
				}
			}()
			s.hand(asker, bodyOf(&p), q)
			if len(reads) != 1 {
				t.Fatalf("the kv/get of %s sent %d store reads; want one, its size read", asked, len(reads))
			}
			s.serve()

			if s.answer != nil {
				t.Fatalf("the kv/get's size read was never answered")
			}
			if got := len(reads) - 1; got != loads {
				t.Errorf("after the size read the core sent %d reads; want %d loads of the value", got, loads)
			}
			switch {
			case len(got) != 1:
				t.Fatalf("the asker was handed %d answers on %s; want one", len(got), answers)
			case !bytes.Equal(got[0], tc.replacement):
				t.Fatalf("the answer is %d bytes, %.20q; want the newest value, %d bytes, %.20q",
					len(got[0]), got[0], len(tc.replacement), tc.replacement)
			}
			if held := inTransit(t, s.reg, "outgoing"); held != 0 {
				t.Errorf("the answer reported written, %d bytes are in transit in the outgoing budget; want none", held)
			}
		})
	}
}
