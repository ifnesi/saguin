package core_test

import (
	"net"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/core"
	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/store"
)

// answered answers every store request among acts, a write saved and a
// read finding nothing, its view confirmed first (design.md, Store reads),
// and every request those answers lead to, all at now, and returns the
// replies among them all. A core still asking after
// settleLimit answers is asking without end.
func answered(tb testing.TB, c *core.Core, acts []core.Action, now time.Time) []core.Action {
	tb.Helper()
	var replies []core.Action
	for n := 0; len(acts) != 0; n++ {
		if n == settleLimit {
			tb.Fatalf("the core still asks the store after %d answers", n)
		}
		a := acts[0]
		acts = acts[1:]
		switch {
		case a.Reply != nil:
			replies = append(replies, a)
		case a.Write != nil:
			acts = c.Answer(acts, store.Answer{Op: a.Write}, now)
		case a.Read != nil:
			acts = c.Answer(acts, store.Answer{Op: a.Read, View: true}, now)
			acts = c.Answer(acts, store.Answer{Op: a.Read}, now)
		}
	}
	return replies
}

// broadcast is the store's startup state with $broadcast, channel 1
// (storage.md, Ids: "The $broadcast channel is channel 1"), as a start finds
// it in an existing database.
var broadcast = store.State{Channels: []store.Channel{{ID: 1, Name: "$broadcast", Type: store.Append, Next: 1, Floor: 1}}}

// design.md, Execution, Capacities: the core's inbound channel, store
// credit, reply slots and the logger's channel: "These are fixed, not
// configuration"; Testing: "An inside test therefore drives it
// with scripted events and fake times". 0009a.md, 261009 211151: "before R2's
// capacities are set here, measure the core alone, with scripted events and
// scripted store answers, no network and no SQLite: events per second, time
// per event (median and 99th percentile), allocations per event [...] One
// publish is several core events".
//
// One MQTT 5 client, connected, publishes into $broadcast at QoS 1, one
// message at a time: each publish is three core events, the reader's ask
// for its body's bytes (BytesRequested: design.md, Message memory, "Every
// such body uses this request"), its *packet.Publish (Received) once given
// them, and the store's answer saving the write the core asks for it,
// which leads to the publish's reply to the client, its PUBACK. The benchmark reports events per second, nanoseconds per event
// (the mean, and the median and 99th percentile of each event timed on its
// own, its timing included) and allocations per event.
func BenchmarkPublishEvents(b *testing.B) {
	reg := metrics.New()
	cfg := newConfig(b)
	c := core.New(cfg, reg, &broadcast, nil, nil, nil, logger.NewCredits(cfg.Broker.LogLevel, reg))
	now := epoch
	answered(b, c, c.Fire(nil, now), now)
	conn := new(connection.Conn)
	accept := connection.Accept{Listener: "bench", Remote: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1883}}
	answered(b, c, c.Handle(nil, connection.Event{Kind: connection.Accepted, Conn: conn, Packet: accept}, now), now)
	replies := answered(b, c, c.Handle(nil, connection.Event{Kind: connection.Received, Conn: conn, Packet: connectOf(packet.V5, "bench")}, now), now)
	if ack := connackIn(replies, conn); ack == nil || ack.ReasonCode != 0 {
		b.Fatalf("the CONNECT answered with %v, want CONNACK 0x00", replies)
	}

	payload := []byte("0123456789abcdef")
	const topic = "bench/topic"
	// The body: the Topic Name, the Packet Identifier, an empty property
	// section and the payload.
	body := int64(2 + len(topic) + 2 + 1 + len(payload))
	publishes := make([]packet.Publish, b.N)
	for i := range publishes {
		publishes[i] = packet.Publish{QoS: 1, PacketID: uint16(i%65_535 + 1), Topic: []byte(topic), Payload: payload}
	}
	times := make([]time.Duration, 0, 3*b.N)
	handled := make([]core.Action, 0, 8)
	answers := make([]core.Action, 0, 8)
	var before, after runtime.MemStats
	b.ReportAllocs()
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for i := range publishes {
		now = now.Add(time.Microsecond)
		start := time.Now()
		handled = c.Handle(handled[:0], connection.Event{Kind: connection.BytesRequested, Conn: conn, Bytes: body}, now)
		times = append(times, time.Since(start))
		if len(handled) != 1 || handled[0].Conn != conn || handled[0].Bytes != body {
			b.Fatalf("an ask for a body of %d bytes, nothing reserved, was answered %+v; want the bytes given", body, handled)
		}
		now = now.Add(time.Microsecond)
		start = time.Now()
		handled = c.Handle(handled[:0], connection.Event{Kind: connection.Received, Conn: conn, Packet: &publishes[i], Bytes: body}, now)
		times = append(times, time.Since(start))
		var write store.Write
		for _, a := range handled {
			if a.Write != nil {
				write = a.Write
			}
		}
		if write == nil {
			b.Fatalf("a QoS 1 PUBLISH into $broadcast asked the store for no write: %+v", handled)
		}
		now = now.Add(time.Microsecond)
		start = time.Now()
		answers = c.Answer(answers[:0], store.Answer{Op: write}, now)
		times = append(times, time.Since(start))
		if !slices.ContainsFunc(answers, func(a core.Action) bool { return a.Conn == conn && a.Reply != nil }) {
			b.Fatalf("the store's answer to a QoS 1 PUBLISH's write sent the publisher no reply: %+v", answers)
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&after)

	events := float64(len(times))
	slices.Sort(times)
	b.ReportMetric(events/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/events, "ns/event")
	b.ReportMetric(float64(times[len(times)/2].Nanoseconds()), "p50-ns/event")
	b.ReportMetric(float64(times[len(times)*99/100].Nanoseconds()), "p99-ns/event")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/events, "allocs/event")
}
