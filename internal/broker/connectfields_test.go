package broker_test

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/brokertest"
	"github.com/ifnesi/saguin/internal/mqtt"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
)

// refusalCounter counts OnConnectRefused. Registered after saguin's hooks,
// it is told of a refusal after saguin's own OnConnectRefused has run in
// the same dispatch, so a count here is a refusal saguin's hook came through.
type refusalCounter struct {
	mqtt.HookBase
	n atomic.Int64
}

func (h *refusalCounter) ID() string           { return "test-refusals" }
func (h *refusalCounter) Provides(b byte) bool { return b == mqtt.OnConnectRefused }
func (h *refusalCounter) OnConnectRefused(*mqtt.Client, packets.Packet, packets.Code) {
	h.n.Add(1)
}

// A packet carries a CONNECT's own fields only if it is a CONNECT
// (packets.ConnectParams, nil on every other packet), so a hook that reads
// them off a packet that may be something else must ask first. The one the
// substrate hands a packet that need not be a CONNECT is OnConnectRefused: a
// connection that opens with any other packet is refused [MQTT-3.1.0-1],
// and so is one whose CONNECT was cut off, oversized or malformed.
//
// **Every packet the codec's own table holds, on every path a client can
// put it**: as the first packet on a connection, and after a CONNECT was
// accepted - where it reaches OnPacketRead, OnPublish, OnSubscribe and the
// rest, and a second CONNECT is a protocol error. A hook that read a
// CONNECT field off any of them unasked would panic, and nothing in the
// substrate recovers one, so the test binary would stop here.
//
// The refusals are counted, so a sweep that stopped reaching
// OnConnectRefused - the one hook these packets are there for - fails
// rather than passes on nothing.
func TestNoHookReadsCONNECTFieldsOffAnotherPacket(t *testing.T) {
	h := brokertest.Start(t)
	counter := new(refusalCounter)
	if err := h.Srv.AddHook(counter, nil); err != nil {
		t.Fatal(err)
	}
	refused := func() int64 { return counter.n.Load() }
	// send writes raw and half-closes the connection, so that a packet
	// declaring more than it carries meets the end of the stream rather
	// than waiting for bytes, and reads what the broker sends until it
	// closes the connection.
	send := func(conn net.Conn, name string, raw []byte) (sent []byte) {
		t.Helper()
		if _, err := conn.Write(raw); err != nil {
			t.Fatalf("%s: write: %v", name, err)
		}
		_ = conn.(*net.TCPConn).CloseWrite()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		for {
			first, _, err := brokertest.ReadRawPacket(conn)
			if err != nil {
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					t.Fatalf("%s: the broker did not close the connection in 5s; it sent %#x", name, sent)
				}
				return sent
			}
			sent = append(sent, first)
		}
	}

	var types []byte
	for typ := range packets.TPacketData {
		types = append(types, typ)
	}
	slices.Sort(types)

	cases, refusals := 0, 0
	for _, typ := range types {
		for _, tc := range packets.TPacketData[typ] {
			if len(tc.RawBytes) == 0 {
				continue
			}
			cases++
			name := fmt.Sprintf("%s %q", packets.PacketNames[typ], tc.Desc)

			// As the first packet on a connection.
			conn, err := net.Dial("tcp", h.Addr)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			before := refused()
			sent := send(conn, name+" first", tc.RawBytes)
			_ = conn.Close()
			// The hook runs on the broker's goroutine for the connection,
			// before or after the close reaches the client, so its count is
			// waited for rather than read once.
			// By its own first byte rather than the table it is in: a few of
			// the table's malformed cases are other packets' bytes.
			if tc.RawBytes[0]>>4 != packets.Connect {
				deadline := time.Now().Add(5 * time.Second)
				for refused() == before && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				if refused() == before {
					t.Fatalf("%s as a connection's first packet was not counted refused; the broker sent %#x", name, sent)
				}
				refusals++
			}

			// After a CONNECT the broker accepted.
			c, _ := connectRawAs(t, h, fmt.Sprintf("hooks-%d-%d", typ, tc.Case), 10, true)
			send(c.conn, name+" after CONNECT", tc.RawBytes)
			_ = c.conn.Close()
		}
	}

	// A CONNECT refused from its opening bytes for its declared size, and
	// one cut off before its body ends.
	for _, raw := range [][]byte{
		{packets.Connect << 4, 0x80, 0x80, 0x40, 0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 60, 0, 0, 1, 'x'},
		{packets.Connect << 4, 20, 0, 4, 'M'},
	} {
		conn, err := net.Dial("tcp", h.Addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		send(conn, fmt.Sprintf("a CONNECT of %d bytes declaring more", len(raw)), raw)
		_ = conn.Close()
	}

	t.Logf("%d packet types, %d cases each sent first and after a CONNECT; %d refusals counted", len(types), cases, refusals)
	if len(types) != 15 || refusals < 100 {
		t.Fatalf("the sweep reached %d packet types and %d refusals in %d cases; "+
			"it did not exercise what it is for", len(types), refusals, cases)
	}

	// And the broker is still here to answer.
	c, _ := connectRawAs(t, h, "after-the-sweep", 10, true)
	_ = c.conn.Close()
}
