// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co
// SPDX-FileContributor: Italo Nesi

package mqtt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/store"

	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/mqtt/system"
	"github.com/ifnesi/saguin/internal/sourcetree"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

var logger = slog.New(slog.NewTextHandler(io.Discard, nil))

// The acknowledgements this broker puts on the wire for three refusals.
//
// They are written here rather than taken from packets.TPacketData because
// that table describes packets that are *encodable*, and its MQTT 5
// acknowledgements carry the User Property a request carried. This broker
// sends none: buildAck drops what the request held and keeps only its own
// Reason String, for the reason set out there.
//
// So each of these is its table entry with the User Property section
// removed - the `38` identifier and the key and value that follow it - and
// the two lengths reduced by those sixteen bytes. Written out in full
// rather than derived from the fixture at run time, because a test that
// computes its own expectation from the thing it is checking has stopped
// being a check.
var (
	// PUBACK, packet id 7, 0x87 with "not authorized".
	ackPubackNotAuthorized = []byte{
		packets.Puback << 4, 21,
		0, 7, // packet id
		packets.ErrNotAuthorized.Code,
		17, // properties length
		31, 0, 14, 'n', 'o', 't', ' ', 'a', 'u',
		't', 'h', 'o', 'r', 'i', 'z', 'e', 'd', // reason string
	}

	// PUBREC, the same refusal at QoS 2.
	ackPubrecNotAuthorized = []byte{
		packets.Pubrec << 4, 21,
		0, 7,
		packets.ErrNotAuthorized.Code,
		17,
		31, 0, 14, 'n', 'o', 't', ' ', 'a', 'u',
		't', 'h', 'o', 'r', 'i', 'z', 'e', 'd',
	}
)

type ProtocolTest []struct {
	protocolVersion byte
	in              packets.TPacketCase
	out             packets.TPacketCase
	data            map[string]any
}

type AllowHook struct {
	HookBase
}

func (h *AllowHook) SetOpts(l *slog.Logger, opts *HookOptions) {
	h.Log = l
	h.Opts = opts
}

func (h *AllowHook) ID() string {
	return "allow-all-auth"
}

func (h *AllowHook) Provides(b byte) bool {
	return bytes.Contains([]byte{OnConnectAuthenticate, OnACLCheck}, []byte{b})
}

func (h *AllowHook) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool { return true }
func (h *AllowHook) OnACLCheck(cl *Client, topic string, write bool) bool     { return true }

type DenyHook struct {
	HookBase
}

func (h *DenyHook) SetOpts(l *slog.Logger, opts *HookOptions) {
	h.Log = l
	h.Opts = opts
}

func (h *DenyHook) ID() string {
	return "deny-all-auth"
}

func (h *DenyHook) Provides(b byte) bool {
	return bytes.Contains([]byte{OnConnectAuthenticate, OnACLCheck}, []byte{b})
}

func (h *DenyHook) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool { return false }
func (h *DenyHook) OnACLCheck(cl *Client, topic string, write bool) bool     { return false }

type DelayHook struct {
	HookBase
	DisconnectDelay time.Duration
}

func (h *DelayHook) SetOpts(l *slog.Logger, opts *HookOptions) {
	h.Log = l
	h.Opts = opts
}

func (h *DelayHook) ID() string {
	return "delay-hook"
}

func (h *DelayHook) Provides(b byte) bool {
	return bytes.Contains([]byte{OnDisconnect}, []byte{b})
}

func (h *DelayHook) OnDisconnect(cl *Client, err error, expire bool) {
	time.Sleep(h.DisconnectDelay)
}

func newServer() *Server {
	cc := NewDefaultServerCapabilities()
	cc.MaximumMessageExpiryInterval = 0
	cc.ReceiveMaximum = 0
	s := New(&Options{
		Logger:       logger,
		Capabilities: cc,
	})
	_ = s.AddHook(new(AllowHook), nil)
	return s
}

func newServerWithInlineClient() *Server {
	cc := NewDefaultServerCapabilities()
	cc.MaximumMessageExpiryInterval = 0
	cc.ReceiveMaximum = 0
	s := New(&Options{
		Logger:       logger,
		Capabilities: cc,
		InlineClient: true,
	})
	_ = s.AddHook(new(AllowHook), nil)
	return s
}

// readPacket reads one whole packet off the client's end of a pipe, as a
// client would: its first byte, its remaining length and its body.
//
// **A test reads what the server must send, rather than closing the pipe a
// moment later and taking whatever had arrived.** A delivery is written by
// the client's own write loop, after the call that made it has returned, so
// a close on a clock cut a correct write short on a busy machine - and on an
// idle one let a second, later write go unread.
func readPacket(t testing.TB, conn net.Conn) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	pk := make([]byte, 1, 64)
	if _, err := io.ReadFull(conn, pk); err != nil {
		t.Fatalf("no packet arrived: %v", err)
	}
	n, shift := 0, 0
	for {
		var b [1]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatalf("a packet's length was cut short: %v", err)
		}
		pk = append(pk, b[0])
		n |= int(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		if shift += 7; shift > 21 {
			t.Fatalf("a malformed remaining length: % x", pk)
		}
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("a packet's body was cut short after % x: %v", pk, err)
	}
	return append(pk, body...)
}

// endMarker is the payload of the packet requireNothingBefore puts behind
// whatever a test drove.
const endMarker = "end-marker"

// upToMarker returns everything a client is sent before an end marker: it
// queues a QoS 0 PUBLISH carrying endMarker for cl, the way every delivery is
// queued, and reads conn until that arrives. A client's queue is written in
// order by its one write loop, so whatever the call under test queued arrives
// ahead of the marker - an end the test reads, where a quiet millisecond was a
// guess about a goroutine.
func upToMarker(t testing.TB, s *Server, cl *Client, conn net.Conn) [][]byte {
	t.Helper()
	marker := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName: "end/marker", Payload: []byte(endMarker)}
	_, err := s.publishToClient(cl, packets.Subscription{Filter: "end/marker"}, marker)
	require.NoError(t, err, "the end marker was not queued")
	var before [][]byte
	for {
		got := readPacket(t, conn)
		if got[0]>>4 == packets.Publish && bytes.HasSuffix(got, []byte(endMarker)) {
			return before
		}
		before = append(before, got)
	}
}

// requireNothingBefore says a client was sent nothing more than the test has
// already read (upToMarker).
func requireNothingBefore(t testing.TB, s *Server, cl *Client, conn net.Conn, why string) {
	t.Helper()
	if extra := upToMarker(t, s, cl, conn); len(extra) > 0 {
		t.Fatalf("%s: % x arrived before the end marker", why, extra)
	}
}

func TestOptionsSetDefaults(t *testing.T) {
	opts := &Options{}
	opts.ensureDefaults()

	require.Equal(t, NewDefaultServerCapabilities(), opts.Capabilities)

	opts = new(Options)
	opts.ensureDefaults()
}

func TestNew(t *testing.T) {
	s := New(nil)
	require.NotNil(t, s)
	require.NotNil(t, s.Clients)
	require.NotNil(t, s.Listeners)
	require.NotNil(t, s.Topics)
	require.NotNil(t, s.Info)
	require.NotNil(t, s.Log)
	require.NotNil(t, s.Options)
	require.NotNil(t, s.loop)
	require.NotNil(t, s.loop.inflightExpiry)
	require.NotNil(t, s.loop.clientExpiry)
	require.NotNil(t, s.hooks)
	require.NotNil(t, s.hooks.Log)
	require.NotNil(t, s.done)
	require.Nil(t, s.inlineClient)
	require.Equal(t, 0, s.Clients.Len())
}

func TestNewWithInlineClient(t *testing.T) {
	s := New(&Options{
		InlineClient: true,
	})
	require.NotNil(t, s.inlineClient)
	require.Equal(t, 1, s.Clients.Len())
}

func TestNewNilOpts(t *testing.T) {
	s := New(nil)
	require.NotNil(t, s)
	require.NotNil(t, s.Options)
}

func TestServerNewClient(t *testing.T) {
	s := New(nil)
	s.Log = logger
	r, _ := net.Pipe()

	cl := s.NewClient(r, "testing", "test", false)
	require.NotNil(t, cl)
	require.Equal(t, "test", cl.ID)
	require.Equal(t, "testing", cl.Net.Listener)
	require.False(t, cl.Net.Inline)
	require.NotNil(t, cl.State.Inflight)
	require.NotNil(t, cl.State.Subscriptions)
	require.NotNil(t, cl.State.TopicAliases)
	require.Equal(t, defaultKeepalive, cl.State.Keepalive)
	require.Equal(t, defaultClientProtocolVersion, cl.Properties.ProtocolVersion)
	require.NotNil(t, cl.Net.Conn)
	require.NotNil(t, cl.Net.bconn)
	require.NotNil(t, cl.ops)
	require.Equal(t, s.Log, cl.ops.log)
}

func TestServerNewClientInline(t *testing.T) {
	s := New(nil)
	cl := s.NewClient(nil, "testing", "test", true)
	require.True(t, cl.Net.Inline)
}

func TestServerAddHook(t *testing.T) {
	s := New(nil)

	s.Log = logger
	require.NotNil(t, s)

	require.Equal(t, int64(0), s.hooks.Len())
	err := s.AddHook(new(HookBase), nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), s.hooks.Len())
}

func TestServerAddListener(t *testing.T) {
	s := newServer()
	defer s.Close()

	require.NotNil(t, s)

	err := s.AddListener(listeners.NewMockListener("t1", ":1882"))
	require.NoError(t, err)

	// add existing listener
	err = s.AddListener(listeners.NewMockListener("t1", ":1882"))
	require.Error(t, err)
	require.Equal(t, ErrListenerIDExists, err)
}

func TestServerAddListenerInitFailure(t *testing.T) {
	s := newServer()
	defer s.Close()

	require.NotNil(t, s)

	m := listeners.NewMockListener("t1", ":1882")
	m.ErrListen = true
	err := s.AddListener(m)
	require.Error(t, err)
}

func TestServerServe(t *testing.T) {
	s := newServer()
	defer s.Close()

	require.NotNil(t, s)

	err := s.AddListener(listeners.NewMockListener("t1", ":1882"))
	require.NoError(t, err)

	err = s.Serve()
	require.NoError(t, err)

	time.Sleep(time.Millisecond)

	require.Equal(t, 1, s.Listeners.Len())
	listener, ok := s.Listeners.Get("t1")

	require.Equal(t, true, ok)
	require.Equal(t, true, listener.(*listeners.MockListener).IsServing())
}

func TestServerEventLoop(t *testing.T) {
	s := newServer()
	defer s.Close()

	s.loop.inflightExpiry = time.NewTicker(time.Millisecond)
	s.loop.clientExpiry = time.NewTicker(time.Millisecond)
	s.loop.retainedExpiry = time.NewTicker(time.Millisecond)
	go s.eventLoop()

	time.Sleep(time.Millisecond * 3)
}

func TestServerReadConnectionPacket(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r, _ := newTestClient()
	s.Clients.Add(cl)

	o := make(chan packets.Packet)
	go func() {
		pk, err := s.readConnectionPacket(cl)
		require.NoError(t, err)
		o <- pk
	}()

	go func() {
		_, _ = r.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
		_ = r.Close()
	}()

	require.Equal(t, *packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet, <-o)
}

func TestServerReadConnectionPacketBadFixedHeader(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r, _ := newTestClient()
	s.Clients.Add(cl)

	o := make(chan error)
	go func() {
		_, err := s.readConnectionPacket(cl)
		o <- err
	}()

	go func() {
		_, _ = r.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMalFixedHeader).RawBytes)
		_ = r.Close()
	}()

	err := <-o
	require.Error(t, err)
	require.Equal(t, packets.ErrMalformedVariableByteInteger, err)
}

func TestServerReadConnectionPacketBadPacketType(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r, _ := newTestClient()
	s.Clients.Add(cl)

	go func() {
		_, _ = r.Write(packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession).RawBytes)
		_ = r.Close()
	}()

	_, err := s.readConnectionPacket(cl)
	require.Error(t, err)
	require.Equal(t, packets.ErrProtocolViolationRequireFirstConnect, err)
}

func TestServerReadConnectionPacketBadPacket(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r, _ := newTestClient()
	s.Clients.Add(cl)

	go func() {
		_, _ = r.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMalProtocolName).RawBytes)
		_ = r.Close()
	}()

	_, err := s.readConnectionPacket(cl)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrMalformedProtocolName)
}

func TestEstablishConnection(t *testing.T) {
	s := newServer()
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.NoError(t, err)

	// Todo:
	// 		s.Clients is already empty here. Is it necessary to check v.StopCause()?

	// for _, v := range s.Clients.GetAll() {
	// 	require.ErrorIs(t, v.StopCause(), packets.CodeDisconnect) // true error is disconnect
	// }

	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession).RawBytes, <-recv)

	_ = w.Close()
	_ = r.Close()

	// client must be deleted on session close if Clean = true
	_, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).Packet.Connect.ClientIdentifier)
	require.False(t, ok)
}

// **Every byte read off a connection is counted** (saguin_bytes_received_total,
// "everything on the wire", RFC 0005), whichever path read it. Counted where
// packets were parsed, four paths counted nothing: the CONNECT's own fixed
// header - 19 bytes written read as 17 - the header and discarded body of a
// packet over the size bound, a busy refusal's opening bytes, and a refused
// CONNECT's prefix. Each arm writes bytes the server reads to the last one,
// and the count must be exactly what was written.
func TestEveryByteReadOffAConnectionIsCounted(t *testing.T) {
	connect := packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes
	disconnect := packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes
	// A CONNECT's opening bytes, declaring a body of 1,048,000 it never sends.
	oversized := []byte{0x10, 0xC0, 0xFB, 0x3F, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00,
		0x00, 0x03, 'b', 'i', 'g'}
	// A PUBLISH of 200 bytes, over a 64-byte Maximum Packet Size.
	publish := append([]byte{0x30, 0xC5, 0x01, 0x00, 0x01, 'x'}, make([]byte, 194)...)

	for _, tc := range []struct {
		why     string
		setup   func(s *Server)
		written [][]byte
	}{
		{"a connection and its DISCONNECT", func(*Server) {}, [][]byte{connect, disconnect}},
		{"a packet over the size bound, discarded unread", func(s *Server) {
			s.Options.Capabilities.MaximumPacketSize = 64
		}, [][]byte{connect, publish}},
		{"a CONNECT over max_connect_size, refused from its opening bytes", func(s *Server) {
			s.Options.ClientMaxConnectSize = 1024
		}, [][]byte{oversized}},
		{"a connection refused busy", func(s *Server) {
			s.Options.Capabilities.MaximumClients = 0
		}, [][]byte{connect}},
	} {
		t.Run(tc.why, func(t *testing.T) {
			s := newServer()
			defer s.Close()
			tc.setup(s)
			r, w := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- s.EstablishConnection("tcp", r) }()
			go func() { _, _ = io.ReadAll(w) }()
			total := 0
			for _, b := range tc.written {
				n, err := w.Write(b)
				require.NoError(t, err)
				total += n
			}
			_ = w.Close()
			<-done
			require.Equal(t, int64(total), s.Info.BytesReceived.Load(),
				"%d bytes written and every one read, and BytesReceived says otherwise", total)
		})
	}
}

func TestEstablishConnectionAckFailure(t *testing.T) {
	s := newServer()
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_ = w.Close()
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)

	_ = r.Close()
}

func TestEstablishConnectionReadError(t *testing.T) {
	s := newServer()
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes) // second connect error
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)

	// Retrieve the client corresponding to the Client Identifier.
	retrievedCl, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).Packet.Connect.ClientIdentifier)
	require.True(t, ok)
	require.ErrorIs(t, retrievedCl.StopCause(), packets.ErrProtocolViolationSecondConnect) // true error is disconnect

	ret := <-recv
	require.Equal(t, append(
		packets.TPacketData[packets.Connack].Get(packets.TConnackMinCleanMqtt5).RawBytes,
		packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectSecondConnect).RawBytes...),
		ret,
	)

	_ = w.Close()
	_ = r.Close()
}

// endingHook counts what a connection's end tells the hooks.
type endingHook struct {
	HookBase
	disconnects, wills atomic.Int32
}

func (h *endingHook) ID() string { return "ending" }
func (h *endingHook) Provides(b byte) bool {
	return b == OnDisconnect || b == OnWill
}
func (h *endingHook) OnDisconnect(cl *Client, err error, expire bool) { h.disconnects.Add(1) }
func (h *endingHook) OnWill(cl *Client, will Will) (Will, error) {
	h.wills.Add(1)
	return will, nil
}

// writesOnce passes its first write, the CONNACK, and refuses every later
// one, as a socket that dies straight after it would.
type writesOnce struct {
	net.Conn
	writes atomic.Int32
}

func (c *writesOnce) Write(b []byte) (int, error) {
	if c.writes.Add(1) > 1 {
		return 0, errors.New("refused for the test")
	}
	return c.Conn.Write(b)
}

// A resumed connection whose resend of its in-flight messages fails ends
// as one whose read fails: its Will is offered to OnWill and OnDisconnect
// is told. It used to return from attachClient at the resend, registered,
// with neither: no Will, and a session store still saying the client was
// connected.
func TestAResendThatFailsEndsTheConnectionAsAReadDoes(t *testing.T) {
	s := newServer()
	defer s.Close()
	h := new(endingHook)
	require.NoError(t, s.AddHook(h, nil))

	const id = "resumer"
	old, _, _ := newTestClient()
	old.Properties.ProtocolVersion = 4
	old.ID = id
	old.State.Subscriptions.Add("a/b/c", packets.Subscription{Filter: "a/b/c", Qos: 1})
	old.State.Inflight.Set(*packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
	s.Clients.Add(old)

	// MQTT 3.1.1, Clean Session 0 so the session is resumed and its
	// in-flight message resent, and a Will on w/t.
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x04, 0x00, 0x3c}
	for _, f := range []string{id, "w/t", "bye"} {
		body = append(body, byte(len(f)>>8), byte(len(f)))
		body = append(body, f...)
	}
	r, w := net.Pipe()
	defer w.Close()
	conn := &writesOnce{Conn: r}
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", conn) }()
	go func() { _, _ = w.Write(append([]byte{0x10, byte(len(body))}, body...)) }()
	go func() { _, _ = io.Copy(io.Discard, w) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never ended")
	}
	require.ErrorContains(t, err, "resend inflight", "the connection did not end at the resend, so this proves nothing")
	require.GreaterOrEqual(t, conn.writes.Load(), int32(2), "the resend was never attempted")
	require.Equal(t, int32(1), h.wills.Load(), "the Will of a connection that ended at its resend was not offered to OnWill")
	require.Equal(t, int32(1), h.disconnects.Load(), "OnDisconnect was not told of a connection that ended at its resend")
}

// registeredHook delivers to the resuming client from OnSessionRegistered,
// which runs once the connection is registered and before its resend - the
// window in which any other sender can reach it - and takes one of the
// in-flight messages the connection inherited off its table, as an
// acknowledgement landing in that window would. It delivers the way the
// fan-out does to each subscriber (publishToClient), not by publishing: the
// engine holds the id's session lock around this hook, and a publish is
// what may wait on it.
type registeredHook struct {
	HookBase
	s     *Server
	acked uint16
	err   error
}

func (h *registeredHook) ID() string { return "registered" }
func (h *registeredHook) Provides(b byte) bool {
	return b == OnSessionRegistered
}
func (h *registeredHook) OnSessionRegistered(cl *Client, pk packets.Packet) {
	cl.State.Inflight.Delete(h.acked)
	_, h.err = h.s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1},
		packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			TopicName: "a/b/c", Payload: []byte("new"), Origin: "someone"})
}

// RFC 0003 "Sessions": a resumed session is sent again what it held
// unacknowledged [MQTT-4.4.0-1], and nothing else.
//
// **A delivery made to the connection after it is registered is a first
// delivery, and its sender writes it.** The resend read the in-flight table
// after the registration, so it also found anything another sender had
// handed the connection in between, and wrote it a second time with DUP
// under the same packet identifier. Measured through a shared group whose
// four members came back at once: 2 of 200 runs under load served a job
// twice to one member, 13 to 257 us apart.
//
// The two controls: the message the previous connection left unacknowledged
// is still sent again, with DUP and its own identifier, and one acknowledged
// after the table was read and before the resend is not.
func TestAResumedSessionResendsOnlyWhatItInherited(t *testing.T) {
	s := newServer()
	defer s.Close()
	h := &registeredHook{s: s, acked: 8}
	require.NoError(t, s.AddHook(h, nil))

	const id = "resumer"
	old, _, _ := newTestClient()
	old.Properties.ProtocolVersion = 4
	old.ID = id
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
	old.State.Subscriptions.Add(sub.Filter, sub)
	s.Topics.Subscribe(id, sub)
	for _, m := range []struct {
		id      uint16
		payload string
	}{{7, "unacknowledged"}, {8, "acknowledged"}} {
		pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			TopicName: "a/b/c", Payload: []byte(m.payload), PacketID: m.id, Origin: "someone"}
		old.State.Inflight.Set(pk) // written on the old connection: Expiry 0
	}
	s.Clients.Add(old)

	// MQTT 3.1.1, Clean Session 0, so the session is resumed.
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x00, 0x00, 0x3c,
		0x00, byte(len(id))}
	body = append(body, id...)
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	go func() { _, _ = w.Write(append([]byte{0x10, byte(len(body))}, body...)) }()

	type got struct {
		payload string
		id      uint16
		dup     bool
	}
	var publishes []got
	rd := bufio.NewReader(w)
	for {
		_ = w.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		hb, err := rd.ReadByte()
		if err != nil {
			break // nothing more within the deadline
		}
		var fh packets.FixedHeader
		require.NoError(t, fh.Decode(hb))
		n, _, err := packets.DecodeLength(rd)
		require.NoError(t, err)
		buf := make([]byte, n)
		_, err = io.ReadFull(rd, buf)
		require.NoError(t, err)
		if fh.Type != packets.Publish {
			continue
		}
		pk := packets.Packet{FixedHeader: fh, ProtocolVersion: 4}
		require.NoError(t, pk.PublishDecode(buf))
		publishes = append(publishes, got{string(pk.Payload), pk.PacketID, fh.Dup})
	}
	_ = w.Close()
	<-done
	require.NoError(t, h.err, "the delivery from OnSessionRegistered was refused, so nothing reached the window")

	var fresh, again []got
	for _, p := range publishes {
		switch p.payload {
		case "new":
			fresh = append(fresh, p)
		case "unacknowledged":
			again = append(again, p)
		case "acknowledged":
			t.Errorf("a message acknowledged before the resend was sent again: %+v", p)
		}
	}
	require.Len(t, fresh, 1, "the delivery made after the registration was written %d times: %+v "+
		"- once by its sender, and again by the resend", len(fresh), publishes)
	require.False(t, fresh[0].dup, "the delivery made after the registration went out as a resend: %+v", fresh[0])
	require.Equal(t, []got{{"unacknowledged", 7, true}}, again,
		"what the previous connection left unacknowledged is sent again once, with DUP and its own identifier")
}

// gapHook makes one delivery to the resuming client from
// OnSessionRegistered - after the connection is registered and before its
// resend is written - by one of the routes a sender takes.
type gapHook struct {
	HookBase
	s     *Server
	route string // "queued", "direct" or "queue full"
	err   error
	// what the route did with it, read by the test to know the route ran
	queued, withheld bool
}

func (h *gapHook) ID() string { return "gap" }
func (h *gapHook) Provides(b byte) bool {
	return b == OnSessionRegistered
}
func (h *gapHook) OnSessionRegistered(cl *Client, _ packets.Packet) {
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		TopicName: "a/b/c", Payload: []byte("new"), Origin: "someone"}
	if h.route == "queued" {
		// The engine's fan-out: registered and queued for the write loop,
		// which is then given its chance to write it before the resend.
		_, h.err = h.s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, pk)
		h.queued = atomic.LoadInt32(&cl.State.outboundQty) > 0
		for end := time.Now().Add(100 * time.Millisecond); atomic.LoadInt32(&cl.State.outboundQty) > 0 &&
			time.Now().Before(end); {
			time.Sleep(time.Millisecond)
		}
		return
	}
	// A broker sender (a shared group, a channel pump): registered, a slot
	// of window taken, and written by WritePacket itself.
	id, err := cl.NextPacketID()
	if err != nil {
		h.err = err
		return
	}
	pk.PacketID = uint16(id)
	cl.State.Inflight.Set(pk)
	cl.State.Inflight.DecreaseSendQuota()
	if h.route == "queue full" {
		cl.State.outbound.max = 0
	}
	h.err = cl.WritePacket(pk)
	if m, ok := cl.State.Inflight.Get(pk.PacketID); ok {
		h.withheld = m.Expiry < 0
	}
}

// [MQTT-4.6.0-6]: a server sends a consumer the messages it received from a
// publisher on a topic in the order it received them - and a resumed
// session's resend is the older of two such messages.
//
// **What reaches the connection while its resend is still to be written
// goes after it**, by every route a sender takes: queued for the write loop,
// written by WritePacket, and withheld where the queue has no room. The
// resend is written once the connection is registered, and a delivery made
// in between went to the socket first by either route, every time: the
// older message, being sent again, arrived after the newer one.
func TestAResumedSessionsResendGoesAheadOfWhatArrivesMeanwhile(t *testing.T) {
	for _, route := range []string{"queued", "direct", "queue full"} {
		t.Run(route, func(t *testing.T) {
			s := newServer()
			defer s.Close()
			h := &gapHook{s: s, route: route}
			require.NoError(t, s.AddHook(h, nil))

			const id = "resumer"
			old, _, _ := newTestClient()
			old.Properties.ProtocolVersion = 4
			old.ID = id
			sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
			old.State.Subscriptions.Add(sub.Filter, sub)
			s.Topics.Subscribe(id, sub)
			// Written on the old connection and never acknowledged, from the
			// same publisher and on the same topic as the one made in the gap.
			old.State.Inflight.Set(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
				TopicName: "a/b/c", Payload: []byte("older"), PacketID: 7, Origin: "someone"})
			s.Clients.Add(old)

			body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x00, 0x00, 0x3c, 0x00, byte(len(id))}
			body = append(body, id...)
			r, w := net.Pipe()
			done := make(chan error, 1)
			go func() { done <- s.EstablishConnection("tcp", r) }()
			go func() { _, _ = w.Write(append([]byte{0x10, byte(len(body))}, body...)) }()

			type got struct {
				payload string
				id      uint16
				dup     bool
			}
			var publishes []got
			rd := bufio.NewReader(w)
			for {
				_ = w.SetReadDeadline(time.Now().Add(time.Second))
				hb, err := rd.ReadByte()
				if err != nil {
					break // nothing more within the deadline
				}
				var fh packets.FixedHeader
				require.NoError(t, fh.Decode(hb))
				n, _, err := packets.DecodeLength(rd)
				require.NoError(t, err)
				buf := make([]byte, n)
				_, err = io.ReadFull(rd, buf)
				require.NoError(t, err)
				if fh.Type != packets.Publish {
					continue
				}
				pk := packets.Packet{FixedHeader: fh, ProtocolVersion: 4}
				require.NoError(t, pk.PublishDecode(buf))
				publishes = append(publishes, got{string(pk.Payload), pk.PacketID, fh.Dup})
			}
			_ = w.Close()
			<-done
			require.NoError(t, h.err, "the delivery in the gap was refused, so nothing was there to go first")
			switch route {
			case "queued":
				require.True(t, h.queued, "the delivery in the gap was never queued, so the write loop was not tested")
			case "queue full":
				require.True(t, h.withheld, "the delivery in the gap was not withheld, so a full queue was not tested")
			}

			require.Len(t, publishes, 2, "want the older message sent again and the newer once: %+v", publishes)
			require.Equal(t, got{"older", 7, true}, publishes[0],
				"the older message, sent again, must reach the client before the newer: %+v", publishes)
			require.Equal(t, "new", publishes[1].payload, "%+v", publishes)
			require.False(t, publishes[1].dup, "the newer message went out as a resend: %+v", publishes)
		})
	}
}

// What holdForResend holds back is every PUBLISH a sender writes, because
// the one write that passes it - writePacket - is called by nothing but the
// gate itself, the write loop, which does not start while a resend is still
// to be written, and the resend. Every sender outside this package writes
// through WritePacket, since writePacket is not exported; the broker's are
// counted, and the five that write a delivery by name must be among them.
func TestOnlyTheGateTheWriteLoopAndTheResendWritePastTheResend(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	allowed := map[string]bool{"WritePacket": true, "writeQueued": true, "drainWithheld": true,
		"resendInflight": true, "writeFirstSend": true}
	senders := map[string]bool{"handToMember": false, "lendTo": false, "batch": false,
		"writeLatest": false, "pumpBatch": false}
	var scanned, ungated, brokerWrites int
	var other []string
	for _, dir := range []string{"internal/mqtt", "internal/broker"} {
		entries, err := os.ReadDir(filepath.Join(root, dir))
		require.NoError(t, err)
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, dir, e.Name())
			scanned++
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			require.NoError(t, err, path)
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch {
					case sel.Sel.Name == "writePacket" && dir == "internal/mqtt":
						ungated++
						if !allowed[fn.Name.Name] {
							other = append(other, fmt.Sprintf("%s:%d in %s", e.Name(),
								fset.Position(call.Pos()).Line, fn.Name.Name))
						}
					case (sel.Sel.Name == "writeTo" || sel.Sel.Name == "WritePacket") && dir == "internal/broker":
						brokerWrites++
						if _, named := senders[fn.Name.Name]; named {
							senders[fn.Name.Name] = true
						}
					}
					return true
				})
			}
		}
	}
	t.Logf("%d files; %d calls of writePacket; %d writes in internal/broker", scanned, ungated, brokerWrites)
	require.Positive(t, ungated, "no call of writePacket was found, so this examined nothing")
	require.Empty(t, other, "writePacket is called past the gate")
	for name, found := range senders {
		require.True(t, found, "%s no longer writes through writeTo or WritePacket: say where it writes", name)
	}
}

func TestEstablishConnectionInheritExisting(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r0, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.SessionExpiryInterval = 60 // a session a takeover inherits (saguin)
	cl.Properties.Username = []byte("mochi")
	cl.ID = packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier
	cl.State.Subscriptions.Add("a/b/c", packets.Subscription{Filter: "a/b/c", Qos: 1})
	cl.State.Inflight.Set(*packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
	s.Clients.Add(cl)

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		err := s.EstablishConnection("tcp", r)
		o <- err
	}()

	connackPlusPacket := append(
		packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedSessionExists).RawBytes,
		packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Dup).RawBytes...,
	)

	// **The DISCONNECT goes once the queued in-flight message has been
	// read**, not a millisecond after the CONNECT: what the resumed session
	// is sent is the thing under test, so the client reads it before it
	// leaves.
	received := make(chan struct{})
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
		<-received
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// receive the disconnect session takeover
	takeover := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(r0)
		require.NoError(t, err)
		takeover <- buf
	}()

	// receive the connack and the resent message, then whatever follows
	recv := make(chan []byte)
	go func() {
		_ = w.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, len(connackPlusPacket))
		n, _ := io.ReadFull(w, buf)
		close(received) // what came within the deadline is what is asserted
		_ = w.SetReadDeadline(time.Time{})
		rest, _ := io.ReadAll(w)
		recv <- append(buf[:n], rest...)
	}()

	err := <-o
	require.NoError(t, err)

	// Retrieve the client corresponding to the Client Identifier.
	retrievedCl, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier)
	require.True(t, ok)
	require.ErrorIs(t, retrievedCl.StopCause(), packets.CodeDisconnect) // true error is disconnect

	require.Equal(t, connackPlusPacket, <-recv)
	require.Equal(t, packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectTakeover).RawBytes, <-takeover)

	_ = w.Close()
	_ = r.Close()

	clw, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier)
	require.True(t, ok)
	require.NotEmpty(t, clw.State.Subscriptions)
	require.True(t, cl.IsTakenOver())

	// Prevent sequential takeover memory-bloom.
	require.Empty(t, cl.State.Subscriptions.GetAll())
}

// See https://github.com/mochi-mqtt/server/issues/173
func TestEstablishConnectionInheritExistingTrueTakeover(t *testing.T) {
	s := newServer()
	d := new(DelayHook)
	d.DisconnectDelay = time.Millisecond * 200
	_ = s.AddHook(d, nil)
	defer s.Close()

	// Clean start, and a session expiry interval: a session that ends with
	// its connection ends at the takeover, and nothing is inherited (saguin).
	cl1RawBytes := []byte{
		packets.Connect << 4, 21, // Fixed header
		0, 4, // Protocol Name - MSB+LSB
		'M', 'Q', 'T', 'T', // Protocol Name
		5,      // Protocol Version
		1 << 1, // Packet Flags
		0, 30,  // Keepalive
		5,               // Properties length
		17, 0, 0, 0, 60, // Session Expiry Interval (17)
		0, 3, // Client ID - MSB+LSB
		'z', 'e', 'n', // Client ID "zen"
	}

	// Make first connection
	r1, w1 := net.Pipe()
	o1 := make(chan error)
	go func() {
		err := s.EstablishConnection("tcp", r1)
		o1 <- err
	}()
	go func() {
		_, _ = w1.Write(cl1RawBytes)
	}()

	// receive the first connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w1)
		require.NoError(t, err)
		recv <- buf
	}()

	// Get the first client pointer, once the server has registered it: its
	// CONNACK is written before it is (attachClient), so the CONNACK says
	// nothing about the table.
	var cl1 *Client
	require.Eventually(t, func() bool {
		var ok bool
		cl1, ok = s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).Packet.Connect.ClientIdentifier)
		return ok
	}, 5*time.Second, time.Millisecond, "the first connection was never registered")
	cl1.State.Subscriptions.Add("a/b/c", packets.Subscription{Filter: "a/b/c", Qos: 1})
	cl1.State.Subscriptions.Add("d/e/f", packets.Subscription{Filter: "d/e/f", Qos: 0})

	// The first client as the second connection will find it, read before
	// that connection exists rather than raced against its takeover.
	clp1 := cl1
	require.Empty(t, clp1.Properties.Username)
	require.NotEmpty(t, clp1.State.Subscriptions.GetAll())

	// Make the second connection
	r2, w2 := net.Pipe()
	o2 := make(chan error)
	go func() {
		err := s.EstablishConnection("tcp", r2)
		o2 <- err
	}()
	go func() {
		x := packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).RawBytes[:]
		x[19] = '.' // differentiate username bytes in debugging
		_, _ = w2.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectUserPass).RawBytes)
	}()

	// receive the second connack
	recv2 := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w2)
		require.NoError(t, err)
		recv2 <- buf
	}()

	err1 := <-o1
	require.Error(t, err1)
	require.ErrorIs(t, err1, io.ErrClosedPipe)

	// Capture second Client pointer, once it holds the id.
	var clp2 *Client
	require.Eventually(t, func() bool {
		var ok bool
		clp2, ok = s.Clients.Get("zen")
		return ok && clp2 != clp1
	}, 5*time.Second, time.Millisecond, "the second connection never took the id over")
	require.Equal(t, []byte(".ochi"), clp2.Properties.Username)
	require.NotEmpty(t, clp2.State.Subscriptions.GetAll())
	require.Empty(t, clp1.State.Subscriptions.GetAll())

	_, _ = w2.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	require.NoError(t, <-o2)

	require.True(t, clp1.IsTakenOver())
	require.False(t, clp2.IsTakenOver())
}

func TestEstablishConnectionResentPendingInflightsError(t *testing.T) {
	s := newServer()
	defer s.Close()

	n := time.Now().Unix()
	cl, r0, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.SessionExpiryInterval = 60 // a session a takeover inherits (saguin)
	cl.ID = packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier
	cl.State.Inflight = NewInflights()
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Created: n - 2}) // no packet type
	s.Clients.Add(cl)

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
	}()

	go func() {
		_, err := io.ReadAll(r0)
		require.NoError(t, err)
	}()

	go func() {
		_, err := io.ReadAll(w)
		require.NoError(t, err)
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrNoValidPacketAvailable)
}

func TestEstablishConnectionInheritExistingClean(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r0, _ := newTestClient()
	cl.ID = packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier
	cl.Properties.ProtocolVersion = 4 // the clean-session rule under test is the v3 one
	cl.Properties.Clean = true
	cl.State.Subscriptions.Add("a/b/c", packets.Subscription{Filter: "a/b/c", Qos: 1})
	s.Clients.Add(cl)

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// a v3 client is closed without a Disconnect packet, so nothing arrives here
	takeover := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(r0)
		require.NoError(t, err)
		takeover <- buf
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.NoError(t, err)

	// Retrieve the client corresponding to the Client Identifier.
	retrievedCl, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier)
	require.True(t, ok)
	require.ErrorIs(t, retrievedCl.StopCause(), packets.CodeDisconnect) // true error is disconnect

	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession).RawBytes, <-recv)
	require.Empty(t, <-takeover)

	require.True(t, cl.IsTakenOver())

	_ = w.Close()
	_ = r.Close()

	clw, ok := s.Clients.Get(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).Packet.Connect.ClientIdentifier)
	require.True(t, ok)
	require.Equal(t, 0, clw.State.Subscriptions.Len())

}

func TestEstablishConnectionBadAuthentication(t *testing.T) {
	s := New(&Options{
		Logger: logger,
	})
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrBadUsernameOrPassword)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackBadUsernamePasswordNoSession).RawBytes, <-recv)

	_ = w.Close()
	_ = r.Close()
}

func TestEstablishConnectionBadAuthenticationAckFailure(t *testing.T) {
	s := New(&Options{
		Logger: logger,
	})
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_ = w.Close()
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)

	_ = r.Close()
}

func TestServerEstablishConnectionInvalidConnect(t *testing.T) {
	s := newServer()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMalReservedBit).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, packets.ErrMalformedReservedBit, err)
	// Section 3.1.2.3: "If the reserved flag is not 0 it is a Malformed
	// Packet", so the CONNACK carries 0x81 rather than 0x82.
	require.Equal(t, []byte{packets.Connack << 4, 2, 0, 0x81}, <-recv)

	_ = r.Close()
}

func TestServerEstablishConnectionZeroValuedProperties(t *testing.T) {
	for _, tt := range []struct {
		name  string
		tCase byte
		code  packets.Code
	}{
		{"receive maximum", packets.TConnectInvalidZeroReceiveMaximum, packets.ErrProtocolViolationZeroReceiveMaximum},
		{"maximum packet size", packets.TConnectInvalidZeroMaximumPacketSize, packets.ErrProtocolViolationZeroMaximumPacketSize},
	} {
		tt := tt // go.mod says go 1.21, so the range variable is shared
		t.Run(tt.name, func(t *testing.T) {
			s := newServer()

			r, w := net.Pipe()
			o := make(chan error)
			go func() {
				o <- s.EstablishConnection("tcp", r)
			}()

			go func() {
				_, _ = w.Write(packets.TPacketData[packets.Connect].Get(tt.tCase).RawBytes)
				// A disconnect behind it so that a server which wrongly
				// accepts the connect ends the test by returning nil,
				// rather than by holding the connection open until the
				// suite times out.
				_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
			}()

			// receive the connack
			recv := make(chan []byte)
			go func() {
				buf, err := io.ReadAll(w)
				require.NoError(t, err)
				recv <- buf
			}()

			err := <-o
			require.Error(t, err)
			// Refused as the properties are read, so the error is the code
			// wrapped by the read that met it.
			require.ErrorIs(t, err, tt.code)

			// The refusal has to reach the client, not merely be returned
			// here: a code the server never sends is one no client can act
			// on. Byte 0 is the fixed header, 2 the session present flag,
			// 3 the reason code.
			buf := <-recv
			require.Equal(t, byte(packets.Connack<<4), buf[0])
			require.Equal(t, tt.code.Code, buf[3])

			_ = r.Close()
		})
	}
}

func TestServerAnswersAnOversizedPacketInsteadOfResetting(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumPacketSize = 100 // larger than the connect below, smaller than the publish
	s := New(&Options{Logger: logger, Capabilities: cc})
	_ = s.AddHook(new(AllowHook), nil)
	defer s.Close()

	// A QoS 0 publish past the bound. Built here rather than taken from a
	// fixture because its whole point is its size.
	body := []byte{0, 5, 'a', '/', 'b', '/', 'c', 0}
	body = append(body, bytes.Repeat([]byte{'x'}, 110)...)
	// One byte of remaining length, which is right up to 127 and silently
	// wrong above it: 128 encodes as 0x80, whose continuation bit swallows
	// the next byte, and the server then answers "malformed packet" to a
	// test that believes it asked about size.
	require.Less(t, len(body), 128, "the remaining length no longer fits in one byte")
	oversized := append([]byte{packets.Publish << 4, byte(len(body))}, body...)

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
		_, _ = w.Write(oversized)
	}()

	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, packets.ErrPacketTooLarge, err)

	// The connack first, then the packet this test is about. Walking the
	// remaining length rather than searching for the type byte, which would
	// find one in a payload.
	buf := <-recv
	n, mult := 0, 1
	for i := 1; ; i++ {
		require.Less(t, i, len(buf), "no complete connack in %02x", buf)
		n += int(buf[i]&127) * mult
		mult *= 128
		if buf[i]&128 == 0 {
			buf = buf[i+1+n:]
			break
		}
	}

	require.NotEmpty(t, buf, "the client was reset with nothing after the connack, so a "+
		"bound this server advertised itself went unexplained")
	require.Equal(t, byte(packets.Disconnect<<4), buf[0], "want a disconnect, got %02x", buf)
	require.Equal(t, packets.ErrPacketTooLarge.Code, buf[2], "disconnected with 0x%02X, want 0x95", buf[2])
}

func TestEstablishConnectionMaximumClientsReached(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 0
	s := New(&Options{
		Logger:       logger,
		Capabilities: cc,
	})
	_ = s.AddHook(new(AllowHook), nil)
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()

	// receive the connack
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrServerBusy)

	_ = r.Close()
}

// See https://github.com/mochi-mqtt/server/issues/178
func TestServerEstablishConnectionZeroByteUsernameIsValid(t *testing.T) {
	s := newServer()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectZeroByteUsername).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	// receive the connack error
	go func() {
		_, err := io.ReadAll(w)
		require.NoError(t, err)
	}()

	err := <-o
	require.NoError(t, err)

	_ = r.Close()
}

func TestServerEstablishConnectionInvalidConnectAckFailure(t *testing.T) {
	s := newServer()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMalReservedBit).RawBytes)
		_ = w.Close()
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)

	_ = r.Close()
}

func TestServerEstablishConnectionBadPacket(t *testing.T) {
	s := newServer()

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnackBadProtocolVersion).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	err := <-o
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationRequireFirstConnect)

	_ = r.Close()
}

func TestServerEstablishConnectionOnConnectError(t *testing.T) {
	s := newServer()
	hook := new(modifiedHookBase)
	hook.fail = true
	err := s.AddHook(hook, nil)
	require.NoError(t, err)

	r, w := net.Pipe()
	o := make(chan error)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()

	err = <-o
	require.Error(t, err)
	require.ErrorIs(t, err, errTestHook)

	_ = r.Close()
}

func TestServerSendConnack(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Options.Capabilities.MaximumQos = 1
	cl.Properties.Props = packets.Properties{
		AssignedClientID: "mochi",
	}
	go func() {
		err := s.SendConnack(cl, packets.CodeSuccess, true, nil)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackMinMqtt5).RawBytes, buf)
}

func TestServerSendConnackFailureReason(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	go func() {
		err := s.SendConnack(cl, packets.ErrUnspecifiedError, true, nil)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackInvalidMinMqtt5).RawBytes, buf)
}

func TestServerSendConnackWithServerKeepalive(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Keepalive = 10
	cl.State.ServerKeepalive = true
	go func() {
		err := s.SendConnack(cl, packets.CodeSuccess, true, nil)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackServerKeepalive).RawBytes, buf)
}

func TestServerValidateConnect(t *testing.T) {
	packet := *packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).Packet
	invalidBitPacket := packet
	invalidBitPacket.ReservedBit = 1
	packetCleanIdPacket := packet
	// Its own CONNECT fields: the table's are shared by every test.
	cleanID := *packet.Connect
	cleanID.Clean = false
	cleanID.ClientIdentifier = ""
	packetCleanIdPacket.Connect = &cleanID
	tt := []struct {
		desc         string
		client       *Client
		capabilities Capabilities
		packet       packets.Packet
		expect       packets.Code
	}{
		{
			desc:         "unsupported protocol version",
			client:       &Client{Properties: ClientProperties{ProtocolVersion: 3}},
			capabilities: Capabilities{MinimumProtocolVersion: 4},
			packet:       packet,
			expect:       packets.ErrUnsupportedProtocolVersion,
		},
		{
			desc:         "will qos not supported",
			client:       &Client{Properties: ClientProperties{Will: Will{Qos: 2}}},
			capabilities: Capabilities{MaximumQos: 1},
			packet:       packet,
			expect:       packets.ErrQosNotSupported,
		},
		{
			desc:         "retain not supported",
			client:       &Client{Properties: ClientProperties{Will: Will{Retain: true}}},
			capabilities: Capabilities{RetainAvailable: 0},
			packet:       packet,
			expect:       packets.ErrRetainNotSupported,
		},
		{
			desc:         "invalid packet validate",
			client:       &Client{Properties: ClientProperties{Will: Will{Retain: true}}},
			capabilities: Capabilities{RetainAvailable: 0},
			packet:       invalidBitPacket,
			expect:       packets.ErrMalformedReservedBit,
		},
		{
			desc:         "mqtt3 clean no client id ",
			client:       &Client{Properties: ClientProperties{ProtocolVersion: 3}},
			capabilities: Capabilities{},
			packet:       packetCleanIdPacket,
			expect:       packets.ErrUnspecifiedError,
		},
	}

	s := newServer()
	for _, tx := range tt {
		t.Run(tx.desc, func(t *testing.T) {
			s.Options.Capabilities = &tx.capabilities
			err := s.validateConnect(tx.client, tx.packet)
			require.Error(t, err)
			require.ErrorIs(t, err, tx.expect)
		})
	}
}

func TestServerSendConnackAdjustedExpiryInterval(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.SessionExpiryInterval = uint32(300)
	s.Options.Capabilities.MaximumSessionExpiryInterval = 120
	go func() {
		err := s.SendConnack(cl, packets.CodeSuccess, false, nil)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedAdjustedExpiryInterval).RawBytes, buf)
}

func TestInheritClientSession(t *testing.T) {
	s := newServer()

	n := time.Now().Unix()

	existing, _, _ := newTestClient()
	existing.Net.Conn = nil
	existing.ID = "mochi"
	existing.State.Subscriptions.Add("a/b/c", packets.Subscription{Filter: "a/b/c", Qos: 1})
	existing.State.Inflight = NewInflights()
	existing.State.Inflight.Set(packets.Packet{PacketID: 1, Created: n - 1})
	existing.State.Inflight.Set(packets.Packet{PacketID: 2, Created: n - 2})

	s.Clients.Add(existing)

	cl, _, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5

	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, 0, cl.State.Subscriptions.Len())

	// Inherit existing client properties
	b := s.inheritClientSession(packets.Packet{Connect: &packets.ConnectParams{ClientIdentifier: "mochi"}}, cl, time.Now())
	require.True(t, b)
	require.Equal(t, 2, cl.State.Inflight.Len())
	require.Equal(t, 1, cl.State.Subscriptions.Len())

	// On clean, clear existing properties
	cl, _, _ = newTestClient()
	cl.Properties.ProtocolVersion = 5
	b = s.inheritClientSession(packets.Packet{Connect: &packets.ConnectParams{ClientIdentifier: "mochi", Clean: true}}, cl, time.Now())
	require.False(t, b)
	require.Equal(t, 0, cl.State.Inflight.Len())
	require.Equal(t, 0, cl.State.Subscriptions.Len())
}

func TestServerUnsubscribeClient(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	pk := packets.Subscription{Filter: "a/b/c", Qos: 1}
	cl.State.Subscriptions.Add("a/b/c", pk)
	s.Topics.Subscribe(cl.ID, pk)
	subs := s.Topics.Subscribers("a/b/c")
	require.Equal(t, 1, len(subs.Subscriptions))
	s.UnsubscribeClient(cl)
	subs = s.Topics.Subscribers("a/b/c")
	require.Equal(t, 0, len(subs.Subscriptions))
}

func TestServerProcessPacketFailure(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	err := s.processPacket(cl, packets.Packet{})
	require.Error(t, err)
}

func TestServerProcessPacketConnect(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()

	err := s.processPacket(cl, *packets.TPacketData[packets.Connect].Get(packets.TConnectClean).Packet)
	require.Error(t, err)
}

func TestServerProcessPacketPingreq(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Pingreq].Get(packets.TPingreq).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Pingresp].Get(packets.TPingresp).RawBytes, buf)
}

func TestServerProcessPacketPingreqError(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Stop(packets.CodeDisconnect)

	err := s.processPacket(cl, *packets.TPacketData[packets.Pingreq].Get(packets.TPingreq).Packet)
	require.Error(t, err)
	require.ErrorIs(t, cl.StopCause(), packets.CodeDisconnect)
}

func TestServerProcessPacketPublishInvalid(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()

	err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishInvalidQosMustPacketID).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationNoPacketID)
}

func TestInjectPacketPublishAndReceive(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	sender, _, w1 := newTestClient()
	sender.Net.Inline = true
	sender.ID = "sender"
	s.Clients.Add(sender)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c"})

	err := s.InjectPacket(sender, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r2))
	requireNothingBefore(t, s, receiver, r2, "the injected publish was written twice")
	_ = w1.Close()
	_ = w2.Close()
}

func TestServerPublishAndReceive(t *testing.T) {
	s := newServerWithInlineClient()

	_ = s.Serve()
	defer s.Close()

	sender, _, w1 := newTestClient()
	sender.Net.Inline = true
	sender.ID = "sender"
	s.Clients.Add(sender)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c"})

	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	err := s.Publish(pkx.TopicName, pkx.Payload, pkx.FixedHeader.Retain, pkx.FixedHeader.Qos)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r2))
	requireNothingBefore(t, s, receiver, r2, "the publish was written twice")
	_ = w1.Close()
	_ = w2.Close()
}

func TestServerPublishNoInlineClient(t *testing.T) {
	s := newServer()
	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	err := s.Publish(pkx.TopicName, pkx.Payload, pkx.FixedHeader.Retain, pkx.FixedHeader.Qos)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInlineClientNotEnabled)
}

func TestInjectPacketError(t *testing.T) {
	s := newServer()
	defer s.Close()
	cl, _, _ := newTestClient()
	cl.Net.Inline = true
	pkx := *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe).Packet
	pkx.Filters = packets.Subscriptions{}
	err := s.InjectPacket(cl, pkx)
	require.Error(t, err)
}

func TestInjectPacketPublishInvalidTopic(t *testing.T) {
	s := newServer()
	defer s.Close()
	cl, _, _ := newTestClient()
	cl.Net.Inline = true
	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	pkx.TopicName = "$SYS/test"
	err := s.InjectPacket(cl, pkx)
	require.NoError(t, err) // bypass topic validity and acl checks
}

func TestServerProcessPacketPublishAndReceive(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	sender, _, w1 := newTestClient()
	sender.ID = "sender"
	s.Clients.Add(sender)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c"})

	require.Equal(t, 0, len(s.Topics.Messages("a/b/c")))

	err := s.processPacket(sender, *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r2))
	requireNothingBefore(t, s, receiver, r2, "the publish was written twice")
	_ = w1.Close()
	_ = w2.Close()
	require.Equal(t, 1, len(s.Topics.Messages("a/b/c")))
}

func TestServerBuildAckError(t *testing.T) {
	s := newServer()
	properties := packets.Properties{
		User: []packets.UserProperty{
			{Key: "hello", Val: "世界"},
		},
	}
	ack := s.buildAck(7, packets.Puback, 1, properties, packets.ErrMalformedPacket)
	require.Equal(t, packets.Puback, ack.FixedHeader.Type)
	require.Equal(t, uint8(1), ack.FixedHeader.Qos)
	require.Equal(t, packets.ErrMalformedPacket.Code, ack.ReasonCode)
	// The refusal's own Reason String survives, and the request's User
	// Property does not. Both halves matter: dropping everything would
	// take the broker's explanation with it.
	require.Equal(t, packets.Properties{
		ReasonString: packets.ErrMalformedPacket.Reason,
	}, ack.Properties)
}

// An acknowledgement drops the properties of the request it answers.
// This was a capability the caller set; it is unconditional now, and the
// assertion is the same either way.
func TestServerBuildAckCarriesNoInheritedProperties(t *testing.T) {
	s := newServer()
	properties := packets.Properties{
		User: []packets.UserProperty{
			{Key: "hello", Val: "世界"},
		},
	}
	ack := s.buildAck(7, packets.Puback, 1, properties, packets.CodeGrantedQos1)
	require.Equal(t, packets.Puback, ack.FixedHeader.Type)
	require.Equal(t, uint8(1), ack.FixedHeader.Qos)
	require.Equal(t, packets.CodeGrantedQos1.Code, ack.ReasonCode)
	require.Equal(t, packets.Properties{}, ack.Properties)
}

func TestServerQoS1PubackReasonCode(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	bufCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		buf, err := io.ReadAll(r)
		if err != nil {
			errCh <- err
			return
		}
		bufCh <- buf
	}()

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	err := s.processPublish(cl, pk)
	require.NoError(t, err)
	_ = w.Close()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case buf := <-bufCh:
		require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)
	}
}

// A withheld packet written once quota frees stays in flight, so its PUBACK
// finds it and returns the quota it took. Deleted instead, the PUBACK found
// nothing and the slot was gone for the life of the session.
func TestServerProcessPacketWritesAWithheldPacketAndKeepsItInFlight(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	next := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	next.Expiry = -1
	cl.State.Inflight.Set(next)
	require.Equal(t, int32(5), cl.State.Inflight.sendQuota)

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
		require.NoError(t, err)
		// The acknowledgement wakes the write loop rather than writing here, so
		// the pipe this test reads stays open until the loop has written.
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if m, ok := cl.State.Inflight.Get(next.PacketID); ok && m.Expiry >= 0 {
				break
			}
		}
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).RawBytes, buf)
	require.Equal(t, int32(4), cl.State.Inflight.sendQuota)
	sent, ok := cl.State.Inflight.Get(next.PacketID)
	require.True(t, ok, "a packet written from the withheld set left the session's in-flight set")
	require.Equal(t, int64(0), sent.Expiry, "a written packet is still marked withheld")

	require.NoError(t, s.processPuback(cl, packets.Packet{PacketID: next.PacketID}))
	require.Equal(t, int32(5), cl.State.Inflight.sendQuota, "its PUBACK did not return the quota")
}

func TestServerProcessPublishAckFailure(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	cl, _, w := newTestClient()
	s.Clients.Add(cl)

	_ = w.Close()
	err := s.processPublish(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestServerProcessPublishOnPublishAckErrorRWError(t *testing.T) {
	s := newServer()
	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.ErrUnspecifiedError
	err := s.AddHook(hook, nil)
	require.NoError(t, err)

	cl, _, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)
	_ = w.Close()

	err = s.processPublish(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestServerProcessPublishOnPublishAckErrorContinue(t *testing.T) {
	s := newServer()
	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.ErrPayloadFormatInvalid
	err := s.AddHook(hook, nil)
	require.NoError(t, err)
	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPubackUnexpectedError).RawBytes, buf)
}

// TestServerProcessPublishOnPublishAckErrorQoS2GetsPubrec ensures a QoS 2
// publish rejected by an OnPublish hook is answered with a PUBREC, not a
// PUBACK - PUBREC is the spec-defined response to a QoS 2 PUBLISH, and a
// PUBACK here previously left a compliant sender's QoS 2 state machine
// with an ack type it never expects for that packet ID.
func TestServerProcessPublishOnPublishAckErrorQoS2GetsPubrec(t *testing.T) {
	s := newServer()
	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.ErrPayloadFormatInvalid
	err := s.AddHook(hook, nil)
	require.NoError(t, err)
	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	expected := &packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Pubrec},
		ProtocolVersion: 5,
		PacketID:        packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet.PacketID,
		ReasonCode:      packets.ErrPayloadFormatInvalid.Code,
		Properties: packets.Properties{
			ReasonString: packets.ErrPayloadFormatInvalid.Reason,
		},
	}
	var expectedBuf bytes.Buffer
	require.NoError(t, expected.PubrecEncode(&expectedBuf))
	require.Equal(t, expectedBuf.Bytes(), buf)
}

func TestServerProcessPublishOnPublishPkIgnore(t *testing.T) {
	s := newServer()
	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.CodeSuccessIgnore
	err := s.AddHook(hook, nil)
	require.NoError(t, err)
	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c"})

	require.Equal(t, 0, len(s.Topics.Messages("a/b/c")))

	receiverBuf := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(r2)
		require.NoError(t, err)
		receiverBuf <- buf
	}()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.NoError(t, err)
		_ = w.Close()
		_ = w2.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)
	require.Equal(t, []byte{}, <-receiverBuf)
	require.Equal(t, 0, len(s.Topics.Messages("a/b/c")))
}

func TestServerProcessPacketPublishMaximumReceive(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.ResetReceiveQuota(0)
	s.Clients.Add(cl)

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.Error(t, err)
		require.ErrorIs(t, err, packets.ErrReceiveMaximum)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectReceiveMaximum).RawBytes, buf)
}

// A publish to a $SYS topic is refused, and a client that asked for an
// acknowledgement gets one. Dropping the packet silently leaves a QoS 1 or
// 2 client waiting for a reply that never comes and holding a slot of its
// send quota for the life of the session.
//
// The answers are the same ones a publish an ACL denies receives, because
// it is the same refusal: the client may not write there.
func TestServerProcessPublishInvalidTopic(t *testing.T) {
	sysTopic := func(c byte) packets.Packet {
		pk := *packets.TPacketData[packets.Publish].Get(c).Packet
		pk.TopicName = "$SYS/any"
		return pk
	}

	tt := []struct {
		name             string
		protocolVersion  byte
		pk               packets.Packet
		expectErr        error
		expectResponse   []byte
		expectDisconnect bool
	}{
		{
			name:            "v4_QOS0",
			protocolVersion: 4,
			pk:              sysTopic(packets.TPublishBasic),
		},
		{
			// MQTT 3.1.1 has no reason code to put in a PUBACK, so the
			// refusal is a disconnect, exactly as it is for an ACL denial.
			// It is a silent one: v3 has no server-to-client Disconnect
			// packet, so the connection closes and nothing is sent.
			name:             "v4_QOS1",
			protocolVersion:  4,
			pk:               sysTopic(packets.TPublishQos1),
			expectErr:        packets.ErrNotAuthorized,
			expectDisconnect: true,
		},
		{
			name:             "v4_QOS2",
			protocolVersion:  4,
			pk:               sysTopic(packets.TPublishQos2),
			expectErr:        packets.ErrNotAuthorized,
			expectDisconnect: true,
		},
		{
			name:            "v5_QOS0",
			protocolVersion: 5,
			pk:              sysTopic(packets.TPublishBasicMqtt5),
		},
		{
			name:            "v5_QOS1",
			protocolVersion: 5,
			pk:              sysTopic(packets.TPublishQos1Mqtt5),
			expectResponse:  ackPubackNotAuthorized,
		},
		{
			name:            "v5_QOS2",
			protocolVersion: 5,
			pk:              sysTopic(packets.TPublishQos2Mqtt5),
			expectResponse:  ackPubrecNotAuthorized,
		},
	}

	for _, tx := range tt {
		t.Run(tx.name, func(t *testing.T) {
			s := newServer()
			_ = s.Serve()
			defer s.Close()

			cl, r, w := newTestClient()
			cl.Properties.ProtocolVersion = tx.protocolVersion
			s.Clients.Add(cl)

			// The published error is captured rather than asserted inside
			// the goroutine: require calls Goexit on failure, which would
			// skip the Close below and leave the read blocking for ever.
			var publishErr error
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { _ = w.Close() }()
				publishErr = s.processPublish(cl, tx.pk)
			}()

			buf, err := io.ReadAll(r)
			require.NoError(t, err)
			wg.Wait()

			require.ErrorIs(t, publishErr, tx.expectErr)
			if tx.expectResponse != nil {
				require.Equal(t, tx.expectResponse, buf)
			} else {
				require.Empty(t, buf)
			}
			require.Equal(t, tx.expectDisconnect, cl.Closed())
		})
	}
}

func TestServerProcessPublishACLCheckDeny(t *testing.T) {
	tt := []struct {
		name             string
		protocolVersion  byte
		pk               packets.Packet
		expectErr        error
		expectReponse    []byte
		expectDisconnect bool
	}{
		{
			name:             "v4_QOS0",
			protocolVersion:  4,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet,
			expectErr:        nil,
			expectReponse:    nil,
			expectDisconnect: false,
		},
		{
			name:             "v4_QOS1",
			protocolVersion:  4,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet,
			expectErr:        packets.ErrNotAuthorized,
			expectReponse:    nil,
			expectDisconnect: true,
		},
		{
			name:             "v4_QOS2",
			protocolVersion:  4,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet,
			expectErr:        packets.ErrNotAuthorized,
			expectReponse:    nil,
			expectDisconnect: true,
		},
		{
			name:             "v5_QOS0",
			protocolVersion:  5,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishBasicMqtt5).Packet,
			expectErr:        nil,
			expectReponse:    nil,
			expectDisconnect: false,
		},
		{
			name:             "v5_QOS1",
			protocolVersion:  5,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Mqtt5).Packet,
			expectErr:        nil,
			expectReponse:    ackPubackNotAuthorized,
			expectDisconnect: false,
		},
		{
			name:             "v5_QOS2",
			protocolVersion:  5,
			pk:               *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2Mqtt5).Packet,
			expectErr:        nil,
			expectReponse:    ackPubrecNotAuthorized,
			expectDisconnect: false,
		},
	}

	for _, tx := range tt {
		tx := tx // one variable per iteration: the goroutine below reads it
		t.Run(tx.name, func(t *testing.T) {
			cc := NewDefaultServerCapabilities()
			s := New(&Options{
				Logger:       logger,
				Capabilities: cc,
			})
			_ = s.AddHook(new(DenyHook), nil)
			_ = s.Serve()
			defer s.Close()

			cl, r, w := newTestClient()
			cl.Properties.ProtocolVersion = tx.protocolVersion
			s.Clients.Add(cl)

			wg := sync.WaitGroup{}
			errCh := make(chan error, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				errCh <- s.processPublish(cl, tx.pk)
				_ = w.Close()
			}()

			buf, err := io.ReadAll(r)
			wg.Wait()
			require.NoError(t, err)

			if tx.expectReponse != nil {
				require.Equal(t, tx.expectReponse, buf)
			}

			require.ErrorIs(t, <-errCh, tx.expectErr)
			require.Equal(t, tx.expectDisconnect, cl.Closed())
		})
	}
}

func TestServerProcessPublishOnMessageRecvRejected(t *testing.T) {
	s := newServer()
	require.NotNil(t, s)
	hook := new(modifiedHookBase)
	hook.fail = true
	hook.err = packets.ErrRejectPacket

	err := s.AddHook(hook, nil)
	require.NoError(t, err)

	_ = s.Serve()
	defer s.Close()
	cl, _, _ := newTestClient()
	err = s.processPublish(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	require.NoError(t, err) // packets rejected silently
}

func TestServerProcessPacketPublishQos0(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte{}, buf)
}

func TestServerProcessPacketPublishQos1PacketIDInUse(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Publish}})

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)

	// A Publish entry is a message the SERVER sent, under an identifier the
	// server assigned, so it is not in use by this exchange and is left
	// alone. The Pubrec case below is the one that is.
}

// Packet Identifiers assigned by the Client and by the Server are
// independent (MQTT-2.2.1), and the specification's own comment there
// describes this case: a client publishing under an identifier the server is
// concurrently using for one of its own deliveries. The delivery has to
// survive it - discarding it ends the QoS 1 flow, so it is never
// acknowledged, never resent, and its send quota is never returned.
func TestServerProcessPacketPublishDoesNotDiscardServerDelivery(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	delivery := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    7,
		TopicName:   "server/delivery",
		Payload:     []byte("out"),
	}
	cl.State.Inflight.Set(delivery)

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)

	kept, ok := cl.State.Inflight.Get(7)
	require.True(t, ok, "the server's own QoS 1 delivery was discarded by an inbound PUBLISH carrying the same packet identifier")
	require.Equal(t, packets.Publish, kept.FixedHeader.Type)
	require.Equal(t, "server/delivery", kept.TopicName)
}

// A QoS 2 PUBLISH repeated under an identifier already awaiting PUBREL is
// answered the way the first one was, and is not delivered again.
//
// §4.3.3 asks a client to re-send the PUBLISH when the PUBREC has not come
// back, so the repeat is the ordinary case rather than an error. The server
// already owns the message, so the second PUBREC says what the first said.
//
// **It used to answer 0x91**, Packet Identifier in use, which by
// [MQTT-4.4.0-2] tells the publisher the exchange is over and the
// identifier is free: it records a failure over a message the broker holds
// and will deliver, and may reuse the identifier for another. 0x91 is for
// an identifier in use by a *different* exchange, and §2.2.1 makes that
// impossible here.
func TestServerProcessPacketPublishQos2RepeatIsAcknowledged(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.Set(packets.Packet{PacketID: 7, FixedHeader: packets.FixedHeader{Type: packets.Pubrec}})

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2Mqtt5).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	// PUBREC, packet id 7, and nothing after it: a success reason code with
	// no properties is the short form, which is the shape that says 0x00.
	require.Equal(t, []byte{packets.Pubrec << 4, 2, 0, 7}, buf)

	// The exchange it was already in is untouched: the entry stays, and the
	// repeat added no second one.
	pki, ok := cl.State.Inflight.Get(7)
	require.True(t, ok, "the first exchange was dropped by the repeat")
	require.Equal(t, packets.Pubrec, pki.FixedHeader.Type)
}

func TestServerProcessPacketPublishQos1(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)
}

func TestServerProcessPacketPublishQos2(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).RawBytes, buf)
}

func TestServerProcessPacketPublishDowngradeQos(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumQos = 1
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos2).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Puback].Get(packets.TPuback).RawBytes, buf)
}

// fanoutProbe watches a fan-out from inside each delivery: at OnQosPublish,
// which makeDelivery calls holding that target's handover read lock, it
// records what the target was sent and how many of the other targets' locks
// are held at that moment. It is the only ACL hook, so what it denies is
// denied.
type fanoutProbe struct {
	HookBase
	mu         sync.Mutex
	targets    map[string]*Client
	deny       map[string]bool
	heldOthers []int
	seen       map[string][]string
}

func (h *fanoutProbe) ID() string { return "fanout-probe" }

func (h *fanoutProbe) Provides(b byte) bool {
	return b == OnQosPublish || b == OnACLCheck || b == OnConnectAuthenticate
}

func (h *fanoutProbe) OnConnectAuthenticate(*Client, packets.Packet) bool { return true }

func (h *fanoutProbe) OnACLCheck(cl *Client, _ string, _ bool) bool { return !h.deny[cl.ID] }

func (h *fanoutProbe) OnQosPublish(cl *Client, pk packets.Packet, _ int64, _ int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	held := 0
	for _, other := range h.targets {
		if other == cl {
			continue
		}
		if other.State.handover.TryLock() {
			other.State.handover.Unlock()
		} else {
			held++
		}
	}
	h.heldOthers = append(h.heldOthers, held)
	h.seen[cl.ID] = append(h.seen[cl.ID], string(pk.Payload))
}

func fanoutServer(t *testing.T, n int, deny func(i int) bool) (*Server, *fanoutProbe) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumMessageExpiryInterval = 0
	cc.ReceiveMaximum = 0
	s := New(&Options{Logger: logger, Capabilities: cc})
	probe := &fanoutProbe{targets: map[string]*Client{}, deny: map[string]bool{}, seen: map[string][]string{}}
	require.NoError(t, s.AddHook(probe, nil))
	for i := 0; i < n; i++ {
		cl, r, _ := newTestClient()
		go func() { _, _ = io.Copy(io.Discard, r) }()
		cl.ID = fmt.Sprintf("sub-%02d", i)
		s.Clients.Add(cl)
		require.True(t, s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b", Qos: 1}))
		probe.targets[cl.ID] = cl
		if deny != nil && deny(i) {
			probe.deny[cl.ID] = true
		}
	}
	return s, probe
}

// **A fan-out holds one target at a time.** Each delivery is made while its
// target is read-locked against a takeover, and no other target is: the two
// passes held every target's lock from the first delivery prepared to the
// last made, so a takeover of any subscriber waited for the whole fan-out.
func TestAFanOutHoldsOneTargetAtATime(t *testing.T) {
	s, probe := fanoutServer(t, 20, nil)
	defer s.Close()
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, TopicName: "a/b", Payload: []byte("m")}
	s.publishToSubscribers(pk)
	probe.mu.Lock()
	defer probe.mu.Unlock()
	require.Len(t, probe.heldOthers, 20, "every subscriber's delivery was made, or this measured fewer")
	for i, held := range probe.heldOthers {
		require.Zero(t, held, "delivery %d was made with %d other subscribers' handover locks held", i, held)
	}
}

// **Per subscriber, a publisher's messages arrive in order, and each is
// checked against that subscriber's ACL** (RFC 0003 "Ordering": broadcast is
// ordered per subscriber, and only per subscriber). Ten subscribers, the odd
// ones denied; five publishes from one publisher, one after another.
func TestEachAllowedSubscriberGetsAPublishersMessagesInOrder(t *testing.T) {
	s, probe := fanoutServer(t, 10, func(i int) bool { return i%2 == 1 })
	defer s.Close()
	want := []string{"m0", "m1", "m2", "m3", "m4"}
	for _, m := range want {
		s.publishToSubscribers(packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
			TopicName: "a/b", Payload: []byte(m)})
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("sub-%02d", i)
		if i%2 == 1 {
			require.Empty(t, probe.seen[id], "%s is denied by the ACL and was sent %v", id, probe.seen[id])
			continue
		}
		require.Equal(t, want, probe.seen[id], "%s was sent its publisher's messages out of order or short", id)
	}
}

func TestPublishToSubscribersSelfNoLocal(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", NoLocal: true})
	require.True(t, subbed)

	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	pkx.Origin = cl.ID
	s.publishToSubscribers(pkx)
	requireNothingBefore(t, s, cl, r, "a No Local subscriber was sent its own publish")
	_ = w.Close()
}

func TestPublishToSubscribers(t *testing.T) {
	s := newServer()
	cl, r1, w1 := newTestClient()
	cl.ID = "cl1"
	cl2, r2, w2 := newTestClient()
	cl2.ID = "cl2"
	cl3, r3, w3 := newTestClient()
	cl3.ID = "cl3"
	s.Clients.Add(cl)
	s.Clients.Add(cl2)
	s.Clients.Add(cl3)
	require.True(t, s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c"}))
	require.True(t, s.Topics.Subscribe(cl2.ID, packets.Subscription{Filter: SharePrefix + "/tmp/a/b/c"}))
	require.True(t, s.Topics.Subscribe(cl3.ID, packets.Subscription{Filter: SharePrefix + "/tmp/a/b/c"}))

	s.publishToSubscribers(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)

	want := packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes
	require.Equal(t, want, readPacket(t, r1))
	requireNothingBefore(t, s, cl, r1, "the plain subscriber was written the publish twice")
	// One member of the shared group, either, and once.
	rcv2, rcv3 := upToMarker(t, s, cl2, r2), upToMarker(t, s, cl3, r3)
	require.Equal(t, [][]byte{want}, append(rcv2, rcv3...),
		"the shared group was not sent the publish exactly once: % x and % x", rcv2, rcv3)
	_ = w1.Close()
	_ = w2.Close()
	_ = w3.Close()
}

func TestPublishToSubscribersMessageExpiryDelta(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumMessageExpiryInterval = 86400
	cl, r1, w1 := newTestClient()
	cl.ID = "cl1"
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)
	require.True(t, s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c"}))

	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	pkx.Created = time.Now().Unix() - 30
	s.publishToSubscribers(pkx)

	b := readPacket(t, r1)
	require.Equal(t, uint32(s.Options.Capabilities.MaximumMessageExpiryInterval-30), binary.BigEndian.Uint32(b[11:15]))
	requireNothingBefore(t, s, cl, r1, "the publish was written twice")
	_ = w1.Close()
}

func TestPublishToSubscribersIdentifiers(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)
	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/+", Identifier: 2})
	require.True(t, subbed)
	subbed = s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/#", Identifier: 3})
	require.True(t, subbed)
	subbed = s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "d/e/f", Identifier: 4})
	require.True(t, subbed)

	go func() {
		s.publishToSubscribers(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	}()

	want := packets.TPacketData[packets.Publish].Get(packets.TPublishSubscriberIdentifier).RawBytes
	receiverBuf := make(chan []byte)
	go func() {
		buf := make([]byte, len(want))
		_, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		receiverBuf <- buf
	}()

	require.Equal(t, want, <-receiverBuf)
	_ = w.Close()
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Empty(t, rest, "the server wrote more than the packet asserted on")
}

func TestPublishToSubscribersPkIgnore(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "#", Identifier: 1})
	require.True(t, subbed)

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
	pk.Ignore = true
	s.publishToSubscribers(pk)
	requireNothingBefore(t, s, cl, r, "a publish marked Ignore was delivered")
	_ = w.Close()
}

// selectSubscribersHook records every call and removes one client from the
// delivery, which is the first thing OnSelectSubscribers is documented to
// be for: "programmatically remove or add clients to a publish to
// subscribers process".
type selectSubscribersHook struct {
	HookBase
	calls  int
	remove string
}

func (h *selectSubscribersHook) ID() string { return "select-subscribers" }

func (h *selectSubscribersHook) Provides(b byte) bool {
	return b == OnSelectSubscribers
}

func (h *selectSubscribersHook) OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers {
	h.calls++
	delete(subs.Subscriptions, h.remove)
	return subs
}

// The OnSelectSubscribers hook is documented as being called "when subscribers have
// been collected for a topic", and as being usable to "programmatically
// remove or add clients to a publish to subscribers process". It was called
// only when the collected set held a shared subscription, so on a topic
// with none - which is most topics on most brokers - the hook never ran and
// that first purpose could not be reached at all.
//
// A hook that withholds a message from a client therefore did nothing, with
// no error and no log line, which reads as a hook that was never installed.
func TestPublishToSubscribersCallsOnSelectSubscribersWithoutSharedSubscriptions(t *testing.T) {
	s := newServer()
	hook := &selectSubscribersHook{remove: "removed"}
	require.NoError(t, s.AddHook(hook, nil))

	kept, r, w := newTestClient()
	kept.ID = "kept"
	s.Clients.Add(kept)
	require.True(t, s.Topics.Subscribe(kept.ID, packets.Subscription{Filter: "#"}))

	removed, r2, w2 := newTestClient()
	removed.ID = "removed"
	s.Clients.Add(removed)
	require.True(t, s.Topics.Subscribe(removed.ID, packets.Subscription{Filter: "#"}))

	s.publishToSubscribers(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)

	require.Equal(t,
		packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r))
	requireNothingBefore(t, s, kept, r, "the kept subscriber was written the publish twice")
	// The one the hook removed receives nothing, which is the whole point
	// of the documented behaviour.
	requireNothingBefore(t, s, removed, r2, "the subscriber the hook removed was sent the publish")
	require.Equal(t, 1, hook.calls)
	_ = w.Close()
	_ = w2.Close()
}

// sharedSelectingHook chooses one member of a shared group and empties the
// group while doing it, which the documentation invites: a hook selecting
// "the subscriber for a shared group in a custom manner" says which by
// filling SharedSelected, and what it leaves in Shared is its own business.
type sharedSelectingHook struct {
	HookBase
	choose string
}

func (h *sharedSelectingHook) ID() string { return "shared-selecting" }

func (h *sharedSelectingHook) Provides(b byte) bool {
	return b == OnSelectSubscribers
}

func (h *sharedSelectingHook) OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers {
	var chosen packets.Subscription
	for _, group := range subs.Shared {
		for id, sub := range group {
			if id == h.choose {
				chosen = sub
			}
		}
	}
	subs.Shared = map[string]map[string]packets.Subscription{}
	subs.SharedSelected = map[string]packets.Subscription{h.choose: chosen}
	return subs
}

// A hook that selects for a shared group by emptying Shared must still have
// its choice delivered.
//
// Whether there were shared subscriptions has to be decided before the hook
// runs. Gating the merge on what the hook left behind reads as no shared
// subscriptions at all, so SharedSelected is never merged into
// Subscriptions and the message reaches nobody - a queue whose workers
// simply stop being given work, with nothing anywhere reporting a fault.
func TestPublishToSubscribersMergesASharedChoiceThatEmptiedTheGroup(t *testing.T) {
	s := newServer()
	require.NoError(t, s.AddHook(&sharedSelectingHook{choose: "worker-b"}, nil))

	a, ra, wa := newTestClient()
	a.ID = "worker-a"
	s.Clients.Add(a)
	require.True(t, s.Topics.Subscribe(a.ID, packets.Subscription{
		Filter: "$share/group/a/b/c", Identifier: 1}))

	b, rb, wb := newTestClient()
	b.ID = "worker-b"
	s.Clients.Add(b)
	require.True(t, s.Topics.Subscribe(b.ID, packets.Subscription{
		Filter: "$share/group/a/b/c", Identifier: 1}))

	s.publishToSubscribers(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)

	// The chosen worker receives it, once, and the other does not.
	require.Len(t, upToMarker(t, s, b, rb), 1, "the chosen member was not sent the publish once")
	requireNothingBefore(t, s, a, ra, "the member the hook did not choose was sent the publish")
	_ = wa.Close()
	_ = wb.Close()
}

func TestPublishToClientServerDowngradeQos(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumQos = 1

	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	_, ok := cl.State.Inflight.Get(1)
	require.False(t, ok)
	cl.State.packetID = 6 // just to match the same packet id (7) in the fixtures

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
		pkx.FixedHeader.Qos = 2
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 2}, pkx)
	}()

	// publishToClient queues the packet; WriteLoop writes it. Reading the
	// bytes the assertion is about is what waits for that write - closing
	// the pipe on a timer instead let ReadAll return early and short, and
	// the failure it produced named the payload rather than the race.
	want := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).RawBytes
	receiverBuf := make(chan []byte)
	go func() {
		buf := make([]byte, len(want))
		_, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		receiverBuf <- buf
	}()

	require.Equal(t, want, <-receiverBuf)

	// And nothing beyond it: reading a fixed length would otherwise pass on
	// a prefix of a longer write, which the ReadAll this replaced caught.
	_ = w.Close()
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Empty(t, rest, "the server wrote more than the packet asserted on")
}

func TestPublishToClientSubscriptionDowngradeQos(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumQos = 2

	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	_, ok := cl.State.Inflight.Get(1)
	require.False(t, ok)
	cl.State.packetID = 6 // just to match the same packet id (7) in the fixtures

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
		pkx.FixedHeader.Qos = 2
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, pkx)
	}()

	// publishToClient queues the packet; WriteLoop writes it. Reading the
	// bytes the assertion is about is what waits for that write - closing
	// the pipe on a timer instead let ReadAll return early and short, and
	// the failure it produced named the payload rather than the race.
	want := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).RawBytes
	receiverBuf := make(chan []byte)
	go func() {
		buf := make([]byte, len(want))
		_, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		receiverBuf <- buf
	}()

	require.Equal(t, want, <-receiverBuf)

	// And nothing beyond it: reading a fixed length would otherwise pass on
	// a prefix of a longer write, which the ReadAll this replaced caught.
	_ = w.Close()
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Empty(t, rest, "the server wrote more than the packet asserted on")
}

func TestPublishToClientExceedClientWritesPending(t *testing.T) {
	var sendQuota uint16 = 5
	s := newServer()

	_, w := net.Pipe()
	cl := newClient(w, &ops{
		info:  new(system.Info),
		hooks: new(Hooks),
		log:   logger,
		options: &Options{
			Capabilities: &Capabilities{
				MaximumClientWritesPending: 3,
				maximumPacketID:            10,
			},
		},
	})
	cl.Properties.Props.ReceiveMaximum = sendQuota
	cl.State.Inflight.ResetSendQuota(int32(cl.Properties.Props.ReceiveMaximum))

	s.Clients.Add(cl)

	// Full of QoS 1 deliveries, which a withheld one may not be written past.
	for i := int32(0); i < cl.ops.options.Capabilities.MaximumClientWritesPending; i++ {
		cl.State.outbound.push(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})
		atomic.AddInt32(&cl.State.outboundQty, 1)
		cl.State.outboundQoS.Add(1)
	}

	id, _ := cl.NextPacketID()
	cl.State.Inflight.Set(packets.Packet{PacketID: uint16(id)})
	cl.State.Inflight.DecreaseSendQuota()
	sendQuota--

	// QoS 0 at a full queue is shed.
	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 2}, packets.Packet{})
	require.Error(t, err)
	require.ErrorIs(t, packets.ErrPendingClientWritesExceeded, err)
	require.Equal(t, int32(sendQuota), atomic.LoadInt32(&cl.State.Inflight.sendQuota))

	// QoS 1 at a full queue waits in its session with its window slot given
	// back, and is not dropped.
	out, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 2}, packets.Packet{FixedHeader: packets.FixedHeader{Qos: 1}})
	require.NoError(t, err)
	require.Equal(t, int32(sendQuota), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	held, ok := cl.State.Inflight.Get(out.PacketID)
	require.True(t, ok, "a QoS 1 delivery that found the queue full left the session")
	require.Negative(t, held.Expiry, "a QoS 1 delivery that found the queue full is not withheld")
}

func TestPublishToClientServerTopicAlias(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.TopicAliasMaximum = 5
	s.Clients.Add(cl)

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasicMqtt5).Packet
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: pkx.TopicName}, pkx)
		_, _ = s.publishToClient(cl, packets.Subscription{Filter: pkx.TopicName}, pkx)
	}()

	// **No outbound alias, however many the client allows** (saguin): an
	// alias is connection state, and a packet kept in flight outlives the
	// connection that registered it. So both publishes are the plain encoding
	// - the topic name and an empty property block - and byte for byte the
	// same. Not the fixture's RawBytes, which carry an alias property of
	// their own. Whatever arrives in a second is what is compared, so a
	// shortened or lengthened packet fails rather than hangs.
	want := []byte{
		0x30, 0x13, // PUBLISH, QoS 0, 19 bytes
		0x00, 0x05, 'a', '/', 'b', '/', 'c', // the topic name, every time
		0x00, // no properties: no Topic Alias (0x23)
		'h', 'e', 'l', 'l', 'o', ' ', 'm', 'o', 'c', 'h', 'i',
	}
	chunks := make(chan []byte, 16)
	go func() {
		for {
			buf := make([]byte, 1024)
			n, err := r.Read(buf)
			if n > 0 {
				chunks <- buf[:n]
			}
			if err != nil {
				return
			}
		}
	}()
	var ret []byte
	for deadline := time.After(time.Second); ; {
		select {
		case c := <-chunks:
			ret = append(ret, c...)
			continue
		case <-deadline:
		}
		break
	}
	require.Equal(t, append(append([]byte{}, want...), want...), ret,
		"two publishes to a client allowing aliases should both be the plain encoding")

	_ = w.Close()
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Empty(t, rest, "the server wrote more than the two packets asserted on")
}

func TestPublishToClientMqtt3RetainFalseLeverageNoConn(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Net.Conn = nil

	out, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", RetainAsPublished: true}, *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.False(t, out.FixedHeader.Retain)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.CodeDisconnect)
}

func TestPublishToClientMqtt5RetainAsPublishedTrueLeverageNoConn(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.Net.Conn = nil

	out, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", RetainAsPublished: true}, *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.True(t, out.FixedHeader.Retain)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.CodeDisconnect)
}

// A session holding its limits.session_queue_bytes of deliveries it cannot
// give up - on the wire to a connected client, unacknowledged - is refused
// the next one, and counted; one that fits exactly is taken; and a session
// holding nothing is never refused for a delivery's size alone.
func TestPublishToClientRefusedAtTheSessionBound(t *testing.T) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	onWire := func(id uint16) packets.Packet {
		m := publish.Copy(false)
		m.PacketID, m.Origin, m.Payload = id, "someone", make([]byte, 256)
		return m
	}
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	for id := uint16(1); id <= 3; id++ {
		cl.State.Inflight.Set(onWire(id))
	}
	held := cl.State.Inflight.Bytes()
	require.Positive(t, held)
	require.Zero(t, cl.State.Inflight.DroppableBytes(false), "nothing held here may be given up")

	// Room for exactly one more.
	s.Options.ClientSessionQueueBytes = held + inflightSize(publish)
	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, publish)
	require.NoError(t, err, "a delivery that fits the bound exactly was refused")
	require.Equal(t, int64(4), cl.State.Inflight.Messages())

	// And none past it.
	_, err = s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, publish)
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
	require.Equal(t, int64(4), cl.State.Inflight.Messages())
	require.Equal(t, int64(1), s.Info.SessionQueueDropped.Load())
	require.Equal(t, int64(1), s.Info.InflightDropped.Load())

	// A session holding nothing takes a delivery larger than its bound.
	empty, er, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, er) }()
	empty.ops.options = s.Options
	big := publish.Copy(false)
	big.Payload = make([]byte, int(s.Options.ClientSessionQueueBytes)*2)
	_, err = s.publishToClient(empty, packets.Subscription{Filter: "a/b/c", Qos: 1}, big)
	require.NoError(t, err, "a session holding nothing was refused a delivery for its size")
}

// RFC 0002 `limits.session_queue_bytes`: a full session holding deliveries it
// could give up takes a new one and gives up its oldest, counted.
func TestAFullSessionGivesUpItsOldest(t *testing.T) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	withheld := func(id uint16) packets.Packet {
		m := publish.Copy(false)
		m.PacketID, m.Origin, m.Expiry, m.Payload = id, "someone", -1, make([]byte, 256)
		return m
	}
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	for id := uint16(1); id <= 3; id++ {
		cl.State.Inflight.Set(withheld(id))
	}
	require.Equal(t, cl.State.Inflight.Bytes(), cl.State.Inflight.DroppableBytes(false),
		"everything held here may be given up, so this proves nothing")
	s.Options.ClientSessionQueueBytes = cl.State.Inflight.Bytes()

	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, publish)
	require.NoError(t, err, "the new delivery was refused where the oldest could go")
	var got []uint16
	for _, m := range cl.State.Inflight.GetAll(false) {
		got = append(got, m.PacketID)
	}
	require.NotContains(t, got, uint16(1), "the oldest was kept")
	require.Positive(t, s.Info.SessionQueueDropped.Load())
	require.LessOrEqual(t, cl.State.Inflight.Bytes(), s.Options.ClientSessionQueueBytes)
}

// RFC 0002 `limits.session_queue_bytes`: no more than half the bound is written
// and unacknowledged. A delivery past that half is withheld, and so is every
// one after it, however small, so none overtakes another; one is written
// whatever its size when nothing is on the wire; and an acknowledgement writes
// as many withheld as the half then has room for, earliest first. The same
// with a window of its own and with none, as a client stating no Receive
// Maximum has, and a window spent on nothing withheld comes back whole.
func TestAtMostHalfTheSessionBoundIsWrittenUnacknowledged(t *testing.T) {
	for _, window := range []int32{100, 0} {
		t.Run(fmt.Sprintf("window %d", window), func(t *testing.T) {
			halfTheBoundIsWritten(t, window)
		})
	}
}

// queueWritten waits until the write loop has written everything queued for
// the client's socket, which it does on a goroutine of its own.
func queueWritten(t *testing.T, cl *Client) {
	t.Helper()
	require.Eventually(t, func() bool { return atomic.LoadInt32(&cl.State.outboundQty) == 0 },
		time.Second, time.Millisecond, "the write loop did not write what was queued")
}

func halfTheBoundIsWritten(t *testing.T, window int32) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	sized := func(payload int) packets.Packet {
		m := publish.Copy(false)
		m.Origin, m.Payload = "someone", make([]byte, payload)
		return m
	}
	big, small := sized(4096), sized(16)
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	cl.State.Inflight.ResetSendQuota(window)
	// The half holds one big delivery and two small ones, and no more.
	s.Options.ClientSessionQueueBytes = 2 * (inflightSize(big) + 2*inflightSize(small))
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}

	withheld := func() []uint16 {
		var ids []uint16
		for _, m := range cl.State.Inflight.GetAll(false) {
			if m.Expiry < 0 {
				ids = append(ids, m.PacketID)
			}
		}
		return ids
	}

	first, err := s.publishToClient(cl, sub, sized(3*int(s.Options.ClientSessionQueueBytes)))
	require.NoError(t, err)
	require.Empty(t, withheld(), "a delivery larger than the half was withheld with nothing on the wire")
	queueWritten(t, cl)
	require.NoError(t, s.processPuback(cl, packets.Packet{PacketID: first.PacketID}))

	one, err := s.publishToClient(cl, sub, big)
	require.NoError(t, err)
	queueWritten(t, cl)
	two, err := s.publishToClient(cl, sub, big)
	require.NoError(t, err)
	three, err := s.publishToClient(cl, sub, small)
	require.NoError(t, err)
	four, err := s.publishToClient(cl, sub, small)
	require.NoError(t, err)
	require.Equal(t, []uint16{two.PacketID, three.PacketID, four.PacketID}, withheld(),
		"past the half, a delivery and every one after it wait, the small ones too")

	require.NoError(t, s.processPacket(cl, packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: one.PacketID}))
	// Written by the client's write loop, which the acknowledgement wakes, so
	// this waits for the write rather than for the goroutine that made room.
	require.Eventually(t, func() bool { return len(withheld()) == 0 }, time.Second, time.Millisecond,
		"one acknowledgement made room for all three, and wrote fewer")

	// What was written fills the half again, so the next waits.
	five, err := s.publishToClient(cl, sub, small)
	require.NoError(t, err)
	require.Equal(t, []uint16{five.PacketID}, withheld(), "the half was full of what one acknowledgement wrote, and more was written")

	for _, id := range []uint16{two.PacketID, three.PacketID, four.PacketID, five.PacketID} {
		require.NoError(t, s.processPacket(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: id}))
	}
	require.Zero(t, cl.State.Inflight.Len())
	require.Equal(t, window, atomic.LoadInt32(&cl.State.Inflight.sendQuota),
		"with everything acknowledged the window is not whole: a withheld delivery took more of it than it gave back")
}

// A delivery prepared as withheld for want of room on the wire is written
// when it is made if an acknowledgement made room in between. That
// acknowledgement found nothing withheld to write, and nothing else would
// write it: with nothing left on the wire, no acknowledgement is coming.
func TestADeliveryWithheldIsWrittenIfRoomOpensBeforeItIsMade(t *testing.T) {
	m := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet.Copy(false)
	m.Origin, m.Payload = "someone", make([]byte, 4096)
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	cl.State.Inflight.ResetSendQuota(100)
	s.Options.ClientSessionQueueBytes = InflightTableCost + 2*inflightSize(m) // the half holds one
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}

	first, err := s.publishToClient(cl, sub, m)
	require.NoError(t, err)
	queueWritten(t, cl)
	d, err := s.prepareDelivery(cl, sub, m)
	require.NoError(t, err)
	require.True(t, d.withheld, "with the half full the delivery was not prepared as withheld, so this says nothing")

	require.NoError(t, s.processPuback(cl, packets.Packet{PacketID: first.PacketID}))
	out, err := s.makeDelivery(d, m)
	require.NoError(t, err)
	// **Written by the client's write loop rather than by the caller.**
	// makeDelivery wakes the loop instead of writing on the goroutine that
	// made the delivery, so this waits for the write; what the rule forbids is
	// the delivery waiting for an acknowledgement that is never coming.
	require.Eventually(t, func() bool {
		made, ok := cl.State.Inflight.Get(out.PacketID)
		return ok && made.Expiry >= 0
	}, time.Second, time.Millisecond,
		"the delivery was left withheld with nothing on the wire and nothing coming to write it")
}

// unwrittenClient is a client of s whose socket is drained but whose write
// loop is not running, so what is queued for it stays queued until a test
// starts the loop.
func unwrittenClient(s *Server) *Client {
	r, w := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl := newClient(w, &ops{info: new(system.Info), hooks: new(Hooks), log: logger, options: s.Options})
	cl.ID = "unwritten"
	cl.State.Inflight.ResetSendQuota(100)
	return cl
}

// The queue for a client's socket holds no more bytes than half its session's
// bound, whatever slots it has free, and an empty queue takes a packet however
// large. A QoS 0 delivery past it is shed and counted; RFC 0005
// `saguin_deliveries_dropped_total`.
func TestTheSocketQueueIsBoundedByBytes(t *testing.T) {
	m := packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet.Copy(false)
	m.Payload = make([]byte, 4096)
	s := newServer()
	s.Options.ClientSessionQueueBytes = 2 * inflightSize(m) // the share holds one
	sub := packets.Subscription{Filter: "a/b/c"}

	cl := unwrittenClient(s)
	require.Greater(t, cl.State.outbound.max, 2, "the queue has slots to spare, so only bytes can refuse")
	_, err := s.publishToClient(cl, sub, m)
	require.NoError(t, err)
	_, err = s.publishToClient(cl, sub, m)
	require.ErrorIs(t, err, packets.ErrPendingClientWritesExceeded, "a queue holding its share in bytes took another")
	require.Equal(t, int32(1), atomic.LoadInt32(&cl.State.outboundQty))

	large := m.Copy(false)
	large.Payload = make([]byte, 10*s.Options.ClientSessionQueueBytes)
	_, err = s.publishToClient(unwrittenClient(s), sub, large)
	require.NoError(t, err, "an empty queue refused a packet larger than the share")
}

// A QoS 1 delivery prepared while the socket's queue had room, and made once
// it had filled, stays in its session rather than being dropped: it is the
// case the preparation cannot see coming [MQTT-4.3.2].
func TestAQoS1DeliveryThatFindsTheQueueFullWhenMadeIsKept(t *testing.T) {
	m := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet.Copy(false)
	s := newServer()
	cl := unwrittenClient(s)
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}

	d, err := s.prepareDelivery(cl, sub, m)
	require.NoError(t, err)
	require.False(t, d.withheld, "prepared as withheld, so the queue filling afterwards says nothing")
	for cl.State.outbound.len() < cl.State.outbound.max {
		cl.State.outbound.push(&packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}})
		atomic.AddInt32(&cl.State.outboundQty, 1)
	}

	out, err := s.makeDelivery(d, m)
	require.NoError(t, err, "a QoS 1 delivery that found the queue full was refused")
	_, ok := cl.State.Inflight.Get(out.PacketID)
	require.True(t, ok, "a QoS 1 delivery that found the queue full left its session")
}

// A withheld delivery is not written straight to the socket while a QoS 1 or
// 2 delivery made before it waits in the queue, which would put the later one
// first [MQTT-4.6.0-1]; the write loop writes it once the queued one is out.
func TestAWithheldDeliveryIsNotWrittenPastAQueuedOne(t *testing.T) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	s := newServer()
	cl := unwrittenClient(s)

	queued := publish.Copy(false)
	queued.PacketID = 1
	cl.State.Inflight.Set(queued)
	require.True(t, cl.enqueue(&queued))
	waiting := publish.Copy(false)
	waiting.PacketID, waiting.Expiry = 2, -1
	cl.State.Inflight.Set(waiting)

	cl.drainWithheld()
	m, _ := cl.State.Inflight.Get(2)
	require.Negative(t, m.Expiry, "a withheld delivery was written past a QoS 1 delivery still queued")

	cl.armWriter()
	require.Eventually(t, func() bool {
		m, _ := cl.State.Inflight.Get(2)
		return m.Expiry >= 0
	}, time.Second, time.Millisecond, "the withheld delivery was not written once the queue was")
}

// A delivery made while a withheld one is claimed and not yet written waits
// behind it: nothing is withheld in that moment, and without the claim counted
// the new one would go to the socket's queue and could be written first.
func TestADeliveryMadeWhileAClaimIsBeingWrittenWaits(t *testing.T) {
	m := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet.Copy(false)
	m.PacketID, m.Expiry = 1, -1
	i := NewInflights()
	i.Set(m)
	_, ok := takeImmediate(i)
	require.True(t, ok)
	require.False(t, i.MayWrite(inflightSize(m), 0), "a delivery may be written while a claimed one is not yet")
	i.Claimed()
	require.True(t, i.MayWrite(inflightSize(m), 0))
}

// A resumed session re-sends what was on the wire, and writes what was
// withheld only within half its bound and in order: a small delivery behind
// a large one that does not fit waits with it rather than overtaking it
// [MQTT-4.6.0-1].
func TestAResumedSessionWritesNoMoreThanItsShareInOrder(t *testing.T) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	entry := func(id uint16, payload int, expiry int64) packets.Packet {
		m := publish.Copy(false)
		m.PacketID, m.Created, m.Expiry = id, int64(id), expiry
		m.Origin, m.Payload = "someone", make([]byte, payload)
		return m
	}
	written, large, small := entry(1, 4096, 0), entry(2, 4096, -1), entry(3, 16, -1)
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	cl.State.Inflight.ResetSendQuota(100)
	// The half holds what was written and the small one, not the large one.
	s.Options.ClientSessionQueueBytes = 2 * (inflightSize(written) + inflightSize(small))
	for _, m := range []packets.Packet{written, large, small} {
		cl.State.Inflight.Set(m)
	}

	require.NoError(t, cl.ResendInflightMessages(true))
	for _, id := range []uint16{large.PacketID, small.PacketID} {
		m, ok := cl.State.Inflight.Get(id)
		require.True(t, ok)
		require.Negative(t, m.Expiry, "delivery %d was written on resume past the half, or ahead of one waiting before it", id)
	}
}

func TestPublishToClientExhaustedPacketID(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	for i := uint32(0); i <= cl.ops.options.Capabilities.maximumPacketID; i++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: uint16(i)})
	}

	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c", Qos: 1}, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
	require.Equal(t, int64(1), s.Info.InflightDropped.Load())
}

func TestPublishToClientACLNotAuthorized(t *testing.T) {
	s := New(&Options{
		Logger: logger,
	})
	err := s.AddHook(new(DenyHook), nil)
	require.NoError(t, err)
	cl, _, _ := newTestClient()

	_, err = s.publishToClient(cl, packets.Subscription{Filter: "a/b/c"}, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrNotAuthorized)
}

func TestPublishToClientNoConn(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Net.Conn = nil

	_, err := s.publishToClient(cl, packets.Subscription{Filter: "a/b/c"}, *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.CodeDisconnect)
}

func TestProcessPublishWithTopicAlias(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 0})
	require.True(t, subbed)

	cl2, _, w2 := newTestClient()
	cl2.Properties.ProtocolVersion = 5
	cl2.State.TopicAliases.Inbound.Set(1, "a/b/c")

	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishMqtt5).Packet
	pkx.Properties.SubscriptionIdentifier = []int{} // must not contain from client to server
	pkx.TopicName = ""
	pkx.Properties.TopicAlias = 1
	_ = s.processPacket(cl2, pkx)

	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r))
	requireNothingBefore(t, s, cl, r, "the aliased publish was written twice")
	_ = w2.Close()
	_ = w.Close()
}

func TestProcessPublishWithUnregisteredTopicAlias(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	go func() {
		// Closed however this goroutine ends. A failed require calls
		// Goexit, so a plain call at the bottom is skipped on failure and
		// the ReadAll below then blocks until the whole run times out --
		// a test that cannot report its own failure.
		defer func() { _ = w.Close() }()

		// A publish carrying an alias but no topic name, on a connection
		// that never registered that alias -- which is every reconnect by a
		// client that aliases, since [MQTT-3.3.2-7] drops the mappings with
		// the connection. Section 3.3.2.3.4 case 3a: a Protocol Error.
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishMqtt5).Packet
		pkx.Properties.SubscriptionIdentifier = []int{} // must not contain from client to server
		pkx.TopicName = ""
		pkx.Properties.TopicAliasFlag = true
		pkx.Properties.TopicAlias = 1

		err := s.processPacket(cl, pkx)
		require.Error(t, err)
		require.ErrorIs(t, err, packets.ErrProtocolViolationNoTopic)
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	require.Equal(t, byte(packets.Disconnect<<4), buf[0])
	require.Equal(t, packets.ErrProtocolViolationNoTopic.Code, buf[2])

	// The alias must not have been registered against the empty topic on
	// the way through, and nothing may have been published to it.
	require.NotContains(t, cl.State.TopicAliases.Inbound.internal, uint16(1))
	require.Equal(t, 0, len(s.Topics.Messages("")))
}

// lockedBuffer is a log destination the server may write from its own
// goroutines while a test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestReceivePacketLogsARefusalAtDebugWithoutThePacket(t *testing.T) {
	s := newServer()

	// This server's own logger, replacing the discarding one after New has
	// taken it, so the buffer below is provably the handler in use rather
	// than one something else has since swapped out. Debug level, because
	// the whole claim is that the refusal is reported there.
	//
	// **Locked**, because Serve starts the event loop, which writes its own
	// debug line to this logger on its own goroutine - and read at the same
	// moment, a plain bytes.Buffer was a data race: 3 in 300 runs under -race.
	buf := new(lockedBuffer)
	s.Log = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	_ = s.Serve()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	payload := []byte("SENTINEL-PAYLOAD-DO-NOT-LOG")

	// Waited on before the buffer is read. DisconnectClient stops the
	// client, so the ReadAll below can return while receivePacket has yet
	// to write its log line -- and a buffer read at that moment reports an
	// empty log about a server that logged.
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer func() { _ = w.Close() }()

		// An ordinary refusal carrying a payload: a wildcard in a topic
		// name, which [MQTT-3.3.2-2] forbids. The client is answered with a
		// DISCONNECT, which is the server working rather than failing.
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishMqtt5).Packet
		pkx.Properties.SubscriptionIdentifier = []int{}
		pkx.TopicName = "a/b/+"
		pkx.Payload = payload

		err := s.receivePacket(cl, pkx)
		require.Error(t, err)
	}()

	_, err := io.ReadAll(r)
	require.NoError(t, err)
	wg.Wait()

	out := buf.String()

	// The instrument first: if nothing was logged at all, everything below
	// passes for the wrong reason.
	require.Contains(t, out, "packet refused",
		"no debug line for the refusal reached this test's logger; if nothing was "+
			"logged at all, the assertions below pass for the wrong reason")

	require.NotContains(t, out, "level=WARN",
		"an ordinary refusal was reported to the operator as a problem")
	require.NotContains(t, out, string(payload),
		"the client's payload was written into the log")
	require.NotContains(t, out, "Payload:",
		"the whole packet was formatted into the log")
}

func TestPublishToSubscribersExhaustedSendQuota(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	cl.State.Inflight.sendQuota = 0

	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 2})
	require.True(t, subbed)

	// coverage: subscriber publish errors are non-returnable
	// can we hook into log/slog ?
	_ = r.Close()
	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	pkx.PacketID = 0
	s.publishToSubscribers(pkx)
	time.Sleep(time.Millisecond)
	_ = w.Close()
}

func TestPublishToSubscribersExhaustedPacketIDs(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	for i := uint32(0); i <= cl.ops.options.Capabilities.maximumPacketID; i++ {
		cl.State.Inflight.Set(packets.Packet{PacketID: 1})
	}

	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 2})
	require.True(t, subbed)

	// coverage: subscriber publish errors are non-returnable
	// can we hook into log/slog ?
	_ = r.Close()
	pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	pkx.PacketID = 0
	s.publishToSubscribers(pkx)
	time.Sleep(time.Millisecond)
	_ = w.Close()
}

func TestPublishToSubscribersNoConnection(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)
	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 2})
	require.True(t, subbed)

	// coverage: subscriber publish errors are non-returnable
	// can we hook into log/slog ?
	_ = r.Close()
	s.publishToSubscribers(*packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet)
	time.Sleep(time.Millisecond)
	_ = w.Close()
}

func TestPublishRetainedToClient(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	subbed := s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 2})
	require.True(t, subbed)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetainMqtt5).Packet)
	require.Equal(t, int64(1), retained)

	s.publishRetainedToClient(cl, packets.Subscription{Filter: "a/b/c"}, false)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).RawBytes, readPacket(t, r))
	requireNothingBefore(t, s, cl, r, "the retained message was written twice")
	_ = w.Close()
}

func TestPublishRetainedToClientIsShared(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	sub := packets.Subscription{Filter: SharePrefix + "/test/a/b/c"}
	subbed := s.Topics.Subscribe(cl.ID, sub)
	require.True(t, subbed)

	go func() {
		s.publishRetainedToClient(cl, sub, false)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte{}, buf)
}

func TestPublishRetainedToClientError(t *testing.T) {
	s := newServer()
	cl, _, w := newTestClient()
	s.Clients.Add(cl)

	sub := packets.Subscription{Filter: "a/b/c"}
	subbed := s.Topics.Subscribe(cl.ID, sub)
	require.True(t, subbed)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	_ = w.Close()
	s.publishRetainedToClient(cl, sub, false)
}

func TestNoRetainMessageIfUnavailable(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.RetainAvailable = 0
	cl, _, _ := newTestClient()
	s.Clients.Add(cl)

	s.retainMessage(new(Client), *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, 0, s.Topics.Retained.Len())
}

func TestNoRetainMessageIfPkIgnore(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	s.Clients.Add(cl)

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet
	pk.Ignore = true
	s.retainMessage(new(Client), pk)
	require.Equal(t, 0, s.Topics.Retained.Len())
}

func TestNoRetainMessage(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	s.Clients.Add(cl)

	s.retainMessage(new(Client), *packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, 1, s.Topics.Retained.Len())
}

func TestServerProcessPacketPuback(t *testing.T) {
	tt := ProtocolTest{
		{
			protocolVersion: 4,
			in:              packets.TPacketData[packets.Puback].Get(packets.TPuback),
		},
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Puback].Get(packets.TPubackMqtt5),
		},
	}

	for _, tx := range tt {
		t.Run(strconv.Itoa(int(tx.protocolVersion)), func(t *testing.T) {
			pID := uint16(7)
			s := newServer()
			cl, _, _ := newTestClient()
			cl.State.Inflight.sendQuota = 3
			cl.State.Inflight.receiveQuota = 3

			cl.State.Inflight.Set(packets.Packet{PacketID: pID})

			err := s.processPacket(cl, *tx.in.Packet)
			require.NoError(t, err)

			require.Equal(t, int32(4), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
			require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))

			_, ok := cl.State.Inflight.Get(pID)
			require.False(t, ok)
		})
	}
}

func TestServerProcessPacketPubackNoPacketID(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	pk := *packets.TPacketData[packets.Puback].Get(packets.TPuback).Packet
	err := s.processPacket(cl, pk)
	require.NoError(t, err)

	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

func TestServerProcessPacketPubrec(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, r, w := newTestClient()
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	cl.State.Inflight.Set(packets.Packet{PacketID: pID})

	recv := make(chan []byte)
	go func() { // receive the ack
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		recv <- buf
	}()

	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).Packet)
	require.NoError(t, err)
	_ = w.Close()

	require.Equal(t, packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).RawBytes, <-recv)

	require.Equal(t, int32(2), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	_, ok := cl.State.Inflight.Get(pID)
	require.True(t, ok)
}

// refuseReleases is a DeliveryKeeper whose store refuses to record a PUBREC.
type refuseReleases struct{ HookBase }

func (h *refuseReleases) ID() string           { return "refuse-releases" }
func (h *refuseReleases) Provides(b byte) bool { return b == OnDeliveryReleased }
func (h *refuseReleases) OnDeliveryReleased(cl *Client, pk packets.Packet) error {
	return errors.New("the store is refusing writes")
}
func (h *refuseReleases) OnDeliveryDone(cl *Client, pk packets.Packet) {}

// Invariant 18 and RFC 0003 "Broadcast": a PUBREL tells the client the
// exchange has moved on, so where a DeliveryKeeper cannot store its PUBREC
// none is sent. The entry stays the PUBLISH, to be sent again when the session
// resumes, and the connection ends: DISCONNECT 0x80 at MQTT 5, and nothing
// written below it, where the error closes the connection.
func TestServerProcessPacketPubrecTheKeeperCannotStore(t *testing.T) {
	for _, v := range []byte{5, 4} {
		t.Run(fmt.Sprintf("protocol %d", v), func(t *testing.T) {
			s := newServer()
			require.NoError(t, s.AddHook(new(refuseReleases), nil))
			cl, r, w := newTestClient()
			cl.Properties.ProtocolVersion = v
			cl.State.Inflight.sendQuota = 3
			cl.State.Inflight.receiveQuota = 3
			pub := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}, PacketID: 7,
				TopicName: "a/b", Payload: []byte("once"), Origin: "publisher"}
			cl.State.Inflight.Set(pub)

			recv := make(chan []byte)
			go func() {
				buf, _ := io.ReadAll(r)
				recv <- buf
			}()
			err := s.receivePacket(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrec},
				PacketID: 7, ProtocolVersion: v})
			require.ErrorIs(t, err, packets.ErrUnspecifiedError)
			_ = w.Close()
			buf := <-recv

			if v == 5 {
				// One packet, and it is the DISCONNECT.
				require.True(t, len(buf) > 2 && buf[0] == packets.Disconnect<<4 && int(buf[1]) == len(buf)-2,
					"want only a DISCONNECT, got % x", buf)
				require.Equal(t, packets.ErrUnspecifiedError.Code, buf[2])
			} else {
				require.Empty(t, buf, "want nothing written")
			}
			held, ok := cl.State.Inflight.Get(7)
			require.True(t, ok)
			require.Equal(t, packets.Publish, held.FixedHeader.Type, "the entry is no longer the PUBLISH")
			require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
		})
	}
}

func TestServerProcessPacketPubrecNoPacketID(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	recv := make(chan []byte)
	go func() { // receive the ack
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		recv <- buf
	}()

	pk := *packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).Packet // not sending properties
	err := s.processPacket(cl, pk)
	require.NoError(t, err)
	_ = w.Close()

	require.Equal(t, packets.TPacketData[packets.Pubrel].Get(packets.TPubrelMqtt5AckNoPacket).RawBytes, <-recv)

	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

func TestServerProcessPacketPubrecInvalidReason(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: pID})
	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrec].Get(packets.TPubrecInvalidReason).Packet)
	require.NoError(t, err)
	_, ok := cl.State.Inflight.Get(pID)
	require.False(t, ok)
}

func TestServerProcessPacketPubrecFailure(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: pID})
	cl.Stop(packets.CodeDisconnect)
	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).Packet)
	require.Error(t, err)
	require.ErrorIs(t, cl.StopCause(), packets.CodeDisconnect)
}

func TestServerProcessPacketPubrel(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, r, w := newTestClient()
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	cl.State.Inflight.Set(packets.Packet{PacketID: pID})

	recv := make(chan []byte)
	go func() { // receive the ack
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		recv <- buf
	}()

	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).Packet)
	require.NoError(t, err)
	_ = w.Close()

	require.Equal(t, int32(4), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
	require.Equal(t, int32(4), atomic.LoadInt32(&cl.State.Inflight.sendQuota))

	require.Equal(t, packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp).RawBytes, <-recv)

	_, ok := cl.State.Inflight.Get(pID)
	require.False(t, ok)
}

func TestServerProcessPacketPubrelNoPacketID(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	recv := make(chan []byte)
	go func() { // receive the ack
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		recv <- buf
	}()

	pk := *packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).Packet // not sending properties
	err := s.processPacket(cl, pk)
	require.NoError(t, err)
	_ = w.Close()

	require.Equal(t, packets.TPacketData[packets.Pubcomp].Get(packets.TPubcompMqtt5AckNoPacket).RawBytes, <-recv)

	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
	require.Equal(t, int32(3), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
}

func TestServerProcessPacketPubrelFailure(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: pID})
	cl.Stop(packets.CodeDisconnect)
	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrel].Get(packets.TPubrel).Packet)
	require.Error(t, err)
	require.ErrorIs(t, cl.StopCause(), packets.CodeDisconnect)
}

func TestServerProcessPacketPubrelBadReason(t *testing.T) {
	pID := uint16(7)
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.Inflight.Set(packets.Packet{PacketID: pID})
	err := s.processPacket(cl, *packets.TPacketData[packets.Pubrel].Get(packets.TPubrelInvalidReason).Packet)
	require.NoError(t, err)
	_, ok := cl.State.Inflight.Get(pID)
	require.False(t, ok)
}

func TestServerProcessPacketPubcomp(t *testing.T) {
	tt := ProtocolTest{
		{
			protocolVersion: 4,
			in:              packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp),
		},
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Pubcomp].Get(packets.TPubcompMqtt5),
		},
	}

	for _, tx := range tt {
		t.Run(strconv.Itoa(int(tx.protocolVersion)), func(t *testing.T) {
			pID := uint16(7)
			s := newServer()
			cl, _, _ := newTestClient()
			cl.Properties.ProtocolVersion = tx.protocolVersion
			cl.State.Inflight.sendQuota = 3
			cl.State.Inflight.receiveQuota = 3

			cl.State.Inflight.Set(packets.Packet{PacketID: pID})

			err := s.processPacket(cl, *tx.in.Packet)
			require.NoError(t, err)

			require.Equal(t, int32(4), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
			require.Equal(t, int32(4), atomic.LoadInt32(&cl.State.Inflight.sendQuota))

			_, ok := cl.State.Inflight.Get(pID)
			require.False(t, ok)
		})
	}
}

func TestServerProcessInboundQos2Flow(t *testing.T) {
	tt := ProtocolTest{
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Publish].Get(packets.TPublishQos2),
			out:             packets.TPacketData[packets.Pubrec].Get(packets.TPubrec),
			data: map[string]any{
				"sendquota": int32(3),
				"recvquota": int32(2),
				"inflight":  int64(1),
			},
		},
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Pubrel].Get(packets.TPubrel),
			out:             packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp),
			data: map[string]any{
				"sendquota": int32(4),
				"recvquota": int32(3),
				"inflight":  int64(0),
			},
		},
	}

	pID := uint16(7)
	s := newServer()
	cl, r, w := newTestClient()
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3

	for i, tx := range tt {
		t.Run("qos step"+strconv.Itoa(i), func(t *testing.T) {
			r, w = net.Pipe()
			cl.Net.Conn = w

			recv := make(chan []byte)
			go func() { // receive the ack
				buf, err := io.ReadAll(r)
				require.NoError(t, err)
				recv <- buf
			}()

			err := s.processPacket(cl, *tx.in.Packet)
			require.NoError(t, err)
			_ = w.Close()

			require.Equal(t, tx.out.RawBytes, <-recv)
			if i == 0 {
				_, ok := cl.State.Inflight.Get(pID)
				require.True(t, ok)
			}

			require.Equal(t, tx.data["inflight"].(int64), int64(cl.State.Inflight.Len()))
			require.Equal(t, tx.data["recvquota"].(int32), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
			require.Equal(t, tx.data["sendquota"].(int32), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
		})
	}

	_, ok := cl.State.Inflight.Get(pID)
	require.False(t, ok)
}

func TestServerProcessOutboundQos2Flow(t *testing.T) {
	tt := ProtocolTest{
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Publish].Get(packets.TPublishQos2),
			out:             packets.TPacketData[packets.Publish].Get(packets.TPublishQos2),
			data: map[string]any{
				"sendquota": int32(2),
				"recvquota": int32(3),
				"inflight":  int64(1),
			},
		},
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Pubrec].Get(packets.TPubrec),
			out:             packets.TPacketData[packets.Pubrel].Get(packets.TPubrel),
			data: map[string]any{
				"sendquota": int32(2),
				"recvquota": int32(2),
				"inflight":  int64(1),
			},
		},
		{
			protocolVersion: 5,
			in:              packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp),
			data: map[string]any{
				"sendquota": int32(3),
				"recvquota": int32(3),
				"inflight":  int64(0),
			},
		},
	}

	pID := uint16(6)
	s := newServer()
	cl, _, _ := newTestClient()
	cl.State.packetID = uint32(6)
	cl.State.Inflight.sendQuota = 3
	cl.State.Inflight.receiveQuota = 3
	s.Clients.Add(cl)
	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c", Qos: 2})

	for i, tx := range tt {
		t.Run("qos step"+strconv.Itoa(i), func(t *testing.T) {
			r, w := net.Pipe()
			cl.Net.Conn = w

			// Reading exactly what this step expects is what waits for
			// the write loop, and the end marker behind it says nothing
			// else was written - step 2's whole assertion, where closing
			// the pipe once the call returned read only what had arrived
			// by then.
			done := make(chan error, 1)
			go func() {
				if i == 0 {
					s.publishToSubscribers(*tx.in.Packet)
					done <- nil
					return
				}
				done <- s.processPacket(cl, *tx.in.Packet)
			}()
			if i != 2 {
				require.Equal(t, tx.out.RawBytes, readPacket(t, r))
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the step did not return once what it wrote was read")
			}
			requireNothingBefore(t, s, cl, r, "step "+strconv.Itoa(i)+" wrote more than it expects")
			_ = w.Close()

			require.Equal(t, tx.data["inflight"].(int64), int64(cl.State.Inflight.Len()))
			require.Equal(t, tx.data["recvquota"].(int32), atomic.LoadInt32(&cl.State.Inflight.receiveQuota))
			require.Equal(t, tx.data["sendquota"].(int32), atomic.LoadInt32(&cl.State.Inflight.sendQuota))
		})
	}

	_, ok := cl.State.Inflight.Get(pID)
	require.False(t, ok)
}

func TestServerProcessPacketSubscribe(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeMqtt5).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackMqtt5).RawBytes, buf)
}

func TestServerProcessPacketSubscribeInvalid(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Properties.ProtocolVersion = 5

	err := s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeSpecQosMustPacketID).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationNoPacketID)
}

func TestServerProcessPacketSubscribeInvalidFilter(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeInvalidFilter).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackInvalidFilter).RawBytes, buf)
}

func TestServerProcessPacketSubscribeInvalidSharedNoLocal(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeInvalidSharedNoLocal).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackInvalidSharedNoLocal).RawBytes, buf)
}

// A Pubrec entry is the client's OWN QoS 2 publish still in its
// PUBREL exchange, so that identifier really is in use by the client
// and 0x91 is correct. A Publish entry is the server's outbound
// message, whose identifiers are a separate space - see
// TestServerProcessSubscribeWithServerInflightPacketID.
func TestServerProcessPacketSubscribePacketIDInUse(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.Set(packets.Packet{PacketID: 15, FixedHeader: packets.FixedHeader{Type: packets.Pubrec}})

	pkx := *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeMqtt5).Packet
	pkx.PacketID = 15
	go func() {
		err := s.processPacket(cl, pkx)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackPacketIDInUse).RawBytes, buf)
}

// A Pubrec entry is the client's OWN QoS 2 publish still in its
// PUBREL exchange, so that identifier really is in use by the client
// and 0x91 is correct. A Publish entry is the server's outbound
// message, whose identifiers are a separate space - see
// TestServerProcessSubscribeWithServerInflightPacketID.
func TestServerProcessPacketUnsubscribePackedIDInUse(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	cl.State.Inflight.Set(packets.Packet{PacketID: 15, FixedHeader: packets.FixedHeader{Type: packets.Pubrec}})
	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribeMqtt5).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Unsuback].Get(packets.TUnsubackPacketIDInUse).RawBytes, buf)
	require.Equal(t, int64(0), s.Info.Subscriptions.Load())
}

// Packet Identifiers assigned by the Client and by the Server are
// independent: MQTT 3.1.1 2.3.1 and MQTT 5.0 2.2.1 both say so, and both
// give the same example - a Client can send a packet with identifier
// 0x1234 while receiving a different one carrying 0x1234 from its Server.
//
// The server's own outbound QoS 1 and 2 messages live in cl.State.Inflight,
// so checking a client-assigned SUBSCRIBE identifier against that store
// refuses a packet the specification permits. It happens whenever a
// persistent session has a queued message and the client subscribes on
// connect, which is what every client library's on-connect callback does.
func TestServerProcessSubscribeWithServerInflightPacketID(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	// A message the server sent and is waiting to have acknowledged, using
	// the same identifier the client is about to choose for its SUBSCRIBE.
	cl.State.Inflight.Set(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    15,
		TopicName:   "x/y/z",
	})

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSuback).RawBytes, ack)
	requireNothingBefore(t, s, cl, r, "more than the acknowledgement was written")
	_ = w.Close()
}

// The same for UNSUBSCRIBE, which carries the identical check.
func TestServerProcessUnsubscribeWithServerInflightPacketID(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c"})
	cl.State.Subscriptions.Add("a/b/c", packets.Subscription{Qos: 0})

	cl.State.Inflight.Set(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1},
		PacketID:    15,
		TopicName:   "x/y/z",
	})

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribe).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Unsuback].Get(packets.TUnsuback).RawBytes, ack)
	requireNothingBefore(t, s, cl, r, "more than the acknowledgement was written")
	_ = w.Close()
}

func TestServerProcessSubscribeWithRetain(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSuback).RawBytes, ack)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).RawBytes, readPacket(t, r))
	requireNothingBefore(t, s, cl, r, "more than the SUBACK and the retained message was written")
	_ = w.Close()
}

func TestServerProcessSubscribeDowngradeQos(t *testing.T) {
	s := newServer()
	s.Options.Capabilities.MaximumQos = 1
	cl, r, w := newTestClient()

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeMany).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, []byte{0, 1, 1}, ack[4:])
	requireNothingBefore(t, s, cl, r, "more than the SUBACK was written")
	_ = w.Close()
}

func TestServerProcessSubscribeWithRetainHandling1(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b/c"})
	s.Clients.Add(cl)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeRetainHandling1).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSuback).RawBytes, ack)
	requireNothingBefore(t, s, cl, r, "more than the acknowledgement was written")
	_ = w.Close()
}

func TestServerProcessSubscribeWithRetainHandling2(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeRetainHandling2).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSuback).RawBytes, ack)
	requireNothingBefore(t, s, cl, r, "more than the acknowledgement was written")
	_ = w.Close()
}

func TestServerProcessSubscribeWithNotRetainAsPublished(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	s.Clients.Add(cl)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	done := make(chan error, 1)
	go func() {
		done <- s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeRetainAsPublished).Packet)
	}()
	ack := readPacket(t, r)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("processPacket did not return once its acknowledgement was read")
	}
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSuback).RawBytes, ack)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).RawBytes, readPacket(t, r))
	requireNothingBefore(t, s, cl, r, "more than the SUBACK and the retained message was written")
	_ = w.Close()
}

func TestServerProcessSubscribeNoConnection(t *testing.T) {
	s := newServer()
	cl, r, _ := newTestClient()
	_ = r.Close()
	err := s.processSubscribe(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestServerProcessSubscribeACLCheckDeny(t *testing.T) {
	s := New(&Options{
		Logger: logger,
	})
	_ = s.Serve()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5

	go func() {
		err := s.processSubscribe(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribe).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackDeny).RawBytes, buf)
}

func TestServerProcessSubscribeErrorDowngrade(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 3
	cl.State.packetID = 1 // just to match the same packet id (7) in the fixtures

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeInvalidSharedNoLocal).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Suback].Get(packets.TSubackUnspecifiedError).RawBytes, buf)
}

func TestServerProcessPacketUnsubscribe(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b", Qos: 0})
	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribeMqtt5).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Unsuback].Get(packets.TUnsubackMqtt5).RawBytes, buf)
	require.Equal(t, int64(-1), s.Info.Subscriptions.Load())
}

func TestServerProcessPacketUnsubscribeInvalid(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	err := s.processPacket(cl, *packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribeSpecQosMustPacketID).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationNoPacketID)
}

func TestServerReceivePacketError(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	err := s.receivePacket(cl, *packets.TPacketData[packets.Unsubscribe].Get(packets.TUnsubscribeSpecQosMustPacketID).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationNoPacketID)
}

func TestServerRecievePacketDisconnectClientZeroNonZero(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.Props.SessionExpiryInterval = 0
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.RequestProblemInfo = 0
	cl.Properties.Props.RequestProblemInfoFlag = true
	go func() {
		err := s.receivePacket(cl, *packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectMqtt5).Packet)
		require.Error(t, err)
		require.ErrorIs(t, err, packets.ErrProtocolViolationZeroNonZeroExpiry)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectZeroNonZeroExpiry).RawBytes, buf)
}

func TestServerRecievePacketDisconnectClient(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5

	go func() {
		err := s.DisconnectClient(cl, packets.CodeDisconnect)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte{
		packets.Disconnect << 4, 2, // fixed header
		packets.CodeDisconnect.Code, // Reason Code
		0,                           // Properties Length
	}, buf)
}

func TestServerRecievePacketDisconnectClientV3(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 4 // MQTT v3.1.1

	go func() {
		err := s.DisconnectClient(cl, packets.CodeDisconnect)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	// The server-to-client Disconnect packet was introduced in MQTT v5. A v3
	// client is closed without one rather than being sent a packet type its
	// own specification only defines in the other direction.
	require.Empty(t, buf)
}

func TestServerProcessPacketDisconnect(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Properties.Props.SessionExpiryInterval = 30
	cl.Properties.ProtocolVersion = 5

	err := s.processPacket(cl, *packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectMqtt5).Packet)
	require.NoError(t, err)

	require.True(t, cl.Closed())
	require.WithinDuration(t, time.Now(), cl.StopTime(), time.Second,
		"the disconnect moment is stamped to the nanosecond, and a lifetime is judged against it")
}

func TestServerProcessPacketDisconnectNonZeroExpiryViolation(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Properties.Props.SessionExpiryInterval = 0
	cl.Properties.ProtocolVersion = 5
	cl.Properties.Props.RequestProblemInfo = 0
	cl.Properties.Props.RequestProblemInfoFlag = true

	err := s.processPacket(cl, *packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectMqtt5).Packet)
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrProtocolViolationZeroNonZeroExpiry)
}

func TestServerProcessPacketDisconnectDisconnectWithWillMessage(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	cl.Properties.Props.SessionExpiryInterval = 30
	cl.Properties.ProtocolVersion = 5

	err := s.processPacket(cl, *packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectMqtt5DisconnectWithWillMessage).Packet)
	require.Error(t, err)

	// The DISCONNECT owns the connection's end from before what it changes is
	// stored until the close, which it makes (Client.claimEnd).
	require.True(t, cl.Closed())
}

func TestServerProcessPacketAuth(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()

	go func() {
		err := s.processPacket(cl, *packets.TPacketData[packets.Auth].Get(packets.TAuth).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte{}, buf)
}

func TestServerProcessPacketAuthInvalidReason(t *testing.T) {
	s := newServer()
	cl, _, _ := newTestClient()
	pkx := *packets.TPacketData[packets.Auth].Get(packets.TAuth).Packet
	pkx.ReasonCode = 99
	err := s.processPacket(cl, pkx)
	require.Error(t, err)
	require.ErrorIs(t, packets.ErrProtocolViolationInvalidReason, err)
}

func TestServerSendLWT(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	sender, _, w1 := newTestClient()
	sender.ID = "sender"
	sender.Properties.Will = Will{
		Flag:      1,
		TopicName: "a/b/c",
		Payload:   []byte("hello mochi"),
	}
	s.Clients.Add(sender)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c", Qos: 0})

	require.Equal(t, 0, len(s.Topics.Messages("a/b/c")))

	s.sendLWT(sender)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r2))
	requireNothingBefore(t, s, receiver, r2, "the Will was written twice")
	_ = w1.Close()
	_ = w2.Close()
}

func TestServerSendLWTRetain(t *testing.T) {
	s := newServer()
	_ = s.Serve()
	defer s.Close()

	sender, _, w1 := newTestClient()
	sender.ID = "sender"
	sender.Properties.Will = Will{
		Flag:      1,
		TopicName: "a/b/c",
		Payload:   []byte("hello mochi"),
		Retain:    true,
	}
	s.Clients.Add(sender)

	receiver, r2, w2 := newTestClient()
	receiver.ID = "receiver"
	s.Clients.Add(receiver)
	s.Topics.Subscribe(receiver.ID, packets.Subscription{Filter: "a/b/c", Qos: 0})

	require.Equal(t, 0, len(s.Topics.Messages("a/b/c")))

	s.sendLWT(sender)
	require.Equal(t, packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).RawBytes, readPacket(t, r2))
	requireNothingBefore(t, s, receiver, r2, "the Will was written twice")
	_ = w1.Close()
	_ = w2.Close()
}

// takesWills takes every Will itself, as saguin's hook does: it hands the Will
// back with nothing the engine could publish, and leaves its delay alone.
type takesWills struct{ HookBase }

func (h *takesWills) ID() string           { return "takes-wills" }
func (h *takesWills) Provides(b byte) bool { return b == OnWill }
func (h *takesWills) OnWill(cl *Client, will Will) (Will, error) {
	will.TopicName, will.Payload, will.Retain, will.User = "", nil, false, nil
	return will, nil
}

// A Will its hook has taken leaves nothing armed in the engine: nothing
// published, and the connection's own flag cleared. And **the engine holds no
// Will for later**: a delayed one no hook took is neither published early nor
// kept, and the drop is logged.
//
// The delay was read from the connection's Will rather than from what the
// hook returned, so an emptied Will with a delay was queued on the engine's
// delayed path anyway. After a takeover the old connection's entry could
// outlive the new connection's CONNACK, and when it fell due it cleared the
// new connection's Will - which then never fired: 14 of 40 runs on the wire.
// Nothing in saguin reached that path, and it was removed.
func TestAWillItsHookTookLeavesNothingArmed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		delay uint32
		taken bool
	}{
		{"delayed, no hook", 2, false},
		{"delayed, taken", 2, true},
		{"immediate, no hook (control)", 0, false},
		{"immediate, taken", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			logged := new(bytes.Buffer)
			s.Log = slog.New(slog.NewTextHandler(logged, nil))
			if tc.taken {
				require.NoError(t, s.AddHook(new(takesWills), nil))
			}
			cl, _, _ := newTestClient()
			cl.ID = "dev"
			cl.Properties.Will = Will{Flag: 1, TopicName: "a/b/c", Payload: []byte("gone"), Qos: 1,
				WillDelayInterval: tc.delay}
			s.Clients.Add(cl)
			watcher, peer, _ := newTestClient()
			watcher.ID = "watcher"
			go func() { _, _ = io.Copy(io.Discard, peer) }()
			s.Clients.Add(watcher)
			require.True(t, s.Topics.Subscribe(watcher.ID, packets.Subscription{Filter: "#", Qos: 1}))

			s.sendLWT(cl)

			published := watcher.State.Inflight.Len()
			dropped := strings.Contains(logged.String(), "a Will with a delay reached the engine")
			if !tc.taken && tc.delay == 0 {
				require.Equal(t, 1, published, "an immediate Will no hook took was not published, "+
					"so a count of none below proves nothing")
				return
			}
			require.Equal(t, 0, published, "the engine published a Will with a delay or one its hook took")
			require.Equal(t, uint32(0), atomic.LoadUint32(&cl.Properties.Will.Flag),
				"the Will is still armed on its connection")
			require.Equal(t, !tc.taken, dropped, "a delayed Will no hook took is dropped and said so, "+
				"and nothing else is: the log read %q", logged.String())
		})
	}
}

func TestServerClose(t *testing.T) {
	s := newServer()

	hook := new(modifiedHookBase)
	_ = s.AddHook(hook, nil)

	cl, r, _ := newTestClient()
	cl.Net.Listener = "t1"
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	err := s.AddListener(listeners.NewMockListener("t1", ":1882"))
	require.NoError(t, err)
	_ = s.Serve()

	// receive the disconnect
	recv := make(chan []byte)
	go func() {
		buf, err := io.ReadAll(r)
		require.NoError(t, err)
		recv <- buf
	}()

	require.Equal(t, 1, s.Listeners.Len())

	listener, ok := s.Listeners.Get("t1")
	require.Equal(t, true, ok)
	// Serve runs each listener on a goroutine of its own, so serving is
	// waited for rather than slept for.
	require.Eventually(t, listener.(*listeners.MockListener).IsServing, 5*time.Second, time.Millisecond,
		"the listener never began serving")

	// Close closes each listener before it returns.
	_ = s.Close()
	require.Equal(t, false, listener.(*listeners.MockListener).IsServing())
	require.Equal(t, packets.TPacketData[packets.Disconnect].Get(packets.TDisconnectShuttingDown).RawBytes, <-recv)
}

// blockingConnectHook holds attachClient inside OnConnect until it is
// released, so that Close is called while a client is demonstrably still
// being attached rather than at a moment the test had to guess.
type blockingConnectHook struct {
	HookBase
	entered chan struct{}
	release chan struct{}
}

func (h *blockingConnectHook) ID() string { return "blocking-connect" }

func (h *blockingConnectHook) Provides(b byte) bool { return b == OnConnect }

func (h *blockingConnectHook) OnConnect(cl *Client, pk packets.Packet) error {
	close(h.entered)
	<-h.release
	return nil
}

// Close does not return while a client is still being attached, including a
// client handed to EstablishConnection directly rather than accepted by a
// listener. Registering only inside the listener's establisher covered the
// second and not the first, so Close returned early for an embedder feeding
// the server its own connections and for every test in this file.
func TestServerCloseWaitsForDirectEstablishConnection(t *testing.T) {
	h := &blockingConnectHook{entered: make(chan struct{}), release: make(chan struct{})}

	s := newServer()
	require.NoError(t, s.AddHook(h, nil))

	r, w := net.Pipe()
	attached := make(chan error, 1)
	go func() {
		attached <- s.EstablishConnection("tcp", r)
	}()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()

	<-h.entered // the client is inside attachClient and cannot leave

	closed := make(chan struct{})
	go func() {
		_ = s.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned while a client was still being attached")
	case <-time.After(250 * time.Millisecond):
	}

	// Hang the connection up from this side before waiting, because Close
	// now genuinely waits for it: the client is attached under a listener
	// id no listener owns, so closeListenerClients never reaches it and it
	// would otherwise sit until its keepalive expired.
	close(h.release)
	_ = w.Close()
	<-closed
	<-attached
}

// A connection arriving after shutdown has latched is refused rather than
// attached, whichever way it arrives.
func TestServerEstablishConnectionRefusedAfterClose(t *testing.T) {
	s := newServer()
	require.NoError(t, s.Close())

	r, w := net.Pipe()
	defer w.Close()

	// Nothing is written to the pipe: a refused connection is closed
	// without its CONNECT being read, so this returns without waiting for
	// one. Unlatched it blocks in readConnectionPacket instead, which is
	// what the deadline below catches.
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("a connection arriving after shutdown was attached rather than refused")
	}

	require.Equal(t, 0, s.Clients.Len())
}

// connectKeepalive60 is an MQTT 5 CONNECT, clean start, keepalive 60s,
// client id "a" - spelled out so the keepalive the test relies on is visible.
var connectKeepalive60 = []byte{0x10, 0x0e, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02,
	0x00, 0x3c, 0x00, 0x00, 0x01, 'a'}

// preConnectPeer is one connection to a real listener, as the test sees it.
type preConnectPeer struct {
	send func([]byte) error
	// read returns the next bytes from the broker, or an error when the
	// broker closed the connection or the wait ran out.
	read  func(wait time.Duration) ([]byte, error)
	close func()
}

// startPreConnectListeners serves one server on a real tcp, ws and unix
// listener, and returns a dialler for each.
func startPreConnectListeners(t *testing.T, connectTimeout time.Duration) (*Server, map[string]func() preConnectPeer) {
	t.Helper()
	s := newServer()
	s.Options.ClientConnectTimeout = connectTimeout
	tcp := listeners.NewTCP(listeners.Config{ID: "tcp", Address: "127.0.0.1:0"})
	ws := listeners.NewWebsocket(listeners.Config{ID: "ws", Address: "127.0.0.1:0"})
	unixAddr := fmt.Sprintf("@saguin-preconnect-%d", time.Now().UnixNano())
	unix := listeners.NewUnixSock(listeners.Config{ID: "unix", Address: unixAddr})
	for _, l := range []listeners.Listener{tcp, ws, unix} {
		require.NoError(t, s.AddListener(l))
	}
	require.NoError(t, s.Serve())

	raw := func(network, addr string) func() preConnectPeer {
		return func() preConnectPeer {
			c, err := net.Dial(network, addr)
			require.NoError(t, err)
			return preConnectPeer{
				send: func(b []byte) error { _, err := c.Write(b); return err },
				read: func(wait time.Duration) ([]byte, error) {
					_ = c.SetReadDeadline(time.Now().Add(wait))
					buf := make([]byte, 64)
					n, err := c.Read(buf)
					return buf[:n], err
				},
				close: func() { _ = c.Close() },
			}
		}
	}
	return s, map[string]func() preConnectPeer{
		"tcp":  raw("tcp", tcp.Address()),
		"unix": raw("unix", unixAddr),
		"ws": func() preConnectPeer {
			d := websocket.Dialer{Subprotocols: []string{"mqtt"}}
			c, _, err := d.Dial("ws://"+ws.Address()+"/", nil)
			require.NoError(t, err)
			return preConnectPeer{
				send: func(b []byte) error { return c.WriteMessage(websocket.BinaryMessage, b) },
				read: func(wait time.Duration) ([]byte, error) {
					_ = c.SetReadDeadline(time.Now().Add(wait))
					_, b, err := c.ReadMessage()
					return b, err
				},
				close: func() { _ = c.Close() },
			}
		},
	}
}

// RFC 0002 "How long a socket may wait to send CONNECT: limits.connect_timeout"
//
// On every listener a connection that sends nothing is closed once the
// bound passes, and one that sends its CONNECT in time is admitted and is
// still open well past the bound - so the deadline is proven both armed and
// cleared, and the reader is proven able to tell open from closed.
func TestASocketThatNeverSendsCONNECTIsClosed(t *testing.T) {
	const bound = 300 * time.Millisecond
	s, dial := startPreConnectListeners(t, bound)
	defer s.Close()

	for _, kind := range []string{"tcp", "ws", "unix"} {
		t.Run(kind, func(t *testing.T) {
			silent := dial[kind]()
			defer silent.close()
			admitted := dial[kind]()
			defer admitted.close()

			require.NoError(t, admitted.send(connectKeepalive60))
			connack, err := admitted.read(2 * time.Second)
			require.NoError(t, err)
			require.NotEmpty(t, connack)
			require.Equal(t, byte(0x20), connack[0], "broker sent % x, not a CONNACK", connack)

			start := time.Now()
			b, err := silent.read(5 * time.Second)
			took := time.Since(start)
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatalf("a socket that sent nothing was still open after 5s against a %v bound", bound)
			}
			require.Error(t, err, "the broker sent % x to a socket that sent nothing", b)
			require.GreaterOrEqual(t, took, bound/2, "closed before the bound: %v", took)

			_, err = admitted.read(3 * bound)
			require.True(t, errors.As(err, &ne) && ne.Timeout(),
				"a client that sent CONNECT in time was closed after the bound: %v", err)
		})
	}
}

// RFC 0002 "A stopping broker does not wait for it."
//
// With no connect bound at all, Close must still return while a socket on
// every listener has sent nothing - and that socket must see the close.
func TestCloseDoesNotWaitForASocketThatNeverSendsCONNECT(t *testing.T) {
	s, dial := startPreConnectListeners(t, 0)
	peers := map[string]preConnectPeer{}
	for _, kind := range []string{"tcp", "ws", "unix"} {
		peers[kind] = dial[kind]()
		defer peers[kind].close()
	}
	// Each socket holds a slot from its arrival, so three slots taken is
	// each listener having accepted its one.
	require.Eventually(t, func() bool { return s.slots.Load() == 3 }, 5*time.Second, time.Millisecond,
		"%d of 3 silent sockets were accepted", s.slots.Load())

	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close is still waiting on sockets that never sent CONNECT")
	}
	for kind, p := range peers {
		_, err := p.read(2 * time.Second)
		var ne net.Error
		require.False(t, errors.As(err, &ne) && ne.Timeout(),
			"%s: the silent socket was not closed by shutdown", kind)
	}
}

// takeoverConnect is an MQTT 5 CONNECT with clean start, keepalive 60s and
// the given client id.
func takeoverConnect(id string) []byte {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00}
	body = append(body, byte(len(id)>>8), byte(len(id)))
	body = append(body, id...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// takeoverConnack reads one CONNACK and returns its reason code, or 0xFF
// when what arrived was not one.
func takeoverConnack(c net.Conn) byte {
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	h := make([]byte, 2)
	if _, err := io.ReadFull(c, h); err != nil || h[0] != 0x20 {
		return 0xFF
	}
	body := make([]byte, h[1])
	if _, err := io.ReadFull(c, body); err != nil || len(body) < 2 {
		return 0xFF
	}
	return body[1]
}

// [MQTT-3.1.4-3] "If the ClientID represents a Client already connected to
// the Server, the Server sends a DISCONNECT packet to the existing Client
// with Reason Code of 0x8E (Session taken over) ... and MUST close the
// Network Connection of the existing Client."
//
// **Two CONNECTs under one client id, arriving together, leave one
// connection.** Establishing a session looked the id up, took over what it
// found and then added itself, each step under the registry's lock and the
// sequence under none - so two arriving together could both find nothing,
// both be added, and both stay connected as the same client. Measured before
// the fix: both left open in 70 of 300 pairs.
//
// A pair is judged after both CONNACKs are read. A takeover writes its
// DISCONNECT before the CONNACK of the connection that took over, so by then
// the superseded one has something to read and the survivor has nothing.
func TestTwoConnectsUnderOneClientIDLeaveOneConnection(t *testing.T) {
	s := newServer()
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	defer s.Close()

	const trials = 100
	both, early := 0, 0
	for trial := 0; trial < trials; trial++ {
		id := fmt.Sprintf("device-%d", trial)
		conns := make([]net.Conn, 2)
		for i := range conns {
			c, err := net.Dial("tcp", tcp.Address())
			require.NoError(t, err)
			conns[i] = c
		}
		codes := make([]byte, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i, c := range conns {
			wg.Add(1)
			go func(i int, c net.Conn) {
				defer wg.Done()
				<-start
				_, _ = c.Write(takeoverConnect(id))
				codes[i] = takeoverConnack(c)
			}(i, c)
		}
		close(start)
		wg.Wait()
		// [MQTT-3.2.0-1] the first packet a server sends is the CONNACK. A
		// connection taken over before its own CONNACK was written read the
		// DISCONNECT first, which is the same race seen from the other side.
		if codes[0] != 0x00 || codes[1] != 0x00 {
			early++
			for _, c := range conns {
				_ = c.Close()
			}
			continue
		}

		open := 0
		for _, c := range conns {
			_ = c.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
			_, err := c.Read(make([]byte, 1))
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				open++
			}
			_ = c.Close()
		}
		if open != 1 {
			both++
		}
	}
	require.Zero(t, both+early, "of %d pairs of CONNECTs under one client id, %d left two connections "+
		"and %d sent a DISCONNECT before a CONNACK", trials, both, early)
}

// **The per-id locks hold nothing for an id once its connections are
// established** (invariant 13): a table keyed by every client id ever seen
// would be unbounded state chosen by clients. Waiters on one id share one
// entry, and it goes when the last of them releases it.
func TestSessionLocksForgetAnIDNobodyHolds(t *testing.T) {
	var l sessionLocks
	for i := 0; i < 1000; i++ {
		l.lock(fmt.Sprintf("device-%d", i))()
	}
	require.Empty(t, l.held, "ids nobody holds are still in the table")

	release := l.lock("shared")
	acquired := make(chan func())
	go func() { acquired <- l.lock("shared") }()
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.held["shared"] != nil && l.held["shared"].waiting == 2
	}, time.Second, time.Millisecond, "a second caller on the same id is not waiting on the same entry")
	select {
	case <-acquired:
		t.Fatal("a second caller took the lock for an id another already held")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	(<-acquired)()
	require.Empty(t, l.held, "the id is still in the table after both callers released it")
}

// **What a holder queues for after the lock runs once it is let go, on the
// goroutine letting it go, and before the next holder has it** (saguin:
// AfterSessionUnlock). A publish queued there must run with the lock
// released, since a publish can wait on another id's lock - and it must
// run on every release, since the hook that queued it cannot know which
// path its caller will leave by. With nothing holding the lock, now is after
// it.
func TestWhatASessionLockHolderQueuesRunsOnceItIsLetGo(t *testing.T) {
	var l sessionLocks
	release := l.lock("dev")
	var ran []string
	l.afterUnlock("dev", func() {
		require.Equal(t, 0, l.waiters("dev"), "a queued function ran while the lock was still held")
		ran = append(ran, "first")
	})
	l.afterUnlock("dev", func() { ran = append(ran, "second") })
	require.Empty(t, ran, "a queued function ran before the lock was let go")
	release()
	require.Equal(t, []string{"first", "second"}, ran, "the release did not run what was queued, in order")

	// Queued by one holder, it is not run by the next.
	release = l.lock("dev")
	release()
	require.Len(t, ran, 2, "a later release ran the earlier holder's queue again")

	l.afterUnlock("nobody", func() { ran = append(ran, "at once") })
	require.Equal(t, "at once", ran[len(ran)-1], "with nothing holding the lock, a queued function did not run at once")
	require.Empty(t, l.held, "queueing under an id nobody held left it in the table")
}

// replaceOnExpiry adds a replacement client under the expiring client's id
// from inside OnClientExpired, which runs between the sweep's check and its
// delete: a client reconnecting under that id at exactly that moment.
type replaceOnExpiry struct {
	HookBase
	s           *Server
	replacement *Client
}

func (h *replaceOnExpiry) ID() string { return "replace-on-expiry" }

func (h *replaceOnExpiry) Provides(b byte) bool { return b == OnClientExpired }

func (h *replaceOnExpiry) OnClientExpired(cl *Client) {
	if cl.ID == h.replacement.ID {
		h.s.Clients.Add(h.replacement)
	}
}

// [MQTT-4.1.0-2] the session is discarded when its expiry has passed - the
// session that expired, and not a newer connection that took its id.
//
// **The sweep deletes the client it judged expired, not whatever holds the
// id by the time it deletes.** It took a snapshot, asked each client whether
// it had expired, and deleted by id - so a device reconnecting between the
// question and the delete was removed from the registry while connected.
func TestTheExpirySweepDoesNotDeleteAClientThatReplacedTheExpiredOne(t *testing.T) {
	s := New(nil)
	n := time.Now()

	expired, _, _ := newTestClient()
	expired.ID = "device"
	expired.State.disconnected.Store(n.Add(-10 * time.Second).UnixNano())
	expired.State.cancelOpen()
	expired.Properties.ProtocolVersion = 5
	expired.Properties.Props.SessionExpiryInterval = 8
	expired.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(expired)

	replacement, _, _ := newTestClient()
	replacement.ID = "device"
	require.NoError(t, s.AddHook(&replaceOnExpiry{s: s, replacement: replacement}, nil))

	s.clearExpiredClients(n)

	got, ok := s.Clients.Get("device")
	require.True(t, ok, "the sweep deleted the client that had replaced the expired one")
	require.Same(t, replacement, got)
}

// **No production code deletes a client by id alone.** Every site that
// removes a client from the registry has examined one particular client -
// the one it judged expired, disconnected or superseded - and a delete keyed
// only by id removes whatever holds the id by then, which may be a
// connection that arrived since. DeleteIf removes that client or nothing.
//
// Walks the syntax tree of every non-test Go file in the repository, and
// counts both what it examined and the conditional deletes it found, so a
// walk that reaches nothing cannot pass.
func TestNoProductionCodeDeletesAClientByIDAlone(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	var scanned, conditional int
	var byID []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, ok := sel.X.(*ast.SelectorExpr)
			if !ok || recv.Sel.Name != "Clients" {
				return true
			}
			switch sel.Sel.Name {
			case "Delete":
				rel, _ := filepath.Rel(root, path)
				byID = append(byID, fmt.Sprintf("%s:%d", rel, fset.Position(call.Pos()).Line))
			case "DeleteIf":
				conditional++
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, scanned, 20, "the walk is not reaching the repository")
	t.Logf("%d source files examined, %d conditional deletes found", scanned, conditional)
	require.Empty(t, byID, "a client is deleted by id alone; use Clients.DeleteIf with the client that was examined")
	require.GreaterOrEqual(t, conditional, 4, "fewer conditional deletes than the four sites that remove a client")
}

// A session is its client, its subscriptions and its in-flight messages, and
// every site that removes the client has to discard the other two with it.
//
// **The sweep was the site that did not**, so an expired member stayed in
// the topic index: a shared group went on selecting it, and the message was
// dropped when the substrate found no client under that id. A test driving
// one departure path cannot say a fifth path added later will not forget;
// the source can, so this walks it. Every function calling Clients.DeleteIf
// must also call UnsubscribeClient and ClearInflights - or EndTakenOver, which
// is ClearInflights under the handover lock with the connection marked taken
// over.
func TestEverySiteThatDeletesAClientDiscardsItsWholeSession(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)

	var scanned, sites int
	var partial []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var deletes, unsubscribes, clears bool
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "DeleteIf":
					if recv, ok := sel.X.(*ast.SelectorExpr); ok && recv.Sel.Name == "Clients" {
						deletes = true
					}
				case "UnsubscribeClient":
					unsubscribes = true
				case "ClearInflights", "EndTakenOver":
					clears = true
				}
				return true
			})
			if !deletes {
				continue
			}
			sites++
			if !unsubscribes || !clears {
				rel, _ := filepath.Rel(root, path)
				partial = append(partial, fmt.Sprintf("%s: %s (UnsubscribeClient %v, ClearInflights %v)",
					rel, fn.Name.Name, unsubscribes, clears))
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Greater(t, scanned, 20, "the walk is not reaching the repository")
	require.GreaterOrEqual(t, sites, 4, "fewer functions deleting a client than the four known "+
		"sites, so this walk has stopped matching the code it guards")
	t.Logf("%d source files examined, %d functions delete a client", scanned, sites)
	require.Empty(t, partial, "a client is deleted without discarding the rest of its session")
}

func TestServerClearExpiredInflights(t *testing.T) {
	s := New(nil)
	require.NotNil(t, s)
	s.Options.Capabilities.MaximumMessageExpiryInterval = 4

	n := time.Now().Unix()
	cl, _, _ := newTestClient()
	cl.ops.info = s.Info

	cl.State.Inflight.Set(packets.Packet{PacketID: 1, Expiry: n - 1})
	cl.State.Inflight.Set(packets.Packet{PacketID: 2, Expiry: n - 2})
	cl.State.Inflight.Set(packets.Packet{PacketID: 3, Created: n - 3}) // within bounds
	cl.State.Inflight.Set(packets.Packet{PacketID: 5, Created: n - 5}) // over max server expiry limit
	cl.State.Inflight.Set(packets.Packet{PacketID: 7, Created: n})
	for _, id := range []uint16{1, 2, 3, 5, 7} {
		cl.State.Inflight.Withhold(id) // never sent: only that expires
	}

	s.Clients.Add(cl)

	require.Len(t, cl.State.Inflight.GetAll(false), 5)
	s.clearExpiredInflights(n)
	require.Len(t, cl.State.Inflight.GetAll(false), 2)

	s.Options.Capabilities.MaximumMessageExpiryInterval = 0
	cl.State.Inflight.Set(packets.Packet{PacketID: 8, Expiry: n - 8})
	cl.State.Inflight.Withhold(8)
	s.clearExpiredInflights(n)
	require.Len(t, cl.State.Inflight.GetAll(false), 3)
}

func TestServerClearExpiredRetained(t *testing.T) {
	s := New(nil)
	require.NotNil(t, s)
	s.Options.Capabilities.MaximumMessageExpiryInterval = 4

	n := time.Now().Unix()
	s.Topics.Retained.Add("a/b/c", packets.Packet{ProtocolVersion: 5, Created: n, Expiry: n - 1})
	s.Topics.Retained.Add("d/e/f", packets.Packet{ProtocolVersion: 5, Created: n, Expiry: n - 2})
	s.Topics.Retained.Add("g/h/i", packets.Packet{ProtocolVersion: 5, Created: n - 3}) // within bounds
	s.Topics.Retained.Add("j/k/l", packets.Packet{ProtocolVersion: 5, Created: n - 5}) // over max server expiry limit
	s.Topics.Retained.Add("m/n/o", packets.Packet{ProtocolVersion: 5, Created: n})

	require.Len(t, s.Topics.Retained.GetAll(), 5)
	s.clearExpiredRetainedMessages(n)
	require.Len(t, s.Topics.Retained.GetAll(), 2)

	s.Topics.Retained.Add("p/q/r", packets.Packet{Created: n, Expiry: n - 1})
	s.Topics.Retained.Add("s/t/u", packets.Packet{Created: n, Expiry: n - 2}) // expiry is ineffective for v3.
	s.Topics.Retained.Add("v/w/x", packets.Packet{Created: n - 3})            // within bounds for v3
	s.Topics.Retained.Add("y/z/1", packets.Packet{Created: n - 5})            // over max server expiry limit
	require.Len(t, s.Topics.Retained.GetAll(), 6)
	s.clearExpiredRetainedMessages(n)
	require.Len(t, s.Topics.Retained.GetAll(), 5)

	s.Options.Capabilities.MaximumMessageExpiryInterval = 0
	s.Topics.Retained.Add("2/3/4", packets.Packet{Created: n - 8})
	s.clearExpiredRetainedMessages(n)
	require.Len(t, s.Topics.Retained.GetAll(), 6)
}

func TestServerClearExpiredClients(t *testing.T) {
	s := New(nil)
	require.NotNil(t, s)

	n := time.Now()

	cl, _, _ := newTestClient()
	cl.ID = "cl"
	s.Clients.Add(cl)

	// No Expiry
	cl0, _, _ := newTestClient()
	cl0.ID = "c0"
	cl0.State.disconnected.Store(n.Add(-10 * time.Second).UnixNano())
	cl0.State.cancelOpen()
	cl0.Properties.ProtocolVersion = 5
	cl0.Properties.Props.SessionExpiryInterval = 12
	cl0.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(cl0)

	// Normal Expiry
	cl1, _, _ := newTestClient()
	cl1.ID = "c1"
	cl1.State.disconnected.Store(n.Add(-10 * time.Second).UnixNano())
	cl1.State.cancelOpen()
	cl1.Properties.ProtocolVersion = 5
	cl1.Properties.Props.SessionExpiryInterval = 8
	cl1.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(cl1)

	// No Expiry, indefinite session
	cl2, _, _ := newTestClient()
	cl2.ID = "c2"
	cl2.State.disconnected.Store(n.Add(-10 * time.Second).UnixNano())
	cl2.State.cancelOpen()
	cl2.Properties.ProtocolVersion = 5
	cl2.Properties.Props.SessionExpiryInterval = 0
	cl2.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(cl2)

	// The expiring session holds a subscription and an in-flight message,
	// which have to go with it.
	cl1.State.Subscriptions.Add("a/b", packets.Subscription{Filter: "a/b"})
	s.Topics.Subscribe("c1", packets.Subscription{Filter: "a/b"})
	cl1.State.Inflight.Set(packets.Packet{PacketID: 1})
	require.Contains(t, s.Topics.Subscribers("a/b").Subscriptions, "c1")

	require.Equal(t, 4, s.Clients.Len())

	s.clearExpiredClients(n)
	require.Equal(t, 2, s.Clients.Len())
	require.NotContains(t, s.Topics.Subscribers("a/b").Subscriptions, "c1",
		"an expired session left its subscription in the topic index")
	require.Equal(t, 0, cl1.State.Inflight.Len(), "an expired session kept its in-flight messages")
}

func TestServerSubscribe(t *testing.T) {
	handler := func(cl *Client, sub packets.Subscription, pk packets.Packet) {}

	s := newServerWithInlineClient()
	require.NotNil(t, s)

	tt := []struct {
		desc       string
		filter     string
		identifier int
		handler    InlineSubFn
		expect     error
	}{
		{
			desc:       "subscribe",
			filter:     "a/b/c",
			identifier: 1,
			handler:    handler,
			expect:     nil,
		},
		{
			desc:       "re-subscribe",
			filter:     "a/b/c",
			identifier: 1,
			handler:    handler,
			expect:     nil,
		},
		{
			desc:       "subscribe d/e/f",
			filter:     "d/e/f",
			identifier: 1,
			handler:    handler,
			expect:     nil,
		},
		{
			desc:       "re-subscribe d/e/f by different identifier",
			filter:     "d/e/f",
			identifier: 2,
			handler:    handler,
			expect:     nil,
		},
		{
			desc:       "subscribe different handler",
			filter:     "a/b/c",
			identifier: 1,
			handler:    func(cl *Client, sub packets.Subscription, pk packets.Packet) {},
			expect:     nil,
		},
		{
			desc:       "subscribe $SYS/info",
			filter:     "$SYS/info",
			identifier: 1,
			handler:    handler,
			expect:     nil,
		},
		{
			desc:       "subscribe invalid ###",
			filter:     "###",
			identifier: 1,
			handler:    handler,
			expect:     packets.ErrTopicFilterInvalid,
		},
		{
			desc:       "subscribe invalid handler",
			filter:     "a/b/c",
			identifier: 1,
			handler:    nil,
			expect:     packets.ErrInlineSubscriptionHandlerInvalid,
		},
	}

	for _, tx := range tt {
		t.Run(tx.desc, func(t *testing.T) {
			require.Equal(t, tx.expect, s.Subscribe(tx.filter, tx.identifier, tx.handler))
		})
	}
}

func TestServerSubscribeNoInlineClient(t *testing.T) {
	s := newServer()
	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInlineClientNotEnabled)
}

func TestServerUnsubscribe(t *testing.T) {
	handler := func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		// handler logic
	}

	s := newServerWithInlineClient()
	err := s.Subscribe("a/b/c", 1, handler)
	require.Nil(t, err)

	err = s.Subscribe("d/e/f", 1, handler)
	require.Nil(t, err)

	err = s.Subscribe("d/e/f", 2, handler)
	require.Nil(t, err)

	err = s.Unsubscribe("a/b/c", 1)
	require.Nil(t, err)

	err = s.Unsubscribe("d/e/f", 1)
	require.Nil(t, err)

	err = s.Unsubscribe("d/e/f", 2)
	require.Nil(t, err)

	err = s.Unsubscribe("not/exist", 1)
	require.Nil(t, err)

	err = s.Unsubscribe("#/#/invalid", 1)
	require.Equal(t, packets.ErrTopicFilterInvalid, err)
}

func TestServerUnsubscribeNoInlineClient(t *testing.T) {
	s := newServer()
	err := s.Unsubscribe("a/b/c", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInlineClientNotEnabled)
}

func TestPublishToInlineSubscriber(t *testing.T) {
	s := newServerWithInlineClient()
	finishCh := make(chan bool)
	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
		s.publishToSubscribers(pkx)
	}()

	require.Equal(t, true, <-finishCh)
}

func TestPublishToInlineSubscribersDifferentFilter(t *testing.T) {
	s := newServerWithInlineClient()
	subNumber := 2
	finishCh := make(chan bool, subNumber)

	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	err = s.Subscribe("z/e/n", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("mochi mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "z/e/n", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
		s.publishToSubscribers(pkx)

		pkx = *packets.TPacketData[packets.Publish].Get(packets.TPublishCopyBasic).Packet
		s.publishToSubscribers(pkx)
	}()

	for i := 0; i < subNumber; i++ {
		require.Equal(t, true, <-finishCh)
	}
}

func TestPublishToInlineSubscribersDifferentIdentifier(t *testing.T) {
	s := newServerWithInlineClient()
	subNumber := 2
	finishCh := make(chan bool, subNumber)

	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	err = s.Subscribe("a/b/c", 2, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 2, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	go func() {
		pkx := *packets.TPacketData[packets.Publish].Get(packets.TPublishBasic).Packet
		s.publishToSubscribers(pkx)
	}()

	for i := 0; i < subNumber; i++ {
		require.Equal(t, true, <-finishCh)
	}
}

func TestServerSubscribeWithRetain(t *testing.T) {
	s := newServerWithInlineClient()
	subNumber := 1
	finishCh := make(chan bool, subNumber)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)
	require.Equal(t, true, <-finishCh)
}

func TestServerSubscribeWithRetainDifferentFilter(t *testing.T) {
	s := newServerWithInlineClient()
	subNumber := 2
	finishCh := make(chan bool, subNumber)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)
	retained = s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishCopyBasic).Packet)
	require.Equal(t, int64(1), retained)

	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	err = s.Subscribe("z/e/n", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("mochi mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "z/e/n", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	for i := 0; i < subNumber; i++ {
		require.Equal(t, true, <-finishCh)
	}
}

func TestServerSubscribeWithRetainDifferentIdentifier(t *testing.T) {
	s := newServerWithInlineClient()
	subNumber := 2
	finishCh := make(chan bool, subNumber)

	retained := s.Topics.RetainMessage(*packets.TPacketData[packets.Publish].Get(packets.TPublishRetain).Packet)
	require.Equal(t, int64(1), retained)

	err := s.Subscribe("a/b/c", 1, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 1, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	err = s.Subscribe("a/b/c", 2, func(cl *Client, sub packets.Subscription, pk packets.Packet) {
		require.Equal(t, []byte("hello mochi"), pk.Payload)
		require.Equal(t, InlineClientId, cl.ID)
		require.Equal(t, LocalListener, cl.Net.Listener)
		require.Equal(t, "a/b/c", sub.Filter)
		require.Equal(t, 2, sub.Identifier)
		finishCh <- true
	})
	require.Nil(t, err)

	for i := 0; i < subNumber; i++ {
		require.Equal(t, true, <-finishCh)
	}
}

func TestMinimum(t *testing.T) {
	require.EqualValues(t, 0, minimum(0, 0))
	require.EqualValues(t, 1, minimum(0, 1))
	require.EqualValues(t, 1, minimum(1, 0))
	require.EqualValues(t, 10, minimum(10, 20))
	require.EqualValues(t, 20, minimum(30, 20))
	require.EqualValues(t, -1, minimum(-1, 0)) // negative values are not used, but included here for completeness
	require.EqualValues(t, -1, minimum(-1, 20))
	require.EqualValues(t, -2, minimum(-1, -2))
}

// refuseFilterHook refuses one filter of a SUBSCRIBE by setting its reason
// code, and leaves the rest of the packet alone.
type refuseFilterHook struct {
	HookBase
	filter string
	code   byte
}

func (h *refuseFilterHook) ID() string { return "refuse-filter" }

func (h *refuseFilterHook) Provides(b byte) bool {
	return bytes.Contains([]byte{OnSubscribe}, []byte{b})
}

func (h *refuseFilterHook) OnSubscribe(cl *Client, pk packets.Packet) packets.Packet {
	pk.ReasonCodes = make([]byte, len(pk.Filters))
	for i, sub := range pk.Filters {
		if sub.Filter == h.filter {
			pk.ReasonCodes[i] = h.code
		}
	}
	return pk
}

// An OnSubscribe hook may refuse a single filter and say why. Without it
// the only hook-driven refusal is OnACLCheck, which can answer 0x87 and
// nothing else, so a hook rejecting a filter for any other reason has to
// mislabel it as an authorization failure or disconnect the client.
func TestServerProcessSubscribeHookReasonCodes(t *testing.T) {
	s := newServer()
	require.NoError(t, s.AddHook(&refuseFilterHook{
		filter: "d/e/f/g/h/i",
		code:   packets.ErrTopicFilterInvalid.Code,
	}, nil))
	_ = s.Serve()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	go func() {
		err := s.processSubscribe(cl, *packets.TPacketData[packets.Subscribe].Get(packets.TSubscribeMany).Packet)
		require.NoError(t, err)
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)

	// The reason codes are the SUBACK payload, so they are the last byte
	// per filter. The refused one carries the hook's code; the others are
	// the granted QoS, untouched.
	require.Equal(t,
		[]byte{0, packets.ErrTopicFilterInvalid.Code, 2},
		buf[len(buf)-3:])

	// A refused filter is not subscribed, or the client would receive
	// messages for a subscription the SUBACK told it it did not have.
	require.Empty(t, s.Topics.Subscribers("d/e/f/g/h/i").Subscriptions)
	require.Contains(t, s.Topics.Subscribers("a/b").Subscriptions, cl.ID)
	require.Contains(t, s.Topics.Subscribers("x/y/z").Subscriptions, cl.ID)

	_, ok := cl.State.Subscriptions.Get("d/e/f/g/h/i")
	require.False(t, ok)
}

// A successful PUBACK must carry a PUBACK reason code. 0x01 is Granted QoS
// 1, a SUBACK code, and [MQTT-3.4.2-1] requires one of the codes §3.4.2.1
// lists - 0x00, 0x10, and the 0x80-and-above refusals.
//
// **This used to read the reason byte off the wire, and cannot any more.**
// The encoder writes that byte only for a refusal or where there are
// properties (encodePubAckRelRecComp), and the way the old version of this
// test produced properties was to publish with a User Property, which the
// acknowledgement then echoed back. Acknowledgements no longer carry
// anything the request held, so a successful PUBACK is now the four-byte
// short form and the byte is never written - which makes a wrong code
// invisible on the wire and, for the moment, harmless there too.
//
// So it asks the packet instead of the bytes. OnPacketEncode sees what the
// publish path built, before the encoder decides what to leave out, which
// is where the wrong code would be. The wire assertion stays beside it as
// the tripwire: if a later change gives a successful acknowledgement a
// property, these bytes grow, the reason byte starts being written, and
// the code matters on the wire again.
func TestServerProcessPacketPublishQos1AckReasonCode(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	seen := new(ackWatcher)
	require.NoError(t, s.AddHook(seen, nil))

	// newTestClient builds its own hooks rather than the server's, so a
	// hook added above would never see this client's packets. Pointing it
	// at the server's is what makes the assertion below about the server.
	cl.ops.hooks = s.hooks

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1Mqtt5).Packet
	pk.Properties.User = []packets.UserProperty{{Key: "mine", Val: "ok"}}

	go func() {
		require.NoError(t, s.processPacket(cl, pk))
		_ = w.Close()
	}()

	buf, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NotEmpty(t, buf)
	require.Equal(t, packets.Puback, buf[0]>>4)

	// It proves it saw the packet at all: without this the assertion below
	// passes on a zero value that no publish ever produced.
	require.True(t, seen.got, "no PUBACK reached OnPacketEncode")
	require.Equal(t, packets.CodeSuccess.Code, seen.code,
		"a successful PUBACK must carry a PUBACK reason code, not a granted-QoS one")

	// The short form, which is what the encoder is entitled to write while
	// a successful acknowledgement carries no properties: fixed header,
	// remaining length 2, and the packet identifier.
	require.Equal(t, []byte{packets.Puback << 4, 2, 0, 7}, buf)
}

// ackWatcher records the reason code of the first PUBACK the server builds,
// as it is handed to the encoder.
type ackWatcher struct {
	HookBase
	got  bool
	code byte
}

func (h *ackWatcher) ID() string { return "ack-watcher" }

func (h *ackWatcher) Provides(b byte) bool { return b == OnPacketEncode }

func (h *ackWatcher) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Puback && !h.got {
		h.got, h.code = true, pk.ReasonCode
	}
	return pk
}

func TestServerUnsubscribeCountsAndAnswersOnlyWhatItRemoved(t *testing.T) {
	s := newServer()

	// Two clients on one filter, so the filter node outlives the first
	// client's subscription on it.
	a, ar, _ := newTestClient()
	a.ID = "cl-a"
	a.Properties.ProtocolVersion = 5
	b, br, _ := newTestClient()
	b.ID = "cl-b"
	b.Properties.ProtocolVersion = 5

	// **Each packet arrives on a channel rather than in a buffer.**
	// net.Pipe hands the write back as soon as the bytes are copied, so a
	// reader appending them to a slice can still be behind when the next
	// assertion runs - the first version of this test read the previous
	// acknowledgement and reported the fix broken.
	acks := make(chan []byte, 8)
	drain := func(c net.Conn) {
		buf := make([]byte, 256)
		for {
			n, err := c.Read(buf)
			if n > 0 {
				acks <- append([]byte(nil), buf[:n]...)
			}
			if err != nil {
				return
			}
		}
	}
	go drain(ar)
	go drain(br)
	reasonCode := func() byte {
		select {
		case pk := <-acks:
			require.NotEmpty(t, pk)
			return pk[len(pk)-1]
		case <-time.After(2 * time.Second):
			t.Fatal("no acknowledgement arrived")
			return 0
		}
	}

	subscribe := func(cl *Client) {
		require.NoError(t, s.processSubscribe(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Subscribe},
			PacketID:    1,
			Filters:     packets.Subscriptions{{Filter: "a/b", Qos: 0}},
		}))
		<-acks // the SUBACK, which this test is not about
	}
	unsubscribe := func(cl *Client) byte {
		require.NoError(t, s.processUnsubscribe(cl, packets.Packet{
			FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe},
			PacketID:    2,
			Filters:     packets.Subscriptions{{Filter: "a/b"}},
		}))
		return reasonCode()
	}

	subscribe(a)
	subscribe(b)
	require.Equal(t, int64(2), s.Info.Subscriptions.Load())

	require.Equal(t, packets.CodeSuccess.Code, unsubscribe(a))
	require.Equal(t, int64(1), s.Info.Subscriptions.Load())

	// cl-a has already gone and cl-b is holding the node open, so there is
	// nothing of cl-a's to remove: no decrement, and 0x11 rather than
	// success.
	require.Equal(t, packets.CodeNoSubscriptionExisted.Code, unsubscribe(a))
	require.Equal(t, int64(1), s.Info.Subscriptions.Load())

	require.Equal(t, packets.CodeSuccess.Code, unsubscribe(b))
	require.Equal(t, int64(0), s.Info.Subscriptions.Load())
}

// refusedHook records the CONNECT refusals a hook is told about.
type refusedHook struct {
	HookBase
	mu    sync.Mutex
	seen  []string
	codes []packets.Code
}

func (h *refusedHook) ID() string { return "refused" }

func (h *refusedHook) Provides(b byte) bool { return b == OnConnectRefused }

func (h *refusedHook) OnConnectRefused(cl *Client, pk packets.Packet, code packets.Code) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, cl.ID)
	h.codes = append(h.codes, code)
}

func (h *refusedHook) refusals() ([]string, []packets.Code) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...), append([]packets.Code(nil), h.codes...)
}

// slowAuthHook admits every client after the delay a password hash costs, so
// connections pile up in authentication the way they do on a real broker.
type slowAuthHook struct {
	HookBase
	refuse string // a user name this hook refuses, when set
}

func (h *slowAuthHook) ID() string { return "slow-auth" }

func (h *slowAuthHook) Provides(b byte) bool {
	return b == OnConnectAuthenticate || b == OnACLCheck
}

func (h *slowAuthHook) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool {
	time.Sleep(50 * time.Millisecond)
	return h.refuse == "" || string(pk.Connect.Username) != h.refuse
}

func (h *slowAuthHook) OnACLCheck(cl *Client, topic string, write bool) bool { return true }

// limitServer serves a TCP listener with the given connection limit and
// connect timeout. Both are set before Serve, because the listener's own
// goroutine reads them for every connection it accepts.
func limitServer(t *testing.T, limit int64, connectTimeout time.Duration, auth *slowAuthHook) (*Server, string) {
	t.Helper()
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: connectTimeout})
	require.NoError(t, s.AddHook(auth, nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return s, tcp.Address()
}

// connectAsUser is an MQTT 5 CONNECT with a user name and password.
func connectAsUser(id, user, password string) []byte {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0xC2, 0x00, 0x3c, 0x00}
	for _, f := range []string{id, user, password} {
		body = append(body, byte(len(f)>>8), byte(len(f)))
		body = append(body, f...)
	}
	return append([]byte{0x10, byte(len(body))}, body...)
}

// dialAndConnect sends one CONNECT and returns the connection and the reason
// code of the CONNACK the broker answered.
func dialAndConnect(t *testing.T, addr string, connect []byte) (net.Conn, byte) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_, _ = c.Write(connect)
	return c, takeoverConnack(c)
}

// RFC 0002 "Every reason code": `0x89` Server busy when
// `limits.max_connections` is reached.
//
// **The limit holds against connections arriving together.** It was checked
// against a count incremented only after authentication, so every CONNECT
// still authenticating passed it: measured before this, 50 of 50 accepted
// against a limit of 5. Counted from accept, as mosquitto, NATS and EMQX
// count, a connection holds its slot from the moment it arrives.
//
// **And past it the broker holds no more than its overflow budget** (RFC
// 0002 "How long a socket may wait to send CONNECT"): exactly
// max_connections are admitted, every other connection is answered 0x89
// while the budget has room or closed with nothing written, no more of
// them are answered than the budget holds, and at no moment are more
// sockets open than max_connections, the budget and the one the accept
// loop has in hand as it closes it.
func TestMaxConnectionsHoldsAgainstConnectsArrivingTogether(t *testing.T) {
	const limit, n = 5, 50
	const budget = 5 // max_connections or 32, whichever is fewer
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc})
	require.NoError(t, s.AddHook(&slowAuthHook{}, nil))
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counted := &countingListener{Listener: raw}
	require.NoError(t, s.AddListener(listeners.NewNet("t", counted)))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	addr := raw.Addr().String()

	conns := make([]net.Conn, n)
	for i := range conns {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		conns[i] = c
		defer c.Close()
	}
	codes := make([]byte, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c net.Conn) {
			defer wg.Done()
			<-start
			_, _ = c.Write(takeoverConnect(fmt.Sprintf("device-%d", i)))
			codes[i] = takeoverConnack(c)
		}(i, c)
	}
	close(start)
	wg.Wait()

	count := map[byte]int{}
	for _, c := range codes {
		count[c]++
	}
	require.Equal(t, limit, count[0x00], "accepted with max_connections %d: %v", limit, count)
	require.Equal(t, n-limit, count[packets.ErrServerBusy.Code]+count[0xFF],
		"a connection past the limit was neither answered 0x89 nor closed: %v", count)
	require.LessOrEqual(t, count[packets.ErrServerBusy.Code], budget,
		"more connections were answered 0x89 than the overflow budget holds: %v", count)
	require.LessOrEqual(t, counted.peak.Load(), int64(limit+budget+1),
		"more sockets were open at once than max_connections, the budget and the one in the accept loop's hand")
}

// **Taking a slot is one step.** Connections are accepted on their own
// goroutines, so their reservations race each other directly: a check of the
// count followed by an increment lets several see the last free slot. Driven
// at the function, because the socket test above dials one connection at a
// time and so takes its slots in order - removing the compare-and-swap left
// it passing.
func TestReserveSlotNeverGrantsMoreThanTheLimit(t *testing.T) {
	const limit, callers, rounds = 10, 64, 200
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = limit
	s := New(&Options{Logger: logger, Capabilities: cc})

	over := 0
	for round := 0; round < rounds; round++ {
		s.slots.Store(0)
		var granted atomic.Int64
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if s.reserveSlot() {
					granted.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if granted.Load() != limit {
			over++
		}
		require.LessOrEqual(t, s.slots.Load(), int64(limit), "round %d: the slot count passed the limit", round)
	}
	require.Zero(t, over, "%d of %d rounds of %d callers did not grant exactly %d slots", over, rounds, callers, limit)
}

// **A connection that has not sent CONNECT holds a slot**, so a peer cannot
// stay under the limit by opening sockets and saying nothing. The slot is
// released when connect_timeout closes it, and a client arriving then is
// admitted.
func TestAConnectionWaitingForCONNECTHoldsASlot(t *testing.T) {
	s, addr := limitServer(t, 2, 300*time.Millisecond, &slowAuthHook{})

	var silent []net.Conn
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer c.Close()
		silent = append(silent, c)
	}
	require.Eventually(t, func() bool { return s.slots.Load() == 2 }, 5*time.Second, time.Millisecond,
		"the two silent sockets were not both accepted")

	c, code := dialAndConnect(t, addr, takeoverConnect("third"))
	_ = c.Close()
	require.Equal(t, packets.ErrServerBusy.Code, code, "a CONNECT was admitted past two sockets holding the two slots")

	// Past connect_timeout both silent sockets are closed, and a slot goes
	// back before its socket closes (Client.beginEnd): read to the close.
	for i, c := range silent {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err := io.Copy(io.Discard, c)
		require.NoError(t, err, "silent socket %d was not closed at connect_timeout", i)
	}
	c, code = dialAndConnect(t, addr, takeoverConnect("fourth"))
	defer c.Close()
	require.Equal(t, byte(0x00), code, "the slots of the closed silent sockets were not released")
}

// RFC 0002 "How long a socket may wait to send CONNECT": a socket holds a
// max_connections slot from the moment it arrives, on the ws door as on the
// others - where it arrives as an HTTP request the engine is handed only
// after the upgrade. Before the door took the slot at accept, fifty idle
// sockets on it held nothing, and a tcp CONNECT was answered 0x00 at a
// limit of two. The time before the upgrade is connect_timeout's, and an
// upgraded MQTT connection holds its one slot, not a second.
func TestASocketOnTheWebsocketDoorHoldsASlotFromArrival(t *testing.T) {
	const bound = time.Second
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 2
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: bound})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	refused := new(socketRefusals)
	require.NoError(t, s.AddHook(refused, nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	ws := listeners.NewWebsocket(listeners.Config{ID: "w", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.AddListener(ws))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })

	// Fifty idle sockets: two take the slots, and the rest are closed at
	// accept with nothing written.
	var idle []net.Conn
	for i := 0; i < 50; i++ {
		c, err := net.Dial("tcp", ws.Address())
		require.NoError(t, err)
		idle = append(idle, c)
	}
	defer func() {
		for _, c := range idle {
			_ = c.Close()
		}
	}()
	// The door says what it did with each socket: a refusal at accept is
	// closed and then reported, and a socket within the limit holds a slot.
	// Read from the broker rather than from fifty reads with a quiet period
	// each, which is a race with the two sockets' own bound.
	require.Eventually(t, func() bool { return refused.count() == 48 && s.slots.Load() == 2 },
		5*time.Second, time.Millisecond, "%d sockets were closed at accept and %d hold slots, want 48 and 2",
		refused.count(), s.slots.Load())
	c, code := dialAndConnect(t, tcp.Address(), takeoverConnect("tcp-a"))
	_ = c.Close()
	require.Equal(t, packets.ErrServerBusy.Code, code,
		"a tcp CONNECT was admitted past two idle websocket sockets holding the two slots")

	// Past connect_timeout the two idle sockets are closed, and their slots
	// come back.
	require.Eventually(t, func() bool { return s.slots.Load() == 0 }, bound+5*time.Second, time.Millisecond,
		"the idle sockets' slots did not come back at connect_timeout")

	// An upgraded connection holds one slot: beside it one tcp CONNECT fits,
	// and a second does not.
	d := websocket.Dialer{Subprotocols: []string{"mqtt"}}
	wc, _, err := d.Dial("ws://"+ws.Address()+"/", nil)
	require.NoError(t, err)
	defer wc.Close()
	require.NoError(t, wc.WriteMessage(websocket.BinaryMessage, takeoverConnect("ws-a")))
	_ = wc.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, b, err := wc.ReadMessage()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(b), 4)
	require.Equal(t, byte(0x00), b[3], "the websocket CONNECT was refused: the idle sockets' slots did not come back")

	c1, code := dialAndConnect(t, tcp.Address(), takeoverConnect("tcp-b"))
	defer c1.Close()
	require.Equal(t, byte(0x00), code, "an upgraded websocket connection held two slots")
	c2, code := dialAndConnect(t, tcp.Address(), takeoverConnect("tcp-c"))
	defer c2.Close()
	require.Equal(t, packets.ErrServerBusy.Code, code, "the upgraded websocket connection held no slot")
}

// A websocket connection gives its slot back once when it ends, however
// many closes reach its socket. An ordinary session end reaches it twice -
// the handler's deferred Close and the engine's teardown - so a slot given
// back on every Close pushed the count below zero, and after three sessions
// four tcp CONNECTs were admitted at a limit of two. (A mutant without the once passed the test above.)
func TestAWebsocketSessionGivesItsSlotBackOnce(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 2
	s := New(&Options{Logger: logger, Capabilities: cc, ClientConnectTimeout: time.Second})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	ws := listeners.NewWebsocket(listeners.Config{ID: "w", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.AddListener(ws))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })

	d := websocket.Dialer{Subprotocols: []string{"mqtt"}}
	for i := 0; i < 3; i++ {
		wc, _, err := d.Dial("ws://"+ws.Address()+"/", nil)
		require.NoError(t, err)
		require.NoError(t, wc.WriteMessage(websocket.BinaryMessage, takeoverConnect(fmt.Sprintf("ws-%d", i))))
		_ = wc.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, b, err := wc.ReadMessage()
		require.NoError(t, err)
		require.Equal(t, byte(0x00), b[3], "websocket session %d was refused", i)
		_ = wc.Close()
	}
	require.Eventually(t, func() bool { return s.slots.Load() == 0 }, 2*time.Second, 10*time.Millisecond,
		"after three websocket sessions ended the server holds %d slots, want 0", s.slots.Load())

	var admitted int
	for i := 0; i < 3; i++ {
		c, code := dialAndConnect(t, tcp.Address(), takeoverConnect(fmt.Sprintf("tcp-%d", i)))
		defer c.Close()
		if code == 0x00 {
			admitted++
		}
	}
	require.Equal(t, 2, admitted, "admitted %d tcp CONNECTs at a limit of 2 after three websocket sessions", admitted)
}

// **An over-limit CONNECT is answered without its body being read.** It
// declares a megabyte and sends only the bytes that name its protocol and
// client id; the broker answers 0x89 from those, and tells the refusal hook
// which client it was. Reading the declared body first is what let 1,000
// such connections hold a gigabyte before any of them was refused.
func TestAnOverLimitConnectIsAnsweredWithoutReadingItsBody(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 0
	s := New(&Options{Logger: logger, Capabilities: cc})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(hook, nil))
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	defer s.Close()

	c, err := net.Dial("tcp", tcp.Address())
	require.NoError(t, err)
	defer c.Close()
	const declared = 1_048_000
	head := []byte{0x10, 0xC0, 0xFB, 0x3F} // remaining length 1,048,000
	head = append(head, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00)
	head = append(head, 0x00, 0x03, 'b', 'i', 'g')
	_, err = c.Write(head) // and nothing more of the megabyte
	require.NoError(t, err)

	start := time.Now()
	code := takeoverConnack(c)
	require.Less(t, time.Since(start), time.Second,
		"an over-limit CONNECT that sent only its opening bytes was not answered promptly")
	require.Equal(t, packets.ErrServerBusy.Code, code,
		"an over-limit CONNECT that sent only its opening bytes was not answered 0x89")

	require.Eventually(t, func() bool { ids, _ := hook.refusals(); return len(ids) == 1 },
		time.Second, 10*time.Millisecond, "the refusal hook was not told")
	ids, codes := hook.refusals()
	require.Equal(t, "big", ids[0], "the refusal did not name the client")
	require.Equal(t, packets.ErrServerBusy, codes[0])
}

// sizeServer serves a TCP listener refusing a CONNECT larger than max bytes,
// with a hook that records every refusal.
func sizeServer(t *testing.T, max uint32) (*refusedHook, string) {
	t.Helper()
	s := New(&Options{Logger: logger, ClientMaxConnectSize: max})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(hook, nil))
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
	require.NoError(t, s.AddListener(tcp))
	require.NoError(t, s.Serve())
	t.Cleanup(func() { _ = s.Close() })
	return hook, tcp.Address()
}

// connectOfSize is an MQTT 5 CONNECT of exactly total bytes, padded in its
// client id.
func connectOfSize(t *testing.T, total int) []byte {
	t.Helper()
	// fixed header byte, remaining length, then 11 bytes of variable header
	// and a 2-byte client id length.
	for bu := 1; bu <= 4; bu++ {
		idLen := total - 1 - bu - 11 - 2
		remaining := 11 + 2 + idLen
		enc := remaining
		n := 1
		for enc >= 128 {
			enc /= 128
			n++
		}
		if n != bu || idLen < 0 {
			continue
		}
		var length []byte
		for x := remaining; ; {
			b := byte(x % 128)
			x /= 128
			if x > 0 {
				b |= 0x80
			}
			length = append(length, b)
			if x == 0 {
				break
			}
		}
		pk := append([]byte{0x10}, length...)
		pk = append(pk, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00)
		pk = append(pk, byte(idLen>>8), byte(idLen))
		pk = append(pk, []byte(strings.Repeat("i", idLen))...)
		require.Len(t, pk, total)
		return pk
	}
	t.Fatalf("no CONNECT of %d bytes", total)
	return nil
}

// RFC 0002 "The largest CONNECT before authentication: limits.max_connect_size"
//
// **A CONNECT larger than the limit is refused from its declared length**,
// before its body is read, as mosquitto and HiveMQ both check it: the client
// declares a megabyte, sends only its opening bytes, and is answered 0x95
// (Packet too large) at once, named in the refusal. Read in full first, a
// megabyte a handshake is what let 10,000 of them hold 10 GiB.
func TestAConnectOverMaxConnectSizeIsAnsweredWithoutItsBody(t *testing.T) {
	hook, addr := sizeServer(t, 1024)

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	head := []byte{0x10, 0xC0, 0xFB, 0x3F} // remaining length 1,048,000
	head = append(head, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3c, 0x00)
	head = append(head, 0x00, 0x03, 'b', 'i', 'g')
	_, err = c.Write(head)
	require.NoError(t, err)

	start := time.Now()
	code := takeoverConnack(c)
	require.Less(t, time.Since(start), time.Second, "an oversized CONNECT sending only its opening bytes was not answered promptly")
	require.Equal(t, packets.ErrPacketTooLarge.Code, code)

	require.Eventually(t, func() bool { ids, _ := hook.refusals(); return len(ids) == 1 },
		time.Second, 10*time.Millisecond, "the refusal hook was not told")
	ids, codes := hook.refusals()
	require.Equal(t, "big", ids[0])
	require.Equal(t, packets.ErrPacketTooLarge, codes[0])
}

// **A 3.1.1 client has no Packet too large to be told**, and every CONNECT
// refusal a 3.1.1 client can reach must be a code 3.1.1 defines - so an
// oversized one is closed with nothing written, as mosquitto closes it.
func TestAnOversizedConnectFromA311ClientIsClosedWithNoReply(t *testing.T) {
	_, addr := sizeServer(t, 1024)

	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	head := []byte{0x10, 0xC0, 0xFB, 0x3F}
	head = append(head, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c)
	head = append(head, 0x00, 0x03, 'o', 'l', 'd')
	_, err = c.Write(head)
	require.NoError(t, err)

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(make([]byte, 8))
	require.Zero(t, n, "an oversized 3.1.1 CONNECT was written a reply")
	var ne net.Error
	require.False(t, errors.As(err, &ne) && ne.Timeout(), "an oversized 3.1.1 CONNECT was left open")
}

// **The limit counts the whole packet**: a CONNECT of exactly max_connect_size
// bytes, fixed header included, is admitted and one byte more is not.
func TestMaxConnectSizeCountsTheWholePacket(t *testing.T) {
	_, addr := sizeServer(t, 300)

	c, code := dialAndConnect(t, addr, connectOfSize(t, 300))
	_ = c.Close()
	require.Equal(t, byte(0x00), code, "a CONNECT of exactly the limit was refused")

	c, code = dialAndConnect(t, addr, connectOfSize(t, 301))
	_ = c.Close()
	require.Equal(t, packets.ErrPacketTooLarge.Code, code, "a CONNECT one byte over the limit was admitted")
}

// **Every way a connection ends gives its slot back**: a CONNECT refused for
// its credentials, and a client that connected and left.
func TestASlotIsReleasedByARefusalAndByADisconnect(t *testing.T) {
	_, addr := limitServer(t, 1, 0, &slowAuthHook{refuse: "bad"})

	c, code := dialAndConnect(t, addr, connectAsUser("a", "bad", "x"))
	_ = c.Close()
	require.Equal(t, packets.ErrBadUsernameOrPassword.Code, code)

	c, code = dialAndConnect(t, addr, connectAsUser("b", "good", "x"))
	require.Equal(t, byte(0x00), code, "a CONNECT refused for its credentials kept its slot")
	_ = c.Close()

	require.Eventually(t, func() bool {
		c, code := dialAndConnect(t, addr, connectAsUser("c", "good", "x"))
		defer c.Close()
		return code == 0x00
	}, 2*time.Second, 50*time.Millisecond, "a client that disconnected kept its slot")
}

// A CONNECT refused before any other hook runs tells a hook which client it
// was. Without it the only record is the returned code, which says nothing
// about who was refused.
func TestOnConnectRefusedNamesTheClient(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 0
	s := New(&Options{Logger: logger, Capabilities: cc})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(hook, nil))
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()

	require.ErrorIs(t, <-o, packets.ErrServerBusy)
	_ = w.Close()

	ids, codes := hook.refusals()
	require.Len(t, ids, 1)
	require.Equal(t, "zen", ids[0]) // the fixture CONNECT's client id

	// The fixture is a 3.1.1 CONNECT, and 3.1.1 has no Server Busy: the
	// client is sent Server Unavailable, so that is what the hook is told.
	require.Equal(t, packets.ErrServerUnavailable, codes[0])
}

// The same refusal to an MQTT 5 client, which is sent Server Busy. The two
// versions are the whole of the difference, so both are checked -- a hook
// told a code the client never received cannot name what it was turned away
// with.
func TestOnConnectRefusedNamesTheCodeTheClientWasSentV5(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 0
	s := New(&Options{Logger: logger, Capabilities: cc})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(hook, nil))
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()

	require.ErrorIs(t, <-o, packets.ErrServerBusy)
	_ = w.Close()

	_, codes := hook.refusals()
	require.Len(t, codes, 1)
	require.Equal(t, packets.ErrServerBusy, codes[0])
}

// The same for a refusal validateConnect makes - here an unacceptable
// protocol version, which is the case an operator meets when they close the
// door on an older fleet.
func TestOnConnectRefusedCoversValidateConnect(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MinimumProtocolVersion = 5
	s := New(&Options{Logger: logger, Capabilities: cc})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(hook, nil))
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()

	require.ErrorIs(t, <-o, packets.ErrUnsupportedProtocolVersion)
	_ = w.Close()

	ids, codes := hook.refusals()
	require.Len(t, ids, 1)
	require.Equal(t, "zen", ids[0]) // the fixture CONNECT's client id
	require.Equal(t, packets.ErrUnsupportedProtocolVersion, codes[0])
}

// A server with no such hook behaves exactly as before, which is what makes
// this additive: HookBase's no-op means every existing hook opts out.
func TestOnConnectRefusedIsOptional(t *testing.T) {
	cc := NewDefaultServerCapabilities()
	cc.MaximumClients = 0
	s := New(&Options{Logger: logger, Capabilities: cc})
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()

	require.ErrorIs(t, <-o, packets.ErrServerBusy)
	_ = w.Close()
}

// A hook that refuses a publish by WRAPPING a packets.Code, which is what
// fmt.Errorf("%w") produces and what a hook returning context with its
// refusal will do.
type wrappingRefusalHook struct {
	HookBase
}

func (h *wrappingRefusalHook) ID() string { return "wrapping-refusal" }

func (h *wrappingRefusalHook) Provides(b byte) bool { return b == OnPublish }

func (h *wrappingRefusalHook) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return pk, fmt.Errorf("refused by policy: %w", packets.ErrNotAuthorized)
}

// A refusal carrying a wrapped code is answered, not panicked on.
//
// errors.As succeeds through a wrapper, so a type assertion on the error
// itself takes the broker down on an ordinary publish refusal - a hook is
// entitled to add context to the code it returns.
func TestPublishRefusedWithAWrappedCodeDoesNotPanic(t *testing.T) {
	s := newServer()
	require.NoError(t, s.AddHook(new(wrappingRefusalHook), nil))
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	// Nothing here drains the pipe, and the refusal is written to it. Left
	// alone the write now waits out the keepalive bound, so this test would
	// take fifteen seconds to assert something that happens immediately.
	cl.ops.options.ClientNetWriteTimeout = 50 * time.Millisecond
	s.Clients.Add(cl)

	go func() {
		_, _ = io.ReadAll(w)
	}()

	pk := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	require.NotPanics(t, func() {
		_ = s.processPublish(cl, pk)
	})

	_ = r.Close()
	_ = w.Close()
}

// wrappingEstablishHook refuses every session by wrapping a packets.Code, as
// a hook adding context to its refusal does.
type wrappingEstablishHook struct {
	HookBase
}

func (h *wrappingEstablishHook) ID() string { return "wrapping-establish" }

func (h *wrappingEstablishHook) Provides(b byte) bool { return b == OnSessionEstablish }

func (h *wrappingEstablishHook) OnSessionEstablish(cl *Client, pk packets.Packet) error {
	return fmt.Errorf("the Will could not be kept: %w", packets.ErrQuotaExceeded)
}

// A session refused with a wrapped code is answered with that code, in the
// CONNACK and to OnConnectRefused, not with 0x80: errors.As finds a code
// through a wrapper where a type assertion on the error does not.
func TestASessionRefusedWithAWrappedCodeIsAnsweredWithThatCode(t *testing.T) {
	s := New(&Options{Logger: logger})
	hook := new(refusedHook)
	require.NoError(t, s.AddHook(new(AllowHook), nil))
	require.NoError(t, s.AddHook(new(wrappingEstablishHook), nil))
	require.NoError(t, s.AddHook(hook, nil))
	defer s.Close()

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
	}()
	got := make(chan []byte)
	go func() { b, _ := io.ReadAll(w); got <- b }()

	require.ErrorIs(t, <-o, packets.ErrQuotaExceeded)
	_ = w.Close()
	buf := <-got
	require.GreaterOrEqual(t, len(buf), 4, "the refusal was answered % x, want a CONNACK", buf)
	require.Equal(t, byte(packets.Connack<<4), buf[0], "answered % x, want a CONNACK", buf)
	require.Equal(t, packets.ErrQuotaExceeded.Code, buf[3], "the CONNACK carried 0x%02X, want 0x97", buf[3])

	_, codes := hook.refusals()
	require.Equal(t, []packets.Code{packets.ErrQuotaExceeded}, codes)
}

// readRefusalHook records what OnConnectionRefused is told, and rejects every
// PUBLISH as it is read when reject is set.
type readRefusalHook struct {
	HookBase
	reject bool
	mu     sync.Mutex
	codes  []packets.Code
}

func (h *readRefusalHook) ID() string { return "read-refusal" }

func (h *readRefusalHook) Provides(b byte) bool {
	return b == OnPacketRead || b == OnConnectionRefused
}

func (h *readRefusalHook) OnPacketRead(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if h.reject && pk.FixedHeader.Type == packets.Publish {
		return pk, fmt.Errorf("%w: the store failed", packets.ErrRejectPacket)
	}
	return pk, nil
}

func (h *readRefusalHook) OnConnectionRefused(cl *Client, code packets.Code) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.codes = append(h.codes, code)
}

// A packet refused as it is decoded is answered with a DISCONNECT carrying
// its code, and OnConnectionRefused is told once. A packet a hook rejects as
// it is read is neither: ErrRejectPacket is the hook contract's own signal,
// never put on the wire, and the hook has done whatever the rejection needs.
func TestOnlyAPacketRefusedAsItIsDecodedIsAnsweredAndToldAsARefusal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reject bool
		packet []byte
		answer []byte // the DISCONNECT's first bytes, or nothing
		told   []packets.Code
	}{{
		name:   "a PUBLISH with both QoS bits set",
		packet: []byte{0x36, 0x07, 0x00, 0x01, 'a', 0x00, 0x01, 0x00, 'x'},
		answer: []byte{0xE0},
		told:   []packets.Code{packets.ErrMalformedQos},
	}, {
		name:   "a PUBLISH a hook rejects as it is read",
		reject: true,
		packet: []byte{0x30, 0x05, 0x00, 0x01, 'a', 0x00, 'x'},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			hook := &readRefusalHook{reject: tc.reject}
			require.NoError(t, s.AddHook(hook, nil))
			defer s.Close()

			r, w := net.Pipe()
			o := make(chan error)
			go func() { o <- s.EstablishConnection("tcp", r) }()
			_ = w.SetDeadline(time.Now().Add(5 * time.Second))
			_, err := w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
			require.NoError(t, err)
			br := bufio.NewReader(w)
			first, err := br.ReadByte()
			require.NoError(t, err)
			require.Equal(t, byte(packets.Connack<<4), first, "the CONNECT was answered 0x%02X", first)
			n, _, err := packets.DecodeLength(br)
			require.NoError(t, err)
			_, err = io.ReadFull(br, make([]byte, n))
			require.NoError(t, err)

			_, err = w.Write(tc.packet)
			require.NoError(t, err)
			rest, _ := io.ReadAll(br)
			<-o
			_ = w.Close()
			if tc.answer == nil {
				require.Empty(t, rest, "answered % x, want the connection closed with nothing written", rest)
			} else {
				require.GreaterOrEqual(t, len(rest), 3, "answered % x, want a DISCONNECT", rest)
				require.Equal(t, tc.answer[0], rest[0], "answered % x, want a DISCONNECT", rest)
				require.Equal(t, tc.told[0].Code, rest[2], "the DISCONNECT carried 0x%02X", rest[2])
			}
			hook.mu.Lock()
			defer hook.mu.Unlock()
			require.Equal(t, tc.told, hook.codes)
		})
	}
}

// DisconnectClient always disconnects, even for a v3 client under what was
// mochi's PassiveClientDisconnect.
//
// That option means "the client was sent a DISCONNECT and will close the
// connection itself". A v3 client is sent nothing, so honouring it there
// would leave the connection open after the server decided to end it.
// A 3.1.1 client the server chooses to disconnect is closed, even though
// it is sent no DISCONNECT packet - that direction does not exist before
// MQTT 5. mochi's PassiveClientDisconnect mode made this case do nothing
// at all: no packet and no close. The mode is gone and the assertion is
// the reason it cannot come back.
func TestDisconnectClientClosesAV3Client(t *testing.T) {
	s := newServer()
	defer s.Close()

	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 4 // 3.1.1: no server-to-client DISCONNECT
	s.Clients.Add(cl)

	go func() { _, _ = io.ReadAll(w) }()

	_ = s.DisconnectClient(cl, packets.ErrNotAuthorized)

	require.True(t, cl.Closed(), "a v3 client the server chose to disconnect was left connected")

	_ = r.Close()
	_ = w.Close()
}

// Overlapping subscriptions held by one client deliver every matching
// Subscription Identifier, in one PUBLISH carrying all of them.
//
// [MQTT-3.3.4-3] requires the server to send the identifiers of every
// subscription that caused the delivery, and [MQTT-3.3.4-4] permits them to
// travel in a single PUBLISH.
//
// **This exists because a conformance suite says it is broken and it is
// not.** `sammiq/mqtt_test` reports "Expected subscription IDs [10, 20], got
// [20]" against this broker, and the broker demonstrably writes both: the
// bytes below carry the property identifier 0x0B twice. Its client models
// the property as a single value and keeps the last one it decodes, so the
// suite cannot see a PUBLISH carrying two whatever the broker sends. The
// finding is the instrument's.
//
// The assertion is on the encoded bytes rather than on the packet struct,
// because the struct was never in doubt: the topic index held both
// identifiers all along and what was questioned is what reaches the socket.
func TestOverlappingSubscriptionsDeliverEveryIdentifier(t *testing.T) {
	s := newServer()
	cl, r, w := newTestClient()
	cl.Properties.ProtocolVersion = 5
	s.Clients.Add(cl)

	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/#", Identifier: 10})
	s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: "a/b", Identifier: 20})

	subs := s.Topics.Subscribers("a/b")
	sub, ok := subs.Subscriptions[cl.ID]
	require.True(t, ok, "the client is not among the subscribers, so nothing below is about it")
	require.Len(t, sub.Identifiers, 2, "the topic index lost an identifier before delivery")

	go func() {
		_, err := s.publishToClient(cl, sub, *packets.TPacketData[packets.Publish].Get(packets.TPublishBasicMqtt5).Packet)
		require.NoError(t, err)
	}()

	// publishToClient queues the packet and WriteLoop writes it, so closing
	// the pipe here would race that write and ReadAll would return nothing.
	// One PUBLISH is one write, so one Read behind a deadline is the whole
	// packet or a failure that says so.
	require.NoError(t, r.SetReadDeadline(time.Now().Add(2*time.Second)))
	raw := make([]byte, 512)
	n, err := r.Read(raw)
	require.NoError(t, err)
	buf := raw[:n]
	defer func() { _ = w.Close() }()

	// 0x0B is the Subscription Identifier property. Two of them, carrying
	// 10 and 20, and in ascending order because publishToClient sorts them
	// so the wire form does not depend on map iteration.
	require.Contains(t, string(buf), string([]byte{0x0B, 10}),
		"identifier 10 is not on the wire: %x", buf)
	require.Contains(t, string(buf), string([]byte{0x0B, 20}),
		"identifier 20 is not on the wire: %x", buf)
	require.Equal(t, 2, bytes.Count(buf, []byte{0x0B}),
		"expected exactly two Subscription Identifier properties: %x", buf)
}

// connackWatcher records, when a CONNACK is encoded for a client, whether the
// registry already held that client.
type connackWatcher struct {
	HookBase
	s     *Server
	found atomic.Int32 // 1 not found, 2 found
}

func (h *connackWatcher) ID() string           { return "connack-watcher" }
func (h *connackWatcher) Provides(b byte) bool { return b == OnPacketEncode }
func (h *connackWatcher) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Connack {
		if got, ok := h.s.Clients.Get(cl.ID); ok && got == cl {
			h.found.Store(2)
		} else {
			h.found.Store(1)
		}
	}
	return pk
}

// [MQTT-3.2.0-1] The first packet a server sends is the CONNACK, so nothing may
// be able to find a connection - and write to it - before its CONNACK has been
// written: a connection is registered only after it.
func TestAConnectionCannotBeFoundBeforeItsConnack(t *testing.T) {
	s := newServer()
	defer s.Close()
	watcher := &connackWatcher{s: s}
	require.NoError(t, s.AddHook(watcher, nil))

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()
	require.NoError(t, <-o)
	_ = w.Close()
	_ = r.Close()

	require.NotZero(t, watcher.found.Load(), "no CONNACK was encoded, so this proves nothing")
	require.Equal(t, int32(1), watcher.found.Load(), "the connection was registered before its CONNACK was written")
}

// A delivery aimed at a connection whose session has been taken over is made to
// the connection that took it, when that one holds the subscription, and not
// at all when it does not; a connection not taken over is delivered to as
// ever.
func TestADeliveryFollowsItsSessionToTheConnectionThatTookIt(t *testing.T) {
	sub := packets.Subscription{Filter: "a/b/c", Qos: 1}
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet

	t.Run("followed", func(t *testing.T) {
		s := newServer()
		old, _, _ := newTestClient()
		taker, _, _ := newTestClient()
		taker.State.Subscriptions.Add(sub.Filter, sub)
		s.Clients.Add(taker)
		old.State.isTakenOver.Store(true)
		_, err := s.publishToClient(old, sub, publish)
		require.NoError(t, err)
		require.Equal(t, 0, old.State.Inflight.Len(), "a delivery was recorded on a connection whose session had moved")
		require.Equal(t, 1, taker.State.Inflight.Len(), "the connection that took the session was not delivered to")
	})
	t.Run("session ended", func(t *testing.T) {
		s := newServer()
		old, _, _ := newTestClient()
		taker, _, _ := newTestClient()
		s.Clients.Add(taker)
		old.State.isTakenOver.Store(true)
		_, _ = s.publishToClient(old, sub, publish)
		require.Equal(t, 0, old.State.Inflight.Len())
		require.Equal(t, 0, taker.State.Inflight.Len(), "a clean start was delivered what the ended session subscribed to")
	})
	t.Run("not taken over", func(t *testing.T) {
		s := newServer()
		cl, _, _ := newTestClient()
		s.Clients.Add(cl)
		_, err := s.publishToClient(cl, sub, publish)
		require.NoError(t, err)
		require.Equal(t, 1, cl.State.Inflight.Len())
	})
}

// A takeover waits for a delivery being recorded on the old connection, so the
// message is in what the takeover copies to the new one rather than left on
// the old one after the copy.
func TestATakeoverWaitsForADeliveryBeingRecorded(t *testing.T) {
	s := newServer()
	old, oldPeer, _ := newTestClient()
	old.Properties.ProtocolVersion = 5
	old.Properties.Props.SessionExpiryInterval = 60 // a session a takeover inherits
	s.Clients.Add(old)
	taker, takerPeer, _ := newTestClient()
	// Read both far ends: the takeover writes the old connection a DISCONNECT,
	// and a pipe nobody reads would hold it there rather than at the lock.
	go func() { _, _ = io.Copy(io.Discard, oldPeer) }()
	go func() { _, _ = io.Copy(io.Discard, takerPeer) }()

	target, ok := s.deliveryTarget(old, packets.Subscription{Filter: "a/b/c", Qos: 1})
	require.True(t, ok)
	require.Same(t, old, target)

	done := make(chan bool)
	go func() {
		done <- s.inheritClientSession(packets.Packet{Connect: &packets.ConnectParams{ClientIdentifier: "mochi"}}, taker, time.Now())
	}()
	select {
	case <-done:
		t.Fatal("the takeover finished while a delivery was still being recorded on the old connection")
	case <-time.After(100 * time.Millisecond):
	}

	m := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	m.PacketID = 7
	old.State.Inflight.Set(m)
	target.State.handover.RUnlock()

	select {
	case present := <-done:
		require.True(t, present)
	case <-time.After(3 * time.Second):
		t.Fatal("the takeover did not finish once the delivery was recorded")
	}
	_, ok = taker.State.Inflight.Get(7)
	require.True(t, ok, "the message recorded before the takeover's copy is not on the connection that took the session")
}

// qosCompleteCounter counts OnQosComplete, the hook an acknowledgement on a
// connection whose session has moved on must not reach.
type qosCompleteCounter struct {
	HookBase
	n atomic.Int32
}

func (h *qosCompleteCounter) ID() string                                  { return "qos-complete-counter" }
func (h *qosCompleteCounter) Provides(b byte) bool                        { return b == OnQosComplete }
func (h *qosCompleteCounter) OnQosComplete(cl *Client, pk packets.Packet) { h.n.Add(1) }

// An acknowledgement on a connection whose session another connection has
// taken over changes nothing: the packet stays in the table the takeover
// copied, and no hook hears of it. The new connection is re-sent the packet
// and its own acknowledgement is the one that counts - RFC 0003 "Sessions"
// rule 1, and the guard deliveryTarget keeps for deliveries. Acted on here, a
// queue job's PUBACK was taken on the old connection while the copy re-sent
// the job on the new one, and the new one ending first returned the job with
// its attempt uncounted.
func TestAnAcknowledgementOnATakenOverConnectionChangesNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		held   packets.Packet
		ack    packets.Packet
		after  byte // the entry's type once a live connection has answered, 0 for gone
		counts int32
	}{
		{"PUBACK", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 7},
			*packets.TPacketData[packets.Puback].Get(packets.TPuback).Packet, 0, 1},
		{"PUBREC", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}, PacketID: 7},
			*packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).Packet, packets.Pubrel, 0},
		{"PUBCOMP", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: 7},
			*packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp).Packet, 0, 1},
	} {
		for _, takenOver := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s taken over %v", tc.name, takenOver), func(t *testing.T) {
				s := newServer()
				counter := new(qosCompleteCounter)
				require.NoError(t, s.AddHook(counter, nil))
				cl, peer, _ := newTestClient()
				go func() { _, _ = io.Copy(io.Discard, peer) }()
				cl.State.Inflight.Set(tc.held)
				cl.State.isTakenOver.Store(takenOver)

				require.NoError(t, s.processPacket(cl, tc.ack))

				got, ok := cl.State.Inflight.Get(7)
				if takenOver {
					require.True(t, ok, "%s on a taken-over connection took the packet out of the table the takeover copied", tc.name)
					require.Equal(t, tc.held.FixedHeader.Type, got.FixedHeader.Type,
						"%s on a taken-over connection changed the packet the takeover copied", tc.name)
					require.Equal(t, int32(0), counter.n.Load(), "%s on a taken-over connection reached OnQosComplete", tc.name)
					return
				}
				// The control: a live connection's acknowledgement does move it,
				// so the refusal above is the guard and not a packet that could
				// never have moved anything.
				if tc.after == 0 {
					require.False(t, ok, "%s on a live connection left the packet in flight", tc.name)
				} else {
					require.True(t, ok)
					require.Equal(t, tc.after, got.FixedHeader.Type)
				}
				require.Equal(t, tc.counts, counter.n.Load())
			})
		}
	}
}

// A takeover waits for an acknowledgement being retired on the old
// connection, so the packet it retired is not in what the takeover copies and
// is not sent again on the new connection. The other order - the takeover
// first - is the test above.
func TestATakeoverWaitsForAnAcknowledgementBeingRetired(t *testing.T) {
	s := newServer()
	old, oldPeer, _ := newTestClient()
	old.Properties.ProtocolVersion = 5
	old.Properties.Props.SessionExpiryInterval = 60 // a session a takeover inherits
	s.Clients.Add(old)
	taker, takerPeer, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, oldPeer) }()
	go func() { _, _ = io.Copy(io.Discard, takerPeer) }()

	m := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	m.PacketID = 7
	old.State.Inflight.Set(m)

	// As processPuback holds it, between reading the PUBACK and retiring.
	require.True(t, ownsInflight(old))

	done := make(chan bool)
	go func() {
		done <- s.inheritClientSession(packets.Packet{Connect: &packets.ConnectParams{ClientIdentifier: "mochi"}}, taker, time.Now())
	}()
	select {
	case <-done:
		t.Fatal("the takeover finished while an acknowledgement was still being retired on the old connection")
	case <-time.After(100 * time.Millisecond):
	}

	require.True(t, old.State.Inflight.Retire(7))
	old.State.handover.RUnlock()

	select {
	case present := <-done:
		require.True(t, present)
	case <-time.After(3 * time.Second):
		t.Fatal("the takeover did not finish once the acknowledgement was retired")
	}
	_, ok := taker.State.Inflight.Get(7)
	require.False(t, ok, "the takeover copied a packet the old connection had already retired, so the new connection would be sent it again")
}

// ackKeeper records, for each delivery it is told has left flight, whether
// it arrived marked Acknowledged.
type ackKeeper struct {
	HookBase
	mu    sync.Mutex
	acked map[uint16]bool
}

func (h *ackKeeper) ID() string                                             { return "ack-keeper" }
func (h *ackKeeper) Provides(b byte) bool                                   { return b == OnDeliveryDone }
func (h *ackKeeper) OnDeliveryReleased(cl *Client, pk packets.Packet) error { return nil }
func (h *ackKeeper) OnDeliveryDone(cl *Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.acked[pk.PacketID] = pk.Acknowledged
}

// What a client's refusing PUBREC retired reaches the keepers marked
// Acknowledged, and what a takeover's clear retires does not.
//
// The keepers run after ownsInflight lets its lock go, so by then the
// connection may have been taken over - and a keeper that asked IsTakenOver
// kept the entry of a delivery the client had answered, which a restart then
// sent again. The mark is the decision made under the lock. A takeover's
// clear is the other side: what it drops the copy carried and the new
// connection still owes, so it must not be marked. A PUBACK and a PUBCOMP
// reach no keeper at all (TestAFinishedDeliverysIdentifierIsFreedOnceItsHooksHaveRun).
func TestTheKeepersAreToldWhatAnAcknowledgementRetired(t *testing.T) {
	refused := *packets.TPacketData[packets.Pubrec].Get(packets.TPubrec).Packet
	refused.ReasonCode = packets.ErrUnspecifiedError.Code
	for _, tc := range []struct {
		name  string
		held  packets.Packet
		ack   *packets.Packet
		acked bool
	}{
		{"PUBREC refusing", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}, PacketID: 7},
			&refused, true},
		{"a takeover's clear", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: 7},
			nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			keeper := &ackKeeper{acked: map[uint16]bool{}}
			require.NoError(t, s.AddHook(keeper, nil))
			cl, peer, _ := newTestClient()
			go func() { _, _ = io.Copy(io.Discard, peer) }()
			require.NoError(t, cl.ops.hooks.Add(keeper, nil))
			cl.State.Inflight.Set(tc.held)

			if tc.ack != nil {
				require.NoError(t, s.processPacket(cl, *tc.ack))
			} else {
				cl.ClearInflights()
			}

			keeper.mu.Lock()
			acked, told := keeper.acked[7]
			keeper.mu.Unlock()
			require.True(t, told, "the keepers were not told the delivery left flight, so this proves nothing")
			require.Equal(t, tc.acked, acked, "%s: the keepers were told Acknowledged=%v", tc.name, acked)
		})
	}
}

// wakeMidDrain signals the writer from inside the writer's own drain, which is
// the moment a signal can be lost.
type wakeMidDrain struct {
	HookBase
	once sync.Once
	arm  func(cl *Client)
}

func (h *wakeMidDrain) ID() string           { return "wake-mid-drain" }
func (h *wakeMidDrain) Provides(b byte) bool { return b == OnPacketSent }

// OnPacketSent runs inside the write, so inside the drain that made it.
func (h *wakeMidDrain) OnPacketSent(cl *Client, pk packets.Packet, b []byte) {
	h.once.Do(func() { h.arm(cl) })
}

// A delivery withheld while the write loop is inside its drain is written: the
// signal raised for it arrives while the loop is busy, and one the loop has
// not yet looked at is kept rather than dropped.
func TestAWakeArrivingMidDrainIsNotLost(t *testing.T) {
	first := packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet.Copy(false)
	first.PacketID, first.Expiry = 1, -1
	second := first.Copy(false)
	second.PacketID, second.Expiry = 2, -1

	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ops.options = s.Options
	cl.State.Inflight.ResetSendQuota(10)
	require.NoError(t, cl.ops.hooks.Add(&wakeMidDrain{arm: func(cl *Client) {
		cl.State.Inflight.Set(second)
		cl.WakeWriter()
	}}, nil))

	cl.State.Inflight.Set(first)
	cl.WakeWriter()

	require.Eventually(t, func() bool {
		m, ok := cl.State.Inflight.Get(second.PacketID)
		return ok && m.Expiry >= 0
	}, 2*time.Second, time.Millisecond,
		"a delivery withheld while the writer was draining was never written: its signal was lost")
}

// TestASessionEndsWhenItsIntervalHasPassedAndNotBefore is the arithmetic
// an earlier version got wrong in one direction and this rule must not get wrong in the
// other. It held the moment and the clock in whole seconds and compared them
// inclusively, so a session was judged expired at the first tick whose second
// number was far enough along - which for a client that disconnected late in
// its second meant as little as 1.001 seconds of a two-second session, taking
// its subscriptions and queued messages with it while its client was still
// inside its own interval.
//
// The row at 1.892 is not invented: it is a conformance run, where a Will
// held against a two-second expiry was published 1.892 seconds after the
// disconnect because the session had already been declared over.
func TestASessionEndsWhenItsIntervalHasPassedAndNotBefore(t *testing.T) {
	s := newServer()
	defer s.Close()

	// Late in its second, which is what made the truncated form fail: the
	// stored moment rounded down nearly a whole second, and the clock it was
	// compared against rounded down too.
	away := time.Date(2026, 9, 17, 19, 36, 5, 974_000_000, time.UTC)

	for _, c := range []struct {
		after time.Duration
		want  bool
		why   string
	}{
		{1892 * time.Millisecond, false, "the conformance case: a Will fired here, and its session had not ended"},
		{1999 * time.Millisecond, false, "one millisecond short of the interval is inside it"},
		{2 * time.Second, true, "the interval has passed at exactly the interval, not a second later"},
		{2001 * time.Millisecond, true, "past the interval"},
	} {
		cl, _, _ := newTestClient()
		cl.ID = "device"
		cl.Properties.ProtocolVersion = 5
		cl.Properties.Props.SessionExpiryIntervalFlag = true
		cl.Properties.Props.SessionExpiryInterval = 2
		cl.State.disconnected.Store(away.UnixNano())

		if got := s.sessionExpired(cl, away.Add(c.after)); got != c.want {
			t.Errorf("a two-second session %v after its client went is expired=%v, want %v: %s",
				c.after, got, c.want, c.why)
		}
	}
}

// TestTheEngineAndTheStoreEndASessionAtTheSameMoment holds the two copies of
// this arithmetic together. The running broker decides a session it holds in
// memory and the store decides one across a restart, and a session that ends
// at 2.000 for one has to end at 2.000 for the other: a client whose session
// survived a restart and then died a second early to the sweep would have
// lost it to the seam between them.
func TestTheEngineAndTheStoreEndASessionAtTheSameMoment(t *testing.T) {
	s := newServer()
	defer s.Close()

	away := time.Date(2026, 9, 17, 19, 36, 5, 974_000_000, time.UTC)
	for _, after := range []time.Duration{
		0, time.Second, 1892 * time.Millisecond, 1999 * time.Millisecond,
		2 * time.Second, 2001 * time.Millisecond, time.Minute,
	} {
		cl, _, _ := newTestClient()
		cl.ID = "device"
		cl.Properties.ProtocolVersion = 5
		cl.Properties.Props.SessionExpiryIntervalFlag = true
		cl.Properties.Props.SessionExpiryInterval = 2
		cl.State.disconnected.Store(away.UnixNano())

		engine := s.sessionExpired(cl, away.Add(after))
		stored := store.SessionExpired(store.Session{
			Client: "device", ExpiryInterval: 2, DisconnectedAt: away,
		}, away.Add(after))
		if engine != stored {
			t.Errorf("%v after the disconnect the engine says expired=%v and the store says %v",
				after, engine, stored)
		}
	}
}

// orderHook records the order the session hooks fire in.
type orderHook struct {
	HookBase
	mu    sync.Mutex
	calls []string
}

func (h *orderHook) ID() string { return "order" }
func (h *orderHook) Provides(b byte) bool {
	return b == OnClientExpired || b == OnSessionEstablish
}
func (h *orderHook) OnClientExpired(cl *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "expired "+cl.ID)
}
func (h *orderHook) OnSessionEstablish(cl *Client, pk packets.Packet) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, "establish "+cl.ID)
	return nil
}

// A session whose expiry has passed, found as its client comes back, is ended
// before the new connection claims its id: a hook that records the id's
// owner at OnSessionEstablish would otherwise see the expired session's end
// arrive after the new connection had claimed it, and end nothing.
func TestAnExpiredSessionIsEndedBeforeItsIDIsClaimed(t *testing.T) {
	s := newServer()
	defer s.Close()
	h := new(orderHook)
	require.NoError(t, s.AddHook(h, nil))

	id := packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).Packet.Connect.ClientIdentifier
	old, _, _ := newTestClient()
	old.ID = id
	old.State.disconnected.Store(time.Now().Add(-10 * time.Second).UnixNano())
	old.State.cancelOpen()
	old.Properties.ProtocolVersion = 5
	old.Properties.Props.SessionExpiryInterval = 1
	old.Properties.Props.SessionExpiryIntervalFlag = true
	s.Clients.Add(old)

	r, w := net.Pipe()
	o := make(chan error)
	go func() { o <- s.EstablishConnection("tcp", r) }()
	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt5).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()
	go func() { _, _ = io.ReadAll(w) }()
	require.NoError(t, <-o)

	h.mu.Lock()
	defer h.mu.Unlock()
	require.Equal(t, []string{"expired " + id, "establish " + id}, h.calls,
		"the expired session was ended after the new connection claimed its id")
}

// reservedID reports whether an identifier is reserved in a table: taken out
// of flight and not yet free for a new delivery.
func reservedID(i *Inflight, id uint16) bool {
	i.RLock()
	defer i.RUnlock()
	_, ok := i.reserved[id]
	return ok
}

// finishWatch records, at OnQosComplete, whether the finished delivery's
// identifier is still reserved, and counts every OnDeliveryDone.
type finishWatch struct {
	HookBase
	mu                sync.Mutex
	id                uint16
	completed         int
	reservedAtFinish  bool
	deliveryDoneCalls int
}

func (h *finishWatch) ID() string { return "finish-watch" }
func (h *finishWatch) Provides(b byte) bool {
	return b == OnQosComplete || b == OnDeliveryDone || b == OnDeliveryReleased
}
func (h *finishWatch) OnQosComplete(cl *Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.completed++
	h.reservedAtFinish = reservedID(cl.State.Inflight, h.id)
}
func (h *finishWatch) OnDeliveryReleased(cl *Client, pk packets.Packet) error { return nil }
func (h *finishWatch) OnDeliveryDone(cl *Client, pk packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deliveryDoneCalls++
}

// A delivery the session has finished with - acknowledged by a PUBACK or a
// PUBCOMP, discarded as too large as if sent, or given up at the session's
// bound - has its packet identifier freed by the engine once the hooks that
// clear what is keyed by it have run, and not before: OnQosComplete finds it
// still reserved, and when the call returns it is free. None of them reaches
// OnDeliveryDone, which is for a delivery that ended without being finished:
// the broadcast log's drain takes an acknowledgement from OnQosComplete, and
// told again there it would let the entry go twice. An identifier never freed
// is one fewer the session can give out until it has none (MQTT-2.2.1-3).
func TestAFinishedDeliverysIdentifierIsFreedOnceItsHooksHaveRun(t *testing.T) {
	const id = 7
	publish := func(expiry int64) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}, PacketID: id,
			TopicName: "a/b", Payload: make([]byte, 64), Origin: "someone", Expiry: expiry}
	}
	for _, tc := range []struct {
		name      string
		held      packets.Packet
		finish    func(s *Server, cl *Client)
		completes bool // whether the finish is an acknowledgement's, which OnQosComplete sees
	}{
		{"PUBACK", publish(0), func(s *Server, cl *Client) {
			require.NoError(t, s.processPacket(cl, *packets.TPacketData[packets.Puback].Get(packets.TPuback).Packet))
		}, true},
		{"PUBCOMP", packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pubrel, Qos: 1}, PacketID: id},
			func(s *Server, cl *Client) {
				require.NoError(t, s.processPacket(cl, *packets.TPacketData[packets.Pubcomp].Get(packets.TPubcomp).Packet))
			}, true},
		{"discarded as too large", publish(0), func(s *Server, cl *Client) {
			cl.discardTooLarge(publish(0))
		}, true},
		{"given up at the bound", publish(-1), func(s *Server, cl *Client) {
			kept := publish(-1)
			kept.PacketID = id + 1
			cl.State.Inflight.Set(kept)
			s.Options.ClientSessionQueueBytes = inflightSize(kept)
			s.boundSession(cl, kept.PacketID)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			watch := &finishWatch{id: id}
			require.NoError(t, s.AddHook(watch, nil))
			cl, peer, _ := newTestClient()
			go func() { _, _ = io.Copy(io.Discard, peer) }()
			cl.ops.hooks = s.hooks
			cl.ops.options = s.Options
			cl.State.Inflight.ResetSendQuota(10)
			require.Equal(t, uint16(id), packets.TPacketData[packets.Puback].Get(packets.TPuback).Packet.PacketID,
				"the acknowledgement names another identifier, so this proves nothing")
			cl.State.Inflight.Set(tc.held)

			tc.finish(s, cl)

			_, inFlight := cl.State.Inflight.Get(id)
			require.False(t, inFlight, "the delivery is still in flight, so this proves nothing")
			watch.mu.Lock()
			defer watch.mu.Unlock()
			if tc.completes {
				require.Equal(t, 1, watch.completed, "OnQosComplete ran %d times, want once", watch.completed)
				require.True(t, watch.reservedAtFinish,
					"the identifier was free while OnQosComplete ran: a delivery claiming it then would have "+
						"the clearing meant for the old one land on it")
			}
			require.False(t, reservedID(cl.State.Inflight, id),
				"the identifier is still reserved once the finish returned: the session would run out of them")
			require.Zero(t, watch.deliveryDoneCalls,
				"a finished delivery reached OnDeliveryDone, which the drain would take as a second end")
		})
	}
}

// completionRows keeps, by packet identifier, what each delivery to a client
// carries - as the broker keys what it tracks of one - from OnQosPublish, and
// clears it at OnQosComplete, where it can hold its first clearing open.
type completionRows struct {
	HookBase
	mu      sync.Mutex
	rows    map[uint16]string
	gateOn  uint16
	once    sync.Once
	entered chan struct{}
	gate    chan struct{}
}

func (h *completionRows) ID() string { return "completion-rows" }
func (h *completionRows) Provides(b byte) bool {
	return b == OnQosPublish || b == OnQosComplete
}
func (h *completionRows) OnQosPublish(cl *Client, pk packets.Packet, sent int64, resends int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rows[pk.PacketID] = string(pk.Payload)
}
func (h *completionRows) OnQosComplete(cl *Client, pk packets.Packet) {
	if pk.PacketID == h.gateOn {
		held := false
		h.once.Do(func() { held = true })
		if held {
			close(h.entered)
			<-h.gate
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.rows, pk.PacketID)
}

// A packet identifier is not given to a new delivery while what is keyed by
// it for the delivery that last held it is still being cleared. A hook keys
// what it tracks of a delivery by client and identifier - the broadcast log's
// entry on the wire, a channel delivery's offset - and clears it at
// OnQosComplete, so a new delivery that took the identifier in that window
// would have the clearing meant for the old one land on it.
//
// Reached here by making the identifier space three wide and the counter sit
// at its wrap, which is what 65,535 deliveries to one client do while an
// acknowledgement's hooks are still running.
func TestAnIdentifierIsNotReusedWhileItsCompletionRuns(t *testing.T) {
	publish := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	with := func(payload string) packets.Packet {
		p := publish.Copy(false)
		p.Payload = []byte(payload)
		return p
	}
	s := newServer()
	s.Options.Capabilities.maximumPacketID = 3
	rows := &completionRows{rows: map[uint16]string{}, gateOn: 1,
		entered: make(chan struct{}), gate: make(chan struct{})}
	require.NoError(t, s.AddHook(rows, nil))
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.ID = "subscriber"
	cl.ops.options = s.Options
	cl.ops.hooks = s.hooks
	cl.State.Inflight.ResetSendQuota(10)
	s.Clients.Add(cl)
	require.True(t, s.Topics.Subscribe(cl.ID, packets.Subscription{Filter: publish.TopicName, Qos: 1}))

	s.publishToSubscribers(with("old"))
	s.publishToSubscribers(with("other"))
	rows.mu.Lock()
	first, second := rows.rows[1], rows.rows[2]
	rows.mu.Unlock()
	require.Equal(t, "old", first, "the first delivery did not take identifier 1, so this proves nothing")
	require.Equal(t, "other", second, "the second delivery did not take identifier 2, so this proves nothing")
	// The counter has come round to the wrap: the next identifier Claim looks
	// at is 1, which the acknowledgement below is about to free.
	atomic.StoreUint32(&cl.State.packetID, 3)

	acked := make(chan struct{})
	go func() {
		defer close(acked)
		_ = s.processPuback(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Puback}, PacketID: 1})
	}()
	<-rows.entered // the old delivery's completion is still clearing
	s.publishToSubscribers(with("new"))
	close(rows.gate)
	<-acked

	rows.mu.Lock()
	defer rows.mu.Unlock()
	found := false
	for _, p := range rows.rows {
		found = found || p == "new"
	}
	if !found {
		var inflight []uint16
		for _, m := range cl.State.Inflight.GetAll(false) {
			inflight = append(inflight, m.PacketID)
		}
		t.Fatalf("the old delivery's completion cleared the new delivery's row: it took the identifier "+
			"while the old one's hooks ran.\n\tin flight: %v\n\trows: %v", inflight, rows.rows)
	}
}

// An upgraded session on the wss door holds one max_connections slot, as on
// the ws door: the slot its socket took at accept is found through TLS, so
// the server does not take a second. Without the unwrap a wss session held
// two, and a tcp CONNECT beside one was refused at a limit of two, with every
// test green.
func TestAWssSessionHoldsOneSlot(t *testing.T) {
	tcpAddr, addr := wsServer(t, 2, time.Second, selfSigned(t))
	wc, err := dialWSS(addr, 0, 0)
	require.NoError(t, err)
	defer wc.Close()
	require.Equal(t, byte(0x00), wsConnack(wc, "wss-a"))

	c1, code := dialAndConnect(t, tcpAddr, takeoverConnect("tcp-a"))
	defer c1.Close()
	require.Equal(t, byte(0x00), code, "a wss session held two slots")
	c2, code := dialAndConnect(t, tcpAddr, takeoverConnect("tcp-b"))
	defer c2.Close()
	require.Equal(t, packets.ErrServerBusy.Code, code, "the wss session held no slot")
}

// A request on the ws door that is not an upgrade ends its socket - and the
// max_connections slot it holds - once it is answered, rather than keeping
// both for HTTP keep-alive. With keep-alive on, a plain GET held its socket
// for net/http's 60 s where it was closed in 0.045 s, and no test failed. Held past a second here it is
// being kept, since the bound it would otherwise wait for is ten.
func TestAWebsocketDoorKeepsNoIdleRequest(t *testing.T) {
	_, addr := wsServer(t, 10, 10*time.Second, nil)
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer c.Close()
	began := time.Now()
	_, err = c.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	require.NoError(t, err)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	answer, _ := io.ReadAll(c)
	held := time.Since(began)
	require.Contains(t, string(answer), "HTTP/1.1 ", "the request was not answered")
	require.Less(t, held, time.Second, "a request that was not an upgrade kept its socket %s after "+
		"its answer", held)
}

// MQTT-3.3.4-9: the server never has more deliveries in flight than the
// client's Receive Maximum. A QoS 2 delivery that expires in flight gives its
// send slot back at the sweep; the client's late PUBREC finds nothing and is
// answered PUBREL 0x92, and the client's PUBCOMP to that PUBREL must not give
// the slot back a second time. It did, because processPubcomp restored both
// quotas whatever it found - which only cancelled out while the sweep leaked
// the slot instead. Kept as a probe.
func TestALatePubcompAfterAnExpiryDoesNotGiveASlotBackTwice(t *testing.T) {
	s := newServer()
	cl, r, _ := newTestClient()
	go func() { _, _ = io.Copy(io.Discard, r) }()
	cl.State.Inflight.ResetSendQuota(4)
	n := time.Now().Unix()
	expiring := packets.Packet{ProtocolVersion: 5, PacketID: 1, Expiry: n - 1, Created: n - 5,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 2}}
	cl.State.Inflight.Set(expiring)
	cl.State.Inflight.DecreaseSendQuota()
	// Queued and never written: a QoS 2 delivery once sent does not expire
	// (Inflight.neverSent), so the slot it took comes back only this way.
	cl.State.Inflight.Queue(&expiring)
	cl.State.Inflight.Set(packets.Packet{ProtocolVersion: 5, PacketID: 3, Expiry: n + 3600, Created: n,
		FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: 1}})
	cl.State.Inflight.DecreaseSendQuota()
	require.Equal(t, int32(2), cl.State.Inflight.SendQuota())

	require.Equal(t, []uint16{1}, cl.ClearExpiredInflights(n, 0), "the sweep did not retire the expired delivery, so this proves nothing")
	require.Equal(t, int32(3), cl.State.Inflight.SendQuota(), "after the sweep")

	require.NoError(t, s.processPubrec(cl, packets.Packet{ProtocolVersion: 5, PacketID: 1,
		FixedHeader: packets.FixedHeader{Type: packets.Pubrec}}))
	_, still := cl.State.Inflight.Get(1)
	require.False(t, still, "the late PUBREC found an entry, so this is not the path under test")
	require.NoError(t, s.processPubcomp(cl, packets.Packet{ProtocolVersion: 5, PacketID: 1,
		FixedHeader: packets.FixedHeader{Type: packets.Pubcomp}}))

	require.Equal(t, int32(3), cl.State.Inflight.SendQuota(),
		"one delivery is in flight at a Receive Maximum of 4, so 3 slots are free")
}

// **An idle connection's goroutines hold at most 14 KB of stack between
// them once the GC has run**, in the head-to-head's idle shape (a
// persistent session with one QoS 1 subscription) and bare.
//
// The reader parks in the netpoller under every frame between the
// listener's goroutine and the socket, and the GC halves a stack only while
// under a quarter of it is in use (runtime.shrinkstack, counting 800 bytes
// of guard). With the CONNECT's handling and the packet's in the loop's own
// frames, 5,960 bytes stayed live and every reader kept the 16 KB it grew
// to on the CONNECT: 19 KB of stack a connection with its writer, against a
// 15 KB target for the whole connection. 14 KB is an 8 KB reader, a writer
// of up to 4 KB, and room for the race detector: under -race the first
// connections of a run measured up to 12.5 KB, the rest 8-9.5.
func TestAnIdleConnectionHoldsLittleStack(t *testing.T) {
	for _, shape := range []struct {
		name      string
		clean     bool
		expiry    uint32
		subscribe bool
	}{
		{"bare", true, 0, false},
		{"persistent and subscribed", false, 3600, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			const n = 400
			s, addr := serveEngine(t)
			defer s.Close()
			stacks0, gs0 := stackBytes()
			for i := range n {
				w := dialWire(t, addr, fmt.Sprintf("idle-%d", i), shape.clean, shape.expiry, 0, 0)
				if shape.subscribe {
					w.subscribe(1, fmt.Sprintf("idle/%d", i), 1)
				}
			}
			waitFor(t, func() bool { return s.Clients.Len() == n }, "every connection registered")
			stacks, gs := stackBytes()

			// The instrument saw the connections: a goroutine each at least,
			// and at least the smallest stack each.
			require.GreaterOrEqual(t, int64(gs)-int64(gs0), int64(n), "goroutines: %d before, %d with %d connections", gs0, gs, n)
			grew := int64(stacks) - int64(stacks0)
			require.GreaterOrEqual(t, grew, int64(n)*2048, "stack bytes: %d before, %d with %d connections", stacks0, stacks, n)
			per := grew / n
			t.Logf("%d connections: %d stack bytes a connection, %d goroutines", n, per, int64(gs)-int64(gs0))
			require.LessOrEqual(t, per, int64(14*1024), "stack bytes a connection")
		})
	}
}

// stackBytes answers the stack memory and the goroutines after enough GCs
// for a stack to be halved twice.
func stackBytes() (stacks, goroutines uint64) {
	for range 4 {
		runtime.GC()
	}
	ss := []metrics.Sample{{Name: "/memory/classes/heap/stacks:bytes"}, {Name: "/sched/goroutines:goroutines"}}
	metrics.Read(ss)
	return ss[0].Value.Uint64(), ss[1].Value.Uint64()
}

// allocatedPerRun is the heap bytes one call of f allocates, averaged over
// runs. Bytes rather than testing.AllocsPerRun's count, because a whole-table
// copy is a handful of large allocations: its count barely moves with the
// table, its size is the table.
func allocatedPerRun(runs int, f func()) uint64 {
	f()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// The one-second sweeps run on every broker, idle or not, and each once
// copied the whole client table (or the whole retained store) to find, on an
// idle broker, nothing: garbage proportional to the number of sessions, per
// second, which is what the collector then holds memory for. A sweep that
// finds nothing allocates nothing that depends on the count.
func TestTheExpirySweepsAllocateNothingProportionalToTheClientCount(t *testing.T) {
	const runs = 20
	const bound = 1024 // bytes a tick may allocate at ten thousand clients
	n := time.Now()
	s := New(nil)
	s.Options.Capabilities.MaximumMessageExpiryInterval = 3600

	add := func(from, to int) {
		for i := from; i < to; i++ {
			cl, _, _ := newTestClient()
			cl.ID = fmt.Sprintf("c%d", i)
			cl.ops.info = s.Info
			// Connected: no stop time, so nothing is expired.
			s.Clients.Add(cl)
			s.Topics.Retained.Add(fmt.Sprintf("r/%d", i), packets.Packet{
				FixedHeader: packets.FixedHeader{Type: packets.Publish, Retain: true},
				TopicName:   fmt.Sprintf("r/%d", i), Payload: []byte("x"), Created: n.Unix(),
			})
		}
	}
	sweeps := map[string]func(){
		"clearExpiredClients":          func() { s.clearExpiredClients(n) },
		"clearExpiredInflights":        func() { s.clearExpiredInflights(n.Unix()) },
		"clearExpiredRetainedMessages": func() { s.clearExpiredRetainedMessages(n.Unix()) },
	}

	for _, size := range []int{1000, 10000} {
		add(s.Clients.Len(), size)
		require.Equal(t, size, s.Clients.Len())
		require.Equal(t, size, s.Topics.Retained.Len())
		// The meter has to see a copy when there is one, or a zero proves
		// nothing (a control: the copy the sweeps used to make).
		control := allocatedPerRun(runs, func() { _ = s.Clients.GetAll() })
		require.Greater(t, control, uint64(size)*8, "the meter did not see a %d-client copy", size)
		for name, sweep := range sweeps {
			got := allocatedPerRun(runs, sweep)
			t.Logf("%s at %d clients: %d bytes a tick (a copy of the table: %d)", name, size, got, control)
			require.LessOrEqual(t, got, uint64(bound), "%s allocates in proportion to %d clients", name, size)
		}
	}
}

// retainedIndexConsistent reports every node that claims a retained value the
// store does not hold, and every node kept alive for nothing.
func retainedIndexConsistent(t *testing.T, x *TopicsIndex) (nodes int) {
	var walk func(n *particle)
	walk = func(n *particle) {
		for _, c := range n.particles.getAll() {
			nodes++
			if c.retainPath != "" {
				_, ok := x.Retained.Get(c.retainPath)
				require.True(t, ok, "node %q has retainPath %q but the store holds nothing", c.key, c.retainPath)
			}
			walk(c)
		}
	}
	walk(x.root)
	return nodes
}

// A retained value published again between the sweep judging the old one
// expired and the sweep deleting it must survive (MQTT-3.3.1-5: the newest
// retained message on a topic is the one kept).
func TestServerClearExpiredRetainedRepublishedMidSweep(t *testing.T) {
	s := New(nil)
	n := time.Now().Unix()
	old := packets.Packet{ProtocolVersion: 5, FixedHeader: packets.FixedHeader{Retain: true},
		TopicName: "a/b", Payload: []byte("old"), Created: n - 10, Expiry: n - 1}
	fresh := packets.Packet{ProtocolVersion: 5, FixedHeader: packets.FixedHeader{Retain: true},
		TopicName: "a/b", Payload: []byte("fresh"), Created: n, Expiry: n + 100}
	require.Equal(t, int64(1), s.Topics.RetainMessage(old))

	calls := 0
	retainedSweepJudged = func(topic string) {
		calls++
		require.Equal(t, "a/b", topic)
		require.Equal(t, int64(1), s.Topics.RetainMessage(fresh))
	}
	defer func() { retainedSweepJudged = nil }()

	s.clearExpiredRetainedMessages(n)
	require.Equal(t, 1, calls, "the seam must have run, or nothing was exercised")

	got, ok := s.Topics.Retained.Get("a/b")
	require.True(t, ok, "the fresh retained value was deleted by the sweep")
	require.Equal(t, "fresh", string(got.Payload))
	msgs := s.Topics.Messages("a/+")
	require.Len(t, msgs, 1)
	require.Equal(t, "fresh", string(msgs[0].Payload))
	require.Equal(t, 2, retainedIndexConsistent(t, s.Topics))
}

// A value the sweep does remove leaves no node behind claiming it.
func TestServerClearExpiredRetainedLeavesIndexClean(t *testing.T) {
	s := New(nil)
	n := time.Now().Unix()
	require.Equal(t, int64(1), s.Topics.RetainMessage(packets.Packet{ProtocolVersion: 5,
		FixedHeader: packets.FixedHeader{Retain: true}, TopicName: "a/b/c",
		Payload: []byte("old"), Created: n - 10, Expiry: n - 1}))
	require.Equal(t, 3, retainedIndexConsistent(t, s.Topics))

	s.clearExpiredRetainedMessages(n)

	require.Equal(t, 0, s.Topics.Retained.Len())
	require.Equal(t, 0, retainedIndexConsistent(t, s.Topics), "nodes left for a value that is gone")
	require.Empty(t, s.Topics.Messages("#"))
}

// heldDisconnecting holds a client's DISCONNECT in OnDisconnecting, where a
// store records what it changed, until release is closed.
type heldDisconnecting struct {
	HookBase
	in      chan struct{}
	release chan struct{}
}

func (h *heldDisconnecting) ID() string { return "held-disconnecting" }

func (h *heldDisconnecting) Provides(b byte) bool {
	return b == OnDisconnecting || b == OnConnectAuthenticate || b == OnACLCheck
}

func (h *heldDisconnecting) OnConnectAuthenticate(*Client, packets.Packet) bool { return true }
func (h *heldDisconnecting) OnACLCheck(*Client, string, bool) bool              { return true }

func (h *heldDisconnecting) OnDisconnecting(*Client, packets.Packet) {
	close(h.in)
	<-h.release
}

// **Nothing is written to a connection once its end has begun** but the
// packet that ends it (Client.beginEnd). A client's DISCONNECT gives its
// max_connections slot back as it is read, before what it changed is
// stored and the socket closed; a delivery queued for it in between went
// out on a connection the broker already counted as gone.
func TestNothingIsWrittenToAConnectionOnceItsEndBegins(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		s := New(&Options{Logger: logger, InlineClient: true})
		h := &heldDisconnecting{in: make(chan struct{}), release: make(chan struct{})}
		require.NoError(t, s.AddHook(h, nil))
		tcp := listeners.NewTCP(listeners.Config{ID: "t", Address: "127.0.0.1:0"})
		require.NoError(t, s.AddListener(tcp))
		require.NoError(t, s.Serve())

		c, code := dialAndConnect(t, tcp.Address(), takeoverConnect("ending"))
		require.Equal(t, byte(0x00), code)
		// SUBSCRIBE packet id 1, no properties, "t" at QoS 0.
		_, err := c.Write([]byte{0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, 't', 0x00})
		require.NoError(t, err)
		suback := make([]byte, 6)
		_, err = io.ReadFull(c, suback)
		require.NoError(t, err)
		require.Equal(t, byte(0x90), suback[0], "no SUBACK: % x", suback)

		// Delivered while subscribed, so the delivery path is proved to reach
		// this connection.
		require.NoError(t, s.Publish("t", []byte("before"), false, 0))
		got := make([]byte, 2)
		_, err = io.ReadFull(c, got)
		require.NoError(t, err)
		got = append(got, make([]byte, got[1])...)
		_, err = io.ReadFull(c, got[2:])
		require.NoError(t, err)
		require.Contains(t, string(got), "before", "the delivery before the DISCONNECT did not arrive: % x", got)

		_, err = c.Write([]byte{0xE0, 0x00})
		require.NoError(t, err)
		select {
		case <-h.in:
		case <-time.After(5 * time.Second):
			t.Fatal("the DISCONNECT never reached OnDisconnecting")
		}
		require.NoError(t, s.Publish("t", []byte("after"), false, 0))
		// **The write loop has had its go at it before the connection is
		// let go**, read off its queue: a delivery queued is counted out
		// once the loop has written it or been refused (Client.dequeued).
		// Given a fixed 100ms, a loop that ran late tried after the close,
		// where a write that should have been refused fails anyway.
		cl, ok := s.Clients.Get("ending")
		require.True(t, ok)
		waitFor(t, func() bool { return atomic.LoadInt32(&cl.State.outboundQty) == 0 },
			"the write loop to take the delivery queued after the DISCONNECT")
		close(h.release)

		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		rest, _ := io.ReadAll(c)
		require.NotContains(t, string(rest), "after",
			"trial %d: a delivery queued after the DISCONNECT was read was written: % x", trial, rest)
		_ = c.Close()
		_ = s.Close()
	}
}

// pipeClient is a client on one end of a pipe, with a slot whose giving back
// it counts, and the other end of the pipe to read what it writes.
func pipeClient(t *testing.T) (*Client, net.Conn, *atomic.Int32) {
	t.Helper()
	s := New(&Options{Logger: logger})
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
	cl := s.NewClient(conn, "t", "racer", false)
	cl.Properties.ProtocolVersion = 5
	released := new(atomic.Int32)
	cl.holdSlot(func() { released.Add(1) })
	return cl, peer, released
}

// **The end of a connection waits for a write already under way**, and lets
// through one ending packet. A write that has begun holds the client's lock
// and may have put part of its packet on the socket, so the slot goes back
// as it finishes, not before; and once the slot is gone the connection
// writes the one CONNACK or DISCONNECT that ends it, not every packet of
// those types (Client.beginEnd).
func TestTheEndWaitsForAWriteUnderWay(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		cl, peer, released := pipeClient(t)
		pub := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
			TopicName: "t", Payload: []byte("under way")}
		wrote := make(chan error, 1)
		go func() { wrote <- cl.WritePacket(pub) }()
		// One byte read: the write is on the socket, and blocked for the rest.
		_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
		first := make([]byte, 1)
		_, err := io.ReadFull(peer, first)
		require.NoError(t, err)
		require.Equal(t, byte(0x30), first[0])

		cl.beginEnd()
		// The end claimed as well: its owner writes the one DISCONNECT
		// (Client.claimEnd).
		require.True(t, cl.claimEnd(), "trial %d: the end decided had an owner already", trial)
		require.Zero(t, released.Load(), "trial %d: the slot went back while a write was on the socket", trial)

		rest := make([]byte, 64)
		n, err := peer.Read(rest)
		require.NoError(t, err)
		require.NoError(t, <-wrote)
		require.Contains(t, string(rest[:n]), "under way")
		require.Equal(t, int32(1), released.Load(), "trial %d: the slot did not go back as the write finished", trial)

		// Nothing but the one ending packet goes out now.
		require.Error(t, cl.WritePacket(pub), "a PUBLISH was written once the connection was ending")
		got := make(chan []byte, 1)
		go func() {
			b := make([]byte, 64)
			n, _ := peer.Read(b)
			got <- b[:n]
		}()
		dis := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Disconnect}}
		require.NoError(t, cl.WritePacket(dis))
		require.Equal(t, byte(0xE0), (<-got)[0], "the ending DISCONNECT was not written")
		_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		go func() { _ = cl.WritePacket(dis) }()
		n, _ = peer.Read(rest)
		require.Zero(t, n, "trial %d: a second ending packet was written: % x", trial, rest[:n])
		require.Equal(t, int32(1), released.Load())
	}
}

// heldEncode holds a write in OnPacketEncode, which a write runs before it
// takes the client's lock, until it is released.
type heldEncode struct {
	HookBase
	reached chan struct{}
	release chan struct{}
}

func (h *heldEncode) ID() string           { return "held-encode" }
func (h *heldEncode) Provides(b byte) bool { return b == OnPacketEncode }
func (h *heldEncode) OnPacketEncode(_ *Client, pk packets.Packet) packets.Packet {
	close(h.reached)
	<-h.release
	return pk
}

// **A writer waiting for the client's lock as the end is decided writes
// nothing.** It is asked whether it may write once it holds the lock
// (Client.beginWrite); asked before, as it was, it passed while the
// connection was open and wrote after the slot had gone back.
func TestAWriterWaitingForTheLockAsTheEndBeginsWritesNothing(t *testing.T) {
	for trial := 0; trial < 5; trial++ {
		cl, peer, released := pipeClient(t)
		pub := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish},
			TopicName: "t", Payload: []byte("waited")}
		// **Held where it has asked everything it asks before the lock**,
		// with the connection open, and let go once the end is decided: the
		// interleaving is made rather than slept for.
		h := &heldEncode{reached: make(chan struct{}), release: make(chan struct{})}
		require.NoError(t, cl.ops.hooks.Add(h, nil))
		cl.Lock() // a write under way, as far as the next writer can tell
		wrote := make(chan error, 1)
		go func() { wrote <- cl.WritePacket(pub) }()
		<-h.reached
		cl.beginEnd()
		require.Equal(t, int32(1), released.Load())
		close(h.release)
		cl.Unlock()

		// A pipe holds nothing: a write the writer made is read here before
		// it returns, and one it did not make leaves this read to the close.
		read := make(chan []byte, 1)
		go func() {
			b := make([]byte, 64)
			n, _ := peer.Read(b)
			read <- b[:n]
		}()
		require.Error(t, <-wrote, "trial %d: a writer that waited for the lock wrote after the slot went back", trial)
		_ = cl.Net.Conn.Close()
		b := <-read
		require.Empty(t, b, "trial %d: a writer that waited for the lock wrote after the slot went back: % x",
			trial, b)
	}
}
