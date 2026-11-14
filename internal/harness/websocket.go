package harness

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"
)

// MQTT over WebSocket, the client's end (MQTT 5 chapter 6; design.md, Flows,
// Transport setup), and the server's end a bridge's test peer serves
// (ServeWebSocket), written from RFC 6455 with the standard library alone,
// never with a WebSocket library or the broker's code. A frame:
//
//	1 byte    FIN (0x80) and the opcode (low nibble)
//	1 byte    MASK (0x80, set on every frame a client sends, never on a
//	          server's) and a length:
//	          up to 125, or 126 and 2 bytes, or 127 and 8 bytes, big-endian
//	4 bytes   the masking key, on a masked frame
//	          the payload, each byte XORed with key[i%4] when masked
//
// Owning its framing, the client reads with Go's deadlines as on TCP: a
// read whose deadline passes consumes nothing, and the next read goes on.
// A WebSocket library may not allow that (gorilla/websocket's
// Conn.SetReadDeadline: "After a read has timed out, the websocket
// connection state is corrupt and all future reads will return an error"),
// and the harness waits with deadlines and reads on after one passes
// (e2efuzz's quiet interval).

// Frame opcodes (RFC 6455 5.2).
const (
	OpContinuation = 0x0
	OpText         = 0x1
	OpBinary       = 0x2
	OpClose        = 0x8
	OpPing         = 0x9
	OpPong         = 0xA
)

// websocketGUID is the string the server appends to the client's key to
// prove it read the upgrade (RFC 6455 1.3, 4.2.2).
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// ErrTextFrame is a text frame from the server: "MQTT Control Packets MUST
// be sent in WebSocket binary data frames. If any other type of data frame
// is received the recipient MUST close the Network Connection"
// [MQTT-6.0.0-1].
var ErrTextFrame = errors.New("a text frame: MQTT travels in binary frames [MQTT-6.0.0-1]")

// ErrCloseFrame is a Close frame from the server: design.md, Flows,
// Transport setup, "Closing a WebSocket connection ends the stream as on
// TCP, with no WebSocket Close frame".
var ErrCloseFrame = errors.New("a WebSocket Close frame: the broker ends the stream as on TCP, with none")

// ErrBadFrame is a frame from the server that RFC 6455 has a client fail
// the connection on, or from the client that it has a server fail it on: a
// masked frame from the server, an unmasked one from the client (5.1); a
// reserved bit set, no extension
// having been negotiated, or an unknown opcode (5.2); a length not in its
// shortest form, or one of 8 bytes with its most significant bit set (5.2);
// a continuation with no fragmented message begun, or a new data frame
// before one has ended (5.4); a control frame fragmented or over 125 bytes
// (5.5); or a Close whose body is one byte, short of its status code
// (5.5.1).
var ErrBadFrame = errors.New("a frame RFC 6455 has a client fail the connection on")

// UpgradeError is a server's refusal of the WebSocket upgrade: its answer,
// some status other than 101 Switching Protocols.
type UpgradeError struct {
	StatusCode int
	Status     string
}

func (e *UpgradeError) Error() string {
	return "the WebSocket upgrade was answered " + e.Status
}

// WebSocketDial is how DialWebSocket opens a connection: its request and
// how its writes are framed.
type WebSocketDial struct {
	// Path is the request target; "" for "/".
	Path string
	// Host is the Host header; "" for the address dialled.
	Host string
	// Subprotocols are offered in Sec-WebSocket-Protocol, none offering
	// none (MQTT-6.0.0-3: a client "MUST include mqtt").
	Subprotocols []string
	// Header holds further request headers, Origin among them, each value
	// on a line of its own.
	Header http.Header
	// TLS, where not nil, speaks wss: TLS with this configuration first,
	// its ServerName the host dialled where it names none.
	TLS *tls.Config
	// Dialer dials the TCP connection; nil for the default.
	Dialer *net.Dialer
	// MessageSize, above zero, cuts each write into binary messages of at
	// most MessageSize bytes; zero sends a write as one message.
	MessageSize int
	// FrameSize, above zero, sends each message in frames of at most
	// FrameSize bytes, continuation frames after the first (RFC 6455 5.4);
	// zero sends a message as one frame.
	FrameSize int
}

// WebSocket is a WebSocket connection's client end, which a Client speaks
// MQTT over: each write a binary message, framed as WebSocketDial says, and
// the payloads of the server's binary frames read as one stream of bytes,
// whatever their boundaries (MQTT-6.0.0-2). ServeWebSocket makes its
// server end, which frames nothing it sends with a mask and reads the
// client's masked frames unmasked.
type WebSocket struct {
	net.Conn // the TCP or TLS connection under it
	// Response is the server's answer to the upgrade; nil on the server's
	// end.
	Response *http.Response

	in                     *bufio.Reader // the other end's bytes, the upgrade read from them
	messageSize, frameSize int
	left                   uint64   // the unread payload of the binary frame being read
	fragmented             bool     // a binary message has begun and its final frame not yet come
	pongs                  [][]byte // the payloads of the Pong frames read, in order
	ended                  error    // what ended reading, the other end's Close frame

	server   bool    // the server's end: it reads masked frames and sends unmasked ones
	key      [4]byte // the masking key of the client's binary frame being read
	unmasked int     // the bytes of that frame's payload already unmasked, which pick its key's byte
}

// DialWebSocket opens a TCP connection to addr within DefaultTimeout, then
// speaks WebSocket over it as UpgradeWebSocket does, and returns a Client
// speaking MQTT in version over it.
func DialWebSocket(addr string, version byte, d *WebSocketDial) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	dialer := d.Dialer
	if dialer == nil {
		dialer = new(net.Dialer)
	}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return UpgradeWebSocket(conn, addr, version, d)
}

// UpgradeWebSocket speaks TLS over conn, a connection to addr, where d
// asks, then upgrades it to a WebSocket (RFC 6455 4.1), each within
// DefaultTimeout, and returns a Client speaking MQTT in version over it. A
// refused upgrade is an *UpgradeError, its connection closed; so is any
// answer a client must fail (RFC 6455 4.1: a wrong Sec-WebSocket-Accept, or
// a subprotocol it did not offer).
func UpgradeWebSocket(conn net.Conn, addr string, version byte, d *WebSocketDial) (*Client, error) {
	if d.TLS != nil {
		config := d.TLS
		if config.ServerName == "" {
			host, _, _ := net.SplitHostPort(addr)
			config = config.Clone()
			config.ServerName = host
		}
		secure := tls.Client(conn, config)
		ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
		err := secure.HandshakeContext(ctx)
		cancel()
		if err != nil {
			_ = conn.Close() // the handshake failed: the connection carries nothing
			return nil, err
		}
		conn = secure
	}
	w, err := upgrade(conn, addr, d)
	if err != nil {
		_ = conn.Close() // the upgrade failed: the connection carries nothing
		return nil, err
	}
	return NewClient(w, version), nil
}

// upgrade sends conn the upgrade d describes and reads the server's answer.
func upgrade(conn net.Conn, addr string, d *WebSocketDial) (*WebSocket, error) {
	if err := conn.SetDeadline(time.Now().Add(DefaultTimeout)); err != nil {
		return nil, err
	}
	nonce := make([]byte, 16)
	_, _ = rand.Read(nonce) // crypto/rand.Read never fails
	key := base64.StdEncoding.EncodeToString(nonce)
	var req bytes.Buffer
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n",
		cmp.Or(d.Path, "/"), cmp.Or(d.Host, addr), key)
	if len(d.Subprotocols) > 0 {
		fmt.Fprintf(&req, "Sec-WebSocket-Protocol: %s\r\n", strings.Join(d.Subprotocols, ", "))
	}
	if err := d.Header.Write(&req); err != nil {
		return nil, err
	}
	req.WriteString("\r\n")
	if _, err := conn.Write(req.Bytes()); err != nil {
		return nil, err
	}
	in := bufio.NewReader(conn)
	resp, err := http.ReadResponse(in, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, fmt.Errorf("reading the answer to the WebSocket upgrade: %w", err)
	}
	_ = resp.Body.Close() // a 101's body is the WebSocket itself, left unread
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, &UpgradeError{StatusCode: resp.StatusCode, Status: resp.Status}
	}
	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), AcceptKey(key); got != want {
		return nil, fmt.Errorf("the upgrade's Sec-WebSocket-Accept is %q; want %q (RFC 6455 4.1)", got, want)
	}
	if p := resp.Header.Get("Sec-WebSocket-Protocol"); p != "" && !slices.ContainsFunc(d.Subprotocols, func(o string) bool { return strings.EqualFold(o, p) }) {
		return nil, fmt.Errorf("the upgrade selected the subprotocol %q, which the client did not offer (RFC 6455 4.1)", p)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return &WebSocket{Conn: conn, Response: resp, in: in, messageSize: d.MessageSize, frameSize: d.FrameSize}, nil
}

// ServeWebSocket reads a WebSocket upgrade from conn, within
// DefaultTimeout, and answers it as a server does (RFC 6455 4.2.2): 101
// Switching Protocols with the Sec-WebSocket-Accept of the client's key,
// selecting the subprotocol mqtt where the client offers it ("The
// WebSocket Subprotocol name selected and returned by the Server MUST be
// mqtt" [MQTT-6.0.0-4]) and none where it does not. It returns the
// server's end of the WebSocket, each write one unmasked binary frame, and
// the request, whatever its path. The 101 answer also carries extra's lines.
// A request that is no WebSocket upgrade
// (RFC 6455 4.2.1: GET, Upgrade websocket, Connection Upgrade, a
// Sec-WebSocket-Key and Sec-WebSocket-Version 13) is answered 400 Bad
// Request, and returned with an error.
func ServeWebSocket(conn net.Conn, extra http.Header) (*WebSocket, *http.Request, error) {
	if err := conn.SetDeadline(time.Now().Add(DefaultTimeout)); err != nil {
		return nil, nil, err
	}
	in := bufio.NewReader(conn)
	req, err := http.ReadRequest(in)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the WebSocket upgrade: %w", err)
	}
	answer := &http.Response{ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{}}
	key := req.Header.Get("Sec-WebSocket-Key")
	if req.Method != http.MethodGet || !hasToken(req.Header, "Upgrade", "websocket") || !hasToken(req.Header, "Connection", "upgrade") ||
		key == "" || req.Header.Get("Sec-WebSocket-Version") != "13" {
		answer.StatusCode = http.StatusBadRequest
		answer.Header.Set("Connection", "close")
		_ = answer.Write(conn) // refused either way: what the client reads of it is its own
		return nil, req, fmt.Errorf("%s %s is no WebSocket upgrade (RFC 6455 4.2.1)", req.Method, req.RequestURI)
	}
	answer.StatusCode = http.StatusSwitchingProtocols
	maps.Copy(answer.Header, extra)
	answer.Header.Set("Upgrade", "websocket")
	answer.Header.Set("Connection", "Upgrade")
	answer.Header.Set("Sec-WebSocket-Accept", AcceptKey(key))
	if slices.Contains(Subprotocols(req.Header), "mqtt") {
		answer.Header.Set("Sec-WebSocket-Protocol", "mqtt")
	}
	if err := answer.Write(conn); err != nil {
		return nil, req, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, req, err
	}
	return &WebSocket{Conn: conn, in: in, server: true}, req, nil
}

// Subprotocols is the subprotocols an upgrade's header offers, in order:
// every Sec-WebSocket-Protocol line, each a comma-separated list (RFC 6455
// 4.1, 11.3.4).
func Subprotocols(h http.Header) []string {
	var offered []string
	for _, line := range h.Values("Sec-WebSocket-Protocol") {
		for p := range strings.SplitSeq(line, ",") {
			if p = strings.TrimSpace(p); p != "" {
				offered = append(offered, p)
			}
		}
	}
	return offered
}

// hasToken reports whether one of h's name lines lists token, compared
// without case, as Upgrade's and Connection's are (RFC 7230 6.1, 6.7).
func hasToken(h http.Header, name, token string) bool {
	for _, line := range h.Values(name) {
		for t := range strings.SplitSeq(line, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// AcceptKey is the Sec-WebSocket-Accept a server answers the client's
// Sec-WebSocket-Key with: the base64 of the SHA-1 of the key followed by
// the GUID (RFC 6455 4.2.2).
func AcceptKey(key string) string {
	sum := sha1.Sum([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// AppendFrame appends one frame a client sends, masked with key: fin, the
// opcode, the payload's length in its shortest form, the key and the
// masked payload (RFC 6455 5.2, 5.3).
func AppendFrame(b []byte, fin bool, opcode byte, key [4]byte, payload []byte) []byte {
	b, at := appendKeySlot(b, fin, opcode, len(payload))
	copy(b[at:], key[:])
	return appendMasked(b, at, payload)
}

// appendFrame appends a frame masked with a fresh key, drawn straight into
// its place in the frame: "The masking key is a 32-bit value chosen at
// random by the client" (RFC 6455 5.3).
func appendFrame(b []byte, fin bool, opcode byte, payload []byte) []byte {
	b, at := appendKeySlot(b, fin, opcode, len(payload))
	_, _ = rand.Read(b[at:]) // crypto/rand.Read never fails
	return appendMasked(b, at, payload)
}

// appendKeySlot appends a client frame's header for a payload of n bytes,
// fin, the opcode and the length in its shortest form, then four bytes for
// its masking key, which begin at at (RFC 6455 5.2).
func appendKeySlot(b []byte, fin bool, opcode byte, n int) (frame []byte, at int) {
	b = appendHeader(b, fin, opcode, 0x80, n)
	return append(b, 0, 0, 0, 0), len(b)
}

// appendHeader appends a frame's first bytes for a payload of n bytes: fin
// and the opcode, then mask, 0x80 on a client's frame and 0 on a server's,
// with the length in its shortest form (RFC 6455 5.2).
func appendHeader(b []byte, fin bool, opcode, mask byte, n int) []byte {
	if fin {
		opcode |= 0x80
	}
	b = append(b, opcode)
	switch {
	case n < 126:
		return append(b, mask|byte(n))
	case n <= 0xFFFF:
		return binary.BigEndian.AppendUint16(append(b, mask|126), uint16(n))
	}
	return binary.BigEndian.AppendUint64(append(b, mask|127), uint64(n))
}

// appendFrameOf appends one frame as w's end sends it: masked with a fresh
// key on a client's end, unmasked on a server's (RFC 6455 5.1).
func (w *WebSocket) appendFrameOf(b []byte, fin bool, opcode byte, payload []byte) []byte {
	if w.server {
		return append(appendHeader(b, fin, opcode, 0, len(payload)), payload...)
	}
	return appendFrame(b, fin, opcode, payload)
}

// appendMasked appends payload masked with the key at b[at:at+4], the
// frame's last four bytes (RFC 6455 5.3).
func appendMasked(b []byte, at int, payload []byte) []byte {
	start := len(b)
	b = append(b, payload...)
	for i := range payload {
		b[start+i] ^= b[at+i%4]
	}
	return b
}

// chunks is b in pieces of at most size bytes, b whole where size is
// zero; b is not empty.
func chunks(b []byte, size int) iter.Seq[[]byte] {
	if size <= 0 {
		size = len(b)
	}
	return slices.Chunk(b, size)
}

// frameLen is the length of a frame w's end sends with a payload of n
// bytes: two bytes, the length's 2 or 8 more where n needs them, the
// masking key on a client's end, and the payload (RFC 6455 5.2).
func (w *WebSocket) frameLen(n int) int {
	head := 2
	if !w.server {
		head += 4
	}
	switch {
	case n < 126:
		return head + n
	case n <= 0xFFFF:
		return head + 2 + n
	}
	return head + 8 + n
}

// wireLen is the length of n bytes framed as Write frames them: messages of
// at most messageSize bytes, each in frames of at most frameSize.
func (w *WebSocket) wireLen(n int) int {
	total := 0
	for message := range chunkSizes(n, w.messageSize) {
		for f := range chunkSizes(message, w.frameSize) {
			total += w.frameLen(f)
		}
	}
	return total
}

// chunkSizes is the sizes of chunks' pieces of n bytes cut at size.
func chunkSizes(n, size int) iter.Seq[int] {
	if size <= 0 {
		size = n
	}
	return func(yield func(int) bool) {
		for ; n > 0; n -= size {
			if !yield(min(n, size)) {
				return
			}
		}
	}
}

// Write sends p as binary messages, cut and framed as the connection was
// dialled to (WebSocketDial), each one frame on a server's end, in one
// write to the connection under it, framed into one buffer of the wire's
// length.
func (w *WebSocket) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	wire := make([]byte, 0, w.wireLen(len(p)))
	for message := range chunks(p, w.messageSize) {
		framed := 0 // the bytes of message already framed: the first frame is the message's, the last FIN
		for f := range chunks(message, w.frameSize) {
			opcode := byte(OpContinuation)
			if framed == 0 {
				opcode = OpBinary
			}
			framed += len(f)
			wire = w.appendFrameOf(wire, framed == len(message), opcode, f)
		}
	}
	if _, err := w.Conn.Write(wire); err != nil {
		return 0, err
	}
	return len(p), nil
}

// WriteFrame sends one frame as it is given, masked on a client's end: a
// text frame, a ping, or a fragment of a message.
func (w *WebSocket) WriteFrame(fin bool, opcode byte, payload []byte) error {
	_, err := w.Conn.Write(w.appendFrameOf(nil, fin, opcode, payload))
	return err
}

// Read reads the payloads of the other end's binary frames, continuation
// frames included, as one stream, a client's unmasked. A Ping is answered
// with a Pong (RFC 6455 5.5.2) and a Pong kept (Pongs). The end of the
// stream under it is the end of the WebSocket's, io.EOF, where it comes
// between messages; inside a frame's header or payload, or between the
// frames of a fragmented message, it is io.ErrUnexpectedEOF. A Close frame
// is ErrCloseFrame from then on, as is a text frame ErrTextFrame, and any
// frame RFC 6455 has a client fail the connection on, or a server on its
// end, ErrBadFrame. A read whose deadline passes between frames consumes
// nothing of the next.
func (w *WebSocket) Read(p []byte) (int, error) {
	for w.left == 0 {
		if w.ended != nil {
			return 0, w.ended
		}
		if err := w.frame(); err != nil {
			return 0, err
		}
	}
	if uint64(len(p)) > w.left {
		p = p[:w.left]
	}
	n, err := w.in.Read(p)
	if w.server {
		w.unmask(p[:n])
	}
	w.left -= uint64(n)
	if w.left != 0 || w.fragmented {
		err = cutShort(err) // the frame's payload, or its message, had more to come
	}
	return n, err // a frame completing its message, then the end: a clean end (TLS can give both at once)
}

// cutShort is err, but the end of the stream is io.ErrUnexpectedEOF: it
// came where more of a frame or a message was owed.
func cutShort(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// frame reads the next frame's header, and a control frame's payload,
// consuming nothing until all of it has arrived and been found valid.
func (w *WebSocket) frame() error {
	head, err := w.in.Peek(2)
	if err != nil {
		if len(head) == 0 && !w.fragmented {
			return err // between messages: the stream's own end
		}
		return cutShort(err)
	}
	if head[0]&0x70 != 0 {
		return fmt.Errorf("%w: reserved bits 0x%02X set, no extension negotiated (5.2)", ErrBadFrame, head[0]&0x70)
	}
	masked := head[1]&0x80 != 0
	switch {
	case masked && !w.server:
		return fmt.Errorf("%w: a masked frame from the server (5.1)", ErrBadFrame)
	case !masked && w.server:
		return fmt.Errorf("%w: an unmasked frame from the client (5.1)", ErrBadFrame)
	}
	size := 2
	switch head[1] & 0x7F {
	case 126:
		size += 2
	case 127:
		size += 8
	}
	if masked {
		size += 4 // the masking key follows the length
	}
	if head, err = w.in.Peek(size); err != nil {
		return cutShort(err)
	}
	n := uint64(head[1] & 0x7F)
	switch n {
	case 126:
		n = uint64(binary.BigEndian.Uint16(head[2:]))
		if n < 126 {
			return fmt.Errorf("%w: a length of %d in 2 bytes, not its shortest form (5.2)", ErrBadFrame, n)
		}
	case 127:
		n = binary.BigEndian.Uint64(head[2:])
		if n>>63 != 0 || n <= 0xFFFF {
			return fmt.Errorf("%w: a length of %d in 8 bytes, its most significant bit set or not its shortest form (5.2)", ErrBadFrame, n)
		}
	}
	fin, opcode := head[0]&0x80 != 0, head[0]&0x0F
	switch opcode {
	case OpBinary, OpContinuation:
		if begun := opcode == OpContinuation; w.fragmented != begun {
			if begun {
				return fmt.Errorf("%w: a continuation frame with no fragmented message begun (5.4)", ErrBadFrame)
			}
			return fmt.Errorf("%w: a new binary message before the fragmented one ended (5.4)", ErrBadFrame)
		}
		if masked {
			w.key, w.unmasked = [4]byte(head[size-4:size]), 0
		}
		_, _ = w.in.Discard(size) // peeked: it is there
		w.left = n
		w.fragmented = !fin
		return nil
	case OpText:
		return ErrTextFrame
	case OpClose, OpPing, OpPong:
	default:
		return fmt.Errorf("%w: the unknown opcode 0x%X (5.2)", ErrBadFrame, opcode)
	}
	switch {
	case !fin:
		return fmt.Errorf("%w: a fragmented control frame, opcode 0x%X (5.5)", ErrBadFrame, opcode)
	case n > 125:
		return fmt.Errorf("%w: a control frame of %d bytes, over 125 (5.5)", ErrBadFrame, n)
	case opcode == OpClose && n == 1:
		return fmt.Errorf("%w: a Close whose body is one byte, short of its status code (5.5.1)", ErrBadFrame)
	}
	whole, err := w.in.Peek(size + int(n))
	if err != nil {
		return cutShort(err)
	}
	payload := bytes.Clone(whole[size:])
	if masked {
		w.key, w.unmasked = [4]byte(whole[size-4:size]), 0
		w.unmask(payload)
	}
	_, _ = w.in.Discard(len(whole)) // peeked: it is there
	switch opcode {
	case OpPing:
		return w.WriteFrame(true, OpPong, payload)
	case OpPong:
		w.pongs = append(w.pongs, payload)
	default:
		w.ended = ErrCloseFrame
	}
	return nil
}

// unmask unmasks b, the next bytes of the client's frame being read, with
// its key (RFC 6455 5.3).
func (w *WebSocket) unmask(b []byte) {
	for i := range b {
		b[i] ^= w.key[w.unmasked%4]
		w.unmasked++
	}
}

// Pongs returns the payloads of the Pong frames read so far, in order.
func (w *WebSocket) Pongs() [][]byte {
	return w.pongs
}

// CloseWrite ends what the client sends, keeping its reading open: the
// end of the stream under the WebSocket, TCP's or TLS's close_notify.
func (w *WebSocket) CloseWrite() error {
	return w.Conn.(interface{ CloseWrite() error }).CloseWrite()
}
