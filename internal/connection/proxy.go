package connection

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"syscall"
	"time"
)

const (
	proxySignature = "\r\n\r\n\x00\r\nQUIT\n"
	// The fixed prefix and the largest value of its two-byte length field.
	maxProxyHeaderSize = 16 + 65535
)

// errProxyHeader distinguishes a broken header from a peer closing or
// resetting the socket before sending any header byte.
var errProxyHeader = errors.New("invalid PROXY v2 header")

// ProxyHeader is the address and verified identity from one PROXY v2 header.
// Source is invalid for an unspecified family; the socket's peer is used then.
// Local asks the caller to close quietly without serving the connection.
// Verified selects the certificate identity even when CommonName is empty.
type ProxyHeader struct {
	Local      bool
	Source     netip.AddrPort
	CommonName string
	Verified   bool
}

// ReadProxyHeader reads exactly one PROXY v2 header, leaving application bytes
// unread. The layout and SSL flags follow proxy-protocol.txt, sections 2.2 and
// 2.2.6: https://www.haproxy.org/download/2.8/doc/proxy-protocol.txt.
// Sagüin accepts only UNSPEC and TCP over IPv4 or IPv6. An incomplete SSL
// record contributes no identity; broken outer framing refuses the header.
func ReadProxyHeader(r io.Reader) (ProxyHeader, error) {
	var fixed [16]byte
	n, err := io.ReadFull(r, fixed[:])
	if err != nil {
		if n == 0 {
			return ProxyHeader{}, err
		}
		return ProxyHeader{}, fmt.Errorf("%w: %w", errProxyHeader, err)
	}
	if string(fixed[:12]) != proxySignature || fixed[12]>>4 != 2 {
		return ProxyHeader{}, fmt.Errorf("%w: expected version 2 signature", errProxyHeader)
	}
	command := fixed[12] & 0x0f
	if command != 0 && command != 1 {
		return ProxyHeader{}, fmt.Errorf("%w: command %d", errProxyHeader, command)
	}
	length := int(binary.BigEndian.Uint16(fixed[14:]))
	addresses := 0
	if command == 1 {
		switch fixed[13] {
		case 0x00: // AF_UNSPEC: keep the socket's peer address
		case 0x11: // AF_INET, STREAM
			addresses = 12
		case 0x21: // AF_INET6, STREAM
			addresses = 36
		default:
			return ProxyHeader{}, fmt.Errorf("%w: family and transport 0x%02x", errProxyHeader, fixed[13])
		}
	}
	if length < addresses {
		return ProxyHeader{}, fmt.Errorf("%w: length %d is short of the addresses", errProxyHeader, length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return ProxyHeader{}, fmt.Errorf("%w: %w", errProxyHeader, err)
	}
	if command == 0 {
		return ProxyHeader{Local: true}, nil
	}
	h := ProxyHeader{}
	switch addresses {
	case 12:
		h.Source = netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[:4])), binary.BigEndian.Uint16(body[8:10]))
	case 36:
		h.Source = netip.AddrPortFrom(netip.AddrFrom16([16]byte(body[:16])), binary.BigEndian.Uint16(body[32:34]))
	}
	for records := body[addresses:]; len(records) != 0; {
		kind, value, rest, ok := splitProxyRecord(records)
		if !ok {
			return ProxyHeader{}, fmt.Errorf("%w: record exceeds the header length", errProxyHeader)
		}
		if kind == 0x20 { // PP2_TYPE_SSL
			h.CommonName, h.Verified = proxyCommonName(value)
		}
		records = rest
	}
	return h, nil
}

// splitProxyRecord shares the bounds check for outer and nested TLVs. Their
// two-byte length excludes the three-byte type and length prefix.
func splitProxyRecord(data []byte) (kind byte, value, rest []byte, ok bool) {
	if len(data) < 3 {
		return 0, nil, nil, false
	}
	length := int(binary.BigEndian.Uint16(data[1:3]))
	if length > len(data)-3 {
		return 0, nil, nil, false
	}
	return data[0], data[3 : 3+length], data[3+length:], true
}

func proxyCommonName(ssl []byte) (string, bool) {
	// CERT_CONN or CERT_SESS proves presentation; verify == 0 proves that
	// certificate was verified. An SSL connection alone proves no identity.
	if len(ssl) < 5 || ssl[0]&0x06 == 0 || binary.BigEndian.Uint32(ssl[1:5]) != 0 {
		return "", false
	}
	var name []byte
	for records := ssl[5:]; len(records) != 0; {
		kind, value, rest, ok := splitProxyRecord(records)
		if !ok {
			return "", false // the entire SSL record must be complete before it proves an identity
		}
		if kind == 0x22 { // PP2_SUBTYPE_SSL_CN
			name = value
		}
		records = rest
	}
	return string(name), true
}

// proxy runs only on the connection reader. Its deadline was fixed at the
// service permit, so an Accepted handover cannot give the header extra time.
func (r *reader) proxy(deadline time.Time) bool {
	if err := r.conn.raw.SetReadDeadline(deadline); err != nil {
		r.failed(err)
		return false
	}
	if r.stopRequested() {
		r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
		return false
	}
	// LimitedReader counts consumed bytes without a connection wrapper or
	// another socket-read loop. The parser leaves MQTT bytes unread.
	input := io.LimitedReader{R: r.conn.raw, N: maxProxyHeaderSize}
	h, err := ReadProxyHeader(&input)
	r.metrics.receivedBytes.Add(uint64(maxProxyHeaderSize - input.N))
	if err != nil {
		if !errors.Is(err, errProxyHeader) && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
			r.failed(err)
		} else {
			r.failure = fmt.Errorf("MQTT listener %s: PROXY header: %w", r.conn.listener, err)
			r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{proxyHeader: true})
		}
		return false
	}
	if h.Local {
		r.end(Event{Kind: Ended, Conn: r.conn}, readEnd{})
		return false
	}
	if h.Source.IsValid() {
		r.conn.proxyAddr = net.TCPAddrFromAddrPort(h.Source)
	}
	r.commonName = h.CommonName
	r.proxyVerified = h.Verified
	return true
}
