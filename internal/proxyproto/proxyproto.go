// Package proxyproto reads a PROXY protocol v2 header.
//
// A proxy that terminates TLS and reaches saguin over a Unix socket leaves
// the broker looking at one local peer for every client in the fleet: every
// log line, every `max_connections` count and every authorization decision
// loses who was actually on the other end. A v2 header carries the client's
// real address, and - where the proxy sends it - the Common Name from the
// certificate the proxy verified.
//
// **v2 only.** v1 carries addresses and no TLVs, so it cannot do the half
// this exists for, and reading it would mean supporting a format that
// answers half the question.
//
// **The header is the peer asserting who its client is.** That is a
// spoofable identity the moment anything untrusted can connect, which is
// why this is read on a Unix socket, whose file permissions already answer
// who may speak to the broker at all.
package proxyproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// signature is the 12 bytes every v2 header opens with.
var signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

const (
	// The address families and transports this reads. Anything else is a
	// header for a connection saguin cannot describe, and is read past
	// rather than guessed at.
	afUnspec = 0x00
	afINET   = 0x11 // TCP over IPv4
	afINET6  = 0x21 // TCP over IPv6

	// TLV types. PP2_TYPE_SSL carries a sub-TLV block, and
	// PP2_SUBTYPE_SSL_CN inside it is the client certificate's Common Name.
	tlvSSL   = 0x20
	tlvSSLCN = 0x22

	// The SSL block's client flags: the connection was TLS, and the client
	// presented a certificate on it or earlier in the session it resumed.
	clientSSL      = 0x01
	clientCertConn = 0x02
	clientCertSess = 0x04
)

// ErrNoHeader says the connection did not open with a v2 signature. It is
// not a malformed header - it is a client that sent none.
var ErrNoHeader = errors.New("no PROXY protocol v2 header")

// Header is what a proxy said about the connection behind it.
type Header struct {
	// SourceAddr is the real client. Nil for a LOCAL header, which a proxy
	// sends for its own health checks.
	SourceAddr net.Addr

	// CommonName is the client certificate's CN, when the proxy sent one
	// and says it verified the certificate. HAProxy sends it with
	// `send-proxy-v2-ssl-cn`, nginx with `proxy_protocol v2`.
	CommonName string

	// Unverified says the proxy forwarded a name from a certificate it
	// presented as failing verification, and VerifyResult is the code it
	// gave - an OpenSSL X509_V_ERR, 21 for an authority it does not trust.
	// CommonName is empty then: the name is kept out of the header rather
	// than left for every reader to remember to check.
	Unverified   bool
	VerifyResult uint32

	// Local says the proxy was describing its own connection rather than a
	// client's, which is what a health check looks like.
	Local bool
}

// Read consumes one v2 header from r.
//
// **It reads exactly the header and not a byte more**, because what follows
// is the client's first MQTT packet and a reader that buffered past the
// header would swallow it - the same shape as a helper that reads the
// socket to print one packet and eats what arrives behind it.
func Read(r io.Reader) (*Header, error) {
	var fixed [16]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return nil, fmt.Errorf("reading the header: %w", err)
	}
	for i, b := range signature {
		if fixed[i] != b {
			// **A v1 header is worth telling apart from no header at all.**
			// It opens with the ASCII "PROXY ", and an operator who wrote
			// `proxy_protocol on` in NGINX gets one - the natural thing to
			// write, and the reason this refusal has to name the fix rather
			// than report an absence. Measured: nginx 1.31.4 sends v1 for
			// `on` and v2 only for `v2`.
			if string(fixed[:6]) == "PROXY " {
				return nil, errors.New("this is a PROXY protocol v1 header and saguin " +
					"reads v2: v1 carries addresses and no TLVs, so it cannot say which " +
					"certificate the proxy verified. Ask the proxy for v2 - in NGINX that " +
					"is `proxy_protocol v2`, in HAProxy `send-proxy-v2-ssl-cn`")
			}
			return nil, ErrNoHeader
		}
	}

	version, command := fixed[12]>>4, fixed[12]&0x0F
	if version != 2 {
		return nil, fmt.Errorf("PROXY protocol version %d: only v2 carries the TLVs this "+
			"reads, and v1 cannot say who the client is", version)
	}

	// **The length is a uint16, and that is the bound** on what one header
	// may claim before any of it is read - invariant 13's shape, held by the
	// type rather than by a check: at most 65,535 bytes, per connection,
	// whatever the peer declares.
	length := int(binary.BigEndian.Uint16(fixed[14:16]))
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("reading %d bytes of header: %w", length, err)
	}

	h := &Header{}
	switch command {
	case 0x00: // LOCAL: the proxy's own connection, addresses to be ignored
		h.Local = true
		return h, nil
	case 0x01: // PROXY
	default:
		return nil, fmt.Errorf("PROXY command 0x%02X is neither LOCAL nor PROXY", command)
	}

	rest, err := addresses(fixed[13], body, h)
	if err != nil {
		return nil, err
	}
	readSSL(rest, h)
	return h, nil
}

// addresses reads the address block for the family, and returns what
// follows it - the TLVs.
func addresses(family byte, body []byte, h *Header) ([]byte, error) {
	switch family {
	case afINET:
		if len(body) < 12 {
			return nil, fmt.Errorf("an IPv4 header carries %d bytes, want at least 12", len(body))
		}
		h.SourceAddr = &net.TCPAddr{
			IP:   net.IP(body[0:4]).To4(),
			Port: int(binary.BigEndian.Uint16(body[8:10])),
		}
		return body[12:], nil
	case afINET6:
		if len(body) < 36 {
			return nil, fmt.Errorf("an IPv6 header carries %d bytes, want at least 36", len(body))
		}
		h.SourceAddr = &net.TCPAddr{
			IP:   net.IP(body[0:16]),
			Port: int(binary.BigEndian.Uint16(body[32:34])),
		}
		return body[36:], nil
	case afUnspec:
		// A proxy describing a connection it cannot express as an address.
		// The TLVs may still be there and are still worth reading.
		return body, nil
	}
	// A family this does not read - a Unix-socket address block, say. The
	// v2 format fixes that block at 216 bytes, so the TLVs behind it could be
	// found; not reading them is a choice, because a proxy whose own
	// frontend is a Unix socket is not the deployment this listener is for.
	return nil, fmt.Errorf("PROXY address family 0x%02X is not one this reads", family)
}

// readSSL walks the TLVs for the SSL block and takes the CN inside it, if
// the proxy says it verified the certificate carrying it.
//
// **A name is only as good as the verification beside it.** nginx forwards
// the CN of any certificate a client presented, including one signed by an
// authority it does not trust when `ssl_verify_client optional_no_ca` let
// the client through, and the verify result is the only thing that tells
// the two apart. The PROXY protocol defines it as zero "if the client
// presented a certificate and it was successfully verified", and zero alone
// is not enough: HAProxy leaves it at zero for a client that presented
// none. So a name counts when the flags say TLS and a certificate, and the
// result is zero. mosquitto checks the same three before it believes a
// certificate.
//
// **Length-checked at every step rather than trusted.** These bytes come
// from the proxy, and a TLV claiming more than the header holds is how a
// parser reads somebody else's memory.
func readSSL(tlvs []byte, h *Header) {
	for len(tlvs) >= 3 {
		typ := tlvs[0]
		n := int(binary.BigEndian.Uint16(tlvs[1:3]))
		if 3+n > len(tlvs) {
			return // truncated: nothing here can be trusted
		}
		value := tlvs[3 : 3+n]
		tlvs = tlvs[3+n:]
		// The SSL TLV opens with a client flags byte and a 32-bit verify
		// result, and the sub-TLVs follow.
		if typ != tlvSSL || len(value) < 5 {
			continue
		}
		cn := subCommonName(value[5:])
		if cn == "" {
			continue
		}
		client, verify := value[0], binary.BigEndian.Uint32(value[1:5])
		presented := client&clientSSL != 0 && client&(clientCertConn|clientCertSess) != 0
		switch {
		case presented && verify == 0:
			h.CommonName = cn
		case presented:
			h.Unverified, h.VerifyResult = true, verify
		}
		return
	}
}

func subCommonName(tlvs []byte) string {
	for len(tlvs) >= 3 {
		typ := tlvs[0]
		n := int(binary.BigEndian.Uint16(tlvs[1:3]))
		if 3+n > len(tlvs) {
			return ""
		}
		if typ == tlvSSLCN {
			return string(tlvs[3 : 3+n])
		}
		tlvs = tlvs[3+n:]
	}
	return ""
}
