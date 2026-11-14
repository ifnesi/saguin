package harness

import (
	"encoding/binary"
	"net/netip"
)

// The PROXY protocol version 2 header, as a proxy in front of a unix
// listener with proxy_protocol sends it before anything else (design.md,
// Flows, Transport setup), written from the bytes of HAProxy's
// specification (proxy-protocol.txt, 2.2, "Binary header format (version
// 2)") with the standard library alone, never with a PROXY library or the
// broker's code:
//
//	12 bytes  the signature \r\n\r\n\0\r\nQUIT\n
//	1 byte    the version (high nibble, 2) and the command (low nibble)
//	1 byte    the address family (high nibble) and transport (low nibble)
//	2 bytes   the length of what follows, big-endian
//	          the addresses: IPv4 4+4+2+2 bytes, IPv6 16+16+2+2, UNIX 108+108
//	          (source address, destination address, source port, destination
//	          port), none for UNSPEC
//	          then records (TLVs): a type byte, a two-byte big-endian length,
//	          the value.

// ProxySignature is the twelve bytes every PROXY v2 header begins with.
const ProxySignature = "\r\n\r\n\x00\r\nQUIT\n"

// The 13th byte: version 2 and its command.
const (
	ProxyLocal = 0x20 // LOCAL: the proxy's own connection, a health check
	ProxyProxy = 0x21 // PROXY: a client's connection, relayed
)

// The 14th byte: an address family and a transport protocol.
const (
	ProxyUnspec     = 0x00 // AF_UNSPEC, unspecified: no addresses
	ProxyTCP4       = 0x11 // AF_INET over STREAM: IPv4 addresses
	ProxyTCP6       = 0x21 // AF_INET6 over STREAM: IPv6 addresses
	ProxyUnixStream = 0x31 // AF_UNIX over STREAM: two 108-byte paths
)

// Record types and the SSL record's fields (proxy-protocol.txt, 2.2.5,
// "The PP2_TYPE_SSL type and subtypes").
const (
	ProxyTypeSSL   = 0x20 // PP2_TYPE_SSL: client flags, verify, then sub-records
	ProxySubtypeCN = 0x22 // PP2_SUBTYPE_SSL_CN: the client certificate's Common Name

	// The SSL record's client field: PP2_CLIENT_SSL, the client spoke TLS;
	// PP2_CLIENT_CERT_CONN, it presented a certificate on this connection;
	// PP2_CLIENT_CERT_SESS, on this TLS session.
	ProxyClientSSL      = 0x01
	ProxyClientCertConn = 0x02
	ProxyClientCertSess = 0x04
)

// ProxyHeader returns a PROXY v2 header: the signature, command (ProxyLocal
// or ProxyProxy, or any other byte to write a header the specification
// does not allow), family, the length of what follows, the addresses of
// source and destination in family's form, then records (ProxyRecord,
// ProxySSL), each as it is. An IPv4 or IPv6 family writes source and
// destination as 4 or 16 bytes; ProxyUnixStream writes two empty 108-byte
// paths; any other family writes no address.
func ProxyHeader(command, family byte, source, destination netip.AddrPort, records ...[]byte) []byte {
	b := append([]byte(ProxySignature), command, family, 0, 0)
	switch family >> 4 {
	case 1:
		b = append(b, source.Addr().AsSlice()...)
		b = append(b, destination.Addr().AsSlice()...)
		b = binary.BigEndian.AppendUint16(b, source.Port())
		b = binary.BigEndian.AppendUint16(b, destination.Port())
	case 2:
		s, d := source.Addr().As16(), destination.Addr().As16()
		b = append(append(b, s[:]...), d[:]...)
		b = binary.BigEndian.AppendUint16(b, source.Port())
		b = binary.BigEndian.AppendUint16(b, destination.Port())
	case 3:
		b = append(b, make([]byte, 2*108)...)
	}
	for _, r := range records {
		b = append(b, r...)
	}
	binary.BigEndian.PutUint16(b[14:16], uint16(len(b)-16))
	return b
}

// ProxyRecord is one record (TLV) of type kind holding value.
func ProxyRecord(kind byte, value []byte) []byte {
	r := append([]byte{kind}, 0, 0)
	binary.BigEndian.PutUint16(r[1:3], uint16(len(value)))
	return append(r, value...)
}

// ProxySSL is a PP2_TYPE_SSL record: its client flags, its verify field (0
// where the client presented a certificate and it was verified), then
// subs, sub-records made by ProxyRecord (a ProxySubtypeCN, say).
func ProxySSL(client byte, verify uint32, subs ...[]byte) []byte {
	value := binary.BigEndian.AppendUint32([]byte{client}, verify)
	for _, s := range subs {
		value = append(value, s...)
	}
	return ProxyRecord(ProxyTypeSSL, value)
}

// ProxyVerifiedCN is the SSL record of a proxy that verified the client
// certificate it was presented, naming cn: TLS, a certificate presented
// on the connection, verify 0, and the Common Name.
func ProxyVerifiedCN(cn string) []byte {
	return ProxySSL(ProxyClientSSL|ProxyClientCertConn, 0, ProxyRecord(ProxySubtypeCN, []byte(cn)))
}
