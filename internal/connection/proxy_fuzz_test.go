package connection_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/netip"
	"slices"
	"testing"
	"testing/iotest"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
)

// The header shapes FuzzReadProxyHeader builds, each a bit or a field of
// its shape argument, so that every input names the identity the design
// gives it.
const (
	// Where a Common Name goes: no CN sub-record in the SSL record; one
	// inside it; a CN record at the top level, beside an SSL record holding
	// none; or an SSL record naming one nested inside a record of an
	// unknown type, with no SSL record at the top level.
	shapeWhere      = 0x0003
	whereNone       = 0
	whereInSSL      = 1
	whereTopLevel   = 2
	whereNested     = 3
	shapeTruncation = 0x000C // what is truncated (truncate*), shifted by 2
	// An unknown record (0xF8, reserved for future use) before the SSL
	// record; a PP2_SUBTYPE_SSL_VERSION sub-record before the CN.
	shapeUnknownFirst = 0x0010
	shapeVersionFirst = 0x0020
	// The header's length one short of its addresses.
	shapeShortLength = 0x0040
	// The parser reads one byte at a time.
	shapeOneByte = 0x0080
	// With truncateInSSL, the sub-record cut is the CN itself, else one
	// after it.
	shapeCutCN = 0x0100
	// With truncateSSLValue, how many of the SSL value's five fixed bytes
	// remain: this field, shifted by 9, modulo 5.
	shapeKept = 0x0E00
	// One byte of the header, at port modulo its length, is XORed with the
	// client field (or 0xFF where that is 0): the identity is then
	// unknown, and only the bytes an accepted header consumed are checked.
	shapeCorrupt = 0x1000
)

// What shapeTruncation truncates: nothing; the SSL record's value, cut
// within its client field and verify; a sub-record inside the SSL record,
// running past the SSL record's value; or a record at the top level,
// running past the header's length.
const (
	truncateNothing = iota
	truncateSSLValue
	truncateInSSL
	truncateRecord
)

// Record types and sub-types the shapes use beside harness's
// (proxy-protocol.txt, 2.2): PP2_SUBTYPE_SSL_VERSION and
// PP2_SUBTYPE_SSL_CIPHER, inside an SSL record; 0xE0, the first custom type;
// 0xF8, the first type reserved for future use.
const (
	subtypeVersion = 0x21
	subtypeCipher  = 0x23
	typeCustom     = 0xE0
	typeFuture     = 0xF8
)

// The command and family bytes the shapes choose from (harness.ProxyHeader's
// 13th and 14th bytes), and whether the design serves each. The commands:
// LOCAL and PROXY in version 2; command 2 and 0xF; PROXY in versions 1
// and 3. The families: unspecified; TCP over IPv4 and IPv6; and the others
// the design refuses: UDP over IPv4 and IPv6, UNIX stream, and IPv4 and
// IPv6 with an unspecified transport.
var (
	proxyCommands = []byte{harness.ProxyLocal, harness.ProxyProxy, 0x22, 0x2F, 0x11, 0x31}
	proxyFamilies = []byte{harness.ProxyUnspec, harness.ProxyTCP4, harness.ProxyTCP6, 0x12, 0x22, harness.ProxyUnixStream, 0x10, 0x20}
	servedFamily  = map[byte]bool{harness.ProxyUnspec: true, harness.ProxyTCP4: true, harness.ProxyTCP6: true}
)

// overrun is record, a type, a two-byte length and a value, with its length
// raised by n: the record runs n bytes past whatever holds it.
func overrun(record []byte, n int) []byte {
	binary.BigEndian.PutUint16(record[1:3], binary.BigEndian.Uint16(record[1:3])+uint16(n))
	return record
}

// proxyCase is one PROXY v2 header, built with harness's writer from the
// specification's bytes, and what the design says it carries: its
// command, family, client address and records chosen by the arguments
// (FuzzReadProxyHeader), never by reading the header back. A refused
// header has refused set; a corrupt one, corrupt set and nothing known.
type proxyCase struct {
	wire             []byte
	want             connection.ProxyHeader
	refused, corrupt bool
}

// newProxyCase builds the header the arguments describe. design.md, Flows,
// Transport setup: "It reads the client's address and, where the proxy
// verified a client certificate, its Common Name as the user name.
// Verification means that the SSL record marks a certificate as presented
// and verified"; "An SSL record that is truncated, or that does not mark a
// certificate as presented and verified, gives no Common Name"; "Records
// of unknown types are skipped"; "An IPv4 or IPv6 address over TCP is the
// client's address"; "On an MQTT listener, an unspecified address family
// serves the client with the socket's own address"; "A LOCAL command closes
// the connection quietly"; "A v1 header, a missing header, [...] another address family,
// transport or command, or a broken length closes the connection".
// proxy-protocol.txt, 2.2.6: the SSL record's client field marks
// PP2_CLIENT_CERT_CONN or PP2_CLIENT_CERT_SESS where the client presented a
// certificate; its verify field "will be zero if the client presented a
// certificate and it was successfully verified, and non-zero otherwise".
func newProxyCase(command, family byte, shape uint16, client byte, verify uint32, port uint16, addr, cn []byte) proxyCase {
	var c proxyCase
	cn = cn[:min(len(cn), 255)] // the header's length stays within its two bytes
	command = proxyCommands[int(command)%len(proxyCommands)]
	family = proxyFamilies[int(family)%len(proxyFamilies)]
	where, truncation := shape&shapeWhere, int(shape&shapeTruncation)>>2
	local := command == harness.ProxyLocal
	if local {
		// The specification has a receiver ignore a LOCAL header's family
		// and addresses; the design names no refusal of either for LOCAL,
		// so its headers keep a family it serves, their lengths whole.
		family = proxyFamilies[int(family)%3]
		shape &^= shapeShortLength
		if truncation == truncateRecord {
			truncation = truncateNothing
		}
	}

	var a [16]byte
	copy(a[:], addr)
	var source, destination netip.AddrPort
	addresses := 0
	switch family >> 4 {
	case 1:
		source = netip.AddrPortFrom(netip.AddrFrom4([4]byte(a[:4])), port)
		destination = netip.MustParseAddrPort("192.0.2.1:1883")
		addresses = 12
	case 2:
		source = netip.AddrPortFrom(netip.AddrFrom16(a), port)
		destination = netip.MustParseAddrPort("[2001:db8::1]:1883")
		addresses = 36
	}

	// The SSL record: its client field and verify, then its sub-records.
	fixed := binary.BigEndian.AppendUint32([]byte{client}, verify)
	var subs []byte
	if shape&shapeVersionFirst != 0 {
		subs = append(subs, harness.ProxyRecord(subtypeVersion, []byte("TLSv1.3"))...)
	}
	cnRecord := harness.ProxyRecord(harness.ProxySubtypeCN, cn)
	if where == whereInSSL {
		if truncation == truncateInSSL && shape&shapeCutCN != 0 {
			cnRecord = overrun(cnRecord, 1+int(client&3))
		}
		subs = append(subs, cnRecord...)
	}
	if truncation == truncateInSSL && (where != whereInSSL || shape&shapeCutCN == 0) {
		subs = append(subs, overrun(harness.ProxyRecord(subtypeCipher, []byte("TLS_AES_128_GCM_SHA256")), 1+int(client&3))...)
	}
	ssl := slices.Concat(fixed, subs)
	if truncation == truncateSSLValue {
		ssl = fixed[:int(shape&shapeKept)>>9%5]
	}

	var records [][]byte
	if shape&shapeUnknownFirst != 0 {
		records = append(records, harness.ProxyRecord(typeFuture, []byte("future")))
	}
	switch where {
	case whereNested:
		records = append(records, harness.ProxyRecord(typeCustom, harness.ProxyRecord(harness.ProxyTypeSSL, slices.Concat(fixed, cnRecord))))
	case whereTopLevel:
		records = append(records, cnRecord, harness.ProxyRecord(harness.ProxyTypeSSL, ssl))
	default:
		records = append(records, harness.ProxyRecord(harness.ProxyTypeSSL, ssl))
	}
	if truncation == truncateRecord {
		records = append(records, overrun(harness.ProxyRecord(typeCustom, []byte("past")), 1+int(client&3)))
	}
	c.wire = harness.ProxyHeader(command, family, source, destination, records...)
	if shape&shapeShortLength != 0 && addresses != 0 {
		binary.BigEndian.PutUint16(c.wire[14:16], uint16(addresses-1))
	}

	switch {
	case shape&shapeCorrupt != 0:
		flip := client
		if flip == 0 {
			flip = 0xFF
		}
		c.wire[int(port)%len(c.wire)] ^= flip
		c.corrupt = true
	case local:
		c.want = connection.ProxyHeader{Local: true}
	case command != harness.ProxyProxy || !servedFamily[family] || truncation == truncateRecord ||
		(shape&shapeShortLength != 0 && addresses != 0):
		c.refused = true
	default:
		c.want.Source = source
		// A complete SSL record at the top level marking a certificate
		// presented and verified selects the certificate identity, whatever
		// its Common Name; the name is its CN sub-record's, none without one.
		presented, verified := client&(harness.ProxyClientCertConn|harness.ProxyClientCertSess) != 0, verify == 0
		c.want.Verified = where != whereNested && truncation != truncateSSLValue && truncation != truncateInSSL && presented && verified
		if c.want.Verified && where == whereInSSL {
			c.want.CommonName = string(cn)
		}
	}
	return c
}

// FuzzReadProxyHeader builds PROXY v2 headers from its arguments
// (newProxyCase), each followed by the client's first bytes, and reads each
// with ReadProxyHeader, the one parser the MQTT and the operations
// listeners share. A header the design serves is read as exactly what the
// arguments made it: LOCAL alone; or the client's address (none for the
// unspecified family), Verified where a complete SSL record marks a
// certificate presented and verified, whatever its name, and that record's
// CN sub-record as the Common Name, none otherwise (design.md, Who may
// call: "The selected source is used alone. Failure does not fall back"; a
// verified identity with an empty name is selected and then refused). It
// reads exactly the header's bytes, whatever its reads' sizes:
// what follows is the client's MQTT. A header the design refuses is an
// error. A corrupted header must not panic, and one it accepts consumed
// exactly the 16 bytes and the length its own bytes give (proxy-protocol.txt,
// 2.2: the 12-byte signature, the version and command, the family, a
// big-endian length, then the addresses and records).
func FuzzReadProxyHeader(f *testing.F) {
	v4 := []byte{192, 0, 2, 10}
	v6 := netip.MustParseAddr("2001:db8::10").AsSlice()
	cn := []byte("device-7")
	const (
		local, proxy = 0, 1
		unspec, tcp4 = 0, 1
		tcp6, unix   = 2, 5
		conn, sess   = harness.ProxyClientSSL | harness.ProxyClientCertConn, harness.ProxyClientSSL | harness.ProxyClientCertSess
	)
	for _, seed := range []struct {
		command, family byte
		shape           uint16
		client          byte
		verify          uint32
	}{
		{local, tcp4, whereInSSL, conn, 0},
		{proxy, unspec, whereNone, conn, 0},
		{proxy, tcp4, whereInSSL, conn, 0},
		{proxy, tcp6, whereInSSL | shapeUnknownFirst | shapeVersionFirst, sess, 0},
		{proxy, tcp4, whereInSSL, harness.ProxyClientSSL, 0},               // no certificate presented
		{proxy, tcp4, whereInSSL, harness.ProxyClientCertConn, 0},          // presented, the SSL flag unset
		{proxy, tcp4, whereInSSL, conn, 0x01000000},                        // verify's first byte nonzero
		{proxy, tcp4, whereInSSL, conn, 0x00010000},                        // its second
		{proxy, tcp4, whereInSSL, conn, 0x00000100},                        // its third
		{proxy, tcp4, whereInSSL, conn, 0x00000001},                        // its fourth
		{proxy, tcp4, whereTopLevel, conn, 0},                              // a CN outside the SSL record
		{proxy, tcp4, whereNested, conn, 0},                                // an SSL record inside an unknown one
		{proxy, tcp4, whereInSSL | truncateSSLValue<<2 | 4<<9, conn, 0},    // verify cut short
		{proxy, tcp4, whereInSSL | truncateInSSL<<2 | shapeCutCN, conn, 0}, // the CN cut short
		{proxy, tcp4, whereInSSL | truncateInSSL<<2, conn, 0},              // a sub-record after it cut short
		{proxy, tcp4, whereInSSL | truncateRecord<<2, conn, 0},             // a record past the header
		{proxy, tcp6, whereInSSL | shapeShortLength, conn, 0},              // a length short of the addresses
		{proxy, unix, whereNone, conn, 0},                                  // another family
		{2, tcp4, whereNone, conn, 0},                                      // command 2
		{4, tcp4, whereNone, conn, 0},                                      // version 1
		{proxy, tcp4, whereInSSL | shapeOneByte, conn, 0},
		{proxy, tcp4, whereInSSL | shapeCorrupt, conn, 0},
	} {
		addr := v4
		if seed.family == tcp6 {
			addr = v6
		}
		f.Add(seed.command, seed.family, seed.shape, seed.client, seed.verify, uint16(51234), addr, cn, []byte{0x10, 0x00})
	}
	// A certificate presented and verified whose CN is empty: verified, no name.
	f.Add(byte(proxy), byte(tcp4), uint16(whereInSSL), byte(conn), uint32(0), uint16(51234), v4, []byte{}, []byte{0x10, 0x00})
	f.Fuzz(func(t *testing.T, command, family byte, shape uint16, client byte, verify uint32, port uint16, addr, cn, tail []byte) {
		c := newProxyCase(command, family, shape, client, verify, port, addr, cn)
		data := append(bytes.Clone(c.wire), tail...)
		in := bytes.NewReader(data)
		var r io.Reader = in
		if shape&shapeOneByte != 0 {
			r = iotest.OneByteReader(in)
		}
		h, err := connection.ReadProxyHeader(r)
		switch {
		case c.corrupt:
			if err == nil && len(data)-in.Len() != 16+int(binary.BigEndian.Uint16(data[14:16])) {
				t.Fatalf("% X: accepted after reading %d bytes; want the header's 16 and its length, %d",
					data, len(data)-in.Len(), binary.BigEndian.Uint16(data[14:16]))
			}
		case c.refused:
			if err == nil {
				t.Fatalf("% X: read as %+v; want it refused", c.wire, h)
			}
		case err != nil:
			t.Fatalf("% X: %v; want %+v", c.wire, err, c.want)
		case h != c.want:
			t.Fatalf("% X: read as %+v; want %+v", c.wire, h, c.want)
		case in.Len() != len(tail):
			t.Fatalf("% X: read %d bytes; want the header's %d, the client's %d left", c.wire, len(data)-in.Len(), len(c.wire), len(tail))
		}
	})
}
