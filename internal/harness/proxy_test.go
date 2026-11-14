package harness

import (
	"bytes"
	"net/netip"
	"testing"
)

// The PROXY v2 headers the tests send are the specification's bytes
// (proxy-protocol.txt, 2.2), written out here by hand: the signature; 0x21,
// version 2 and PROXY; 0x11, AF_INET over STREAM; a length of 12 and the
// records', big-endian; source and destination addresses, then their
// ports; then a PP2_TYPE_SSL record (0x20), its length, client 0x03
// (PP2_CLIENT_SSL and PP2_CLIENT_CERT_CONN), verify 0, and a
// PP2_SUBTYPE_SSL_CN sub-record (0x22) naming "cn". A LOCAL header
// (0x20) of the unspecified family (0x00) has a length of 0.
func TestProxyHeaderIsTheSpecificationsBytes(t *testing.T) {
	signature := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
	for _, c := range []struct {
		name      string
		got, want []byte
	}{
		{"an IPv4 PROXY header with a verified Common Name",
			ProxyHeader(ProxyProxy, ProxyTCP4, netip.MustParseAddrPort("192.0.2.10:51234"), netip.MustParseAddrPort("192.0.2.1:1883"), ProxyVerifiedCN("cn")),
			append(append([]byte(nil), signature...), 0x21, 0x11, 0x00, 12+3+5+3+2,
				192, 0, 2, 10, 192, 0, 2, 1, 0xC8, 0x22, 0x07, 0x5B,
				0x20, 0x00, 5+3+2, 0x03, 0, 0, 0, 0, 0x22, 0x00, 0x02, 'c', 'n')},
		{"a LOCAL header",
			ProxyHeader(ProxyLocal, ProxyUnspec, netip.AddrPort{}, netip.AddrPort{}),
			append(append([]byte(nil), signature...), 0x20, 0x00, 0x00, 0x00)},
	} {
		if !bytes.Equal(c.got, c.want) {
			t.Errorf("%s: % X; want % X", c.name, c.got, c.want)
		}
	}
}
