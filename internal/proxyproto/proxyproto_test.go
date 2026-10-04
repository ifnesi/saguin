package proxyproto_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/proxyproto"
)

// **Captured from two real proxies**, both terminating mTLS in front of a
// Unix socket, with the client certificate CN `lighthouse-99`:
//
//	HAProxy 3.x  server s unix@/sock/saguin.sock send-proxy-v2-ssl-cn
//	nginx 1.31.4 proxy_pass unix:/sock/saguin.sock; proxy_protocol v2;
//
// A header this package wrote itself would only prove it reads its own
// bytes. The TLV layout is exactly where a specification and an
// implementation differ, and these two differ from each other: HAProxy
// sends one TLV, nginx sends three, and the SSL block is not first in
// nginx's. Both were taken by putting a listener on the socket that
// hexdumps what arrives.
const (
	haproxyHeader = "" +
		"0d0a0d0a000d0a515549540a2111002e7f0000017f000001d6ec22b320001f07" +
		"00000000210007544c5376312e3322000d6c69676874686f7573652d3939"
	nginxHeader = "" +
		"0d0a0d0a000d0a515549540a211100717f0000017f000001c18c269b0200096c" +
		"6f63616c686f737420004f0700000000210007544c5376312e3322000d6c6967" +
		"6874686f7573652d3939230016544c535f4145535f3132385f47434d5f534841" +
		"32353624000a5253412d53484132353625000752534132303438030004bde755" +
		"f2"
	// The CONNECT that followed the header on the wire, byte for byte.
	firstMQTTPacket = "101200044d5154540502000000000570726f6265"
)

func decode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decoding a captured header: %v", err)
	}
	return b
}

func TestReadingWhatARealProxySent(t *testing.T) {
	for _, tc := range []struct {
		name, raw, remote string
	}{
		{"haproxy send-proxy-v2-ssl-cn", haproxyHeader, "127.0.0.1:55020"},
		// nginx 1.31.4 sends an authority TLV, the SSL block and a CRC32C -
		// so the SSL block is neither first nor last, and a parser that
		// assumed either would find no name here.
		{"nginx proxy_protocol v2", nginxHeader, "127.0.0.1:49548"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := proxyproto.Read(bytes.NewReader(decode(t, tc.raw)))
			if err != nil {
				t.Fatalf("reading a header this proxy really sent: %v", err)
			}
			if got := h.SourceAddr.String(); got != tc.remote {
				t.Errorf("the client address is %q, want %q: without it every client "+
					"behind one proxy is the same local peer", got, tc.remote)
			}
			if h.CommonName != "lighthouse-99" {
				t.Errorf("the common name is %q, want lighthouse-99: the certificate the "+
					"proxy verified is the identity a rule is about", h.CommonName)
			}
		})
	}
}

// **The header and not a byte more.** What follows is the client's first
// MQTT packet - the real one, captured behind the real header - and a
// reader that buffered past the header would swallow it. That is the shape
// that has cost this repository five findings in other places.
func TestReadStopsAtTheEndOfTheHeader(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"haproxy", haproxyHeader},
		{"nginx", nginxHeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(decode(t, tc.raw+firstMQTTPacket))
			if _, err := proxyproto.Read(r); err != nil {
				t.Fatalf("read: %v", err)
			}
			rest, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("reading what follows: %v", err)
			}
			if got := hex.EncodeToString(rest); got != firstMQTTPacket {
				t.Errorf("after the header the connection holds %q, want the CONNECT %q: "+
					"the client's first packet was consumed with the header",
					got, firstMQTTPacket)
			}
		})
	}
}

// **Captured from nginx 1.31.4 with `ssl_verify_client optional_no_ca`**,
// which lets a client through whatever certificate it presents. Three
// clients, the listener on the socket hexdumping each header:
//
//	trusted    CN=device-7, signed by the authority nginx was given
//	untrusted  CN=device-7, signed by an authority nginx has never seen
//	none       no certificate at all
//
// nginx forwards the untrusted name exactly as it forwards the trusted one,
// and only the SSL block's client flags and verify result tell them apart:
// 0x07 and 0 for trusted, 0x07 and 21 (X509_V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE)
// for untrusted, 0x01 and 1 with no name for none.
const (
	nginxTrustedHeader = "" +
		"0d0a0d0a000d0a515549540a211100607f0000017f000001c8a68fa320004a07" +
		"00000000210007544c5376312e332200086465766963652d37230016544c535f" +
		"4145535f3235365f47434d5f53484133383424000a5253412d53484132353625" +
		"0007525341323034380300047e24cfdd"
	nginxUntrustedHeader = "" +
		"0d0a0d0a000d0a515549540a211100607f0000017f000001c8aa8fa320004a07" +
		"00000015210007544c5376312e332200086465766963652d37230016544c535f" +
		"4145535f3235365f47434d5f53484133383424000a5253412d53484132353625" +
		"000752534132303438030004f1afb700"
	nginxNoCertificateHeader = "" +
		"0d0a0d0a000d0a515549540a211100557f0000017f000001c8b28fa320003f01" +
		"00000001210007544c5376312e33230016544c535f4145535f3235365f47434d" +
		"5f53484133383424000a5253412d534841323536250007525341323034380300" +
		"048cb50e0b"
)

// **A name is the client's only when the proxy says it verified the
// certificate carrying it.** The PROXY protocol's own words: the verify
// field "will be zero if the client presented a certificate and it was
// successfully verified", and a certificate was presented when the flags
// say PP2_CLIENT_CERT_CONN (0x02) or PP2_CLIENT_CERT_SESS (0x04) on a
// PP2_CLIENT_SSL (0x01) connection. A zero alone is not enough: HAProxy
// leaves verify at zero for a client that presented nothing.
//
// The edited cases change only the SSL block's flags byte of the trusted
// capture, so each differs from a header that names device-7 by the one
// thing it tests. nginx's CRC32C no longer matches them, which this reader
// does not check.
func TestOnlyAVerifiedCertificateNamesTheClient(t *testing.T) {
	const trustedSSL = "20004a0700000000"
	edit := func(flags string) string {
		return strings.Replace(nginxTrustedHeader, trustedSSL, "20004a"+flags+"00000000", 1)
	}
	for _, tc := range []struct {
		name, raw, want string
		unverified      bool
	}{
		{"nginx, a certificate it verified", nginxTrustedHeader, "device-7", false},
		{"nginx, a certificate it could not verify", nginxUntrustedHeader, "", true},
		{"nginx, no certificate", nginxNoCertificateHeader, "", false},
		{"verify zero with no certificate presented", edit("01"), "", false},
		{"verify zero and certificate flags without TLS", edit("06"), "", false},
		{"presented on this connection only", edit("03"), "device-7", false},
		{"presented earlier in a resumed session", edit("05"), "device-7", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := proxyproto.Read(bytes.NewReader(decode(t, tc.raw)))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if h.CommonName != tc.want {
				t.Errorf("the client is named %q, want %q", h.CommonName, tc.want)
			}
			if h.Unverified != tc.unverified {
				t.Errorf("Unverified is %v, want %v: an operator is told about a certificate "+
					"that failed only if this says so", h.Unverified, tc.unverified)
			}
			if tc.unverified && h.VerifyResult != 21 {
				t.Errorf("the verify result is %d, want the 21 nginx sent", h.VerifyResult)
			}
		})
	}
}

// A LOCAL header is a proxy describing its own connection, which is what a
// health check is: there is no client to name and nothing wrong.
func TestALocalHeaderNamesNoClient(t *testing.T) {
	h, err := proxyproto.Read(bytes.NewReader(decode(t, "0d0a0d0a000d0a515549540a20000000")))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !h.Local {
		t.Error("a LOCAL header was not read as one, so a health check would be served " +
			"as though it were a client")
	}
}

// A v1 header is the natural mistake - `proxy_protocol on` in nginx sends
// one, and that was measured rather than assumed - so the refusal names the
// setting to change rather than reporting an absence.
func TestAV1HeaderIsToldApartFromNone(t *testing.T) {
	_, err := proxyproto.Read(strings.NewReader("PROXY TCP4 10.0.0.7 10.0.0.1 43981 8883\r\n"))
	if err == nil {
		t.Fatal("a v1 header was read as v2")
	}
	for _, want := range []string{"v1", "proxy_protocol v2", "send-proxy-v2-ssl-cn"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q, so an operator reads it as a broken "+
				"proxy rather than one setting: %v", want, err)
		}
	}
}

// Nothing about a header is trusted: these are the shapes that read past
// the end of somebody else's memory in a parser that believes its input.
func TestAHeaderThatCannotBeTrusted(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"no header at all", "48656c6c6f20776f726c64212020202020202020"},
		{"a length longer than the body", "0d0a0d0a000d0a515549540a211100ff7f000001"},
		{"an ipv4 block too short for its addresses", "0d0a0d0a000d0a515549540a2111000400000000"},
		{"an address family this does not read", "0d0a0d0a000d0a515549540a2131000c000000000000000000000000"},
		{"a version that is not 2", "0d0a0d0a000d0a515549540a1111000c7f0000017f00000195f822bb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := proxyproto.Read(bytes.NewReader(decode(t, tc.raw))); err == nil {
				t.Error("accepted a header that cannot be trusted")
			}
		})
	}
}

// A TLV claiming more than the header holds is how a parser reads memory
// that is not its own. It yields no name rather than panicking, and the
// addresses before it are still good.
func TestATruncatedTLVIsNotFatal(t *testing.T) {
	// The real HAProxy header with the SSL TLV's length raised past the end
	// of what follows it: 001f becomes 00ff, everything else untouched.
	broken := strings.Replace(haproxyHeader, "20001f07", "2000ff07", 1)
	h, err := proxyproto.Read(bytes.NewReader(decode(t, broken)))
	if err != nil {
		t.Fatalf("a truncated TLV made the whole header unreadable: %v", err)
	}
	if h.CommonName != "" {
		t.Errorf("a truncated TLV yielded the name %q", h.CommonName)
	}
	if h.SourceAddr == nil {
		t.Error("the addresses were lost with the TLV, though they are read before it")
	}
}
