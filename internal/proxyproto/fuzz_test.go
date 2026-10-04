package proxyproto_test

// The fuzz targets of the one network-facing parser outside the engine.
// Both are about what Read promises in its own words - the header and not a
// byte more, a name only beside a verification - and both seed from the
// captured headers the package's other tests hold.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"net"
	"testing"

	"github.com/ifnesi/saguin/internal/proxyproto"
)

// FuzzReadNeverReadsPastTheHeader: for any bytes at all, Read does not
// panic, consumes at most the 16 fixed bytes plus what the header declares,
// consumes exactly that when it succeeds, and never returns a header beside
// an error, a LOCAL header naming anybody, or a name beside Unverified.
func FuzzReadNeverReadsPastTheHeader(f *testing.F) {
	for _, s := range []string{haproxyHeader, nginxHeader, nginxTrustedHeader,
		nginxUntrustedHeader, nginxNoCertificateHeader, "0d0a0d0a000d0a515549540a20000000"} {
		b, err := hex.DecodeString(s)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		c, _ := hex.DecodeString(firstMQTTPacket)
		f.Add(append(append([]byte{}, b...), c...))
	}
	f.Add([]byte("PROXY TCP4 10.0.0.7 10.0.0.1 43981 8883\r\n"))
	f.Add([]byte("PROXY UNKNOWN\r\n"))
	f.Fuzz(func(t *testing.T, in []byte) {
		r := bytes.NewReader(in)
		h, err := proxyproto.Read(r)
		consumed := len(in) - r.Len()
		if len(in) >= 16 {
			declared := int(binary.BigEndian.Uint16(in[14:16]))
			if consumed > 16+declared {
				t.Fatalf("consumed %d bytes of a header declaring %d: the client's first packet is eaten", consumed, declared)
			}
			if err == nil && consumed != 16+declared {
				t.Fatalf("returned a header after %d bytes where it declared %d", consumed, declared)
			}
		}
		if err != nil {
			if h != nil {
				t.Fatalf("a header %+v beside the error %v", h, err)
			}
			return
		}
		if h.Local && (h.SourceAddr != nil || h.CommonName != "" || h.Unverified) {
			t.Fatalf("a LOCAL header names %v / %q / unverified=%v", h.SourceAddr, h.CommonName, h.Unverified)
		}
		if h.CommonName != "" && h.Unverified {
			t.Fatalf("named %q and unverified at once", h.CommonName)
		}
		if a, ok := h.SourceAddr.(*net.TCPAddr); ok {
			if n := len(a.IP); n != 4 && n != 16 {
				t.Fatalf("an IP of %d bytes", n)
			}
		}
	})
}

func tlv(typ byte, value []byte) []byte {
	out := []byte{typ, byte(len(value) >> 8), byte(len(value))}
	return append(out, value...)
}

func clip(b []byte, n int) []byte {
	if len(b) > n {
		return b[:n]
	}
	return b
}

// FuzzReadRecoversWhatAProxyEncoded builds a v2 header the way a proxy
// does - either address family, a TLV of arbitrary content before the SSL
// block, an arbitrary sub-TLV before the CN, a TLV after - and checks Read
// finds the address, and the name exactly when the flags say TLS plus a
// certificate and the verify result is zero, and Unverified exactly when
// the flags say so and the result is not.
func FuzzReadRecoversWhatAProxyEncoded(f *testing.F) {
	f.Add(true, []byte{127, 0, 0, 1, 127, 0, 0, 1}, uint16(55020), uint16(8883), byte(0x07), uint32(0), "lighthouse-99", []byte{}, []byte{}, []byte{})
	f.Add(false, make([]byte, 32), uint16(1), uint16(2), byte(0x07), uint32(21), "device-7", []byte("localhost"), []byte("TLSv1.3"), []byte{1, 2, 3, 4})
	f.Add(true, []byte{10, 0, 0, 7, 10, 0, 0, 1}, uint16(1), uint16(2), byte(0x01), uint32(1), "", []byte{}, []byte{}, []byte{})
	f.Add(true, []byte{10, 0, 0, 7, 10, 0, 0, 1}, uint16(1), uint16(2), byte(0x06), uint32(0), "nobody", []byte{}, []byte{}, []byte{})
	f.Fuzz(func(t *testing.T, v4 bool, addrs []byte, sport, dport uint16, flags byte, verify uint32,
		cn string, before, sub, after []byte) {
		before, sub, after = clip(before, 4000), clip(sub, 4000), clip(after, 4000)
		if len(cn) > 4000 {
			cn = cn[:4000]
		}
		var body []byte
		var family byte
		want := &net.TCPAddr{Port: int(sport)}
		if v4 {
			family = 0x11
			for len(addrs) < 8 {
				addrs = append(addrs, 0)
			}
			body = append(body, addrs[:8]...)
			want.IP = net.IP(append([]byte{}, addrs[:4]...))
		} else {
			family = 0x21
			for len(addrs) < 32 {
				addrs = append(addrs, 0)
			}
			body = append(body, addrs[:32]...)
			want.IP = net.IP(append([]byte{}, addrs[:16]...))
		}
		body = binary.BigEndian.AppendUint16(body, sport)
		body = binary.BigEndian.AppendUint16(body, dport)

		ssl := []byte{flags}
		ssl = binary.BigEndian.AppendUint32(ssl, verify)
		ssl = append(ssl, tlv(0x21, sub)...)
		ssl = append(ssl, tlv(0x22, []byte(cn))...)
		body = append(body, tlv(0x01, before)...)
		body = append(body, tlv(0x20, ssl)...)
		body = append(body, tlv(0x03, after)...)

		hdr := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, 0x21, family}
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(body)))
		hdr = append(hdr, body...)
		hdr = append(hdr, 0x10) // the client's first byte

		r := bytes.NewReader(hdr)
		h, err := proxyproto.Read(r)
		if err != nil {
			t.Fatalf("a well-formed header was refused: %v", err)
		}
		if r.Len() != 1 {
			t.Fatalf("%d bytes left after the header, want the client's 1", r.Len())
		}
		if got := h.SourceAddr.String(); got != want.String() {
			t.Fatalf("the client is %q, want %q", got, want.String())
		}
		presented := flags&0x01 != 0 && flags&0x06 != 0
		wantName, wantUnverified := "", false
		if cn != "" && presented {
			if verify == 0 {
				wantName = cn
			} else {
				wantUnverified = true
			}
		}
		if h.CommonName != wantName {
			t.Fatalf("named %q, want %q (flags %#x, verify %d)", h.CommonName, wantName, flags, verify)
		}
		if h.Unverified != wantUnverified {
			t.Fatalf("unverified %v, want %v (flags %#x, verify %d, cn %q)", h.Unverified, wantUnverified, flags, verify, cn)
		}
		if wantUnverified && h.VerifyResult != verify {
			t.Fatalf("verify result %d, want %d", h.VerifyResult, verify)
		}
	})
}
