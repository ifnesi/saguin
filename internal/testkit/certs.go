package testkit

import (
	"cmp"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// The certificates of the tests of TLS, made with the standard library
// alone: an authority, and the certificates it issues to a server and to
// clients, each in the two forms a test needs, the PEM files a
// configuration names and the tls.Certificate a Go client presents. Every
// test of a TLS listener, MQTT, WebSocket or operations, takes its
// certificates from here.

// Authority is a certificate authority made for a test: a self-signed
// ECDSA P-256 certificate and its key, which issue the test's
// certificates.
type Authority struct {
	Cert *x509.Certificate // the authority's own certificate
	PEM  []byte            // Cert in PEM, as a client_ca_file holds it
	key  *ecdsa.PrivateKey
}

// Leaf says what a certificate an Authority issues names, and whose
// authentication it serves.
type Leaf struct {
	CommonName  string   // the subject's Common Name, none where empty
	DNSNames    []string // the subject alternative DNS names, in order
	IPAddresses []net.IP // the subject alternative IP addresses
	Server      bool     // for a server's authentication, else a client's
	// Ends is how long after its making the certificate's validity ends,
	// negative for one already ended; zero for validity, a year.
	Ends time.Duration
}

// Issued is a certificate an Authority issued, with its key.
type Issued struct {
	Certificate tls.Certificate // as a Go client or server presents it
	CertPEM     []byte          // the certificate in PEM, as a cert_file holds it
	KeyPEM      []byte          // its key in PEM (PKCS #8), as a key_file holds it
}

// validity is how long around its making a certificate is valid: from an
// hour before, so a clock a little behind accepts it, to a year after, far
// from the 30 days within which the broker warns of a certificate's end
// (design.md, Logging: "one WARN line for each listener's or bridge's
// certificate that expires within 30 days"), so only a test asking for a
// nearer end (Leaf.Ends) meets that warning.
const validity = 365 * 24 * time.Hour

// NewAuthority makes an authority whose certificate's Common Name is name.
func NewAuthority(name string) (*Authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(validity),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &Authority{Cert: cert, PEM: pemOf("CERTIFICATE", der), key: key}, nil
}

// Pool is a certificate pool holding a's certificate alone: what a client
// trusts to verify a server a issued for, or a server to verify a's
// clients.
func (a *Authority) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(a.Cert)
	return pool
}

// Issue makes a key and a certificate for it, signed by a, naming what l
// names.
func (a *Authority) Issue(l *Leaf) (*Issued, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	usage := x509.ExtKeyUsageClientAuth
	if l.Server {
		usage = x509.ExtKeyUsageServerAuth
	}
	now := time.Now()
	// From an hour before its making, or before its end where that is
	// earlier: a certificate is never valid for no time at all.
	notBefore, notAfter := now.Add(-time.Hour), now.Add(cmp.Or(l.Ends, validity))
	if notAfter.Before(now) {
		notBefore = notAfter.Add(-time.Hour)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: l.CommonName},
		DNSNames:     l.DNSNames,
		IPAddresses:  l.IPAddresses,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.Cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	certPEM, keyPEM := pemOf("CERTIFICATE", der), pemOf("PRIVATE KEY", keyDER)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &Issued{Certificate: pair, CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// serialNumber is a random serial of up to 128 bits, as RFC 5280 4.1.2.2
// allows (at most 20 octets), so no two certificates of a run share one.
func serialNumber() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// pemOf is der in a PEM block of kind.
func pemOf(kind string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der})
}
