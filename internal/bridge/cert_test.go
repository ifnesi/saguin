package bridge

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSigned writes a certificate named name and its key to the two
// paths, replacing whatever was there.
func writeSelfSigned(t *testing.T, certFile, keyFile, name string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

// RFC 0002 "TLS on a listener": a bridge's own cert_file and key_file are read
// at every handshake, so a pair renewed on disk is what the next reconnect
// presents.
func TestABridgePresentsARenewedCertificateAtItsNextHandshake(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeSelfSigned(t, certFile, keyFile, "bridge-old")
	present := presentedCertificate(certFile, keyFile)

	name := func() string {
		t.Helper()
		pair, err := present(nil)
		if err != nil {
			t.Fatalf("the certificate was not presented: %v", err)
		}
		leaf, err := x509.ParseCertificate(pair.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		return leaf.Subject.CommonName
	}
	if got := name(); got != "bridge-old" {
		t.Fatalf("presented %q before any renewal", got)
	}
	writeSelfSigned(t, certFile, keyFile, "bridge-new")
	if got := name(); got != "bridge-new" {
		t.Errorf("after the pair was renewed on disk the next handshake presented %q, want bridge-new", got)
	}

	// A pair that stops loading fails the handshake, naming the keys, rather
	// than presenting nothing and connecting unauthenticated.
	if err := os.WriteFile(keyFile, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := present(nil); err == nil {
		t.Error("a pair that does not load was presented without an error")
	}
}
