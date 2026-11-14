package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// LoadTLS reads one immutable listener configuration for startup or file
// checking. Pair and CA failures are independent findings; report both.
func (l *Listener) LoadTLS() (*tls.Config, error) {
	cfg := l.TLS
	if cfg == nil {
		return nil, nil
	}
	secure, err := loadTLSFiles(cfg.CertFile, cfg.KeyFile, cfg.ClientCAFile, cfg.MinVersion, false)
	if err != nil {
		return nil, fmt.Errorf("listener %s: %w", l.Name, err)
	}
	if cfg.ClientCAFile != "" {
		secure.ClientAuth = tls.VerifyClientCertIfGiven
		if cfg.RequireCertificate {
			secure.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return secure, nil
}

// LoadTLS reads a bridge's peer trust and optional client credentials with
// the same file checks as a listener. Standard TLS verification checks the
// peer's authority and host; absent roots use the system's trust store.
func (b *Bridge) LoadTLS() (*tls.Config, error) {
	cfg := b.TLS
	if cfg == nil {
		cfg = &BridgeTLS{MinVersion: tls.VersionTLS12}
	}
	secure, err := loadTLSFiles(cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.MinVersion, true)
	if err != nil {
		return nil, fmt.Errorf("bridge %s: %w", b.Name, err)
	}
	return secure, nil
}

// loadTLSFiles builds immutable material for either end of a TLS link.
// Certificate and authority failures are independent findings.
func loadTLSFiles(certFile, keyFile, caFile string, minimum uint16, client bool) (*tls.Config, error) {
	secure := &tls.Config{MinVersion: minimum}
	var findings []error
	if certFile != "" || keyFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			findings = append(findings, fmt.Errorf("TLS certificate %s and key %s: %w", certFile, keyFile, err))
		} else {
			secure.Certificates = []tls.Certificate{pair}
			if client {
				// The configured identity is sent whenever requested, even
				// when the peer's acceptable-CA list omits its issuer.
				secure.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
					return &secure.Certificates[0], nil
				}
			}
		}
	}
	if caFile != "" {
		label := "TLS client CA "
		if client {
			label = "TLS CA "
		}
		pem, err := os.ReadFile(caFile)
		if err != nil {
			findings = append(findings, fmt.Errorf("%s%s: %w", label, caFile, err))
		} else {
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				findings = append(findings, fmt.Errorf("%s%s: no certificates found", label, caFile))
			}
			if client {
				secure.RootCAs = roots
			} else {
				secure.ClientCAs = roots
			}
		}
	}
	if err := errors.Join(findings...); err != nil {
		return nil, err
	}
	return secure, nil
}

// CheckTLS applies startup's loader to every listener and bridge TLS file,
// without binding sockets. One bad entry does not hide later findings.
func (c *Config) CheckTLS() error {
	_, err := c.loadTLS()
	return err
}
