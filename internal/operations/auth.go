package operations

import (
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/passwd"
)

// authorize selects exactly one identity source, then checks its scope.
// /health deliberately ignores credentials, including rejected ones.
func (l *Listener) authorize(r *http.Request, passwords *passwd.File) (code int, user string) {
	if r.URL.Path == "/health" {
		return http.StatusOK, ""
	}
	proxy, _ := r.Context().Value(proxyContextKey{}).(*proxyConn)
	name, verified := "", false
	if r.TLS != nil && len(r.TLS.VerifiedChains) != 0 {
		cert := r.TLS.VerifiedChains[0][0]
		name, verified = cert.Subject.CommonName, true
		if name == "" && len(cert.DNSNames) != 0 {
			name = cert.DNSNames[0]
		}
	} else if proxy != nil && proxy.header.Verified {
		name, verified = proxy.header.CommonName, true
	}
	if verified {
		if !connection.ValidIdentityName(name) {
			return http.StatusForbidden, name
		}
		if passwords == nil || passwords.Reaches(name, r.URL.Path) {
			return http.StatusOK, name
		}
		return http.StatusForbidden, name
	}
	if _, supplied := r.Header["Authorization"]; supplied {
		user, password, ok := r.BasicAuth()
		if !ok || passwords == nil || !passwords.Verify(user, password) {
			return http.StatusUnauthorized, user
		}
		if !passwords.Reaches(user, r.URL.Path) {
			return http.StatusForbidden, user
		}
		return http.StatusOK, user
	}
	if l.localRequest(r, proxy) {
		return http.StatusOK, ""
	}
	return http.StatusForbidden, ""
}

// localRequest never resolves a name. A PROXY connection must prove a
// loopback client address even though the proxy itself uses a Unix socket.
func (l *Listener) localRequest(r *http.Request, proxy *proxyConn) bool {
	local := l.unix
	if proxy != nil {
		local = proxy.header.Source.IsValid() && proxy.header.Source.Addr().IsLoopback()
	} else if !local {
		peer, err := netip.ParseAddrPort(r.RemoteAddr)
		local = err == nil && peer.Addr().IsLoopback()
	}
	if !local {
		return false
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	_, err := netip.ParseAddr(host)
	return err == nil
}
