package harness

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
)

// The operations listeners a test configures beside Options.Operations' plain
// one, and the caller that reaches any of them as an operator does: over
// TCP, TLS or a unix socket, behind a PROXY header, with or without HTTP
// Basic credentials (design.md, Operations, HTTP and Who may call; config.md,
// broker.operations).

// OpsListener is one listener of Options.OpsListeners (config.md,
// broker.operations, listen): a unix listener where Unix is set, its path,
// mode and proxy_protocol Unix's; else a tcp listener on a free port of Host,
// 127.0.0.1 where Host is "", with TLS's tls block, none where TLS is nil or
// names no CertFile. PasswordFile is the listener's own password_file,
// written where set; without it the listener takes broker.operations'
// password_file.
type OpsListener struct {
	Unix         *Unix
	Host         string
	TLS          *TLS
	PasswordFile string
}

// appendOperations appends broker.operations: with no o.OpsListeners, one
// tcp listener at plain, unnamed, as Options.Operations has always written
// it; otherwise plain's listener, where o.Operations, named ops, then each
// of o.OpsListeners, named ops1, ops2 and so on, the tcp ones at addrs in
// order; and o.OpsPasswordFile where set.
func appendOperations(data []byte, o *Options, plain string, addrs []string) []byte {
	data = append(data, "  operations:\n    listen:\n"...)
	if len(o.OpsListeners) == 0 {
		data = fmt.Appendf(data, "      tcp:\n        address: %q\n", plain)
	} else {
		var tcp, unix []byte
		if o.Operations {
			tcp = fmt.Appendf(tcp, "        - name: ops\n          address: %q\n", plain)
		}
		next := 0
		for i, l := range o.OpsListeners {
			name := fmt.Sprintf("        - name: ops%d\n", i+1)
			if l.Unix != nil {
				unix = append(unix, name...)
				unix = fmt.Appendf(unix, "          path: %q\n", l.Unix.Path)
				if l.Unix.Mode != "" {
					unix = fmt.Appendf(unix, "          mode: %q\n", l.Unix.Mode)
				}
				if l.Unix.ProxyProtocol {
					unix = append(unix, "          proxy_protocol: true\n"...)
				}
				unix = appendLogin(unix, "          ", Login{PasswordFile: l.PasswordFile})
				continue
			}
			tcp = append(tcp, name...)
			tcp = fmt.Appendf(tcp, "          address: %q\n", addrs[next])
			next++
			tcp = appendLogin(tcp, "          ", Login{PasswordFile: l.PasswordFile})
			tcp = appendTLS(tcp, "          ", l.TLS)
		}
		if len(tcp) > 0 {
			data = append(append(data, "      tcp:\n"...), tcp...)
		}
		if len(unix) > 0 {
			data = append(append(data, "      unix:\n"...), unix...)
		}
	}
	if o.OpsPasswordFile != "" {
		data = fmt.Appendf(data, "    password_file: %q\n", o.OpsPasswordFile)
	}
	return data
}

// opsTCP is how many of o.OpsListeners listen on TCP.
func opsTCP(o *Options) int {
	n := 0
	for _, l := range o.OpsListeners {
		if l.Unix == nil {
			n++
		}
	}
	return n
}

// opsAddresses returns the address of each tcp listener of o.OpsListeners,
// in order, each the port of the matching one of free on its Host, or on
// free's own host, 127.0.0.1, where Host is "".
func opsAddresses(o *Options, free []string) ([]string, error) {
	addrs := make([]string, 0, len(free))
	next := 0
	for _, l := range o.OpsListeners {
		if l.Unix != nil {
			continue
		}
		addr := free[next]
		next++
		if l.Host != "" {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			addr = net.JoinHostPort(l.Host, port)
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// OpsTarget is how a caller reaches an operations listener: on Network,
// "tcp" or "unix", at Address, its address or socket path; speaking TLS
// with TLS where it is not nil, its ServerName the address's host where
// TLS names none; and sending Proxy, a PROXY header (ProxyHeader), first on
// each connection where it is not nil.
type OpsTarget struct {
	Network, Address string
	TLS              *tls.Config
	Proxy            []byte
}

// OpsCall is one request: Method on Path, a Path of "*" being the request
// target of a request to the server as a whole, as OPTIONS * is (RFC 9112,
// 3.2.4); its Host header Host, "" for the target's address, or localhost on
// a unix socket; and HTTP Basic credentials User and Password where User is
// set (RFC 7617).
type OpsCall struct {
	Method, Path, Host string
	User, Password     string
}

// OpsAnswer is one answer of an operations listener: its status, the media
// type of its Content-Type, its body, and its WWW-Authenticate header, ""
// for none.
type OpsAnswer struct {
	Status    int
	MediaType string
	Body      string
	Challenge string
}

// Dial opens one connection to the listener as t says, within
// DefaultTimeout: dialled, its PROXY header sent, its TLS handshake done.
func (t OpsTarget) Dial() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()
	return t.dial(ctx)
}

// dial is Dial within ctx.
func (t OpsTarget) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, t.Network, t.Address)
	if err != nil {
		return nil, err
	}
	if t.Proxy != nil {
		if _, err := conn.Write(t.Proxy); err != nil {
			_ = conn.Close() // it never carried a request
			return nil, err
		}
	}
	if t.TLS == nil {
		return conn, nil
	}
	config := t.TLS.Clone()
	if config.ServerName == "" {
		if config.ServerName, _, err = net.SplitHostPort(t.Address); err != nil {
			_ = conn.Close() // it never carried a request
			return nil, err
		}
	}
	secured := tls.Client(conn, config)
	if err := secured.HandshakeContext(ctx); err != nil {
		_ = conn.Close() // it never carried a request
		return nil, err
	}
	return secured, nil
}

// Do sends c to the listener and returns its answer, or the error that kept
// it from coming within DefaultTimeout. On a plain TCP target, calls share
// Go's default transport, so one after another they travel on one
// connection; on any other, each has a connection of its own, so each sends
// its own PROXY header and makes its own handshake.
func (t OpsTarget) Do(c *OpsCall) (OpsAnswer, error) {
	scheme, host := "http", t.Address
	if t.TLS != nil {
		scheme = "https"
	}
	if t.Network == "unix" {
		host = "localhost"
	}
	target := scheme + "://" + host + c.Path
	if c.Path == "*" {
		target = scheme + "://" + host
	}
	req, err := http.NewRequest(c.Method, target, http.NoBody)
	if err != nil {
		return OpsAnswer{}, err
	}
	if c.Path == "*" {
		req.URL.Opaque = "*" // the request line's target, net/http's way of sending it
	}
	if c.Host != "" {
		req.Host = c.Host
	}
	if c.User != "" {
		req.SetBasicAuth(c.User, c.Password)
	}
	client := http.Client{Timeout: DefaultTimeout}
	if t.Network != "tcp" || t.TLS != nil || t.Proxy != nil {
		dial := func(ctx context.Context, _, _ string) (net.Conn, error) { return t.dial(ctx) }
		own := &http.Transport{DisableKeepAlives: true, DialContext: dial}
		if t.TLS != nil {
			own.DialTLSContext = dial
		}
		client.Transport = own
	}
	resp, err := client.Do(req)
	if err != nil {
		return OpsAnswer{}, err
	}
	defer func() { _ = resp.Body.Close() }() // read whole below
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return OpsAnswer{}, err
	}
	a := OpsAnswer{Status: resp.StatusCode, Body: string(body), Challenge: resp.Header.Get("WWW-Authenticate")}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		if a.MediaType, _, err = mime.ParseMediaType(ct); err != nil {
			return OpsAnswer{}, fmt.Errorf("Content-Type %q: %w", ct, err)
		}
	}
	return a, nil
}
