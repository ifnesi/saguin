package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ifnesi/saguin/internal/authz"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/mqtt/listeners"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/proxyproto"
	"github.com/ifnesi/saguin/internal/store"
)

// healthLockTimeout is how long the handler waits for the broker's own
// lock before answering 503.
//
// A constant rather than a configuration key, because an operator has no
// real reason to change it (RFC 0005 "The health endpoint"). A publish
// costs on the order of a hundred microseconds, so the lock is held for
// microseconds at a time, and any threshold separating "busy" from
// "wedged" is four orders of magnitude away from both. Nothing in saguin
// holds this lock across disk I/O - a snapshot builds its state under it
// and releases it before writing, the retention sweep copies the store
// maps under it so no removal is done holding it - so a hold this long is
// a defect rather than a large channel or a slow disk.
const healthLockTimeout = 5 * time.Second

// Responsive reports whether the broker can still take its own lock.
//
// It answers the one question a liveness probe is entitled to ask: would
// restarting this process help? saguin has one broker-wide mutex on the
// publish path, so a broker that cannot take it is one whose publishes
// have stopped, and that is a failure a restart does fix.
//
// The lock is taken in a goroutine and waited for, rather than polled with
// TryLock. TryLock does not queue: under the load where a false negative
// would be worst - thousands of publishes a second, all contending - a
// poller can lose the race repeatedly and report a healthy broker wedged.
// A blocking Lock joins the mutex's own queue, which hands off to waiters
// rather than starving them. When the wait times out the goroutine is left
// blocked on a lock that is not coming back, which is exactly the state
// being reported, and the process is about to be restarted.
func (b *Broker) Responsive(timeout time.Duration) bool {
	got := make(chan struct{})
	go func() {
		b.mu.Lock()
		b.mu.Unlock()
		close(got)
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-got:
		return true
	case <-t.C:
		return false
	}
}

// operationsHandler answers the operations listener, /health among it.
// RFC 0005 has the whole of /health's contract; the short version is that
// it reports nothing about the world outside this process, because a probe
// that fails when somebody else's broker is unreachable gets this one
// restarted for a fault a restart cannot fix.
//
// The body carries nothing but the status, and that is a decision rather
// than an omission: /health is never authenticated, so whatever it returns
// is readable by anybody who can reach the port. That is what a probe
// needs - a supervisor asking whether to restart a process cannot be made
// to hold a credential - and it is why nothing about the broker's contents
// is in there.
// The timeout is a parameter rather than the constant read inside, so that
// the 503 can be tested at the contract level in milliseconds instead of
// the five seconds a real probe waits. It is not a knob: ServeOperations is
// the only caller and passes healthLockTimeout.
func (b *Broker) operationsHandler(timeout time.Duration, minScrape time.Duration, operators *Operators) http.Handler {
	mux := http.NewServeMux()
	// **`/metrics` is behind the credential and `/health` is not**, which
	// is RFC 0005 and is not symmetry anybody should tidy up. /metrics
	// carries channel names, message volumes and consumer positions - the
	// shape of somebody's fleet - while /health answers one question a
	// supervisor asks before restarting a process, and a probe cannot hold a
	// credential.
	for _, r := range b.operationsRoutes(operators, minScrape) {
		mux.Handle(r.path, b.requireOperator(operators, r.handler))
	}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "", http.StatusMethodNotAllowed)
			return
		}

		status, body := http.StatusOK, `{"status":"ok"}`
		if !b.Responsive(timeout) {
			status, body = http.StatusServiceUnavailable, `{"status":"unavailable"}`
			b.log.Error("health: the broker did not answer within the timeout",
				"timeout", timeout)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		// A HEAD gets the status and the headers and no body, which is
		// what some probes send. net/http discards a body written to a
		// HEAD anyway; not writing one keeps that explicit.
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(body + "\n"))
		}
	})
	return mux
}

// ServeOperations binds the operations listener and serves it until the
// returned function is called. It answers `/health` and `/metrics`, and
// `404` to anything else (RFC 0005 "The operations listener").
//
// **A door of each kind, or a list of named ones (RFC 0002 "Several
// listeners of a kind"), the same shape broker.mqtt.listen's kinds take.**
// certs is this call's TLS, one entry per tcp door named in it; a door
// absent from the map, or mapped to nil, serves no TLS.
//
// The bind happens before this returns, so a port already in use is an
// error the caller can report at startup naming the address, rather than
// something that surfaces later from inside a goroutine. When more than one
// is configured and a later one fails, every earlier one is closed again: a
// broker that starts having opened part of what its configuration asked
// for is a broker whose operator learns the rest is missing from a scrape
// that never arrives.
//
// **One server across every door.** They answer the same paths with the
// same handler and the same timeouts, because the difference between them
// is who can reach saguin, which is the listener's business and not the
// handler's.
func (b *Broker) ServeOperations(l config.OperationsListen, minScrape time.Duration,
	operators *Operators, certs map[string]*tls.Config) (stop func(), err error) {

	var lns []net.Listener
	closeAll := func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
	}
	for _, d := range l.TCP {
		ln, err := net.Listen("tcp", d.Address.Address)
		if err != nil {
			closeAll()
			return nil, err
		}
		// **Tagged with its own name before anything else wraps it**, so a
		// request arriving here is credentialed against this door's own
		// file and no other's (Operators.forDoor) - the same fact
		// SetListenerCredentials keys the MQTT side on, read here off
		// which listener accepted the connection rather than off anything
		// the caller sent.
		ln = &namedListener{Listener: ln, name: d.Name}
		// Wrapped here rather than by serving with ServeTLS, so that the
		// bind and the certificate are two separate failures with two
		// separate messages - and so that the socket half below is
		// untouched: a Unix socket carries the credential inside the
		// machine, where a certificate would be ceremony rather than
		// security.
		if cert := certs[d.Name]; cert != nil {
			ln = tls.NewListener(ln, cert)
		}
		lns = append(lns, ln)
	}
	for _, d := range l.Unix {
		ln, err := listenUnix(d.Path, d.FileMode())
		if err != nil {
			closeAll()
			return nil, err
		}
		ln = &namedListener{Listener: ln, name: d.Name}
		// **A proxy in front, saying who its client is.** nginx terminates
		// TLS in its stream module and forwards here with
		// `proxy_protocol v2`, which carries the client certificate's
		// Common Name - the same arrangement the MQTT socket already uses,
		// now available to the routes an operator reads.
		if d.ProxyProtocol {
			ln = proxyproto.NewListener(ln, b.log)
		}
		lns = append(lns, ln)
	}
	if len(lns) == 0 {
		// Validation refuses this, so reaching it means somebody built an
		// Operations block in Go rather than loading one.
		return nil, errors.New("operations listener: nothing to listen on")
	}

	srv := &http.Server{
		Handler: b.operationsHandler(healthLockTimeout, minScrape, operators),
		// **Which door this connection came in by**, decided from the
		// connection rather than from anything it sent. A caller can put
		// any name in a header; what it cannot do is choose which socket
		// it arrived on, so that is what says whether the header may be
		// believed.
		//
		// The connection is stored and not its Common Name: net/http calls
		// this in the loop that accepts every connection, and asking a
		// proxied connection its name reads its header, so one silent peer
		// asked here would stop the door for everybody.
		ConnContext: connContext,
		// A probe that opens a connection and never sends anything must
		// not hold one open for ever; these are generous for a request
		// with no body and no work behind it.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      healthLockTimeout + 5*time.Second,
	}

	for _, ln := range lns {
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				b.log.Error("operations listener stopped", "address", ln.Addr().String(),
					"error", err)
			}
		}()
	}

	return func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			_ = srv.Close()
		}
	}, nil
}

// namedListener tags every connection it accepts with the door it came
// through - ServeOperations wraps each configured door's net.Listener in
// one of these before any TLS or PROXY-protocol wrapper goes on, so the
// name survives however many layers a request passes through to reach
// ConnContext.
type namedListener struct {
	net.Listener
	name string
}

func (l *namedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &namedConn{Conn: c, name: l.name}, nil
}

// namedConn is a connection tagged with the name of the door it arrived
// on. Every other method is the wrapped connection's own, promoted.
type namedConn struct {
	net.Conn
	name string
}

// doorNameKey carries the name of the door a request arrived on - the
// operations listener's own id space (RFC 0002 "Several listeners of a
// kind"), which Operators.forDoor keys its credential lookup on.
type doorNameKey struct{}

// doorNameOf is the name namedListener tagged a connection with, unwrapped
// through whatever wrapped it afterwards: a TLS handshake (*tls.Conn, whose
// NetConn method reaches the connection under it) and a PROXY protocol
// header (*proxyproto.Conn, whose Unwrap does the same). Anything else -
// a connection built directly, by a test that never went through
// ServeOperations - has no name, the same way gatedDoor answers false for
// one: the conservative reading of "nobody tagged this deliberately".
func doorNameOf(c net.Conn) string {
	for {
		switch v := c.(type) {
		case *namedConn:
			return v.name
		case *tls.Conn:
			c = v.NetConn()
		case *proxyproto.Conn:
			c = v.Unwrap()
		default:
			return ""
		}
	}
}

// connContext is the http.Server's ConnContext for the operations listener:
// what the handlers read off the connection itself.
//
// **A connection whose door tag cannot be found carries no door name**, and
// requireOperator refuses a request with none. Every connection here comes
// through a namedListener, so a lost tag means a wrapper was added that
// doorNameOf does not unwrap - a bug, and one that must not be answered
// with the most permissive answer (the one an unauthenticated door gets).
func connContext(ctx context.Context, c net.Conn) context.Context {
	ctx = context.WithValue(ctx, gatedDoorKey{}, gatedDoor(c))
	if name := doorNameOf(c); name != "" {
		ctx = context.WithValue(ctx, doorNameKey{}, name)
	}
	return context.WithValue(ctx, proxyConnKey{}, c)
}

// notePrincipal records who read a route that names devices.
//
// **Only the `/v1` routes, and only at Info.** Refusals were logged and
// successes were not, so the record of who read which consumers are behind
// lived in the proxy's log rather than in the broker's - and a deployment
// with no proxy had none at all. `/metrics` is left out on purpose: it is
// scraped every minute by design, and a line per scrape is a log nobody can
// read for the lines that matter.
//
// The name is bounded: it can come from a header a caller wrote.
func (b *Broker) notePrincipal(r *http.Request, name string) {
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		return
	}
	b.log.Info("operations: a principal read an inspection route",
		"user", b.limits.Loggable(name),
		"path", b.limits.Loggable(r.URL.Path),
		"remote", hostOf(r.RemoteAddr))
}

// refuseRoute answers a caller saguin knows and does not let through here.
//
// **403, not 401**, and the difference is what a scraper does next. A 401
// says "try again with a credential", so Prometheus retries and logs an
// authentication failure - sending an operator to check a password that is
// correct. A 403 says the caller is known and does not reach here, which is
// the truth.
//
// **The body says what the log line says**, because whoever meets this is
// looking at a browser rather than at the broker's output, and an empty
// page is a dead end at the one moment somebody needs to be told
// something. It does not name the credential back: they know what they
// sent, and it is a caller-chosen string that has no business in a
// response body.
//
// **The browser line is only for a password.** HTTP Basic has no way to
// offer a different credential once a browser holds one, and the answer -
// a private window - is not a thing most people reach for while looking at
// a blank page. A caller named by a certificate or by a proxy has no such
// problem, and telling them to open a private window would send them
// somewhere the fix is not.
func (b *Broker) refuseRoute(w http.ResponseWriter, r *http.Request) {
	// Written out twice rather than assembled into a variable: the guard
	// that keeps caller strings out of log lines and response bodies reads
	// the argument, and a variable - however safely built - reads to it
	// exactly like one carrying whatever arrived. Two literals cost a
	// duplicated sentence and keep that check strict for everybody else.
	if _, _, basic := r.BasicAuth(); basic {
		http.Error(w, "this credential does not reach this route.\n"+
			"Widen it:  saguin --passwd scope <file> <user> <routes>\n"+
			"A browser cannot be asked for a different credential once it "+
			"holds one: use a private window.\n", http.StatusForbidden)
		return
	}
	http.Error(w, "this credential does not reach this route.\n"+
		"Widen it:  saguin --passwd scope <file> <user> <routes>\n",
		http.StatusForbidden)
}

// Operators is who may read the operations routes, by the door they arrive
// at (RFC 0005, RFC 0002 "Several listeners of a kind").
//
// **Which door decides which file, and the door is read off the
// connection** - namedListener tags it and doorNameOf reads the tag back,
// the same way gatedDoor reads a structural fact off the connection rather
// than trusting anything a caller sent: a caller can put any name in a
// header, but it cannot choose which listener accepted it.
//
// **Already resolved.** Each entry is the file that door authenticates
// against, whether it was written on the listener or inherited from
// broker.operations.password_file; config.Operations.OperatorFiles does
// that once, so the startup that loads these files and the --check-config
// that opens them cannot disagree. A door with no entry has no credential,
// which validation has already made a loopback address or a socket whose
// permissions are the gate.
//
// **The map can be replaced while every door is open**, which is what
// SIGUSR1 does: an operator who withdraws a credential and signals expects
// the next request to be refused, not the next restart. So it is read
// under a lock rather than closed over, and the startup path still writes
// it directly - it runs before anything is listening.
type Operators struct {
	mu     sync.RWMutex
	byDoor map[string]*passwd.File
}

// Set replaces every door's file at once.
//
// **The whole map, always, and never one entry of it**, because a
// deployment whose doors name several files has withdrawn a credential
// from all of them or from none: applying half of a re-read leaves one
// door admitting somebody the operator has just removed, which is the
// failure the re-read exists to prevent.
func (o *Operators) Set(byDoor map[string]*passwd.File) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.byDoor = byDoor
}

// forDoor is the file a request authenticates against, given the door it
// arrived at.
//
// **No name in the context means no file**, never the file of whichever
// door happens to be first in the map. That is not by itself safe: no file
// reads as "no credential configured", so requireOperator refuses a request
// with no door name before it asks here.
func (o *Operators) forDoor(r *http.Request) *passwd.File {
	if o == nil {
		return nil
	}
	name, _ := r.Context().Value(doorNameKey{}).(string)
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.byDoor[name]
}

// gatedDoorKey marks a request that arrived somewhere the operator decides
// who may knock.
type gatedDoorKey struct{}

// gatedDoor reports whether a connection arrived on a door with an access
// control of its own: a Unix socket, whose file permissions say which user
// or group may speak to it.
//
// **A loopback port is not such a door, and treating it as one was a
// hole.** It is unreachable from the network, which is not the same as
// being reachable only by whoever the operator chose: *every* process on
// the machine may connect to it. A header there let any local process name
// itself any operator and read which devices are behind, without the
// password the operators file exists to check.
//
// This is the rule `broker.mqtt.listen.unix.proxy_protocol` already states
// in so many words - "only here, and never on a TCP listener … a socket's
// file permissions are what already decide who may assert it, and a TCP
// listener would need a trusted-proxy allowlist before this could be
// offered there at all". The HTTP side was written without applying it, and
// the two disagreed about the same door.
//
// **Asked of the local address, not the remote one.** The remote address of
// a proxied connection is the proxy's own socket, so asking it would answer
// yes for a caller from anywhere in the world. What matters is which door
// the broker opened.
func gatedDoor(c net.Conn) bool {
	_, unix := c.LocalAddr().(*net.UnixAddr)
	return unix
}

// askedByName reports whether a request named this broker rather than
// addressing it: `Host: saguin.example` rather than `Host: 127.0.0.1:9090`.
//
// `localhost` counts as an address. It is the one name nobody else's DNS can
// point somewhere else, and it is what an operator types.
//
// A request with no Host at all is not a browser - every browser sends one -
// so it is not a name.
func askedByName(r *http.Request) bool {
	host := r.Host
	if host == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// A bare IPv6 literal arrives bracketed and without a port, which
	// SplitHostPort refuses rather than unwraps.
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if strings.EqualFold(host, "localhost") {
		return false
	}
	return net.ParseIP(host) == nil
}

// principal is who saguin believes is asking, and where that belief came
// from. An empty name is nobody, which is not the same as a rejected one.
//
// **Three sources, most-proved first.** A password the broker checked
// itself; a client certificate an authority this broker names signed; and a
// header a proxy set, which is believed only on a door nothing off this
// machine can open. A header on a routable port is not evidence of
// anything - anyone who can reach the port can write it - so it is not read
// there at all rather than read and weighed.
func principal(r *http.Request, operators *passwd.File) (name string, proved bool) {
	if user, password, ok := r.BasicAuth(); ok {
		if operators != nil && operators.Verify(user, password) {
			return user, true
		}
		// **A credential that was offered and failed ends it here.**
		// Falling through to the header let a wrong password beside a
		// header answer 200 - a rejected credential that is not rejected,
		// and the easiest of these to reach by accident: a scraper with a
		// stale password behind a proxy that also names it.
		return "", false
	}
	if cn := proxyCommonName(r); cn != "" {
		return cn, true
	}
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		// The handshake verified the chain against the configured
		// authority, so whatever name is in it is as good as that
		// authority. **`proved` is true whether or not one is found**:
		// this caller got through a door that admits nobody without a
		// certificate, and treating a nameless one as an anonymous local
		// reader gave it *more* than a named one - every route, where
		// `CN=device-7` reached only /metrics. Being unnamed must never
		// be worth more than being named.
		return certificateName(r.TLS.PeerCertificates[0]), true
	}
	if gated, _ := r.Context().Value(gatedDoorKey{}).(bool); gated {
		if name := r.Header.Get(principalHeader); name != "" {
			return name, true
		}
	}
	return "", false
}

// certificateName is the name in a client certificate, or "" when it holds
// none this reads.
//
// **Both doors name a certificate here**: the MQTT listener's verifiedName
// calls this too, so one certificate is one identity at either.
//
// **Exactly as the certificate states it**, which is how mosquitto, NATS and
// EMQX take it: trimmed here and not at the MQTT door, a CN of "ops " named
// the operator "ops" at this door and the client "ops " at the other - one
// certificate, two identities.
//
// **The Common Name first, then the first DNS name.** CN is where an
// operator's own authority usually puts it, and it is what an acl_file is
// written against. But CN has been deprecated as an identifier for years
// and many authorities now issue certificates with a subject alternative
// name and nothing else - which arrived here as no name at all until one was
// minted and seen to reach every route.
func certificateName(cert *x509.Certificate) string {
	if cn := cert.Subject.CommonName; cn != "" {
		return cn
	}
	for _, dns := range cert.DNSNames {
		if dns != "" {
			return dns
		}
	}
	return ""
}

// proxyConnKey carries the connection a request arrived on, so the handler
// can ask it what a PROXY v2 header said about the client.
type proxyConnKey struct{}

// proxyCommonName is the Common Name a proxy verified and sent ahead of
// this connection, or "" when there was no proxy or it sent none.
func proxyCommonName(r *http.Request) string {
	c, _ := r.Context().Value(proxyConnKey{}).(net.Conn)
	if c == nil {
		return ""
	}
	return proxyproto.CommonNameOf(c)
}

// principalHeader is what a reverse proxy sets to say who it authenticated.
//
// **Read on a Unix socket and nowhere else.** nginx sets
// it after terminating TLS, from a map over `$ssl_client_s_dn` - there is
// no CN-only variable in stock nginx, and the obvious guess
// (`$ssl_client_s_dn_cn`) does not exist and stops nginx starting.
//
// PROXY protocol carries the same name on a socket and is the better of
// the two where it fits, because a socket declared to speak it refuses a
// connection that sends no header, where a header simply is not there. An
// earlier note here said nginx's stream module sends no TLVs at all; the
// README drove it against nginx 1.31.4 and the name arrived.
const principalHeader = "X-Saguin-Principal"

// requireOperator puts a handler behind HTTP Basic, or in front of nobody
// when no password file is configured.
//
// Basic is what every scraper already sends and every proxy already
// understands, and it carries the credential in clear - which is what TLS
// is for on a link the operator does not own, and why the loopback rule
// only lifts when this file exists rather than whenever somebody asks.
//
// **The refusal says a credential was rejected and does not contain it.**
// Not the password, for the obvious reason, and not the username either:
// it arrives in a header, so it is whatever the caller chose to send, and a
// log line that copies it is an unbounded client-chosen string in a file
// somebody has to store (the same rule TestNoLogLineCarriesAnUnbounded-
// ClientString exists for). What is worth having is that a rejection
// happened and where from.
func (b *Broker) requireOperator(ops *Operators, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// **Resolved per request rather than closed over**, because one
		// http.Server answers both doors and the door is a property of the
		// connection. Closing over one file would be the whole listener
		// taking whichever door happened to be configured first.
		if door, _ := r.Context().Value(doorNameKey{}).(string); door == "" {
			// **Fail closed, on the absence of a name** rather than on the
			// presence of a marker somebody must remember to set: a request
			// context built anywhere but connContext carries neither, and
			// was served as an unauthenticated door. With no door there is
			// no file to judge the caller by, and "no file" would read as
			// "no credential configured" below - the most permissive
			// answer. /health is not behind this handler, so a probe still
			// works.
			b.log.Warn("operations: refused a request on a connection with no door tag, "+
				"so it cannot be judged by any door's credentials",
				"path", b.limits.Loggable(r.URL.Path), "remote", hostOf(r.RemoteAddr))
			http.Error(w, "this connection carries no door, so nothing can authenticate it\n",
				http.StatusUnauthorized)
			return
		}
		operators := ops.forDoor(r)
		// **No password file is not the same as no rules**, and treating it
		// so was a hole: a certificate names its holder whether or not a
		// file exists, and returning early here handed every route to
		// anyone the certificate authority had ever signed for. What the
		// absent file means is that nobody is scoped - so a named caller
		// gets the unscoped answer below, and an unnamed one is trusted by
		// the door it arrived at, which validation has already made
		// loopback or a socket.
		name, proved := principal(r, operators)
		// **A name with a NUL or a control character is nobody's**, from
		// whichever of the four sources it came (ValidName): refused rather
		// than served nameless, so a certificate or a proxy cannot hand this
		// door a name that rewrites the line logging it.
		if name != "" && !ValidName(name) {
			b.log.Warn("operations: refused a request whose name holds a NUL or a control character",
				"principal", b.limits.Loggable(strconv.QuoteToASCII(name)), "path", b.limits.Loggable(r.URL.Path),
				"remote", hostOf(r.RemoteAddr))
			http.Error(w, "that name holds a control character, so it is not a name\n", http.StatusForbidden)
			return
		}
		if operators == nil && !proved {
			// **A door with no credential answers to an address, not to a
			// name.** This is the one branch that serves a caller nobody
			// named, and validation has already confined it to loopback or
			// a socket - which is unreachable from the network, and is not
			// the same as being unreachable from the internet. A page on
			// another site whose domain re-resolves to 127.0.0.1 becomes
			// same-origin to this listener and reads it through whoever's
			// browser has it open: the catalogue, the sessions, the ACL,
			// the user list and the resolved configuration. Nothing can be
			// changed that way - every route here is a GET - but all of it
			// can be read, and the operator's browser is the one asking.
			//
			// **The Host header is the one thing that page cannot forge**,
			// because a browser sends the name it was loaded by. So a name
			// is refused here and an address is served.
			//
			// **Only here.** A listener with a password file, a client
			// certificate or a proxy that names its caller never reaches
			// this branch, so a viewer or a scraper calling a real
			// deployment by its hostname is untouched - and by the
			// configuration rule above, anything off this machine has one
			// of those. A socket is exempt for the reason it is exempt
			// everywhere else in this file: no browser can open one, and
			// whatever proxy is in front of it sends the name it was asked
			// for.
			//
			// `/health` is deliberately not behind this. It is never
			// authenticated, so it never reaches here at all - and it
			// carries nothing about this broker's contents by design,
			// precisely so that a probe which cannot hold a credential can
			// read it from wherever it runs.
			if gated, _ := r.Context().Value(gatedDoorKey{}).(bool); !gated && askedByName(r) {
				b.log.Warn("operations: an unauthenticated listener was asked for by name",
					"host", b.limits.Loggable(r.Host),
					"path", b.limits.Loggable(r.URL.Path),
					"remote", hostOf(r.RemoteAddr))
				// The name is not written back, for the reason refuseRoute
				// does not write a credential back: it is a string the
				// caller chose. The log line has it, bounded.
				http.Error(w, "this listener has no credential configured, so it "+
					"answers to an address and not to a name.\n"+
					"A browser sends the name it was loaded by, so a name here is a "+
					"page on another site reading this broker through a browser.\n"+
					"Reach it by address, or name the operators in "+
					"broker.operations.password_file, which lifts this.\n",
					http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		// **A name the broker did not check a password for still names
		// somebody.** A client certificate an authority signed, or a
		// principal a proxy set on a door only this machine can open, are
		// both answers to "who is asking" - so they reach here rather than
		// falling through to a 401 that would tell a correctly configured
		// deployment its credential was rejected.
		//
		// What such a name does *not* carry is what it may reach, because
		// that lives in the password file and this name may not be in it.
		// **A principal with no entry reaches /metrics and nothing else**:
		// enough to be scraped, which is what a certificate or a proxy is
		// almost always arranged for, and not the routes that name a
		// device. Widening it is an entry in the file, which is the same
		// answer as for everybody else.
		if proved {
			// **A caller this door proved but could not name gets what a
			// named stranger gets, never more.** A certificate with no
			// name in it is still a certificate the authority signed.
			if name != "" && operators.Known(name) {
				if operators.Reaches(name, r.URL.Path) {
					b.notePrincipal(r, name)
					next.ServeHTTP(w, r)
					return
				}
			} else if r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}
			b.log.Warn("operations: a principal that does not reach this route",
				"user", b.limits.Loggable(name),
				"path", b.limits.Loggable(r.URL.Path),
				"remote", hostOf(r.RemoteAddr))
			b.refuseRoute(w, r)
			return
		}
		// The realm is what a browser puts in its prompt and what a scraper
		// logs; naming saguin rather than the deployment keeps it from
		// carrying anything about whose broker this is.
		w.Header().Set("WWW-Authenticate", `Basic realm="saguin"`)
		b.log.Warn("operations: a credential was rejected", "remote", hostOf(r.RemoteAddr))
		http.Error(w, "", http.StatusUnauthorized)
	})
}

// inspectionRows is the most rows either inspection route returns.
//
// **A cap is not optional here.** Invariant 13 is about everything that
// accumulates, and a response body accumulates: without one, a route that
// lists positions is a route that can be asked to read a fleet of ten
// thousand into memory and out over a socket.
//
// **Worst-first ordering is what makes the cap safe.** The rows that fall
// off the end are the ones nobody was looking for - the consumers that have
// caught up, the jobs that arrived last. And every response says how many
// exist, because a caller shown a hundred of three hundred and not told so
// believes it has seen the fleet.
//
// It is a constant rather than a setting. What an operator would tune it
// for is "show me more", and the answer to that is that the interesting
// rows are already at the top.
const inspectionRows = 100

// consumersHandler answers "which of my devices is behind".
//
// The catalogue cannot: `saguin_channel_consumer_position_min` is one
// number per channel, so a fleet of three hundred produces one reading and
// one straggler looks exactly like a fleet that has stopped.
func (b *Broker) consumersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	type position struct {
		Reader   string `json:"reader"`
		Offset   uint64 `json:"offset"`
		Behind   uint64 `json:"behind"`
		LastSeen string `json:"last_seen"`
	}
	type channelRows struct {
		Channel    string     `json:"channel"`
		NextOffset uint64     `json:"next_offset"`
		Consumers  int        `json:"consumers"`
		Returned   int        `json:"returned"`
		Positions  []position `json:"positions"`
	}
	out := struct {
		Channels []channelRows `json:"channels"`
	}{Channels: []channelRows{}}

	b.mu.Lock()
	logs := make(map[string]LogStore, len(b.logs))
	for name, lg := range b.logs {
		logs[name] = lg
	}
	b.mu.Unlock()

	for _, name := range sortedKeys(logs) {
		lg := logs[name]
		rows, total, err := lg.ListPositions(inspectionRows)
		if err != nil {
			b.log.Error("operations: cannot read the stored positions",
				"channel", name, "error", err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		next := lg.Next()
		ch := channelRows{Channel: name, NextOffset: next, Consumers: total,
			Returned: len(rows), Positions: []position{}}
		for _, p := range rows {
			// **How far behind, computed here rather than left to the
			// reader.** It is the number somebody is actually after, and
			// `next - offset` is a subtraction they would otherwise do in
			// their head, once per row, at the moment they are in a hurry.
			var behind uint64
			if next > p.Offset {
				behind = next - p.Offset
			}
			ch.Positions = append(ch.Positions, position{
				Reader: p.Reader, Offset: p.Offset, Behind: behind,
				LastSeen: p.LastSeen.UTC().Format(time.RFC3339),
			})
		}
		out.Channels = append(out.Channels, ch)
	}
	writeJSON(w, b.log, out)
}

// DeclaredSlice is one filter's partition declaration as the operations
// consumers route answers it.
//
// **Declared beside the route rather than with the function that fills
// it**, which is where it was first written: the check that holds this
// handler to RFC 0005 reads field names out of this file, so a response
// type living elsewhere is one the document can promise and nothing sends.
type DeclaredSlice struct {
	Filter  string `json:"filter"`
	Count   int    `json:"count"`
	Indices []int  `json:"indices"`
}

// sessionsHandler answers the question no other route does: **who is
// connected, and who is holding a session with nothing behind it**.
//
// `saguin_connections` and `saguin_sessions_offline` are two numbers, and
// three hundred devices with two hundred connected is either a rota or a
// hundred that have stopped calling. The counts cannot tell those apart and
// this can, because it names them.
//
// **It answers nothing about positions**, which `/consumers` already does
// and keys by the same client id. A second route reading them would be a
// second answer to one question, kept in step with the first for ever - the
// rule this document draws around records, applied to a number.
func (b *Broker) sessionsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	type session struct {
		ClientID      string `json:"client_id"`
		Connected     bool   `json:"connected"`
		User          string `json:"user"`
		Protocol      int    `json:"protocol"`
		Listener      string `json:"listener"`
		Remote        string `json:"remote"`
		Keepalive     int    `json:"keepalive"`
		ExpiresAfter  uint32 `json:"expires_after"`
		Clean         bool   `json:"clean_start"`
		Subscriptions int    `json:"subscriptions"`

		// **Where the client-chosen numbers live.** A partition count and
		// its indices are declared by the client, so RFC 0005's Labels rule
		// keeps them out of every metric - a series keyed by one is a series
		// count chosen by whoever connects. This route is not a metric and
		// has no cardinality budget: it is answered on demand, capped like
		// every other list here, and it is the one place an operator can see
		// who declared what.
		//
		// **A list of objects rather than a map keyed by filter**, so that
		// every key in this response is a field name saguin chose and every
		// client-chosen string is a value. A filter as a JSON key puts an
		// arbitrary string where a reader expects a schema, and the check
		// that holds this route to RFC 0005 reads the document's keys as
		// field names - correctly, which is how it caught the first shape.
		//
		// Omitted entirely for a session that declared nothing, which is
		// almost all of them, so the ordinary answer is the shape it always
		// was.
		Partitions []DeclaredSlice `json:"partitions,omitempty"`
	}
	out := struct {
		Sessions  []session `json:"sessions"`
		Total     int       `json:"total"`
		Connected int       `json:"connected"`
		Offline   int       `json:"offline"`
		Returned  int       `json:"returned"`
	}{Sessions: []session{}}

	all := []session{}
	for _, cl := range b.srv.Clients.GetAll() {
		// **The substrate's own inline client is in this table and is
		// nobody's session**: it is how the broker publishes on its own
		// behalf. Counting it made a broker with two clients connected
		// report one session already offline, which is the defect the
		// metrics scrape carries this same guard for.
		if cl.Net.Inline {
			continue
		}
		s := session{
			ClientID:  cl.ID,
			Connected: !cl.Closed(),
			// Bounded because both come back in the body, and an unbounded
			// string echoed into a response is a way to put somebody else's
			// text on an operator's screen. The client id is already bounded
			// by `limits.max_client_id_length` at CONNECT; a user name is
			// not bounded anywhere on a broker with no password file, which
			// is exactly the broker where anything may be sent.
			User:          truncate(string(cl.Properties.Username), maxExplainedName),
			Protocol:      int(cl.Properties.ProtocolVersion),
			Listener:      cl.Net.Listener,
			Remote:        truncate(cl.Net.Remote, maxExplainedName),
			Keepalive:     int(cl.State.Keepalive),
			ExpiresAfter:  cl.Properties.Props.SessionExpiryInterval,
			Clean:         cl.Properties.Clean,
			Subscriptions: cl.State.Subscriptions.Len(),
			Partitions:    b.declaredBy(cl.ID),
		}
		if s.Connected {
			out.Connected++
		} else {
			out.Offline++
		}
		all = append(all, s)
	}
	out.Total = len(all)

	// **Offline first, then by client id.** The row cap cuts the end off,
	// and the rows nobody was looking for are the ones behaving: a fleet of
	// three hundred connected and two holding a session with nothing behind
	// them is a page about those two. Within each group the order is the
	// client id, so two scrapes of an unchanged broker read the same.
	sort.Slice(all, func(i, j int) bool {
		if all[i].Connected != all[j].Connected {
			return !all[i].Connected
		}
		return all[i].ClientID < all[j].ClientID
	})
	if len(all) > inspectionRows {
		all = all[:inspectionRows]
	}
	out.Sessions = all
	out.Returned = len(all)
	writeJSON(w, b.log, out)
}

// truncate bounds a string that is about to be written into a response.
//
// **By runes rather than by bytes**, so a name cut in the middle of a
// multi-byte character does not leave a broken one in the JSON - which is
// the sort of thing a reader blames on their own tooling.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// queueHandler answers "what is actually stuck in that queue".
//
// `saguin_queue_depth` says four hundred and nothing says what. The only
// other way to look is to consume, which takes work from the workers the
// viewer was sent to diagnose - so this leases nothing and carries no
// payloads.
func (b *Broker) queueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/operations/queues/")
	if name == "" || strings.Contains(name, "/") {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	b.mu.Lock()
	q := b.queues[name]
	b.mu.Unlock()
	if q == nil {
		// **Not the names of the queues that do exist.** This route is
		// behind a credential, but a 404 that lists the configuration is a
		// route that returns the configuration, which RFC 0005 refuses.
		http.Error(w, "", http.StatusNotFound)
		return
	}

	rows, total, err := q.Unresolved(inspectionRows)
	if err != nil {
		b.log.Error("operations: cannot read the unresolved work",
			"channel", name, "error", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	type record struct {
		Offset     uint64 `json:"offset"`
		Topic      string `json:"topic"`
		Attempts   int    `json:"attempts"`
		State      string `json:"state"`
		Holder     string `json:"holder,omitempty"`
		LeaseUntil string `json:"lease_until,omitempty"`
		FirstSeen  string `json:"first_seen,omitempty"`
		LastSeen   string `json:"last_seen,omitempty"`
	}
	out := struct {
		Channel    string   `json:"channel"`
		Unresolved int      `json:"unresolved"`
		Returned   int      `json:"returned"`
		Records    []record `json:"records"`
	}{Channel: name, Unresolved: total, Returned: len(rows), Records: []record{}}

	states := map[store.State]string{
		store.Available: "waiting", store.Delivering: "delivering", store.Leased: "leased",
	}
	for _, it := range rows {
		rec := record{Offset: it.Offset, Topic: it.Topic, Attempts: it.Attempts,
			State: states[it.State], Holder: it.Holder}
		if !it.LeaseUntil.IsZero() {
			rec.LeaseUntil = it.LeaseUntil.UTC().Format(time.RFC3339)
		}
		if !it.FirstSeen.IsZero() {
			rec.FirstSeen = it.FirstSeen.UTC().Format(time.RFC3339)
		}
		if !it.LastSeen.IsZero() {
			rec.LastSeen = it.LastSeen.UTC().Format(time.RFC3339)
		}
		out.Records = append(out.Records, rec)
	}
	writeJSON(w, b.log, out)
}

// writeJSON is the one writer for these routes, so two of them cannot come
// to different answers about the content type or about what a failure
// halfway through looks like.
func writeJSON(w http.ResponseWriter, log *slog.Logger, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		log.Error("operations: cannot write the answer", "error", err)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	// Marshalled whole before anything is written, so a failure is a 500
	// rather than a 200 carrying half a document - which a reader cannot
	// tell from a fleet that really is that size.
	_, _ = w.Write(body)
}

// hostOf is the address a request came from, without the port.
//
// The port changes on every connection, so keeping it makes two attempts
// from one machine look like two machines - and the question somebody
// brings to this log line is which machine is failing to authenticate.
func hostOf(remote string) string {
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	return remote
}

// listenUnix binds a socket file and gives it its permissions, through
// listeners.ListenUnix like every other Unix socket saguin opens: a socket
// left behind by a broker that did not stop cleanly is replaced, one another
// saguin is serving is refused, and anything at the path that is not a socket
// is refused and kept.
//
// Between the bind creating the file and the chmod there is a window where
// its permissions are whatever the umask made them, and the containing
// directory is the real defence against that - the same trade the MQTT
// socket makes.
func listenUnix(path string, mode os.FileMode) (net.Listener, error) {
	return listeners.ListenUnix(path, mode)
}

// configSections is what `?section=` may name, and where each one lives in
// the resolved document.
//
// **A named vocabulary rather than "whatever key you type"**, for the
// reason CheckScopes gives about a scope naming no route: a section that
// matched nothing would answer 200 with an empty object, which reads as
// "there are no providers configured" about a broker that has three. A name
// this does not hold is a 400 listing the ones that exist.
//
// **And the names are the operator's rather than the document's.**
// `providers` is what somebody asks for; `broker.storage.providers` is
// where it happens to sit, and a caller should not have to know the shape
// of the tree to ask a question about it. That is also why this is a table
// rather than a path parser - every answerable question is written here,
// and one nobody wrote cannot be asked by accident.
var configSections = map[string][]string{
	"bridges":    {"bridges"},
	"channels":   {"channels"},
	"limits":     {"broker", "limits"},
	"listeners":  {"broker", "mqtt", "listen"},
	"operations": {"broker", "operations"},
	"providers":  {"broker", "storage", "providers"},
	"storage":    {"broker", "storage"},
}

// configSectionNames is what a refusal lists, joined once from the table
// above so that the two cannot drift apart. Deterministic, because
// sortedKeys sorts: two refusals name the sections in the same order.
var configSectionNames = strings.Join(sortedKeys(configSections), ", ")

// configHandler answers `/v1/operations/config` with the configuration the
// broker resolved at startup (RFC 0005).
//
// **It is a report and not a configuration**, which is the difference from
// `--check-config --output` and is why this is JSON while that is YAML.
// What the flag prints loads again - that is the property RFC 0004's
// promotion runbook rests on, and it is a shell job on a box somebody has
// access to. What this answers is "what is that broker over there actually
// running", for a caller that has a credential and no shell, and for that a
// document `jq` reads beside the three other `/v1` routes is worth more
// than one that could be fed back.
//
// **No secret is in it, and that is by construction rather than by
// filtering.** The schema holds no credential anywhere: a password file is
// a path, an ACL file is a path, and a bridge authenticates upstream with a
// client certificate whose key is a path too. So there is no redaction pass
// here to forget to update when a key is added - there is a test that reads
// a real password file's hash and asserts it appears in no response, which
// is the check that keeps being true as the schema grows.
func (b *Broker) configHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	doc := b.configDoc.Load()
	if doc == nil {
		// A wiring failure, not a configuration. Saying so is the whole
		// difference between this and an empty object, which a caller
		// cannot tell from a broker that configures nothing.
		b.log.Error("operations: /v1/operations/config was served by a broker that was " +
			"never given its configuration; this is a wiring fault in the binary")
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	wanted := r.URL.Query()["section"]
	if len(wanted) == 0 {
		writeJSON(w, b.log, *doc)
		return
	}

	out := map[string]any{}
	for _, name := range wanted {
		path, ok := configSections[name]
		if !ok {
			// **The name the caller sent is not echoed back.** It is a
			// query string, so it is unbounded and attacker-chosen, and
			// reflecting one into a response body is how a refusal becomes
			// a way to put somebody else's text on an operator's screen.
			// The caller knows what it asked for; what it does not know is
			// the vocabulary, so that is what the refusal carries.
			http.Error(w, "unknown section: saguin serves "+configSectionNames+"\n",
				http.StatusBadRequest)
			return
		}
		// A section the configuration does not use is present and empty
		// rather than missing, so a caller asking for `bridges` on a broker
		// with none gets an answer rather than having to tell an absent key
		// from a name it spelled wrongly. The 400 above is what tells it
		// that.
		out[name] = map[string]any{}
		if v := descend(*doc, path); v != nil {
			out[name] = v
		}
	}
	writeJSON(w, b.log, out)
}

// descend walks a path of keys into a decoded document, or nil where the
// path is not there.
func descend(doc map[string]any, path []string) any {
	var cur any = doc
	for _, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = m[key]
		if !ok {
			return nil
		}
	}
	return cur
}

// aclHandler answers "why can device-7 not publish?" over HTTP, which is
// the question `saguin --acl` answers at a shell (RFC 0005).
//
// **The same body the flag prints, from the same function.** Roles are what
// save an operator the work of a rule per device, and indirection is what
// they cost: the answer is a pattern match and two lookups, done in
// somebody's head, at the moment a device that should be working is not.
// Two tools answering that with two derivations would be two answers, and
// the operator comparing them is exactly the person who must not get them -
// so the shape and the derivation both live in internal/authz and this
// route renders what that returns.
//
// **A name the file does not match is granted nothing, and that is an
// answer rather than a refusal.** Users are patterns, so there is no list
// of real names to check a query against: `device-99` on a file naming
// `device-*` is granted what that pattern grants, and `printer-3` on the
// same file is granted nothing at all. Neither is a typo the broker can
// detect, which is why this route cannot answer 404 the way an unknown
// `?section=` answers 400 - and why the body always carries the patterns
// that matched and the patterns the file holds, so a caller looking at an
// empty grant list can see whether it asked the wrong question.
func (b *Broker) aclHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	src := b.acl.Load()
	if src == nil {
		b.log.Error("operations: /v1/operations/acl was served by a broker that was never " +
			"told about its authorization file; this is a wiring fault in the binary")
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	// **No user is a refusal, not a listing.** This route answers about
	// one name; a body listing every pattern instead would be a different
	// route wearing this one's path, and a caller that forgot the parameter
	// would read it as the answer.
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "name the user: /v1/operations/acl?user=<name>\n", http.StatusBadRequest)
		return
	}
	if len(user) > maxExplainedName {
		// Bounded because it comes back in the body, and an unbounded
		// string echoed into a response is a way to put somebody else's
		// text on an operator's screen.
		http.Error(w, "that user name is too long to be one\n", http.StatusBadRequest)
		return
	}
	clientID := r.URL.Query().Get("client_id")
	if len(clientID) > maxExplainedName {
		http.Error(w, "that client id is too long to be one\n", http.StatusBadRequest)
		return
	}

	// **No acl_file is a state with an answer**, unlike the nil above. An
	// empty grant list here would be the JSON spelling of "granted
	// nothing" about a broker where every authenticated client may do
	// anything, which is the reverse of the truth.
	if src.file == nil {
		writeJSON(w, b.log, authz.NoACLFile())
		return
	}
	writeJSON(w, b.log, authz.Explain(src.file, src.path, user, clientID))
}

// maxExplainedName bounds what `/v1/operations/acl` will echo. It is the
// longest client id MQTT allows saguin to accept and the longest user name
// a password file can hold, rounded to something an operator would never
// hit - a name longer than this cannot be a client's, so refusing it costs
// nobody an answer and stops the route becoming a way to reflect an
// arbitrary string.
const maxExplainedName = 1024

// usersHandler answers "who may connect to this broker" (RFC 0005).
//
// **The names come from what the broker is admitting, not from a file read
// again.** `SetCredentials` and `SetListenerCredentials` already hold every
// password file this process loaded - the same values `credentialsFor`
// consults on every CONNECT - so this route cannot disagree with who
// actually gets in. Re-reading the paths would produce a second answer, and
// the second answer would be right about the disk and wrong about the
// broker the moment somebody edited a file under a running process.
//
// **The names alone are never the answer, so two fields travel with them.**
// A door admitting anonymous clients lets in names that are in no file at
// all. A door requiring a client certificate is worse than that in both
// directions: none of the names listed can connect, because they have
// passwords and the handshake wants a certificate, and whoever the
// authority signed can, with no password and no entry anywhere. A body
// carrying only the names would read "closed to all but these three" about
// a door standing open to a CA.
//
// **What it cannot do is list the certificate holders**, because saguin
// does not know them - the authority mints those. So it says which kind of
// door each one is, which is the honest and the useful thing: an operator
// can tell "these names, by password" from "whoever your CA signed".
//
// **Only the clients**, never `broker.operations.password_file`. That file
// names who may read the broker and which routes each of them reaches, and
// handing it out over the very interface it guards widens what one leaked
// credential is worth. An operator wanting it has the file.
//
// No hash is here, and none can be: passwd.File.Users returns names.
func (b *Broker) usersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	type door struct {
		Users            []string `json:"users"`
		AnonymousAllowed bool     `json:"anonymous_allowed"`
		Certificate      string   `json:"certificate"`
	}
	out := struct {
		Users            []string        `json:"users"`
		AnonymousAllowed bool            `json:"anonymous_allowed"`
		Listeners        map[string]door `json:"listeners"`
	}{Users: []string{}, Listeners: map[string]door{}}

	if f := b.credentials.Load(); f != nil {
		out.Users = sorted(f.Users())
	}
	out.AnonymousAllowed = b.anonymous.Load()

	// **Every configured door, always, and each one a complete answer.**
	// This said "only the doors with a file of their own", and the binary
	// never did that: main.go registers every listener on purpose, so that
	// nothing there can disagree with Auth.Resolve about which are special.
	// The rule was also the wrong one to want - a door writing
	// `allow_anonymous: true` and no file of its own would have been absent
	// under it, sending the reader to the broker-wide pair, which says the
	// opposite about that door. A row per door has no fallback to get
	// wrong, and with the certificate field beside the names it is not the
	// repetition the old rule was avoiding.
	b.mu.Lock()
	untold := ""
	for id, c := range b.listenerAuth {
		// **A door nobody told about is a wiring fault, not a door that
		// examines no certificate.** Defaulting to `none` here was how the
		// binary guard came to compare nothing: the field was never empty,
		// so an assertion on emptiness could not fire, and a door the
		// binary had failed to describe reported a fact about itself that
		// nobody established - the exact misreading this field exists to
		// prevent. `/config` already answers an unwired broker with a 500
		// and a log line rather than an empty document; this is the same
		// case.
		if c.certificate == "" {
			untold = id
			break
		}
		d := door{Users: []string{}, AnonymousAllowed: c.anonymous, Certificate: c.certificate}
		if c.users != nil {
			d.Users = sorted(c.users.Users())
		}
		out.Listeners[id] = d
	}
	b.mu.Unlock()
	if untold != "" {
		b.log.Error("operations: a listener was registered with no certificate state, so "+
			"/v1/operations/users cannot say what it does about client certificates; "+
			"this is a wiring fault in the binary", "listener", untold)
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	writeJSON(w, b.log, out)
}

// sorted is a copy in a stable order, so two scrapes of one broker answer
// with the same list and a diff between two brokers is about the users
// rather than about map iteration.
func sorted(in []string) []string {
	out := append([]string{}, in...)
	sort.Strings(out)
	return out
}

// operationsRoute is one path the operations listener serves behind the
// operator credential.
type operationsRoute struct {
	path    string
	handler http.Handler
}

// operationsRoutes is every credentialed route, and the only place one is
// added. `/health` is not here: it is unauthenticated, so nothing scopes it.
//
// **One table, because the scope vocabulary has to match it.** A password
// file's scope names routes from passwd.KnownRoutes, which lives apart so
// `--passwd` can check a file with no configuration - and two routes were
// once added to the listener and not to that list, so no operator could be
// scoped to either. TestEveryOperationsRouteCanBeScoped holds this table and
// that list to each other in both directions.
func (b *Broker) operationsRoutes(operators *Operators, minScrape time.Duration) []operationsRoute {
	return []operationsRoute{
		{"/metrics", b.metricsHandler(minScrape)},

		// **`/v1/` is saguin's own, and that is why it is versioned.**
		// `/health` answers a probe and `/metrics` answers a scraper: two
		// contracts saguin conforms to and does not own, which is why neither
		// carries a version and neither ever changes shape. What is under
		// `/v1/` is saguin's, will grow, and will one day change - so it says
		// which it is, and a `/v2` can stand beside it while people move.
		//
		// **It is not metrics and must not be scraped.** These answer with a
		// list rather than a number, on demand rather than on a timer, and
		// they carry what the catalogue is deliberately closed against: a
		// client id is a string a client chose, so a series keyed by one is a
		// series count chosen by whoever connects.
		{"/v1/operations/acl", http.HandlerFunc(b.aclHandler)},
		{"/v1/operations/config", http.HandlerFunc(b.configHandler)},
		{"/v1/operations/consumers", http.HandlerFunc(b.consumersHandler)},
		{"/v1/operations/users", http.HandlerFunc(b.usersHandler)},
		{"/v1/operations/sessions", http.HandlerFunc(b.sessionsHandler)},
		{"/v1/operations/refused", http.HandlerFunc(b.refusedHandler)},
		{"/v1/operations/position-lost", http.HandlerFunc(b.positionLostHandler)},
		{"/v1/operations/queues/", http.HandlerFunc(b.queueHandler)},
	}
}

// refusals is the bounded record of clients this broker refused, at CONNECT
// or by ending the connection, on any protocol, and it is what
// `/v1/operations/refused` reads.
//
// **A counter says how much and never which one** (RFC 0005 "Labels, and
// where the catalogue stops"). A fleet in a
// reconnect loop shows on `saguin_connections_refused_total` as a rate that
// will not come down, and an operator's next question is which device - a
// question a metric must never answer, because a client id is a string a
// client chose and a series keyed by one is a series count chosen by
// whoever connects. So it is a route, behind the same credential as the
// others, capped at the same hundred rows.
//
// **The accumulator is bounded too, and that is invariant 13 rather than
// tidiness.** Client ids arrive from strangers: a fleet that takes a fresh
// one every boot would otherwise write an unbounded map into a broker whose
// whole point is that it runs on one small box.
//
// **What it does when full is worst-first**, and the shape is chosen so the
// bad case cannot hide. A newcomer displaces an entry that has flapped
// exactly once, never one that has flapped twice - so a device that
// disconnects repeatedly is safe once it has done so a second time, and
// what is evicted is always a one-off. If every entry has flapped more than
// once the newcomer is refused and counted in `beyond`, which is honest:
// the operator has a thousand genuine repeat offenders to look at and one
// more name would not help.
//
// **And so are the strings in it.** A client id and a user name each arrive
// in a CONNECT from whoever connected, up to 65,535 bytes, and this record
// keeps them after the connection is gone. Kept whole, a thousand refused
// CONNECTs with 65,000-byte user names grew the process from 13.6 MB to
// 110.6 MB, measured on the binary. record bounds both on the way in.
type refusals struct {
	mu      sync.Mutex
	seen    map[string]*refusal
	beyond  uint64
	ceiling int
}

type refusal struct {
	user     string
	reason   string
	count    uint64
	lastSeen time.Time
}

// refusalCeiling is how many distinct clients the record holds. A
// thousand names is more than an operator will read and small enough that
// the map cannot be a memory problem on the hardware saguin targets.
const refusalCeiling = 1000

func newRefusals() *refusals {
	return &refusals{seen: map[string]*refusal{}, ceiling: refusalCeiling}
}

// record adds one refusal, with the client id and the user name passed
// through bound first. bound is a parameter rather than something a caller
// does beforehand, so that no caller can keep either whole.
func (f *refusals) record(clientID, user, reason string, bound func(string) string) {
	if f == nil {
		return
	}
	clientID, user = bound(clientID), bound(user)
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.seen[clientID]; ok {
		e.count++
		e.reason = reason
		e.user = user
		e.lastSeen = time.Now()
		return
	}
	if len(f.seen) >= f.ceiling {
		// Displace a one-off if there is one; otherwise say so and stop.
		var victim string
		for id, e := range f.seen {
			if e.count == 1 {
				victim = id
				break
			}
		}
		if victim == "" {
			f.beyond++
			return
		}
		delete(f.seen, victim)
		f.beyond++
	}
	f.seen[clientID] = &refusal{user: user, reason: reason, count: 1, lastSeen: time.Now()}
}

// rows returns the record worst-first, at most n of them, with how many are
// held and how many were never admitted.
func (f *refusals) rows(n int) (out []refusalRow, tracked int, beyond uint64) {
	if f == nil {
		return nil, 0, 0
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	all := make([]refusalRow, 0, len(f.seen))
	for id, e := range f.seen {
		all = append(all, refusalRow{
			ClientID: id, User: e.user, Reason: e.reason, Count: e.count,
			LastSeen: e.lastSeen.UTC().Format(time.RFC3339),
		})
	}
	// **Worst first, and by name where two are equal**, so that two reads of
	// an unchanged record answer the same way - a route whose row order
	// wobbles is one an operator cannot diff.
	sort.Slice(all, func(i, j int) bool {
		if all[i].Count != all[j].Count {
			return all[i].Count > all[j].Count
		}
		return all[i].ClientID < all[j].ClientID
	})
	tracked, beyond = len(all), f.beyond
	if len(all) > n {
		all = all[:n]
	}
	return all, tracked, beyond
}

type refusalRow struct {
	ClientID string `json:"client_id"`
	User     string `json:"user"`
	Reason   string `json:"reason"`
	Count    uint64 `json:"count"`
	LastSeen string `json:"last_seen"`
}

// passings is the bounded record of readers the retention floor passed, and
// it is what `/v1/operations/position-lost` reads.
//
// **A counter says how much and never which one** (RFC 0005 "Labels, and
// where the catalogue stops"), and this is that
// rule applied to the loss invariant 1 is about.
// `saguin_channel_position_lost_total` says a channel passed its readers
// two thousand times; the operator's next question is which devices lost
// telemetry and how much of it, and a metric must never answer that - a
// client id is a string a client chose, so a series keyed by one is a
// series count chosen by whoever connects.
//
// **Keyed by reader AND channel**, unlike the refusal record beside it. A
// device reading three channels can be passed on all three, at different
// positions and for different amounts, and keyed by reader alone the third
// one would overwrite the first two and report the fleet as healthier than
// it is.
//
// **The reader carries its scheme**, as it does in the store and on
// /v1/operations/consumers: `mqtt:` for a session and `bridge:` for an
// outbound rule. store.BridgeReader names the reason - a client may call
// itself `bridge:head-office`, MQTT putting almost no rule on a client id -
// and this record has to keep them apart for exactly the reason the store
// does. Given a raw client id here, that device and the link of that name
// share a key: the rows merge, the counts sum, and an operator reads one
// row that attributes the link's missing records to a device.
//
// **Bounded, and that is invariant 13 rather than tidiness**, for the same
// reason the refusal record is: client ids arrive from strangers, and each
// string is held to the bound a log line keeps them to. It is held in
// memory and emptied by a restart, deliberately - the counter it stands in
// front of is a process-lifetime number too, and a record that outlived it
// would put the two permanently out of step, so that the sum of these rows
// could never be reconciled against the counter again. The durable record
// is the broker's log, which writes a line at every one of these sites.
type passings struct {
	// **`rowsMu` rather than `mu`**, following the package's convention for
	// a lock that is not the broker's - bridgeMu, watchMu, storageMu - and
	// here the name is load-bearing rather than tidy.
	// TestNoClientLockSectionReachesTheBrokerLock reads `<ident>.mu.Lock()`
	// as the broker-wide lock and over-approximates on purpose, which is the
	// safe direction for a rule against deadlock; it also cannot see an
	// unlock nested inside a switch, so it treats the rest of pumpBatch as
	// still holding a client's cmu. A field called `mu` here would read to
	// that walk as b.mu taken inside cmu, on a site that is in fact past the
	// unlock and takes a leaf lock: this one is taken alone, and nothing
	// takes cmu while holding it.
	rowsMu  sync.Mutex
	seen    map[passingKey]*passing
	beyond  uint64
	ceiling int
	log     *slog.Logger
}

// passingKey is one reader, scheme and all, on one channel.
type passingKey struct{ reader, channel string }

// passingKind is which kind of reader a row is about. A named type over a
// bare string because the set is closed and small, and the three values
// decide both what an operator is told and which scheme the reader's name
// must carry: a fourth spelled by hand at a call site would read as a kind
// with no scheme and collide silently, where a fourth constant declared
// here has to be given one below.
type passingKind string

const (
	kindConsumer passingKind = "consumer" // overtaken while connected; cannot be told
	kindSession  passingKind = "session"  // passed while away; told with Session Present = 0
	kindBridge   passingKind = "bridge"   // an outbound rule; the peer is told nothing
)

type passing struct {
	kind     passingKind
	reported bool
	count    uint64 // occurrences: the floor passes a laggard repeatedly
	missed   uint64 // records lost across all of them
	position uint64 // at the most recent passing
	floor    uint64 // at the most recent passing
	lastSeen time.Time
}

// passingCeiling is how many (reader, channel) pairs the record holds. The
// refusal record's thousand, for the same argument: more names than an
// operator will read, and small enough that the map cannot be a memory
// problem on the hardware saguin targets.
const passingCeiling = 1000

// schemeFor is the reader scheme a kind's names must carry, or "" for a
// kind this switch has not been taught. "" is not a permission: add treats
// it as the loudest answer of the three, because a kind that reaches this
// record without a scheme is how two readers come to share a row.
func schemeFor(kind passingKind) string {
	switch kind {
	case kindConsumer, kindSession:
		return store.ReaderPrefixMQTT
	case kindBridge:
		return store.ReaderPrefixBridge
	}
	return ""
}

func newPassings(log *slog.Logger) *passings {
	return &passings{seen: map[passingKey]*passing{}, ceiling: passingCeiling, log: log}
}

// add records one passing. The reader name is passed through bound first,
// so that no caller can keep a stranger's string whole.
//
// **Named `add` rather than `record` because the name is load-bearing.**
// TestNoClientLockSectionReachesTheBrokerLock matches methods by name and
// over-approximates on purpose - the safe direction for a rule against
// deadlock - and `Broker.record` takes b.mu, so a second `record` here
// reads to that walk as the broker-wide lock being taken inside a client's
// cmu. This lock is a leaf: it is taken alone and nothing takes cmu while
// holding it.
//
// **`missed` accumulates and the positions do not.** How much a reader has
// lost altogether is the operator's question and the order this record is
// read in; where it was the last time is the detail that explains the most
// recent row. Summing the positions would be meaningless and keeping only
// the last `missed` would report a reader passed a thousand times as having
// lost whatever the last sweep took.
func (p *passings) add(reader, channel string, kind passingKind, reported bool,
	position, floor uint64, bound func(string) string) {
	if p == nil || floor <= position {
		return
	}
	// **The scheme is checked here and never applied here.** store.go keeps
	// the prefix so that one rule governs what is written into a snapshot or
	// a database, and a second scheme-applier in the broker is exactly the
	// drift that comment exists to prevent. So this asks whether the name it
	// was handed carries the scheme its kind implies, and says so when it
	// does not: a reader kind added later and wired up unprefixed would
	// otherwise share a key with whatever client calls itself by that name,
	// silently, which is the defect this check is the tripwire for.
	//
	// It still records. A row under a suspect name is worth more to an
	// operator than a loss that went unnamed, and the log line is what says
	// the record cannot be trusted to keep the two apart.
	switch want := schemeFor(kind); {
	case want == "":
		// **The loudest case, not the skipped one.** schemeFor answers ""
		// for a kind its switch has not been taught, so a guard that only
		// checked a non-empty answer would let a fourth passingKind -
		// declared here and never given a scheme - through in silence,
		// which is the one mistake this whole check exists to catch. Read
		// the other way round it is the type earning its keep: a new kind
		// says so the first time it fires.
		p.log.Error("a reader kind with no declared scheme reached the position-lost "+
			"record, so its names cannot be told from a client's and may share a row "+
			"with one",
			"reader", bound(reader), "kind", string(kind))
	case !strings.HasPrefix(reader, want):
		p.log.Error("a reader the retention floor passed does not carry the scheme its "+
			"kind implies, so /v1/operations/position-lost cannot be relied on to tell "+
			"it from a client of the same name",
			"reader", bound(reader), "kind", string(kind), "expected_prefix", want)
	}
	k := passingKey{reader: bound(reader), channel: bound(channel)}
	missed := floor - position
	p.rowsMu.Lock()
	defer p.rowsMu.Unlock()
	if e, ok := p.seen[k]; ok {
		e.count++
		e.missed += missed
		// **`reported` is sticky-false, where `kind` is last-wins.** One
		// row aggregates every passing of this reader on this channel, and
		// a slow one can be passed while connected - untold, MQTT offering
		// no way - then drop, be passed again while away, and come back to
		// Session Present = 0. Taking the latest answer would end with the
		// row saying it was told, and an operator sent to `reported: false`
		// first would skip the one reader carrying a hole nobody ever
		// mentioned. The field answers "was every passing here reported",
		// which is the question being asked of it.
		e.kind = kind
		e.reported = e.reported && reported
		e.position, e.floor = position, floor
		e.lastSeen = time.Now()
		return
	}
	if len(p.seen) >= p.ceiling {
		// **Displace the reader that has lost least, and only for one that
		// has lost more.** Worst-first ordering is what makes the cap safe,
		// so the row that falls off the end has to be the one nobody was
		// looking for. When every row held is worse than the newcomer, the
		// newcomer is what does not fit.
		var victim passingKey
		least := missed
		for key, e := range p.seen {
			if e.missed < least {
				victim, least = key, e.missed
			}
		}
		if least == missed {
			p.beyond++
			return
		}
		delete(p.seen, victim)
		p.beyond++
	}
	p.seen[k] = &passing{kind: kind, reported: reported, count: 1, missed: missed,
		position: position, floor: floor, lastSeen: time.Now()}
}

// worst returns the record worst-first, at most n of them, with how many are
// held and how many never fitted.
//
// **Named `worst` rather than `rows` for the reason `add` is not `record`**,
// and this one is sharper: TestNoClientLockSectionReachesTheBrokerLock keys
// every function in the package by its bare name, so a second `rows` here
// would not sit beside `refusals.rows` - it would replace it, and that walk
// would stop knowing `rows` reaches a lock at all. Measured: the guard went
// from 150 functions to 148 with this method called `rows`. An instrument
// that quietly covers less is the failure this package writes checks
// against, and a method name is not worth paying it.
func (p *passings) worst(n int) (out []passingRow, tracked int, beyond uint64) {
	if p == nil {
		return nil, 0, 0
	}
	p.rowsMu.Lock()
	defer p.rowsMu.Unlock()
	all := make([]passingRow, 0, len(p.seen))
	for k, e := range p.seen {
		all = append(all, passingRow{
			Reader: k.reader, Channel: k.channel, Kind: string(e.kind),
			Reported: e.reported, Count: e.count, RecordsMissed: e.missed,
			LastPosition: e.position, LastFloor: e.floor,
			LastSeen: e.lastSeen.UTC().Format(time.RFC3339),
		})
	}
	// **Worst first, and by name where two are equal**, so that two reads of
	// an unchanged record answer the same way - a route whose row order
	// wobbles is one an operator cannot diff.
	sort.Slice(all, func(i, j int) bool {
		if all[i].RecordsMissed != all[j].RecordsMissed {
			return all[i].RecordsMissed > all[j].RecordsMissed
		}
		if all[i].Reader != all[j].Reader {
			return all[i].Reader < all[j].Reader
		}
		return all[i].Channel < all[j].Channel
	})
	tracked, beyond = len(all), p.beyond
	if len(all) > n {
		all = all[:n]
	}
	return all, tracked, beyond
}

type passingRow struct {
	Reader        string `json:"reader"`
	Channel       string `json:"channel"`
	Kind          string `json:"kind"`
	Reported      bool   `json:"reported"`
	Count         uint64 `json:"count"`
	RecordsMissed uint64 `json:"records_missed"`
	LastPosition  uint64 `json:"last_position"`
	LastFloor     uint64 `json:"last_floor"`
	LastSeen      string `json:"last_seen"`
}

// positionLostHandler answers GET /v1/operations/position-lost: which
// readers the retention floor passed, and how much each of them lost.
func (b *Broker) positionLostHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	rows, tracked, beyond := b.passed.worst(inspectionRows)
	if rows == nil {
		rows = []passingRow{}
	}
	writeJSON(w, b.log, struct {
		Readers  []passingRow `json:"readers"`
		Returned int          `json:"returned"`
		Tracked  int          `json:"tracked"`
		Beyond   uint64       `json:"beyond"`
	}{rows, len(rows), tracked, beyond})
}

// refusedHandler answers GET /v1/operations/refused.
func (b *Broker) refusedHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
	rows, tracked, beyond := b.refused.rows(inspectionRows)
	if rows == nil {
		rows = []refusalRow{}
	}
	writeJSON(w, b.log, struct {
		Clients  []refusalRow `json:"clients"`
		Returned int          `json:"returned"`
		Tracked  int          `json:"tracked"`
		Beyond   uint64       `json:"beyond"`
	}{rows, len(rows), tracked, beyond})
}
