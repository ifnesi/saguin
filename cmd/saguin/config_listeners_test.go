package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// The listeners (config.md: broker.mqtt's listen, A listener, the identity
// sources, broker.operations).

// The files the tls cases name, and a tls mapping's keys for flow YAML: the
// certificate pair alone, and with a client CA.
const (
	serverCert = dirToken + "/server.crt"
	serverKey  = dirToken + "/server.key"
	clientCA   = dirToken + "/clients-ca.crt"
	pair       = "cert_file: " + serverCert + ", key_file: " + serverKey
	pairCA     = pair + ", client_ca_file: " + clientCA
)

// servedTLS is how --output prints a tls mapping of pair: min_version "1.2
// (default)", no client CA; verifiedTLS one of pairCA: require_certificate
// "true by default with a CA".
const (
	servedTLS   = `{` + pair + `, min_version: "1.2"}`
	verifiedTLS = `{` + pair + `, min_version: "1.2", client_ca_file: ` + clientCA + `, require_certificate: true}`
)

// tcpListener, wsListener and unixListener are how --output prints an MQTT
// listener of each kind as the defaults leave it: admitting anonymous
// clients (the broker's allow_anonymous, true without a password_file), no
// tls, a ws listener's same_origin true and no allowed_origins, a unix
// listener's mode "0660" and no PROXY protocol.
func tcpListener(name, address string) string {
	return fmt.Sprintf(`{name: %s, address: %q, allow_anonymous: true, tls: none}`, name, address)
}

func wsListener(name, address string) string {
	return fmt.Sprintf(`{name: %s, address: %q, allow_anonymous: true, tls: none, same_origin: true, allowed_origins: none}`, name, address)
}

func unixListener(name, path string) string {
	return fmt.Sprintf(`{name: %s, path: %q, mode: "0660", proxy_protocol: false, allow_anonymous: true}`, name, path)
}

// opsTCP and opsUnix are the same for an operations listener, which takes
// no allow_anonymous.
func opsTCP(name, address string) string {
	return fmt.Sprintf(`{name: %s, address: %q, tls: none}`, name, address)
}

func opsUnix(name, path string) string {
	return fmt.Sprintf(`{name: %s, path: %q, mode: "0660", proxy_protocol: false}`, name, path)
}

// operations is how --output prints broker.operations with listen and the
// defaults: no operators' file, min_scrape_interval "60s".
func operations(listen string) string {
	return `{listen: ` + listen + `, password_file: none, min_scrape_interval: 1m}`
}

// mqtt is how --output prints broker.mqtt with listen, its password_file,
// allow_anonymous and acl_file, and min_protocol_version "3.1.1".
func mqtt(listen, passwordFile, allowAnonymous, aclFile string) string {
	return fmt.Sprintf(`{listen: %s, password_file: %s, allow_anonymous: %s, min_protocol_version: 3.1.1, acl_file: %s}`,
		listen, passwordFile, allowAnonymous, aclFile)
}

// file is a configuration of key/value pairs written over the required keys.
func file(sets ...string) configuration { return configuration{sets: sets} }

// listenerDefaults writes one listener of every kind, MQTT and operations,
// with only the keys each must have, and a client CA on the ws listener.
var listenerDefaults = []string{
	"broker.mqtt.listen.tcp.address", `":1883"`,
	"broker.mqtt.listen.ws", `{address: ":8443", tls: {` + pairCA + `}}`,
	"broker.mqtt.listen.unix.path", "saguin.sock",
	"broker.operations.listen.tcp.address", `"127.0.0.1:9090"`,
	"broker.operations.listen.unix.path", "/run/saguin/ops.sock",
}

// listenerDefaultsPrinted is what --output prints for listenerDefaults:
// every default, none included where its row allows none (tls,
// allowed_origins, broker.operations' password_file); a listener's
// password_file that takes a none, which no file may write on a listener,
// left out.
var listenerDefaultsPrinted = []string{
	"broker.mqtt.listen", `{tcp: ` + tcpListener("tcp", ":1883") + `, ` +
		`ws: {name: ws, address: ":8443", allow_anonymous: true, tls: ` + verifiedTLS + `, same_origin: true, allowed_origins: none}, ` +
		`unix: ` + unixListener("unix", "saguin.sock") + `}`,
	"broker.operations", operations(`{tcp: ` + opsTCP("tcp", "127.0.0.1:9090") + `, unix: ` + opsUnix("unix", "/run/saguin/ops.sock") + `}`),
}

// everyListenerKey sets every key of broker.mqtt, of a listener of each
// kind and of broker.operations, each to a value other than its default
// where the rules between keys allow one: a list and single listeners, a
// relative Unix path, a ws listener's own file.
var everyListenerKey = []string{
	"broker.mqtt.listen.tcp", `[{name: plain, address: "127.0.0.1:1883", password_file: <dir>/plain.passwd, allow_anonymous: false}, ` +
		`{name: secure, address: "[::]:8883", allow_anonymous: false, tls: {` + pairCA + `, min_version: "1.3", require_certificate: false}}]`,
	"broker.mqtt.listen.ws", `{name: web, address: "0.0.0.0:8080", password_file: <dir>/web.passwd, allow_anonymous: false, ` +
		`tls: {` + pair + `}, same_origin: false, allowed_origins: ["https://example.com", "http://[::1]:8080"]}`,
	"broker.mqtt.listen.unix", `{name: local, path: saguin.sock, mode: "0600", proxy_protocol: true, password_file: <dir>/local.passwd, allow_anonymous: false}`,
	"broker.mqtt.password_file", dirToken + "/clients.passwd",
	"broker.mqtt.allow_anonymous", "false",
	"broker.mqtt.min_protocol_version", "5",
	"broker.mqtt.acl_file", dirToken + "/acl.yaml",
	"broker.operations.listen.tcp", `{name: metrics, address: "[::1]:9090", password_file: <dir>/metrics.passwd, ` +
		`tls: {` + pairCA + `, min_version: 1.2, require_certificate: true}}`,
	"broker.operations.listen.unix", `{name: admin, path: /run/saguin/ops.sock, mode: "0640", proxy_protocol: true}`,
	"broker.operations.password_file", dirToken + "/operators.passwd",
	"broker.operations.min_scrape_interval", "2m",
}

// everyListenerKeyPrinted is what --output prints for everyListenerKey:
// every key it sets, and every key a listener there inherits (the secure
// listener the broker's password_file, the admin listener the operators');
// a tls's client_ca_file and require_certificate left out where it has no
// CA, which no file may write.
var everyListenerKeyPrinted = []string{
	"broker.mqtt", `{listen: {` +
		`tcp: [{name: plain, address: "127.0.0.1:1883", password_file: <dir>/plain.passwd, allow_anonymous: false, tls: none}, ` +
		`{name: secure, address: "[::]:8883", password_file: <dir>/clients.passwd, allow_anonymous: false, ` +
		`tls: {` + pair + `, min_version: "1.3", client_ca_file: ` + clientCA + `, require_certificate: false}}], ` +
		`ws: {name: web, address: "0.0.0.0:8080", password_file: <dir>/web.passwd, allow_anonymous: false, tls: ` + servedTLS + `, ` +
		`same_origin: false, allowed_origins: ["https://example.com", "http://[::1]:8080"]}, ` +
		`unix: {name: local, path: saguin.sock, mode: "0600", proxy_protocol: true, password_file: <dir>/local.passwd, allow_anonymous: false}}, ` +
		`password_file: <dir>/clients.passwd, allow_anonymous: false, min_protocol_version: "5", acl_file: <dir>/acl.yaml}`,
	"broker.operations", `{listen: {` +
		`tcp: {name: metrics, address: "[::1]:9090", password_file: <dir>/metrics.passwd, tls: ` + verifiedTLS + `}, ` +
		`unix: {name: admin, path: /run/saguin/ops.sock, mode: "0640", proxy_protocol: true, password_file: <dir>/operators.passwd}}, ` +
		`password_file: <dir>/operators.passwd, min_scrape_interval: 2m}`,
}

// listenAs is a case whose MQTT listeners --output prints as listen, exactly.
func listenAs(name, listen string, sets ...string) printed {
	return printed{name: name, configuration: file(sets...), want: []string{"broker.mqtt.listen", listen}}
}

// listenerDefaultCases: broker.mqtt's listen, "one tcp listener on :1883";
// "by kind (tcp, ws, unix), each kind one listener or a list of at least
// one; writing any replaces the default". A listener's name: "its kind", "required once a
// kind has two listeners"; mode: "0660"; same_origin: true;
// allow_anonymous: "the broker's", true without a password_file.
// broker.operations: "absent, Sagüin starts no operations listener"
// (requiredDefaults); its listen: "tcp and unix listeners with name [...] as
// above".
var listenerDefaultCases = []printed{
	{name: "every listener kind", configuration: file(listenerDefaults...), want: listenerDefaultsPrinted},
	listenAs("tcp written", `{tcp: `+tcpListener("tcp", "127.0.0.1:1884")+`}`, "broker.mqtt.listen.tcp.address", `"127.0.0.1:1884"`),
	listenAs("ws alone replaces the default", `{ws: `+wsListener("ws", ":8080")+`}`, "broker.mqtt.listen.ws.address", `":8080"`),
	listenAs("unix alone replaces the default", `{unix: `+unixListener("unix", "/run/saguin/mqtt.sock")+`}`,
		"broker.mqtt.listen.unix.path", "/run/saguin/mqtt.sock"),
	listenAs("a list of one", `{tcp: `+tcpListener("tcp", ":1884")+`}`, "broker.mqtt.listen.tcp", `[{address: ":1884"}]`),
	listenAs("a tcp list of two, in order", `{tcp: [`+tcpListener("b", ":1884")+`, `+tcpListener("a", ":1883")+`]}`,
		"broker.mqtt.listen.tcp", `[{name: b, address: ":1884"}, {name: a, address: ":1883"}]`),
	listenAs("a ws list of two, in order", `{ws: [`+wsListener("b", ":8081")+`, `+wsListener("a", ":8080")+`]}`,
		"broker.mqtt.listen.ws", `[{name: b, address: ":8081"}, {name: a, address: ":8080"}]`),
	listenAs("a unix list of two, in order", `{unix: [`+unixListener("b", "b.sock")+`, `+unixListener("a", "a.sock")+`]}`,
		"broker.mqtt.listen.unix", `[{name: b, path: b.sock}, {name: a, path: a.sock}]`),
	listenAs("one of each kind", `{tcp: `+tcpListener("tcp", ":1883")+`, ws: `+wsListener("ws", ":8080")+`, unix: `+unixListener("unix", "saguin.sock")+`}`,
		"broker.mqtt.listen.tcp.address", `":1883"`, "broker.mqtt.listen.ws.address", `":8080"`, "broker.mqtt.listen.unix.path", "saguin.sock"),
	{
		name: "operations lists of two, in order",
		configuration: file(
			"broker.operations.listen.tcp", `[{name: b, address: "127.0.0.1:9091"}, {name: a, address: "127.0.0.1:9090"}]`,
			"broker.operations.listen.unix", `[{name: d, path: /run/saguin/d.sock}, {name: c, path: /run/saguin/c.sock}]`,
		),
		want: []string{"broker.operations", operations(`{tcp: [` + opsTCP("b", "127.0.0.1:9091") + `, ` + opsTCP("a", "127.0.0.1:9090") + `], ` +
			`unix: [` + opsUnix("d", "/run/saguin/d.sock") + `, ` + opsUnix("c", "/run/saguin/c.sock") + `]}`)},
	},
}

// each makes one case of each value.
func each[V, C any](values []V, c func(V) C) []C {
	cases := make([]C, 0, len(values))
	for _, v := range values {
		cases = append(cases, c(v))
	}
	return cases
}

// The sections a listener's name and address are tried in, and what else
// each listener needs: an operations tcp listener has a password file, so
// that a host that is not loopback is allowed, and its own port, 9090 for
// 1883, apart from the MQTT default's.
type section struct{ key, listener, extra, port string }

var (
	namedSections = []section{
		{key: "broker.mqtt.listen.tcp", listener: `address: ":1883"`},
		{key: "broker.mqtt.listen.unix", listener: `path: saguin.sock`},
		{key: "broker.operations.listen.tcp", listener: `address: "127.0.0.1:9090"`},
		{key: "broker.operations.listen.unix", listener: `path: /run/saguin/ops.sock`},
	}
	addressSections = []section{
		{key: "broker.mqtt.listen.tcp", port: "1883"},
		{key: "broker.mqtt.listen.ws", port: "1883"},
		{key: "broker.operations.listen.tcp", extra: ", password_file: <dir>/ops.passwd", port: "9090"},
	}
)

// Names: "1 to 64 of A–Z, a–z, 0–9, - and _", on an MQTT listener and on an
// operations one ("as above"), of any kind.
var (
	goodNames = []string{"a", "Edge_09-z", "-_", strings.Repeat("n", 64)}
	badNames  = []string{"", strings.Repeat("n", 65), "a\nb", "a\n", "a\rb", "a b", "a.b", "a/b", "a:b", "édge"}
)

// address (tcp, ws, and an operations tcp listener's "as above"): "host:port,
// the port in decimal, IPv6 in brackets; the host a name as in
// allowed_origins, an IPv4 or IPv6 address, never a multicast one, with a
// zone (%eth0) only on a link-local IPv6 address and never empty, or empty
// for every address". A name is dot-separated labels of 1 to 63 letters,
// digits, - and _, none starting or ending with -, the last not all digits.
// Multicast is IPv4 224.0.0.0/4 and IPv6 ff00::/8; link-local IPv6 is
// fe80::/10. Port 0 is a port, set aside by the rule between listeners.
var (
	goodAddresses = []string{
		":1883", "localhost:1883", "a:1883", "a_b:1883", "_a.example.com:1883", "a_:1883", "Broker-1.Example.COM:1883",
		"1a:1883", "1.example.com:1883", "a.b1:1883", strings.Repeat("a", 63) + ".example.com:1883", "a:0", "a:65535",
		"10.0.0.1:1883", "223.255.255.255:1883", "240.0.0.1:1883", "255.255.255.255:1883",
		"[::1]:1883", "[2001:db8::1]:1883", "[fe80::1]:1883", "[fe80::1%eth0]:1883", "[febf::1%eth0]:1883", "[fe80::1%2]:1883",
		"[feff::1]:1883",
	}
	badAddresses = []string{
		"-a:1883", "a-:1883", "a.-b:1883", "a..b:1883", ".a:1883", "a.:1883", "*.example.com:1883", "a!b:1883", "a b:1883",
		"a/b:1883", "é.example.com:1883", strings.Repeat("a", 64) + ".example.com:1883",
		"999.1.1.1:1883", "256.0.0.1:1883", "1.2.3:1883", "1.2.3.4.5:1883", "a.123:1883", "123:1883",
		"224.0.0.1:1883", "239.255.255.255:1883", "232.1.2.3:1883",
		"[ff02::1]:1883", "[ff00::]:1883", "[ff0e::1]:1883", "[ffff::1]:1883", "[ff02::1%eth0]:1883",
		"[::1%eth0]:1883", "[2001:db8::1%eth0]:1883", "[fec0::1%eth0]:1883", "[fe80::1%]:1883",
		"[example.com]:1883", "[1.1.1.1]:1883", "[a]:1883",
		":mqtt", "a:http", ":0x75b", ":+1883", ":1e3", ": 1883",
	}
)

// allowed_origins: "the origins allowed, each http:// or https:// with a
// host (a name of dot-separated labels of 1 to 63 letters, digits, - and _,
// none starting or ending with -, the last not all digits; an IPv4 address
// or an IPv6 address in brackets) and optional port (1 to 65,535, without a
// leading zero)". An origin has no path and no query.
var (
	goodOrigins = []string{
		"http://a", "http://a-b.example.com", "http://1a.example.com", "http://1.1.1.1", "https://255.255.255.255:8443",
		"http://" + strings.Repeat("a", 63) + ".example.com", "http://a_b", "http://a_b.example", "http://_a.example", "http://a_",
		"http://A.Example.COM", "http://1.example.com", "http://a.b1", "http://[::1]", "https://[2001:db8::1]:8443", "http://a:1",
		"http://a:65535", "http://a:10",
	}
	badOrigins = []string{
		"ftp://example.com", "example.com", "http://", "https://example.com/app", "https://example.com?a=1", "ws://example.com",
		"https://example.com:abc", "https://example.com:65536", "http://a:0", "http://a:01", "http://a:080", "https://example.com:00443",
		"http://a:", "http://a:+80", "http://a:-1",
		"http://*.a.com", "http://-a", "http://a-", "http://a..b", "http://.a", "http://999.1.1.1", "https://999.1.1.1:8443",
		"http://" + strings.Repeat("a", 64) + ".example.com", "http://a.-b", "http://a-.b", "http://a.", "http://a!b", "http://a~b",
		"http://é.example.com", "http://256.0.0.1", "http://1.2.3", "http://1.2.3.4.5", "http://a.123", "http://123", "http://::1",
		"http://[example.com]", "http://[1.1.1.1]", "http://[::1", "http://user@example.com",
	}
)

// namedAs is a listener of section named name, and what --output prints of
// the listeners' names: the one MQTT listener so named, or beside an
// operations one so named, the MQTT default "tcp".
func namedAs(s section, name string) printed {
	want := []string{s.key + ".name", strconv.Quote(name)}
	kind := s.key[strings.LastIndex(s.key, ".")+1:]
	if strings.HasPrefix(s.key, "broker.mqtt.") {
		for _, other := range []string{"tcp", "ws", "unix"} {
			if other != kind {
				want = append(want, "broker.mqtt.listen."+other, absent)
			}
		}
	} else {
		other := map[string]string{"tcp": "unix", "unix": "tcp"}[kind]
		want = append(want, "broker.mqtt.listen.tcp.name", "tcp", "broker.operations.listen."+other, absent)
	}
	return printed{
		name:          s.key + " name " + strconv.Quote(name),
		configuration: file(s.key, `{name: `+strconv.Quote(name)+`, `+s.listener+`}`),
		want:          want,
	}
}

// listenerAccepted: each key of A listener on a listener of a kind its row
// names, read as its row allows; tls, origins, listeners kept apart, who a
// listener admits, broker.operations.
var listenerAccepted = slices.Concat(
	[]printed{
		listenAs("name", `{tcp: `+tcpListener("plain", ":1883")+`}`, "broker.mqtt.listen.tcp", `{name: plain, address: ":1883"}`),
		// proxy_protocol (unix): false by default; a boolean.
		listenAs("proxy_protocol", `{unix: {name: unix, path: saguin.sock, mode: "0660", proxy_protocol: true, allow_anonymous: true}}`,
			"broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: true}`),
		listenAs("proxy_protocol false", `{unix: `+unixListener("unix", "saguin.sock")+`}`,
			"broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: false}`),
		// allow_anonymous: a boolean.
		listenAs("allow_anonymous true", `{tcp: `+tcpListener("tcp", ":1883")+`}`,
			"broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: true}`),
	},
	slices.Concat(each(namedSections, func(s section) []printed {
		return each(goodNames, func(name string) printed { return namedAs(s, name) })
	})...),
	// address: "host:port, the port in decimal, IPv6 in brackets".
	each([]string{":1883", "127.0.0.1:1883", "0.0.0.0:1", "[::1]:1883", "[::]:0", "localhost:1883", "broker.example.com:65535"},
		func(a string) printed {
			return listenAs("address "+a, `{tcp: `+tcpListener("tcp", a)+`}`, "broker.mqtt.listen.tcp.address", strconv.Quote(a))
		}),
	slices.Concat(each(addressSections, func(s section) []printed {
		return each(goodAddresses, func(a string) printed {
			a = strings.ReplaceAll(a, "1883", s.port)
			return printed{
				name:          s.key + " address " + a,
				configuration: file(s.key, `{address: `+strconv.Quote(a)+s.extra+`}`),
				want:          []string{s.key + ".address", strconv.Quote(a)},
			}
		})
	})...),
	// path (unix): "the socket file, at most 107 bytes"; "every file path is
	// absolute, except a Unix socket's" (Values). Bytes, not characters: é
	// is two.
	each([]string{"saguin.sock", "/run/saguin/mqtt.sock", "/run/" + strings.Repeat("s", 102), strings.Repeat("é", 53) + "s"},
		func(p string) printed {
			return listenAs(fmt.Sprintf("path of %d bytes", len(p)), `{unix: `+unixListener("unix", p)+`}`, "broker.mqtt.listen.unix.path", p)
		}),
	// path (unix), MQTT and operations: a file's name may begin with a dot
	// or hold an @.
	each([]string{".saguin.sock", "/run/saguin/..sock", "/run/saguin/a@b.sock"}, func(p string) printed {
		return listenAs("broker.mqtt path "+p, `{unix: `+unixListener("unix", p)+`}`, "broker.mqtt.listen.unix.path", strconv.Quote(p))
	}),
	each([]string{".saguin.sock", "/run/saguin/..sock", "/run/saguin/a@b.sock"}, func(p string) printed {
		return printed{
			name:          "broker.operations path " + p,
			configuration: file("broker.operations.listen.unix.path", strconv.Quote(p)),
			want:          []string{"broker.operations.listen", `{unix: ` + opsUnix("unix", p) + `}`},
		}
	}),
	// mode (unix): "the socket's permissions, in octal: at most 0777, the
	// owner's read and write bits set", quoted as config.md writes "0660" or
	// not, with or without its leading zero.
	each([][2]string{{`"0660"`, "0660"}, {`"0600"`, "0600"}, {`0640`, "0640"}, {`"0777"`, "0777"}, {`"0700"`, "0700"}, {`660`, "0660"}, {`600`, "0600"}},
		func(m [2]string) printed {
			return listenAs("mode "+m[0], `{unix: {name: unix, path: saguin.sock, mode: "`+m[1]+`", proxy_protocol: false, allow_anonymous: true}}`,
				"broker.mqtt.listen.unix", `{path: saguin.sock, mode: `+m[0]+`}`)
		}),
	listenerTLSAccepted,
	listenerOriginsAccepted,
	listenerCollisionsAccepted,
	listenerIdentityAccepted,
	operationsAccepted,
)

// tcpTLS is how --output prints the tcp listener at ":8883" with tls.
func tcpTLS(tls string) string {
	return `{tcp: {name: tcp, address: ":8883", allow_anonymous: true, tls: ` + tls + `}}`
}

// listenerTLSAccepted: a listener's tls (tcp, ws), "none" by default:
// "cert_file and key_file, written together; min_version, 1.2 (default) or
// 1.3; client_ca_file, which asks for client certificates;
// require_certificate, true by default with a CA and refused without one".
var listenerTLSAccepted = []printed{
	listenAs("tls", tcpTLS(servedTLS), "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`}}`),
	listenAs("tls none", `{tcp: `+tcpListener("tcp", ":8883")+`}`, "broker.mqtt.listen.tcp", `{address: ":8883", tls: none}`),
	listenAs("tls on ws", `{ws: {name: ws, address: ":8080", allow_anonymous: true, tls: `+servedTLS+`, same_origin: true, allowed_origins: none}}`,
		"broker.mqtt.listen.ws", `{address: ":8080", tls: {`+pair+`}}`),
	listenAs("min_version 1.3", tcpTLS(`{`+pair+`, min_version: "1.3"}`), "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, min_version: 1.3}}`),
	listenAs("min_version 1.2 quoted", tcpTLS(servedTLS), "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, min_version: "1.2"}}`),
	listenAs("a client CA", tcpTLS(verifiedTLS), "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`}}`),
	listenAs("a client CA, certificate required", tcpTLS(verifiedTLS),
		"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`, require_certificate: true}}`),
	listenAs("a client CA, certificate not required",
		tcpTLS(`{`+pair+`, min_version: "1.2", client_ca_file: `+clientCA+`, require_certificate: false}`),
		"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`, require_certificate: false}}`),
}

// wsOrigins is how --output prints the ws listener at ":8080" with
// same_origin and allowed_origins.
func wsOrigins(sameOrigin, origins string) string {
	return `{ws: {name: ws, address: ":8080", allow_anonymous: true, tls: none, same_origin: ` + sameOrigin + `, allowed_origins: ` + origins + `}}`
}

// listenerOriginsAccepted: a ws listener's same_origin, true by default, and
// allowed_origins, "none: no additional allowed origins", else the origins
// allowed.
var listenerOriginsAccepted = append([]printed{
	listenAs("same_origin false", wsOrigins("false", "none"), "broker.mqtt.listen.ws", `{address: ":8080", same_origin: false}`),
	listenAs("allowed_origins", wsOrigins("true", `["https://example.com", "http://example.com:8080", "http://[::1]:8080", "https://sub.example.com"]`),
		"broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: `+
			`["https://example.com", "http://example.com:8080", "http://[::1]:8080", "https://sub.example.com"]}`),
	listenAs("allowed_origins none", `{ws: `+wsListener("ws", ":8080")+`}`, "broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: none}`),
}, each(goodOrigins, func(origin string) printed {
	return listenAs("allowed origin "+origin, wsOrigins("true", `["`+origin+`"]`),
		"broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: ["`+origin+`"]}`)
})...)

// hosts is two tcp listeners in a list: on a clash ("no two TCP listeners,
// operations included, on one port with equal or wildcard hosts, port 0
// aside") the second is refused, naming the first; otherwise both are
// read. Hosts compare as IP addresses, or as names spelled alike.
type hosts struct {
	first, second string
	clash         bool
}

var tcpPairs = []hosts{
	{"127.0.0.1:1883", "127.0.0.1:1883", true},
	{"127.0.0.1:1883", "127.0.0.2:1883", false},
	{"127.0.0.1:1883", "127.0.0.1:1884", false},
	{"127.0.0.1:1883", "0.0.0.0:1883", true},
	{"[::]:1883", "127.0.0.1:1883", true},
	{":1883", "[::1]:1883", true},
	{"[::1]:1883", "[0:0:0:0:0:0:0:1]:1883", true}, // one host, spelled twice
	{"localhost:1883", "localhost:1883", true},
	{"localhost:1883", ":1883", true},
	{":0", ":0", false},
	{"127.0.0.1:0", "0.0.0.0:0", false},
}

// twoTCP is a tcp list of a and b at the two addresses.
func twoTCP(h hosts) []string {
	return []string{"broker.mqtt.listen.tcp", `[{name: a, address: "` + h.first + `"}, {name: b, address: "` + h.second + `"}]`}
}

// listenerCollisionsAccepted: listeners kept apart (address, name, path:
// "unique, held by a <path>.lock file").
var listenerCollisionsAccepted = append([]printed{
	{
		name:          "MQTT and operations on two ports",
		configuration: file("broker.mqtt.listen.tcp.address", `":1883"`, "broker.operations.listen.tcp.address", `"127.0.0.1:9090"`),
		want: []string{
			"broker.mqtt.listen", `{tcp: ` + tcpListener("tcp", ":1883") + `}`,
			"broker.operations", operations(`{tcp: ` + opsTCP("tcp", "127.0.0.1:9090") + `}`),
		},
	},
	listenAs("two paths", `{unix: [`+unixListener("a", "/run/saguin/a.sock")+`, `+unixListener("b", "/run/saguin/b.sock")+`]}`,
		"broker.mqtt.listen.unix", `[{name: a, path: /run/saguin/a.sock}, {name: b, path: /run/saguin/b.sock}]`),
	listenAs("two paths and their lock files apart",
		`{unix: [`+unixListener("a", "/run/saguin/mqtt.sock")+`, `+unixListener("b", "/run/saguin/mqtt.lock")+`]}`,
		"broker.mqtt.listen.unix", `[{name: a, path: /run/saguin/mqtt.sock}, {name: b, path: /run/saguin/mqtt.lock}]`),
}, each(slices.DeleteFunc(slices.Clone(tcpPairs), func(h hosts) bool { return h.clash }), func(h hosts) printed {
	return listenAs(h.first+" and "+h.second, `{tcp: [`+tcpListener("a", h.first)+`, `+tcpListener("b", h.second)+`]}`, twoTCP(h)...)
})...)

// listenerIdentityAccepted: who a listener admits. broker.mqtt
// allow_anonymous: "true without password_file, false with one". A
// listener's password_file: "the broker's"; its allow_anonymous: "the
// broker's; false with the listener's own password_file"; an identity
// source is "a password file, a client CA or the PROXY protocol". acl_file:
// "needs a listener with an identity source".
var listenerIdentityAccepted = []printed{
	{
		name:          "the broker's password file",
		configuration: file("broker.mqtt.password_file", dirToken+"/passwd"),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":1883", password_file: <dir>/passwd, allow_anonymous: false, tls: none}}`,
			dirToken+"/passwd", "false", "none")},
	},
	{
		name:          "anonymous beside the broker's password file",
		configuration: file("broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.allow_anonymous", "true"),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":1883", password_file: <dir>/passwd, allow_anonymous: true, tls: none}}`,
			dirToken+"/passwd", "true", "none")},
	},
	{
		name:          "a listener's own password file",
		configuration: file("broker.mqtt.listen.tcp", `{address: ":1883", password_file: <dir>/tcp.passwd}`),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: false, tls: none}}`,
			"none", "true", "none")},
	},
	listenAs("a listener's own password file over the broker's",
		`{tcp: [{name: a, address: ":1883", password_file: <dir>/passwd, allow_anonymous: true, tls: none}, `+
			`{name: b, address: ":1884", password_file: <dir>/b.passwd, allow_anonymous: false, tls: none}]}`,
		"broker.mqtt.password_file", dirToken+"/passwd",
		"broker.mqtt.allow_anonymous", "true",
		"broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: b, address: ":1884", password_file: <dir>/b.passwd}]`),
	listenAs("anonymous beside a listener's own password file",
		`{tcp: {name: tcp, address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: true, tls: none}}`,
		"broker.mqtt.listen.tcp", `{address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: true}`),
	listenAs("a client CA is an identity source",
		`{tcp: {name: tcp, address: ":8883", allow_anonymous: false, tls: {`+pair+`, min_version: "1.2", client_ca_file: `+clientCA+`, require_certificate: false}}}`,
		"broker.mqtt.allow_anonymous", "false",
		"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`, require_certificate: false}}`),
	listenAs("the PROXY protocol is an identity source",
		`{unix: {name: unix, path: saguin.sock, mode: "0660", proxy_protocol: true, allow_anonymous: false}}`,
		"broker.mqtt.allow_anonymous", "false",
		"broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: true}`),
	listenAs("a listener's own password file is an identity source",
		`{tcp: {name: tcp, address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: false, tls: none}}`,
		"broker.mqtt.listen.tcp", `{address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: false}`),
	listenAs("a listener's true over the broker's false", `{tcp: `+tcpListener("tcp", ":1883")+`}`,
		"broker.mqtt.allow_anonymous", "false",
		"broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: true}`),

	// acl_file.
	{
		name:          "acl_file beside the broker's password file",
		configuration: file("broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", dirToken+"/acl.yaml"),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":1883", password_file: <dir>/passwd, allow_anonymous: false, tls: none}}`,
			dirToken+"/passwd", "false", dirToken+"/acl.yaml")},
	},
	{
		name: "acl_file beside a client CA",
		configuration: file(
			"broker.mqtt.allow_anonymous", "false",
			"broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`}}`,
		),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":8883", allow_anonymous: false, tls: `+verifiedTLS+`}}`,
			"none", "false", dirToken+"/acl.yaml")},
	},
	{
		name: "acl_file beside the PROXY protocol",
		configuration: file(
			"broker.mqtt.allow_anonymous", "false",
			"broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: true}`,
		),
		want: []string{"broker.mqtt", mqtt(`{unix: {name: unix, path: saguin.sock, mode: "0660", proxy_protocol: true, allow_anonymous: false}}`,
			"none", "false", dirToken+"/acl.yaml")},
	},
	{
		name: "acl_file and one listener of two with an identity source",
		configuration: file(
			"broker.mqtt.allow_anonymous", "false",
			"broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`}}`,
			"broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: true}`,
		),
		want: []string{"broker.mqtt", mqtt(`{tcp: {name: tcp, address: ":8883", allow_anonymous: false, tls: `+verifiedTLS+`}, `+
			`unix: {name: unix, path: saguin.sock, mode: "0660", proxy_protocol: true, allow_anonymous: false}}`,
			"none", "false", dirToken+"/acl.yaml")},
	},
}

// opsAs is a case whose broker.operations --output prints as ops, exactly.
func opsAs(name, ops string, sets ...string) printed {
	return printed{name: name, configuration: file(sets...), want: []string{"broker.operations", ops}}
}

// oneOpsTCP is broker.operations with one tcp listener, printed as listener.
func oneOpsTCP(listener string) string { return operations(`{tcp: ` + listener + `}`) }

// operationsAccepted: broker.operations. listen: "tcp and unix listeners
// with name, address or path, mode, proxy_protocol, tls as above and
// password_file (default: the operators' file below), never
// allow_anonymous; their names are their own; a non-loopback address [...]
// needs a password file or a required client certificate". password_file:
// "none" by default. min_scrape_interval: "60s", "at least 60s".
var operationsAccepted = slices.Concat(
	[]printed{
		opsAs("a unix listener", operations(`{unix: {name: unix, path: /run/saguin/ops.sock, mode: "0600", proxy_protocol: true}}`),
			"broker.operations.listen.unix", `{path: /run/saguin/ops.sock, mode: "0600", proxy_protocol: true}`),
		opsAs("the operators' file, taken by a listener",
			`{listen: {tcp: {name: tcp, address: ":9090", password_file: <dir>/operators.passwd, tls: none}}, `+
				`password_file: <dir>/operators.passwd, min_scrape_interval: 1m}`,
			"broker.operations.password_file", dirToken+"/operators.passwd",
			"broker.operations.listen.tcp.address", `":9090"`),
		opsAs("the operators' file none", oneOpsTCP(opsTCP("tcp", "127.0.0.1:9090")),
			"broker.operations.password_file", "none",
			"broker.operations.listen.tcp.address", `"127.0.0.1:9090"`),
		opsAs("a listener's own file", oneOpsTCP(`{name: tcp, address: ":9090", password_file: <dir>/metrics.passwd, tls: none}`),
			"broker.operations.listen.tcp", `{address: ":9090", password_file: <dir>/metrics.passwd}`),
		opsAs("a required client certificate", oneOpsTCP(`{name: tcp, address: ":9090", tls: `+verifiedTLS+`}`),
			"broker.operations.listen.tcp", `{address: ":9090", tls: {`+pairCA+`}}`),
		// "their names are their own": an operations listener may share an
		// MQTT listener's name, here both their kind's.
		{
			name:          "names apart from the MQTT listeners'",
			configuration: file("broker.mqtt.listen.tcp.address", `":1883"`, "broker.operations.listen.tcp.address", `"127.0.0.1:9090"`),
			want:          []string{"broker.mqtt.listen.tcp.name", "tcp", "broker.operations.listen.tcp.name", "tcp"},
		},
	},
	// Loopback needs no password file.
	each([]string{"127.0.0.1:9090", "127.0.0.2:9090", "[::1]:9090"}, func(a string) printed {
		return opsAs("loopback "+a, oneOpsTCP(opsTCP("tcp", a)), "broker.operations.listen.tcp.address", strconv.Quote(a))
	}),
	// min_scrape_interval: "at least 60s", a duration without ms (Values).
	each([]string{"60s", "1m", "90s", "2h", "1d"}, func(v string) printed {
		return printed{
			name:          "min_scrape_interval " + v,
			configuration: file("broker.operations.listen.unix.path", "/run/saguin/ops.sock", "broker.operations.min_scrape_interval", v),
		}
	}),
)

// refuse is a case refused naming key, its message naming each of says.
func refuse(name, key string, c configuration, says ...string) refusal {
	return refusal{name: name, configuration: c, key: key, says: says}
}

// misplaced is a key on a listener kind its row does not name: path, mode
// and proxy_protocol are unix's ("TCP listeners never read" a PROXY
// header); address and tls are tcp's and ws's; same_origin and
// allowed_origins are ws's.
type misplaced struct{ kind, listener, key string }

// listenerRefused: each listener's key past what its row allows, and the
// rules between listeners and their keys broken.
var listenerRefused = slices.Concat(
	[]refusal{
		// listen: "each kind one listener or a list of at least one"; a
		// listen written with no listener, or a kind written as an empty
		// list, is refused, MQTT or operations.
		refuse("listen empty", "broker.mqtt.listen", file("broker.mqtt.listen", "{}")),
		refuse("an empty tcp list", "broker.mqtt.listen.tcp", file("broker.mqtt.listen.tcp", "[]")),
		refuse("an empty ws list", "broker.mqtt.listen.ws", file("broker.mqtt.listen.ws", "[]")),
		refuse("an empty unix list", "broker.mqtt.listen.unix", file("broker.mqtt.listen.unix", "[]")),
		refuse("an empty unix list beside a tcp listener", "broker.mqtt.listen.unix",
			file("broker.mqtt.listen.tcp.address", `":1883"`, "broker.mqtt.listen.unix", "[]")),
		refuse("operations listen empty", "broker.operations.listen", file("broker.operations.listen", "{}")),
		refuse("an empty operations tcp list", "broker.operations.listen.tcp", file("broker.operations.listen.tcp", "[]")),
		refuse("an empty operations unix list", "broker.operations.listen.unix", file("broker.operations.listen.unix", "[]")),

		// A list of two needs every name, the first missing one refused.
		refuse("a tcp list of two unnamed", "broker.mqtt.listen.tcp[0].name",
			file("broker.mqtt.listen.tcp", `[{address: ":1883"}, {address: ":1884"}]`)),
		refuse("a tcp list's second unnamed", "broker.mqtt.listen.tcp[1].name",
			file("broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {address: ":1884"}]`)),
		refuse("a ws list's second unnamed", "broker.mqtt.listen.ws[1].name",
			file("broker.mqtt.listen.ws", `[{name: a, address: ":8080"}, {address: ":8081"}]`)),
		refuse("a unix list of two unnamed", "broker.mqtt.listen.unix[0].name",
			file("broker.mqtt.listen.unix", `[{path: a.sock}, {path: b.sock}]`)),
		refuse("an operations tcp list's second unnamed", "broker.operations.listen.tcp[1].name",
			file("broker.operations.listen.tcp", `[{name: a, address: "127.0.0.1:9090"}, {address: "127.0.0.1:9091"}]`)),
		refuse("an operations unix list of two unnamed", "broker.operations.listen.unix[0].name",
			file("broker.operations.listen.unix", `[{path: /run/saguin/a.sock}, {path: /run/saguin/b.sock}]`)),

		// address (tcp, ws) and path (unix): "required".
		refuse("tcp without address", "broker.mqtt.listen.tcp.address", file("broker.mqtt.listen.tcp.name", "plain")),
		refuse("ws without address", "broker.mqtt.listen.ws.address", file("broker.mqtt.listen.ws.name", "web")),
		refuse("unix without path", "broker.mqtt.listen.unix.path", file("broker.mqtt.listen.unix.name", "local")),
		refuse("unix path empty", "broker.mqtt.listen.unix.path", file("broker.mqtt.listen.unix.path", `""`)),
		refuse("proxy_protocol yes", "broker.mqtt.listen.unix.proxy_protocol",
			file("broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: yes}`)),

		// password_file: "every file path is absolute", and "none is
		// accepted where a row says so, or where it is the default"
		// (Values): its default is "the broker's", so a listener cannot
		// write none, MQTT or operations.
		refuse("password_file relative", "broker.mqtt.listen.tcp.password_file",
			file("broker.mqtt.listen.tcp", `{address: ":1883", password_file: tcp.passwd}`)),
		refuse("password_file none", "broker.mqtt.listen.tcp.password_file",
			file("broker.mqtt.listen.tcp", `{address: ":1883", password_file: none}`)),
		refuse("password_file empty", "broker.mqtt.listen.tcp.password_file",
			file("broker.mqtt.listen.tcp", `{address: ":1883", password_file: ""}`)),
		refuse("an operations listener's password_file none", "broker.operations.listen.tcp.password_file",
			file("broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: none}`)),
		refuse("an operations listener's password_file empty", "broker.operations.listen.unix.password_file",
			file("broker.operations.listen.unix", `{path: /run/saguin/ops.sock, password_file: ""}`)),
		refuse("allow_anonymous yes", "broker.mqtt.listen.tcp.allow_anonymous",
			file("broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: yes}`)),
	},
	slices.Concat(each(namedSections, func(s section) []refusal {
		return each(badNames, func(name string) refusal {
			return refuse(s.key+" name "+strconv.Quote(name), s.key+".name", file(s.key, `{name: `+strconv.Quote(name)+`, `+s.listener+`}`))
		})
	})...),
	each([]string{"", "1883", ":", "127.0.0.1", "[::1]", "::1:1883", ":65536", ":-1", "none"}, func(a string) refusal {
		return refuse("address "+strconv.Quote(a), "broker.mqtt.listen.tcp.address", file("broker.mqtt.listen.tcp.address", strconv.Quote(a)))
	}),
	slices.Concat(each(addressSections, func(s section) []refusal {
		return each(badAddresses, func(a string) refusal {
			a = strings.ReplaceAll(a, "1883", s.port)
			return refuse(s.key+" address "+strconv.Quote(a), s.key+".address", file(s.key, `{address: `+strconv.Quote(a)+s.extra+`}`))
		})
	})...),
	each([]string{"/run/" + strings.Repeat("s", 103), strings.Repeat("é", 54)}, func(p string) refusal {
		return refuse(fmt.Sprintf("path of %d bytes", len(p)), "broker.mqtt.listen.unix.path", file("broker.mqtt.listen.unix.path", p))
	}),
	// A path names a file: never an abstract socket (@, which has no file),
	// a directory (one ending in /, /. or /.., or . or .. alone) or blank.
	slices.Concat(each([]string{"broker.mqtt", "broker.operations"}, func(s string) []refusal {
		return each([]string{"@saguin", "/run/saguin/", "/", "/run/saguin/.", "/run/saguin/..", ".", "..", " ", "\t"}, func(p string) refusal {
			return refuse(s+" path "+strconv.Quote(p), s+".listen.unix.path", file(s+".listen.unix.path", strconv.Quote(p)))
		})
	})...),
	each([]string{`"0680"`, `"rw-rw----"`, `"01777"`, `"1777"`, `"2660"`, `"0000"`, `"0400"`, `"0200"`, `"0500"`, `"0466"`, `"0266"`, `"0077"`, "none", `""`},
		func(v string) refusal {
			return refuse("mode "+v, "broker.mqtt.listen.unix.mode", file("broker.mqtt.listen.unix", `{path: saguin.sock, mode: `+v+`}`))
		}),
	each([]misplaced{
		{"tcp", `{address: ":1883", path: saguin.sock}`, "path"},
		{"tcp", `{address: ":1883", mode: "0600"}`, "mode"},
		{"tcp", `{address: ":1883", proxy_protocol: true}`, "proxy_protocol"},
		{"tcp", `{address: ":1883", same_origin: true}`, "same_origin"},
		{"tcp", `{address: ":1883", allowed_origins: ["https://example.com"]}`, "allowed_origins"},
		{"ws", `{address: ":8080", path: saguin.sock}`, "path"},
		{"ws", `{address: ":8080", mode: "0600"}`, "mode"},
		{"ws", `{address: ":8080", proxy_protocol: true}`, "proxy_protocol"},
		{"unix", `{path: saguin.sock, address: ":1883"}`, "address"},
		{"unix", `{path: saguin.sock, tls: {` + pair + `}}`, "tls"},
		{"unix", `{path: saguin.sock, same_origin: true}`, "same_origin"},
		{"unix", `{path: saguin.sock, allowed_origins: ["https://example.com"]}`, "allowed_origins"},
	}, func(k misplaced) refusal {
		return refuse(k.kind+" "+k.key, "broker.mqtt.listen."+k.kind+"."+k.key, file("broker.mqtt.listen."+k.kind, k.listener))
	}),
	listenerTLSRefused,
	each(badOrigins, func(origin string) refusal {
		// The second entry of a list is named by its index.
		return refuse("allowed origin "+origin, "broker.mqtt.listen.ws.allowed_origins[1]",
			file("broker.mqtt.listen.ws", `{address: ":8080", allowed_origins: ["https://example.com", "`+origin+`"]}`))
	}),
	[]refusal{refuse("same_origin yes", "broker.mqtt.listen.ws.same_origin", file("broker.mqtt.listen.ws", `{address: ":8080", same_origin: yes}`))},
	listenerCollisionsRefused,
	listenerIdentityRefused,
	operationsRefused,
)

// listenerTLSRefused: "written together": the missing one is refused; a TLS
// listener serves a certificate, so a tls mapping needs both files;
// require_certificate "refused without" a CA; its files absolute paths, none
// not among their values (Values); min_version "1.2 (default) or 1.3".
var listenerTLSRefused = slices.Concat(
	[]refusal{
		refuse("require_certificate yes", "broker.mqtt.listen.tcp.tls.require_certificate",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`, require_certificate: yes}}`)),
		refuse("cert_file alone", "broker.mqtt.listen.tcp.tls.key_file",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {cert_file: <dir>/server.crt}}`), "broker.mqtt.listen.tcp.tls.cert_file"),
		refuse("key_file alone", "broker.mqtt.listen.ws.tls.cert_file",
			file("broker.mqtt.listen.ws", `{address: ":8443", tls: {key_file: <dir>/server.key}}`), "broker.mqtt.listen.ws.tls.key_file"),
		refuse("neither file", "broker.mqtt.listen.tcp.tls.cert_file",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {min_version: 1.3}}`)),
		refuse("neither file, on an operations listener", "broker.operations.listen.tcp.tls.cert_file",
			file("broker.operations.listen.tcp", `{address: "127.0.0.1:9090", tls: {client_ca_file: `+clientCA+`}}`)),
		refuse("require_certificate true without a CA", "broker.mqtt.listen.tcp.tls.require_certificate",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, require_certificate: true}}`), "broker.mqtt.listen.tcp.tls.client_ca_file"),
		refuse("require_certificate false without a CA", "broker.mqtt.listen.tcp.tls.require_certificate",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, require_certificate: false}}`), "broker.mqtt.listen.tcp.tls.client_ca_file"),
	},
	each([][2]string{
		{`cert_file: server.crt, key_file: ` + serverKey, "cert_file"},
		{`cert_file: none, key_file: ` + serverKey, "cert_file"},
		{`cert_file: "", key_file: ` + serverKey, "cert_file"},
		{`cert_file: ` + serverCert + `, key_file: server.key`, "key_file"},
		{`cert_file: ` + serverCert + `, key_file: none`, "key_file"},
		{`cert_file: ` + serverCert + `, key_file: ""`, "key_file"},
		{pair + `, client_ca_file: clients-ca.crt`, "client_ca_file"},
		{pair + `, client_ca_file: none`, "client_ca_file"},
		{pair + `, client_ca_file: ""`, "client_ca_file"},
	}, func(f [2]string) refusal {
		return refuse(f[0], "broker.mqtt.listen.tcp.tls."+f[1], file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+f[0]+`}}`))
	}),
	each([]string{"1.0", "1.1", "1.4", `"TLS1.3"`, "13", "none"}, func(v string) refusal {
		return refuse("min_version "+v, "broker.mqtt.listen.tcp.tls.min_version",
			file("broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pair+`, min_version: `+v+`}}`))
	}),
)

// listenerCollisionsRefused: address: "no two TCP listeners, operations
// included, on one port with equal or wildcard hosts, port 0 aside" (a ws
// listener is a TCP listener); name: "unique among MQTT listeners"; path:
// "unique, held by a <path>.lock file", among MQTT and operations
// listeners, a socket's path that is another socket's lock file refused
// whichever comes first. The later is refused, naming the earlier.
var listenerCollisionsRefused = append([]refusal{
	refuse("tcp and ws on one port", "broker.mqtt.listen.ws.address",
		file("broker.mqtt.listen.tcp.address", `":1883"`, "broker.mqtt.listen.ws.address", `":1883"`), "broker.mqtt.listen.tcp.address"),
	refuse("MQTT and operations on one port", "broker.operations.listen.tcp.address",
		file("broker.mqtt.listen.tcp.address", `":1883"`, "broker.operations.listen.tcp.address", `"127.0.0.1:1883"`), "broker.mqtt.listen.tcp.address"),
	refuse("ws and operations on one port", "broker.operations.listen.tcp.address",
		file("broker.mqtt.listen.ws.address", `":8080"`, "broker.operations.listen.tcp.address", `"[::1]:8080"`), "broker.mqtt.listen.ws.address"),
	refuse("one name twice in a kind", "broker.mqtt.listen.tcp[1].name",
		file("broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: a, address: ":1884"}]`), "broker.mqtt.listen.tcp[0].name"),
	refuse("one name on two kinds", "broker.mqtt.listen.unix.name",
		file("broker.mqtt.listen.tcp", `{name: edge, address: ":1883"}`, "broker.mqtt.listen.unix", `{name: edge, path: saguin.sock}`),
		"broker.mqtt.listen.tcp.name"),
	refuse("a name that is another kind's default", "broker.mqtt.listen.ws.name",
		file("broker.mqtt.listen.tcp", `{name: ws, address: ":1883"}`, "broker.mqtt.listen.ws.address", `":8080"`)),
	refuse("one path twice", "broker.mqtt.listen.unix[1].path",
		file("broker.mqtt.listen.unix", `[{name: a, path: /run/saguin/mqtt.sock}, {name: b, path: /run/saguin/mqtt.sock}]`),
		"broker.mqtt.listen.unix[0].path"),
	refuse("one path for MQTT and operations", "broker.operations.listen.unix.path",
		file("broker.mqtt.listen.unix.path", "/run/saguin/saguin.sock", "broker.operations.listen.unix.path", "/run/saguin/saguin.sock"),
		"broker.mqtt.listen.unix.path"),
	refuse("a path that is the lock file of the one before", "broker.mqtt.listen.unix[1].path",
		file("broker.mqtt.listen.unix", `[{name: a, path: /run/saguin/mqtt.sock}, {name: b, path: /run/saguin/mqtt.sock.lock}]`),
		"broker.mqtt.listen.unix[0].path"),
	refuse("a path whose lock file is the one before", "broker.mqtt.listen.unix[1].path",
		file("broker.mqtt.listen.unix", `[{name: a, path: /run/saguin/mqtt.lock}, {name: b, path: /run/saguin/mqtt}]`),
		"broker.mqtt.listen.unix[0].path"),
	refuse("a relative path that is the lock file of the one before", "broker.mqtt.listen.unix[1].path",
		file("broker.mqtt.listen.unix", `[{name: a, path: saguin.sock}, {name: b, path: saguin.sock.lock}]`),
		"broker.mqtt.listen.unix[0].path"),
	refuse("an operations path that is an MQTT path's lock file", "broker.operations.listen.unix.path",
		file("broker.mqtt.listen.unix.path", "/run/saguin/saguin.sock", "broker.operations.listen.unix.path", "/run/saguin/saguin.sock.lock"),
		"broker.mqtt.listen.unix.path"),
	refuse("an operations path whose lock file is an MQTT path", "broker.operations.listen.unix.path",
		file("broker.mqtt.listen.unix.path", "/run/saguin/saguin.sock.lock", "broker.operations.listen.unix.path", "/run/saguin/saguin.sock"),
		"broker.mqtt.listen.unix.path"),
}, each(slices.DeleteFunc(slices.Clone(tcpPairs), func(h hosts) bool { return !h.clash }), func(h hosts) refusal {
	return refuse(h.first+" and "+h.second, "broker.mqtt.listen.tcp[1].address", file(twoTCP(h)...), "broker.mqtt.listen.tcp[0].address")
})...)

// aclBeside is a file with the broker's password file and an acl_file, and
// sets.
func aclBeside(sets ...string) configuration {
	return file(slices.Concat([]string{"broker.mqtt.password_file", dirToken + "/passwd", "broker.mqtt.acl_file", dirToken + "/acl.yaml"}, sets)...)
}

// listenerIdentityRefused: allow_anonymous false "is refused on a listener
// with no identity source" (written on the listener, the listener's key is
// refused; taken from the broker's, the broker's, naming the listener);
// true, "on the broker or any listener, is refused beside an acl_file"
// (naming the acl_file); acl_file "needs a listener with an identity
// source", an MQTT listener.
var listenerIdentityRefused = []refusal{
	refuse("a listener's false with no identity source", "broker.mqtt.listen.tcp.allow_anonymous",
		file("broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: false}`)),
	refuse("a listener's false with server TLS alone", "broker.mqtt.listen.tcp.allow_anonymous",
		file("broker.mqtt.listen.tcp", `{address: ":8883", allow_anonymous: false, tls: {`+pair+`}}`)),
	refuse("the broker's false with no identity source", "broker.mqtt.allow_anonymous",
		file("broker.mqtt.allow_anonymous", "false"), "broker.mqtt.listen.tcp"),
	refuse("the broker's false on one listener of two with no identity source", "broker.mqtt.allow_anonymous",
		file("broker.mqtt.allow_anonymous", "false", "broker.mqtt.listen.tcp", `{address: ":8883", tls: {`+pairCA+`}}`,
			"broker.mqtt.listen.ws.address", `":8080"`),
		"broker.mqtt.listen.ws"),
	refuse("a tcp listener's true beside an acl_file", "broker.mqtt.listen.tcp.allow_anonymous",
		aclBeside("broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: true}`), "broker.mqtt.acl_file"),
	refuse("a ws listener's true beside an acl_file", "broker.mqtt.listen.ws.allow_anonymous",
		aclBeside("broker.mqtt.listen.ws", `{address: ":8080", allow_anonymous: true}`), "broker.mqtt.acl_file"),
	refuse("a unix listener's true beside an acl_file", "broker.mqtt.listen.unix.allow_anonymous",
		aclBeside("broker.mqtt.listen.unix", `{path: saguin.sock, proxy_protocol: true, allow_anonymous: true}`), "broker.mqtt.acl_file"),
	refuse("a list's second listener's true beside an acl_file", "broker.mqtt.listen.tcp[1].allow_anonymous",
		aclBeside("broker.mqtt.listen.tcp", `[{name: a, address: ":1883"}, {name: b, address: ":1884", allow_anonymous: true}]`), "broker.mqtt.acl_file"),
	refuse("a listener's true beside its own password file and an acl_file", "broker.mqtt.listen.tcp.allow_anonymous",
		file("broker.mqtt.allow_anonymous", "false", "broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.tcp", `{address: ":1883", password_file: <dir>/tcp.passwd, allow_anonymous: true}`),
		"broker.mqtt.acl_file"),
	refuse("a listener's true over the broker's false beside an acl_file", "broker.mqtt.listen.tcp.allow_anonymous",
		aclBeside("broker.mqtt.allow_anonymous", "false", "broker.mqtt.listen.tcp", `{address: ":8883", allow_anonymous: true, tls: {`+pairCA+`}}`),
		"broker.mqtt.acl_file"),
	refuse("the broker's true beside an acl_file", "broker.mqtt.allow_anonymous",
		aclBeside("broker.mqtt.allow_anonymous", "true"), "broker.mqtt.acl_file"),
	// The default, true without a password_file, is refused as a written
	// true is; the listener's own false has its CA, so no other rule is
	// broken.
	refuse("the broker's default true beside an acl_file", "broker.mqtt.allow_anonymous",
		file("broker.mqtt.acl_file", dirToken+"/acl.yaml", "broker.mqtt.listen.tcp", `{address: ":8883", allow_anonymous: false, tls: {`+pairCA+`}}`),
		"broker.mqtt.acl_file"),
	refuse("acl_file relative", "broker.mqtt.acl_file", file("broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", "acl.yaml")),
	refuse("acl_file empty", "broker.mqtt.acl_file", file("broker.mqtt.password_file", dirToken+"/passwd", "broker.mqtt.acl_file", `""`)),
	// Without an identity source, every MQTT listener's allow_anonymous
	// breaks a rule too: true is refused beside an acl_file, and false with
	// no identity source. Which of the two keys is named, config.md leaves
	// open.
	{
		name: "acl_file with no identity source",
		configuration: file("broker.mqtt.allow_anonymous", "false", "broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: true}`),
		key: "broker.mqtt.acl_file", or: "broker.mqtt.listen.tcp.allow_anonymous",
	},
	{
		// An operations listener's password file proves operators, not
		// clients.
		name: "acl_file whose only identity source is an operations listener",
		configuration: file("broker.mqtt.allow_anonymous", "false", "broker.mqtt.acl_file", dirToken+"/acl.yaml",
			"broker.mqtt.listen.tcp", `{address: ":1883", allow_anonymous: true}`,
			"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: <dir>/operators.passwd}`),
		key: "broker.mqtt.acl_file", or: "broker.mqtt.listen.tcp.allow_anonymous",
	},
}

// operationsRefused: broker.operations' listen "required"; "a non-loopback
// address (a host name included, localhost too) needs a password file or a
// required client certificate", a wildcard host not a loopback one;
// operations listeners' names unique, "as above"; the operators' file "a
// different file from the clients'", compared as paths cleaned;
// min_scrape_interval "at least 60s".
var operationsRefused = slices.Concat(
	[]refusal{
		refuse("listen required", "broker.operations.listen", file("broker.operations.min_scrape_interval", "60s")),
		refuse("a client certificate not required", "broker.operations.listen.tcp.address",
			file("broker.operations.listen.tcp", `{address: ":9090", tls: {`+pairCA+`, require_certificate: false}}`)),
		refuse("TLS without a client CA", "broker.operations.listen.tcp.address",
			file("broker.operations.listen.tcp", `{address: ":9090", tls: {`+pair+`}}`)),
		refuse("the second of a list not loopback", "broker.operations.listen.tcp[1].address",
			file("broker.operations.listen.tcp", `[{name: a, address: "127.0.0.1:9090"}, {name: b, address: ":9091"}]`)),
		refuse("one name on two operations listeners", "broker.operations.listen.unix.name",
			file("broker.operations.listen.tcp", `{name: ops, address: "127.0.0.1:9090"}`,
				"broker.operations.listen.unix", `{name: ops, path: /run/saguin/ops.sock}`),
			"broker.operations.listen.tcp.name"),
		refuse("the operators' file is the broker's", "broker.operations.password_file",
			file("broker.mqtt.password_file", dirToken+"/passwd", "broker.operations.password_file", dirToken+"/passwd",
				"broker.operations.listen.unix.path", "/run/saguin/ops.sock"),
			"broker.mqtt.password_file"),
		refuse("the operators' file is the broker's, spelled otherwise", "broker.operations.password_file",
			file("broker.mqtt.password_file", dirToken+"/passwd", "broker.operations.password_file", dirToken+"//conf/../passwd",
				"broker.operations.listen.unix.path", "/run/saguin/ops.sock"),
			"broker.mqtt.password_file"),
		refuse("the operators' file is an MQTT listener's", "broker.operations.password_file",
			file("broker.mqtt.listen.tcp", `{address: ":1883", password_file: <dir>/tcp.passwd}`,
				"broker.operations.password_file", dirToken+"/tcp.passwd", "broker.operations.listen.unix.path", "/run/saguin/ops.sock"),
			"broker.mqtt.listen.tcp.password_file"),
		refuse("an operations listener's file is the broker's", "broker.operations.listen.tcp.password_file",
			file("broker.mqtt.password_file", dirToken+"/passwd",
				"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: <dir>/passwd}`),
			"broker.mqtt.password_file"),
		refuse("an operations listener's file is an MQTT listener's, spelled otherwise", "broker.operations.listen.tcp.password_file",
			file("broker.mqtt.listen.ws", `{address: ":8080", password_file: <dir>/ws.passwd}`,
				"broker.operations.listen.tcp", `{address: "127.0.0.1:9090", password_file: <dir>/./ws.passwd}`),
			"broker.mqtt.listen.ws.password_file"),
		refuse("the operators' file relative", "broker.operations.password_file",
			file("broker.operations.password_file", "operators.passwd", "broker.operations.listen.unix.path", "/run/saguin/ops.sock")),
		refuse("the operators' file empty", "broker.operations.password_file",
			file("broker.operations.password_file", `""`, "broker.operations.listen.unix.path", "/run/saguin/ops.sock")),
	},
	each([]string{":9090", "0.0.0.0:9090", "[::]:9090", "192.0.2.10:9090", "[2001:db8::1]:9090", "localhost:9090", "LOCALHOST:9090",
		"broker.example.com:9090"}, func(a string) refusal {
		return refuse("not loopback "+a, "broker.operations.listen.tcp.address", file("broker.operations.listen.tcp.address", strconv.Quote(a)))
	}),
	each([]string{"59s", "60000ms", "0s", "1", "none"}, func(v string) refusal {
		return refuse("min_scrape_interval "+v, "broker.operations.min_scrape_interval",
			file("broker.operations.listen.unix.path", "/run/saguin/ops.sock", "broker.operations.min_scrape_interval", v))
	}),
)
