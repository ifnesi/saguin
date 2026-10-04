package config_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/passwd"
	"github.com/ifnesi/saguin/internal/store"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "saguin.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

const oneChannel = "\nchannels:\n  events:\n    type: append\n"

// RFC 0002 "Configuration"
//
// A Unix socket's permissions are its access control, so the mode is
// parsed as octal and defaulted to something closed rather than to
// whatever the process umask happens to be.
func TestSocketMode(t *testing.T) {
	for _, tc := range []struct {
		name, listen string
		want         os.FileMode
	}{
		{"default is owner and group", "      unix:\n        path: /tmp/s.sock\n", 0o660},
		{"explicit octal", "      unix:\n        path: /tmp/s.sock\n        mode: \"0600\"\n", 0o600},
		{"world readable if asked for", "      unix:\n        path: /tmp/s.sock\n        mode: \"0666\"\n", 0o666},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := config.Load(write(t,
				"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n"+tc.listen+oneChannel))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := f.Broker.MQTT.Listen.Unix[0].FileMode(); got != tc.want {
				t.Fatalf("mode %#o, want %#o", got, tc.want)
			}
		})
	}
}

// A mode that is not an octal file mode is a startup error, not a silent
// fallback: an operator who wrote 660 meaning 0660 has said something
// different from what they meant, and only a refusal tells them.
func TestSocketModeMustBeOctal(t *testing.T) {
	_, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      unix:\n        path: /tmp/s.sock\n"+
			"        mode: \"rw-rw----\"\n"+oneChannel))
	if err == nil {
		t.Fatal("a mode that is not octal was accepted")
	}
	if !strings.Contains(err.Error(), "listen.unix.mode") {
		t.Fatalf("the error does not name the key: %v", err)
	}
}

// A setting belonging to one listener cannot be written beside another,
// because each listener is a block of its own. This is the rule that used
// to need a validation of its own, and now needs none: there is nowhere
// to put a socket mode except on a socket.
func TestASettingCannotBeWrittenOnTheWrongListener(t *testing.T) {
	_, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:1883\n"+
			"        mode: \"0660\"\n"+oneChannel))
	if err == nil {
		t.Fatal("a socket mode was accepted on a TCP listener")
	}

	// And a listener block with nothing in it is a mistake, not a default.
	if _, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp: {}\n"+oneChannel)); err == nil {
		t.Fatal("a tcp listener with no address was accepted")
	}
	if _, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      unix: {}\n"+oneChannel)); err == nil {
		t.Fatal("a unix listener with no path was accepted")
	}
}

// RFC 0002 "Configuration"
//
// The default port applies only when no listener at all was asked for. A
// socket-only broker must not silently also open a TCP port, which is the
// listener its author was avoiding.
func TestListenDefaults(t *testing.T) {
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      unix:\n        path: /tmp/s.sock\n"+oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(f.Broker.MQTT.Listen.TCP) != 0 {
		t.Fatalf("a socket-only configuration also opened %q", f.Broker.MQTT.Listen.TCP[0].Address.Address)
	}

	f, _, err = config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      ws:\n        address: 127.0.0.1:8083\n"+oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(f.Broker.MQTT.Listen.TCP) != 0 {
		t.Fatalf("a websocket-only configuration also opened %q", f.Broker.MQTT.Listen.TCP[0].Address.Address)
	}

	f, _, err = config.Load(write(t, "broker:\n  id: t\n"+memStorage+oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(f.Broker.MQTT.Listen.TCP) != 1 || f.Broker.MQTT.Listen.TCP[0].Address.Address != ":1883" {
		t.Fatalf("with no listener configured, tcp is %v, want :1883", f.Broker.MQTT.Listen.TCP)
	}
}

// A kind written `null` means the same as not writing it at all - as it did
// before a kind could hold a list - so `tcp:`, `ws:` and `unix:` each
// written with nothing after them open nothing, and the broker falls back
// to the default TCP door exactly as it does when the keys are omitted
// outright (TestListenDefaults).
func TestAListenKindWrittenNullIsAbsent(t *testing.T) {
	f, _, err := config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n      ws:\n      unix:\n"+
			oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(f.Broker.MQTT.Listen.WS) != 0 || len(f.Broker.MQTT.Listen.Unix) != 0 {
		t.Fatalf("a null ws or unix opened one: ws=%v unix=%v",
			f.Broker.MQTT.Listen.WS, f.Broker.MQTT.Listen.Unix)
	}
	if len(f.Broker.MQTT.Listen.TCP) != 1 || f.Broker.MQTT.Listen.TCP[0].Address.Address != ":1883" {
		t.Fatalf("a null tcp did not fall back to the default door: %v", f.Broker.MQTT.Listen.TCP)
	}
}

// relToCwd spells abs relative to the working directory, for a path that
// must name the same file another door names absolutely.
func relToCwd(abs string) string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	rel, err := filepath.Rel(wd, abs)
	if err != nil {
		panic(err)
	}
	return rel
}

// RFC 0002 "Several listeners of a kind": --check-config refuses, by name
// with both doors quoted, a second door of a kind with no name of its own,
// two doors sharing one name, two doors that would fight over one port, and
// two Unix doors at one path. Each case has a passing control - the same
// shape with the one thing that collided made different - so the finding is
// proven to be about the clash and not about the shape.
func TestCheckConfigRefusesDoorClashesByName(t *testing.T) {
	for _, tc := range []struct {
		name, listen string
		want         string // substring of the error, or "" for no error
	}{
		{"two tcp doors, same host and port",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18830\n" +
				"        - name: b\n          address: 127.0.0.1:18830\n",
			"broker.mqtt.listen.tcp[a] (127.0.0.1:18830) and broker.mqtt.listen.tcp[b] " +
				"(127.0.0.1:18830): the same port"},
		{"control: two tcp doors, different ports",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18830\n" +
				"        - name: b\n          address: 127.0.0.1:18831\n", ""},
		{"a tcp door and a ws door, same port, one a wildcard host",
			"      tcp:\n        - name: fleet\n          address: 0.0.0.0:18832\n" +
				"      ws:\n        - name: web\n          address: 127.0.0.1:18832\n",
			"broker.mqtt.listen.tcp[fleet] (0.0.0.0:18832) and broker.mqtt.listen.ws[web] " +
				"(127.0.0.1:18832): the same port"},
		{"control: a tcp door and a ws door, same wildcard host, different ports",
			"      tcp:\n        - name: fleet\n          address: 0.0.0.0:18832\n" +
				"      ws:\n        - name: web\n          address: 0.0.0.0:18833\n", ""},
		{"two tcp doors, same port, one host is the IPv6 wildcard",
			"      tcp:\n        - name: a\n          address: \"[::]:18834\"\n" +
				"        - name: b\n          address: 127.0.0.1:18834\n",
			"the same port"},
		{"control: two tcp doors both on port 0 - the kernel picks a free port each time",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:0\n" +
				"        - name: b\n          address: 127.0.0.1:0\n", ""},
		{"a tcp door and the operations port, same host and port",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18835\n",
			"broker.mqtt.listen.tcp[fleet] (127.0.0.1:18835) and broker.operations.listen.tcp " +
				"(127.0.0.1:18835): the same port"},
		{"two unix doors, same path",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: /tmp/saguin-clash-a.sock\n",
			"broker.mqtt.listen.unix[a] and broker.mqtt.listen.unix[b]: the same path"},
		{"control: two unix doors, different paths",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: /tmp/saguin-clash-b.sock\n", ""},
		{"two tcp doors sharing a name",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18836\n" +
				"        - name: fleet\n          address: 127.0.0.1:18837\n",
			`are both named "fleet"`},
		{"control: two tcp doors, distinct names",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18836\n" +
				"        - name: local\n          address: 127.0.0.1:18837\n", ""},
		{"a tcp door named after ws's own default, beside a bare ws map",
			"      tcp:\n        - name: ws\n          address: 127.0.0.1:18838\n" +
				"      ws:\n        address: 127.0.0.1:18839\n",
			`are both named "ws"`},
		{"control: a tcp door named after its own kind, beside a bare ws map",
			"      tcp:\n        - name: tcp\n          address: 127.0.0.1:18838\n" +
				"      ws:\n        address: 127.0.0.1:18839\n", ""},
		{"a second tcp door with no name",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18840\n" +
				"        - address: 127.0.0.1:18841\n",
			"broker.mqtt.listen.tcp[2]: a second tcp door, and this one names none"},
		{"control: a second tcp door, named",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18840\n" +
				"        - name: local\n          address: 127.0.0.1:18841\n", ""},
		{"a single unnamed tcp list beside a single unnamed ws list are named tcp and ws, no clash",
			"      tcp:\n        - address: 127.0.0.1:18843\n      ws:\n        - address: 127.0.0.1:18844\n", ""},
		{"a single unnamed ws list beside a tcp door named ws",
			"      tcp:\n        - name: ws\n          address: 127.0.0.1:18843\n" +
				"      ws:\n        - address: 127.0.0.1:18844\n",
			`are both named "ws"`},
		{"two unix doors, one path spelled with ./",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: /tmp/./saguin-clash-a.sock\n",
			"broker.mqtt.listen.unix[a] and broker.mqtt.listen.unix[b]: the same path"},
		{"two unix doors, one path spelled with ..",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: /tmp/x/../saguin-clash-a.sock\n",
			"the same path"},
		{"two unix doors, one path with a doubled slash",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: /tmp//saguin-clash-a.sock\n",
			"the same path"},
		{"an absolute unix path and the same file relative to the working directory",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-a.sock\n" +
				"        - name: b\n          path: " + relToCwd("/tmp/saguin-clash-a.sock") + "\n",
			"the same path"},
		{"an MQTT unix door and the operations socket, one spelled with ./",
			"      unix:\n        - name: a\n          path: /tmp/saguin-clash-o.sock\n",
			"the same path"},
		{"two tcp doors, IPv4-mapped IPv6 host against the IPv4 one",
			"      tcp:\n        - name: a\n          address: \"[::ffff:127.0.0.1]:18845\"\n" +
				"        - name: b\n          address: 127.0.0.1:18845\n",
			"the same port"},
		{"two tcp doors, expanded IPv6 loopback against ::1",
			"      tcp:\n        - name: a\n          address: \"[0:0:0:0:0:0:0:1]:18846\"\n" +
				"        - name: b\n          address: \"[::1]:18846\"\n",
			"the same port"},
		{"two tcp doors, [::0] is the wildcard",
			"      tcp:\n        - name: a\n          address: \"[::0]:18847\"\n" +
				"        - name: b\n          address: 127.0.0.1:18847\n",
			"the same port"},
		{"control: two tcp doors, ::1 against 127.0.0.1 are different hosts",
			"      tcp:\n        - name: a\n          address: \"[::1]:18848\"\n" +
				"        - name: b\n          address: 127.0.0.1:18848\n", ""},
		{"a tcp door and the operations port, the port written with a leading zero",
			"      tcp:\n        address: 127.0.0.1:018835\n", "the same port"},
		{"a tcp door and the operations port, the port written with a +",
			"      tcp:\n        address: 127.0.0.1:+18835\n", "the same port"},
		{"a tcp door with no port",
			"      tcp:\n        address: 127.0.0.1\n", "missing port in address"},
		{"a ws door with no port",
			"      ws:\n        address: 127.0.0.1\n", "missing port in address"},
		{"a tcp door with a port above 65535",
			"      tcp:\n        address: 127.0.0.1:65536\n", "is not a number from 0 to 65535"},
		{"a tcp door with a negative port",
			"      tcp:\n        address: 127.0.0.1:-1\n", "is not a number from 0 to 65535"},
		{"a tcp door with a port that is no service name",
			"      tcp:\n        address: 127.0.0.1:nosuchsvc\n", "is not a number from 0 to 65535"},
		{"a ws door with a bracketless IPv6 host",
			"      ws:\n        address: \"::1:18870\"\n", "too many colons"},
		{"a tcp door with white space in the host",
			"      tcp:\n        address: \" 127.0.0.1:18871\"\n", "is not a host name or an IP literal"},
		{"control: a tcp door with an empty port, the kernel's pick",
			"      tcp:\n        address: \"127.0.0.1:\"\n", ""},
		{"control: a tcp door whose port is a number with a leading zero, on a free port",
			"      tcp:\n        address: 127.0.0.1:018872\n", ""},
		{"a unix door with a NUL in the path",
			"      unix:\n        path: \"/tmp/saguin-bad\\0.sock\"\n", "contains a NUL byte"},
		{"a unix door whose path is 107 bytes and a trailing space, 108 as bound",
			"      unix:\n        path: \"/tmp/" + strings.Repeat("z", 102) + " \"\n", "characters and a Unix socket path may be"},
		{"control: a unix door whose path is exactly 107 bytes",
			"      unix:\n        path: \"/tmp/" + strings.Repeat("z", 102) + "\"\n", ""},
		{"two tcp doors, a zone on ::1 is ignored by the kernel",
			"      tcp:\n        - name: a\n          address: \"[::1%lo]:18880\"\n" +
				"        - name: b\n          address: \"[::1]:18880\"\n", "the same port"},
		{"two tcp doors, two different zones on ::1 are one address",
			"      tcp:\n        - name: a\n          address: \"[::1%lo]:18881\"\n" +
				"        - name: b\n          address: \"[::1%2]:18881\"\n", "the same port"},
		{"two tcp doors, [::%lo] is the wildcard whatever its zone",
			"      tcp:\n        - name: a\n          address: \"[::%lo]:18894\"\n" +
				"        - name: b\n          address: \"[::1]:18894\"\n", "the same port"},
		{"control: two tcp doors, one link-local address in two zones is two sockets",
			"      tcp:\n        - name: a\n          address: \"[fe80::1%eth0]:18882\"\n" +
				"        - name: b\n          address: \"[fe80::1%eth1]:18882\"\n", ""},
		{"two tcp doors, one link-local address in one zone",
			"      tcp:\n        - name: a\n          address: \"[fe80::1%eth0]:18883\"\n" +
				"        - name: b\n          address: \"[fe80::1%eth0]:18883\"\n", "the same port"},
		{"a tcp door with an empty zone",
			"      tcp:\n        address: \"[::1%]:18884\"\n", "is not an IP literal"},
		{"a tcp door with a vertical tab in the host",
			"      tcp:\n        address: \"127.0.0.1\\v:18885\"\n", "is not a host name or an IP literal"},
		{"a tcp door with a no-break space in the host",
			"      tcp:\n        address: \"127.0.0.1\\xa0:18885\"\n", "is not a host name or an IP literal"},
		{"a tcp door with a control character in the host",
			"      tcp:\n        address: \"127.0.0.1\\x1b:18885\"\n", "is not a host name or an IP literal"},
		{"a tcp door with an underscore in the host name",
			"      tcp:\n        address: \"a_b.example:18886\"\n", "is not a host name or an IP literal"},
		{"a tcp door with a host label starting in a hyphen",
			"      tcp:\n        address: \"-a.example:18887\"\n", "labels are 1 to 63"},
		{"a tcp door with an empty host label",
			"      tcp:\n        address: \"a..b:18888\"\n", "labels are 1 to 63"},
		{"a tcp door with a 64-character host label",
			"      tcp:\n        address: \"" + strings.Repeat("a", 64) + ":18889\"\n", "labels are 1 to 63"},
		{"a tcp door with an IPv6 multicast host",
			"      tcp:\n        address: \"[ff02::1%eth0]:18890\"\n", "multicast"},
		{"a tcp door with a link-local host and no zone",
			"      tcp:\n        address: \"[fe80::1]:18891\"\n", "link-local address with no zone"},
		{"control: a tcp door with a well-formed host name",
			"      tcp:\n        address: \"broker-1.example.com.:18892\"\n", ""},
		{"control: a tcp door with a zone on loopback alone",
			"      tcp:\n        address: \"[::1%lo]:18893\"\n", ""},
		{"a unix door whose path ends in /.",
			"      unix:\n        path: /tmp/saguin-dir/.\n", "its last element is \".\""},
		{"a unix door whose path ends in /..",
			"      unix:\n        path: /tmp/saguin-dir/..\n", "its last element is \"..\""},
		{"a unix door whose path ends in //",
			"      unix:\n        path: /tmp/saguin-dir//\n", "its last element is \"\""},
		{"control: a unix door whose file name merely contains dots",
			"      unix:\n        path: /tmp/saguin-dir/..sock\n", ""},
		{"a unix door whose path ends in a slash",
			"      unix:\n        path: /tmp/saguin-dir/\n", "its last element is \"\""},
		{"control: a single tcp door, the map form, needs no name at all",
			"      tcp:\n        address: 127.0.0.1:18842\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n" + tc.listen +
				"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:18835\n" +
				"      unix:\n        path: /tmp/./saguin-clash-o.sock\n" +
				"    password_file: /etc/saguin/operations.passwd\n" +
				oneChannel
			_, _, err := config.Load(write(t, body))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("a passing configuration was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("a clash was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not say %q: %v", tc.want, err)
			}
		})
	}
}

// RFC 0005 "The operations listener": the operations doors' names are their
// own set (a tcp door named like an MQTT door is no clash), and within it a
// second unnamed door, a repeated name, a shared port and a shared path are
// refused by name, each beside a passing control.
func TestCheckConfigRefusesOperationsDoorClashesByName(t *testing.T) {
	for _, tc := range []struct{ name, ops, want string }{
		{"two tcp doors, same port",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18850\n" +
				"        - name: b\n          address: 127.0.0.1:18850\n",
			"broker.operations.listen.tcp[a] (127.0.0.1:18850) and " +
				"broker.operations.listen.tcp[b] (127.0.0.1:18850): the same port"},
		{"control: two tcp doors, two ports",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18850\n" +
				"        - name: b\n          address: 127.0.0.1:18851\n", ""},
		{"two tcp doors sharing a name",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18850\n" +
				"        - name: a\n          address: 127.0.0.1:18851\n",
			`are both named "a"`},
		{"a second tcp door with no name",
			"      tcp:\n        - name: a\n          address: 127.0.0.1:18850\n" +
				"        - address: 127.0.0.1:18851\n",
			"broker.operations.listen.tcp[2]: a second tcp door, and this one names none"},
		{"two unix doors, same path",
			"      unix:\n        - name: a\n          path: /tmp/saguin-ops-a.sock\n" +
				"        - name: b\n          path: /tmp/saguin-ops-a.sock\n",
			"broker.operations.listen.unix[a] and broker.operations.listen.unix[b]: the same path"},
		{"control: an operations door named like an MQTT door shares no namespace",
			"      tcp:\n        - name: fleet\n          address: 127.0.0.1:18850\n" +
				"        - name: other\n          address: 127.0.0.1:18851\n", ""},
		{"an operations door with no port",
			"      tcp:\n        address: 127.0.0.1\n", "missing port in address"},
		{"an operations door and an MQTT door, the port with a leading zero",
			"      tcp:\n        address: 127.0.0.1:018860\n", "the same port"},
		{"an operations unix door with a NUL in the path",
			"      unix:\n        path: \"/tmp/saguin-ops-bad\\0.sock\"\n" +
				"    password_file: /etc/saguin/ops.passwd\n", "contains a NUL byte"},
		{"a routable door with no credential names itself",
			"      tcp:\n        - name: probe\n          address: 127.0.0.1:18850\n" +
				"        - name: fleet\n          address: 0.0.0.0:18851\n",
			"broker.operations.listen.tcp[fleet].address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "broker:\n  id: t\n" + memStorage +
				"  mqtt:\n    listen:\n      tcp:\n        - name: fleet\n" +
				"          address: 127.0.0.1:18860\n        - name: b\n          address: 127.0.0.1:18861\n" +
				"  operations:\n    listen:\n" + tc.ops + oneChannel
			_, _, err := config.Load(write(t, body))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("a passing configuration was refused: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("a refusable configuration was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not say %q: %v", tc.want, err)
			}
		})
	}
}

// RFC 0002 "Configuration"
//
// The example configuration in the RFC is the first thing a contributor
// copies. Unknown keys are a hard error, so every key it shows must be a
// key the broker accepts - this test is the tie between the two, and it
// fails the moment the RFC grows a key the loader does not know.
func TestTheLimitsBlockFromRFC0002Loads(t *testing.T) {
	f, _, err := config.Load(write(t, `
broker:
  id: edge-1
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
  mqtt:
    listen:
      tcp:
        address: 0.0.0.0:1883
      unix:
        path: /run/saguin/saguin.sock
        mode: "0660"
  limits:
    max_message_size: 1MiB
    max_topic_length: 1024
    max_client_id_length: 256
    max_header_count: 32
    max_header_bytes: 8KiB
    max_connections: 10000
    max_session_expiry: 30d
    write_timeout: 5s
    connect_timeout: 10s
    max_connect_size: 100KiB
    max_connect_rate: 500

channels:
  jobs:
    type: queue
    visibility_timeout: 30s
    job_expires_after: 6h
    retry:
      max_attempts: 5
`))
	if err != nil {
		t.Fatalf("the RFC's own configuration does not load: %v", err)
	}
	got := f.Broker.Limits.Resolve()
	want := config.Resolved{
		MaxMessageSize:    1 << 20,
		MaxTopicLength:    1024,
		MaxClientIDLength: 256,
		MaxHeaderCount:    32,
		MaxHeaderBytes:    8 << 10,
		MaxConnections:    10000,
		MaxSubscriptions:  10000,
		MaxTopicLevels:    200,
		MaxSessionExpiry:  30 * 24 * 3600,
		WriteTimeout:      5 * time.Second,
		ConnectTimeout:    10 * time.Second,
		MaxConnectSize:    100 << 10,
		MaxConnectRate:    500,
		SessionQueueBytes: 1 << 20,
	}
	if got != want {
		t.Fatalf("limits %+v, want %+v", got, want)
	}
}

// RFC 0002 "How much a session may hold: limits.session_queue_bytes"
//
// Absent is 1 MiB, or max_message_size where that is larger; a size loads; and
// `none`, zero, a size below max_message_size and anything unparseable are
// refused by name.
func TestSessionQueueBytes(t *testing.T) {
	cfg := func(lines string) string {
		return "broker:\n  id: t\n" + memStorage + "  limits:\n" + lines + oneChannel
	}
	for _, tc := range []struct {
		lines string
		want  int64
	}{
		{"    max_topic_length: 1024\n", 1 << 20},
		{"    session_queue_bytes: 64MiB\n", 64 << 20},
		{"    max_message_size: 8MiB\n", 8 << 20},
		{"    max_message_size: 64KiB\n    session_queue_bytes: 64KiB\n", 64 << 10},
	} {
		f, _, err := config.Load(write(t, cfg(tc.lines)))
		if err != nil {
			t.Fatalf("%q does not load: %v", tc.lines, err)
		}
		if got := f.Broker.Limits.Resolve().SessionQueueBytes; got != tc.want {
			t.Errorf("%q resolved to %d, want %d", tc.lines, got, tc.want)
		}
	}
	for _, lines := range []string{
		"    session_queue_bytes: none\n",
		"    session_queue_bytes: 0\n",
		"    session_queue_bytes: lots\n",
		"    session_queue_bytes: 512KiB\n",
		"    max_message_size: 4MiB\n    session_queue_bytes: 2MiB\n",
	} {
		_, _, err := config.Load(write(t, cfg(lines)))
		if err == nil || !strings.Contains(err.Error(), "limits.session_queue_bytes") {
			t.Errorf("%q was not refused by name: %v", lines, err)
		}
	}
}

// RFC 0002 "Validation": limits.session_queue_full is refused by name, in
// every value, because a full session always gives up its oldest. Absent, the
// file loads; any value, the one that described that behaviour included, is
// refused naming the key and what a full session does.
func TestSessionQueueFull(t *testing.T) {
	cfg := func(lines string) string {
		return "broker:\n  id: t\n" + memStorage + "  limits:\n" + lines + oneChannel
	}
	if _, _, err := config.Load(write(t, cfg("    max_topic_length: 1024\n"))); err != nil {
		t.Fatalf("a file without the key does not load: %v", err)
	}
	for _, v := range []string{"drop_oldest", "drop_newest", "DROP_NEWEST", "drop", "none"} {
		_, _, err := config.Load(write(t, cfg("    session_queue_full: "+v+"\n")))
		if err == nil || !strings.Contains(err.Error(), "limits.session_queue_full is gone") ||
			!strings.Contains(err.Error(), "gives up the oldest") {
			t.Errorf("session_queue_full: %s was not refused by name, saying what a full session does: %v", v, err)
		}
	}
}

// RFC 0002 "How long a socket may wait to send CONNECT: limits.connect_timeout"
//
// Absent is 10s; a whole number of seconds from 1s loads; `none`, zero and a
// sub-second value are refused. Each refusal is matched on the key's name so
// a case that loads for a different reason cannot pass as refused.
func TestConnectTimeout(t *testing.T) {
	cfg := func(v string) string {
		lim := ""
		if v != "" {
			lim = "  limits:\n    connect_timeout: " + v + "\n"
		}
		return "broker:\n  id: t\n" + memStorage + lim + oneChannel
	}
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{{"", 10 * time.Second}, {"1s", time.Second}, {"2m", 2 * time.Minute}} {
		f, _, err := config.Load(write(t, cfg(tc.value)))
		if err != nil {
			t.Fatalf("connect_timeout %q does not load: %v", tc.value, err)
		}
		if got := f.Broker.Limits.Resolve().ConnectTimeout; got != tc.want {
			t.Errorf("connect_timeout %q resolved to %v, want %v", tc.value, got, tc.want)
		}
	}
	for _, v := range []string{"none", "0s", "500ms", "soon"} {
		_, _, err := config.Load(write(t, cfg(v)))
		if err == nil || !strings.Contains(err.Error(), "limits.connect_timeout") {
			t.Errorf("connect_timeout %q was not refused by name: %v", v, err)
		}
	}
}

// RFC 0002 "The largest CONNECT before authentication: limits.max_connect_size"
// and "How fast connections are accepted: limits.max_connect_rate".
//
// Absent is 100 KiB and 500 a second; a size and a whole number load; `none`
// turns the rate off; a size of zero, one larger than max_message_size, a
// rate of zero or below, and anything unparseable are refused by name.
func TestMaxConnectSizeAndRate(t *testing.T) {
	cfg := func(lines string) string {
		return "broker:\n  id: t\n" + memStorage + "  limits:\n" + lines + oneChannel
	}
	for _, tc := range []struct {
		lines string
		size  uint32
		rate  int
	}{
		{"    max_topic_length: 1024\n", 100 << 10, 500},
		{"    max_connect_size: 16KiB\n    max_connect_rate: 2000\n", 16 << 10, 2000},
		{"    max_connect_rate: none\n", 100 << 10, 0},
		// A max_message_size below the default caps the default rather than
		// making an unwritten key a refusal.
		{"    max_message_size: 64KiB\n", 64 << 10, 500},
		// Above the default max_message_size once that is raised to hold it.
		{"    max_message_size: 4MiB\n    max_connect_size: 2MiB\n", 2 << 20, 500},
	} {
		f, _, err := config.Load(write(t, cfg(tc.lines)))
		if err != nil {
			t.Fatalf("%q does not load: %v", tc.lines, err)
		}
		got := f.Broker.Limits.Resolve()
		if got.MaxConnectSize != tc.size || got.MaxConnectRate != tc.rate {
			t.Errorf("%q resolved to size %d rate %d, want %d and %d",
				tc.lines, got.MaxConnectSize, got.MaxConnectRate, tc.size, tc.rate)
		}
	}
	for _, tc := range []struct{ lines, key string }{
		{"    max_connect_size: 0\n", "limits.max_connect_size"},
		{"    max_connect_size: big\n", "limits.max_connect_size"},
		{"    max_message_size: 64KiB\n    max_connect_size: 100KiB\n", "limits.max_connect_size"},
		// Larger than max_message_size where that is left at its default of
		// 1MiB, which validation did not ask; and
		// 4GiB, which passed and wrapped to 0 in Resolve's uint32.
		{"    max_connect_size: 2MiB\n", "limits.max_connect_size"},
		{"    max_connect_size: 4GiB\n", "limits.max_connect_size"},
		{"    max_connect_rate: 0\n", "limits.max_connect_rate"},
		{"    max_connect_rate: -5\n", "limits.max_connect_rate"},
		{"    max_connect_rate: fast\n", "limits.max_connect_rate"},
	} {
		_, _, err := config.Load(write(t, cfg(tc.lines)))
		if err == nil || !strings.Contains(err.Error(), tc.key) {
			t.Errorf("%q was not refused by name: %v", tc.lines, err)
		}
	}
}

// RFC 0002 "Which web pages may connect: same_origin and allowed_origins"
//
// A list of origins loads on the ws listener and resolves to the form they
// are compared in; an entry that is not an origin is refused by name; and
// the key does not exist on the tcp listener, where no browser can arrive.
func TestAllowedOrigins(t *testing.T) {
	ws := func(origins string) string {
		return "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      ws:\n        address: 127.0.0.1:8083\n" +
			origins + oneChannel
	}
	f, _, err := config.Load(write(t, ws("        allowed_origins:\n"+
		"          - https://Dashboard.example.com:443\n          - http://10.0.0.5:8080\n")))
	if err != nil {
		t.Fatalf("a list of origins does not load: %v", err)
	}
	got := f.Broker.MQTT.Listen.WS[0].Origins()
	want := []string{"https://dashboard.example.com", "http://10.0.0.5:8080"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("allowed_origins resolved to %q, want %q", got, want)
	}

	f, _, err = config.Load(write(t, ws("")))
	if err != nil {
		t.Fatalf("a ws listener with no allowed_origins does not load: %v", err)
	}
	if got := f.Broker.MQTT.Listen.WS[0].Origins(); len(got) != 0 {
		t.Errorf("no allowed_origins resolved to %q, want none", got)
	}
	// same_origin is on unless the file turns it off.
	if !f.Broker.MQTT.Listen.WS[0].SameOriginOn() {
		t.Error("a ws listener that does not mention same_origin resolved it off")
	}
	f, _, err = config.Load(write(t, ws("        same_origin: false\n")))
	if err != nil {
		t.Fatalf("same_origin: false does not load: %v", err)
	}
	if f.Broker.MQTT.Listen.WS[0].SameOriginOn() {
		t.Error("same_origin: false resolved on")
	}

	for _, bad := range []string{"https://dashboard.example.com/app", "dashboard.example.com", "*"} {
		_, _, err := config.Load(write(t, ws("        allowed_origins:\n          - \""+bad+"\"\n")))
		if err == nil || !strings.Contains(err.Error(), "broker.mqtt.listen.ws.allowed_origins") {
			t.Errorf("allowed_origins entry %q was not refused by name: %v", bad, err)
		}
	}

	_, _, err = config.Load(write(t, "broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        address: 127.0.0.1:1883\n        allowed_origins:\n          - https://a.example\n"+
		oneChannel))
	if err == nil || !strings.Contains(err.Error(), "allowed_origins") {
		t.Errorf("allowed_origins on the tcp listener loaded: %v", err)
	}
}

// A configuration naming no limits still gets them. An unbounded broker is
// not a sensible default on the hardware saguin targets.
func TestLimitsDefault(t *testing.T) {
	f, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := f.Broker.Limits.Resolve(); got.MaxMessageSize != 1<<20 ||
		got.MaxTopicLength != 1024 || got.MaxClientIDLength != 256 ||
		got.MaxHeaderCount != 32 ||
		got.MaxHeaderBytes != 8<<10 || got.MaxConnections != 10000 ||
		got.MaxSessionExpiry != 30*24*3600 {
		t.Fatalf("defaults are %+v", got)
	}
}

func TestLimitsAreRefused(t *testing.T) {
	for _, tc := range []struct{ name, limits, want string }{
		{"a size with no unit we know", "    max_message_size: 1MB\n", "max_message_size"},
		{"a size that is not a number", "    max_header_bytes: big\n", "max_header_bytes"},
		{"zero bytes", "    max_message_size: 0\n", "max_message_size"},
		{"a negative topic length", "    max_topic_length: -1\n", "max_topic_length"},
		{"zero headers", "    max_header_count: -3\n", "max_header_count"},
		// Below 23 rather than below 1: MQTT requires a server to accept any
		// client id of 1 to 23 bytes, so a lower bound than that is a broker
		// refusing connections the specification says it must take.
		{"a client id bound MQTT forbids", "    max_client_id_length: 16\n", "max_client_id_length"},
		{"no connections at all", "    max_connections: -1\n", "max_connections"},
		// `none` is accepted by the retention keys and refused here on
		// purpose: a session that never expires is the unbounded table this
		// key exists to close.
		{"a session that never expires", "    max_session_expiry: none\n", "max_session_expiry"},
		{"a session expiry with no unit", "    max_session_expiry: 2592000\n", "max_session_expiry"},
		{"a session expiry below a second", "    max_session_expiry: 500ms\n", "max_session_expiry"},
		{"a session expiry of zero", "    max_session_expiry: 0s\n", "max_session_expiry"},
		{"headers larger than any message", "    max_header_bytes: 5GiB\n", "max_header_bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t,
				"broker:\n  id: t\n"+memStorage+"  limits:\n"+tc.limits+oneChannel))
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the error does not name %s: %v", tc.want, err)
			}
		})
	}
}

// **A size too large to hold is refused, never wrapped.** The count was
// multiplied by its unit with no bound, so 17179869185GiB - 2^64 bytes and
// one more GiB - came back as exactly 1GiB and was accepted, and a
// retention bound meant as "keep everything" trimmed at a gigabyte. The
// largest that fits is accepted exactly, so a guard refusing too much fails
// here as well; and the retention key is driven through Load, since that is
// where an operator meets it.
func TestASizeTooLargeToHoldIsRefusedRatherThanWrapped(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64 // 0 for a refusal
	}{
		{"17179869185GiB", 0},
		{"17179869184GiB", 0},
		{"8589934592GiB", 0},
		{"9223372036854775807KiB", 0},
		{"8589934591GiB", 8589934591 << 30},
		{"9223372036854775807", 9223372036854775807},
	} {
		got, err := config.ParseBytes(tc.in)
		switch {
		case tc.want == 0 && err == nil:
			t.Errorf("%s was read as %d bytes, want a refusal", tc.in, got)
		case tc.want == 0 && !strings.Contains(err.Error(), "larger than saguin can hold"):
			t.Errorf("%s was refused without saying it is too large: %v", tc.in, err)
		case tc.want != 0 && (err != nil || got != tc.want):
			t.Errorf("%s was read as %d, %v; want %d", tc.in, got, err, tc.want)
		}
	}
	_, _, err := config.Load(write(t, "broker:\n  id: t\n  storage:\n    default: mem\n"+
		"    default_retention_period: none\n    default_retention_bytes: 17179869185GiB\n"+
		"    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"+oneChannel))
	if err == nil || !strings.Contains(err.Error(), "default_retention_bytes") {
		t.Errorf("a retention bound of 17179869185GiB was not refused naming the key: %v", err)
	}
}

// RFC 0002 "Validation": visibility_timeout is greater than zero and less
// than job_expires_after. Otherwise a record can expire while a worker still
// holds it, and the worker's answer arrives for a record that is gone.
func TestJobExpiresAfter(t *testing.T) {
	queue := func(body string) string {
		return "broker:\n  id: t\n" + memStorage + "channels:\n  jobs:\n    type: queue\n" + body
	}

	_, reg, err := config.Load(write(t, queue("    visibility_timeout: 30s\n    job_expires_after: 6h\n")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := reg.All()["jobs"].JobExpiresAfter; got != 6*60*60 {
		t.Fatalf("job_expires_after is %d seconds, want %d", got, 6*60*60)
	}

	for _, tc := range []struct{ name, body string }{
		{"at the visibility timeout", "    visibility_timeout: 30s\n    job_expires_after: 30s\n"},
		{"below the visibility timeout", "    visibility_timeout: 1m\n    job_expires_after: 30s\n"},
		{"not a duration", "    job_expires_after: soon\n"},
		{"zero", "    job_expires_after: 0s\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := config.Load(write(t, queue(tc.body))); err == nil {
				t.Fatal("accepted")
			}
		})
	}

	// job_expires_after is queue policy. On any other type it is a statement the
	// broker cannot honour, so it is a startup error rather than a no-op.
	_, _, err = config.Load(write(t,
		"broker:\n  id: t\n"+memStorage+"channels:\n  events:\n    type: append\n    job_expires_after: 6h\n"))
	if err == nil {
		t.Fatal("job_expires_after was accepted on an append channel")
	}
}

// The example configuration is what a new reader runs first, and unknown
// keys are a hard error. This test is the tie between the shipped example
// and the loader: it fails if a key is documented there before it exists,
// or if a key is removed while the example still names it.
func TestTheShippedExampleLoads(t *testing.T) {
	f, reg, err := config.Load(filepath.Join("..", "..", "examples", "saguin.yaml"))
	if err != nil {
		t.Fatalf("examples/saguin.yaml does not load: %v", err)
	}
	for _, name := range []string{"events", "state", "jobs"} {
		if _, ok := reg.All()[name]; !ok {
			t.Fatalf("the example no longer configures %q, which the demo uses", name)
		}
	}
	// Every limit the example states is the default, so a reader who
	// deletes the block gets the same broker.
	if got, want := f.Broker.Limits.Resolve(), (config.Limits{}).Resolve(); got != want {
		t.Fatalf("the example states limits %+v, but the defaults are %+v", got, want)
	}
}

// The example's first line is "Every key saguin accepts today", and
// loading cannot hold it to that: a live unknown key fails to load, but a
// key the loader GROWS is invisible to every test that only loads. Seven
// keys were added that way - `start`, `min_protocol_version` and the two
// commit keys among them - while the file went on making the claim, and
// nothing could fail.
//
// So the key set comes from the loader's own structs, by walking this
// package's syntax trees for yaml tags, and "names" is what is checked:
// the key appearing as a word - live, commented out, or in a comment
// listing keys deliberately not written - because several keys cannot be
// written live here (a copy key on a demonstration that publishes to
// every channel it has), and the file's convention is to name those in a
// comment saying why.
func TestTheShippedExampleNamesEveryKey(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "examples", "saguin.yaml"))
	if err != nil {
		t.Fatalf("cannot read the example: %v", err)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("cannot read the package directory: %v", err)
	}
	fset := token.NewFileSet()
	keys := map[string]bool{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, e.Name(), nil, 0)
		if err != nil {
			t.Fatalf("cannot parse %s: %v", e.Name(), err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			st, ok := n.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range st.Fields.List {
				if field.Tag == nil {
					continue
				}
				tag, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					continue
				}
				name, _, _ := strings.Cut(reflect.StructTag(tag).Get("yaml"), ",")
				if name != "" && name != "-" {
					keys[name] = true
				}
			}
			return true
		})
	}

	// A walk that read the wrong directory finds nothing and would pass
	// every assertion below. The loader accepts 76 keys today; a count
	// well under that means the instrument broke, not that keys left.
	if len(keys) < 60 {
		t.Fatalf("only %d yaml keys found walking this package; the loader "+
			"accepts more than that, so this walk read the wrong thing", len(keys))
	}

	// **The keys that exist only to be refused are not keys the example may
	// document.** Each is a spelling that used to work, kept on its struct
	// so the refusal can say what became of it rather than showing an
	// operator the name of a Go type - see BridgeConfig.Upstream and
	// Broker.Replica. Writing one into the example would teach a key that
	// stops the broker starting, which is the opposite of what the file is
	// for.
	//
	// Named individually with the reason, rather than matched by a naming
	// convention: the list is what makes adding a *real* key and forgetting
	// to document it still fail, which is the whole of this test.
	refusedOnly := map[string]string{
		"upstream":           "renamed to peer:, kept so the refusal can say so",
		"inbound":            "renamed to topics:, kept so the refusal can say so",
		"channel":            "gone from a bridge rule; the topic decides",
		"replica":            "gone; a copy is a bridge plus a copy of the storage",
		"bridge_source":      "gone with the replica; a channel is not a copy of anything",
		"dlq_bridge_source":  "gone with the replica, for the same reason",
		"commit_interval":    "renamed to publish_commit_interval, kept so the refusal can say so",
		"commit_max_records": "renamed to publish_commit_max_records, kept so the refusal can say so",
	}
	for name := range keys {
		if why, ok := refusedOnly[name]; ok {
			// It must still be refused, or this exemption is hiding a key
			// that quietly works and is documented nowhere.
			if !refuses(t, name) {
				t.Errorf("%q is exempt from the example as a key that only exists to be "+
					"refused (%s), and the loader accepted a file using it", name, why)
			}
			continue
		}
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).Match(raw) {
			t.Errorf("examples/saguin.yaml does not name %q, which the loader "+
				"accepts - the file's first line promises every key", name)
		}
	}
}

// refuses reports whether a configuration written with a removed key is
// refused, which is what an exemption above is claiming.
func refuses(t *testing.T, key string) bool {
	t.Helper()
	var body string
	switch key {
	case "replica":
		body = "broker:\n  id: t\n  replica: true\n" + memStorage + "channels: {}\n"
	case "bridge_source", "dlq_bridge_source":
		body = "broker:\n  id: t\n" + memStorage +
			"channels:\n  events:\n    type: append\n    " + key + ": somewhere\n"
	case "upstream":
		body = "broker:\n  id: t\n" + memStorage + "channels:\n  events:\n    type: append\n" +
			"bridges:\n  a:\n    upstream: tcp://h:1883\n    client_id: v\n    topics:\n" +
			"      - filter: x/#\n        topic: events/$#\n        direction: in\n"
	case "inbound":
		body = "broker:\n  id: t\n" + memStorage + "channels:\n  events:\n    type: append\n" +
			"bridges:\n  a:\n    peer: tcp://h:1883\n    client_id: v\n    inbound:\n" +
			"      - filter: x/#\n        topic: events/$#\n        direction: in\n"
	case "channel":
		body = "broker:\n  id: t\n" + memStorage + "channels:\n  events:\n    type: append\n" +
			"bridges:\n  a:\n    peer: tcp://h:1883\n    client_id: v\n    topics:\n" +
			"      - filter: x/#\n        topic: events/$#\n        channel: events\n" +
			"        direction: in\n"
	case "commit_interval", "commit_max_records":
		body = "broker:\n  id: t\n  storage:\n    default: p\n    default_retention_period: none\n" +
			"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
			"        file_path: /var/lib/s.db\n        " + key + ": 4\n" +
			"channels:\n  events:\n    type: append\n"
	default:
		t.Fatalf("no probe written for the exempt key %q", key)
	}
	_, _, err := config.Load(write(t, body))
	return err != nil
}

// Loading is not running, and the difference is a path.
//
// `TestTheShippedExampleLoads` accepts `pid_file: /run/saguin/saguin.pid`
// because it is absolute, which is the whole of what the loader asks. The
// broker then refuses to start, because `/run/saguin` does not exist on the
// machine a reader is on - and it refuses at the right moment, after the
// listeners are up and the log says "saguin listening", which is where a
// reader stops believing the fault is theirs. That happened here: the key
// went into the example pointing at the path a real deployment uses, and
// `make demo` died at the first connection.
//
// So the rule this checks is not "is the path absolute" but "does the demo
// make it": every live path in the example is under the directory
// demo.py stages, or is a file checked in beside it. A commented-out key is
// exempt and is where `/etc/saguin` and `/run/saguin` belong - that is the
// file's convention, live keys for the machine in front of you and
// commented ones for the deployment you are heading towards.
func TestEveryPathTheShippedExampleNamesIsOneTheDemoMakes(t *testing.T) {
	const example = "../../examples/saguin.yaml"
	raw, err := os.ReadFile(example)
	if err != nil {
		t.Fatalf("cannot read the example: %v", err)
	}
	// A path is a value ending in a key we know names one. Anything the
	// demo does not create is a path a reader cannot have.
	pathKey := regexp.MustCompile(`^\s*([a-z_]*(?:file|path|dir|file_path))\s*:\s*(\S+)`)
	live, checked := 0, 0
	for i, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		m := pathKey.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		checked++
		value := strings.Trim(m[2], `"'`)
		if !strings.HasPrefix(value, "/") {
			continue // relative values are refused by the loader already
		}
		live++
		if !strings.HasPrefix(value, "/tmp/saguin-demo") {
			t.Errorf("examples/saguin.yaml:%d: %s names %s, which nothing "+
				"creates - the demo stages /tmp/saguin-demo and only that, "+
				"so this starts a broker that refuses to run on the machine "+
				"of whoever reads it. A deployment path belongs in a "+
				"commented-out key", i+1, m[1], value)
		}
	}
	// A regexp that matched nothing passes every assertion above, which is
	// the failure this shape is prone to rather than a wrong answer.
	if checked < 8 || live < 5 {
		t.Fatalf("only %d path keys found and %d of them absolute; the "+
			"example names more than that, so this test read the wrong "+
			"file or the pattern stopped matching", checked, live)
	}
	t.Logf("%d path keys examined, %d of them absolute", checked, live)
}

// The connectors stack's configuration, tied the same way.
//
// **It is the one shipped configuration nothing but a Docker daemon
// loaded.** That is the shape an earlier test was written to end: a checked-in
// configuration the broker refuses, sitting in the repository because
// nothing in the suite ever asked it to load. A stack whose README opens
// with `docker compose up` fails at the daemon, minutes in, with a
// container that exits and a reader who does not yet know the difference
// between a bad configuration and a bad install.
//
// It checks the channels the dashboard's panels are drawn against, because
// a configuration that loads while holding none of them would satisfy a
// bare Load and leave every queue and bridge panel empty. The three
// measurement channels are the Bento pipelines' and the other two are
// traffic.py's, and the list is both because each half of the load reaches
// panels the other cannot.
func TestTheConnectorsExampleLoads(t *testing.T) {
	f, reg, err := config.Load(filepath.Join("..", "..", "examples", "bento-connectors", "saguin.yaml"))
	if err != nil {
		t.Fatalf("examples/bento-connectors/saguin.yaml does not load: %v", err)
	}
	for _, name := range []string{
		"weather-measurement", "water-measurement", "water-location",
		"jobs", "fleet",
	} {
		if _, ok := reg.All()[name]; !ok {
			t.Errorf("the connectors example no longer configures %q, which its dashboard "+
				"draws a panel for", name)
		}
	}
	// The stack exists to be scraped, so an operations listener is not
	// incidental to it.
	if f.Broker.Operations == nil || len(f.Broker.Operations.Listen.TCP) == 0 {
		t.Fatal("the connectors example no longer opens an operations listener, which is " +
			"the whole of what Prometheus scrapes")
	}
	// And a bridge, which is the only way five of the metrics on that
	// dashboard ever get a series.
	if len(f.BridgeSet()) == 0 {
		t.Error("the connectors example no longer configures a bridge, so every " +
			"saguin_bridge_* panel on its dashboard would be empty")
	}
}

// The configuration in README.md is presented as "Channels are
// configuration, not client-side objects:", so it is the first thing a
// reader copies - and until retention existed the broker refused it three
// ways. It loads now, and this is the tie that keeps it loading: an
// example a reader cannot run is worse than no example.
func TestTheREADMEConfigurationLoads(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	_, rest, ok := strings.Cut(string(md), "Channels are configuration, not client-side objects:")
	if !ok {
		t.Fatal("README.md no longer introduces a configuration example; move or delete this test")
	}
	_, rest, ok = strings.Cut(rest, "```yaml\n")
	if !ok {
		t.Fatal("no yaml block follows the configuration example's introduction")
	}
	body, _, ok := strings.Cut(rest, "```")
	if !ok {
		t.Fatal("the configuration example's yaml block is not closed")
	}
	// So this cannot pass by extracting nothing and loading it.
	for _, must := range []string{"channels:", "retention_period", "dlq_retention_period"} {
		if !strings.Contains(body, must) {
			t.Fatalf("the block extracted from README.md has no %q in it, so it is not the "+
				"configuration example:\n%s", must, body)
		}
	}

	if _, _, err := config.Load(write(t, body)); err != nil {
		t.Fatalf("the configuration README shows a reader does not load: %v", err)
	}
}

// The same rule for every complete configuration the documents show, rather
// than for the one file somebody remembered.
//
// **The test above guarded the README's block and nothing guarded RFC
// 0002's**, which is the document an operator reads to find out what a key
// is called - and its "every key" example had carried a key that no longer
// existed since the day `max_publish_rate` was renamed. `--check-config`
// answered `field max_publish_rate not found in type config.Limits`: the
// canonical example of the configuration schema, in the specification of
// the configuration schema, refused by the binary it specifies.
//
// Fixing that one line would have left the next one to be found later. A
// rule that can be stated about the source belongs in the source.
//
// **Complete means broker, id and channels**, which is what separates a
// configuration from an excerpt. The documents are full of excerpts showing
// one block - those are not meant to load and are not asked to. The count
// below is what stops the filter quietly matching nothing: a check whose
// subject is "all of them" has to prove it looked.
func TestEveryCompleteConfigurationInTheDocumentsLoads(t *testing.T) {
	docs := []string{
		filepath.Join("..", "..", "README.md"),
	}
	rfcs, err := filepath.Glob(filepath.Join("..", "..", "docs", "rfcs", "*.md"))
	if err != nil {
		t.Fatalf("glob the RFCs: %v", err)
	}
	if len(rfcs) == 0 {
		t.Fatal("no RFCs were found, so this scanned the README alone")
	}
	docs = append(docs, rfcs...)

	complete, refused := 0, 0
	for _, doc := range docs {
		md, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("read %s: %v", doc, err)
		}
		rest := string(md)
		for block := 1; ; block++ {
			var body string
			var ok bool
			_, rest, ok = strings.Cut(rest, "```yaml\n")
			if !ok {
				break
			}
			body, rest, ok = strings.Cut(rest, "```")
			if !ok {
				t.Fatalf("%s: a yaml block is never closed", doc)
			}
			// An excerpt shows one block and is not meant to load. A whole
			// configuration names the broker, gives it an id and places at
			// least one channel.
			if !strings.HasPrefix(body, "broker:\n") ||
				!strings.Contains(body, "\n  id:") ||
				!strings.Contains(body, "\nchannels:\n") {
				continue
			}
			// An !include names files beside the configuration, which a
			// document shows rather than ships. Loading it would be a test
			// of this test's temporary directory.
			if strings.Contains(body, "!include") {
				continue
			}
			complete++
			if _, _, err := config.Load(write(t, body)); err != nil {
				refused++
				t.Errorf("%s: the complete configuration in yaml block %d does not "+
					"load, so a reader copying it out of the documents cannot start "+
					"the broker: %v", doc, block, err)
			}
		}
	}

	// Two today: the README's and RFC 0002's. Named as a floor rather than
	// an equality so that adding one is not a failure, and as a number
	// rather than nothing so that a filter matching none of them is.
	if complete < 2 {
		t.Fatalf("only %d complete configurations were found across %d documents, so "+
			"this passed by extracting almost nothing", complete, len(docs))
	}
	// **What it counted, and what that count means, in the same breath.**
	// Written unconditionally, this line said "all of which load" directly
	// beneath the errors saying one of them does not - a test's own report
	// contradicting its result, which is worth more than nothing only if it
	// is true. The count is the evidence that the filter matched something,
	// so it is still printed when the run fails; what changes is the claim.
	if refused == 0 {
		t.Logf("%d complete configurations across %d documents, all of which load",
			complete, len(docs))
	} else {
		t.Logf("%d complete configurations across %d documents, of which %d failed to load",
			complete, len(docs), refused)
	}
}

// RFC 0002 "Validation"
//
// A memory provider states either a snapshot_dir or snapshot_dir: none.
// Neither default is right: a default path invents a filename in the
// operator's filesystem, and defaulting to none makes "durable channel,
// memory provider" quietly mean "discarded at shutdown" - the durability
// surprise invariant 14 exists to prevent.
func TestMemoryProviderStatesWhetherItKeepsAnything(t *testing.T) {
	provider := func(body string) string {
		return "broker:\n  id: t\n  storage:\n    default: local\n    default_retention_period: none\n    default_retention_bytes: none\n    providers:\n      local:\n" + body + oneChannel
	}
	for name, tc := range map[string]struct {
		body    string
		wantErr string
	}{
		"a directory":         {"        type: memory\n        snapshot_dir: /var/lib/saguin\n", ""},
		"explicitly none":     {"        type: memory\n        snapshot_dir: none\n", ""},
		"silence is refused":  {"        type: memory\n", "snapshot_dir"},
		"relative directory":  {"        type: memory\n        snapshot_dir: ./snaps\n", "relative"},
		"sqlite":              {"        type: sqlite\n        file_path: /var/lib/saguin.db\n", ""},
		"sqlite with no path": {"        type: sqlite\n", "file_path"},
		"sqlite relative":     {"        type: sqlite\n        file_path: ./saguin.db\n", "relative"},
		// The two keys name different kinds of thing, and each belongs to
		// exactly one provider. Accepting the wrong one would leave an
		// operator believing they had configured something.
		"sqlite with a snapshot dir": {"        type: sqlite\n        file_path: /var/lib/s.db\n        snapshot_dir: /var/lib/snaps\n", "belongs to a memory provider"},
		"memory with a file path":    {"        type: memory\n        snapshot_dir: none\n        file_path: /var/lib/s.db\n", "belongs to a sqlite provider"},
		"max_bytes is a size":        {"        type: memory\n        snapshot_dir: none\n        max_bytes: soon\n", "not a byte count"},
		// A provider holds back one largest-possible record so that the
		// operations which free it are never refused. One that cannot also
		// hold a record beyond that is not a provider, and 8MiB against a
		// 1MiB max_message_size is comfortably clear of it.
		"max_bytes under the reserve": {"        type: memory\n        snapshot_dir: none\n        max_bytes: 1MiB\n", "for the operations that free it"},
		"max_bytes clear of it":       {"        type: memory\n        snapshot_dir: none\n        max_bytes: 8MiB\n", ""},
		"no type":                     {"        snapshot_dir: none\n", "no type"},
		"unknown type":                {"        type: postgres\n", "unknown type"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, provider(tc.body)))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("load: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("accepted")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error does not mention %q: %v", tc.wantErr, err)
			}
		})
	}
}

// Every channel's storage names a defined provider, and a channel that
// names none gets the default.
func TestChannelStorageNamesAProvider(t *testing.T) {
	const providers = "broker:\n  id: t\n  storage:\n    default: local\n    default_retention_period: none\n    default_retention_bytes: none\n    providers:\n" +
		"      local:\n        type: memory\n        snapshot_dir: /var/lib/saguin\n" +
		"      volatile:\n        type: memory\n        snapshot_dir: none\n"

	f, reg, err := config.Load(write(t, providers+
		"\nchannels:\n  events:\n    type: append\n  scratch:\n    type: latest\n    storage: volatile\n"+
		"  jobs:\n    type: queue\n    storage: volatile\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := reg.Get("events").Storage; got != "local" {
		t.Errorf("a channel naming no storage got %q, want the default", got)
	}
	if got := reg.Get("scratch").Storage; got != "volatile" {
		t.Errorf("scratch got %q, want volatile", got)
	}
	// A derived dead-letter channel takes its queue's provider. Nobody can
	// write it in the file - the suffix is refused as a configured name -
	// so if it is not taken here it is not set anywhere, and the move out
	// of the queue and into it stops being one operation.
	if got := reg.Get("jobs__dlq").Storage; got != "volatile" {
		t.Errorf("jobs__dlq got storage %q, want its queue's %q", got, "volatile")
	}
	// Only the provider that keeps something has a directory.
	dirs := f.Broker.Storage.SnapshotDirs()
	if len(dirs) != 1 || dirs["local"] != "/var/lib/saguin" {
		t.Errorf("snapshot directories = %v, want local only", dirs)
	}

	_, _, err = config.Load(write(t, providers+
		"\nchannels:\n  events:\n    type: append\n    storage: nowhere\n"))
	if err == nil || !strings.Contains(err.Error(), "nowhere") {
		t.Fatalf("a channel naming an undefined provider was accepted: %v", err)
	}
}

// A configuration with no storage block is today's broker: nothing
// survives a restart. Naming a provider that does not exist is still an
// error, so "storage: local" cannot be quietly ignored.
func TestAProviderAndADefaultAreMandatory(t *testing.T) {
	const retention = "    default_retention_period: none\n    default_retention_bytes: none\n"
	const mem = "    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"
	for _, tc := range []struct{ name, broker, want string }{
		{"no storage block", "", "define at least one provider"},
		{"a default and no providers", "  storage:\n    default: mem\n" + retention,
			"define at least one provider"},
		{"no providers, and a channel naming one", "", "define at least one provider"},
		{"providers and no default", "  storage:\n" + retention + mem, "broker.storage.default: name"},
		{"a default naming no provider", "  storage:\n    default: disk\n" + retention + mem,
			`broker.storage.default names "disk", which is not a defined provider`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channels := oneChannel
			if strings.Contains(tc.name, "naming one") {
				channels += "    storage: local\n"
			}
			_, _, err := config.Load(write(t, "broker:\n  id: t\n"+tc.broker+channels))
			if err == nil {
				t.Fatalf("loaded, want a refusal mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused with %q, want it to mention %q", err, tc.want)
			}
			// One finding for one mistake: a channel repeating that there are
			// no providers would bury the line that says what to write.
			if n := strings.Count(err.Error(), "\n  - "); tc.want == "define at least one provider" && n != 1 {
				t.Errorf("%d findings, want 1: %v", n, err)
			}
		})
	}

	f, reg, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+oneChannel))
	if err != nil {
		t.Fatalf("the smallest valid storage block does not load: %v", err)
	}
	if got := reg.Get("events").Storage; got != "mem" {
		t.Errorf("channel storage = %q, want the default, mem", got)
	}
	if len(f.Broker.Storage.SnapshotDirs()) != 0 {
		t.Error("snapshot_dir: none produced a snapshot directory")
	}
}

// A channel name is not a file name. This must be caught at startup,
// where it costs a restart, and not at the shutdown that would discover
// it by losing the channel.
func TestChannelNameMustFitASnapshotFile(t *testing.T) {
	// 128 bytes, the channel limit, but 263 once the dots are encoded. A
	// dot is the only character a name may hold that the file name encodes,
	// which is what keeps this rule reachable at all.
	long := strings.Repeat("a.", 63) + "bb"
	_, _, err := config.Load(write(t,
		"broker:\n  id: t\n  storage:\n    default: local\n    default_retention_period: none\n    default_retention_bytes: none\n    providers:\n      local:\n"+
			"        type: memory\n        snapshot_dir: /var/lib/saguin\n"+
			"\nchannels:\n  "+long+":\n    type: append\n"))
	if err == nil || !strings.Contains(err.Error(), "file name") {
		t.Fatalf("a channel name too long to store was accepted: %v", err)
	}

	// The same name is fine where nothing is written.
	if _, _, err := config.Load(write(t,
		"broker:\n  id: t\n  storage:\n    default: local\n    default_retention_period: none\n    default_retention_bytes: none\n    providers:\n      local:\n"+
			"        type: memory\n        snapshot_dir: none\n"+
			"\nchannels:\n  "+long+":\n    type: append\n")); err != nil {
		t.Fatalf("a provider that stores nothing refused a long channel name: %v", err)
	}
}

// A channel name holds letters, digits, and `-`, `_`, `.` - the set a Kafka
// topic holds (RFC 0002 "Channel names").
//
// It is tested through the loader rather than against the rule itself,
// because the loader is where an operator meets it: a name is a key in
// their file, and the answer they get is a startup error naming it.
//
// The refusals are not interchangeable, which is why they are enumerated.
// A `/` used to be legal and made a name hierarchical, and every rule that
// followed from a name had to cope: `$saguin/consumer/site/events/seek` was
// unrecognisable as a seek, so it answered PUBACK Success, sent no reply,
// and logged a complaint about a payload that was neither ack nor return.
// `+`, `#` and a leading `$` are MQTT's own reserved characters. `.` and
// `..` are inside the character set and refused anyway, because both name a
// directory rather than a channel in every listing that will ever hold one.
func TestChannelNameCharacters(t *testing.T) {
	load := func(name string) error {
		_, _, err := config.Load(write(t,
			"broker:\n  id: t\n"+memStorage+
				"\nchannels:\n  ? "+name+"\n  :\n    type: append\n"))
		return err
	}

	for _, name := range []string{"events", "device-state", "device_state", "a.b", "A1", "jobs2"} {
		if err := load(name); err != nil {
			t.Errorf("channel %q was refused: %v", name, err)
		}
	}

	for name, want := range map[string]string{
		"site/events": "holds only letters",
		"a+b":         "holds only letters",
		"a#b":         "holds only letters",
		"$internal":   "holds only letters",
		"a b":         "holds only letters",
		".":           "names a directory",
		"..":          "names a directory",
		"":            "empty",
		"jobs__dlq":   "reserved",
	} {
		err := load("\"" + name + "\"")
		if err == nil {
			t.Errorf("channel %q was accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("channel %q was refused with %v, want a reason saying %q", name, err, want)
		}
	}
}

// RFC 0002 uses `age: 7d` and `dlq_retention: 30d`, which Go's own
// time.ParseDuration cannot read - it has no day unit. It also accepts
// units below a second, and every duration in this file is held in whole
// seconds, so `500ms` would parse and then quietly mean one second.
func TestDurations(t *testing.T) {
	queueWith := func(body string) string {
		return "broker:\n  id: t\n" + memStorage + "channels:\n  jobs:\n    type: queue\n" + body
	}
	for _, tc := range []struct {
		in   string
		want int64 // seconds, or 0 when it must be refused
	}{
		{"30s", 30},
		{"90s", 90},
		{"5m", 300},
		{"2h", 7200},
		{"7d", 7 * 24 * 60 * 60},
		{"30d", 30 * 24 * 60 * 60},
		{" 6h ", 6 * 60 * 60},

		{"500ms", 0}, // held in whole seconds; rounding to 1s would be a lie
		{"1h30m", 0}, // one number, one unit - write 90m
		{"0s", 0},
		{"-5m", 0},
		{"soon", 0},
		{"6", 0},
		{"h", 0},
		{"6w", 0},
		{"9999999999999d", 0}, // longer than a Duration can hold
	} {
		t.Run(tc.in, func(t *testing.T) {
			_, reg, err := config.Load(write(t, queueWith(
				"    visibility_timeout: "+tc.in+"\n")))
			if tc.want == 0 {
				if err == nil {
					t.Fatalf("%q was accepted", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("%q: %v", tc.in, err)
			}
			if got := reg.Get("jobs").VisibilityTimeout; got != tc.want {
				t.Fatalf("%q became %d seconds, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// RFC 0002 "Validation", invariant 14
//
// Every channel type may name a sqlite provider. A queue was refused there
// until one was built, because half a provider is the kind of thing that
// looks like it works: the jobs would be taken, worked and acknowledged,
// and the first unclean stop would lose every one still outstanding.
//
// It is built, so the refusal is gone - and this is what says so, since
// nothing else in the configuration distinguishes a queue from any other
// channel on a database.
func TestEveryChannelTypeMayBeKeptInSQLite(t *testing.T) {
	const providers = "broker:\n  id: t\n  storage:\n    default: local\n" +
		"    default_retention_period: none\n    default_retention_bytes: none\n" +
		"    providers:\n      local:\n        type: sqlite\n        file_path: /var/lib/saguin.db\n"

	for name, body := range map[string]string{
		"append": "\nchannels:\n  events:\n    type: append\n",
		"latest": "\nchannels:\n  state:\n    type: latest\n",
		"queue": "\nchannels:\n  jobs:\n    type: queue\n" +
			"    visibility_timeout: 30s\n    retry:\n      max_attempts: 5\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, reg, err := config.Load(write(t, providers+body))
			if err != nil {
				t.Fatalf("a %s channel on a sqlite provider was refused: %v", name, err)
			}
			for _, c := range reg.All() {
				if c.Storage != "local" {
					t.Fatalf("channel %q was assigned storage %q, want local", c.Name, c.Storage)
				}
			}
		})
	}
}

// RFC 0002 "Bounds on what a channel holds"
func TestMaxBytesOnAChannel(t *testing.T) {
	for _, c := range []struct {
		name, yaml, want string
		bytes            int64
	}{
		{name: "append takes it", yaml: "  events:\n    type: append\n    max_bytes: 2MiB\n", bytes: 2 << 20},
		{name: "queue takes it", yaml: "  jobs:\n    type: queue\n    max_bytes: 512\n", bytes: 512},
		{
			name: "latest does not",
			yaml: "  state:\n    type: latest\n    max_bytes: 1MiB\n",
			want: "does not apply to a latest channel",
		},
		{
			name: "and it is a size",
			yaml: "  events:\n    type: append\n    max_bytes: soon\n",
			want: "not a byte count",
		},
		{
			name: "and it is not zero",
			yaml: "  events:\n    type: append\n    max_bytes: 0\n",
			want: "greater than zero",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, reg, err := config.Load(write(t, "broker:\n  id: b\n"+memStorage+"channels:\n"+c.yaml))
			if c.want != "" {
				if err == nil {
					t.Fatalf("accepted, want a finding mentioning %q", c.want)
				}
				if !strings.Contains(err.Error(), c.want) {
					t.Errorf("the finding does not say why: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			for name, ch := range reg.All() {
				if strings.HasSuffix(name, "__dlq") {
					// A dead-letter channel takes no bound of its own. One
					// there would refuse the move that relieves the queue.
					if ch.MaxBytes != 0 {
						t.Errorf("the dead-letter channel inherited max_bytes %d", ch.MaxBytes)
					}
					continue
				}
				if ch.MaxBytes != c.bytes {
					t.Errorf("channel %q has max_bytes %d, want %d", name, ch.MaxBytes, c.bytes)
				}
			}
		})
	}
}

// RFC 0002 "Bounds on what a channel holds" and "Retention is stated once".
//
// The four retention keys, the two broker-wide defaults they fall back to,
// and the three shapes that are refused. Each refusal is a key that would
// configure something which cannot happen, so accepting it would leave an
// operator believing they had set a policy.
func TestRetentionKeys(t *testing.T) {
	// Retention is only reachable where something is stored, so every case
	// here defines a provider - which is also what makes the two defaults
	// required.
	withDefaults := func(period, bytes, channels string) string {
		return "broker:\n  id: b\n  storage:\n    default: p\n" +
			"    default_retention_period: " + period + "\n" +
			"    default_retention_bytes: " + bytes + "\n" +
			"    providers:\n      p:\n        type: memory\n        snapshot_dir: none\n" +
			"\nchannels:\n" + channels
	}

	t.Run("a channel takes its own, and falls back to the default", func(t *testing.T) {
		_, reg, err := config.Load(write(t, withDefaults("3d", "10MiB",
			"  events:\n    type: append\n    retention_period: 7d\n    retention_bytes: 20MiB\n"+
				"  quiet:\n    type: append\n")))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if c := reg.Get("events"); c.RetentionPeriod != 7*24*60*60 || c.RetentionBytes != 20<<20 {
			t.Errorf("events has period %d bytes %d", c.RetentionPeriod, c.RetentionBytes)
		}
		if c := reg.Get("quiet"); c.RetentionPeriod != 3*24*60*60 || c.RetentionBytes != 10<<20 {
			t.Errorf("a channel that said nothing has period %d bytes %d, want the defaults",
				c.RetentionPeriod, c.RetentionBytes)
		}
	})

	// `none` is a value, not an omission: a channel that keeps everything
	// says so, and says it against a default that does not.
	t.Run("none overrides a default that is set", func(t *testing.T) {
		_, reg, err := config.Load(write(t, withDefaults("3d", "10MiB",
			"  audit:\n    type: append\n    retention_period: none\n    retention_bytes: none\n")))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if c := reg.Get("audit"); c.RetentionPeriod != 0 || c.RetentionBytes != 0 {
			t.Errorf("a channel that keeps everything has period %d bytes %d",
				c.RetentionPeriod, c.RetentionBytes)
		}
	})

	// The two dlq keys are the only way to reach a derived channel, because
	// its name is not the operator's to write.
	t.Run("a queue configures its dead-letter channel", func(t *testing.T) {
		_, reg, err := config.Load(write(t, withDefaults("3d", "10MiB",
			"  jobs:\n    type: queue\n    dlq_retention_period: 30d\n    dlq_retention_bytes: 1GiB\n")))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		dlq := reg.Get("jobs__dlq")
		if dlq == nil {
			t.Fatal("no dead-letter channel was derived")
		}
		if dlq.RetentionPeriod != 30*24*60*60 || dlq.RetentionBytes != 1<<30 {
			t.Errorf("the dead-letter channel has period %d bytes %d",
				dlq.RetentionPeriod, dlq.RetentionBytes)
		}
		// The queue itself removes records by resolution and by nothing else.
		if q := reg.Get("jobs"); q.RetentionPeriod != 0 || q.RetentionBytes != 0 {
			t.Errorf("the queue itself took retention: period %d bytes %d",
				q.RetentionPeriod, q.RetentionBytes)
		}
	})

	t.Run("a dead-letter channel falls back to the defaults too", func(t *testing.T) {
		_, reg, err := config.Load(write(t, withDefaults("3d", "10MiB",
			"  jobs:\n    type: queue\n")))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if dlq := reg.Get("jobs__dlq"); dlq.RetentionPeriod != 3*24*60*60 || dlq.RetentionBytes != 10<<20 {
			t.Errorf("the dead-letter channel has period %d bytes %d, want the defaults",
				dlq.RetentionPeriod, dlq.RetentionBytes)
		}
	})

	// A latest channel takes the period and not the size: it holds one value
	// per topic rather than a history, so what grows is the topic count.
	t.Run("latest takes the period and not the size", func(t *testing.T) {
		_, reg, err := config.Load(write(t, withDefaults("3d", "10MiB",
			"  state:\n    type: latest\n    retention_period: 30d\n")))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		c := reg.Get("state")
		if c.RetentionPeriod != 30*24*60*60 {
			t.Errorf("state has period %d", c.RetentionPeriod)
		}
		if c.RetentionBytes != 0 {
			t.Errorf("a latest channel took retention_bytes %d from the default", c.RetentionBytes)
		}
	})

	for name, tc := range map[string]struct{ channels, want string }{
		"retention_bytes on latest": {
			"  state:\n    type: latest\n    retention_bytes: 1MiB\n",
			"does not apply to a latest channel",
		},
		"retention on a queue": {
			"  jobs:\n    type: queue\n    retention_period: 7d\n",
			"do not apply to a queue",
		},
		"retention_bytes on a queue": {
			"  jobs:\n    type: queue\n    retention_bytes: 1MiB\n",
			"do not apply to a queue",
		},
		"dlq keys on an append channel": {
			"  events:\n    type: append\n    dlq_retention_period: 7d\n",
			"apply only to a queue",
		},
		"dlq keys on a latest channel": {
			"  state:\n    type: latest\n    dlq_retention_bytes: 1MiB\n",
			"apply only to a queue",
		},
		// Zero is refused rather than read as none: an operator who meant
		// `none` and typed 0 would get a channel that discards every record
		// it is given.
		"a period of zero": {
			"  events:\n    type: append\n    retention_period: 0s\n",
			"greater than zero",
		},
		"a size of zero": {
			"  events:\n    type: append\n    retention_bytes: 0\n",
			"greater than zero",
		},
		"a period that is not a duration": {
			"  events:\n    type: append\n    retention_period: soon\n",
			"not a duration",
		},
		"a size that is not a size": {
			"  events:\n    type: append\n    retention_bytes: lots\n",
			"not a byte count",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, withDefaults("3d", "10MiB", tc.channels)))
			if err == nil {
				t.Fatalf("accepted, want a finding mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}

// The two defaults are required, exactly as broker.storage.default is.
// Silence would have to mean one of the two built-in figures RFC 0002
// refuses, so silence is the error.
func TestTheRetentionDefaultsAreRequired(t *testing.T) {
	provider := "broker:\n  id: b\n  storage:\n    default: p\n%s" +
		"    providers:\n      p:\n        type: memory\n        snapshot_dir: none\n" +
		"\nchannels:\n  events:\n    type: append\n"

	for name, tc := range map[string]struct{ block, want string }{
		"neither stated": {"", "default_retention_period"},
		"only the period": {
			"    default_retention_period: 3d\n", "default_retention_bytes",
		},
		"only the size": {
			"    default_retention_bytes: 1GiB\n", "default_retention_period",
		},
		"a period that is not one": {
			"    default_retention_period: soon\n    default_retention_bytes: none\n",
			"not a duration",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, fmt.Sprintf(provider, tc.block)))
			if err == nil {
				t.Fatalf("accepted, want a finding mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}

// RFC 0002: a provider's max_bytes bounds every channel it holds, on
// either kind of provider. The two enforce it differently - SQLite bounds
// its own file, a memory provider counts - but an operator writes one key.
func TestMaxBytesOnAProvider(t *testing.T) {
	body := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n    default_retention_bytes: none\n    providers:\n      p:\n%s" +
		"\nchannels:\n  events:\n    type: append\n"

	for name, tc := range map[string]struct {
		provider string
		want     int64
	}{
		"memory": {"        type: memory\n        snapshot_dir: none\n        max_bytes: 64MiB\n", 64 << 20},
		"sqlite": {"        type: sqlite\n        file_path: /var/lib/s.db\n        max_bytes: 1GiB\n", 1 << 30},
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := config.Load(write(t, fmt.Sprintf(body, tc.provider)))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if got := f.Broker.Storage.MaxBytes()["p"]; got != tc.want {
				t.Errorf("max_bytes is %d, want %d", got, tc.want)
			}
		})
	}

	// Unset means no bound, and the map says so by leaving it out rather
	// than by reporting zero, which a caller could mistake for a ceiling.
	f, _, err := config.Load(write(t, fmt.Sprintf(body,
		"        type: memory\n        snapshot_dir: none\n")))
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if _, bounded := f.Broker.Storage.MaxBytes()["p"]; bounded {
		t.Error("a provider with no max_bytes reports one")
	}
}

// tree writes a master configuration and the files it includes into one
// directory, and returns the path of the master. The leaves are keyed by
// the path the master names them by, so a test writes exactly what it
// means and the resolution rule is exercised rather than described.
func tree(t *testing.T, master string, leaves map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range leaves {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	path := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(path, []byte(master), 0o600); err != nil {
		t.Fatalf("write master: %v", err)
	}
	return path
}

// RFC 0002 "Configuration"
//
// One file stops being the right shape below the number of channels a
// fleet reaches, so a master file names files that hold the rest - and
// still writes channels of its own, because an operator with one local
// channel should not have to make a file for it.
func TestChannelsComeFromFilesAndFromTheMasterTogether(t *testing.T) {
	f, reg, err := config.Load(tree(t, `
broker:
  id: t
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
channels:
  - !include channels/telemetry.yaml
  - !include fleet/vessel-07/jobs.yaml
  - events:
      type: append
    state:
      type: latest
`, map[string]string{
		"channels/telemetry.yaml":   "readings:\n  type: append\n",
		"fleet/vessel-07/jobs.yaml": "work:\n  type: queue\n",
	}))
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if reg == nil {
		t.Fatal("no registry")
	}
	want := map[string]string{
		"readings": "append", // from the first file
		"work":     "queue",  // from the second, two directories down
		"events":   "append", // written in the master alongside them
		"state":    "latest",
	}
	if len(f.Channels) != len(want) {
		t.Fatalf("got %d channels, want %d: %v", len(f.Channels), len(want), f.Channels)
	}
	for name, typ := range want {
		if f.Channels[name].Type != typ {
			t.Errorf("channel %q: type %q, want %q", name, f.Channels[name].Type, typ)
		}
	}
}

// A path is resolved against the file that wrote it, never against the
// directory the process happens to be started from - otherwise `make demo`
// and a systemd unit disagree about what the same configuration means.
func TestAnIncludedPathIsRelativeToTheFileThatNamesIt(t *testing.T) {
	// "channels/telemetry.yaml" does not exist relative to the test's own
	// working directory, so this loads only if it resolved against the
	// master file's directory.
	if _, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include channels/telemetry.yaml\n",
		map[string]string{"channels/telemetry.yaml": "readings:\n  type: append\n"})); err != nil {
		t.Fatalf("refused: %v", err)
	}
}

// Nesting buys an operator very little and costs cycle detection, a depth
// bound, and an error message that has to explain a chain. It is refused
// from both sides: inside a file, and inside a group in the master.
func TestAnIncludeCannotBeNested(t *testing.T) {
	t.Run("inside an included file", func(t *testing.T) {
		_, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include a.yaml\n",
			map[string]string{
				"a.yaml": "readings:\n  type: append\nmore: !include b.yaml\n",
				"b.yaml": "work:\n  type: queue\n",
			}))
		if err == nil {
			t.Fatal("a file included another and was accepted")
		}
		// Not merely "it failed": without the check the decoder refuses the
		// tag on its own, with a message about types that names the file
		// too. The rule is what has to be reported.
		if !strings.Contains(err.Error(), "cannot include another") {
			t.Errorf("refused, but not as nesting: %v", err)
		}
		if !strings.Contains(err.Error(), "a.yaml") {
			t.Errorf("the message does not name the file that did it: %v", err)
		}
	})

	t.Run("inside a group in the master", func(t *testing.T) {
		_, _, err := config.Load(tree(t,
			"broker:\n  id: t\n"+memStorage+"channels:\n  - events:\n      type: append\n    more: !include a.yaml\n",
			map[string]string{"a.yaml": "work:\n  type: queue\n"}))
		if err == nil {
			t.Fatal("an include inside a group was accepted")
		}
		if !strings.Contains(err.Error(), "cannot sit inside a group") {
			t.Errorf("refused, but not as nesting: %v", err)
		}
	})
}

// Two domains each defining `events` is the hazard this whole shape
// creates, and one silently winning is the worst outcome available. The
// message names both files, because being told there is a duplicate
// without being told where the other one is helps nobody.
func TestADuplicateChannelNameNamesBothFiles(t *testing.T) {
	t.Run("across two included files", func(t *testing.T) {
		_, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include a.yaml\n  - !include b.yaml\n",
			map[string]string{
				"a.yaml": "events:\n  type: append\n",
				"b.yaml": "events:\n  type: latest\n",
			}))
		if err == nil {
			t.Fatal("the same channel was defined in two files and one of them won")
		}
		for _, want := range []string{"events", "a.yaml", "b.yaml"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message does not mention %q: %v", want, err)
			}
		}
	})

	t.Run("between an included file and the master", func(t *testing.T) {
		_, _, err := config.Load(tree(t,
			"broker:\n  id: t\n"+memStorage+"channels:\n  - !include a.yaml\n  - events:\n      type: latest\n",
			map[string]string{"a.yaml": "events:\n  type: append\n"}))
		if err == nil {
			t.Fatal("a file and the master both defined a channel and one of them won")
		}
		for _, want := range []string{"events", "a.yaml", "saguin.yaml"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message does not mention %q: %v", want, err)
			}
		}
	})
}

// A wildcard makes what the broker serves depend on a directory listing,
// and an editor's backup file becomes configuration. Explicit names cost
// one line each and can be reviewed.
func TestAnIncludeTakesNoWildcard(t *testing.T) {
	_, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include channels/*.yaml\n",
		map[string]string{"channels/telemetry.yaml": "readings:\n  type: append\n"}))
	if err == nil {
		t.Fatal("a wildcard was accepted")
	}
	if !strings.Contains(err.Error(), "wildcard") {
		t.Errorf("the message does not say why: %v", err)
	}
}

// Losing the position of a mistake is most of what would make a split
// configuration worse than a single one, so every message names the file
// the mistake is in and a line inside it.
func TestAnUnknownKeyIsRefusedWhereverItIsWritten(t *testing.T) {
	t.Run("inside an included file", func(t *testing.T) {
		_, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include channels/a.yaml\n",
			map[string]string{"channels/a.yaml": "readings:\n  type: append\n  storag: durable\n"}))
		if err == nil {
			t.Fatal("a typo in an included file was accepted")
		}
		// The included file is parsed on its own, so the position comes
		// straight from the parser: line 3 of that file, not of the master.
		for _, want := range []string{"a.yaml", "storag", "line 3"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message does not mention %q: %v", want, err)
			}
		}
	})

	t.Run("inside a group in the master", func(t *testing.T) {
		// 1 blank, 2 broker, 3 id, 4 channels, 5 the include, 6 `- events:`,
		// 7 type, 8 the typo.
		_, _, err := config.Load(tree(t, `
broker:
  id: t
channels:
  - !include a.yaml
  - events:
      type: append
      storag: durable
`, map[string]string{"a.yaml": "readings:\n  type: append\n"}))
		if err == nil {
			t.Fatal("a typo in the master was accepted")
		}
		for _, want := range []string{"saguin.yaml", "storag", "line 8"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message does not mention %q: %v", want, err)
			}
		}
	})
}

// The file that names no files at all is the one everybody already has,
// and it keeps the positions the parser reported for it.
func TestTheSingleMappingFormIsUnchanged(t *testing.T) {
	f, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+"channels:\n  events:\n    type: append\n"))
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if f.Channels["events"].Type != "append" {
		t.Errorf("channel: %v", f.Channels)
	}

	_, _, err = config.Load(write(t, "broker:\n  id: t\n"+memStorage+"channels:\n  events:\n    type: append\n    storag: durable\n"))
	if err == nil {
		t.Fatal("a typo was accepted")
	}
	// Line 14: broker, id, eight lines of storage, channels, events, type, the typo.
	if !strings.Contains(err.Error(), "line 14") {
		t.Errorf("the position moved: %v", err)
	}
}

// A file that is named and is not there is an ordinary mistake, and the
// message has to say which line named it.
func TestAnIncludedFileThatIsMissingSaysWhereItWasNamed(t *testing.T) {
	_, _, err := config.Load(tree(t, "broker:\n  id: t\n"+memStorage+"channels:\n  - !include channels/gone.yaml\n", nil))
	if err == nil {
		t.Fatal("a missing file was accepted")
	}
	// Line 12: broker, id, eight lines of storage, channels, the include.
	for _, want := range []string{"saguin.yaml:12", "gone.yaml"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message does not mention %q: %v", want, err)
		}
	}
}

// RFC 0005 "The operations listener": absent means no listener is opened,
// and a block naming no address is a startup error rather than a listener
// nothing in it is a mistake worth naming, because it reads as though the
// port was configured.
func TestOperationsListener(t *testing.T) {
	base := "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      tcp:\n        address: :1883\n"

	t.Run("absent means no listener", func(t *testing.T) {
		f, _, err := config.Load(write(t, base+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if f.Broker.Operations != nil {
			t.Fatalf("a configuration with no operations block produced %+v", f.Broker.Operations)
		}
	})

	t.Run("an address is carried through", func(t *testing.T) {
		f, _, err := config.Load(write(t,
			base+"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if f.Broker.Operations == nil {
			t.Fatal("the operations block was dropped")
		}
		if len(f.Broker.Operations.Listen.TCP) == 0 {
			t.Fatal("the tcp listener was dropped")
		}
		if got := f.Broker.Operations.Listen.TCP[0].Address.Address; got != "127.0.0.1:9090" {
			t.Fatalf("address %q", got)
		}
		// The default an unwritten key gets, which is the floor: there is no
		// interval between "the shortest allowed" and "what saying nothing
		// gets you" that would mean anything.
		if got := f.Broker.Operations.ScrapeInterval(); got != 60*time.Second {
			t.Fatalf("min_scrape_interval defaulted to %v, want 60s", got)
		}
	})

	// **A minute is the floor, and a shorter one is refused rather than
	// raised.** An operator given 60 where they wrote 10 reads their graphs
	// believing they have ten-second resolution, and every conclusion about
	// the shape of a spike is drawn at the wrong scale.
	t.Run("a relative pid_file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+"  pid_file: saguin.pid\n"+
			"  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:1883\n"+oneChannel))
		if err == nil {
			t.Fatal("a relative pid_file was accepted: two copies started from two " +
				"directories would each write a different file and each believe it " +
				"held the only one")
		}
		if !strings.Contains(err.Error(), "relative") {
			t.Errorf("the refusal does not say what is wrong: %v", err)
		}
	})

	t.Run("below the floor is refused, naming it", func(t *testing.T) {
		_, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"    min_scrape_interval: 10s\n"+oneChannel))
		if err == nil {
			t.Fatal("a scrape interval below the floor was accepted, so the cost of " +
				"being observed is set by whoever configures the scraper")
		}
		for _, want := range []string{"below the", "1m0s", "floor"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not say %q, so an operator cannot see what to "+
					"write instead: %v", want, err)
			}
		}
	})

	// The floor exactly is fine: it is the default, so refusing it would
	// refuse a file that says out loud what saying nothing already means.
	t.Run("the floor itself is accepted", func(t *testing.T) {
		if _, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"    min_scrape_interval: 60s\n"+oneChannel)); err != nil {
			t.Fatalf("the floor itself was refused: %v", err)
		}
	})

	t.Run("min_scrape_interval is carried through", func(t *testing.T) {
		f, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"    min_scrape_interval: 90s\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if got := f.Broker.Operations.ScrapeInterval(); got != 90*time.Second {
			t.Fatalf("min_scrape_interval is %v, want 90s", got)
		}
	})

	// A block that opens nothing is a configuration asking for /metrics and
	// then not serving it anywhere. Absent is how an operator says no
	// listener; present and empty is how they say it by accident.
	t.Run("a block that opens nothing is refused", func(t *testing.T) {
		for _, what := range []string{
			"  operations:\n    listen:\n      tcp:\n        address: \"\"\n",
			"  operations:\n    listen: {}\n",
			"  operations: {}\n",
		} {
			_, _, err := config.Load(write(t, base+what+oneChannel))
			if err == nil {
				t.Errorf("%q was accepted, so the listener opens nothing and nothing says so", what)
				continue
			}
			if !strings.Contains(err.Error(), "broker.operations") {
				t.Errorf("%q: the error does not name the key: %v", what, err)
			}
		}
	})

	// The socket is the other half of the loopback rule: a local reader
	// needs no port at all, and the file's permissions are the access
	// control. Same two keys as the MQTT socket, same default mode.
	t.Run("a unix socket is carried through, and defaults its mode", func(t *testing.T) {
		f, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      unix:\n        path: /run/saguin/ops.sock\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(f.Broker.Operations.Listen.Unix) == 0 {
			t.Fatal("the unix listener was dropped")
		}
		u := f.Broker.Operations.Listen.Unix[0]
		if u.Path != "/run/saguin/ops.sock" {
			t.Fatalf("path %q", u.Path)
		}
		if got := u.FileMode(); got != 0o660 {
			t.Fatalf("mode defaulted to %v, want 0660 - owner and group, nobody else", got)
		}
	})

	t.Run("a socket with no path is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      unix:\n        mode: \"0660\"\n"+oneChannel))
		if err == nil {
			t.Fatal("a unix listener with no path was accepted")
		}
		if !strings.Contains(err.Error(), "broker.operations.listen.unix") {
			t.Fatalf("the error does not name the key: %v", err)
		}
	})

	// Both, which is the deployment this item exists for: a socket for the
	// local agent and a port for a human with curl.
	t.Run("both listeners together are accepted", func(t *testing.T) {
		f, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"      unix:\n        path: /run/saguin/ops.sock\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(f.Broker.Operations.Listen.TCP) == 0 || len(f.Broker.Operations.Listen.Unix) == 0 {
			t.Fatal("one of the two listeners was dropped")
		}
	})

	// **The old name is a startup error naming itself.** Unknown keys are
	// refused, so a configuration written against `health:` stops the
	// broker rather than leaving it quietly serving nothing - which is the
	// whole argument for renaming the block instead of adding beside it.
	t.Run("the old name is refused by name", func(t *testing.T) {
		_, _, err := config.Load(write(t,
			base+"  health:\n    address: 127.0.0.1:9090\n"+oneChannel))
		if err == nil {
			t.Fatal("broker.health was accepted, so an operator upgrading gets no listener and no error")
		}
		if !strings.Contains(err.Error(), "health") {
			t.Fatalf("the error does not name the key that is no longer read: %v", err)
		}
	})

	// RFC 0005 "Authentication and TLS": the rule that stops
	// "authentication later" being for ever. There is nothing to configure
	// yet, so today this is the operations listener being loopback only -
	// which is the intended state, not an oversight.
	t.Run("a non-loopback address is refused while there is no authentication", func(t *testing.T) {
		// Quoted in the YAML: a bare [::]:9090 is a flow sequence and the
		// parser refuses it before saguin sees it at all.
		for _, addr := range []string{"0.0.0.0:9090", "\":9090\"", "192.0.2.10:9090", "\"[::]:9090\""} {
			_, _, err := config.Load(write(t,
				base+"  operations:\n    listen:\n      tcp:\n        address: "+addr+"\n"+oneChannel))
			if err == nil {
				t.Errorf("%s was accepted: /metrics would serve channel names, volumes "+
					"and consumer positions to anyone who can reach the port", addr)
				continue
			}
			if !strings.Contains(err.Error(), "authentication") {
				t.Errorf("%s: the error does not say why: %v", addr, err)
			}
		}
	})

	// **A required client certificate is authentication too**, and the rule
	// lifts for it.
	//
	// The port is not reachable by whoever can route to it: nothing without
	// a certificate this broker's authority signed completes the handshake.
	// That is the same argument the Unix socket already wins on, where file
	// permissions are accepted as the gate with no password file at all -
	// and the broker was found refusing this configuration while
	// telling the operator no authentication was configured, which was
	// false in the one case it was refusing.
	t.Run("mutual TLS lifts the rule, and a certificate that is merely "+
		"verified does not", func(t *testing.T) {
		tlsBlock := func(extra string) string {
			return "  operations:\n    listen:\n      tcp:\n" +
				"        address: 0.0.0.0:9090\n" +
				"        tls:\n" +
				"          cert_file: /etc/saguin/c.pem\n" +
				"          key_file: /etc/saguin/k.pem\n" + extra
		}
		for _, tc := range []struct {
			name, extra string
			accepted    bool
		}{
			{"server TLS alone is not authentication", "", false},
			{"a client authority, required by default",
				"          client_ca_file: /etc/saguin/ca.pem\n", true},
			{"required in so many words",
				"          client_ca_file: /etc/saguin/ca.pem\n" +
					"          require_certificate: true\n", true},
			// The mixed mode: a client without a certificate falls through
			// to the password file, and there is none, so it falls through
			// to nothing.
			{"verified if given, which admits a client that gives none",
				"          client_ca_file: /etc/saguin/ca.pem\n" +
					"          require_certificate: false\n", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := config.Load(write(t, base+tlsBlock(tc.extra)+oneChannel))
				// The certificate files do not exist here, so a
				// configuration that passes this rule fails on the files
				// instead. That is the answer we want: the address was not
				// the objection.
				refusedForAddress := err != nil &&
					strings.Contains(err.Error(), "not a loopback address")
				if tc.accepted && refusedForAddress {
					t.Errorf("a routable address was refused although nothing "+
						"reaches this listener without a certificate: %v", err)
				}
				if !tc.accepted && !refusedForAddress {
					t.Errorf("a routable address was accepted although a client "+
						"presenting no certificate still connects: %v", err)
				}
			})
		}
	})

	// **The rule lifts when there is a credential**, which is what it was
	// always for: not "loopback for ever" but "say who may read this before
	// you let the network reach it".
	t.Run("a password file is what lets it leave loopback", func(t *testing.T) {
		if _, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 0.0.0.0:9090\n"+
			"    password_file: /etc/saguin/operations.passwd\n"+oneChannel)); err != nil {
			t.Errorf("a bound port with authentication configured was refused: %v", err)
		}
	})

	t.Run("a relative password file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"    password_file: operations.passwd\n"+oneChannel))
		if err == nil {
			t.Fatal("a relative password file was accepted: which operators may read " +
				"/metrics would depend on where the broker was started from")
		}
		if !strings.Contains(err.Error(), "password_file") {
			t.Errorf("the error does not name the key: %v", err)
		}
	})

	// **`allow_anonymous` is refused on both doors**, and it is the one key
	// of the two that stays refused. The operations listener has no
	// anonymous answer to give: a door with a password file makes every
	// reader prove who it is, and a door with none has no credential at
	// all. `true` on a loopback port would re-open the hole gatedDoor
	// documents, and on a routable one it would contradict the rule below.
	t.Run("per-listener allow_anonymous is refused", func(t *testing.T) {
		for _, tc := range []struct{ name, block, key string }{
			{
				"tcp",
				"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"        allow_anonymous: false\n",
				"broker.operations.listen.tcp.allow_anonymous",
			},
			{
				"unix",
				"  operations:\n    listen:\n      unix:\n        path: /run/saguin/ops.sock\n" +
					"        allow_anonymous: true\n",
				"broker.operations.listen.unix.allow_anonymous",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := config.Load(write(t, base+tc.block+oneChannel))
				if err == nil {
					t.Fatalf("%s was accepted, and nothing reads it", tc.key)
				}
				if !strings.Contains(err.Error(), tc.key) {
					t.Errorf("the error does not name the key written: %v", err)
				}
			})
		}
	})

	// **A door's own password file wins; a door that names none takes the
	// block's.** The two doors reach two populations more often than the key
	// table suggests - a monitoring system on the port, a local agent on the
	// socket - and one file for both makes their credentials rotate
	// together.
	t.Run("a password file resolves per door", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			block     string
			tcp, unix string
		}{
			{
				"the block's file covers both doors",
				"    password_file: /etc/saguin/ops.passwd\n" +
					"    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"      unix:\n        path: /run/saguin/ops.sock\n",
				"/etc/saguin/ops.passwd", "/etc/saguin/ops.passwd",
			},
			{
				"a door's own file wins over the block's",
				"    password_file: /etc/saguin/ops.passwd\n" +
					"    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"        password_file: /etc/saguin/remote.passwd\n" +
					"      unix:\n        path: /run/saguin/ops.sock\n",
				"/etc/saguin/remote.passwd", "/etc/saguin/ops.passwd",
			},
			{
				"each door its own, with no block file at all",
				"    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"        password_file: /etc/saguin/remote.passwd\n" +
					"      unix:\n        path: /run/saguin/ops.sock\n" +
					"        password_file: /etc/saguin/local.passwd\n",
				"/etc/saguin/remote.passwd", "/etc/saguin/local.passwd",
			},
			{
				// A socket with no file anywhere has no credential, and
				// that is not a hole: its permissions are the gate, which
				// is the same argument the loopback rule already accepts.
				"a door with no file anywhere has none",
				"    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"        password_file: /etc/saguin/remote.passwd\n" +
					"      unix:\n        path: /run/saguin/ops.sock\n",
				"/etc/saguin/remote.passwd", "",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f, _, err := config.Load(write(t, base+"  operations:\n"+tc.block+oneChannel))
				if err != nil {
					t.Fatalf("load: %v", err)
				}
				got := map[string]string{}
				for _, o := range f.Broker.Operations.OperatorFiles() {
					got[o.Door] = o.Path
				}
				if got["tcp"] != tc.tcp {
					t.Errorf("the tcp door authenticates against %q, want %q", got["tcp"], tc.tcp)
				}
				if got["unix"] != tc.unix {
					t.Errorf("the unix door authenticates against %q, want %q", got["unix"], tc.unix)
				}
			})
		}
	})

	// **The rule is asked of the door's own file, not the block's.** A
	// listener naming its own operators has authenticated itself, and
	// asking the block instead was the defect that made a per-listener file
	// read as protection while governing nothing.
	t.Run("a listener's own password file lifts the loopback rule", func(t *testing.T) {
		if _, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 0.0.0.0:9090\n"+
			"        password_file: /etc/saguin/remote.passwd\n"+oneChannel)); err != nil {
			t.Errorf("a bound port naming its own operators was refused: %v", err)
		}
	})

	// A file on the *other* door says nothing about this one, so the port
	// is still unauthenticated and still refused.
	t.Run("a file on the socket does not authenticate the port", func(t *testing.T) {
		_, _, err := config.Load(write(t, base+
			"  operations:\n    listen:\n      tcp:\n        address: 0.0.0.0:9090\n"+
			"      unix:\n        path: /run/saguin/ops.sock\n"+
			"        password_file: /etc/saguin/local.passwd\n"+oneChannel))
		if err == nil {
			t.Fatal("a bound port was accepted on the socket's credential")
		}
		if !strings.Contains(err.Error(), "not a loopback address") {
			t.Errorf("the error is not the loopback rule: %v", err)
		}
	})

	// The same rule the block's file takes, at both new places it can be
	// written: which operators may read /metrics must not depend on the
	// directory the broker was started from.
	t.Run("a relative per-listener password file is refused", func(t *testing.T) {
		for _, tc := range []struct{ name, block, key string }{
			{
				"tcp",
				"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n" +
					"        password_file: remote.passwd\n",
				"broker.operations.listen.tcp.password_file",
			},
			{
				"unix",
				"  operations:\n    listen:\n      unix:\n        path: /run/saguin/ops.sock\n" +
					"        password_file: local.passwd\n",
				"broker.operations.listen.unix.password_file",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := config.Load(write(t, base+tc.block+oneChannel))
				if err == nil {
					t.Fatalf("a relative %s was accepted", tc.key)
				}
				if !strings.Contains(err.Error(), tc.key) {
					t.Errorf("the error does not name the key: %v", err)
				}
			})
		}
	})

	t.Run("loopback is accepted", func(t *testing.T) {
		for _, addr := range []string{"127.0.0.1:9090", "localhost:9090", "\"[::1]:9090\""} {
			if _, _, err := config.Load(write(t,
				base+"  operations:\n    listen:\n      tcp:\n        address: "+addr+"\n"+oneChannel)); err != nil {
				t.Errorf("%s was refused: %v", addr, err)
			}
		}
	})
}

// RFC 0002 "What this broker is called", and examples/saguin.yaml:
// broker.id has no default and is refused when absent. It names the broker
// in its own logs, and an unnamed one is unhelpful the first time two
// appear in one.
func TestBrokerIDIsRequired(t *testing.T) {
	listen := "  mqtt:\n    listen:\n      tcp:\n        address: :1883\n"

	if _, _, err := config.Load(write(t, "broker:\n"+listen+oneChannel)); err == nil {
		t.Fatal("a configuration with no broker.id was accepted")
	} else if !strings.Contains(err.Error(), "broker.id") {
		t.Fatalf("the error does not name the key: %v", err)
	}

	// An empty one is the same mistake written out.
	if _, _, err := config.Load(write(t, "broker:\n  id: \"\"\n"+listen+oneChannel)); err == nil {
		t.Fatal("an empty broker.id was accepted")
	}

	if _, _, err := config.Load(write(t, "broker:\n  id: edge-1\n"+memStorage+listen+oneChannel)); err != nil {
		t.Fatalf("a named broker was refused: %v", err)
	}
}

// RFC 0002 "Retained messages on a broadcast topic"
//
// The store always exists. The block only changes its provider and its
// period, and both have defaults worth checking rather than assuming - the
// provider is the one channels get, and the period is `none`, deliberately.
func TestTheRetainedStoreIsConfigured(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
      spare:
        type: memory
        snapshot_dir: none
`
	for _, tc := range []struct {
		name, block  string
		wantProvider string
		wantPeriod   int64
	}{
		{"absent takes both defaults", "", "local", 0},
		{"an empty block takes both defaults", "  retained:\n", "local", 0},
		{"a provider of its own", "  retained:\n    storage: spare\n", "spare", 0},
		{"a period, in seconds", "  retained:\n    retention_period: 30m\n", "local", 1800},
		{"none is a value", "  retained:\n    retention_period: none\n", "local", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			provider, period := f.RetainedStore()
			if provider != tc.wantProvider {
				t.Errorf("provider %q, want %q", provider, tc.wantProvider)
			}
			if period != tc.wantPeriod {
				t.Errorf("retention period %ds, want %ds", period, tc.wantPeriod)
			}
		})
	}
}

// Every one of these is a line an operator could write believing it did
// something. A key that is accepted and ignored is a bound somebody thinks
// they set.
func TestTheRetainedStoreIsRefused(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
`
	for _, tc := range []struct{ name, config, want string }{
		{"a provider that does not exist",
			providers + "  retained:\n    storage: nowhere\n", "not a defined provider"},
		{"a size bound it does not have",
			providers + "  retained:\n    retention_bytes: 8MiB\n", "retention_bytes does not apply"},
		{"the other size bound it does not have",
			providers + "  retained:\n    max_bytes: 8MiB\n", "max_bytes does not apply"},
		{"a period that is not a duration",
			providers + "  retained:\n    retention_period: soon\n", "retention_period"},
		{"nowhere to keep it",
			"broker:\n  id: t\n  retained:\n    storage: local\n", "define at least one provider"},
		// The block is read through a yaml.Node so that `retained:` with
		// nothing under it can mean the defaults rather than nothing at all.
		// A Node decodes without KnownFields, so this is the case that says
		// the strict decode inside RetainedBlock is still doing its job.
		{"a typo inside the block",
			providers + "  retained:\n    retenion_period: 30m\n", "retenion_period"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.config+oneChannel))
			if err == nil {
				t.Fatalf("loaded, want a refusal mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused with %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// RFC 0002 "Exactly-once publishes"
//
// QoS 2 is always offered. The block only changes how many unfinished
// publishes one client may hold, and how long one waits, and both have
// defaults worth checking rather than assuming.
func TestTheQoS2LimitsAreConfigured(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
      spare:
        type: memory
        snapshot_dir: none
`
	for _, tc := range []struct {
		name, block  string
		wantInflight int
		wantExpires  time.Duration
	}{
		{"absent takes every default", "", 20, 5 * time.Minute},
		{"an empty block takes every default", "  qos2:\n", 20, 5 * time.Minute},
		{"a per-client count", "  qos2:\n    max_inflight_per_client: 4\n", 4, 5 * time.Minute},
		{"an age", "  qos2:\n    expires_after: 90s\n", 20, 90 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			inflight, expires := f.QoS2Limits()
			if inflight != tc.wantInflight {
				t.Errorf("max_inflight_per_client %d, want %d", inflight, tc.wantInflight)
			}
			if expires != tc.wantExpires {
				t.Errorf("expires_after %s, want %s", expires, tc.wantExpires)
			}
		})
	}
}

// Every one of these is a line an operator could write believing it did
// something, and the last two are the ones that would otherwise produce a
// broker advertising exactly-once while being unable to hold a single
// message.
func TestTheQoS2StoreIsRefused(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
`
	for _, tc := range []struct{ name, config, want string }{
		// A held publish waits in the store of the channel it is for, so a
		// provider named here would be a key that does nothing.
		{"a provider, which it does not take",
			providers + "  qos2:\n    storage: local\n", "held in the store of the channel it is for"},
		{"a size bound it does not have",
			providers + "  qos2:\n    max_bytes: 8MiB\n", "max_bytes does not apply"},
		{"an age that is not a duration",
			providers + "  qos2:\n    expires_after: soon\n", "expires_after"},
		{"an allowance of none at all",
			providers + "  qos2:\n    max_inflight_per_client: 0\n", "write 1 or more"},
		{"nowhere to hold them",
			"broker:\n  id: t\n  qos2:\n    storage: local\n", "define at least one provider"},
		// The block is read through a yaml.Node so that `qos2:` with nothing
		// under it can mean the defaults rather than nothing at all. A Node
		// decodes without KnownFields, so this is the case that says the
		// strict decode inside QoS2Block is still doing its job.
		{"a typo inside the block",
			providers + "  qos2:\n    expires_afer: 90s\n", "expires_afer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.config+oneChannel))
			if err == nil {
				t.Fatalf("loaded, want a refusal mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused with %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// RFC 0002 `retry.backoff` and `retry.backoff_base`.
//
// The pair is stated together or not at all, and the reason is that either
// one alone is somebody who meant to have a backoff and has not got one. A
// shape with no base has no gap to build; a base with no shape is a duration
// nothing reads. Both would start a broker that looks configured and retries
// a downed dependency five times a second, which is the moment nobody is
// reading the configuration file.
func TestRetryBackoff(t *testing.T) {
	queue := func(body string) string {
		return "broker:\n  id: t\n" + memStorage + "channels:\n  jobs:\n    type: queue\n    retry:\n" + body
	}

	t.Run("accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			body string
			kind store.BackoffKind
			base int64
		}{
			{"linear", "      backoff: linear\n      backoff_base: 2s\n", store.BackoffLinear, 2},
			{"exponential", "      backoff: exponential\n      backoff_base: 30s\n", store.BackoffExponential, 30},
			{"none written out", "      backoff: none\n", store.BackoffNone, 0},
			{"nothing written at all", "      max_attempts: 5\n", store.BackoffNone, 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, reg, err := config.Load(write(t, queue(tc.body)))
				if err != nil {
					t.Fatalf("load: %v", err)
				}
				c := reg.All()["jobs"]
				if c.Backoff != tc.kind {
					t.Errorf("backoff is %v, want %v", c.Backoff, tc.kind)
				}
				if c.BackoffBase != tc.base {
					t.Errorf("backoff_base is %d seconds, want %d", c.BackoffBase, tc.base)
				}
			})
		}
	})

	t.Run("refused", func(t *testing.T) {
		for _, tc := range []struct{ name, body, want string }{
			{"a shape with no base", "      backoff: linear\n", "backoff_base"},
			{"a base with no shape", "      backoff_base: 2s\n", "backoff_base"},
			{"a base beside an explicit none", "      backoff: none\n      backoff_base: 2s\n", "backoff_base"},
			{"a shape nobody implements", "      backoff: fibonacci\n      backoff_base: 2s\n", "fibonacci"},
			// The same rule every other interval in the file follows: the
			// parser admits whole seconds, so a gap that would round to zero
			// is refused by name rather than silently becoming none.
			{"a base below a second", "      backoff: linear\n      backoff_base: 500ms\n", "backoff_base"},
			{"a base with no unit", "      backoff: linear\n      backoff_base: 2\n", "backoff_base"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := config.Load(write(t, queue(tc.body)))
				if err == nil {
					t.Fatal("accepted")
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("the error does not name %s: %v", tc.want, err)
				}
			})
		}
	})

	// retry belongs to a queue, like visibility_timeout and job_expires_after
	// beside it. On any other type it is a key that would be read by nothing.
	t.Run("only on a queue", func(t *testing.T) {
		_, _, err := config.Load(write(t,
			"broker:\n  id: t\n"+memStorage+"channels:\n  events:\n    type: append\n"+
				"    retry:\n      backoff: linear\n      backoff_base: 2s\n"))
		if err == nil {
			t.Fatal("accepted a backoff on an append channel")
		}
		if !strings.Contains(err.Error(), "only to a queue") {
			t.Fatalf("the error does not say it applies only to a queue: %v", err)
		}
	})
}

// Providers split across files, which is the point of the feature: a
// domain's storage moves into a file its own owners review, and a channel
// in one file names a provider from another because resolution is by name.
//
// The rules are the channels' rules and the implementation is now literally
// the same code, so what these check is that providers reach it - a second
// copy of every include test would be testing the same function twice.
func TestProvidersCanBeIncluded(t *testing.T) {
	const master = "broker:\n  id: t\n" +
		"  storage:\n    default: local\n" +
		"    default_retention_period: none\n    default_retention_bytes: none\n" +
		"    providers:\n      - !include storage/local.yaml\n      - !include storage/edge.yaml\n" +
		"channels:\n  events:\n    type: append\n    storage: edge\n"

	t.Run("two files, and a channel naming a provider from one of them", func(t *testing.T) {
		f, _, err := config.Load(tree(t, master, map[string]string{
			"storage/local.yaml": "local:\n  type: sqlite\n  file_path: /var/lib/saguin/saguin.db\n",
			"storage/edge.yaml":  "edge:\n  type: memory\n  snapshot_dir: /var/lib/saguin/snapshots\n",
		}))
		if err != nil {
			t.Fatalf("refused: %v", err)
		}
		if len(f.Broker.Storage.Providers) != 2 {
			t.Fatalf("%d providers, want the two the includes named", len(f.Broker.Storage.Providers))
		}
		if got := f.Broker.Storage.Providers["edge"].SnapshotDir; got != "/var/lib/saguin/snapshots" {
			t.Errorf("the included provider did not survive: snapshot_dir %q", got)
		}
	})

	t.Run("a name defined twice names both files", func(t *testing.T) {
		_, _, err := config.Load(tree(t, master, map[string]string{
			"storage/local.yaml": "local:\n  type: sqlite\n  file_path: /var/lib/saguin/a.db\n",
			"storage/edge.yaml":  "local:\n  type: sqlite\n  file_path: /var/lib/saguin/b.db\n",
		}))
		if err == nil {
			t.Fatal("one provider name in two files was accepted, so one of them wins silently")
		}
		for _, want := range []string{"defined twice", "local.yaml", "edge.yaml"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the message does not carry %q: %v", want, err)
			}
		}
	})

	t.Run("a typo is refused in the file it was written in", func(t *testing.T) {
		_, _, err := config.Load(tree(t, master, map[string]string{
			"storage/local.yaml": "local:\n  type: sqlite\n  file_path: /var/lib/saguin/a.db\n",
			"storage/edge.yaml":  "edge:\n  type: memory\n  snapshot_dirr: /var/lib/saguin/s\n",
		}))
		if err == nil {
			t.Fatal("an unknown key in an included provider file was accepted")
		}
		if !strings.Contains(err.Error(), "edge.yaml") || !strings.Contains(err.Error(), "snapshot_dirr") {
			t.Errorf("the message does not name the file and the key: %v", err)
		}
	})

	t.Run("nesting and wildcards are refused here too", func(t *testing.T) {
		_, _, err := config.Load(tree(t, master, map[string]string{
			"storage/local.yaml":  "local:\n  type: sqlite\n  file_path: /var/lib/saguin/a.db\nmore: !include deeper.yaml\n",
			"storage/edge.yaml":   "edge:\n  type: memory\n  snapshot_dir: /var/lib/saguin/s\n",
			"storage/deeper.yaml": "deep:\n  type: memory\n  snapshot_dir: none\n",
		}))
		if err == nil || !strings.Contains(err.Error(), "cannot include another") {
			t.Errorf("a nested include under providers: %v", err)
		}

		_, _, err = config.Load(tree(t,
			strings.Replace(master, "- !include storage/edge.yaml", "- !include \"storage/*.yaml\"", 1),
			map[string]string{
				"storage/local.yaml": "local:\n  type: sqlite\n  file_path: /var/lib/saguin/a.db\n",
			}))
		if err == nil || !strings.Contains(err.Error(), "no wildcards") {
			t.Errorf("a wildcard under providers: %v", err)
		}
	})
}

// **Two provider names on one store are two writers on it**, each with its
// own bound and its own retention. A database dies at runtime on its lock
// file, which is late and says nothing about the configuration; a snapshot
// directory has no lock at all, so the two write one another's files at
// shutdown and the loser's channels come back short.
//
// The message names both files because that is what changes once providers
// can be included: the two halves are written by different people, and a
// finding naming one of them sends the reader to the file that is fine.
func TestTwoProvidersOnOneStoreAreRefused(t *testing.T) {
	const master = "broker:\n  id: t\n" +
		"  storage:\n    default: a\n" +
		"    default_retention_period: none\n    default_retention_bytes: none\n" +
		"    providers:\n      - !include storage/a.yaml\n      - !include storage/b.yaml\n" +
		"channels:\n  events:\n    type: append\n"

	for _, tc := range []struct{ name, a, b, path string }{
		{"one database", "a:\n  type: sqlite\n  file_path: /var/lib/saguin/one.db\n",
			"b:\n  type: sqlite\n  file_path: /var/lib/saguin/one.db\n", "one.db"},
		// The same store written two ways. Cleaning is what makes this the
		// clash it is rather than two paths that merely look different.
		{"one database, spelled two ways", "a:\n  type: sqlite\n  file_path: /var/lib/saguin/one.db\n",
			"b:\n  type: sqlite\n  file_path: /var/lib/saguin/./one.db\n", "one.db"},
		{"one snapshot directory", "a:\n  type: memory\n  snapshot_dir: /var/lib/saguin/s\n",
			"b:\n  type: memory\n  snapshot_dir: /var/lib/saguin/s\n", "/var/lib/saguin/s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(tree(t, master, map[string]string{
				"storage/a.yaml": tc.a, "storage/b.yaml": tc.b,
			}))
			if err == nil {
				t.Fatal("two providers on one store were accepted: they are two writers on it, " +
					"and for a snapshot directory nothing even fails at runtime")
			}
			for _, want := range []string{"a.yaml", "b.yaml", "two writers"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message does not carry %q: %v", want, err)
				}
			}
		})
	}

	// Both in one file is the ordinary case and the message says so shortly
	// rather than naming one file twice.
	t.Run("both in the master file", func(t *testing.T) {
		_, _, err := config.Load(write(t, "broker:\n  id: t\n"+
			"  storage:\n    default: a\n"+
			"    default_retention_period: none\n    default_retention_bytes: none\n"+
			"    providers:\n      a:\n        type: sqlite\n        file_path: /var/lib/saguin/one.db\n"+
			"      b:\n        type: sqlite\n        file_path: /var/lib/saguin/one.db\n"+
			"channels:\n  events:\n    type: append\n"))
		if err == nil {
			t.Fatal("two providers on one database in one file were accepted")
		}
		if !strings.Contains(err.Error(), "Both are in") {
			t.Errorf("the message does not say they share a file: %v", err)
		}
	})

	// A provider that keeps nothing is not on any store, so two of them are
	// not on one store. Without this, `snapshot_dir: none` twice would be a
	// clash on the literal word.
	t.Run("keeping nothing is not a clash", func(t *testing.T) {
		if _, _, err := config.Load(write(t, "broker:\n  id: t\n"+
			"  storage:\n    default: a\n"+
			"    default_retention_period: none\n    default_retention_bytes: none\n"+
			"    providers:\n      a:\n        type: memory\n        snapshot_dir: none\n"+
			"      b:\n        type: memory\n        snapshot_dir: none\n"+
			"channels:\n  events:\n    type: append\n")); err != nil {
			t.Errorf("two providers that each keep nothing were refused: %v", err)
		}
	})
}

// The configuration a split one resolves to, in one document.
//
// RFC 0002 `--check-config --output`, and RFC 0004's promotion runbook,
// which asks an operator to diff one deployment's configuration against
// another's. **The property everything else rests on is that what comes out
// loads again** - a document that could not be started is a report, and a
// diff between two reports is not evidence about two brokers. So that is
// asserted first, and asserted by loading it rather than by reading it.
func TestTheFlattenedConfigurationLoadsAgain(t *testing.T) {
	master := "broker:\n  id: t\n" +
		"  storage:\n    default: local\n" +
		"    default_retention_period: 7d\n    default_retention_bytes: none\n" +
		"    providers:\n      - !include storage/local.yaml\n" +
		"channels:\n  - !include channels/telemetry.yaml\n" +
		"  - jobs:\n      type: queue\n      visibility_timeout: 30s\n" +
		"      job_expires_after: 1h\n      dlq_retention_bytes: 2MiB\n" +
		"      retry:\n        max_attempts: 3\n        backoff: linear\n        backoff_base: 2s\n" +
		"bridges:\n  dr-link:\n    peer: tls://master:8883\n    client_id: dr\n" +
		"    topics:\n      - filter: readings/#\n        topic: readings/$#\n        direction: in\n"
	leaves := map[string]string{
		"storage/local.yaml": "local:\n  type: memory\n  snapshot_dir: /var/lib/saguin/s\n",
		"channels/telemetry.yaml": "readings:\n  type: append\n  retention_period: 3d\n" +
			"  retention_bytes: 4MiB\n  max_bytes: 8MiB\n  start: tail\n" +
			"state:\n  type: latest\n  retention_period: 1d\n",
	}

	path := tree(t, master, leaves)
	f, reg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	doc, err := f.Flatten(reg)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}

	// Written where the includes are not, so that a document still carrying
	// one would fail to find them rather than quietly resolve them again.
	flat := filepath.Join(t.TempDir(), "flat.yaml")
	if err := os.WriteFile(flat, doc, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, again, err := config.Load(flat)
	if err != nil {
		t.Fatalf("the flattened configuration does not load, so it is a report rather "+
			"than a configuration: %v\n%s", err, doc)
	}

	// And it is the same broker. Every channel the registry holds, with
	// every resolved value, because "it loads" and "it is the configuration
	// it came from" are different claims and only the second is the feature.
	before, after := reg.All(), again.All()
	if len(before) != len(after) {
		t.Fatalf("%d channels became %d:\n%s", len(before), len(after), doc)
	}
	for name, want := range before {
		got, ok := after[name]
		if !ok {
			t.Errorf("channel %q is missing from the flattened configuration", name)
			continue
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("channel %q resolved differently:\n before %+v\n  after %+v", name, want, got)
		}
	}

	// **And the fixture has to reach every key, or the comparison above is
	// a comparison of nothing.** It is a DeepEqual over the whole channel,
	// so it catches any value flattening drops - but only for a value this
	// fixture sets, and a key was once dropped for exactly that reason: the
	// test was thorough and the fixture had never heard of it.
	//
	// So the fixture is checked rather than trusted. Every field of a
	// channel must be non-zero somewhere in it; the three below are the ones
	// no configuration writes.
	unwritten := map[string]bool{"Name": true, "Type": true, "DLQ": true}
	covered := map[string]bool{}
	for _, c := range before {
		v := reflect.ValueOf(*c)
		for i := range v.NumField() {
			if !v.Field(i).IsZero() {
				covered[v.Type().Field(i).Name] = true
			}
		}
	}
	var missed []string
	rt := reflect.TypeOf(channel.Channel{})
	for i := range rt.NumField() {
		if name := rt.Field(i).Name; !unwritten[name] && !covered[name] {
			missed = append(missed, name)
		}
	}
	if len(missed) > 0 {
		t.Errorf("no channel in this fixture sets %v, so the comparison above says nothing "+
			"about whether flattening keeps them; set each one on a channel here", missed)
	}
}

// What a reader gets, and what a parser must not depend on.
func TestTheFlattenedConfigurationIsOneDocument(t *testing.T) {
	master := "broker:\n  id: t\n" +
		"  storage:\n    default: local\n" +
		"    default_retention_period: none\n    default_retention_bytes: none\n" +
		"    providers:\n      - !include storage/local.yaml\n" +
		"channels:\n  - !include channels/telemetry.yaml\n" +
		"  - jobs:\n      type: queue\n      visibility_timeout: 30s\n"
	leaves := map[string]string{
		"storage/local.yaml":      "local:\n  type: memory\n  snapshot_dir: none\n",
		"channels/telemetry.yaml": "readings:\n  type: append\n",
	}
	f, reg, err := config.Load(tree(t, master, leaves))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	doc, err := f.Flatten(reg)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	got := string(doc)

	if strings.Contains(got, "!include") {
		t.Errorf("an include survived, so this is the master file rather than the "+
			"configuration:\n%s", got)
	}

	// A queue's companion is not the operator's to name - the suffix is
	// refused as a configured name - so writing it out produces a document
	// that does not load.
	if strings.Contains(got, "jobs__dlq") {
		t.Errorf("the derived dead-letter channel was written out, and its name is "+
			"refused on the way back in:\n%s", got)
	}

	// Every default filled in: neither channel named a provider and both
	// have one here, which is the question this document answers - which of
	// my channels would survive this machine.
	if strings.Count(got, "storage: local") != 2 {
		t.Errorf("the default provider was not filled in for both channels:\n%s", got)
	}

	// The provenance is a comment because a reader wants it and a parser
	// must not. Strip every comment and the configuration is unchanged.
	if !strings.Contains(got, "# from") {
		t.Errorf("no channel says which file it came from:\n%s", got)
	}
	var stripped []string
	for _, line := range strings.Split(got, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			stripped = append(stripped, line)
		}
	}
	bare := filepath.Join(t.TempDir(), "bare.yaml")
	if err := os.WriteFile(bare, []byte(strings.Join(stripped, "\n")), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := config.Load(bare); err != nil {
		t.Errorf("the document depends on its comments, which a parser is entitled to "+
			"ignore: %v", err)
	}
}

// A configuration that came from one file has nothing to say about which
// file each channel came from, and saying it anyway is a line of noise per
// channel in a document somebody has to read.
func TestASingleFileConfigurationCarriesNoProvenanceComments(t *testing.T) {
	f, reg, err := config.Load(write(t, "broker:\n  id: t\n"+
		"  storage:\n    default: local\n"+
		"    default_retention_period: none\n    default_retention_bytes: none\n"+
		"    providers:\n      local:\n        type: memory\n        snapshot_dir: none\n"+
		oneChannel))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	doc, err := f.Flatten(reg)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	if strings.Contains(string(doc), "# from") {
		t.Errorf("a single-file configuration was annotated with the file it came "+
			"from, on every channel:\n%s", doc)
	}
}

// RFC 0002 `broker.mqtt.password_file` and `broker.mqtt.allow_anonymous`.
func TestClientAuthenticationKeys(t *testing.T) {
	const listen = "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      tcp:\n        address: :1883\n"

	t.Run("absent takes its answer from the password file", func(t *testing.T) {
		f, _, err := config.Load(write(t, listen+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if !f.Broker.MQTT.Anonymous() {
			t.Error("a broker with no password file refuses anonymous clients, so nothing " +
				"that worked yesterday connects today")
		}

		f, _, err = config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n"+oneChannel))
		if err != nil {
			t.Fatalf("load with a password file: %v", err)
		}
		if f.Broker.MQTT.Anonymous() {
			t.Error("a broker whose operator named who may connect admits clients that " +
				"name nobody")
		}
	})

	// Written, it wins either way. `true` beside a password file is
	// Mosquitto's mixed mode, which a fleet migrating one batch at a time
	// needs.
	t.Run("written, it wins", func(t *testing.T) {
		f, _, err := config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n    allow_anonymous: true\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if !f.Broker.MQTT.Anonymous() {
			t.Error("allow_anonymous: true was not honoured beside a password file")
		}
	})

	t.Run("false with no password file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+"    allow_anonymous: false\n"+oneChannel))
		if err == nil {
			t.Fatal("a broker that admits nobody and has nobody to admit was accepted: " +
				"every client is refused 0x86 and nothing says why")
		}
		if !strings.Contains(err.Error(), "nothing can connect") {
			t.Errorf("the error does not say what is wrong: %v", err)
		}
	})

	// RFC 0002 "What a client may do": an acl_file governs an identity, so
	// it needs something that establishes one. **Two things do** - a
	// password file, or a listener with a client authority, where the
	// identity is the certificate's Common Name - and this listener has
	// neither, which is what makes it a refusal. The certificate route has
	// its own test, because requiring a password file of an estate whose
	// clients are all certificates made it write one that authenticates
	// nobody.
	t.Run("an acl file with no password file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+
			"    acl_file: /etc/saguin/acl.yaml\n"+oneChannel))
		if err == nil {
			t.Fatal("rules governing nobody were accepted: a file full of them reads as " +
				"protection and is none")
		}
		if !strings.Contains(err.Error(), "nothing authenticates a client") {
			t.Errorf("the error does not say what is wrong: %v", err)
		}
		// Both ways out, named: an operator told only "give it a password
		// file" writes a decoy when every client they have is a
		// certificate.
		for _, want := range []string{"password_file", "client_ca_file"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not name %q as a way out: %v", want, err)
			}
		}
	})

	t.Run("an acl file beside allow_anonymous true is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n"+
			"    acl_file: /etc/saguin/acl.yaml\n    allow_anonymous: true\n"+oneChannel))
		if err == nil {
			t.Fatal("an anonymous client was admitted beside an acl_file, where the `*` " +
				"pattern grants it whatever that role holds")
		}
		// **What the refusal says matters as much as that it fires.** It
		// said such a client would be "admitted with no rule able to apply
		// to it", which is not what happens: an anonymous client's identity
		// is the empty string and `*` matches it, so it is admitted *and
		// granted*. An operator who believed the first version would think
		// the risk was an unauthorized client with no permissions.
		if !strings.Contains(err.Error(), "the `*` pattern in the acl_file matches its empty name") {
			t.Errorf("the error does not say what is wrong: %v", err)
		}
	})

	// The same pair written on a listener, which is where it escaped: the
	// two rules read broker.mqtt directly until a listener could carry its
	// own, and a socket allowing anonymous connections beside an acl_file
	// passed as ok and started.
	t.Run("an acl file beside a listener that allows anonymous is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+"  mqtt:\n"+
			"    password_file: /etc/saguin/clients.passwd\n"+
			"    acl_file: /etc/saguin/acl.yaml\n"+
			"    listen:\n      tcp:\n        address: :1883\n"+
			"      unix:\n        path: /run/saguin/saguin.sock\n"+
			"        allow_anonymous: true\n"+oneChannel))
		if err == nil {
			t.Fatal("a listener admitting anonymous clients beside an acl_file was " +
				"accepted; the broker-level pair is refused and this is the same hole")
		}
		if !strings.Contains(err.Error(), "broker.mqtt.listen.unix") {
			t.Errorf("the finding does not name the listener it is about: %v", err)
		}
	})

	// And a listener taking the broker's pair is judged once, by the
	// broker-level rules. One typo must not become one finding per door.
	t.Run("a listener inheriting the pair is not reported twice", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n"+
			"    acl_file: /etc/saguin/acl.yaml\n    allow_anonymous: true\n"+oneChannel))
		if err == nil {
			t.Fatal("the pair was accepted")
		}
		if n := strings.Count(err.Error(), "matches its empty name"); n != 1 {
			t.Errorf("the same pair produced %d findings, want 1 - a listener that "+
				"writes neither key takes the broker's answer, which the broker-level "+
				"rule already judges:\n%v", n, err)
		}
	})

	// The pair above must not fire on the ordinary configuration: with a
	// password file and allow_anonymous unwritten, an anonymous client is
	// already refused, so there is nothing for the two keys to disagree
	// about.
	t.Run("an acl file beside a password file is accepted", func(t *testing.T) {
		f, _, err := config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n"+
			"    acl_file: /etc/saguin/acl.yaml\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if f.Broker.MQTT.ACLFile != "/etc/saguin/acl.yaml" {
			t.Errorf("acl_file resolved to %q", f.Broker.MQTT.ACLFile)
		}
	})

	t.Run("a relative acl file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+
			"    password_file: /etc/saguin/clients.passwd\n"+
			"    acl_file: acl.yaml\n"+oneChannel))
		if err == nil {
			t.Fatal("a relative acl file was accepted: what a client may do would depend " +
				"on where the broker was started from")
		}
		if !strings.Contains(err.Error(), "acl_file") {
			t.Errorf("the error does not name the key: %v", err)
		}
	})

	t.Run("a relative password file is refused", func(t *testing.T) {
		_, _, err := config.Load(write(t, listen+"    password_file: clients.passwd\n"+oneChannel))
		if err == nil {
			t.Fatal("a relative password file was accepted: which clients may connect " +
				"would depend on where the broker was started from")
		}
		if !strings.Contains(err.Error(), "password_file") {
			t.Errorf("the error does not name the key: %v", err)
		}
	})
}

// RFC 0002 `tls` on a listener: a certificate and its key, both absolute.
func TestListenerTLSKeys(t *testing.T) {
	const head = "broker:\n  id: t\n" + memStorage + "  mqtt:\n    listen:\n      tcp:\n        address: :8883\n"

	t.Run("a certificate and its key are carried through", func(t *testing.T) {
		f, _, err := config.Load(write(t, head+
			"        tls:\n          cert_file: /etc/saguin/tls/cert.pem\n"+
			"          key_file: /etc/saguin/tls/key.pem\n"+oneChannel))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		tls := f.Broker.MQTT.Listen.TCP[0].TLS
		if tls == nil {
			t.Fatal("the tls block was dropped")
		}
		if tls.CertFile != "/etc/saguin/tls/cert.pem" || tls.KeyFile != "/etc/saguin/tls/key.pem" {
			t.Errorf("cert %q key %q", tls.CertFile, tls.KeyFile)
		}
	})

	// Half a TLS block is the shape that would otherwise fail at the first
	// handshake, on somebody else's machine.
	for _, tc := range []struct{ name, block, want string }{
		{"no key", "        tls:\n          cert_file: /etc/saguin/tls/cert.pem\n", "key_file"},
		{"no certificate", "        tls:\n          key_file: /etc/saguin/tls/key.pem\n", "cert_file"},
		{"an empty block", "        tls: {}\n", "cert_file"},
		{"a relative certificate", "        tls:\n          cert_file: cert.pem\n" +
			"          key_file: /etc/saguin/tls/key.pem\n", "relative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, head+tc.block+oneChannel))
			if err == nil {
				t.Fatal("accepted, so the listener fails at its first handshake instead")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the error does not say what is wrong (%q): %v", tc.want, err)
			}
		})
	}

	t.Run("the version floor", func(t *testing.T) {
		for written, want := range map[string]uint16{"": 0x0303, "1.2": 0x0303, "1.3": 0x0304} {
			body := head + "        tls:\n          cert_file: /c.pem\n          key_file: /k.pem\n"
			if written != "" {
				body += "          min_version: \"" + written + "\"\n"
			}
			f, _, err := config.Load(write(t, body+oneChannel))
			if err != nil {
				t.Fatalf("min_version %q: %v", written, err)
			}
			if got := f.Broker.MQTT.Listen.TCP[0].TLS.Version(); got != want {
				t.Errorf("min_version %q gave 0x%04X, want 0x%04X", written, got, want)
			}
		}
	})

	// A floor below 1.2 is the setting this key exists to not have, and a
	// typo in it must not quietly mean 1.2.
	t.Run("anything else is refused", func(t *testing.T) {
		for _, v := range []string{"1.1", "1.0", "tlsv1.2", "1.4", "yes"} {
			_, _, err := config.Load(write(t, head+
				"        tls:\n          cert_file: /c.pem\n          key_file: /k.pem\n"+
				"          min_version: \""+v+"\"\n"+oneChannel))
			if err == nil {
				t.Errorf("min_version %q was accepted", v)
				continue
			}
			if !strings.Contains(err.Error(), "min_version") {
				t.Errorf("min_version %q: the error does not name the key: %v", v, err)
			}
		}
	})

	// The operations listener takes the same block, and the same rules.
	t.Run("the operations listener too", func(t *testing.T) {
		_, _, err := config.Load(write(t, head+
			"  operations:\n    listen:\n      tcp:\n        address: 127.0.0.1:9090\n"+
			"        tls:\n          cert_file: cert.pem\n          key_file: /k.pem\n"+oneChannel))
		if err == nil || !strings.Contains(err.Error(), "broker.operations.listen.tcp.tls") {
			t.Errorf("a relative certificate on the operations listener: %v", err)
		}
	})
}

// RFC 0003: `deletion_retention_period` is how long a latest channel keeps a
// deletion, and it is a period of its own because a value and a deletion are
// not the same thing to keep. It applies to no other channel type, so
// writing it elsewhere is refused rather than ignored.
func TestDeletionRetentionAppliesToLatestChannelsOnly(t *testing.T) {
	const head = "broker:\n  id: t\n" + memStorage + "channels:\n  c:\n"
	for name, tc := range map[string]struct {
		body    string
		wantErr string
		want    int64
	}{
		"a period on a latest channel": {"    type: latest\n    deletion_retention_period: 3d\n", "", 3 * 24 * 60 * 60},
		"none on a latest channel":     {"    type: latest\n    deletion_retention_period: none\n", "", 0},
		"the default":                  {"    type: latest\n", "", config.DefaultDeletionRetentionPeriod},
		"on an append channel":         {"    type: append\n    deletion_retention_period: 3d\n", "applies only to a latest", 0},
		"on a queue":                   {"    type: queue\n    deletion_retention_period: 3d\n", "applies only to a latest", 0},
		"not a period":                 {"    type: latest\n    deletion_retention_period: soon\n", "deletion_retention_period", 0},
	} {
		t.Run(name, func(t *testing.T) {
			_, reg, err := config.Load(write(t, head+tc.body))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("accepted")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error does not mention %q: %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := reg.Get("c").DeletionRetentionPeriod; got != tc.want {
				t.Errorf("deletion_retention_period resolved to %d seconds, want %d", got, tc.want)
			}
		})
	}
}

// **A listener's authentication, and what it inherits when it says
// nothing.**
//
// The rule that is easy to get wrong is the third row: a listener naming
// its own password file and saying nothing about anonymous connections
// means "these users, and nobody else". Taking the broker's `true` there
// would admit everyone to the listener that had just been handed a
// credential list, which is the opposite of what writing one says - and it
// would do it silently, because nothing in the file would look wrong.
func TestListenerAuthResolvesAgainstTheBroker(t *testing.T) {
	yes, no := true, false
	for name, tc := range map[string]struct {
		broker   config.MQTT
		listener config.Auth
		wantFile string
		wantAnon bool
	}{
		"a listener saying nothing takes the broker's pair": {
			broker:   config.MQTT{PasswordFile: "/etc/saguin/clients", AllowAnonymous: &no},
			wantFile: "/etc/saguin/clients", wantAnon: false,
		},
		"a listener may open a door the broker keeps shut": {
			broker:   config.MQTT{PasswordFile: "/etc/saguin/clients", AllowAnonymous: &no},
			listener: config.Auth{AllowAnonymous: &yes},
			wantFile: "/etc/saguin/clients", wantAnon: true,
		},
		"its own file means its own users and nobody else": {
			broker:   config.MQTT{PasswordFile: "/etc/saguin/clients", AllowAnonymous: &yes},
			listener: config.Auth{PasswordFile: "/etc/saguin/admins"},
			wantFile: "/etc/saguin/admins", wantAnon: false,
		},
		"and it may still say otherwise": {
			broker:   config.MQTT{PasswordFile: "/etc/saguin/clients", AllowAnonymous: &no},
			listener: config.Auth{PasswordFile: "/etc/saguin/admins", AllowAnonymous: &yes},
			wantFile: "/etc/saguin/admins", wantAnon: true,
		},
		"no file anywhere is the broker's own default of anonymous": {
			wantFile: "", wantAnon: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			file, anon := tc.listener.Resolve(tc.broker)
			if file != tc.wantFile || anon != tc.wantAnon {
				t.Errorf("resolved to file %q anonymous %v, want %q and %v",
					file, anon, tc.wantFile, tc.wantAnon)
			}
		})
	}
}

// RFC 0002: a sqlite provider may collect the publishes that arrive close
// together into one transaction, and a batch ends at whichever of the two
// settings is reached first.
//
// The accepted half. A provider that says nothing has no CommitGroup, so the
// provider collects without waiting; `none` is a CommitGroup with no
// interval, a transaction per publish. The two must not read alike.
func TestHowASQLiteProviderCommits(t *testing.T) {
	body := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
		"        file_path: /var/lib/s.db\n%s" +
		"\nchannels:\n  events:\n    type: append\n"

	for name, tc := range map[string]struct {
		keys     string
		said     bool
		interval time.Duration
		records  int
	}{
		"nothing said": {"", false, 0, 0},
		"both": {
			"        publish_commit_interval: 2ms\n        publish_commit_max_records: 64\n", true, 2 * time.Millisecond, 64,
		},
		"none, written down": {
			"        publish_commit_interval: none\n", true, 0, 0,
		},
		"none, in capitals": {
			"        publish_commit_interval: NONE\n", true, 0, 0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := config.Load(write(t, fmt.Sprintf(body, tc.keys)))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			g, ok := f.Broker.Storage.CommitGroups()["p"]
			if ok != tc.said {
				t.Fatalf("a CommitGroup is there=%v, want %v", ok, tc.said)
			}
			if !tc.said {
				return
			}
			if g.Interval != tc.interval {
				t.Errorf("publish_commit_interval is %v, want %v", g.Interval, tc.interval)
			}
			if g.MaxRecords != tc.records {
				t.Errorf("publish_commit_max_records is %d, want %d", g.MaxRecords, tc.records)
			}
		})
	}
}

// The refused half, and it is the half that matters: every one of these is
// a file that reads as though publishes are collected and collects none, or
// a number that would make the broker slower than saying nothing at all. A
// knob whose wrong settings are accepted in silence is a defect waiting for
// somebody to set it.
//
// Each case asserts the words the message has to carry, because a refusal
// that does not say which key and why sends an operator to the source.
func TestHowASQLiteProviderCommitsIsRefused(t *testing.T) {
	sqliteBody := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
		"        file_path: /var/lib/s.db\n%s" +
		"\nchannels:\n  events:\n    type: append\n"
	// The same provider under a connection limit small enough that a
	// perfectly ordinary record count is above it - which is the bound that
	// catches the commonest way to get this wrong.
	sqliteBodyWithConnections := "broker:\n  id: b\n" + "  limits:\n    max_connections: 32\n  storage:" +
		"\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
		"        file_path: /var/lib/s.db\n%s" +
		"\nchannels:\n  events:\n    type: append\n"
	memoryBody := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: memory\n" +
		"        snapshot_dir: none\n%s" +
		"\nchannels:\n  events:\n    type: append\n"

	for name, tc := range map[string]struct{ body, keys, want string }{
		"an interval that is not a duration": {
			sqliteBody, "        publish_commit_interval: soon\n", "not a duration",
		},
		"an interval at zero": {
			sqliteBody, "        publish_commit_interval: 0ms\n", "greater than zero",
		},
		"a negative interval": {
			sqliteBody, "        publish_commit_interval: -5ms\n", "greater than zero",
		},
		"an interval in seconds rather than milliseconds": {
			sqliteBody, "        publish_commit_interval: 5s\n", "one record per connection at a time",
		},
		"a negative record count": {
			sqliteBody, "        publish_commit_interval: 5ms\n        publish_commit_max_records: -4\n",
			"the least saguin accepts is 2",
		},
		"a record count of one": {
			sqliteBody, "        publish_commit_interval: 5ms\n        publish_commit_max_records: 1\n",
			"the least saguin accepts is 2",
		},
		"a record count above limits.max_connections": {
			sqliteBodyWithConnections, "        publish_commit_interval: 5ms\n        publish_commit_max_records: 64\n",
			"limits.max_connections is 32",
		},
		"a record count above what a batch could ever hold": {
			sqliteBody, "        publish_commit_interval: 5ms\n        publish_commit_max_records: 300000\n",
			"one record per connection at a time",
		},
		"a record count with nothing to open a batch": {
			sqliteBody, "        publish_commit_max_records: 256\n", "publish_commit_interval is what makes a transaction wait",
		},
		// Leaving the interval out collects without waiting, so a refusal that
		// sends the operator there for one record per transaction sends them
		// to the opposite of what they asked for.
		"a record count of one names none for a transaction per publish": {
			sqliteBody, "        publish_commit_interval: 5ms\n        publish_commit_max_records: 1\n",
			"write publish_commit_interval: none for that",
		},
		"a record count beside none": {
			sqliteBody, "        publish_commit_interval: none\n        publish_commit_max_records: 256\n",
			"gives no wait (absent or `none`)",
		},
		"an interval with no record count": {
			sqliteBody, "        publish_commit_interval: 5ms\n", "There is no default",
		},
		"an interval on a memory provider": {
			memoryBody, "        publish_commit_interval: 5ms\n", "belongs to a sqlite provider",
		},
		"a record count on a memory provider": {
			memoryBody, "        publish_commit_max_records: 64\n", "belongs to a sqlite provider",
		},
		"the interval under its earlier name": {
			sqliteBody, "        commit_interval: 2ms\n        publish_commit_max_records: 4\n",
			"commit_interval is now publish_commit_interval",
		},
		"the record count under its earlier name": {
			sqliteBody, "        publish_commit_interval: 2ms\n        commit_max_records: 4\n",
			"commit_max_records is now publish_commit_max_records",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, fmt.Sprintf(tc.body, tc.keys)))
			if err == nil {
				t.Fatalf("accepted, want a finding saying %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}

// RFC 0002: read_connections, how many read connections a sqlite provider
// opens beside its writer. Absent is store.SQLiteReadConnections; 0 and the
// ceiling are taken as written; below 0, past the ceiling, and on a memory
// provider it is refused, naming the key.
func TestReadConnections(t *testing.T) {
	sqliteBody := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
		"        file_path: /var/lib/s.db\n%s" +
		"\nchannels:\n  events:\n    type: append\n"
	for name, tc := range map[string]struct {
		keys string
		want int
	}{
		"absent":           {"", store.SQLiteReadConnections},
		"none":             {"        read_connections: 0\n", 0},
		"one":              {"        read_connections: 1\n", 1},
		"the most allowed": {fmt.Sprintf("        read_connections: %d\n", store.SQLiteMaxReadConnections), store.SQLiteMaxReadConnections},
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := config.Load(write(t, fmt.Sprintf(sqliteBody, tc.keys)))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if got, ok := f.Broker.Storage.ReadConnections()["p"]; !ok || got != tc.want {
				t.Fatalf("read_connections is %d (present %v), want %d", got, ok, tc.want)
			}
		})
	}
	memoryBody := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: memory\n" +
		"        snapshot_dir: none\n        read_connections: 2\n" +
		"\nchannels:\n  events:\n    type: append\n"
	for name, tc := range map[string]struct{ body, want string }{
		"below zero":        {fmt.Sprintf(sqliteBody, "        read_connections: -1\n"), "read_connections is -1"},
		"past the ceiling":  {fmt.Sprintf(sqliteBody, fmt.Sprintf("        read_connections: %d\n", store.SQLiteMaxReadConnections+1)), "read_connections is"},
		"a memory provider": {memoryBody, "read_connections belongs to a sqlite provider"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.body))
			if err == nil {
				t.Fatalf("accepted, want a finding saying %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}

// RFC 0002 `broker.session`: how long a connected session's acknowledgements
// may wait to be stored. Absent is 200ms; anything from 10ms to a second is
// taken as written; everything else is refused, naming the key and why -
// there is no value that turns storing them off.
func TestTheAckCommitInterval(t *testing.T) {
	body := "broker:\n  id: b\n" + memStorage + "%s\nchannels:\n  events:\n    type: append\n"
	for name, tc := range map[string]struct {
		session string
		want    time.Duration
	}{
		"absent":             {"", 200 * time.Millisecond},
		"an empty block":     {"  session:\n", 200 * time.Millisecond},
		"the least accepted": {"  session:\n    ack_commit_interval: 10ms\n", 10 * time.Millisecond},
		"the most accepted":  {"  session:\n    ack_commit_interval: 1s\n", time.Second},
		"in between":         {"  session:\n    ack_commit_interval: 50ms\n", 50 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := config.Load(write(t, fmt.Sprintf(body, tc.session)))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if got := f.AckCommitInterval(); got != tc.want {
				t.Errorf("ack_commit_interval is %v, want %v", got, tc.want)
			}
		})
	}
	for name, tc := range map[string]struct{ value, want string }{
		"zero":             {"0ms", "ack_commit_interval"},
		"negative":         {"-50ms", "ack_commit_interval"},
		"none":             {"none", "acknowledgements are always stored"},
		"not a duration":   {"soon", "not a duration"},
		"below the least":  {"5ms", "no setting that turns storing them off"},
		"above the most":   {"2s", "what an unclean stop sends again"},
		"a minute, surely": {"1m", "what an unclean stop sends again"},
	} {
		t.Run("refused/"+name, func(t *testing.T) {
			_, _, err := config.Load(write(t, fmt.Sprintf(body, "  session:\n    ack_commit_interval: "+tc.value+"\n")))
			if err == nil {
				t.Fatalf("%q accepted, want a finding saying %q", tc.value, tc.want)
			}
			if !strings.Contains(err.Error(), "broker.session.ack_commit_interval") || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not name the key and say why: %v", err)
			}
		})
	}
}

// How many keys may be given a value below a second, said once in the
// document and once in the code.
//
// **This sentence has rotted twice in one day.** One edit repaired it "in
// three places" in the morning; a later one added `limits.write_timeout` to
// the keys that honour `ms` and repaired the Rules paragraph alone, leaving two
// sentences saying two. A statement made in three places is a list, and a
// list is repaired one entry at a time until somebody counts.
//
// So the count is checked rather than restated: every sentence in RFC 0002
// that gives a number of such keys must give the same number, and each key
// those sentences name must really accept a sub-second value. The first
// half is what caught the two that were missed; the second is what stops
// the three agreeing on a number the parser disagrees with.
//
// **What it does not catch**, said plainly rather than left to be
// discovered: a key that accepts `ms` and is named in none of the
// sentences. There is no way to enumerate those from the source without
// listing the duration keys, which is the thing this check exists not to
// be - the Rules paragraph is where a change to the parser is supposed to
// land, and this holds the other two to it.
func TestEverySentenceCountingSubSecondKeysAgrees(t *testing.T) {
	md, err := os.ReadFile(filepath.Join("..", "..", "docs", "rfcs",
		"0002-channels-and-configuration.md"))
	if err != nil {
		t.Fatalf("read RFC 0002: %v", err)
	}
	text := string(md)

	// The two shapes the document uses for this count, both anchored on the
	// number word so that a sentence rewritten around it still matches.
	shapes := []*regexp.Regexp{
		regexp.MustCompile(`the ([a-z]+) places a sub-second value is honoured`),
		// **Whitespace where the prose wraps, not a literal space.** One of
		// the three sits in an indented list and breaks after "the", which
		// the first version of this pattern did not match - so the counter
		// below reported two and refused to compare anything, which is the
		// only reason this line is right.
		regexp.MustCompile(`one of the\s+([a-z]+)\s+keys in this file that may be given a\s+(?:value below a\s+second|sub-second value)`),
	}
	words := map[string]int{"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}

	said := map[string][]int{}
	total := 0
	for _, re := range shapes {
		for _, m := range re.FindAllStringSubmatchIndex(text, -1) {
			word := text[m[2]:m[3]]
			said[word] = append(said[word], 1+strings.Count(text[:m[0]], "\n"))
			total++
		}
	}

	// **The counter this check owes.** The sentences are prose and a rewrite
	// could stop matching either shape, at which point every comparison
	// below passes over a document it never read.
	if total < 3 {
		t.Fatalf("found %d sentences counting the sub-second keys and this document has had at "+
			"least three: the patterns no longer match the prose, so nothing "+
			"below means anything (found %v)", total, said)
	}
	if len(said) != 1 {
		t.Errorf("RFC 0002 gives %d different counts of how many keys may take a sub-second "+
			"value: %v (word -> the lines saying it). They are one fact", len(said), said)
	}

	var n int
	for word := range said {
		if n = words[word]; n == 0 {
			t.Fatalf("the document says %q keys, which this check cannot read as a number; "+
				"add it to the table here", word)
		}
	}

	// And the number against the parser: each of the four keys the document
	// names, loaded with a sub-second value.
	//
	// **All four are driven here, and two of them used to be a comment.**
	// The first version listed them and skipped two with a note that the
	// bridge and storage suites cover them - which they do, so the fact was
	// checked and the sentence above it was still describing something this
	// loop did not do. A comment claiming what a neighbouring suite does is
	// the smallest version of a claim nobody runs, and it is cheaper to
	// drive the two than to explain why they are not driven.
	for _, tc := range []struct{ name, yaml string }{
		{
			"limits.write_timeout",
			"broker:\n  id: t\n" + memStorage + "  limits:\n    write_timeout: 500ms\n" +
				"channels:\n  events:\n    type: append\n",
		},
		{
			"broker.session.ack_commit_interval",
			"broker:\n  id: t\n" + memStorage + "  session:\n    ack_commit_interval: 10ms\n" +
				"channels:\n  events:\n    type: append\n",
		},
		{
			"a bridge's ack_interval",
			"broker:\n  id: t\n" + memStorage + bridgeChannels +
				"bridges:\n  head-office:\n" +
				"    peer: tls://mqtt.example.com:8883\n" +
				"    client_id: vessel-07\n" +
				"    ack_interval: 1ms\n" +
				"    topics:\n      - filter: fleet/+/telemetry/#\n" +
				"        topic: telemetry/$1/$#\n        direction: in\n",
		},
		{
			// Both keys or neither, which RFC 0002 says and the loader
			// enforces - so the ceiling rides along rather than being the
			// thing under test.
			"a sqlite provider's publish_commit_interval",
			"broker:\n  id: t\n  storage:\n    default: durable\n" +
				// A storage block must state what a channel keeps when it
				// does not say; neither is what this drives.
				"    default_retention_period: none\n" +
				"    default_retention_bytes: none\n" +
				"    providers:\n      durable:\n        type: sqlite\n" +
				"        file_path: /var/lib/saguin/saguin.db\n" +
				"        publish_commit_interval: 1ms\n        publish_commit_max_records: 4\n" +
				"channels:\n  events:\n    type: append\n",
		},
	} {
		if _, _, err := config.Load(write(t, tc.yaml)); err != nil {
			t.Errorf("RFC 0002 counts %s among the %d keys that may be given a sub-second "+
				"value, and a sub-second value does not load: %v", tc.name, n, err)
		}
	}
}

// RFC 0002 "Validation", "Which channel a topic belongs to" - invariant 12
//
// Every way a `filter` can be wrong, refused before a listener opens.
//
// **Each row is a configuration that reads as working**, which is the whole
// reason they are startup errors rather than runtime ones. A `#` in the
// middle of a filter matches nothing and looks like it matches everything
// below it; a `+` at the first level quietly claims `$SYS` and `$saguin`
// alongside the topics that were meant; two channels with the same filter
// give the broker a tie it has nothing in the file to break. Left to run,
// each of them is a broker doing something nobody wrote down.
func TestAFilterThatCannotMeanWhatItSaysIsRefused(t *testing.T) {
	channels := func(body string) string {
		return "broker:\n  id: t\n" + memStorage + "channels:\n" + body
	}
	for name, tc := range map[string]struct {
		body    string
		wantErr string
	}{
		"a plain filter": {
			"  a:\n    type: append\n    filter: iot/water/+/+/#\n", ""},
		"two channels whose filters cross": {
			"  a:\n    type: latest\n    filter: iot/water/+/location/#\n" +
				"  b:\n    type: append\n    filter: iot/water/+/+/#\n", ""},
		"braces": {
			"  a:\n    type: append\n    filter: iot/weather/+/{data,events}\n", ""},
		"no filter at all, which claims <name>/#": {
			"  a:\n    type: append\n", ""},

		// `#` is the rest, so it can only be the last thing there is.
		"a # that is not last": {
			"  a:\n    type: append\n    filter: iot/#/water\n", "only be the last level"},
		// A wildcard first level claims the broker's own control topics
		// along with everything else.
		"a + at the first level": {
			"  a:\n    type: append\n    filter: +/water/#\n", "first level is spelled out"},
		"a # alone": {
			"  a:\n    type: append\n    filter: \"#\"\n", "first level is spelled out"},
		"the $ space": {
			"  a:\n    type: append\n    filter: $SYS/#\n", "the broker's own"},
		// A wildcard takes a whole level; `a+b` is a literal level holding a
		// character MQTT reserves, and it matches nothing at all.
		"a wildcard inside a level": {
			"  a:\n    type: append\n    filter: iot/wa+ter/#\n", "each take a whole level"},

		// The one tie the ordering rule can be handed, and there is nothing
		// in the file to break it with.
		"two channels with the same filter": {
			"  a:\n    type: append\n    filter: iot/water/#\n" +
				"  b:\n    type: append\n    filter: iot/water/#\n", "both claim the filter"},
		"the same filter after braces expand": {
			"  a:\n    type: append\n    filter: iot/{water,air}/#\n" +
				"  b:\n    type: append\n    filter: iot/air/#\n", "both claim the filter"},

		// Every filter carrying a `__dlq` level is one the broker derived.
		"an operator-written __dlq level": {
			"  a:\n    type: append\n    filter: iot/water/__dlq/#\n", "which is reserved"},

		// A queue's workers pin one exact string, and two strings are two
		// consumer groups that each take a copy of every job.
		"braces in a queue's filter": {
			"  a:\n    type: queue\n    filter: iot/{water,air}/+/work\n",
			"a queue's filter holds no"},

		"a brace that is not a whole level": {
			"  a:\n    type: append\n    filter: iot/x{a,b}/#\n", "a brace is a whole level"},
		"an empty alternative": {
			"  a:\n    type: append\n    filter: iot/{a,}/#\n", "an alternative is empty"},
		"a wildcard inside a brace": {
			"  a:\n    type: append\n    filter: iot/{a,+}/#\n", "holds a wildcard"},

		// **Every rule is asked of what a brace expanded to, not of the
		// brace.** A rule about levels cannot see inside one, so checking
		// the written form leaves exactly one hole per rule: this filter
		// reads as a first level that is spelled out and does not begin
		// with `$`, and expands to a route claiming the broker's own tree.
		"a brace hiding the reserved space": {
			"  a:\n    type: append\n    filter: \"{$SYS,iot}/#\"\n", "the broker's own"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, channels(tc.body)))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("a configuration that should load does not: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted, want a startup error naming %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("the error does not name %q:\n%v", tc.wantErr, err)
			}
		})
	}
}

// RFC 0002 "Validation"
//
// A filter must leave room for a topic to be published under it, or the
// channel claims topics `limits.max_topic_length` would refuse - a channel
// that reads as configured and can never hold a record.
func TestAFilterLongerThanATopicMayBeIsRefused(t *testing.T) {
	long := "iot/" + strings.Repeat("x", 200)
	body := "broker:\n  id: t\n" + memStorage + "  limits:\n    max_topic_length: 64\nchannels:\n" +
		"  a:\n    type: append\n    filter: " + long + "/#\n"
	_, _, err := config.Load(write(t, body))
	if err == nil {
		t.Fatal("a filter longer than max_topic_length was accepted, so the channel claims " +
			"topics no publish could carry")
	}
	if !strings.Contains(err.Error(), "max_topic_length") {
		t.Fatalf("the error does not name the limit it broke:\n%v", err)
	}
}

// RFC 0002 "The operations listener", and broker.mqtt.listen.unix
//
// **The kernel's own refusal does not mention the length.** A socket path
// over the limit fails `bind` with "invalid argument", which reads as a
// malformed address, a permissions problem or a bug - and sends somebody
// checking everything except the number of characters. It cost two
// restarts in one session, on a scratch directory nobody chose the length
// of.
//
// **The limit is asked of the kernel here rather than compared against the
// constant**, because a test that reads the same number the code does
// agrees with the code and proves nothing about the machine. `sun_path` is
// 108 bytes including its terminator; if that ever differs, this fails
// where a mirror of the constant would not.
func TestASocketPathTooLongIsRefusedBeforeTheKernelSaysNothing(t *testing.T) {
	// **Linux only, because the constant it checks is Linux's on purpose.**
	// `sun_path` is 108 bytes here and 104 on macOS, so the kernel's real
	// limit there is 103 and this test would build a 104-character path that
	// `maxSocketPath` correctly accepts for the platform saguin ships on.
	// Lowering the constant to macOS's figure would refuse socket paths that
	// are legal where saguin runs, which is a worse answer than not running
	// this check on a machine it was never about.
	if runtime.GOOS != "linux" {
		t.Skip("maxSocketPath is Linux's sun_path; this machine is " + runtime.GOOS)
	}
	limit := 0
	for n := 100; n <= 120; n++ {
		path := filepath.Join("/tmp", strings.Repeat("s", n-len("/tmp/")))
		if len(path) != n {
			t.Fatalf("built a path of %d characters, wanted %d", len(path), n)
		}
		ln, err := net.Listen("unix", path)
		if err != nil {
			continue
		}
		_ = ln.Close()
		_ = os.Remove(path)
		limit = n
	}
	if limit == 0 {
		t.Skip("no socket path in this range could be bound at all")
	}

	for _, tc := range []struct {
		where, block string
	}{
		{"broker.operations.listen.unix", "  operations:\n    listen:\n      unix:\n        path: "},
		{"broker.mqtt.listen.unix", "  mqtt:\n    listen:\n      unix:\n        path: "},
	} {
		t.Run(tc.where, func(t *testing.T) {
			long := filepath.Join("/tmp", strings.Repeat("s", limit+1-len("/tmp/")))
			cfg := "broker:\n  id: t\n" + memStorage + tc.block + long + "\n"
			if strings.Contains(tc.block, "operations") {
				cfg += "  mqtt:\n    listen:\n      tcp:\n        address: 127.0.0.1:0\n"
			} else {
				cfg += "    allow_anonymous: true\n"
			}
			_, _, err := config.Load(write(t, cfg+oneChannel))
			if err == nil {
				t.Fatalf("a %d-character socket path was accepted, and the kernel "+
					"will refuse it with \"invalid argument\"", len(long))
			}
			for _, want := range []string{"characters", "invalid argument"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q, so it reads like the "+
						"kernel's: %v", want, err)
				}
			}
			// And the longest one that binds is not refused.
			ok := filepath.Join("/tmp", strings.Repeat("s", limit-len("/tmp/")))
			cfg = strings.Replace(cfg, long, ok, 1)
			if _, _, err := config.Load(write(t, cfg+oneChannel)); err != nil &&
				strings.Contains(err.Error(), "characters") {
				t.Errorf("a %d-character path binds but was refused: %v", limit, err)
			}
		})
	}
}

// RFC 0005 "/v1/operations/config".
//
// **No secret reaches the report, and this proves it by knowing what the
// secrets are** rather than by listing the keys that would carry one. A
// list of keys is a thing to forget to update: the schema holds no
// credential today - a password file is a path, an ACL file is a path, and
// a bridge's upstream certificate is three paths - so a test asserting
// "`password` is not a key" passes for ever while saying nothing about the
// key somebody adds next year.
//
// What this does instead is read every file the report names and require
// that none of its contents came back. A key that inlined a credential, or
// a Report that started reading a file rather than naming it, fails here
// whatever it is called.
//
// **It counts what it read.** A sweep that resolved no paths would pass by
// comparing nothing, which is the failure mode of every check shaped like
// this one.
func TestTheConfigurationReportCarriesNoFileContents(t *testing.T) {
	dir := t.TempDir()

	// A real password file, with a real hash in it - the one string in this
	// deployment that must never travel.
	pwPath := filepath.Join(dir, "clients.passwd")
	pf := passwd.New(pwPath)
	if err := pf.Set("device-7", "correct horse battery staple"); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := pf.Save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	aclPath := filepath.Join(dir, "acl.yaml")
	if err := os.WriteFile(aclPath, []byte(
		"roles:\n  sensor:\n    - {channel: events, verbs: [publish]}\n"+
			"users:\n  \"device-*\":\n    roles: [sensor]\n"), 0o600); err != nil {
		t.Fatalf("write the acl: %v", err)
	}

	cfgPath := filepath.Join(dir, "saguin.yaml")
	if err := os.WriteFile(cfgPath, []byte(`broker:
  id: secrets
  mqtt:
    password_file: `+pwPath+`
    acl_file: `+aclPath+`
    listen:
      tcp:
        address: 127.0.0.1:1883
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      mem:
        type: memory
        snapshot_dir: none
channels:
  events:
    type: append
`), 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}

	f, reg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	doc, err := f.Report(reg)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	rendered, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(rendered)

	// The paths themselves are in the report, deliberately - an operator
	// asking what the broker is running wants to know which files it read.
	if !strings.Contains(body, pwPath) {
		t.Errorf("the report does not name the password file at all, so this sweep is not "+
			"looking at the configuration it thinks it is: %s", body)
	}

	read := 0
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		case string:
			if !strings.HasPrefix(x, dir) {
				return
			}
			// **The configuration's own file is not a file it names.** Each
			// channel's configured_in points at it, and the report is a
			// rendering of it, so what is in it is the report's by
			// definition. The files that hold secrets are the ones it
			// names - the password file, the ACL - and those are read.
			if x == cfgPath {
				return
			}
			content, err := os.ReadFile(x)
			if err != nil {
				return // a path to something not written, which is not a leak
			}
			read++
			// **Fields rather than lines**, split on the delimiters a
			// password file uses. Comparing whole lines misses a leak of
			// one field - the digest out of `user:$7$iters$salt$sum` is
			// not the line, and a sweep looking for the line finds
			// nothing and passes. Short fields collide by chance: `roles:`
			// is in the ACL file and could be a key anywhere.
			for _, field := range strings.FieldsFunc(string(content), func(r rune) bool {
				return r == ':' || r == '$' || r == '\n'
			}) {
				field = strings.TrimSpace(field)
				if len(field) < 16 {
					continue
				}
				if strings.Contains(body, field) {
					t.Errorf("the report carries part of %s:\n\t%q\n"+
						"The configuration names files; it must never carry what is in them.",
						x, field)
				}
			}
		}
	}
	walk(doc)

	if read < 2 {
		t.Fatalf("the sweep read %d of the files the report names, which is too few to be "+
			"checking anything - it is passing by comparing nothing", read)
	}
	t.Logf("%d files named by the report read and compared against it", read)
}

// RFC 0002 "Where a reader starts: `start`"
//
// **The key is refused where the question has no meaning**, rather than
// accepted and ignored. A `latest` subscriber is always served current
// state - that is what the type is - and a `queue` has no per-consumer
// position at all, since work is claimed rather than read from a place. A
// key that quietly does nothing on two of the three types is a key somebody
// sets and then trusts.
func TestStartIsAppendOnlyAndOneOfTwoWords(t *testing.T) {
	body := func(s string) string {
		return "broker:\n  id: t\n" + memStorage + "channels:\n" + s
	}
	for name, tc := range map[string]struct{ yaml, want string }{
		"floor on an append channel": {
			"  events:\n    type: append\n    start: floor\n", ""},
		"tail on an append channel": {
			"  events:\n    type: append\n    start: tail\n", ""},
		"absent, which is the default": {
			"  events:\n    type: append\n", ""},
		"a word that is neither": {
			"  events:\n    type: append\n    start: beginning\n",
			`is neither "floor" nor "tail"`},
		"on a latest channel": {
			"  state:\n    type: latest\n    start: tail\n",
			"always serves current state"},
		"on a queue": {
			"  jobs:\n    type: queue\n    start: floor\n",
			"claimed rather than read from a position"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, body(tc.yaml)))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("refused, and it is a valid file: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("accepted, want a finding saying %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}

// The key reaches the channel the broker is handed, which is the hand-off
// nothing else would notice going.
func TestStartTailReachesTheChannel(t *testing.T) {
	_, reg, err := config.Load(write(t, "broker:\n  id: t\n"+memStorage+"channels:\n"+
		"  events:\n    type: append\n    start: tail\n"+
		"  archive:\n    type: append\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c := reg.Get("events"); c == nil || !c.StartAtTail {
		t.Errorf("start: tail did not reach the channel: %+v", c)
	}
	if c := reg.Get("archive"); c == nil || c.StartAtTail {
		t.Errorf("a channel writing no start is not at the tail: %+v", c)
	}
}

// RFC 0002 "Which versions may connect"
//
// **The tour must derive what the door admits, not restate it.** It said
// "MQTT 5 only - a 3.1.1 client is rejected at CONNECT" for a day after the
// gate opened, against its own broker, which answers such a client CONNACK
// Success. Nothing noticed: the guard that makes the RFCs move with the
// gate reads the RFCs, and `make docs` never read the demo's narration.
//
// **This asks for the property rather than for the absence of the old
// sentence.** A check that hunted for "MQTT 5 only" would pass the moment
// somebody wrote the same claim in different words, and would trip over the
// demo's own explanation of why that sentence is gone. What is provable is
// that the line comes from the running broker: the helper is defined, it is
// called, and what it returns is printed. A sentence derived that way
// cannot be wrong about a gate that moved.
func TestTheExampleTourDerivesWhatTheDoorAdmits(t *testing.T) {
	const tour = "../../examples/demo.py"
	raw, err := os.ReadFile(tour)
	if err != nil {
		t.Fatalf("cannot read the tour: %v", err)
	}
	src := string(raw)

	// The name is the thing that can rot: rename the helper and every check
	// below stops matching, so each one is counted rather than assumed.
	const helper = "admitted_protocols"
	if !strings.Contains(src, "def "+helper+"(") {
		t.Fatalf("examples/demo.py defines no %s(): the tour's statement about "+
			"which protocols are admitted has to come from the broker, because "+
			"one written by hand went stale the day the gate moved", helper)
	}
	if !strings.Contains(src, `re.search(r'protocols="([^"]*)"'`) {
		t.Errorf("%s() no longer reads the broker's startup line. That line is "+
			"the only place the gate reports itself, and reading anything else "+
			"is restating the configuration rather than the door", helper)
	}

	// Called, and its answer printed - a helper nothing calls is a helper
	// that guards nothing.
	printed := 0
	for _, line := range strings.Split(src, "\n") {
		if strings.Contains(line, helper+"()") && strings.Contains(line, "note(") {
			printed++
		}
	}
	if printed != 1 {
		t.Errorf("the tour prints %s() on %d lines, want exactly 1: the "+
			"protocols it names must be the ones the broker just reported, and "+
			"neither zero nor several of those", helper, printed)
	}
}

// memStorage is the one provider every configuration must define, for the
// tests whose subject is something else. It goes straight after `id:`.
const memStorage = "  storage:\n    default: mem\n    default_retention_period: none\n" +
	"    default_retention_bytes: none\n    providers:\n      mem:\n        type: memory\n        snapshot_dir: none\n"

// RFC 0002 "Every session's state: `broker.session`" - one store for every
// session, persistent or not, on broker.storage.default where the block says
// nothing.
func TestTheSessionStoreIsConfigured(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
      spare:
        type: memory
        snapshot_dir: none
`
	for _, tc := range []struct{ name, block, want string }{
		{"absent takes the default provider", "", "local"},
		{"an empty block takes the default provider", "  session:\n", "local"},
		{"a provider of its own", "  session:\n    storage: spare\n", "spare"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if got := f.SessionStore(); got != tc.want {
				t.Errorf("session store on %q, want %q", got, tc.want)
			}
		})
	}
	for _, tc := range []struct{ name, block, want string }{
		{"a provider that does not exist", "  session:\n    storage: nowhere\n", "not a defined provider"},
		{"a size bound it does not have", "  session:\n    max_bytes: 8MiB\n", "max_bytes does not apply"},
		// A Node decodes without KnownFields, so this says the strict decode
		// inside SessionBlock is still doing its job.
		{"a typo inside the block", "  session:\n    storrage: spare\n", "storrage"},
		{"not a mapping", "  session: spare\n", "write the keys"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refused with %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// RFC 0002 "Validation": a sqlite provider's max_bytes has to leave room for
// its empty database as well as the reserve, or it opens over its own bound
// holding nothing. A memory provider has no schema, so the same figure is
// accepted there - which is what says the rule is about the file and not a
// general floor raised by accident.
func TestASqliteProviderTooSmallForItsOwnSchemaIsRefused(t *testing.T) {
	const limits = "  limits:\n    max_message_size: 4KiB\n    max_header_bytes: 4KiB\n"
	reserve := int64(8 << 10)
	floor := store.SQLiteEmptyBytes + 2*reserve
	body := func(kind string, maxBytes int64) string {
		where := "        snapshot_dir: none\n"
		if kind == "sqlite" {
			where = "        file_path: /var/lib/saguin/p.db\n"
		}
		return "broker:\n  id: t\n" + limits + "  storage:\n    default: p\n" +
			"    default_retention_period: none\n    default_retention_bytes: none\n" +
			"    providers:\n      p:\n        type: " + kind + "\n" + where +
			fmt.Sprintf("        max_bytes: %d\n", maxBytes) + oneChannel
	}
	for _, tc := range []struct {
		kind    string
		size    int64
		refused bool
	}{
		{"sqlite", floor - 1, true},
		{"sqlite", floor, false},
		{"memory", floor - 1, false},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.kind, tc.size), func(t *testing.T) {
			_, _, err := config.Load(write(t, body(tc.kind, tc.size)))
			refused := err != nil && strings.Contains(err.Error(), "empty database")
			if refused != tc.refused {
				t.Fatalf("refused for the empty database: %v, want %v (%v)", refused, tc.refused, err)
			}
			if !tc.refused && err != nil {
				t.Fatalf("load: %v", err)
			}
		})
	}
}

// TestTheShareBlockIsReadAndRefusedLikeItsSiblings holds the block to the
// contract the stores beside it work to: an absent block is the defaults, a
// duration is taken, and anything unparseable is a finding rather than a
// silent fallback. storage is refused by name, naming where a group's
// backlog is kept now.
func TestTheShareBlockIsReadAndRefusedLikeItsSiblings(t *testing.T) {
	const providers = `
broker:
  id: t
  storage:
    default: local
    default_retention_period: none
    default_retention_bytes: none
    providers:
      local:
        type: memory
        snapshot_dir: none
      spare:
        type: memory
        snapshot_dir: none
`

	for _, tc := range []struct {
		name, block string
		wantExpires time.Duration
	}{
		{"absent takes every default", "", 0},
		{"an empty block takes every default", "  share:\n", 0},
		{"an expiry", "  share:\n    expires_after: 90s\n", 90 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if expires := f.ShareExpiry(); expires != tc.wantExpires {
				t.Errorf("expires_after %s, want %s", expires, tc.wantExpires)
			}
		})
	}

	for _, tc := range []struct {
		name, block, want string
	}{
		{"a provider it names", "  share:\n    storage: spare\n",
			"broker.share.storage is gone. A shared group's backlog is the broadcast log behind the " +
				"group's cursor, kept on the provider broker.session.storage names"},
		{"the provider it would default to", "  share:\n    storage: local\n", "broker.share.storage is gone"},
		{"an unparseable expiry", "  share:\n    expires_after: soon\n", "broker.share.expires_after"},
		{"an unknown key under it", "  share:\n    hold_forever: true\n", "broker.share"},
	} {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			_, _, err := config.Load(write(t, providers+tc.block+oneChannel))
			if err == nil {
				t.Fatalf("%s was accepted; a store an operator cannot have is a refusal, not a "+
					"silent fallback", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal says %q, want it to name %q", err.Error(), tc.want)
			}
		})
	}
}

// RFC 0005 "What the `/v1` routes answer with": the resolved document
// has "every default filled in".
//
// **A default that is resolved but not written back is indistinguishable
// from an absent key.** `/v1/operations/config` and `--check-config
// --output` both render this document, and RFC 0005 sells that route as
// "the resolved values, not the written ones, which is the whole reason to
// ask a running broker". Four keys were resolved into the internal struct
// and never written back, while `write_timeout` and every `max_*` beside
// them were - so a reader could not tell which of the two kinds of absence
// they were looking at, and the CLI viewer reading `min_scrape_interval`
// from that route had to carry the RFC's floor as its own fallback, which
// is a second place for the number to be wrong.
//
// **Asserted as a class rather than a list of four.** The four were found
// one at a time; what stops the fifth is a test that walks every key a
// document is expected to carry, so a default added later without a
// write-back fails here rather than at somebody's dashboard.
func TestEveryDocumentedDefaultIsWrittenIntoTheResolvedDocument(t *testing.T) {
	// A file that says as little as it is allowed to say: everything below
	// is a default rather than something written here.
	path := write(t, `broker:
  id: bare
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
  operations:
    listen:
      tcp:
        address: 127.0.0.1:0
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
`+oneChannel)

	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}

	for _, c := range []struct {
		what string
		got  string
		want string
	}{
		{"broker.log_level", f.Broker.LogLevel, "info"},
		{"broker.limits.write_timeout", f.Broker.Limits.WriteTimeout, "5s"},
		{"broker.limits.session_queue_bytes", f.Broker.Limits.SessionQueueBytes, "1MiB"},
		{"broker.operations.min_scrape_interval", f.Broker.Operations.MinScrapeInterval, "60s"},
	} {
		if c.got == "" {
			t.Errorf("%s is absent from the resolved document: RFC 0005 promises every "+
				"default filled in, and a reader cannot tell a key nobody set from "+
				"one the broker defaulted", c.what)
			continue
		}
		if c.got != c.want {
			t.Errorf("%s resolved to %q, want %q", c.what, c.got, c.want)
		}
	}
}

// The one default that is not a constant: the session queue takes the
// message size where that is the larger, which is RFC 0002's sentence. A
// write-back that ignored the dependency would read 1MiB on a broker whose
// sessions actually hold four.
func TestTheSessionQueueDefaultFollowsTheMessageSize(t *testing.T) {
	path := write(t, `broker:
  id: bigger
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:0
  limits:
    max_message_size: 4MiB
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
`+oneChannel)

	f, _, err := config.Load(path)
	if err != nil {
		t.Fatalf("the test's own configuration does not load: %v", err)
	}
	got := f.Broker.Limits.SessionQueueBytes
	if got != "4MiB" {
		t.Errorf("session_queue_bytes resolved to %q with max_message_size 4MiB, "+
			"want 4MiB: a session that cannot hold one message of the size the "+
			"broker accepts is a bound that refuses what it just took", got)
	}
	// And the resolved number agrees with the document, or the two are a
	// pair of answers rather than one.
	if r := f.Broker.Limits.Resolve(); r.SessionQueueBytes != 4<<20 {
		t.Errorf("Resolve says %d bytes, the document says %q", r.SessionQueueBytes, got)
	}
}

// RFC 0005 "/v1/operations/config": **every channel carries a configured_in
// naming the file it was written in, whatever the file count**. A single-file configuration's channels carried none,
// because only the list form recorded where each came from, so a script
// reading the route met the field on some deployments and not others. And
// the flattened document still comments a channel's file only where there
// are two or more, so a single file reads as it did.
func TestEveryChannelSaysWhichFileItWasWrittenIn(t *testing.T) {
	path := write(t, "broker:\n  id: t\n"+memStorage+`channels:
  events:
    type: append
  state:
    type: latest
`)
	f, reg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	doc, err := f.Report(reg)
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	channels, ok := doc["channels"].(map[string]any)
	if !ok || len(channels) != 2 {
		t.Fatalf("the report holds %d channels, want 2: %v", len(channels), doc["channels"])
	}
	for name, v := range channels {
		c, _ := v.(map[string]any)
		if got, _ := c["configured_in"].(string); got != path {
			t.Errorf("channel %s: configured_in %q, want %q", name, got, path)
		}
	}
	flat, err := f.Flatten(reg)
	if err != nil {
		t.Fatalf("flatten: %v", err)
	}
	if strings.Contains(string(flat), "# from ") {
		t.Errorf("a single-file configuration's flattened document comments where its channels "+
			"came from:\n%s", flat)
	}
}

// RFC 0002 `flush_interval`: how often a sqlite provider forces its
// write-ahead log to disk. Absent is 150ms; 10ms to 1s is taken as written;
// everything else, `none` included, is refused naming the key and the bounds.
func TestTheFlushInterval(t *testing.T) {
	body := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: sqlite\n" +
		"        file_path: /var/lib/s.db\n%s" +
		"\nchannels:\n  events:\n    type: append\n"
	for name, tc := range map[string]struct {
		keys string
		want time.Duration
	}{
		"absent":             {"", 150 * time.Millisecond},
		"the least accepted": {"        flush_interval: 10ms\n", 10 * time.Millisecond},
		"the most accepted":  {"        flush_interval: 1s\n", time.Second},
		"in between":         {"        flush_interval: 40ms\n", 40 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			f, _, err := config.Load(write(t, fmt.Sprintf(body, tc.keys)))
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if got := f.Broker.Storage.FlushIntervals()["p"]; got != tc.want {
				t.Fatalf("flush_interval is %s, want %s", got, tc.want)
			}
		})
	}
	memory := "broker:\n  id: b\n  storage:\n    default: p\n    default_retention_period: none\n" +
		"    default_retention_bytes: none\n    providers:\n      p:\n        type: memory\n" +
		"        snapshot_dir: none\n        flush_interval: 50ms\n" +
		"\nchannels:\n  events:\n    type: append\n"
	for name, tc := range map[string]struct{ body, want string }{
		"9ms":               {fmt.Sprintf(body, "        flush_interval: 9ms\n"), "flush_interval \"9ms\" is shorter than 10ms"},
		"1001ms":            {fmt.Sprintf(body, "        flush_interval: 1001ms\n"), "flush_interval \"1001ms\" is longer than 1s"},
		"none":              {fmt.Sprintf(body, "        flush_interval: none\n"), "flush_interval cannot be none"},
		"not a duration":    {fmt.Sprintf(body, "        flush_interval: often\n"), "flush_interval \"often\""},
		"a memory provider": {memory, "flush_interval belongs to a sqlite provider"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := config.Load(write(t, tc.body))
			if err == nil {
				t.Fatalf("accepted, want a finding saying %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the finding does not say why: %v", err)
			}
		})
	}
}
