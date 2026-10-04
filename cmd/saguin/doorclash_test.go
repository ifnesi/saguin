package main

// RFC 0002 "Several listeners of a kind": --check-config and a plain start
// share one validation (config.Load), so a clashing configuration is
// refused identically either way and a real start never binds any of its
// doors - not "binds the doors that didn't clash". Driven through the
// binary because that is the guarantee at risk: two packages could agree
// on the finding and still let main skip it on the path that does not
// pass --check-config.

import (
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runBinary runs bin with args to completion and returns its exit code,
// stdout and stderr.
func runBinary(t *testing.T, bin string, args ...string) (exit int, out, errOut string) {
	t.Helper()
	var o, e strings.Builder
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = &o, &e
	err := cmd.Run()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode(), o.String(), e.String()
	}
	if err != nil {
		t.Fatalf("running %v: %v", args, err)
	}
	return 0, o.String(), e.String()
}

// A configuration with two tcp doors on the same host and port is refused
// the same way by --check-config and by a plain start - same exit code,
// same message - and a plain start never opens either port: the clash is
// caught before the first listener binds, not after the first one already
// has.
func TestStartupRefusesTheSameDoorClashCheckConfigDoes(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	addr := freeAddr(t)
	dir := t.TempDir()
	cfg := filepath.Join(dir, "saguin.yaml")
	writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n      tcp:\n"+
		"        - name: a\n          address: "+addr+"\n"+
		"        - name: b\n          address: "+addr+"\nchannels:\n  events:\n    type: append\n"))

	checkExit, checkOut, checkErr := runBinary(t, bin, "--check-config", cfg)
	if checkExit == 0 {
		t.Fatalf("--check-config accepted a port clash: %s%s", checkOut, checkErr)
	}
	if !strings.Contains(checkErr, "the same port") {
		t.Fatalf("--check-config's refusal does not name the clash: %s", checkErr)
	}

	startExit, startOut, startErr := runBinary(t, bin, "--config", cfg)
	if startExit == 0 {
		t.Fatalf("a plain start accepted a port clash: %s%s", startOut, startErr)
	}
	if !strings.Contains(startErr, "the same port") {
		t.Fatalf("a plain start's refusal does not name the clash: %s", startErr)
	}
	if startExit != checkExit {
		t.Errorf("a plain start exited %d, --check-config exited %d for the same file",
			startExit, checkExit)
	}
	if startErr != checkErr {
		t.Errorf("a plain start and --check-config disagree about the finding:\n"+
			"start: %s\ncheck: %s", startErr, checkErr)
	}

	// Neither door opened: the clash is caught before the first listener
	// binds, so a connection to the address they both named is refused
	// rather than answered by whichever door happened to bind first.
	if conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		conn.Close()
		t.Error("a broker refused to start still answered on the clashing address - " +
			"it bound at least one of the two doors before the clash was caught")
	}
}

// **--check-config and a plain start give one verdict on a door
// configuration** (RFC 0002 "Several listeners of a kind"). Each case is
// run through the real binary both ways: a start that is still serving
// after startWait accepted the file, one that exited refused it. The two
// must agree, and must agree with `refused`, so a case cannot pass by both
// being wrong the same way. A name-less single door in a list is named after
// its kind, equivalent socket paths and equivalent IP literals are one path
// and one host; `localhost` against 127.0.0.1 is left to the bind (no DNS at
// check time), so it is not here.
func TestCheckConfigAndStartupAgreeOnDoors(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "saguin")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Skipf("main's wiring is UNCHECKED: the binary would not build here: %v\n%s", err, out)
	}
	const anon = "          allow_anonymous: true\n"
	const anon8 = "        allow_anonymous: true\n"
	for _, tc := range []struct {
		name    string
		listen  func(dir string, a, b string) string // the broker.mqtt.listen body
		refused bool
	}{
		{"unnamed single tcp list beside unnamed single ws list", func(_, a, b string) string {
			return "      tcp:\n        - address: " + a + "\n" + anon +
				"      ws:\n        - address: " + b + "\n" + anon
		}, false},
		{"unnamed single unix list beside unnamed single ws list", func(d, _, b string) string {
			return "      unix:\n        - path: " + d + "/r.sock\n" + anon +
				"      ws:\n        - address: " + b + "\n" + anon
		}, false},
		{"unnamed single tcp list beside a ws door named tcp", func(_, a, b string) string {
			return "      tcp:\n        - address: " + a + "\n" + anon +
				"      ws:\n        - name: tcp\n          address: " + b + "\n" + anon
		}, true},
		{"unix paths, one with ./", func(d, _, _ string) string {
			return "      unix:\n        - name: a\n          path: " + d + "/r.sock\n" + anon +
				"        - name: b\n          path: " + d + "/./r.sock\n" + anon
		}, true},
		{"unix paths, one with ..", func(d, _, _ string) string {
			return "      unix:\n        - name: a\n          path: " + d + "/r.sock\n" + anon +
				"        - name: b\n          path: " + d + "/x/../r.sock\n" + anon
		}, true},
		{"unix paths, doubled slash", func(d, _, _ string) string {
			return "      unix:\n        - name: a\n          path: " + d + "/r.sock\n" + anon +
				"        - name: b\n          path: " + d + "//r.sock\n" + anon
		}, true},
		{"control: unix paths, different files", func(d, _, _ string) string {
			return "      unix:\n        - name: a\n          path: " + d + "/r.sock\n" + anon +
				"        - name: b\n          path: " + d + "/./q.sock\n" + anon
		}, false},
		{"tcp hosts, IPv4-mapped IPv6 against IPv4", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: \"[::ffff:127.0.0.1]:" + port + "\"\n" + anon +
				"        - name: b\n          address: 127.0.0.1:" + port + "\n" + anon
		}, true},
		{"tcp hosts, expanded IPv6 loopback against ::1", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: \"[0:0:0:0:0:0:0:1]:" + port + "\"\n" + anon +
				"        - name: b\n          address: \"[::1]:" + port + "\"\n" + anon
		}, true},
		{"tcp hosts, [::0] wildcard against 127.0.0.1", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: \"[::0]:" + port + "\"\n" + anon +
				"        - name: b\n          address: 127.0.0.1:" + port + "\n" + anon
		}, true},
		{"tcp port written with a leading zero", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: 127.0.0.1:0" + port + "\n" + anon +
				"        - name: b\n          address: 127.0.0.1:" + port + "\n" + anon
		}, true},
		{"tcp port written with a +", func(_, a, b string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: 127.0.0.1:" + port + "\n" + anon8 +
				"      ws:\n        address: 127.0.0.1:+" + port + "\n" + anon8
		}, true},
		{"tcp address with no port", func(_, _, _ string) string {
			return "      tcp:\n        address: 127.0.0.1\n" + anon8
		}, true},
		{"ws address with no port", func(_, _, _ string) string {
			return "      ws:\n        address: 127.0.0.1\n" + anon8
		}, true},
		{"tcp port above 65535", func(_, _, _ string) string {
			return "      tcp:\n        address: 127.0.0.1:65536\n" + anon8
		}, true},
		{"tcp bracketless IPv6", func(_, _, _ string) string {
			return "      tcp:\n        address: \"::1:1883\"\n" + anon8
		}, true},
		{"unix path ending in a slash", func(d, _, _ string) string {
			return "      unix:\n        path: " + d + "/\n" + anon8
		}, true},
		{"unix path ending in /.", func(d, _, _ string) string {
			return "      unix:\n        path: " + d + "/.\n" + anon8
		}, true},
		{"unix path ending in /..", func(d, _, _ string) string {
			return "      unix:\n        path: " + d + "/..\n" + anon8
		}, true},
		{"IPv6 zone on ::1 beside the same address without one", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: \"[::1%lo]:" + port + "\"\n" + anon +
				"        - name: b\n          address: \"[::1]:" + port + "\"\n" + anon
		}, true},
		{"IPv6 wildcard with a zone beside ::1", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: \"[::%lo]:" + port + "\"\n" + anon +
				"        - name: b\n          address: \"[::1]:" + port + "\"\n" + anon
		}, true},
		{"IPv6 host with an empty zone", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: \"[::1%]:" + port + "\"\n" + anon8
		}, true},
		{"host with a vertical tab", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: \"127.0.0.1\\v:" + port + "\"\n" + anon8
		}, true},
		{"IPv6 link-local host with no zone", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: \"[fe80::1]:" + port + "\"\n" + anon8
		}, true},
		{"IPv6 multicast host", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: \"[ff02::1%lo]:" + port + "\"\n" + anon8
		}, true},
		{"control: IPv6 zone on ::1 alone", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: \"[::1%lo]:" + port + "\"\n" + anon8
		}, false},
		{"unix path with a NUL", func(d, _, _ string) string {
			return "      unix:\n        path: \"" + d + "/bad\\0.sock\"\n" + anon8
		}, true},
		{"control: tcp port written with a leading zero, alone", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        address: 127.0.0.1:0" + port + "\n" + anon8
		}, false},
		{"control: tcp hosts, 127.0.0.1 against 127.0.0.2", func(_, a, _ string) string {
			_, port, _ := net.SplitHostPort(a)
			return "      tcp:\n        - name: a\n          address: 127.0.0.2:" + port + "\n" + anon +
				"        - name: b\n          address: 127.0.0.1:" + port + "\n" + anon
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "sg")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(dir) })
			if err := os.MkdirAll(filepath.Join(dir, "x"), 0o755); err != nil {
				t.Fatal(err)
			}
			listen := tc.listen(dir, freeAddr(t), freeAddr(t))
			// **127.0.0.2 is a loopback address only where the host routes
			// it**: Linux routes all of 127/8 to lo, macOS gives lo0
			// 127.0.0.1 alone. There the bind fails and the start is
			// refused for the host's reason rather than the configuration's,
			// so the case says nothing about the two verdicts agreeing.
			if strings.Contains(listen, "127.0.0.2:") {
				if l, err := net.Listen("tcp", "127.0.0.2:0"); err != nil {
					t.Skipf("this host cannot bind 127.0.0.2 (%v), so a start is refused for the "+
						"host's reason, not the configuration's", err)
				} else {
					_ = l.Close()
				}
			}
			cfg := filepath.Join(dir, "saguin.yaml")
			writeFile(t, cfg, []byte("broker:\n  id: t\n"+memStorage+"  mqtt:\n    listen:\n"+
				listen+"channels:\n  events:\n    type: append\n"))

			checkExit, _, checkErr := runBinary(t, bin, "--check-config", cfg)
			checkRefused := checkExit != 0

			// **A start's verdict is what it says, not how long it lives.** A
			// refused start exits; an accepted one says "saguin listening",
			// which it writes once every door is bound. Read as "still
			// running after two seconds", a refusal slower than that on a
			// busy machine read as an acceptance.
			run := exec.Command(bin, "--config", cfg)
			var out lockedLog
			run.Stdout, run.Stderr = &out, &out
			if err := run.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- run.Wait() }()
			startRefused := false
			for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				select {
				case <-done:
					startRefused = true
				default:
				}
				if startRefused {
					break
				}
				if strings.Contains(out.String(), "msg=\"saguin listening\"") {
					_ = run.Process.Kill()
					<-done
					break
				}
				if time.Now().After(deadline) {
					_ = run.Process.Kill()
					<-done
					t.Fatalf("a start neither exited nor said it was listening in 30s:\n%s", out.String())
				}
			}

			if checkRefused != startRefused {
				t.Errorf("--check-config refused=%v, a plain start refused=%v\ncheck: %s\nstart: %s",
					checkRefused, startRefused, checkErr, out.String())
			}
			if startRefused != tc.refused {
				t.Errorf("a plain start refused=%v, want %v\n%s", startRefused, tc.refused, out.String())
			}
		})
	}
}
