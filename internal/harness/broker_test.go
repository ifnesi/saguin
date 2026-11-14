package harness

import (
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A port the harness hands out is never handed out again while its
// reservation holds: a peer listening, or a broker's next reservation, while
// the last 20 brokers' ports are held, gets none of them. Without the
// reservations (ports released as soon as they were chosen), the kernel gave
// 51 peers in 20000 one of the last 20 ports a broker had been given: the
// gap between a broker's ports being chosen and its listeners binding them.
func TestReservedPortsAreHandedOutOnce(t *testing.T) {
	held := map[string]bool{}
	var order []string
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for range 20000 {
		addrs, release, err := reserveAddrs(1)
		if err != nil {
			t.Fatal(err)
		}
		if held[addrs[0]] {
			t.Fatalf("reservation of %s, already held, handed out again", addrs[0])
		}
		held[addrs[0]] = true
		order, releases = append(order, addrs[0]), append(releases, release)
		if len(order) > 20 {
			releases[0]()
			delete(held, order[0])
			order, releases = order[1:], releases[1:]
		}
		p, err := ListenPeer()
		if err != nil {
			t.Fatal(err)
		}
		addr := p.Addr()
		_ = p.Close()
		if held[addr] {
			t.Fatalf("a peer listens on %s, a port reserved for a broker", addr)
		}
	}
}

// Listening takes only the broker's own listener for its: a listener of
// another's that holds the broker's address before the broker binds it is
// never sent a probe, and the broker's failed start is reported. The
// broker runs under a shell that waits, so the other listener takes the
// address first, as a peer given a released port did in a failed run
// (TestBridgeKeepsTheClientsSide): Listening reported the broker
// listening, the other listener received the probe as a link ending
// before any CONNECT, and the broker, exited for the address in use, never
// dialled its own bridge's peer. A broker the shell starts as its child,
// not in its place, is found listening as the shell's.
func TestListeningIsTheBrokersOwn(t *testing.T) {
	const binary = "../../bin/saguin"
	t.Run("another listener on its address", func(t *testing.T) {
		b, err := Start(binary, &Options{Dir: t.TempDir(), Under: []string{"sh", "-c", `sleep 0.5; exec "$@"`, "sh"}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = b.Signal(syscall.SIGKILL); _, _ = b.Wait(time.Second); b.Release() }()
		// Go's listeners set SO_REUSEADDR, as the broker's do: naming the
		// reserved port, this one takes it.
		ln, err := net.Listen("tcp", b.Addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = ln.Close() }()
		probed := make(chan net.Conn, 1)
		go func() {
			if conn, err := ln.Accept(); err == nil {
				probed <- conn
			}
		}()
		err = b.Listening()
		if err == nil || !strings.Contains(err.Error(), "exited before listening") || !strings.Contains(err.Error(), "address already in use") {
			t.Errorf("Listening: %v; want the broker's exit, naming the address in use", err)
		}
		select {
		case conn := <-probed:
			_ = conn.Close()
			t.Error("the other listener received a probe meant for the broker")
		default:
		}
	})
	t.Run("a broker started as a child", func(t *testing.T) {
		b, err := Start(binary, &Options{Dir: t.TempDir(), Under: []string{"sh", "-c", `"$@"; exit $?`, "sh"}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			// The shell does not pass a signal on: its child is killed too.
			for _, pid := range processTree(b.PID()) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
			_, _ = b.Wait(time.Second)
			b.Release()
		}()
		if err := b.Listening(); err != nil {
			t.Fatalf("Listening: %v; want the shell's child found listening", err)
		}
	})
}

// A broker still running when Wait's limit comes may be hung, its core
// serving nobody, as audit finding 261109-001a found one: Wait asks for its
// goroutines' stacks before it kills it. SIGQUIT has Go's runtime write
// every goroutine's stack to the process's standard error, the broker's
// log, and end the process; the error Wait returns carries that log. A
// broker left alone, serving and stopping for nothing, outlasts a short
// limit, and the error holds the runtime's report of the SIGQUIT and the
// core goroutine's stack: a broker launched itself, and one a shell starts
// as its child (Options.Under), as strace starts it, the shell passing no
// signal on.
func TestWaitLimitKeepsTheStacks(t *testing.T) {
	for name, under := range map[string][]string{"launched": nil, "under a wrapper": {"sh", "-c", `"$@"; exit $?`, "sh"}} {
		t.Run(name, func(t *testing.T) {
			b, err := Launch("../../bin/saguin", &Options{Dir: t.TempDir(), Under: under})
			if err != nil {
				t.Fatal(err)
			}
			tree := processTree(b.PID())
			defer func() {
				// What Wait left running, the broker orphaned by its wrapper's
				// end included, is killed: the test is over.
				for _, pid := range tree {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
				b.Release()
			}()
			_, err = b.Wait(100 * time.Millisecond)
			if err == nil {
				t.Fatal("Wait returned no error for a broker that outlasted its limit")
			}
			for _, want := range []string{"exceeded wait limit", "SIGQUIT: quit", "core.(*Core).Run("} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Wait's error, the broker past its limit, holds no %q:\n%s", want, err)
				}
			}
		})
	}
}
