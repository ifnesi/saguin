// Command soak drives N harness clients against a broker for a duration and
// fails on any answer the design or the MQTT standards do not give, or one
// that does not come. With no -addr it starts the broker binary itself
// (harness.StartBroker), with max_connect_rate none, so that the rounds run
// as fast as the broker serves them, never paced by the accept rate. A
// broker that cannot start or a run that completes no round fails. The
// duration must be positive.
//
// The workload covers connections, sessions, pings and takeovers. Each
// client owns one client id and, until
// the time is up, repeats a round: it connects, in MQTT 5 or 3.1.1 by turns,
// some sessions durable, and checks the CONNACK (success, and Session
// Present as design.md, Flows, CONNECT gives it); it pings and checks the
// PINGRESP; on every other round a second connection takes the session over
// (design.md, CONNECT, 5: the old MQTT 5 connection gets DISCONNECT 0x8E, an
// MQTT 3.1.1 one just closes) and pings; the connection left ends with a
// clean DISCONNECT, after which the broker closes it. Every wait is bounded
// by -timeout: an answer that does not arrive in it is lost. The rounds
// exercise neither publishing nor subscribing.
//
// It is a program rather than a test because it runs for minutes against a
// broker it may start, with its size set on the command line. Run it under a
// memory bound.
//
//	go run ./internal/harness/cmd/soak [-addr host:port] [-clients 8] [-duration 30s]
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ifnesi/saguin/internal/harness"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// startSoakBroker starts the broker binary as the soak runs it: config.md's
// defaults, but max_connect_rate none.
func startSoakBroker(binary string) (addr string, stop func() error, err error) {
	return harness.StartBroker(binary, &harness.Options{MaxConnectRate: harness.NoConnectRate})
}

// startBroker is the soak's seam for starting the broker.
var startBroker = startSoakBroker

// run is the whole command, apart from os.Exit, so tests can call it.
func run(args []string, stdout, stderr io.Writer) (code int) {
	flags := flag.NewFlagSet("soak", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", "", "the broker's MQTT address; empty starts -binary")
	binary := flags.String("binary", "bin/saguin", "the broker binary started when -addr is empty")
	clients := flags.Int("clients", 8, "clients at once")
	duration := flags.Duration("duration", 30*time.Second, "how long the clients keep going")
	timeout := flags.Duration("timeout", harness.DefaultTimeout, "the longest wait for one answer")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *clients < 1 {
		harness.Say(stderr, "soak: -clients must be at least 1\n")
		return 2
	}
	if *duration <= 0 {
		harness.Say(stderr, "soak: -duration must be greater than zero\n")
		return 2
	}

	if *addr == "" {
		started, stop, err := startBroker(*binary)
		if err != nil {
			harness.Say(stderr, "soak: starting %s: %v\n", *binary, err)
			return 1
		}
		defer func() {
			if err := stop(); err != nil {
				harness.Say(stderr, "soak: stopping the broker: %v\n", err)
				code = 1
			}
		}()
		*addr = started
	}

	harness.Say(stdout, "soak: %d clients against %s for %s\n", *clients, *addr, *duration)
	rounds, err := soak(*addr, *clients, *duration, *timeout)
	if err != nil {
		harness.Say(stdout, "soak: FAIL after %d rounds: %v\n", rounds, err)
		return 1
	}
	if rounds == 0 {
		harness.Say(stdout, "soak: FAIL: no round completed\n")
		return 1
	}
	harness.Say(stdout, "soak: ok, %d rounds\n", rounds)
	return 0
}

// soak runs the clients until the duration ends or one fails, and returns the
// rounds completed and the first error. Once one fails the others stop at the
// end of their round.
func soak(addr string, clients int, duration, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(duration)
	var rounds atomic.Int64
	var failed atomic.Bool
	var first error
	var once sync.Once
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() {
			err := drive(addr, i, deadline, timeout, &rounds, &failed)
			if err != nil {
				once.Do(func() { first = fmt.Errorf("client %d: %w", i, err) })
				failed.Store(true)
			}
		})
	}
	wg.Wait()
	return rounds.Load(), first
}

// sessionExpiry is the Session Expiry Interval, in seconds, of an MQTT 5
// client's durable session: long enough to outlast the soak.
const sessionExpiry = 3600

// asked is what one CONNECT asks for.
type asked struct {
	version byte
	clean   bool   // Clean Start; Clean Session in MQTT 3.1.1
	expiry  uint32 // MQTT 5's Session Expiry Interval
}

// ask is the CONNECT of client i's connection k (0, or 1 for a taker) in
// round n. The version changes from one connection to the next; round 0
// starts clean, so a session an earlier run left is ended; after that,
// Clean Start and the expiry vary on cycles of their own, so every pairing
// comes round.
func ask(i, n, k int) asked {
	a := asked{version: harness.V5, clean: n == 0 || (n+k)%3 == 0}
	if (i+n+k)%2 == 1 {
		a.version = harness.V311
	}
	if a.version == harness.V5 && (n+k)%2 == 0 {
		a.expiry = sessionExpiry
	}
	return a
}

// durable reports whether the session outlasts its connection (design.md,
// Words: "Durable when its expiry is above zero"): MQTT 5's Session Expiry
// Interval, or MQTT 3.1.1's Clean Session 0, which takes
// max_session_expiry (config.md).
func (a asked) durable() bool {
	if a.version == harness.V311 {
		return !a.clean
	}
	return a.expiry > 0
}

// packet is the CONNECT for client id.
func (a asked) packet(id string) *harness.Connect {
	p := &harness.Connect{ClientID: id, CleanStart: a.clean, KeepAlive: 60}
	if a.expiry > 0 {
		p.Props = harness.Properties{{ID: harness.SessionExpiryInterval, Int: a.expiry}}
	}
	return p
}

// drive is one client's life: rounds until the deadline or a failure
// elsewhere.
func drive(addr string, i int, deadline time.Time, timeout time.Duration, rounds *atomic.Int64, failed *atomic.Bool) error {
	id := fmt.Sprintf("soak-%d", i)
	kept := false // a session exists for id, away
	for n := 0; time.Now().Before(deadline) && !failed.Load(); n++ {
		var err error
		if kept, err = round(addr, id, i, n, kept, timeout); err != nil {
			return fmt.Errorf("round %d: %w", n, err)
		}
		rounds.Add(1)
	}
	return nil
}

// round is one round of client i, whose id has a session already where
// kept; it returns whether one remains after the round.
func round(addr, id string, i, n int, kept bool, timeout time.Duration) (bool, error) {
	a := ask(i, n, 0)
	c, err := connect(addr, id, a, kept, timeout)
	if err != nil {
		return false, err
	}
	// The round's outcome is decided by what the broker sent; a failed
	// close changes nothing.
	defer func() { _ = c.Close() }()
	if err := c.Ping(); err != nil {
		return false, err
	}
	last := c // the connection the round ends with a DISCONNECT
	if n%2 == 1 {
		// design.md, CONNECT: the old connection's session exists for
		// the taker's Session Present when its expiry is above 0; "A takeover
		// of a session whose expiry is 0 ends it and starts a new one".
		b := ask(i, n, 1)
		taker, err := connect(addr, id, b, a.durable(), timeout)
		if err != nil {
			return false, fmt.Errorf("takeover: %w", err)
		}
		defer func() { _ = taker.Close() }()
		if err := takenOver(c); err != nil {
			return false, err
		}
		if err := taker.Ping(); err != nil {
			return false, fmt.Errorf("takeover: %w", err)
		}
		a, last = b, taker
	}
	if err := last.Send(&harness.Disconnect{}); err != nil {
		return false, fmt.Errorf("DISCONNECT: %w", err)
	}
	if err := last.AwaitClose(); err != nil {
		return false, fmt.Errorf("after DISCONNECT: %w", err)
	}
	// design.md, DISCONNECT: a clean DISCONNECT keeps a durable session
	// away; MQTT 3.1.1's Clean Session 1, or MQTT 5's expiry 0, ends it.
	return a.durable(), nil
}

// connect dials addr and connects as id with a, and checks the CONNACK:
// success, and Session Present 1 exactly when the CONNECT keeps its session
// and one it can keep exists for the client id (design.md, Flows, CONNECT).
func connect(addr, id string, a asked, exists bool, timeout time.Duration) (*harness.Client, error) {
	c, err := harness.Dial(addr, a.version)
	if err != nil {
		return nil, err
	}
	c.Timeout = timeout
	ack, err := c.Connect(a.packet(id))
	if err == nil && (ack.Reason != 0 || ack.SessionPresent != (!a.clean && exists)) {
		err = fmt.Errorf("CONNACK 0x%02X, Session Present %v; want 0x00, Session Present %v", ack.Reason, ack.SessionPresent, !a.clean && exists)
	}
	if err != nil {
		_ = c.Close() // the connection failed; nothing more to learn from it
		return nil, fmt.Errorf("CONNECT %s, clean %v, expiry %d: %w", harness.VersionName(a.version), a.clean, a.expiry, err)
	}
	return c, nil
}

// takenOver checks the old connection of a takeover (design.md, CONNECT,
// 5): DISCONNECT 0x8E on MQTT 5, then the close; MQTT 3.1.1, the close
// alone.
func takenOver(c *harness.Client) error {
	if c.Version == harness.V5 {
		p, err := c.Recv()
		if err != nil {
			return fmt.Errorf("the old connection, waiting for DISCONNECT 0x8E: %w", err)
		}
		if d, ok := p.(*harness.Disconnect); !ok || d.Reason != 0x8E {
			return fmt.Errorf("the old connection got %s %+v, want DISCONNECT 0x8E", harness.Name(p.Type()), p)
		}
	}
	if err := c.AwaitClose(); err != nil {
		return fmt.Errorf("the old connection: %w", err)
	}
	return nil
}
