package connection_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/connection"
	"github.com/ifnesi/saguin/internal/harness"
	"github.com/ifnesi/saguin/internal/metrics"
	"github.com/ifnesi/saguin/internal/packet"
	"github.com/ifnesi/saguin/internal/testkit"
)

// idleConnections is how many connections the probe leaves idle, and
// bigPacket the size of the packets each has read and written before, and
// of the packets left unread behind each refused CONNECT.
const (
	idleConnections = 32
	bigPacket       = 60 << 10
)

// design.md, Flows, Packet buffers: "Between packets, a connection holds no
// read or write buffer." 0005a, 261012: "between packets a connection holds
// no read or write buffer". A buffer is a slice: the probe, a run of the
// test binary of its own with every allocation recorded, opens connections
// that each read a CONNECT and write a CONNACK of 60 KiB, then a PINGREQ
// and its PINGRESP, and leaves them idle, the core's stand-in keeping
// nothing. It then counts the slices still live that the connections' code
// allocated, itself or through the packet codecs and the buffer helpers of
// the standard library (bufio, bytes, slices, io, strings), against the
// same count before the first connection: none may be left, of any size.
// Before the idle ones, as many connections are refused with 60 KiB of
// PINGREQs behind their CONNECT, which the writer's orderly close reads and
// discards (audits 261012-002a), and are gone before the count: whatever
// the close read into, none may keep it. A slice the standard library's
// network code allocates on its own account is not the connection's buffer,
// and is not counted.
//
// The check reads the memory profile of a build with the race detector,
// which records every allocation on its own and through the slice
// allocators that make it (audits 261012-003a); without it, Go's tiny
// allocator packs pointer-free allocations under 16 bytes into shared
// blocks and records only a block's first, and the compiler turns a make
// followed by a copy into a bare allocation, as it does a struct, so the
// test skips there. The profile records no types: past the allocator's own
// frames, a slice's array is the allocation whose stack starts at one of
// the runtime's slice allocators, and the connection's own structs,
// channels and goroutines are not. The probe first checks this on the
// allocation forms it makes itself (checkProfile), not on every form a
// future compiler may produce, and fails when it does not hold.
func TestIdleConnectionsHoldNoBuffer(t *testing.T) {
	if !raceEnabled {
		t.Skip("R6's idle-buffer check reads a race-build memory profile, as make ci runs it: " +
			"go test -race -run TestIdleConnectionsHoldNoBuffer ./internal/connection/")
	}
	t.Parallel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^$")
	cmd.Env = append(os.Environ(), idleProbe+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the probe: %v\n%s", err, out)
	}
}

// probeIdle runs the probe: 0 when the idle connections hold no slice, 1
// when they do, listing each place that allocated one, and 2 when the probe
// cannot run, its memory profile failing checkProfile included.
func probeIdle() int {
	if why := checkProfile(); why != "" {
		fmt.Println("the memory profile cannot tell a slice's array from other allocations:", why)
		return 2
	}
	cfg := newConfig(&probeTB{})
	cfg.Broker.Limits.MaxConnectSize = 2 * bigPacket
	reg := metrics.New()
	m := connection.NewMetrics(reg)
	inbound := make(chan connection.Event, 64)
	l, err := connection.Listen(cfg, &cfg.Broker.MQTT.Listen.TCP[0], inbound, connection.NewPermits(cfg.Broker.Limits.MaxConnections), m)
	if err != nil {
		fmt.Println("Listen:", err)
		return 2
	}
	go func() { _ = l.Run() }() // the probe exits with the listener running

	// The core's stand-in answers and keeps nothing; one CONNACK, never
	// changed, serves every connection it accepts.
	connack := &packet.Connack{Properties: props(packet.CONNACK, packet.Property{ID: packet.UserProperty,
		Pair: packet.StringPair{Name: "u", Value: strings.Repeat("v", bigPacket)}})}
	go func() {
		for ev := range inbound {
			switch {
			case ev.Kind == connection.Ended:
				ev.Conn.Replies() <- connection.Close{}
			case ev.Kind != connection.Received:
			case ev.Packet == connection.Pingreq{}:
				ev.Conn.Replies() <- connection.Pingresp{}
			case ev.Reason != 0:
				ev.Conn.Replies() <- &packet.Connack{ReasonCode: ev.Reason}
			default:
				ev.Conn.Replies() <- connack
			}
		}
	}()

	before := liveSlices(connectionCode)
	if code := refuseWithBytesUnread(l.Addr().String(), reg); code != 0 {
		return code
	}
	clients := make([]*harness.Client, idleConnections)
	password := bytes.Repeat([]byte{'p'}, bigPacket)
	for i := range clients {
		c, err := harness.Dial(l.Addr().String(), harness.V5)
		if err != nil {
			fmt.Println("dial:", err)
			return 2
		}
		c.Timeout = backstop
		ack, err := c.Connect(&harness.Connect{CleanStart: true, ClientID: fmt.Sprint("idle-", i), HasPassword: true, Password: password})
		if err != nil || ack.Reason != 0 {
			fmt.Println("CONNECT:", ack, err)
			return 2
		}
		if err := c.Send(&harness.Pingreq{}); err != nil {
			fmt.Println("PINGREQ:", err)
			return 2
		}
		if p, err := c.Recv(); err != nil || p.Type() != harness.PINGRESP {
			fmt.Println("PINGRESP:", p, err)
			return 2
		}
		clients[i] = c
	}
	after := liveSlices(connectionCode)
	runtime.KeepAlive(clients)

	var held []string
	total := int64(0)
	for site, h := range after {
		if grown := h.bytes - before[site].bytes; grown > 0 {
			held = append(held, fmt.Sprintf("%d bytes: %s", grown, site))
			total += grown
		}
	}
	if total == 0 {
		return 0
	}
	slices.Sort(held)
	fmt.Printf("%d idle connections, after %d refused and gone, hold %d bytes of slices, %d each:\n%s\n",
		idleConnections, idleConnections, total, total/idleConnections, strings.Join(held, "\n"))
	return 1
}

// refuseWithBytesUnread opens idleConnections connections to addr, each
// sending a CONNECT the reader refuses (an empty client id with Clean Start
// 0: 0x85) with bigPacket bytes of PINGREQs behind it in the same write,
// reads its CONNACK and the close, and hangs up. It returns
// once every socket is closed (saguin_connections_open is 0), or 2 when it
// cannot run.
func refuseWithBytesUnread(addr string, reg *metrics.Registry) int {
	tb := &probeTB{}
	wire := harness.Encoded(tb, &harness.Connect{ProtocolName: "MQTT", Version: harness.V5}, harness.V5)
	wire = append(wire, bytes.Repeat(harness.Encoded(tb, &harness.Pingreq{}, harness.V5), bigPacket/2)...)
	for range idleConnections {
		c, err := harness.Dial(addr, harness.V5)
		if err != nil {
			fmt.Println("dial:", err)
			return 2
		}
		c.Timeout = backstop
		if err := c.SendRaw(wire); err != nil {
			fmt.Println("refused CONNECT:", err)
			return 2
		}
		if p, err := c.Recv(); err != nil || p.Type() != harness.CONNACK {
			fmt.Println("CONNACK:", p, err)
			return 2
		}
		_, _ = c.Recv() // the close, nothing after the CONNACK
		_ = c.Close()
	}
	testkit.AwaitSample(tb, reg, "saguin_connections_open", 0, backstop)
	return 0
}

// sliceAllocators are the runtime's functions that allocate a slice's
// array: for make, append, a make followed by a copy, and the buffers of
// bytes and strings.
var sliceAllocators = []string{"runtime.makeslice", "runtime.growslice", "runtime.makeslicecopy", "internal/bytealg.MakeNoZero"}

// onBehalf are the packages whose allocations, made for an owner's code,
// are the owner's buffers.
var onBehalf = []string{"bufio.", "bytes.", "slices.", "io.", "strings.", module + "internal/packet."}

// connectionCode names the connection's functions, the owner the probe
// judges, and calibration the probe's own heldSlices, heldTiny and
// heldOthers, which checkProfile judges.
const (
	connectionCode = module + "internal/connection."
	calibration    = module + "internal/connection_test.held"
)

// inUse is what the live slices allocated at one place take.
type inUse struct{ objects, bytes int64 }

// liveSlices returns, by allocation stack, the slices still live that the
// functions named with the owner prefix allocated, themselves or through
// onBehalf.
func liveSlices(owner string) map[string]inUse {
	// MemProfile reports what the collector has had the chance to free:
	// up to two cycles old.
	for range 3 {
		runtime.GC()
	}
	var records []runtime.MemProfileRecord
	for {
		n, ok := runtime.MemProfile(records, false)
		if ok {
			records = records[:n]
			break
		}
		records = make([]runtime.MemProfileRecord, n+64)
	}
	live := map[string]inUse{}
	for i := range records {
		r := &records[i]
		if site, ok := ownedSlice(r.Stack(), owner); ok {
			u := live[site]
			u.objects += r.InUseObjects()
			u.bytes += r.InUseBytes()
			live[site] = u
		}
	}
	return live
}

// ownedSlice reports whether an allocation stack, innermost first, is a
// slice's array that the owner's code allocated, and names it. Past the
// allocator's own frames (runtime.mallocgc), the innermost frame says what
// was allocated: one of sliceAllocators for a slice's array; anything else
// for a struct, a channel, a goroutine or the runtime's own bookkeeping,
// even when a slice allocation further out set it off.
func ownedSlice(stack []uintptr, owner string) (string, bool) {
	var names []string
	frames := runtime.CallersFrames(stack)
	for more := true; more; {
		var f runtime.Frame
		f, more = frames.Next()
		names = append(names, f.Function)
	}
	allocator := slices.IndexFunc(names, func(name string) bool { return !strings.HasPrefix(name, "runtime.mallocgc") })
	if allocator < 0 || !slices.Contains(sliceAllocators, names[allocator]) {
		return "", false
	}
	for i := allocator + 1; i < len(names); i++ {
		switch {
		case strings.HasPrefix(names[i], owner):
			return strings.Join(names[:i+1], " < "), true
		case !slices.ContainsFunc(onBehalf, func(p string) bool { return strings.HasPrefix(names[i], p) }):
			return "", false
		}
	}
	return "", false
}

// checkProfile proves, in the build at hand, what ownedSlice relies on:
// each slice's array heldSlices and heldTiny make has a record of its own
// naming a slice allocator, and none of heldOthers' allocations has. It
// returns how the profile fails, or "".
func checkProfile() string {
	arrays, tiny, others := heldSlices(1), heldTiny(1), heldOthers(1)
	live := liveSlices(calibration)
	runtime.KeepAlive(arrays)
	runtime.KeepAlive(tiny)
	runtime.KeepAlive(others)
	found := int64(0)
	var sites []string
	for site, u := range live {
		if strings.HasSuffix(site, ".heldOthers") {
			return "a struct or a channel counted as a slice: " + site
		}
		found += u.objects
		sites = append(sites, fmt.Sprintf("%d arrays, %d bytes: %s", u.objects, u.bytes, site))
	}
	if want := int64(calibrationArrays + len(tiny)); found != want {
		slices.Sort(sites)
		return fmt.Sprintf("%d of the %d arrays made found:\n%s", found, want, strings.Join(sites, "\n"))
	}
	return ""
}

// calibrationSlices holds heldSlices' allocations: calibrationArrays
// slices' arrays, and the structs of bytes.Buffer, strings.Builder,
// bufio.Reader and bufio.Writer, which are not.
type calibrationSlices struct {
	small, large, grown, copied []byte
	buffer                      *bytes.Buffer
	builder                     *strings.Builder
	reader                      *bufio.Reader
	writer                      *bufio.Writer
}

const calibrationArrays = 8

// heldSlices makes a slice's array in each way the connection's code could
// make a buffer: make, small and of a large object's size, append, make
// followed by copy, and the buffers of bytes.Buffer, strings.Builder,
// bufio.Reader and bufio.Writer. n is 1, out of the compiler's sight, so
// that each size is variable, as a packet's is.
//
//go:noinline
func heldSlices(n int) calibrationSlices {
	small := make([]byte, n<<5)
	copied := make([]byte, len(small))
	copy(copied, small)
	buffer, builder := new(bytes.Buffer), new(strings.Builder)
	buffer.Grow(n << 12)
	builder.Grow(n << 5)
	return calibrationSlices{
		small: small, large: make([]byte, n<<16), grown: append([]byte(nil), small...), copied: copied,
		buffer: buffer, builder: builder,
		reader: bufio.NewReaderSize(nil, n<<5), writer: bufio.NewWriterSize(nil, n<<5),
	}
}

// heldTiny makes four slices of n bytes, one after the other, which the
// tiny allocator, where it is on, packs into one block.
//
//go:noinline
func heldTiny(n int) [4][]byte {
	return [4][]byte{make([]byte, n), make([]byte, n), make([]byte, n), make([]byte, n)}
}

// calibrationOthers holds heldOthers' allocations, none a slice's array,
// as the connection's own are not: a struct holding pointers, a struct of
// scalars and a buffered channel.
type calibrationOthers struct {
	pointers *calibrationOthers
	scalars  *struct{ a, b int64 }
	queue    chan any
}

// heldOthers makes allocations that are not slices' arrays; n is 1, as for
// heldSlices.
//
//go:noinline
func heldOthers(n int) calibrationOthers {
	return calibrationOthers{
		pointers: &calibrationOthers{}, scalars: &struct{ a, b int64 }{int64(n), 0}, queue: make(chan any, n),
	}
}

// probeTB stands for a testing.TB in the probe, which runs outside a test:
// newConfig's failure stops the probe.
type probeTB struct{ testing.TB }

func (*probeTB) Helper() {}

func (*probeTB) Fatalf(format string, args ...any) {
	fmt.Printf(format+"\n", args...)
	os.Exit(2)
}
