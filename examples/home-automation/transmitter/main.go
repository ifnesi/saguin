// Command rtltcp-emu answers, on a TCP socket, the part of the rtl_tcp
// protocol that rtl_433 needs to believe it has a radio - and nothing else.
//
// rtl_433 takes its samples from a USB dongle or, because people put dongles
// on the network, from an rtl_tcp server. That makes the radio a swappable
// back end, the same way zigbee2mqtt's serial-over-TCP does for the Zigbee
// half of this demo: this program answers where a real dongle would, so the
// demo runs with no radio hardware, and pointing rtl_433 at a real rtl_tcp
// server instead changes an address and nothing else.
//
// **What goes over the socket is radio, not readings.** The devices in
// devices.go are modulated into raw samples here, and rtl_433 demodulates
// and decodes them with no idea anything is emulated. Nothing in this
// program speaks MQTT, which is the point: every topic the broker ends up
// holding got there because a real gateway decoded a real waveform.
package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// The sample format is cu8: unsigned 8-bit I and Q interleaved, 128 being
// the midpoint. rtl_433's AM path takes the magnitude of each pair, so a
// carrier is any steady offset from the midpoint and silence is the
// midpoint itself.
const (
	sampleRate = 250000 // what rtl_433 defaults to at 433.92 MHz
	midpoint   = 128
	carrier    = 90 // magnitude of a mark: about 25 dB over the noise below
)

// **The noise floor is not silence, and that is not decoration.** A
// mathematically flat floor gives the demodulator nothing to estimate a
// threshold against, and rtl_433 reports no pulses at all: not a failed
// decode, no signal. Measured while building this: a floor of exactly 128
// produced an empty run, and +/-6 counts of noise produced a clean 25.6 dB
// SNR and a correct decode of the same waveform.
const noise = 6

// rtl_tcp's greeting: the magic, the tuner type and how many gain steps it
// has. rtl_433 reads these twelve bytes and then expects samples for ever.
// The values say R820T, which is what most dongles are.
func greeting() []byte {
	b := make([]byte, 12)
	copy(b, "RTL0")
	binary.BigEndian.PutUint32(b[4:], 5)
	binary.BigEndian.PutUint32(b[8:], 29)
	return b
}

// modulator renders on-off keying into cu8.
//
// Every device here is OOK with pulse-position coding: a mark of fixed
// width, then a gap whose length carries the bit. That is what the cheap
// 433 MHz sensors in a garden actually send.
type modulator struct {
	rng *rand.Rand
	buf []byte
}

// emit appends d worth of samples at the given magnitude: `carrier` for a
// mark, zero for a gap.
func (m *modulator) emit(d time.Duration, amp int) {
	n := int(float64(sampleRate) * d.Seconds())
	for i := 0; i < n; i++ {
		iq := midpoint + amp + m.rng.Intn(2*noise+1) - noise
		q := midpoint + m.rng.Intn(2*noise+1) - noise
		m.buf = append(m.buf, clamp(iq), clamp(q))
	}
}

func clamp(v int) byte {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return byte(v)
}

func (m *modulator) take() []byte {
	out := m.buf
	m.buf = nil
	return out
}

func main() {
	addr := flag.String("listen", ":1234", "address to serve rtl_tcp on")
	every := flag.Duration("report", 30*time.Second, "how often each device transmits")
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	log.Printf("rtl_tcp on %s, %d devices, one round every %s",
		*addr, len(newDevices()), *every)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		log.Printf("%s connected: a receiver has tuned in", conn.RemoteAddr())
		serve(conn, *every)
		conn.Close()
	}
}

// serve streams samples to one receiver until it goes away.
//
// **Paced to real time**, because rtl_433 timestamps what it decodes with
// the clock rather than with a sample count once it is reading a stream. A
// server that sent as fast as the socket allowed would deliver a day of
// readings in a second and stamp them all now.
func serve(conn net.Conn, every time.Duration) {
	// The twelve bytes a receiver reads before it will believe anything
	// that follows is samples.
	if _, err := conn.Write(greeting()); err != nil {
		log.Printf("%s went away before the greeting: %v", conn.RemoteAddr(), err)
		return
	}

	// A dongle is told its frequency, sample rate and gain by five-byte
	// commands. This one is already where it needs to be, so they are read
	// and discarded: not reading them would eventually block the sender.
	go func() { _, _ = io.Copy(io.Discard, conn) }()

	m := &modulator{rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	devices := newDevices()

	// A tenth of a second of quiet, rendered once and reused. It is what
	// the air sounds like between transmissions, and it is most of what
	// goes over this socket.
	m.emit(100*time.Millisecond, 0)
	quiet := m.take()

	gap := every / time.Duration(len(devices))
	for {
		for _, d := range devices {
			d.drift()
			m.transmit(d)
			if _, err := conn.Write(m.take()); err != nil {
				log.Printf("%s gone: %v", conn.RemoteAddr(), err)
				return
			}
			log.Printf("transmitted %s: %.1fC %d%%", d.name, d.tempC, d.humidity)

			// The quiet between devices, in tenths, so a receiver that
			// disconnects is noticed within one of them.
			for i := 0; i < int(gap/(100*time.Millisecond)); i++ {
				if _, err := conn.Write(quiet); err != nil {
					log.Printf("%s gone: %v", conn.RemoteAddr(), err)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func init() { log.SetOutput(os.Stdout); log.SetFlags(log.LstdFlags) }
