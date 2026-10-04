package main

import (
	"math/rand"
	"time"
)

// The devices behind the emulated dongle, and the protocol they speak.
//
// **Nexus-TH**, which rtl_433 knows as protocol 19, is the format of the
// cheap outdoor thermometers sold under a dozen names. It is one-way: these
// devices announce and are never addressed, which is the honest difference
// from the Zigbee half of this demo, where a command reaches a plug and
// comes back. A 433 MHz sensor has no receiver to command.
//
// Frame: a 500us mark, a 4000us gap, then 36 bits, each a 500us mark and a
// gap that is 1000us for a nought and 2000us for a one.
//
//	 8  id, which changes when the batteries are replaced
//	 1  battery ok
//	 1  always nought
//	 2  channel, as the switch in the battery compartment sets it
//	12  temperature in tenths of a degree, two's complement
//	 4  always 1111
//	 8  humidity, whole percent
const (
	mark    = 500 * time.Microsecond
	sync    = 4000 * time.Microsecond
	gapZero = 1000 * time.Microsecond
	gapOne  = 2000 * time.Microsecond

	// **Six repeats, and they have to arrive together.** A real sensor
	// repeats so that one collision does not cost the reading, and rtl_433
	// wants to see a message more than once before it believes it. They
	// must fall inside one burst: measured while building this, repeats
	// spaced 20ms apart were split into separate packages and nothing
	// decoded, while the same six back to back decoded first time.
	repeats = 6
)

type device struct {
	name     string
	id       int // the 8-bit id in the frame
	channel  int // 1 to 3, as the sensor's own switch sets it
	tempC    float64
	humidity int
	// drift keeps each device wandering around its own comfortable range
	// rather than around one shared number, so the viewer shows three
	// different lines rather than three copies of one.
	baseTemp float64
	baseHum  int
	rng      *rand.Rand
}

// newDevices is what is out there transmitting. Three sensors, on three
// channels, in the places a household actually puts them: the ids are the
// arbitrary numbers a sensor picks at power-on and keeps until its
// batteries are changed.
func newDevices() []*device {
	seed := time.Now().UnixNano()
	return []*device{
		{name: "garden", id: 0x5A, channel: 1, baseTemp: 11.5, baseHum: 71,
			tempC: 11.5, humidity: 71, rng: rand.New(rand.NewSource(seed + 1))},
		{name: "garage", id: 0x8C, channel: 2, baseTemp: 16.0, baseHum: 55,
			tempC: 16.0, humidity: 55, rng: rand.New(rand.NewSource(seed + 2))},
		{name: "loft", id: 0xB3, channel: 3, baseTemp: 24.5, baseHum: 38,
			tempC: 24.5, humidity: 38, rng: rand.New(rand.NewSource(seed + 3))},
	}
}

// drift moves a reading a little, and pulls it back towards where that
// sensor lives, so a long run wanders rather than walking off.
func (d *device) drift() {
	d.tempC += (d.rng.Float64() - 0.5) + (d.baseTemp-d.tempC)*0.1
	d.tempC = float64(int(d.tempC*10)) / 10
	d.humidity += d.rng.Intn(3) - 1 + int((float64(d.baseHum)-float64(d.humidity))*0.1)
	if d.humidity < 0 {
		d.humidity = 0
	}
	if d.humidity > 100 {
		d.humidity = 100
	}
}

// bits is the frame this device would send now, most significant bit first.
func (d *device) bits() []int {
	var out []int
	put := func(v, n int) {
		for i := n - 1; i >= 0; i-- {
			out = append(out, (v>>i)&1)
		}
	}
	put(d.id, 8)
	put(1, 1) // battery ok: a flat one is its own demo
	put(0, 1)
	put(d.channel-1, 2)
	put(int(d.tempC*10)&0xFFF, 12)
	put(0xF, 4)
	put(d.humidity, 8)
	return out
}

// transmit renders one device's burst: its frame, repeated, back to back.
func (m *modulator) transmit(d *device) {
	for i := 0; i < repeats; i++ {
		m.emit(mark, carrier)
		m.emit(sync, 0)
		for _, b := range d.bits() {
			m.emit(mark, carrier)
			if b == 1 {
				m.emit(gapOne, 0)
			} else {
				m.emit(gapZero, 0)
			}
		}
	}
}
