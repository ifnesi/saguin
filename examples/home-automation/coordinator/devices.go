package main

import (
	"encoding/binary"
	"log"
	"math/rand"
	"sync"
	"time"
)

// The devices behind the radio.
//
// This is what makes the demo honest: nothing publishes to the broker except
// zigbee2mqtt. Readings start here, as ZCL attribute reports from an emulated
// device, cross the socket as the radio's own frames, and zigbee2mqtt decodes
// them, names them and publishes them. A generator writing device state
// straight to MQTT would prove nothing about zigbee2mqtt at all.
//
// The devices present as models zigbee2mqtt has converters for, and report
// over standard ZCL clusters rather than any vendor's private protocol - so
// what arrives is decoded by the same code that would decode real hardware.

// ZCL clusters these devices speak.
const (
	clusterGenPowerCfg = 0x0001
	clusterGenOnOff    = 0x0006
	clusterTemperature = 0x0402
	clusterHumidity    = 0x0405
)

// ZCL attribute ids.
const (
	attrOnOff                      = 0x0000
	attrStartUpOnOff               = 0x4003
	attrMeasuredValue              = 0x0000
	attrBatteryPercentageRemaining = 0x0021
)

// ZCL data types, as the wire spells them.
const (
	zclTypeBool   = 0x10
	zclTypeUint8  = 0x20
	zclTypeUint16 = 0x21
	zclTypeInt16  = 0x29
	zclTypeEnum8  = 0x30
)

// ZCL commands, profile-wide.
const (
	zclReadAttributes         = 0x00
	zclReadAttributesResponse = 0x01
	zclDefaultResponse        = 0x0B
	zclReportAttributes       = 0x0A
)

type deviceKind int

const (
	kindClimate deviceKind = iota // temperature, humidity, battery
	kindSwitch                    // on/off, and it takes commands
)

type device struct {
	name     string // zigbee2mqtt's friendly name, for the log only
	nwkAddr  uint16
	ieee     []byte // 8 bytes, as written in the seeded database
	kind     deviceKind
	endpoint byte

	mu      sync.Mutex
	on      bool    // kindSwitch
	startUp byte    // kindSwitch: genOnOff startUpOnOff, what the device does after power loss
	tempC   float64 // kindClimate
	humid   float64
	batt    float64
	seq     byte // ZCL transaction sequence, per device
}

func (d *device) nextSeq() byte {
	d.seq++
	return d.seq
}

// The house. Two models, both chosen because zigbee2mqtt decodes them from
// standard clusters rather than a vendor protocol:
//
//	CK-TLSR8656-SS5-02(7014)  eWeLink  temperature, humidity, battery
//	LDSENK01F                 ADEO     on/off
//
// Their ieee addresses and model names are repeated in seed/devices.jsonl,
// which is what tells zigbee2mqtt these devices are already paired - the
// normal state of a hub that has been running, and a different thing from
// the pairing flow, which is its own demo.
func newDevices() []*device {
	return []*device{
		{name: "living-room-sensor", nwkAddr: 0x1A01, ieee: []byte{0x00, 0x12, 0x4B, 0x00, 0x11, 0x11, 0x11, 0x01}, kind: kindClimate, endpoint: 1, tempC: 20.5, humid: 48, batt: 92},
		{name: "bedroom-sensor", nwkAddr: 0x1A02, ieee: []byte{0x00, 0x12, 0x4B, 0x00, 0x11, 0x11, 0x11, 0x02}, kind: kindClimate, endpoint: 1, tempC: 18.5, humid: 52, batt: 77},
		{name: "kitchen-plug", nwkAddr: 0x1A03, ieee: []byte{0x00, 0x12, 0x4B, 0x00, 0x11, 0x11, 0x11, 0x03}, kind: kindSwitch, endpoint: 1, on: false, startUp: 0xFF},
	}
}

// zclFrame builds a ZCL frame: the header, then the command's own payload.
//
// Frame control 0x18 for a response - profile-wide command, from server to
// client, disabling the default response so zigbee2mqtt is not obliged to
// answer a report it did not ask for.
func zclFrame(control byte, seq byte, command byte, payload []byte) []byte {
	return append([]byte{control, seq, command}, payload...)
}

// attr appends one attribute id, type and value, the shape a report and a
// read response both use for each attribute they carry.
func attr(out []byte, id uint16, typ byte, value []byte) []byte {
	out = binary.LittleEndian.AppendUint16(out, id)
	out = append(out, typ)
	return append(out, value...)
}

func u16le(v uint16) []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, v); return b }
func i16le(v int16) []byte  { return u16le(uint16(v)) }

// reports returns the attribute reports this device would send, one ZCL frame
// per cluster, as (cluster, frame) pairs.
func (d *device) reports() []struct {
	cluster uint16
	frame   []byte
} {
	d.mu.Lock()
	defer d.mu.Unlock()

	var out []struct {
		cluster uint16
		frame   []byte
	}
	add := func(cluster uint16, payload []byte) {
		out = append(out, struct {
			cluster uint16
			frame   []byte
		}{cluster, zclFrame(0x18, d.nextSeq(), zclReportAttributes, payload)})
	}

	switch d.kind {
	case kindClimate:
		// Temperature is reported in hundredths of a degree, humidity in
		// hundredths of a percent: the units the ZCL clusters define, which
		// zigbee2mqtt divides by 100 on the way out.
		add(clusterTemperature, attr(nil, attrMeasuredValue, zclTypeInt16, i16le(int16(d.tempC*100))))
		add(clusterHumidity, attr(nil, attrMeasuredValue, zclTypeUint16, u16le(uint16(d.humid*100))))
		// Battery percentage is in half-percent units, which is the one
		// place this encoding surprises people.
		add(clusterGenPowerCfg, attr(nil, attrBatteryPercentageRemaining, zclTypeUint8, []byte{byte(d.batt * 2)}))
	case kindSwitch:
		v := byte(0)
		if d.on {
			v = 1
		}
		add(clusterGenOnOff, attr(nil, attrOnOff, zclTypeBool, []byte{v}))
	}
	return out
}

// drift moves a device's readings a little, so the demo shows values that
// change rather than a constant somebody could mistake for a stuck feed.
func (d *device) drift() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.kind != kindClimate {
		return
	}
	d.tempC += (rand.Float64() - 0.5) * 0.4
	if d.tempC < 14 {
		d.tempC = 14
	}
	if d.tempC > 28 {
		d.tempC = 28
	}
	d.humid += (rand.Float64() - 0.5) * 1.5
	if d.humid < 30 {
		d.humid = 30
	}
	if d.humid > 70 {
		d.humid = 70
	}
	// Batteries go one way. Slowly, and never past a floor, because a demo
	// left running overnight should not end with every sensor at zero.
	if d.batt > 20 && rand.Float64() < 0.05 {
		d.batt -= 0.5
	}
}

// More ZCL commands, the ones zigbee2mqtt sends TO a device.
const (
	zclWriteAttributes         = 0x02
	zclWriteAttributesResponse = 0x04
	zclConfigureReporting      = 0x06
	zclConfigureReportingRsp   = 0x07
)

// handleCommand applies a ZCL frame zigbee2mqtt sent to this device and
// returns the frame the device answers with, plus whether its state changed.
//
// **A device that does not answer is a device that failed.** zigbee2mqtt
// waits for a ZCL response and gives up after ten seconds, so an emulator
// that only accepts commands makes every configure step and every command
// time out - which reads as a broken device rather than a missing reply.
//
// This is the far end of the command path: the viewer published to
// `<device>/set`, which is broadcast because no channel claims it,
// zigbee2mqtt turned it into a ZCL command, and it arrives here.
func (d *device) handleCommand(cluster uint16, frame []byte) (reply []byte, changed bool) {
	if len(frame) < 3 {
		return nil, false
	}
	control := frame[0]
	seq := frame[1]
	command := frame[2]
	payload := frame[3:]

	// Bit 2 of the frame control means a manufacturer code sits between the
	// control byte and the sequence, which shifts everything after it.
	if control&0x04 != 0 {
		if len(frame) < 5 {
			return nil, false
		}
		seq = frame[3]
		command = frame[4]
		payload = frame[5:]
	}

	// A response carries the request's sequence, is addressed back the way it
	// came, and asks for no answer of its own.
	respond := func(cmd byte, body []byte) []byte {
		return zclFrame(0x18, seq, cmd, body)
	}

	// Cluster-specific, which for these devices means the on/off commands.
	if control&0x03 == 0x01 {
		if cluster != clusterGenOnOff {
			// Answer anyway: an unsupported command must be refused rather
			// than met with silence, or zigbee2mqtt waits out its timeout.
			return respond(zclDefaultResponse, []byte{command, 0x81}), false
		}
		d.mu.Lock()
		switch command {
		case 0x00:
			changed = d.on
			d.on = false
		case 0x01:
			changed = !d.on
			d.on = true
		case 0x02:
			d.on = !d.on
			changed = true
		default:
			d.mu.Unlock()
			return respond(zclDefaultResponse, []byte{command, 0x81}), false
		}
		state := d.on
		d.mu.Unlock()
		log.Printf("device %s: %s -> on=%v", d.name, map[byte]string{0: "off", 1: "on", 2: "toggle"}[command], state)
		return respond(zclDefaultResponse, []byte{command, 0x00}), changed
	}

	// Profile-wide.
	switch command {
	case zclReadAttributes:
		// Answer each attribute asked for, and say plainly when one is not
		// supported rather than omitting it.
		var body []byte
		for i := 0; i+1 < len(payload); i += 2 {
			id := binary.LittleEndian.Uint16(payload[i : i+2])
			typ, value, ok := d.attribute(cluster, id)
			body = binary.LittleEndian.AppendUint16(body, id)
			if !ok {
				body = append(body, 0x86) // unsupported attribute
				continue
			}
			body = append(body, 0x00, typ)
			body = append(body, value...)
		}
		return respond(zclReadAttributesResponse, body), false

	case zclWriteAttributes:
		// Actually store it, because the converter writes this attribute and
		// then reads it back to check. Accepting a write and forgetting it
		// passes the write and fails the read, which is a worse failure than
		// refusing outright: it looks like the device lied.
		d.write(cluster, payload)
		return respond(zclWriteAttributesResponse, []byte{0x00}), false

	case zclConfigureReporting:
		// Also accepted wholesale. These devices report on their own timer
		// rather than honouring an interval, which is a simplification worth
		// knowing about: asking for a faster report will not produce one.
		return respond(zclConfigureReportingRsp, []byte{0x00}), false
	}

	return respond(zclDefaultResponse, []byte{command, 0x81}), false
}

// write stores the attributes zigbee2mqtt set, for the few this device keeps.
func (d *device) write(cluster uint16, payload []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := 0; i+3 <= len(payload); {
		id := binary.LittleEndian.Uint16(payload[i : i+2])
		typ := payload[i+2]
		i += 3
		// Only the fixed-width types these devices carry; anything else
		// would need a length, and none of them use one.
		size := map[byte]int{zclTypeBool: 1, zclTypeUint8: 1, zclTypeEnum8: 1, zclTypeUint16: 2, zclTypeInt16: 2}[typ]
		if size == 0 || i+size > len(payload) {
			return
		}
		if cluster == clusterGenOnOff && id == attrStartUpOnOff {
			d.startUp = payload[i]
			log.Printf("device %s: startUpOnOff -> 0x%02x", d.name, d.startUp)
		}
		i += size
	}
}

// attribute returns one attribute's type and encoded value.
func (d *device) attribute(cluster uint16, id uint16) (typ byte, value []byte, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case cluster == clusterGenOnOff && id == attrStartUpOnOff:
		// A real C205 carries this, and its converter reads it back after
		// writing it. Answering "unsupported attribute" is honest about an
		// emulator and still fails the device's configure step, so the
		// emulated device supports what the model it claims supports.
		return zclTypeEnum8, []byte{d.startUp}, true
	case cluster == clusterGenOnOff && id == attrOnOff:
		v := byte(0)
		if d.on {
			v = 1
		}
		return zclTypeBool, []byte{v}, true
	case cluster == clusterTemperature && id == attrMeasuredValue:
		return zclTypeInt16, i16le(int16(d.tempC * 100)), true
	case cluster == clusterHumidity && id == attrMeasuredValue:
		return zclTypeUint16, u16le(uint16(d.humid * 100)), true
	case cluster == clusterGenPowerCfg && id == attrBatteryPercentageRemaining:
		return zclTypeUint8, []byte{byte(d.batt * 2)}, true
	}
	return 0, nil, false
}

// announceEvery is how often each device announces itself.
//
// A real end device announces on join and after a reset. Doing it on a timer
// here is what gives zigbee2mqtt real `device_announce` events to publish to
// `bridge/event` - so the append channel fills with events that genuinely
// came out of zigbee2mqtt, rather than with anything this program wrote to
// MQTT.
const announceEvery = 90 * time.Second

// reportEvery is how often a device sends its readings. Slow enough to read
// in the viewer, fast enough that the demo does something while somebody
// watches it.
const reportEvery = 10 * time.Second
