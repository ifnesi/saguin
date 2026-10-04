// Command zstack-emu answers, on a TCP socket, the small part of the Texas
// Instruments Z-Stack monitor protocol that zigbee2mqtt needs to finish
// starting - and nothing else.
//
// zigbee2mqtt talks to its radio over a serial port or, because people run
// network-attached coordinators, over TCP. That makes the radio a swappable
// back end: this program answers where a real coordinator would, so the demo
// runs with no hardware, and pointing zigbee2mqtt at a real coordinator-over-IP
// instead changes an address and nothing else.
//
// It presents a coordinator that is ALREADY commissioned, already running as a
// coordinator, and already carrying every endpoint zigbee2mqtt registers.
// Each of those skips a branch of the startup, which is why this is a few
// hundred lines rather than an implementation of the protocol: zigbee-herdsman
// only takes the short path when five stored structures agree with the network
// it was configured for, so the structures below are the actual work and the
// command handling is the easy half.
package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// UNPI framing. A frame is SOF, the data length, cmd0 packing the type and
// subsystem, the command id, the data, and a checksum that is the XOR of
// everything from the length byte to the end of the data.
const sof = 0xFE

const (
	typeSREQ = 1
	typeAREQ = 2
	typeSRSP = 3
)

const (
	subSYS  = 1
	subAF   = 4
	subZDO  = 5
	subUTIL = 7
)

// The NV items zigbee2mqtt reads before it will accept the adapter.
const (
	nvEXTADDR                = 1
	nvNIB                    = 33
	nvNWK_ACTIVE_KEY_INFO    = 58
	nvNWK_ALTERN_KEY_INFO    = 59
	nvZNP_HAS_CONFIGURED_ZS3 = 96
	nvPRECFGKEY              = 98
	nvNWKKEY                 = 130
)

// The endpoints zigbee-herdsman registers, with the descriptor each one
// answers when asked. Reporting them all as already active means AF register
// is never called; answering the descriptors is what lets the controller
// build its record of the coordinator itself.
//
// Transcribed from zigbee-herdsman's own endpoint table rather than invented,
// including the reasons some of them exist: endpoint 11 carries the IAS
// clusters, 13 carries OTA, 110 is there for TERNCY devices and 239 for
// Shelly's custom profile.
type endpoint struct {
	id          byte
	profileID   uint16
	deviceID    uint16
	inClusters  []uint16
	outClusters []uint16
}

var endpoints = []endpoint{
	{id: 1, profileID: 0x0104, deviceID: 0x0005},
	{id: 2, profileID: 0x0101, deviceID: 0x0005},
	{id: 3, profileID: 0x0104, deviceID: 0x0005},
	{id: 4, profileID: 0x0107, deviceID: 0x0005},
	{id: 5, profileID: 0x0108, deviceID: 0x0005},
	{id: 6, profileID: 0x0109, deviceID: 0x0005},
	{id: 8, profileID: 0x0104, deviceID: 0x0005},
	{id: 10, profileID: 0x0104, deviceID: 0x0005},
	{
		id: 11, profileID: 0x0104, deviceID: 0x0400,
		inClusters:  []uint16{0x0501, 0x000A}, // ssIasAce, genTime
		outClusters: []uint16{0x0500, 0x0502}, // ssIasZone, ssIasWd
	},
	{id: 110, profileID: 0x0104, deviceID: 0x0005}, // 0x6e
	{id: 12, profileID: 0xC05E, deviceID: 0x0005},
	{id: 13, profileID: 0x0104, deviceID: 0x0005, inClusters: []uint16{0x0019}}, // genOta
	{id: 47, profileID: 0x0104, deviceID: 0x0005},
	{id: 239, profileID: 0xC001, deviceID: 0x0005}, // Shelly's custom profile
	{id: 242, profileID: 0xA1E0, deviceID: 0x0005},
}

func endpointIDs() []byte {
	out := make([]byte, 0, len(endpoints))
	for _, e := range endpoints {
		out = append(out, e.id)
	}
	return out
}

// simpleDescriptor renders one endpoint as the descriptor body a ZDO simple
// descriptor response carries, without the leading status and address.
func (e endpoint) simpleDescriptor() []byte {
	d := []byte{e.id}
	d = binary.LittleEndian.AppendUint16(d, e.profileID)
	d = binary.LittleEndian.AppendUint16(d, e.deviceID)
	d = append(d, 0x00) // device version
	d = append(d, byte(len(e.inClusters)))
	for _, c := range e.inClusters {
		d = binary.LittleEndian.AppendUint16(d, c)
	}
	d = append(d, byte(len(e.outClusters)))
	for _, c := range e.outClusters {
		d = binary.LittleEndian.AppendUint16(d, c)
	}
	return d
}

type frame struct {
	typ  byte
	sub  byte
	cmd  byte
	data []byte
}

func (f frame) encode() []byte {
	out := make([]byte, 0, len(f.data)+5)
	out = append(out, sof, byte(len(f.data)), (f.typ<<5)&0xE0|(f.sub&0x1F), f.cmd)
	out = append(out, f.data...)
	var fcs byte
	for _, b := range out[1:] {
		fcs ^= b
	}
	return append(out, fcs)
}

func readFrame(r *bufio.Reader) (frame, error) {
	// Resynchronise on SOF rather than trusting the stream to be aligned: a
	// client that reconnects mid-frame would otherwise poison every frame
	// after it.
	for {
		b, err := r.ReadByte()
		if err != nil {
			return frame{}, err
		}
		if b == sof {
			break
		}
	}
	hdr := make([]byte, 3) // length, cmd0, cmd1
	if _, err := io.ReadFull(r, hdr); err != nil {
		return frame{}, err
	}
	data := make([]byte, hdr[0])
	if _, err := io.ReadFull(r, data); err != nil {
		return frame{}, err
	}
	got, err := r.ReadByte()
	if err != nil {
		return frame{}, err
	}
	want := hdr[0] ^ hdr[1] ^ hdr[2]
	for _, b := range data {
		want ^= b
	}
	if got != want {
		return frame{}, fmt.Errorf("bad checksum: got %#02x want %#02x", got, want)
	}
	return frame{
		typ:  (hdr[1] & 0xE0) >> 5,
		sub:  hdr[1] & 0x1F,
		cmd:  hdr[2],
		data: data,
	}, nil
}

// network is what both sides have to agree on. zigbee2mqtt's configuration
// carries the same three values; if they disagree, zigbee-herdsman decides the
// coordinator needs commissioning and walks a much longer road than this
// program implements.
type network struct {
	panID         uint16
	extendedPanID []byte // 8 bytes, as written in configuration.yaml
	networkKey    []byte // 16 bytes
	channel       byte
	ieee          []byte // the coordinator's own address
}

// nib builds the Network Information Base exactly as zigbee-herdsman parses
// it: 110 bytes, unaligned, with the two 8-byte addresses stored reversed.
//
// Only nwkPanId and extendedPANID are compared against the configuration - the
// channel check is commented out upstream because changing channel is
// supported - but the whole structure has to be the right length and shape or
// it does not parse at all.
func (n network) nib() []byte {
	b := make([]byte, 110)
	b[0] = 0x00                                   // SequenceNum
	b[1] = 0x08                                   // PassiveAckTimeout
	b[2] = 0x03                                   // MaxBroadcastRetries
	b[3] = 0x14                                   // MaxChildren
	b[4] = 0x0F                                   // MaxDepth
	b[5] = 0x05                                   // MaxRouters
	b[6] = 0x00                                   // dummyNeighborTable
	b[7] = 0x0F                                   // BroadcastDeliveryTime
	b[8] = 0x00                                   // ReportConstantCost
	b[9] = 0x00                                   // RouteDiscRetries
	b[10] = 0x00                                  // dummyRoutingTable
	b[11] = 0x00                                  // SecureAllFrames
	b[12] = 0x05                                  // SecurityLevel
	b[13] = 0x00                                  // SymLink
	b[14] = 0x00                                  // CapabilityFlags
	binary.LittleEndian.PutUint16(b[15:], 7)      // TransactionPersistenceTime
	b[17] = 0x02                                  // nwkProtocolVersion
	b[18] = 0x05                                  // RouteDiscoveryTime
	b[19] = 0x3C                                  // RouteExpiryTime
	binary.LittleEndian.PutUint16(b[20:], 0x0000) // nwkDevAddress: the coordinator is 0
	b[22] = n.channel                             // nwkLogicalChannel
	binary.LittleEndian.PutUint16(b[23:], 0x0000) // nwkCoordAddress
	copy(b[25:33], reversed(n.ieee))              // nwkCoordExtAddress, reversed
	binary.LittleEndian.PutUint16(b[33:], n.panID)
	b[35] = 0x09                                                // nwkState: NWK_ROUTER
	binary.LittleEndian.PutUint32(b[36:], 1<<uint32(n.channel)) // channelList as a mask
	b[40] = 0x0F                                                // beaconOrder
	b[41] = 0x0F                                                // superFrameOrder
	b[42] = 0x04                                                // scanDuration
	b[43] = 0x00                                                // battLifeExt
	binary.LittleEndian.PutUint32(b[44:], 0)                    // allocatedRouterAddresses
	binary.LittleEndian.PutUint32(b[48:], 0)                    // allocatedEndDeviceAddresses
	b[52] = 0x00                                                // nodeDepth
	copy(b[53:61], reversed(n.extendedPanID))                   // extendedPANID, reversed
	b[61] = 0x01                                                // nwkKeyLoaded
	// spare1 and spare2 are both nwkKeyDescriptor: one sequence byte and a
	// 16-byte key, 17 bytes each. Left zero; nothing reads them.
	b[96] = 0x00                                   // spare3
	b[97] = 0x00                                   // spare4
	b[98] = 0x0F                                   // nwkLinkStatusPeriod
	b[99] = 0x03                                   // nwkRouterAgeLimit
	b[100] = 0x01                                  // nwkUseMultiCast
	b[101] = 0x01                                  // nwkIsConcentrator
	b[102] = 0x78                                  // nwkConcentratorDiscoveryTime
	b[103] = 0x0A                                  // nwkConcentratorRadius
	b[104] = 0x00                                  // nwkAllFresh
	binary.LittleEndian.PutUint16(b[105:], 0x0000) // nwkManagerAddr
	binary.LittleEndian.PutUint16(b[107:], 0x0000) // nwkTotalTransmissions
	b[109] = 0x00                                  // nwkUpdateId
	return b
}

func reversed(in []byte) []byte {
	out := make([]byte, len(in))
	for i, b := range in {
		out[len(in)-1-i] = b
	}
	return out
}

// nvItems is the whole of this coordinator's non-volatile memory as far as
// zigbee2mqtt is concerned.
func (n network) nvItems() map[uint16][]byte {
	keyDescriptor := func() []byte {
		d := make([]byte, 17) // keySeqNum, then the 16-byte key
		copy(d[1:], n.networkKey)
		return d
	}
	return map[uint16][]byte{
		// Length 21 is how zigbee-herdsman decides this platform packs its
		// structures without padding. 24 would mean aligned, and every
		// structure below would then need padding inserted.
		nvNWKKEY:                 make([]byte, 21),
		nvZNP_HAS_CONFIGURED_ZS3: {0x55}, // the one value that counts as configured
		nvNIB:                    n.nib(),
		nvPRECFGKEY:              n.networkKey,
		nvNWK_ACTIVE_KEY_INFO:    keyDescriptor(),
		nvNWK_ALTERN_KEY_INFO:    keyDescriptor(),
		nvEXTADDR:                reversed(n.ieee),
	}
}

type emulator struct {
	net     network
	nv      map[uint16][]byte
	verbose bool
	unknown map[string]int

	devices []*device

	// The connected client, and the lock that keeps two goroutines from
	// interleaving halves of two frames on it. zigbee2mqtt is the only
	// client, but the reporting timer and the command path both write.
	mu   sync.Mutex
	conn net.Conn
}

// send writes one frame to the connected client, if there is one. Frames
// pushed by a timer arrive between whatever the client is asking about, which
// is exactly how a radio behaves.
func (e *emulator) send(f frame) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conn == nil {
		return
	}
	if _, err := e.conn.Write(f.encode()); err != nil {
		log.Printf("coordinator: write: %v", err)
	}
}

// announce tells zigbee2mqtt a device is present. It produces a real
// `device_announce` on `zigbee2mqtt/bridge/event`, which is what fills the
// append channel - with an event zigbee2mqtt published, from something that
// happened on the radio.
func (e *emulator) announce(d *device) {
	data := make([]byte, 0, 13)
	data = binary.LittleEndian.AppendUint16(data, d.nwkAddr) // srcaddr
	data = binary.LittleEndian.AppendUint16(data, d.nwkAddr) // nwkaddr
	data = append(data, reversed(d.ieee)...)
	data = append(data, 0x80) // capabilities: a mains-powered receiver
	e.logf("announce %s (0x%04x)", d.name, d.nwkAddr)
	e.send(frame{typeAREQ, subZDO, 193, data})
}

// deliver wraps one ZCL frame as an incoming message from a device, which is
// how everything a device says reaches zigbee2mqtt.
func (e *emulator) deliver(d *device, cluster uint16, zcl []byte) {
	data := make([]byte, 0, 17+len(zcl))
	data = binary.LittleEndian.AppendUint16(data, 0)       // groupid
	data = binary.LittleEndian.AppendUint16(data, cluster) // clusterid
	data = binary.LittleEndian.AppendUint16(data, d.nwkAddr)
	data = append(data, d.endpoint, 1) // srcendpoint, dstendpoint
	data = append(data, 0)             // wasbroadcast
	data = append(data, byte(60+rand.Intn(100)))
	data = append(data, 0) // securityuse
	data = binary.LittleEndian.AppendUint32(data, uint32(time.Now().Unix()))
	data = append(data, zcl[1]) // transseqnumber, the ZCL sequence
	data = append(data, byte(len(zcl)))
	data = append(data, zcl...)
	e.send(frame{typeAREQ, subAF, 129, data})
}

// greet announces every device and sends one round of readings.
//
// Called when a client connects, NOT when this process starts: frames written
// before zigbee2mqtt is attached go nowhere, so a cold start would show an
// empty bridge event log until the next announcement timer, and look like the
// append channel does not work.
func (e *emulator) greet() {
	// A moment, so this lands after zigbee2mqtt has finished starting its
	// adapter rather than during it.
	time.Sleep(3 * time.Second)
	for _, d := range e.devices {
		e.announce(d)
		for _, r := range d.reports() {
			e.deliver(d, r.cluster, r.frame)
		}
	}
}

// run drives the devices on a timer. Nothing here touches MQTT - every one of
// these frames is a thing the radio says, and zigbee2mqtt decides what to
// publish.
func (e *emulator) run() {
	reports := time.NewTicker(reportEvery)
	announces := time.NewTicker(announceEvery)
	defer reports.Stop()
	defer announces.Stop()
	for {
		select {
		case <-reports.C:
			for _, d := range e.devices {
				d.drift()
				for _, r := range d.reports() {
					e.deliver(d, r.cluster, r.frame)
				}
			}
		case <-announces.C:
			for _, d := range e.devices {
				e.announce(d)
			}
		}
	}
}

// handle answers one request. A nil reply means the request needs no answer.
func (e *emulator) handle(f frame) []frame {
	switch {
	case f.sub == subSYS && f.cmd == 1: // ping
		d := make([]byte, 2)
		binary.LittleEndian.PutUint16(d, 0x0079) // the usual capability set
		return []frame{{typeSRSP, subSYS, 1, d}}

	case f.sub == subSYS && f.cmd == 2: // version
		d := make([]byte, 9)
		d[0] = 2 // transportrev
		d[1] = 1 // product: ZStack3x0, which selects the modern startup path
		d[2] = 2 // majorrel
		d[3] = 7 // minorrel
		d[4] = 1 // maintrel
		binary.LittleEndian.PutUint32(d[5:], 20250101)
		return []frame{{typeSRSP, subSYS, 2, d}}

	case f.sub == subSYS && f.cmd == 4: // getExtAddr
		return []frame{{typeSRSP, subSYS, 4, reversed(e.net.ieee)}}

	case f.sub == subSYS && f.cmd == 19: // osalNvLength
		id := binary.LittleEndian.Uint16(f.data)
		d := make([]byte, 2)
		// An item this coordinator does not carry answers length 0, which
		// zigbee-herdsman reads as "absent" rather than as an error.
		binary.LittleEndian.PutUint16(d, uint16(len(e.nv[id])))
		e.logf("osalNvLength id=%d -> %d", id, len(e.nv[id]))
		return []frame{{typeSRSP, subSYS, 19, d}}

	case f.sub == subSYS && f.cmd == 28: // osalNvReadExt
		id := binary.LittleEndian.Uint16(f.data[0:2])
		off := binary.LittleEndian.Uint16(f.data[2:4])
		item := e.nv[id]
		if int(off) > len(item) {
			return []frame{{typeSRSP, subSYS, 28, []byte{0x01, 0}}}
		}
		chunk := item[off:]
		if len(chunk) > 240 { // the frame carries at most 250 bytes of data
			chunk = chunk[:240]
		}
		d := append([]byte{0x00, byte(len(chunk))}, chunk...)
		e.logf("osalNvReadExt id=%d off=%d -> %d bytes", id, off, len(chunk))
		return []frame{{typeSRSP, subSYS, 28, d}}

	case f.sub == subSYS && f.cmd == 50: // nvLength, the extended-table form
		// Every extended table is empty, which is the truth about a
		// coordinator with nothing paired to it: no address manager entries,
		// no link keys, no security material. A zero length stops the table
		// read at its first entry, so nvRead below is never reached for one.
		e.logf("nvLength (table) -> empty")
		return []frame{{typeSRSP, subSYS, 50, []byte{0x00}}}

	case f.sub == subSYS && f.cmd == 51: // nvRead, the extended-table form
		// Unreachable while every table reports empty, and answered rather
		// than left hanging so that a future zigbee2mqtt asking for one gets
		// a refusal it can act on instead of a six-second timeout.
		return []frame{{typeSRSP, subSYS, 51, []byte{0x01, 0x00}}}

	case f.sub == subUTIL && f.cmd == 0: // getDeviceInfo
		d := []byte{0x00} // status
		d = append(d, reversed(e.net.ieee)...)
		d = append(d, 0x00, 0x00) // shortaddr: the coordinator is 0x0000
		d = append(d, 0x07)       // devicetype: coordinator capable
		// devicestate 9 is ZB_COORD - "already started as a coordinator",
		// which is what makes zigbee-herdsman skip startupFromApp and the
		// state-change indication it would then wait sixty seconds for.
		d = append(d, 0x09)
		d = append(d, 0x00) // numassocdevices
		e.logf("getDeviceInfo -> coordinator, already started")
		return []frame{{typeSRSP, subUTIL, 0, d}}

	case f.sub == subZDO && f.cmd == 5: // activeEpReq
		// The SRSP only says the request was accepted; the answer follows as
		// an unsolicited frame, which is how ZDO works on this protocol.
		ids := endpointIDs()
		zdo := []byte{0x00, 0x00, 0x00, byte(len(ids))} // status, nwkaddr, count
		zdo = append(zdo, ids...)
		rsp := append([]byte{0x00, 0x00}, zdo...) // srcaddr, then the ZDO payload
		e.logf("activeEpReq -> %d endpoints, all already registered", len(endpoints))
		return []frame{
			{typeSRSP, subZDO, 5, []byte{0x00}},
			{typeAREQ, subZDO, 133, rsp},
		}

	case f.sub == subZDO && f.cmd == 4: // simpleDescReq
		// dstaddr, then the address of interest, then the endpoint asked for.
		want := f.data[4]
		var found *endpoint
		for i := range endpoints {
			if endpoints[i].id == want {
				found = &endpoints[i]
				break
			}
		}
		if found == nil {
			// 0x82 is INVALID_EP, which is what a real coordinator answers
			// for an endpoint it does not carry.
			e.logf("simpleDescReq endpoint=%d -> no such endpoint", want)
			return []frame{
				{typeSRSP, subZDO, 4, []byte{0x00}},
				{typeAREQ, subZDO, 132, []byte{0x00, 0x00, 0x82}},
			}
		}
		desc := found.simpleDescriptor()
		zdo := []byte{0x00, 0x00, 0x00, byte(len(desc))} // status, nwkaddr, descriptor length
		zdo = append(zdo, desc...)
		e.logf("simpleDescReq endpoint=%d -> profile 0x%04x", want, found.profileID)
		return []frame{
			{typeSRSP, subZDO, 4, []byte{0x00}},
			{typeAREQ, subZDO, 132, append([]byte{0x00, 0x00}, zdo...)},
		}

	case f.sub == subZDO && f.cmd == 80: // extNwkInfo
		// What the controller reads back to decide whether the network is on
		// the channel it was configured for. Disagree here and it tries to
		// move the network to another channel, which is a much longer path
		// than this program implements.
		d := make([]byte, 0, 24)
		d = append(d, 0x00, 0x00) // shortaddr: the coordinator is 0x0000
		d = append(d, 0x09)       // devstate: ZB_COORD
		d = binary.LittleEndian.AppendUint16(d, e.net.panID)
		d = append(d, 0x00, 0x00) // parentaddr
		d = append(d, reversed(e.net.extendedPanID)...)
		d = append(d, reversed(e.net.ieee)...) // parentextaddr
		d = append(d, e.net.channel)
		e.logf("extNwkInfo -> pan 0x%04x channel %d", e.net.panID, e.net.channel)
		return []frame{{typeSRSP, subZDO, 80, d}}

	case f.sub == subZDO && f.cmd == 69: // extRouteDisc
		// zigbee2mqtt discovers a route after something to a device failed.
		// Answering it is not a fix for whatever failed, but leaving it
		// unanswered turns one failure into a stalled recovery.
		return []frame{{typeSRSP, subZDO, 69, []byte{0x00}}}

	case f.sub == subZDO && f.cmd == 33, f.sub == subZDO && f.cmd == 34: // bindReq, unbindReq
		// A device's converter binds its clusters to the coordinator so the
		// device knows where to send reports. These devices report anyway -
		// this emulator pushes on a timer rather than being asked - so the
		// binding changes nothing here, but zigbee2mqtt waits for the answer
		// and marks the device as failing to configure without it.
		rsp := byte(161) // bindRsp
		if f.cmd == 34 {
			rsp = 162 // unbindRsp
		}
		dst := binary.LittleEndian.Uint16(f.data[0:2])
		e.logf("bind/unbind for 0x%04x -> accepted", dst)
		return []frame{
			{typeSRSP, subZDO, f.cmd, []byte{0x00}},
			{typeAREQ, subZDO, rsp, []byte{f.data[0], f.data[1], 0x00}}, // srcaddr, status
		}

	case f.sub == subZDO && f.cmd == 74: // extFindGroup
		// A coordinator that is already commissioned already carries the
		// green-power group, so this reports it found. Answering "not found"
		// instead is equally valid and costs one extAddGroup.
		d := []byte{0x00, 0x00, 0x00, 0x00} // status, groupid, namelen
		copy(d[1:3], f.data[1:3])           // echo the group asked about
		e.logf("extFindGroup group=%d -> already present", binary.LittleEndian.Uint16(f.data[1:3]))
		return []frame{{typeSRSP, subZDO, 74, d}}

	case f.sub == subZDO && f.cmd == 75: // extAddGroup
		return []frame{{typeSRSP, subZDO, 75, []byte{0x00}}}

	case f.sub == subAF && f.cmd == 1: // dataRequest
		// zigbee2mqtt sending a ZCL frame to a device. The command path ends
		// here: the viewer published to `<device>/set`, which no channel
		// claims so it was ordinary broadcast, zigbee2mqtt turned it into a
		// ZCL command, and the device now acts on it.
		// dstaddr(2) destendpoint(1) srcendpoint(1) clusterid(2) transid(1)
		// options(1) radius(1) len(1) data - and every field after the two
		// endpoints has to be counted, not guessed. Reading the cluster one
		// byte early gives a plausible-looking number and a ZCL frame shifted
		// by one, which parses as a different command and is answered
		// confidently and wrongly.
		if len(f.data) < 10 {
			return []frame{{typeSRSP, subAF, 1, []byte{0x01}}}
		}
		dst := binary.LittleEndian.Uint16(f.data[0:2])
		srcEndpoint := f.data[3]
		cluster := binary.LittleEndian.Uint16(f.data[4:6])
		transid := f.data[6]
		zcl := f.data[10:]
		e.logf("dataRequest dst=0x%04x cluster=0x%04x zcl=%x", dst, cluster, zcl)

		out := []frame{
			{typeSRSP, subAF, 1, []byte{0x00}},
			// The radio confirming it put the packet on the air. Without
			// this zigbee2mqtt waits for it and eventually reports the
			// command as failed, even though the device acted on it.
			{typeAREQ, subAF, 128, []byte{0x00, srcEndpoint, transid}},
		}
		for _, d := range e.devices {
			if d.nwkAddr != dst {
				continue
			}
			reply, changed := d.handleCommand(cluster, zcl)
			if reply != nil {
				e.logf("  reply cluster=0x%04x zcl=%x", cluster, reply)
				e.deliver(d, cluster, reply)
			}
			if changed {
				// Report the new state straight back, the way a device that
				// changed does. This is what closes the loop: the viewer
				// sees the switch move because the DEVICE said so, not
				// because anything echoed the command.
				go func(d *device) {
					time.Sleep(120 * time.Millisecond)
					for _, r := range d.reports() {
						e.deliver(d, r.cluster, r.frame)
					}
				}(d)
			}
			break
		}
		return out

	case f.sub == subAF && f.cmd == 0: // register
		return []frame{{typeSRSP, subAF, 0, []byte{0x00}}}
	}

	// Anything else is reported rather than guessed at. A wrong answer is
	// worse than none: it would be believed. This is also the list to work
	// from if zigbee2mqtt changes what it asks for.
	key := fmt.Sprintf("sub=%d cmd=%d", f.sub, f.cmd)
	e.unknown[key]++
	if e.unknown[key] == 1 {
		log.Printf("UNANSWERED %s type=%d data=%x", key, f.typ, f.data)
	}
	// Deliberately no reply, not even a failure status. Every command has its
	// own response shape, so a status-only frame is malformed for most of
	// them: the client then reports a buffer overrun from its parser instead
	// of the command it was left waiting for, which hides the thing that
	// needs implementing. A timeout names the command.
	return nil
}

func (e *emulator) logf(format string, args ...any) {
	if e.verbose {
		log.Printf(format, args...)
	}
}

func (e *emulator) serve(c net.Conn) {
	defer c.Close()
	log.Printf("coordinator: %s connected", c.RemoteAddr())
	e.mu.Lock()
	e.conn = c
	e.mu.Unlock()
	go e.greet()
	defer func() {
		e.mu.Lock()
		if e.conn == c {
			e.conn = nil
		}
		e.mu.Unlock()
	}()
	r := bufio.NewReader(c)
	for {
		f, err := readFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("coordinator: %s: %v", c.RemoteAddr(), err)
			}
			log.Printf("coordinator: %s disconnected", c.RemoteAddr())
			return
		}
		for _, out := range e.handle(f) {
			if _, err := c.Write(out.encode()); err != nil {
				log.Printf("coordinator: write: %v", err)
				return
			}
		}
	}
}

func parseHex(s string, want int, what string) []byte {
	s = strings.NewReplacer("0x", "", ":", "", " ", "").Replace(s)
	if len(s) != want*2 {
		log.Fatalf("%s must be %d bytes as hex, got %q", what, want, s)
	}
	out := make([]byte, want)
	for i := range out {
		v, err := strconv.ParseUint(s[i*2:i*2+2], 16, 8)
		if err != nil {
			log.Fatalf("%s: %v", what, err)
		}
		out[i] = byte(v)
	}
	return out
}

func main() {
	addr := flag.String("listen", ":8888", "address to answer on, where zigbee2mqtt's serial port points")
	panID := flag.Int("pan-id", 0x1A62, "network PAN ID; must match zigbee2mqtt's pan_id")
	extPan := flag.String("ext-pan-id", "dddddddddddddddd", "extended PAN ID as hex; must match zigbee2mqtt's ext_pan_id")
	netKey := flag.String("network-key", "01030507090b0d0f00020406080a0c0d", "network key as hex; must match zigbee2mqtt's network_key")
	ieee := flag.String("ieee", "00124b0001020304", "the coordinator's own address")
	channel := flag.Int("channel", 11, "the channel the network is on")
	verbose := flag.Bool("verbose", false, "log every request answered, not only the unanswered ones")
	flag.Parse()

	n := network{
		panID:         uint16(*panID),
		extendedPanID: parseHex(*extPan, 8, "ext-pan-id"),
		networkKey:    parseHex(*netKey, 16, "network-key"),
		ieee:          parseHex(*ieee, 8, "ieee"),
		channel:       byte(*channel),
	}
	e := &emulator{net: n, nv: n.nvItems(), verbose: *verbose, unknown: map[string]int{}, devices: newDevices()}

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("emulated Z-Stack coordinator on %s", l.Addr())
	log.Printf("  pan_id 0x%04x  ext_pan_id %x  channel %d", n.panID, n.extendedPanID, n.channel)
	log.Printf("  zigbee2mqtt must carry the same three, or it will try to commission a new network")
	if os.Getenv("ZBEMU_READY_FILE") != "" {
		_ = os.WriteFile(os.Getenv("ZBEMU_READY_FILE"), []byte("ready\n"), 0o644)
	}
	go e.run()

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatalf("accept: %v", err)
		}
		go e.serve(c)
	}
}
