package harness

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

// Packet is one MQTT control packet.
type Packet interface {
	Type() byte
}

// Connect is CONNECT (MQTT 3.1.1 §3.1, MQTT 5.0 §3.1). Version is the
// protocol level written on the wire and decides the layout, whatever the
// version passed to Encode, so a test can send an unknown level.
type Connect struct {
	ProtocolName string // "MQTT"; Client.Connect fills it in when empty
	Version      byte
	CleanStart   bool // Clean Session in 3.1.1
	KeepAlive    uint16
	Props        Properties // MQTT 5 only
	ClientID     string
	Will         *Will
	HasUsername  bool
	Username     string
	HasPassword  bool
	Password     []byte
}

// Will is the Will Message a CONNECT carries.
type Will struct {
	QoS     byte
	Retain  bool
	Props   Properties // MQTT 5 only
	Topic   string
	Payload []byte
}

// Connack is CONNACK. Reason is MQTT 5's Connect Reason Code, or 3.1.1's
// Connect Return code.
type Connack struct {
	SessionPresent bool
	Reason         byte
	Props          Properties
}

// Publish is PUBLISH. ID is written only when QoS is above 0. ByAlias
// marks a Topic Name sent empty, its Topic Alias standing for Topic (MQTT 5
// 3.3.2.3.4): a Client sets it on a PUBLISH it reads (Client.Recv), and
// Encode then writes an empty Topic Name; Decode, knowing no mapping, leaves
// Topic empty instead.
type Publish struct {
	Dup     bool
	QoS     byte
	Retain  bool
	ByAlias bool
	Topic   string
	ID      uint16
	Props   Properties
	Payload []byte
}

// Ack is PUBACK, PUBREC, PUBREL or PUBCOMP, named by Kind: the four share one
// layout (MQTT 3.1.1 §3.4-3.7, MQTT 5.0 §3.4-3.7).
type Ack struct {
	Kind   byte
	ID     uint16
	Reason byte // MQTT 5 only
	Props  Properties
}

// Subscription is one topic filter of a SUBSCRIBE with its options (MQTT 5.0
// §3.8.3.1). In 3.1.1 only QoS exists; the other options are still written
// if set, which puts bits in the reserved part of the byte.
type Subscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}

// Subscribe is SUBSCRIBE.
type Subscribe struct {
	ID    uint16
	Props Properties
	Subs  []Subscription
}

// Suback is SUBACK: one reason code (3.1.1: return code) per filter.
type Suback struct {
	ID      uint16
	Props   Properties
	Reasons []byte
}

// Unsubscribe is UNSUBSCRIBE.
type Unsubscribe struct {
	ID      uint16
	Props   Properties
	Filters []string
}

// Unsuback is UNSUBACK. 3.1.1's carries only the identifier.
type Unsuback struct {
	ID      uint16
	Props   Properties
	Reasons []byte
}

// Pingreq is PINGREQ.
type Pingreq struct{}

// Pingresp is PINGRESP.
type Pingresp struct{}

// Disconnect is DISCONNECT. 3.1.1's has no body.
type Disconnect struct {
	Reason byte
	Props  Properties
}

// Auth is MQTT 5's AUTH.
type Auth struct {
	Reason byte
	Props  Properties
}

// Type returns CONNECT.
func (*Connect) Type() byte { return CONNECT }

// Type returns CONNACK.
func (*Connack) Type() byte { return CONNACK }

// Type returns PUBLISH.
func (*Publish) Type() byte { return PUBLISH }

// Type returns the acknowledgement's own type, its Kind.
func (a *Ack) Type() byte { return a.Kind }

// Type returns SUBSCRIBE.
func (*Subscribe) Type() byte { return SUBSCRIBE }

// Type returns SUBACK.
func (*Suback) Type() byte { return SUBACK }

// Type returns UNSUBSCRIBE.
func (*Unsubscribe) Type() byte { return UNSUBSCRIBE }

// Type returns UNSUBACK.
func (*Unsuback) Type() byte { return UNSUBACK }

// Type returns PINGREQ.
func (*Pingreq) Type() byte { return PINGREQ }

// Type returns PINGRESP.
func (*Pingresp) Type() byte { return PINGRESP }

// Type returns DISCONNECT.
func (*Disconnect) Type() byte { return DISCONNECT }

// Type returns AUTH.
func (*Auth) Type() byte { return AUTH }

// Encode writes p as the protocol version says: V311 or V5.
func Encode(p Packet, version byte) ([]byte, error) {
	v5 := version == V5
	e := &encoder{}
	var flags byte
	switch p := p.(type) {
	case *Connect:
		encodeConnect(e, p)
	case *Connack:
		if p.SessionPresent {
			e.byte(1)
		} else {
			e.byte(0)
		}
		e.byte(p.Reason)
		if v5 {
			e.properties(p.Props)
		}
	case *Publish:
		// Fixed header flags: DUP bit 3, QoS bits 2-1, RETAIN bit 0
		// (3.1.1 §3.3.1, 5.0 §3.3.1). A QoS of 3 is written as given.
		flags = p.QoS&3<<1 | bit(p.Dup, 3) | bit(p.Retain, 0)
		if p.ByAlias {
			e.string("")
		} else {
			e.string(p.Topic)
		}
		if p.QoS > 0 {
			e.uint16(p.ID)
		}
		if v5 {
			e.properties(p.Props)
		}
		e.bytes(p.Payload)
	case *Ack:
		if p.Kind < PUBACK || p.Kind > PUBCOMP {
			return nil, fmt.Errorf("an Ack's Kind must be PUBACK to PUBCOMP, not %d", p.Kind)
		}
		if p.Kind == PUBREL {
			flags = 0b0010
		}
		e.uint16(p.ID)
		// MQTT 5 drops the reason code and property length when they say
		// nothing (5.0 §3.4.2.1, §3.4.2.2.1): the shortest form is written.
		if v5 && (p.Reason != 0 || len(p.Props) > 0) {
			e.byte(p.Reason)
			if len(p.Props) > 0 {
				e.properties(p.Props)
			}
		}
	case *Subscribe:
		flags = 0b0010
		e.uint16(p.ID)
		if v5 {
			e.properties(p.Props)
		}
		for _, s := range p.Subs {
			e.string(s.Filter)
			e.byte(s.QoS | bit(s.NoLocal, 2) | bit(s.RetainAsPublished, 3) | s.RetainHandling<<4)
		}
	case *Suback:
		e.uint16(p.ID)
		if v5 {
			e.properties(p.Props)
		}
		e.bytes(p.Reasons)
	case *Unsubscribe:
		flags = 0b0010
		e.uint16(p.ID)
		if v5 {
			e.properties(p.Props)
		}
		for _, f := range p.Filters {
			e.string(f)
		}
	case *Unsuback:
		e.uint16(p.ID)
		if v5 {
			e.properties(p.Props)
			e.bytes(p.Reasons)
		}
	case *Pingreq, *Pingresp:
	case *Disconnect:
		if v5 {
			reasonAndProps(e, p.Reason, p.Props)
		}
	case *Auth:
		if !v5 {
			return nil, errors.New("AUTH exists only in MQTT 5")
		}
		reasonAndProps(e, p.Reason, p.Props)
	default:
		return nil, fmt.Errorf("cannot encode %T", p)
	}
	if e.err != nil {
		return nil, fmt.Errorf("encoding packet type %d: %w", p.Type(), e.err)
	}
	if len(e.b) > MaxVarInt {
		return nil, fmt.Errorf("remaining length %d is over %d", len(e.b), MaxVarInt)
	}
	out := AppendVarInt([]byte{p.Type()<<4 | flags}, uint32(len(e.b)))
	return append(out, e.b...), nil
}

// reasonAndProps writes DISCONNECT's and AUTH's variable header in its
// shortest form: nothing for a reason of 0x00 with no properties (5.0
// §3.14.2.1, §3.15.2.1), the reason alone when there are no properties.
func reasonAndProps(e *encoder, reason byte, props Properties) {
	if reason == 0 && len(props) == 0 {
		return
	}
	e.byte(reason)
	if len(props) > 0 {
		e.properties(props)
	}
}

func encodeConnect(e *encoder, p *Connect) {
	v5 := p.Version == V5
	e.string(p.ProtocolName)
	e.byte(p.Version)
	// Connect Flags (3.1.1 §3.1.2.3, 5.0 §3.1.2.3): User Name bit 7,
	// Password bit 6, Will Retain bit 5, Will QoS bits 4-3, Will Flag bit 2,
	// Clean Start bit 1, bit 0 reserved.
	flags := bit(p.HasUsername, 7) | bit(p.HasPassword, 6) | bit(p.CleanStart, 1)
	if p.Will != nil {
		flags |= bit(p.Will.Retain, 5) | p.Will.QoS&3<<3 | 1<<2
	}
	e.byte(flags)
	e.uint16(p.KeepAlive)
	if v5 {
		e.properties(p.Props)
	}
	// Payload order: Client Identifier, Will Properties, Will Topic, Will
	// Payload, User Name, Password (3.1.1 §3.1.3, 5.0 §3.1.3).
	e.string(p.ClientID)
	if p.Will != nil {
		if v5 {
			e.properties(p.Will.Props)
		}
		e.string(p.Will.Topic)
		e.binary(p.Will.Payload)
	}
	if p.HasUsername {
		e.string(p.Username)
	}
	if p.HasPassword {
		e.binary(p.Password)
	}
}

func bit(on bool, n uint) byte {
	if on {
		return 1 << n
	}
	return 0
}

// ReadPacket reads one whole packet from r and decodes it. version decides the
// layout of every packet but CONNECT, which names its own. A packet whose size
// is over maxSize is refused before the body is read; 0 means no limit but
// the protocol's. The size is the whole Control Packet, fixed header included
// (5.0 §3.1.2.11.4 Maximum Packet Size, defined by §2.1.4), as read: a 3.1.1
// Remaining Length written longer than it needs counts at its written length.
func ReadPacket(r io.Reader, version byte, maxSize int) (Packet, error) {
	header := &countReader{r: r}
	br := byteReader{r: header}
	first, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	n, err := readVarInt(br, version == V5)
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	if size := header.n + int(n); maxSize > 0 && size > maxSize {
		return nil, fmt.Errorf("%w: %d bytes, over the limit of %d", ErrTooLarge, size, maxSize)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, unexpectedEOF(err)
	}
	return decodeBody(first, body, version)
}

// Decode decodes exactly one packet from b; bytes left over are an error.
func Decode(b []byte, version byte) (Packet, error) {
	r := bytes.NewReader(b)
	p, err := ReadPacket(r, version, 0)
	if err != nil {
		return nil, err
	}
	if r.Len() > 0 {
		return nil, fmt.Errorf("%d bytes after the packet", r.Len())
	}
	return p, nil
}

// unexpectedEOF reports a packet cut short: an EOF after the first byte is not
// a clean end of stream.
func unexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// byteReader reads single bytes from an io.Reader without buffering ahead, so
// nothing past the packet is consumed.
type byteReader struct{ r io.Reader }

// countReader counts the bytes read through it: ReadPacket reads the fixed
// header through one to learn its length.
type countReader struct {
	r io.Reader
	n int
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

func (b byteReader) ReadByte() (byte, error) {
	var one [1]byte
	_, err := io.ReadFull(b.r, one[:])
	return one[0], err
}

// requiredFlags are the fixed header flags each packet type must carry
// (3.1.1 §2.2.2 Table 2.2, 5.0 §2.1.3 Table 2-2); PUBLISH is checked apart.
var requiredFlags = [16]byte{PUBREL: 0b0010, SUBSCRIBE: 0b0010, UNSUBSCRIBE: 0b0010}

func decodeBody(first byte, body []byte, version byte) (Packet, error) {
	kind, flags := first>>4, first&0x0F
	v5 := version == V5
	d := &decoder{b: body}
	if kind == 0 || (kind == AUTH && !v5) {
		return nil, fmt.Errorf("%w: reserved packet type %d", ErrMalformed, kind)
	}
	if kind != PUBLISH && flags != requiredFlags[kind] {
		return nil, fmt.Errorf("%w: packet type %d with flags %04b, not %04b", ErrMalformed, kind, flags, requiredFlags[kind])
	}
	var p Packet
	switch kind {
	case CONNECT:
		p = decodeConnect(d)
	case CONNACK:
		c := &Connack{}
		ack := d.byte()
		if ack&0xFE != 0 {
			d.malformed("CONNACK reserved flag bits set: %08b", ack) // 5.0 [MQTT-3.2.2-1], 3.1.1 §3.2.2.1
		}
		c.SessionPresent = ack&1 == 1
		c.Reason = d.byte()
		if v5 {
			c.Props = d.properties(in(CONNACK))
		}
		p = c
	case PUBLISH:
		p = decodePublish(d, flags, v5)
	case PUBACK, PUBREC, PUBREL, PUBCOMP:
		a := &Ack{Kind: kind, ID: d.uint16()}
		// 5.0 §3.4.2.1: a Remaining Length of 2 means reason 0x00; below 4
		// there is no Property Length (§3.4.2.2.1). The same for §3.5-3.7.
		if v5 && len(d.b) > 0 {
			a.Reason = d.byte()
			if len(d.b) > 0 {
				a.Props = d.properties(in(kind))
			}
			if !slices.Contains(ackReasons[kind], a.Reason) {
				d.protocol(fmt.Sprintf("%s reason 0x%02X", Name(kind), a.Reason)) // [MQTT-3.4.2-1] and its like
			}
		}
		nonZeroID(d, a.ID)
		p = a
	case SUBSCRIBE:
		p = decodeSubscribe(d, v5)
	case SUBACK:
		s := &Suback{ID: d.uint16()}
		if v5 {
			s.Props = d.properties(in(SUBACK))
		}
		s.Reasons = d.rest()
		nonZeroID(d, s.ID)
		p = s
	case UNSUBSCRIBE:
		u := &Unsubscribe{ID: d.uint16()}
		if v5 {
			u.Props = d.properties(in(UNSUBSCRIBE))
		}
		for len(d.b) > 0 && d.err == nil {
			u.Filters = append(u.Filters, d.string())
		}
		if len(u.Filters) == 0 {
			d.protocol("UNSUBSCRIBE with no topic filter") // [MQTT-3.10.3-2]
		}
		nonZeroID(d, u.ID)
		p = u
	case UNSUBACK:
		u := &Unsuback{ID: d.uint16()}
		if v5 {
			u.Props = d.properties(in(UNSUBACK))
			u.Reasons = d.rest()
		}
		nonZeroID(d, u.ID)
		p = u
	case PINGREQ:
		p = &Pingreq{}
	case PINGRESP:
		p = &Pingresp{}
	case DISCONNECT:
		dis := &Disconnect{}
		if v5 {
			dis.Reason, dis.Props = decodeReasonAndProps(d, DISCONNECT)
			if !slices.Contains(disconnectReasons, dis.Reason) {
				d.protocol(fmt.Sprintf("DISCONNECT reason 0x%02X", dis.Reason)) // [MQTT-3.14.2-1]
			}
		}
		p = dis
	case AUTH:
		a := &Auth{}
		a.Reason, a.Props = decodeReasonAndProps(d, AUTH)
		p = a
	}
	if d.err == nil && len(d.b) > 0 {
		d.malformed("%d bytes left after packet type %d", len(d.b), kind)
	}
	if d.err != nil {
		return nil, d.err
	}
	return p, nil
}

// disconnectReasons is Table 3-10's Hex column (5.0 §3.14.2.1): "The
// Client or Server sending the DISCONNECT packet MUST use one of the
// DISCONNECT Reason Code values" [MQTT-3.14.2-1].
var disconnectReasons = []byte{0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E,
	0x8F, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2}

// ackReasons are the Hex columns of Tables 3-4 to 3-7 (5.0 §3.4.2.1 to
// §3.7.2.1), by packet type: "The Client or Server sending the PUBACK
// packet MUST use one of the PUBACK Reason Codes" [MQTT-3.4.2-1], and the
// same of PUBREC [MQTT-3.5.2-1], PUBREL [MQTT-3.6.2-1] and PUBCOMP
// [MQTT-3.7.2-1]. A code outside its table is data the protocol does not
// allow, a Protocol Error (5.0 §1.2), as for DISCONNECT.
var ackReasons = [PUBCOMP + 1][]byte{
	PUBACK:  {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
	PUBREC:  {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
	PUBREL:  {0x00, 0x92},
	PUBCOMP: {0x00, 0x92},
}

// decodeReasonAndProps reads DISCONNECT's and AUTH's variable header: absent
// means 0x00 and no properties (5.0 §3.14.2.1, §3.15.2.1).
func decodeReasonAndProps(d *decoder, kind byte) (byte, Properties) {
	if len(d.b) == 0 {
		return 0, nil
	}
	reason := d.byte()
	if len(d.b) == 0 {
		return reason, nil
	}
	return reason, d.properties(in(kind))
}

// nonZeroID checks that a Packet Identifier is not 0 (3.1.1 [MQTT-2.3.1-1],
// 5.0 §2.2.1); an acknowledgement repeats its packet's, so it holds there too.
func nonZeroID(d *decoder, id uint16) {
	if id == 0 {
		d.protocol("packet identifier 0")
	}
}

func decodeConnect(d *decoder) *Connect {
	c := &Connect{ProtocolName: d.string(), Version: d.byte()}
	if d.err == nil && c.ProtocolName != "MQTT" {
		// 5.0 §3.1.2.1 and 3.1.1 §3.1.2.1 name it "MQTT" [MQTT-3.1.2-1];
		// the harness speaks no other protocol.
		d.malformed("Protocol Name %q", c.ProtocolName)
	}
	v5 := c.Version == V5
	flags := d.byte()
	if flags&1 != 0 {
		d.malformed("CONNECT reserved flag set") // [MQTT-3.1.2-3]
	}
	if !v5 && flags&(1<<6) != 0 && flags&(1<<7) == 0 {
		d.protocol("Password Flag without the User Name Flag") // 3.1.1 [MQTT-3.1.2-22]
	}
	c.CleanStart = flags&(1<<1) != 0
	c.HasUsername = flags&(1<<7) != 0
	c.HasPassword = flags&(1<<6) != 0
	willQoS, willRetain := flags>>3&3, flags&(1<<5) != 0
	c.KeepAlive = d.uint16()
	if v5 {
		c.Props = d.properties(in(CONNECT))
		_, method := c.Props.Get(AuthenticationMethod)
		if _, data := c.Props.Get(AuthenticationData); data && !method {
			// 5.0 §3.1.2.11.10: "It is a Protocol Error to include
			// Authentication Data if there is no Authentication Method".
			d.protocol("Authentication Data without an Authentication Method")
		}
	}
	c.ClientID = d.string()
	if flags&(1<<2) != 0 {
		if willQoS == 3 {
			d.malformed("Will QoS 3")
		}
		c.Will = &Will{QoS: willQoS, Retain: willRetain}
		if v5 {
			c.Will.Props = d.properties(willProps)
			if rt, ok := c.Will.Props.Get(ResponseTopic); ok && strings.ContainsAny(rt.Str, "+#") {
				// design.md, CONNECT: "A malformed CONNECT includes one whose
				// Will's Response Topic holds + or #"; 5.0 [MQTT-3.3.2-14].
				d.malformed("Will Response Topic %q holds a wildcard", rt.Str)
			}
		}
		c.Will.Topic = d.string()
		c.Will.Payload = d.binary()
	} else if willQoS != 0 || willRetain {
		// 5.0 [MQTT-3.1.2-11] and -13, 3.1.1 [MQTT-3.1.2-13] and -15: the
		// flags parse, but the standard forbids them (a Protocol Error, 5.0
		// §1.2).
		d.protocol("Will QoS or Retain set without the Will Flag")
	}
	if c.HasUsername {
		c.Username = d.string()
	}
	if c.HasPassword {
		c.Password = d.binary()
	}
	return c
}

func decodePublish(d *decoder, flags byte, v5 bool) *Publish {
	p := &Publish{Dup: flags&8 != 0, QoS: flags >> 1 & 3, Retain: flags&1 != 0}
	if p.QoS == 3 {
		d.malformed("PUBLISH with QoS 3") // [MQTT-3.3.1-4]
	}
	if p.QoS == 0 && p.Dup {
		d.protocol("PUBLISH with QoS 0 and DUP set") // [MQTT-3.3.1-2]
	}
	p.Topic = d.string()
	if strings.ContainsAny(p.Topic, "+#") {
		d.malformed("Topic Name %q holds a wildcard", p.Topic) // [MQTT-3.3.2-2]
	}
	if p.QoS > 0 {
		p.ID = d.uint16()
		nonZeroID(d, p.ID)
	}
	if v5 {
		p.Props = d.properties(in(PUBLISH))
		if rt, ok := p.Props.Get(ResponseTopic); ok && strings.ContainsAny(rt.Str, "+#") {
			// "The Response Topic MUST NOT contain wildcard characters"
			// [MQTT-3.3.2-14]; design.md, PUBLISH: such a PUBLISH is a
			// malformed packet.
			d.malformed("Response Topic %q holds a wildcard", rt.Str)
		}
		if _, alias := p.Props.Get(TopicAlias); p.Topic == "" && !alias {
			// 5.0 §3.3.2.1: "It is a Protocol Error if the Topic Name is
			// zero length and there is no Topic Alias".
			d.protocol("empty Topic Name without a Topic Alias")
		}
	} else if p.Topic == "" {
		d.malformed("empty Topic Name") // 3.1.1 [MQTT-4.7.3-1]
	}
	p.Payload = d.rest()
	return p
}

func decodeSubscribe(d *decoder, v5 bool) *Subscribe {
	s := &Subscribe{ID: d.uint16()}
	if v5 {
		s.Props = d.properties(in(SUBSCRIBE))
	}
	for len(d.b) > 0 && d.err == nil {
		sub := Subscription{Filter: d.string()}
		opts := d.byte()
		sub.QoS = opts & 3
		sub.NoLocal = opts&(1<<2) != 0
		sub.RetainAsPublished = opts&(1<<3) != 0
		sub.RetainHandling = opts >> 4 & 3
		switch {
		case !v5 && sub.QoS == 3:
			d.malformed("subscription QoS 3") // 3.1.1 §3.8.3 [MQTT-3-8.3-4], so spelled
		case !v5 && opts&0xFC != 0:
			d.malformed("3.1.1 subscription reserved bits set: %08b", opts) // 3.1.1 §3.8.3 [MQTT-3-8.3-4], so spelled
		case v5 && opts&0xC0 != 0:
			d.malformed("subscription reserved bits set: %08b", opts) // [MQTT-3.8.3-5]
		case sub.QoS == 3:
			d.protocol("Maximum QoS 3") // 5.0 §3.8.3.1: "a Protocol Error"
		case sub.RetainHandling == 3:
			d.protocol("Retain Handling 3")
		}
		s.Subs = append(s.Subs, sub)
	}
	if len(s.Subs) == 0 {
		d.protocol("SUBSCRIBE with no topic filter") // 5.0 [MQTT-3.8.3-2], 3.1.1 [MQTT-3.8.3-3]
	}
	nonZeroID(d, s.ID)
	return s
}
