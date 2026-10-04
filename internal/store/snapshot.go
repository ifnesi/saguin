package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"time"
)

// A snapshot file is what a memory channel survives a graceful shutdown
// as, and it is the whole of that channel's durability: no snapshot, no
// data (invariant 14).
//
// The layout is written out by hand rather than left to a self-describing
// encoder. gob and JSON both turn a field that moved, or a version that
// does not match, into a zero value and report success - a broker that
// starts with plausible-looking wrong state and says nothing. Here a file
// this broker does not fully understand is refused by name.
//
// The header is a contract that holds across every format version:
//
//	 0  magic           8 bytes
//	 8  format version  uint32
//	12  kind            uint8
//	13  writer          24 bytes, NUL-padded
//	37  written at      int64, unix nanoseconds
//	45  ...             the body, which is what the version describes
//	    checksum        uint32, CRC32C of every byte before it
//
// Those fields never move, and the last four bytes are always the
// checksum. That is what lets a broker meeting a file of another format say
// which format it holds, which build wrote it and when, rather than
// shrugging at bytes it cannot parse.
//
// Everything is little-endian.
const (
	// formatVersion is the one format this broker writes and reads. A file
	// of any other is refused by name - the format it holds and the build
	// that wrote it - and nothing converts one: a memory provider's data
	// does not cross a change of format (RFC 0004).
	formatVersion = 21

	headerSize  = 45
	writerSize  = 24
	trailerSize = 4
)

// magic identifies a saguin snapshot. The high bytes are there so that a
// tool which sniffs a file for text cannot mistake it for any, and 0x1a
// stops `cat` on a terminal that honours it.
var magic = [8]byte{'s', 'a', 'g', 'u', 'i', 'n', 0x1a, 0x00}

// The kinds of file a snapshot directory holds. The byte is in the header
// so that one is never misread as the other - a manifest that happened to
// parse as channels would restore a broker to nothing.
const (
	kindChannels uint8 = 1
	kindManifest uint8 = 2
	// kindSessions is a memory provider's persistent sessions and their
	// in-flight tables: one file per provider, beside its channels.
	kindSessions uint8 = 3
	// Kind 4 was a provider's unfinished exactly-once publishes, which are
	// now in their channel's file (HeldPublish). Not reused, so a file of it
	// is refused rather than read as something else.
	//
	// Kind 5 was a provider's shared-group backlogs, which are now the
	// broadcast log behind each group's cursor, in the sessions file. Not
	// reused, for the same reason.
	// kindBroadcast is that provider's broadcast log: every message a
	// session that outlives its connection may still be owed. It travels
	// with the sessions file because a cursor is worth nothing without the
	// log it points into, and its body is one channel section, because the
	// log is kept in an append channel's store under a reserved name
	// (BroadcastLog).
	kindBroadcast uint8 = 6
)

// Castagnoli rather than the IEEE polynomial: it is the one with a
// hardware instruction behind it on every processor saguin runs on.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Kind is a channel's semantic as a snapshot records it. It repeats
// channel.Type on purpose - these numbers are on disk, and they must not
// change because somebody renamed a constant in another package.
type Kind uint8

const (
	KindAppend Kind = 1
	KindLatest Kind = 2
	KindQueue  Kind = 3
)

func (k Kind) String() string {
	switch k {
	case KindAppend:
		return "append"
	case KindLatest:
		return "latest"
	case KindQueue:
		return "queue"
	}
	return fmt.Sprintf("unknown kind %d", uint8(k))
}

// Position is a durable consumer's place in one channel.
//
// It is stored with the moment it last moved and the expiry interval of
// the session it belongs to. A position belongs to a session, and one
// kept past its session's life is a row for every client id anybody ever
// mistyped, with nothing that ever takes it out again (invariant 13).
// DropExpiredPositions applies the rule when the file is loaded.
type Position struct {
	Reader    string // a reader name: MQTTReader's or BridgeReader's
	Offset    uint64
	LastSeen  time.Time
	ExpiresIn time.Duration
}

// ChannelState is one channel as a snapshot holds it.
type ChannelState struct {
	Name string
	Kind Kind

	// Next and Floor are stored, never recomputed from the records that
	// survive. MAX(offset)+1 restarts at 1 on a channel retention has
	// emptied, handing a stored consumer position at unrelated data
	// (invariant 9); MIN(offset) reads as "nothing was ever removed" on
	// that same channel, which is the loss invariant 1 exists to report.
	Next  uint64
	Floor uint64

	Records   []Record // append and latest
	Items     []Item   // queue
	Positions []Position

	// Holds are the exactly-once publishes the channel was holding for
	// their release (HeldPublish), which a graceful restart keeps as it
	// keeps the records: the publisher was told at its PUBREC that the
	// message was taken.
	Holds []HeldPublish
}

// Snapshot is the content of one snapshot file: the channels written
// together, and who wrote them when.
//
// A queue and its dead-letter channel are written into the same file, so
// that the move out of the queue and into the DLQ cannot be recorded by
// half - a crash between two renames would lose the record, which is the
// outcome dead-lettering exists to prevent (invariant 5). Every other
// channel is a file of its own.
type Snapshot struct {
	Writer    string // the saguin version that wrote it, for error messages only
	WrittenAt time.Time
	Channels  []ChannelState
}

// Encode writes the snapshot and its trailing checksum.
func (s *Snapshot) Encode(w io.Writer) error {
	sum := crc32.New(crcTable)
	e := &encoder{w: io.MultiWriter(w, sum)}

	writeHeader(e, kindChannels, s.Writer, s.WrittenAt)

	e.u32(uint32(len(s.Channels)))
	for i := range s.Channels {
		s.Channels[i].encode(e)
	}
	return seal(e, w, sum)
}

// writeHeader writes the fields every kind of snapshot file begins with,
// at the offsets they hold in every format version.
func writeHeader(e *encoder, kind uint8, writer string, at time.Time) {
	e.write(magic[:])
	e.u32(formatVersion)
	e.u8(kind)

	// Truncated rather than refused: the writer string reaches nothing but
	// an error message, and failing a shutdown snapshot over the length of
	// a version string would be the worst trade in the design.
	var name [writerSize]byte
	copy(name[:], writer)
	e.write(name[:])

	e.time(at)
}

// seal finishes a file with the checksum of everything before it.
//
// The checksum trails rather than sitting in the header. A file that
// stopped early has no trailer at all, so a truncated write is caught by
// the same check as a flipped bit - and the writer never seeks back over
// bytes it has already handed to the kernel.
func seal(e *encoder, w io.Writer, sum hash.Hash32) error {
	if e.err != nil {
		return e.err
	}
	var trailer [trailerSize]byte
	binary.LittleEndian.PutUint32(trailer[:], sum.Sum32())
	_, err := w.Write(trailer[:])
	return err
}

// header is what every snapshot file begins with, whatever it holds.
type header struct {
	version   uint32
	kind      uint8
	writer    string
	writtenAt time.Time
}

// describe says who wrote a file and when, in words that go straight into
// an error an operator has to act on.
func (h header) describe() string {
	wrote := "an unknown version of saguin"
	if h.writer != "" {
		wrote = "saguin " + h.writer
	}
	when := "an unrecorded time"
	if !h.writtenAt.IsZero() {
		when = h.writtenAt.UTC().Format(time.RFC3339)
	}
	return "written by " + wrote + " at " + when
}

// readHeader verifies a file as far as its kind and returns its body:
// everything between the header and the checksum.
//
// The checks run in this order on purpose. A damaged file has damaged
// version bytes too, so verifying the checksum before the version is what
// keeps "format 3947382" from describing the wrong problem.
func readHeader(b []byte, want uint8) (header, []byte, error) {
	var h header
	if len(b) < headerSize+trailerSize {
		return h, nil, fmt.Errorf("too short to be a saguin snapshot: %d bytes", len(b))
	}
	if !bytes.Equal(b[:len(magic)], magic[:]) {
		return h, nil, fmt.Errorf("not a saguin snapshot: the file does not start with saguin's magic bytes")
	}

	h = header{
		version:   binary.LittleEndian.Uint32(b[8:12]),
		kind:      b[12],
		writer:    string(bytes.TrimRight(b[13:13+writerSize], "\x00")),
		writtenAt: decodeTime(int64(binary.LittleEndian.Uint64(b[37:45]))),
	}

	body, trailer := b[:len(b)-trailerSize], b[len(b)-trailerSize:]
	wantSum := binary.LittleEndian.Uint32(trailer)
	if got := crc32.Checksum(body, crcTable); got != wantSum {
		return h, nil, fmt.Errorf(
			"damaged or not written completely: its checksum is %08x and its contents come to %08x (%s)",
			wantSum, got, h.describe())
	}
	if h.version != formatVersion {
		return h, nil, fmt.Errorf("format %d, %s; this broker reads format %d",
			h.version, h.describe(), formatVersion)
	}
	if h.kind != want {
		return h, nil, fmt.Errorf("holds kind %d, not kind %d (%s)", h.kind, want, h.describe())
	}
	return h, body[headerSize:], nil
}

func (c *ChannelState) encode(e *encoder) {
	e.str(c.Name)
	e.u8(uint8(c.Kind))
	e.u64(c.Next)
	e.u64(c.Floor)

	e.u32(uint32(len(c.Records)))
	for i := range c.Records {
		encodeRecord(e, &c.Records[i])
	}

	e.u32(uint32(len(c.Items)))
	for i := range c.Items {
		it := &c.Items[i]
		encodeRecord(e, &it.Record)
		e.u32(uint32(it.Attempts))
		e.time(it.FirstSeen)
		e.time(it.LastSeen)
		// State, Epoch, DeliveryID, Holder and LeaseUntil are absent on
		// purpose: after a restart no record is in flight, and a restored
		// deadline belongs to a session that no longer exists (invariant
		// 15). Attempts survives, so a record that has spent its attempts
		// is dead-lettered rather than starting over; FirstSeen and
		// LastSeen survive because they are the history a dead-lettered
		// record carries in its saguin-dlq-first and saguin-dlq-last
		// headers, and a restart must not erase why something failed.
	}

	e.u32(uint32(len(c.Positions)))
	for _, p := range c.Positions {
		e.str(p.Reader)
		e.u64(p.Offset)
		e.time(p.LastSeen)
		e.i64(int64(p.ExpiresIn))
	}

	e.u32(uint32(len(c.Holds)))
	for i := range c.Holds {
		h := &c.Holds[i]
		e.str(h.Exchange.Client)
		e.u32(uint32(h.Exchange.PacketID))
		e.time(h.HeldAt)
		encodeRecord(e, &h.Record)
	}
}

func encodeRecord(e *encoder, r *Record) {
	e.u64(r.Offset)
	e.str(r.MessageID)
	e.str(r.Topic)
	e.time(r.Timestamp)

	e.u32(uint32(len(r.Headers)))
	// **In the publisher's order.** One state must encode to one sequence
	// of bytes, or nobody can tell a file that changed from one that was
	// merely rewritten, and an ordered list already does: the property is
	// kept by storing the order rather than by imposing one.
	for _, h := range r.Headers {
		e.str(h.Key)
		e.str(h.Value)
	}

	e.blob(r.Payload)

	// The publish properties and what saguin keeps beside them. A record
	// carrying none of them writes the flag byte and nothing else, which is
	// almost every record.
	if !r.HasProps() {
		e.u8(0)
		return
	}
	e.u8(1)
	e.str(r.ContentType)
	e.str(r.ResponseTopic)
	e.blob(r.CorrelationData)
	e.props(r.Props())
	e.u32(r.MessageExpiry)
	e.u8(b2u8(r.Retain))
	e.str(r.Bridge)
	e.str(r.Publisher)
	e.u8(r.QoS)
	e.u8(b2u8(r.ForGroups))
	e.str(r.OwedTo)
}

// b2u8 writes a bool as the one byte the format has for one.
func b2u8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

// DecodeSnapshot reads a snapshot and verifies it.
//
// It takes the bytes rather than a reader because the checksum trails the
// content: nothing in the file may be trusted until every byte before the
// trailer has been read, so there is no honest way to decode as it
// streams. A memory channel's snapshot is bounded by the memory the
// channel was already holding, so reading it whole costs nothing new.
//
// Every error names what was wrong. None of them is ever an empty start,
// which is indistinguishable from a fresh install and destroys the
// evidence (invariant 14).
func DecodeSnapshot(b []byte) (*Snapshot, error) {
	h, body, err := readHeader(b, kindChannels)
	if err != nil {
		return nil, err
	}

	s := &Snapshot{Writer: h.writer, WrittenAt: h.writtenAt}
	d := &decoder{b: body}

	n := d.count(minChannelSize)
	s.Channels = make([]ChannelState, 0, n)
	for range n {
		var c ChannelState
		c.decode(d)
		s.Channels = append(s.Channels, c)
	}
	if d.err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), d.err)
	}
	if d.i != len(body) {
		return nil, fmt.Errorf("%s: %d bytes remain after the last channel", h.describe(), len(body)-d.i)
	}
	for i := range s.Channels {
		if err := s.Channels[i].validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", h.describe(), err)
		}
	}
	return s, nil
}

func (c *ChannelState) decode(d *decoder) {
	c.Name = d.str()
	c.Kind = Kind(d.u8())
	c.Next = d.u64()
	c.Floor = d.u64()

	n := d.count(minRecordSize)
	c.Records = make([]Record, 0, n)
	for range n {
		c.Records = append(c.Records, decodeRecord(d))
	}

	n = d.count(minItemSize)
	c.Items = make([]Item, 0, n)
	for range n {
		it := Item{Record: decodeRecord(d)}
		it.Attempts = int(d.u32())
		it.FirstSeen = d.time()
		it.LastSeen = d.time()
		c.Items = append(c.Items, it)
	}

	n = d.count(minPositionSize)
	c.Positions = make([]Position, 0, n)
	for range n {
		c.Positions = append(c.Positions, Position{
			Reader:    d.str(),
			Offset:    d.u64(),
			LastSeen:  d.time(),
			ExpiresIn: time.Duration(d.i64()),
		})
	}

	n = d.count(minHoldSize)
	c.Holds = make([]HeldPublish, 0, n)
	for range n {
		h := HeldPublish{Exchange: Exchange{Client: d.str()}}
		h.Exchange.PacketID = uint16(d.u32())
		h.HeldAt = d.time()
		h.Record = decodeRecord(d)
		c.Holds = append(c.Holds, h)
	}
}

func decodeRecord(d *decoder) Record {
	r := Record{
		Offset:    d.u64(),
		MessageID: d.str(),
		Topic:     d.str(),
		Timestamp: d.time(),
	}
	if n := d.count(minHeaderSize); n > 0 {
		r.Headers = make([]Header, 0, n)
		for range n {
			k := d.str()
			r.Headers = append(r.Headers, Header{Key: k, Value: d.str()})
		}
	}
	r.Payload = d.blob()

	// The flag says whether the rest follows: a record carrying none of it
	// is the flag and nothing else.
	if d.u8() == 1 {
		r.ContentType = d.str()
		r.ResponseTopic = d.str()
		r.CorrelationData = d.blob()
		p := d.props()
		r.PayloadFormatFlag, r.PayloadFormat = p.PayloadFormatFlag, p.PayloadFormat
		r.ContentTypeEmpty, r.ResponseTopicEmpty, r.CorrelationDataEmpty =
			p.ContentTypeEmpty, p.ResponseTopicEmpty, p.CorrelationDataEmpty
		r.MessageExpiry = d.u32()
		r.Retain = d.u8() == 1
		r.Bridge = d.str()
		r.Publisher = d.str()
		r.QoS = d.u8()
		r.ForGroups = d.u8() == 1
		r.OwedTo = d.str()
	}
	return r
}

// validate rejects a file that parsed but cannot be true. A checksum
// proves the bytes are the bytes that were written; it says nothing about
// whether the writer was right, and every rule below is one a later read
// depends on.
func (c *ChannelState) validate() error {
	switch c.Kind {
	case KindAppend, KindLatest, KindQueue:
	default:
		return fmt.Errorf("channel %q: %s", c.Name, c.Kind)
	}
	if c.Name == "" {
		return fmt.Errorf("a channel section has no name")
	}
	// Offsets start at 1. A next of 0 would hand the first record after a
	// restore an offset below the floor, where it could never be read.
	if c.Next < 1 {
		return fmt.Errorf("channel %q: the next offset is 0", c.Name)
	}
	if c.Floor < 1 {
		return fmt.Errorf("channel %q: the retention floor is 0", c.Name)
	}
	if c.Floor > c.Next {
		return fmt.Errorf("channel %q: the retention floor %d is past the next offset %d", c.Name, c.Floor, c.Next)
	}
	if c.Kind == KindQueue && len(c.Records) > 0 {
		return fmt.Errorf("channel %q is a queue but carries %d loose records", c.Name, len(c.Records))
	}
	if c.Kind != KindQueue && len(c.Items) > 0 {
		return fmt.Errorf("channel %q is an %s but carries %d queue items", c.Name, c.Kind, len(c.Items))
	}

	// Reads walk the slice in order and stop at the first record past
	// what the caller asked for, so an out-of-order restore would hide
	// every record behind the first one that went backwards.
	var last uint64
	ordered := c.Kind != KindLatest
	check := func(off uint64) error {
		if off >= c.Next {
			return fmt.Errorf("channel %q: offset %d is at or past the next offset %d", c.Name, off, c.Next)
		}
		if ordered {
			if off < c.Floor {
				return fmt.Errorf("channel %q: offset %d is below the retention floor %d", c.Name, off, c.Floor)
			}
			if off <= last && last != 0 {
				return fmt.Errorf("channel %q: offset %d follows offset %d, which is not ascending", c.Name, off, last)
			}
			last = off
		}
		return nil
	}
	for i := range c.Records {
		if err := check(c.Records[i].Offset); err != nil {
			return err
		}
	}
	for i := range c.Items {
		if err := check(c.Items[i].Offset); err != nil {
			return err
		}
	}
	for _, p := range c.Positions {
		// A position at Next is a consumer that is caught up, which is
		// ordinary. Past it is not.
		if p.Offset > c.Next {
			return fmt.Errorf("channel %q: %q holds position %d, past the next offset %d",
				c.Name, p.Reader, p.Offset, c.Next)
		}
	}
	// A hold has no offset until its release gives it one, names the
	// exchange its PUBREL will name, and is held once.
	seen := make(map[Exchange]bool, len(c.Holds))
	for _, h := range c.Holds {
		switch {
		case h.Exchange.Client == "" || h.Exchange.PacketID == 0:
			return fmt.Errorf("channel %q: a held publish names no exchange (client %q, packet %d)",
				c.Name, h.Exchange.Client, h.Exchange.PacketID)
		case h.Record.Offset != 0:
			return fmt.Errorf("channel %q: the publish held for %q packet %d carries offset %d, and a hold "+
				"has none", c.Name, h.Exchange.Client, h.Exchange.PacketID, h.Record.Offset)
		case seen[h.Exchange]:
			return fmt.Errorf("channel %q: %q packet %d is held twice", c.Name, h.Exchange.Client,
				h.Exchange.PacketID)
		}
		seen[h.Exchange] = true
	}
	return nil
}

// DropExpiredPositions removes every stored position whose session would
// have expired while the broker was down, and reports how many went.
//
// A position with no expiry left is dropped rather than kept, so a client
// id that never comes back does not hold a row for ever (invariant 13). A
// session that asked never to expire is stored with an interval long
// enough that this never fires, which is what MQTT's own 0xFFFFFFFF means.
func (s *Snapshot) DropExpiredPositions(now time.Time) int {
	dropped := 0
	for i := range s.Channels {
		c := &s.Channels[i]
		kept := c.Positions[:0]
		for _, p := range c.Positions {
			if PositionExpires(p.Reader) && p.LastSeen.Add(p.ExpiresIn).Before(now) {
				dropped++
				continue
			}
			kept = append(kept, p)
		}
		c.Positions = kept
	}
	return dropped
}

// Export copies the log's state for a snapshot. next and floor are taken
// as they are stored, not derived from the records that survive
// (invariants 1 and 9).
func (l *Log) Export() (next, floor uint64, records []Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// live(), not records: everything before the trim's start index has been
	// removed, and a snapshot holding it would bring it back at the next
	// start - below the floor that was written beside it, which is a channel
	// whose own two numbers disagree with its contents. The broadcast log's
	// holes are left out for the same reason: each would come back as a
	// message nobody is owed.
	live := l.live()
	records = make([]Record, 0, len(live)-len(l.holes))
	for _, r := range live {
		if !l.hole(r.Offset) {
			records = append(records, r)
		}
	}
	return l.next, l.floor, records
}

// RestoreLog rebuilds a log from a snapshot. Reader names are taken exactly
// as they arrive: every one in a file this broker reads carries its scheme.
func RestoreLog(next, floor uint64, records []Record, positions []Position) *Log {
	l := &Log{next: next, floor: floor, records: records, positions: map[string]Position{}}
	// Summed once at startup rather than stored, because a snapshot is read
	// whole anyway and the records are already in hand. A store that does
	// not read its records at startup keeps the number instead.
	for i := range records {
		l.held += RecordSize(records[i])
	}
	for _, p := range positions {
		l.positions[p.Reader] = p
	}
	return l
}

// BroadcastSnapshot is what a memory provider's broadcast log survives a
// graceful shutdown as, in its own file beside the sessions (RFC 0004).
//
// Next and Floor are the log's own, as it stores them, and never derived from
// the records that survive: a log every session has read to the end holds no
// records at all, and still has to say where it had got to, or its next
// message takes an offset a stored cursor already points past (invariant 9).
type BroadcastSnapshot struct {
	Writer    string
	WrittenAt time.Time
	Next      uint64
	Floor     uint64
	Records   []Record

	// Positions are the sessions' cursors: each durable session's reader
	// position on the log, kept as a durable consumer's is on a channel.
	Positions []Position

	// Holds are the exactly-once broadcasts the log holds for their release
	// (HeldPublish), written in the channel section's holds as a channel's
	// are, so the file's format is the channel section's.
	Holds []HeldPublish
}

// Encode writes the broadcast log file and its trailing checksum. The body is
// one channel section named BroadcastLog, with the sessions' cursors as its
// reader positions and no queue items.
func (s *BroadcastSnapshot) Encode(w io.Writer) error {
	sum := crc32.New(crcTable)
	e := &encoder{w: io.MultiWriter(w, sum)}
	writeHeader(e, kindBroadcast, s.Writer, s.WrittenAt)
	c := ChannelState{Name: BroadcastLog, Kind: KindAppend, Next: s.Next, Floor: s.Floor,
		Records: s.Records, Positions: s.Positions, Holds: s.Holds}
	c.encode(e)
	return seal(e, w, sum)
}

// DecodeBroadcast reads a broadcast log file. A file that is damaged,
// truncated, from a format this broker does not read, or that parses into
// something that is not the log is an error, and the broker does not start
// on it: an empty log would restart its offsets under every stored cursor
// (invariants 9 and 14).
func DecodeBroadcast(b []byte) (*BroadcastSnapshot, error) {
	h, body, err := readHeader(b, kindBroadcast)
	if err != nil {
		return nil, err
	}
	d := &decoder{b: body}
	var c ChannelState
	c.decode(d)
	if d.err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), d.err)
	}
	if d.i != len(body) {
		return nil, fmt.Errorf("%s: %d bytes remain after the broadcast log", h.describe(), len(body)-d.i)
	}
	if err := validateBroadcast(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", h.describe(), err)
	}
	return &BroadcastSnapshot{Writer: h.writer, WrittenAt: h.writtenAt,
		Next: c.Next, Floor: c.Floor, Records: c.Records, Positions: c.Positions, Holds: c.Holds}, nil
}

// validateBroadcast refuses a section that parsed but is not the broadcast
// log. The name and kind come first, because an append section is what the
// ordering rules below are written for; then a channel's own rules - offsets
// ascending, inside the floor and the next offset, and no cursor past next -
// since a cursor is resumed against exactly those. The rules refuse queue
// items too, which only a queue carries.
func validateBroadcast(c *ChannelState) error {
	if c.Name != BroadcastLog || c.Kind != KindAppend {
		return fmt.Errorf("holds the %s section %q, not the broadcast log", c.Kind, c.Name)
	}
	return c.validate()
}

// RestoreBroadcastLog rebuilds a session provider's broadcast log from its
// file, with the sessions' cursors as its positions: RestoreLog, for a log
// that may also Remove.
func RestoreBroadcastLog(next, floor uint64, records []Record, positions []Position) *Log {
	l := RestoreLog(next, floor, records, positions)
	l.gapped = true
	return l
}

// Export copies the latest channel's state for a snapshot.
func (l *Latest) Export() (next uint64, records []Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Record, 0, len(l.current))
	for _, r := range l.current {
		out = append(out, r)
	}
	sortByOffset(out)
	return l.next, out
}

// RestoreLatest rebuilds a latest channel from a snapshot, keyed by each
// record's own topic.
func RestoreLatest(next uint64, records []Record) *Latest {
	l := &Latest{current: make(map[string]Record, len(records)), next: next}
	for _, r := range records {
		l.current[r.Topic] = r
	}
	return l
}

// Export copies the queue's items for a snapshot, in offset order.
func (q *Queue) Export() (next uint64, items []Item) {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Item, 0, len(q.order))
	for _, off := range q.order {
		if it, ok := q.items[off]; ok {
			out = append(out, *it)
		}
	}
	return q.next, out
}

// RestoreQueue rebuilds a queue from a snapshot.
//
// Every item comes back Available with no epoch, no Delivery ID, no
// holder and no deadline, whatever the caller passed. After a restart no
// record is in flight: a restored deadline belongs to a session that no
// longer exists, so the work stalls for the length of a timeout nobody is
// racing, and a restored Delivery ID can be resolved by a client retrying
// across the very restart that interrupted it (invariant 15). This is the
// enforcement point, not the decoder, because it holds for any caller.
func RestoreQueue(next uint64, items []Item) *Queue {
	q := &Queue{items: make(map[uint64]*Item, len(items)), next: next}
	for _, it := range items {
		it.State = Available
		it.Epoch = 0
		it.DeliveryID = ""
		it.Holder = ""
		it.LeaseUntil = time.Time{}

		stored := it
		q.items[it.Offset] = &stored
		q.order = append(q.order, it.Offset)
		q.held += RecordSize(it.Record)
	}
	return q
}

// encoder writes the primitives, holding the first error rather than
// making every call site check one.
type encoder struct {
	w   io.Writer
	err error
}

func (e *encoder) write(b []byte) {
	if e.err != nil {
		return
	}
	_, e.err = e.w.Write(b)
}

func (e *encoder) u8(v uint8) { e.write([]byte{v}) }

// The bits of the byte that follows a record's, or a Will's, Content Type,
// Response Topic and Correlation Data. Bit 0 was the whole byte once - "a
// Payload Format Indicator follows" - so what was written before the other
// three existed reads the same.
const (
	propPayloadFormat = 1 << iota
	propContentTypeEmpty
	propResponseTopicEmpty
	propCorrelationDataEmpty
	propKnown = propCorrelationDataEmpty<<1 - 1
)

// props writes that byte, and the Payload Format Indicator when it says one
// follows. The three other bits are the properties that were sent present
// and empty, which the three values above them cannot say.
func (e *encoder) props(p Props) {
	var m uint8
	if p.PayloadFormatFlag {
		m |= propPayloadFormat
	}
	if p.ContentTypeEmpty {
		m |= propContentTypeEmpty
	}
	if p.ResponseTopicEmpty {
		m |= propResponseTopicEmpty
	}
	if p.CorrelationDataEmpty {
		m |= propCorrelationDataEmpty
	}
	e.u8(m)
	if p.PayloadFormatFlag {
		e.u8(p.PayloadFormat)
	}
}

func (e *encoder) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	e.write(b[:])
}

func (e *encoder) u64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	e.write(b[:])
}

func (e *encoder) i64(v int64) { e.u64(uint64(v)) }

func (e *encoder) blob(b []byte) {
	e.u32(uint32(len(b)))
	e.write(b)
}

func (e *encoder) str(s string) { e.blob([]byte(s)) }

// time stores the zero time as 0. UnixNano on a zero Time is a number
// nothing means, and a decoder that read it back would produce a date in
// 1754 rather than "not set".
func (e *encoder) time(t time.Time) {
	if t.IsZero() {
		e.i64(0)
		return
	}
	e.i64(t.UnixNano())
}

// The smallest each repeated thing can encode to, used to refuse a count
// the file could not possibly contain before the memory for it is taken.
const (
	minRecordSize   = 8 + 4 + 4 + 8 + 4 + 4 // offset, two strings, time, header count, payload
	minItemSize     = minRecordSize + 4 + 8 + 8
	minPositionSize = 4 + 8 + 8 + 8
	minHoldSize     = 4 + 4 + 8 + minRecordSize // client, packet identifier, time, record
	minHeaderSize   = 4 + 4
	minChannelSize  = 4 + 1 + 8 + 8 + 4 + 4 + 4
)

// decoder reads the primitives out of a buffer that is already known to
// be complete and undamaged. Every read is bounded by the bytes actually
// present, so nothing the file declares can make the broker allocate more
// than the file's own size (invariant 13).
type decoder struct {
	b   []byte
	i   int
	err error
}

func (d *decoder) fail(format string, args ...any) {
	if d.err == nil {
		d.err = fmt.Errorf(format, args...)
	}
}

func (d *decoder) bytes(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.b)-d.i {
		d.fail("the file ends inside a value that declares %d more bytes, with %d left", n, len(d.b)-d.i)
		return nil
	}
	b := d.b[d.i : d.i+n]
	d.i += n
	return b
}

func (d *decoder) u8() uint8 {
	b := d.bytes(1)
	if b == nil {
		return 0
	}
	return b[0]
}

// props reads what encoder.props wrote.
func (d *decoder) props() (p Props) {
	m := d.u8()
	if m&^propKnown != 0 {
		d.fail("its publish property flags 0x%02x name a property this broker does not know", m)
		return p
	}
	if m&propPayloadFormat != 0 {
		p.PayloadFormatFlag = true
		p.PayloadFormat = d.u8()
	}
	p.ContentTypeEmpty = m&propContentTypeEmpty != 0
	p.ResponseTopicEmpty = m&propResponseTopicEmpty != 0
	p.CorrelationDataEmpty = m&propCorrelationDataEmpty != 0
	return p
}

func (d *decoder) u32() uint32 {
	b := d.bytes(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (d *decoder) u64() uint64 {
	b := d.bytes(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *decoder) i64() int64 { return int64(d.u64()) }

func (d *decoder) str() string { return string(d.bytes(int(d.u32()))) }

func (d *decoder) blob() []byte {
	b := d.bytes(int(d.u32()))
	if len(b) == 0 {
		return nil
	}
	// Copied rather than aliased. A payload pointing into the file buffer
	// would hold the whole file alive - every header and every record
	// beside it - for as long as the broker runs.
	return append([]byte(nil), b...)
}

func (d *decoder) time() time.Time { return decodeTime(d.i64()) }

func decodeTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// count reads a length that is about to drive an allocation, and refuses
// one the remaining bytes could not hold. The check runs before the
// allocation, not after the short read that would eventually follow: a
// count of four billion in a damaged file otherwise reserves the memory
// first, and on an edge box that is the OOM killer rather than an error
// message (invariant 13).
func (d *decoder) count(minEach int) int {
	n := int(d.u32())
	if d.err != nil {
		return 0
	}
	if n < 0 || n > (len(d.b)-d.i)/minEach {
		d.fail("a count of %d cannot fit in the %d bytes that remain", n, len(d.b)-d.i)
		return 0
	}
	return n
}
