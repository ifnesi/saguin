# RFC 0004 - Storage

**Status:** Draft
**Authoritative on:** how a channel's records are kept, what each storage
provider promises and what it does not, and the two on-disk formats.

What a client is guaranteed is RFC 0003, and where the two could be read
as saying different things RFC 0003 is right; the configuration keys are
RFC 0002. What is left here is the mechanism: what is written, what is
deliberately not written, and which order things reach the disk in.

## Two providers: memory and sqlite

A **storage provider** is a place records are kept. Every channel names
one, and a channel's records, its counters and its consumers' positions
all live in the provider it named - nothing about a channel is ever split
across two.

| | memory, with a snapshot | sqlite |
|---|---|---|
| Survives a graceful shutdown | yes, as far as the last snapshot | yes |
| Survives a crash, an OOM kill, `kill -9` | no | yes, whole |
| Survives a power cut | no | less the last commits |
| What a record costs | a write to memory | a transaction |
| Readable by other tools | at shutdown, as a snapshot file | while the broker runs, with `sqlite3` |

The rows are the whole trade, and [invariants.md](../invariants.md) 14 is
the rule behind them: memory durability is exactly the last successful
snapshot - what the operator chose in exchange for a write that costs
nothing - and SQLite's power-cut loss of the last unsynced commits is the
same class, written wherever the durability is promised.

A broker whose configuration gives no channel anywhere to keep anything
says so at startup. Silence about it is the same surprise, arriving later.

## What a store is asked for

The broker declares three interfaces and does not care which
implementation is behind one. An `append` channel is asked for:

```
Append(Record) (Record, error)          ReadFrom(offset) ([]Record, error)
ReadFromN(offset, max) ([]Record, error)
Next() uint64                           Floor() uint64
Bytes() int64                           LowestPosition() uint64
FirstAtOrAfter(time) (offset, bool, error)
SetMaxBytes(bytes)
Trim(before, maxBytes) (removed, freed, error)
Position(reader) (Position, bool, error)
SavePosition(Position) error            DropPosition(reader) (bool, error)
```

`FirstAtOrAfter` is what a seek by time resolves against, and both stores
answer it by looking at every candidate rather than by bisecting: the
timestamp is the broker's receipt clock, so a step backwards from NTP
leaves records whose times run the other way from their offsets, and the
answer must be the earliest *offset* at or after the moment rather than the
record nearest it.

A `latest` channel is asked for `Set`, `Get`, `Delete`, `Match`,
`MatchWithDeletions` and `Trim`, and a `queue` for `Enqueue`,
`SetMaxBytes`, `Bytes`, `Depth`, `SetBackoff`, `Offer`, `Lease`,
`Resolve`, `Release`, `ExpiredLeases` and `ExpireOlderThan`.

`Bytes`, `Depth` and `LowestPosition` are the numbers RFC 0005's
catalogue reads - each a field a store already keeps, returning no error
for the reason the counters below return none - and `SetBackoff` hands a
queue the retry gaps RFC 0003 specifies under "Backoff".

**Every operation that keeps a record or reads records back can fail,
and says so.** A memory store's error is always nil; the error is in the
signature because a record the broker was told was stored and was not
must never be acknowledged - a failed write becomes the `PUBACK` of
`0x83` RFC 0002's Publishing table carries, or a dropped packet at QoS 0.

**The counters cannot fail.** `Next` and `Floor` return no error. A store
is the only writer of its own channel, so it can hold both in memory
whatever it keeps the records on, and they are read on the delivery path,
where a failure has nothing useful to do about itself. Neither is ever a
guess: both are loaded from what is stored when the channel is opened, and
advanced only from what a transaction has actually committed.

**Two implementations of one contract are worth having only if they
agree.** Both stores are driven through one script by a conformance test
that compares every answer - the offsets assigned, the records returned,
the counters, the refusal below the floor, and a queue through offer,
lease, answer, resolve and dead-letter. A divergence is the failure nobody
sees, because neither store looks wrong on its own.

## What is stored, and what is not

### `next` and `floor` are stored values, never derived

Every durable channel keeps two numbers beside its records: the offset the
next record will take, and the oldest offset that can still be read.
Neither is ever computed from the records that survive.

Computing either from the surviving rows is invariant 9's and invariant
1's failure on a channel retention has emptied: `next` restarts at 1 and
reissues offsets a stored position points at, and `floor` reads as
"nothing was ever removed".

Deleting every record of a channel therefore changes neither number. No
removal touches them except to raise the floor, in the same operation that
does the removing - and a channel that is no longer configured keeps them
rather than being tidied away, because the numbers are where "everything
up to 5000 is gone" is recorded.

### A queue row is an unresolved record

Which worker holds a record, under which Delivery ID, until when, and the
epoch that fences a superseded resolution are **not stored** - in either
provider. Invariant 15 is why a restart restores none of it; storing them
would mean writing a value on the delivery path in order to throw it away
at the next start.

What survives is the record, how many attempts have been made on it, and
when it was first and last delivered - so a record that has already spent
its attempts is dead-lettered rather than starting over, and it still
carries in its dead-letter headers why it failed.

### A position lives with the records it points into

A durable consumer's position is stored by the same provider as the
channel it indexes, and it is one row or one entry per reader per channel -
a cursor, never a copy of the records, so a consumer a week behind costs
one number rather than a week of messages.

Kept anywhere else, a position can outlive the channel it names, or
survive a restore that replaced the records underneath it.

A reader is named by a scheme and an identifier: `mqtt:` followed by
the client id for a consumer that is an MQTT session, and `bridge:`
followed by the bridge's name for an outbound rule, which holds a
position with no MQTT session at all. The rule lives in the store
because a snapshot and a database both write these names and one rule
has to govern what goes into either.

Positions are taken for writing every `broker.session.ack_commit_interval`
rather than as each record goes out, and additionally the moment a
consumer goes away and before a snapshot is taken. What that costs and why
it is safe in only one direction is RFC 0003 under durable consumers: a
stored position may lag, and it never leads. This document owes that
section two things. The lag is that interval, 200 ms by default, and the
write behind it, which on a `sqlite` provider waits behind the writes
already queued for its one connection - those a client's packet is waiting
on first, the rest in turns ("Group commit") - so under a storage backlog
it is longer. And the two flushes above are what stop a deferred write
becoming a lost one rather than merely a late one.

A position whose session would have expired while the broker was down is
dropped when the provider is opened. Without that, making positions
durable makes them accumulate for ever: one row for every client id
anybody ever mistyped, with nothing that ever takes it out again.

**That sweep reaches readers that have a session and no others.** A reader
with no MQTT session states no expiry interval, and a zero one cannot be
read as "never expires": MQTT's own Session Expiry Interval of 0 means the
session ends with the connection, so a client that asked for zero is one
whose position must go. The two zeroes mean opposite things and only the
scheme separates them.

What keeps that from being an unbounded table is that a reader with no
session is one the operator wrote down: a bridge exists because a line of
configuration names it, and it stops existing when that line goes. It is
not a name a client chooses.

**A session's cursor on the broadcast log is not swept.** It is the
session's reader position there, kept as a durable consumer's is, and it
lives exactly as long as the session record in the same provider: a
session's ending takes it with the record, and whether the session has
expired is the record's to say, so a session connected at a crash comes back
with its place however long ago its cursor last moved. A cursor whose
session is not held is dropped at the start.

What makes that sweep reach every row rather than most of them is
`limits.max_session_expiry` (RFC 0002), which bounds the interval every
row is judged against.

### A record on disk

What a record is made of is RFC 0003 under "From publish to record", and
both formats keep exactly those fields. Storage adds one rule to them: a
timestamp is stored as Unix nanoseconds with the zero time as 0. `UnixNano`
on a zero time is a number that means nothing, and a reader taking it at
face value produces a date in 1754 rather than "not set". The two formats
follow the same rule, so they agree about what an unset timestamp is.

## Memory, with a snapshot

### The file format

A snapshot is written out by hand rather than left to a self-describing
encoder. `gob` and JSON both turn a field that moved, or a version that
does not match, into a zero value and report success - which is a broker
starting with plausible-looking wrong state and saying nothing. Here a
file this broker does not fully understand is refused by name.

The header is a contract that holds across every format version:

```
 0  magic           8 bytes
 8  format version  uint32
12  kind            uint8      channels, or a manifest
13  writer          24 bytes, NUL-padded
37  written at      int64, unix nanoseconds
45  ...             the body, which is what the version describes
    checksum        uint32, CRC32C of every byte before it
```

Those fields never move and the last four bytes are always the checksum,
so a broker meeting a file of another format can say which format it holds,
which build wrote it and when, instead of shrugging at bytes it cannot
parse. Everything is little-endian. The writer is the build's own
revision, read from the binary rather than written down: a constant is
wrong from the moment somebody forgets to bump it, and a version that
lies about which code wrote a file is worse than no version at all.

The checksum trails the content rather than sitting in the header. A file
that stopped early has no trailer at all, so a truncated write is caught
by the same check as a flipped bit, and the writer never seeks back over
bytes it has already handed to the kernel. Decoding therefore takes the
whole file rather than a stream: nothing in it may be trusted until every
byte before the trailer has been read, and a memory channel's snapshot is
bounded by the memory the channel was already holding, so reading it whole
costs nothing new.

The kind byte separates a channel file from a manifest so that neither can
ever be read as the other - a manifest that happened to parse as channels
would restore a broker to nothing.

**A snapshot file is in format 21: the broker writes 21 and reads no
other.** A record holds its offset, message id, topic, timestamp, User
Properties in the publisher's order, payload, five publish properties -
content type, response topic, correlation data, payload format indicator
and message expiry interval - the retain flag the publisher set, the
bridge it arrived on, the client id that published it, the QoS it was
published at, and, on a broadcast log message, whether it is a channel's
record copied for the shared groups over it and the one session it is
owed to, where it is a retained value copied for that session. A session
holds its subscriptions, its Will - with the Will's User Properties in the
client's order and the name its client authenticated as - and its
in-flight table on the broadcast log: the window it was written under, and
for each message on the wire its offset, packet identifier, QoS, how far
its exchange has got and the shared group that handed it to the session,
if one did. The file holds too each shared group's cursor and the
deliveries returned to it. A channel holds, beside its records, the
exactly-once publishes it is holding for their release: for each, the
client and packet identifier of the exchange, when it was held, and the
record, without an offset.

A file of any other format is refused by name: the broker says which format
the file holds, which build wrote it and which format it reads, and does not
start. Nothing converts one, so a memory provider's data does not cross a
change of format; drain it first.

Every declared length is checked against the bytes actually present, and
every count that is about to drive an allocation is refused before the
memory is taken rather than after the short read that would follow. A
count of four billion in a damaged file is otherwise the OOM killer, which
arrives without a log line.

### One file per channel, and the manifest

RFC 0002 states the shape: one file per channel, a queue and its
dead-letter channel sharing one, and a manifest beside them. What that
buys is a channel dropped from the configuration being a file nobody opens
rather than a startup error, and a damaged file costing one channel
instead of all of them.

What it trades away is the one thing a single file gives for free: a crash
partway through a shutdown leaves some channels from this run and some
from the last, with nothing to say so. **The manifest is what says so**,
and it is the whole reason there is one.

A channel name is not a file name - `a.b` is a legal channel and `.` and
`..` are two directories - so everything outside a small safe set of
characters is percent-encoded, including the percent itself, and two channel
names can never reach one file. That encoding trebles every dot, which is
why a name inside the 128-byte channel limit can still be too long to store
and is refused during configuration validation.

The safe set is narrower than the set of characters a name may hold, and
deliberately. A dot is legal in a name - `a.b`, `...`, `.hidden` - and is
encoded anyway, so nothing reaching the encoder can come out as `.` or `..`
whatever the configuration admitted that day. RFC 0002 refuses those two
names as well, and this is the half that does not depend on it: the same
directory is read by the migration commands and by a broker whose name rule
has changed under it.

**One more file for the retained store** on the provider that holds it. It holds
every retained message on a broadcast topic, and it is one file rather than
one per topic for the reason the channel files are one per channel and not
one per record: ten thousand retained topics must not become ten thousand
files and ten thousand fsyncs at a shutdown. It is written under
`$saguin/retained`, which reaches the directory as
`%24saguin%2Fretained.snapshot` - a name no channel can produce, because a
channel name is one topic level and may not hold a `/`. On a sqlite
provider it is a row set in the same database instead, keyed like a `latest`
channel's, and there is no file at all.

What it promises is what its provider promises, and nothing more: on a
memory provider the retained messages survive a graceful shutdown and not a
power cut, and on a sqlite provider they survive a crash. A fleet that
publishes its device tree retained and expects it back after a restart is
choosing that provider whether it knows it or not (invariant 14).

**And one for persistent sessions**, `saguin.sessions`: every session the
provider keeps, its subscriptions - each with the broadcast log's offset
when it was made - and its window - the messages on the wire with their
packet identifiers and how far each has got - and every shared group's
cursor, with the deliveries returned to it. **The broadcast log itself is
the file `saguin.broadcast`**, with each session's cursor in it as a
reader position and the QoS 2 broadcasts it holds for their release,
written and read with the sessions file because a cursor is worth nothing
without the log it points into. The log is written first and made durable
before the sessions file, so a stop that crashes between the two leaves a
log at least as far on as the sessions beside it, and a sessions file
with no `saguin.broadcast` beside it has lost its log: the broker does
not start on it. When the pair is read, an in-flight entry behind its
session's cursor goes - unless a shared group handed it over, which puts
it behind the group's cursor instead, so it stays while its session does -
and one at or past the log's next offset, or a table larger than its
window, is refused. One file for the provider rather than one per
session, for the reason the retained store is one file, and a name no
channel can produce because it does not end in `.snapshot`. It carries
the same header, version and checksum as a channel's file, is replaced
only once its successor is whole and synced, and a damaged one is a
startup error rather than an empty start. On a sqlite provider the
sessions are rows in the same database instead.

**An unfinished exactly-once publish is in its channel's file**, with the
moment it was held, which is what `expires_after` measures: it is kept as
far as the channel is, and RFC 0003 "Exactly once" states what a restart
costs. A channel's file older than the shutdown the manifest records - one
that shutdown did not rewrite, because its write never landed or the
channel was not configured then - is read for its records and not for its
holds, each counted as abandoned: that shutdown's broker answered their
releases without them.

### The order a shutdown writes in

Each file is written to a temporary name in the same directory, synced,
renamed over its predecessor, and the directory synced after. Four
decisions, and **only a power cut distinguishes any of them from the wrong
answer** - remove a sync or reorder the writes and the bytes are still in
the page cache, so a process that exits normally sees them all and reads
back everything it wrote:

- **The temporary file is in the same directory**, because a rename is
  atomic only within one filesystem.
- **The file is synced before the rename and not after.** The rename is
  what makes this file the one that loads; renaming over the last
  known-good snapshot while the new one's bytes are still only in the page
  cache is how a power cut destroys both at once.
- **The directory is synced after the rename.** Without it a file's
  contents survive and the directory entry naming them need not, which
  leaves the new snapshot written, synced, and invisible.
- **The manifest goes first**, and is made durable before any channel is
  written. Written last, a crash before it would leave channel files
  stamped later than the manifest, which is a state nothing can interpret.
  Written first, every crash reads cleanly: the manifest says when this
  shutdown began, and a channel file older than that did not finish.

The consequence is the one invariants.md 14 asks for: a write that fails
at any point leaves either the file that was there before, whole, or the
new one, whole, and never half of either. A shutdown in which one
channel's file cannot be written reports the failure and leaves that
channel's last good snapshot exactly as it was.

Files are written several at a time, bounded, because fifteen channels
must not mean fifteen concurrent fsyncs on an edge box - and every failure
is reported rather than only the first, since an operator whose disk
filled needs to know which channels did not make it.

### What a shutdown costs, and why there is no periodic snapshot

A snapshot happens once, on the way out. **There is no periodic one.**

The shutdown is two phases and only the first holds the broker's lock:
state is copied out of the channels, the lock goes back, and the writing
happens after. One append channel of 128-byte records, at `-benchtime 2s
-count 2`, on an AMD Ryzen 7 260 (8 cores, 16 threads, 53GB) writing to
ext4 over NVMe, and on a MacBook Pro M1 Pro (10 cores, 32GB) writing to
APFS on the internal SSD:

| records | copy, under the lock | write, after it |
|---|---|---|
| 1,000 - Ryzen 7 260 | 37us | 2.3ms |
| 1,000 - M1 Pro | 26us | 25ms |
| 10,000 - Ryzen 7 260 | 272–273us | 5.0ms |
| 10,000 - M1 Pro | 274–308us | 26–28ms |
| 100,000 - Ryzen 7 260 | 2.5–2.7ms | 29ms |
| 100,000 - M1 Pro | 2.0–2.2ms | 49ms |
| 1,000,000 - Ryzen 7 260 | 15–17ms | 261–266ms |
| 1,000,000 - M1 Pro | 15–16ms | 273–275ms |

`BenchmarkSnapshotCopyUnderLock` and `BenchmarkSnapshotWrite` take those.
The first column has a second term that does not depend on how much data
there is: the set of channels to write is sorted on every call, which
`BenchmarkSnapshotChannelSelection` puts at 0.7us for ten channels, 105us
for a thousand and 1.4ms for ten thousand on the Ryzen. Against a write of
2.3ms for the smallest channel above, the sort is not where the time goes
at any size worth having.

**The write column is a real disk's**, and on a spinning disk or an SD
card it is larger by the whole figure. A tmpfs measures the page cache,
because `fsync` there does nothing, so reproducing it needs `TMPDIR` on
real storage.

**The M1 Pro's write column stands an order of magnitude above the Ryzen's
at the small sizes, and that is the disk rather than the machine.** Go
issues `F_FULLFSYNC` on Darwin - a full drive-cache flush - so a snapshot
write pays a floor of about 25ms whatever it holds, which is why the
thousand-record write and the ten-thousand-record write are barely apart
and only the million-record one is dominated by its own data. An operator
sizing a shutdown grace period on macOS budgets from that fsync floor, not
from the Ryzen's figure.

**A snapshot is a whole-file rewrite**, which is what a periodic one would
cost: the size of the channel on every tick, however few records changed,
growing with the channel rather than with the traffic. A memory channel's
loss window is therefore the whole life of the process, which
invariants.md 14 states plainly. A channel that cannot afford that window
belongs on sqlite, which keeps every commit.

#### Many channels rather than one large one

The table above is one channel holding a great deal. The other shape is ten
thousand channels holding a little each, which is what a configuration split
across files is for, and it is a different cost: one file and one `fsync`
per channel, four at a time, plus the manifest and two directory syncs.
`BenchmarkShutdownSnapshotAcrossChannels` takes the whole of `SaveSnapshots`
at eight records a channel, on the Ryzen 7 260 over ext4 on NVMe:

| channels | a graceful shutdown takes | the same on tmpfs |
|---|---|---|
| 10 | 3.6–5.7ms | 0.4ms |
| 100 | 25–28ms | 1.9ms |
| 1,000 | 252–256ms | 15ms |
| 10,000 | 2.5–2.7s | 140ms |

**It is linear in the channel count, and the `fsync` is most of it.** The
tmpfs column is the same work with the syncs doing nothing: eighteen times
faster at ten thousand channels, and less than ten times at ten, where the
manifest and the two directory syncs are a larger share of a smaller
figure. So the disk column measures storage rather than Sagüin, and a
slower one multiplies it directly.

**Darwin is arithmetic rather than a measurement, and it is the one worth
doing.** Every sync there is an `F_FULLFSYNC` with a floor of about 25ms
whatever it holds - measured, in the table above - and files are written
four at a time, so ten thousand channels is on the order of a minute
rather than the Ryzen's two and a half seconds. The two figures it rests
on are both measured, and it is stated because a stop timeout is exactly
the kind of thing sized from the wrong platform's number.

**What the write column does mean is how long a graceful shutdown takes**,
and that is the number an operator needs. It is per channel and they add
up, and it runs after the listeners have closed - so a supervisor whose
grace period expires first sends `SIGKILL`, and everything since the last
snapshot is gone. That is invariants.md 14's failure arriving through a
stop timeout rather than through a power cut, and it is the one way a
*graceful* shutdown loses a memory channel. Kubernetes allows 30 seconds
by default and systemd 90, so a single large channel has room; several
large ones on a slow disk are what to check before trusting the default.

### What the retention sweep costs, against the channel count

The sweep is the other thing that walks every channel, and unlike the
shutdown it runs for the life of the process: by size every second, by age
on an interval derived from the shortest retention any channel configured.
`BenchmarkRetentionSweepBySize` and `BenchmarkRetentionSweepByAge` measure
one pass with nothing over the bound, which is the steady state.

**The two halves cost different amounts, and it is worth knowing which is
which.** One pass, Ryzen 7 260, the database on ext4 over NVMe:

| channels | by size, memory | by size, sqlite | by age, memory | by age, sqlite |
|---|---|---|---|---|
| 10 | 0.9–1.1us | 1.3–1.6us | 1.4–1.5us | 159–215us |
| 100 | 5.0–7.0us | 7.9–9.6us | 6.3–6.8us | 1.30–1.31ms |
| 1,000 | 93–134us | 84us | 81–93us | 13.6–13.9ms |
| 10,000 | 1.50–1.54ms | 1.29–1.52ms | 1.41–1.70ms | 162ms |

**A size sweep reads nothing.** Whether a channel is over `retention_bytes`
is answered from the byte count the channel already keeps - the same number
a publish is refused against - so a channel under its bound is settled
without asking storage. That is why the two size columns are the same
figure: neither provider reads. It is also why the interval can be one
second at any channel count, which is the argument `sizeSweepPeriod` rests
on.

**An age sweep has to look.** A record becomes too old with nothing written
to its channel, so there is no counter to consult, and on a `sqlite`
provider that is a read per channel: 162ms a pass at ten thousand channels.
Memory answers the same question from records it already holds.

**What decides whether that matters is the age sweep's period**, and it is
derived rather than fixed: a tenth of the shortest retention any channel
configures, and never below a second. A fleet retaining for an hour sweeps
every six minutes, where 162ms is nothing at all. A channel retaining for
ten seconds puts the sweep on the one-second floor, where ten thousand
channels on `sqlite` is a sixth of a core spent walking them - so a short
retention period is not only a statement about how long records live, it is
the thing that sets this cost. Ten thousand channels and a ten-second
retention is the combination to size for.

**It is not a publish stall either way.** The store map is copied under the
broker's lock and the lock given back before anything is read or removed,
so a sweep never holds a publisher up for the length of its walk.

### Loading

A channel file that is damaged, truncated, or in a format this broker does
not read is an error and **the broker does not start**. An empty start is
indistinguishable from a fresh install and destroys the evidence, so every
error names what was wrong and none of them is ever silence.

The manifest's own failures are warnings instead. It holds no records, so
refusing to start over a file that carries nothing would be the worst
trade in the design. What it buys is the reporting a directory of separate
files cannot otherwise do:

| What the directory shows | What the operator is told |
|---|---|
| A file the manifest listed and which is not there | that channel's records are gone |
| A channel written before the manifest's moment | everything published after that moment is gone |
| A channel written after it | the channels finished and the manifest did not |
| A file nobody asked for | nothing - a channel the configuration dropped is the operator's own doing |

A checksum proves the bytes are the bytes that were written and says
nothing about whether the writer was right, so decoding is followed by a
pass over what a checksum cannot cover: offsets ascending, at or above the
floor and below the next offset, records and items only on the kinds that
admit them, and no position past the next offset. Reads stop at the first
record past what was asked for, so an out-of-order restore would hide
every record behind the one that went backwards.

A channel whose configured type no longer matches the one in its file
refuses to load, rather than putting append records into a latest map or
queue work into a log where nothing can acknowledge it.

Every queue item comes back available with no epoch, no Delivery ID, no
holder and no deadline, whatever the file holds, and that rule is enforced
where the queue is rebuilt rather than in the decoder, because it holds
for any caller.

## SQLite

### The driver

`modernc.org/sqlite`, which is SQLite's C source translated to Go rather
than linked through cgo. Sagüin stays one static binary that cross-compiles
without a C toolchain, which is most of what makes it deployable at the
edge at all.

### WAL, and `synchronous=NORMAL`

Both ride in the connection string rather than being executed after
opening, so that a pool which reconnects cannot produce a connection
without them.

**WAL** because it lets a reader run while the writer commits, which is
what keeps the database readable by other tools while the broker is
running.

**`synchronous=NORMAL`** does not sync at commit, and does sync at a
checkpoint. Every `flush_interval` (RFC 0002, `150ms` by default, `10ms`
to `1s`) a goroutine also fsyncs the write-ahead log, and only when
something has been committed since the last fsync, so an idle broker
writes nothing. **A power cut can therefore lose the acknowledged publishes
committed in about the last flush interval plus one fsync - never a hole
below a record that survives, and never the file.** Measured on a Raspberry
Pi 4 with a USB SSD: at the `150ms` default, two power cuts lost the
acknowledged records of the last 52 and 138 ms; over four cuts with no
interval at all, 28-467 ms. No holes and no duplicates in any of them.

**A flush that fails is never silent**, because it can mean acknowledged
commits did not reach the disk: each failure counts in
`saguin_storage_errors_total{provider}`, the first of a streak is logged at
`ERROR`, and the flusher tries again every interval.

**The price is disk wear.** Each fsync that has new data writes at least
one partly filled block, so the worst-case extra writes under steady
small-message traffic are about 4 KB times the flushes a second: at `150ms`
that is about 27 KB/s, or 0.85 TB a year. A consumer SD card (roughly
500-3,000 write cycles a cell) may prefer a longer interval or an
industrial card; an SSD rated for 70-150 TBW is unaffected. Nothing is
written while idle. It costs nothing measurable at 5-200 ms in publish
rate, latency, CPU, queue depth or a reconnect storm; the fsync's p99 stayed
at or under about 20 ms during a storm.

`FULL` syncs at every commit. Measured, with group commit it costs nothing
on publish rate or latency and 2.2 times on connect and queue stores - but
it is still not used, because a reconnect storm under it timed clients
out. `OFF` removes the checkpoint sync as well and can leave a database
that does not open at all, losing data committed hours earlier - worse than
memory storage rather than equal to it.

**`synchronous` is not a knob**, and the reason is not that `OFF` buys
nothing. `OFF` is faster still and one power cut from nothing, and `FULL`
times out a reconnect storm; the interval above is the setting an operator
has.

What `NORMAL` costs is one row of `BenchmarkAppend`: a single-row commit
into a WAL database, 128-byte payload, the file on the machine's own disk,
at `-benchtime 2s -count 2`.

**A range spans the runs taken, not one sitting's spread**, so the width
of a row says how much the figure moves between runs.

| `synchronous=NORMAL` | per commit | commits a second |
|---|---|---|
| Ryzen 7 260, ext4 on NVMe | 34.7–35.4us | 28,200–28,800 |
| MacBook Pro M1 Pro, APFS on the internal SSD | 52.2–54.2us | 18,500–19,200 |

Every disk figure in this document is taken with `TMPDIR` on real
storage; on a tmpfs the same row reads a third faster, because it is
measuring memory.

The broker adds a packet parse, its own lock and a socket write to each of
those, so what a publisher sees is RFC 0002's table rather than this one.

### One database per provider

The whole provider is one file. A queue and its dead-letter channel are
therefore two tables in one database, which is what lets the move between
them be one commit.

The alternative - a file per channel, attached to one connection - does
not work, and it fails in the shape this design refuses everywhere.
SQLite's atomic commit does not span attached databases in WAL mode. It
does not report that: a transaction writing into two attached WAL
databases commits and returns success, having written each database's
write-ahead log separately, and a host that dies mid-commit can leave one
applied and the other not. In rollback-journal mode SQLite writes a master
journal naming both files, which is the mechanism that makes the multi-file
commit atomic, and WAL has no equivalent. So a queue and its dead-letter
channel in two files would look exactly like a working implementation
until a crash lost a record - which is the outcome dead-lettering exists
to prevent.

### The schema

Eleven tables, at `user_version` 20, `STRICT` throughout so that a column
declared `INTEGER` cannot quietly hold text.

**A record's User Properties are an ordered JSON array of name/value
pairs**, carrying a repeated name and the publisher's order (RFC 0003
*Headers*).

**A `props` column** on `records`, `latest_values` and `queue_items` holds
the five publish properties as one JSON object and NULL for a record that
carries none. Correlation data is arbitrary bytes and is written base64.

**`held_publishes` holds an exactly-once publish in the database of the
channel it is for - or of the broadcast log, under `$saguin/broadcast`,
for a broadcast - between the `PUBLISH` that starts it and the `PUBREL`
that finishes it**, beside the records it will join, keyed by channel,
client and packet identifier, because MQTT gives each session its own set
of identifiers. It carries no offset - a position assigned before the
release could be passed by a consumer while the row still waited, and the
record would then become readable below a position that had already moved
over it - so no reader and no retention sees it, and the page ceiling
counts it. Its release is one transaction, written in the reserve: the
record is written at the channel's next offset and the row deleted, so
there is never a moment with both or with neither.

A row outlives a restart where its session does, and is dropped and
counted where it does not - RFC 0003 "Exactly once" has the guarantee. A
row for a channel the configuration does not keep in this database is
deleted at the start and counted; the broadcast log's are kept in the
session provider's database. The rows and the sessions table are read
together or neither is worth reading, which is also what makes a restored
copy coherent: it carries the sessions that were open when it was taken as
well as the exchanges, so the two agree.

**`sessions` holds persistent MQTT sessions, the broadcast log what they
are owed, and `share_groups` each shared group's cursor in that log.** A
session row keeps its client id, the expiry interval it was granted after
`limits.max_session_expiry`, when its client went away (0 while
connected), its subscriptions as one JSON array with every option the
`SUBSCRIBE` carried, any partition declaration and the broadcast log's
offset when each was made, and the window its in-flight table was written
under, the client's Receive Maximum. The table is `session_inflight`, a
row for each message on the wire: its offset, the packet identifier MQTT
requires a re-send to a resumed session to carry (MQTT-4.4.0-1), its QoS
and how far it has got - sent and unanswered, or answered with a `PUBREC`
so that the `PUBREL` is what is owed - and the shared group that handed
it to the session, if one did. When the file is opened, an entry behind
its session's cursor goes, unless a group handed it over: that one is
behind the group's cursor, which passed it in the same transaction that
wrote it, and stays while its session does. A table larger than its
window, or one with no log to point into, is refused. The log is kept in
the append channels' tables, as the retained store is kept in the latest
channels': under the reserved name `$saguin/broadcast`, a `channels` row
holding its `next`, `floor` and bytes, and one `records` row per message,
written once however many sessions are owed it. It is not a channel, and
nothing can subscribe to it. A message leaves it once nobody owes it -
within `broker.session.ack_commit_interval`, in one removal with the others
released meanwhile, or at once when 500 are waiting, the log's provider
needs the room, or the broker stops - so the log may have gaps: the floor
is the lowest offset still held, and no offset is reused. A session's
cursor in it is a `positions` row, as a durable consumer's is on a
channel, and goes in the same transaction as its session. It is session
state, so the migration commands leave it behind with the sessions. A
`share_groups` row is a group and its cursor: what the group is owed is the
log after it. It is kept while a session holds the group's filter, and goes
in the same transaction as the last of them. A `share_returned` row is a
delivery a member's session ending returned to its group (RFC 0003
"Broadcast"), written in that ending's transaction: the group is owed it
again, ahead of its cursor, and the hand-over that gives it to another
member takes the row out. A group's returned rows go with its cursor. A
channel's records are kept by the channel, and the log holds one only as a
copy a shared group over the channel is owed (RFC 0003 "Broadcast"). A
session goes with its window in one transaction, and an acknowledgement
clears its entries - each named by its identifier and its offset - and moves
the session's cursor in one transaction. What they survive is what the file
survives.

There is no in-place upgrade. A file from any earlier version is refused by
the broker **and** by `--sqlite-to-snapshots`, so carrying one across takes
the older binary to export it first. The refusal names the version the file
holds and the build that wrote it:

```console
$ saguin --config saguin.yaml
level=ERROR msg="cannot open a storage provider" error="storage /var/lib/saguin/saguin.db: schema version 5 (created by saguin 0123456789ab at 2026-01-01T00:00:00Z); this broker reads version 20"
```

**`--check-config` does not report it.** It opens every file the
configuration *names* - the password files, the ACL file, the certificates -
and does not open the database, so a version it cannot read is found when
the broker starts rather than by the pre-check. An `ExecStartPre` gating on
`--check-config` passes and the unit then fails.

| | |
|---|---|
| `meta` | who wrote this file and when, for an error message |
| `channels` | name, kind, `next`, `floor`, `bytes` - one row per channel |
| `records` | append records, keyed by (channel, offset) |
| `latest_values` | one row per topic, keyed by (channel, topic) |
| `queue_items` | one row per unresolved record |
| `positions` | one row per reader per channel |

The primary key on (channel, offset) makes a reused offset a failed insert
rather than a silent overwrite, which is why nothing in this schema uses
`INSERT OR REPLACE` - the lock below does, in a database of its own, and
there the unconditional write is the point.

**The two kinds of channel are keyed by what they are.** An append channel
is a sequence, so `records` is keyed by offset, which is also how every
read of it walks. A latest channel is a map from topic to current value -
its offsets exist only so a consumer can tell which of two values it holds
is newer - so `latest_values` is keyed by topic, and replacing a value is
one statement straight to the row. One table for both would key the map by
the sequence's key: replacing a topic's value would have to find it by
scanning the channel, and fixing that with an index on topic would make
every append pay for an index no append ever reads.

**Headers are JSON text.** A sqlite provider is one an operator can open
with the `sqlite3` shell, and headers are where they will look when a
message did not arrive as expected. A table of their own would mean an
insert per header on every publish and a join on every read, which is
worse on both counts; the binary encoding the snapshot uses would be
unreadable there. Records carrying no headers store NULL and do no work at
all.

The counter and the record always move in one transaction. Assigning an
offset outside it would let two records take one number - the primary key
refuses the second, which is the safe failure but a failure all the same.

### One writer, one store per channel

The write pool is capped at a single connection, which is what RFC 0001's
non-goals already say the SQLite write path is. It also makes every
transaction serial with every other, so a read-modify-write of a channel's
counters cannot interleave with another one, and the deferred transaction
SQLite opens by default never has to lose a lock upgrade to a second
writer.

Asking a provider twice for one channel gives back the store it built the
first time. Two stores for one channel would be two writers with two
copies of one counter: both would assign the same offset and the primary
key would refuse the second, so nothing would be lost or duplicated - but
the losing store's counter would never advance, and it would refuse every
publish to that channel from then on while the broker looked healthy.
Handing back the store already built removes the case rather than
documenting it.

**Channel reads have connections of their own.** Beside the writer a
provider opens up to `read_connections` read-only connections (RFC 0002
"How a sqlite provider reads"), their pragmas in the connection string as
the writer's are, with `query_only` added so nothing sent down one can
write. A consumer's window of records is read there, in one read
transaction per read - the floor and the records above it in one snapshot,
so a trim committing between them cannot hand the reader the survivors as
though nothing was missing (invariant 1), and a fresh one each time, so
every read sees every commit before it. A session's record, every
session's, and the deliveries returned to shared groups are read there too,
each in a read transaction of its own: a SUBSCRIBE reads its session's
record before writing it, and that read does not queue behind every other
client's write. A write that returned has committed, so the read after it
sees it. The broadcast log's reads, and any read made inside a write
transaction, stay on the write connection: they decide what a write does.
`read_connections: 0` opens none, and every read goes down the write
connection.

Statements are prepared once and closed with the database. The read
connections keep a cache of their own, every statement they run prepared
when the provider opens, so a read never asks for a second connection while
holding one. Handing SQL text to the driver on every append means parsing
it again on every append, and in a pure-Go SQLite that is a large share of
what an append costs.

### What an acknowledging consumer costs a publisher

Every QoS 1 delivery ends in a `PUBACK`, and Sagüin answers one by feeding
that consumer its next record. That path has no record in hand, so it reads
the channel: on this provider a query on a read connection above. **The
query is made under no lock at all, and the batch under the consumer's own**:
what to read is decided under that consumer's lock, the records are read
without it, and they are sent only if neither the consumer's position nor its
filters moved in between, or read again if they did. The broker-wide lock is
not taken on this path. So a slow read costs that consumer's next batch and
holds back no publisher, no other consumer, no subscription and no resumed
session's re-send - and a busy consumer does not hold back the deliveries
of any other.

`BenchmarkPublishWithConsumers` carries the figure, at eight publishers and
128-byte payloads, `-benchtime 2s`, five runs, on the Ryzen 7
260 pinned to four physical cores (CPUs 4-7 and 12-15), with `TMPDIR` on
ext4 over NVMe:

| acknowledging consumers | memory | sqlite |
|---|---|---|
| 4 | 31.6–34.1us, 29,332–31,621/s | 49.5–51.1us, 19,578–20,193/s |
| 50 | 316–328us, 3,048–3,161/s | 310–334us, 2,991–3,226/s |

**Measured with the consumers in the broker's own process**, as a Go
benchmark runs them: they share the broker's CPUs and its heap, so these
rows are what broker and clients together reach on that box, not the
broker's ceiling. A rig that pins the broker to its own cores and runs lean
clients in another process is what measures the broker alone.

**The rates are publishes, and every consumer receives every one**: each is
delivered to all of them and acknowledged by each. So fifty consumers at
2,991–3,226 publishes a second on `sqlite` is 150,000–161,000 deliveries a
second, each followed by an acknowledgement and a read, and four at
19,578–20,193 is 78,000–81,000.

**Each batch takes that consumer's own lock twice, once to decide and once
to commit**, and the broker-wide lock not at all, so no other consumer's
batch and no publisher ever queues behind a delivery.

**It scales with the consumer count and does not run away.** Twelve and a
half times the consumers costs memory 9.8× and sqlite 6.4× - both sub-linear
at this width. What it does cost is a factor against memory at the narrow
end: about one and a half times at four consumers, and nothing at fifty,
where the two providers are level.

**So the number to size against is the width, not the provider's ceiling.**
Fifty consumers acknowledging one channel on a `sqlite` provider is about
three thousand publishes a second, where the same eight publishers reach
40,827–41,081/s on the same disk with nobody acknowledging - the `0cons`
row of this benchmark, taken with the rows above, which is where to re-take
it rather than from this sentence. A deployment wanting more than that from
one channel wants the consumers spread over more of them, or a memory
provider, or both.

The rows above are the acknowledging half; the `0cons`, `1cons` and `4cons`
rows of the same benchmark are fan-out at QoS 0, where nothing comes back
and a caught-up consumer is handed the record already in hand without the
store being asked at all.

### Group commit: one transaction, several publishes

A provider collects without waiting unless its `publish_commit_interval`
says otherwise (RFC 0002 "How a sqlite provider commits publishes"): a
publish that finds no transaction committing is stored at once in one of
its own, and the publishes that arrive while one commits are stored
together in the next, up to 256 records, which starts the moment it
ends. `publish_commit_interval: none` gives every publish its own
transaction. A duration makes a transaction wait for company, and it
closes at whichever comes first: `publish_commit_max_records` records, or
`publish_commit_interval` since it opened.

What is collected is the three publish paths - a record appended, a value
replaced, a job enqueued. A retention sweep, a dead-letter move, a queue
resolution and the copy paths keep transactions of their own, because each
of those reads what it is about to write and a shared transaction would
only make that harder to reason about.

**A client's session writes are collected too, in groups of their own.** A
session begun, saved, disconnected or ended, its in-flight table, its
acknowledgements, a shared group's cursor and returned deliveries, an
exactly-once publish held or dropped, and a consumer's position saved or
dropped, arriving while a transaction commits share the next, up to eight
of them, whatever `publish_commit_interval` says: they never wait for
company. A group holds publishes or session writes, never both, so what is
said here about a batch of publishes is unchanged, and **the two kinds
take turns**. **A transaction of session writes gives way**: after each
write it looks for a publish waiting, and for its own time passing 5ms,
and on either commits what it has and queues the rest again behind that
publish - so a publish waits for one client's write and a commit, never
for a group. Five milliseconds is a whole group on the Ryzen box and three
writes on a Raspberry Pi 4, where each costs about 1.5ms. With 10,000
sessions reconnecting and ten publishers sending, a PUBACK's median was
0.30-0.34ms and its p99 61-83ms, and the storm took 1.75s. Each client is
answered only once the transaction holding its write has committed
(invariant 18). A write made after another has returned commits after it,
so what one connection writes in turn - a session begun, then its
subscriptions - and one client id's writes across a takeover, each made
under the id's session lock, land in the order they were made. Writes a
session makes from two places at once - its connection and its deliveries'
acknowledgements - are not ordered against each other: the turns below may
serve the later first, and each is whole whichever lands first.

**A write a client is waiting on goes first.** A session begun and an
exactly-once publish held answer a packet - a CONNACK, a PUBREC - and are
served ahead of the writes nobody waits on. So is **every write for a
client whose packet is waiting on the store**, from the moment the packet
is read until it is answered: a CONNECT until its CONNACK, and a
SUBSCRIBE, an UNSUBSCRIBE, a QoS 1 or 2 PUBLISH, a PUBREL and an AUTH
while each is handled. That is how a SUBSCRIBE's record, the position a
seek or a Retain Handling 2 subscription stores, and the exchange a PUBREL
lets go are served first. It is also how a CONNECT is served what it waits
behind: the connection it takes over holds the client id's session lock
while its last writes are stored, and those - a disconnect recorded, a
session ended - answer no packet of their own. Marked as it is read,
before it waits for that lock, the CONNECT moves each queued group holding
one of its id's writes up among the writes that answer, whole, and every
write for the id that joins meanwhile is one of them. So a reconnecting
client's CONNACK waits behind its own id's writes, not behind every
disconnect a storm has queued: in the reconnect storm measured below, no
CONNACK timed out. And a transaction that gave way keeps its place: what
it had not run goes first when its turn comes again, not behind everything
queued meanwhile.

**The rest take turns, four to one.** The writes nobody waits on get every
fifth turn, so a stream of packets cannot hold them back for ever. Among
them, a departed client's writes - a disconnect recorded, a session ended
by its client or by the expiry sweep, and what a refused write is still
owed - get four turns to every one of a connected session's -
acknowledgements, in-flight tables and positions, a shared group's cursor.
Departed clients' writes take most turns because a reconnecting client's
CONNACK waits behind the disconnects queued ahead of it - a CONNECT moves
up only its own id's writes - and connected sessions' writes take every
fifth because a subscriber's acknowledgement waiting behind a whole storm
keeps its window shut for as long (invariant 18). Measured on the Pi with
10,000 sessions reconnecting at once while ten publishers send 2,000 QoS 1
messages a second to one durable subscriber, over ten storms: no CONNACK
timed out; the worst CONNACK was 4.4s and the worst p99 2.8s; the longest
acknowledgement waited 75ms to be stored, and the subscriber's session
queue shed nothing; and the storm took 44.5-47.3s. The cost is the
publishers': during a storm a PUBACK's median was 2.5ms and its p99 22ms.
What an acknowledgement still waits is the write behind
`broker.session.ack_commit_interval` that invariant 18 allows, and what an
unclean stop in that window sends again.

**What a connected session's write can wait for.** Its background write - an
acknowledgement, a position, an in-flight table - waits up to four answering
groups for each of four departed clients' groups, and four more before its
own: up to 24 write groups, and the publish turns between them. On a
Raspberry Pi 4 under a reconnect storm with churn that was about 1.2s. Two
waits lie outside this queue. A Retain Handling 2 subscription reads its
stored position on the read pool, so it waits for no write group, but a
position still queued behind them is not yet stored, and it reads the one
before. And the broker's position flush writes the consumers' positions one
after another under its own lock, so a position saved while it runs waits for
those writes.

**A session write refused is refused alone.** Each reads first what decides
it - a session not held, a window full, a group with no cursor - and that
refusal is its own answer while the others go on. What ends the shared
transaction is a failure while writing: the provider out of room, or the
file unwritable. SQLite ends the whole transaction on that, savepoints
included, and a statement sent after it would commit on its own - so none
is sent. The writes run again: all but the one that failed, together, once;
then the one that failed, and every write of a second attempt that also
failed, each alone, as it ran before writes were collected, with the answer
it would have had. Two shared attempts at most, so a reconnect storm into a
full provider costs what it did.

Three properties hold in all three modes, and they are what the
implementation is arranged around rather than what it documents
afterwards.

**An offset is taken in the transaction's order.** One publisher at a time
reserves, from the first reservation of a batch to the last outcome of it,
so no reservation falls between a batch taking its offsets and the
transaction that stores them. A batch that fails puts back exactly what it
took: no offset is skipped and none is reused. This is why the other
writers of a channel's counter row - both retention sweeps, the
dead-letter move - take the provider's write lock before the channel's own,
and it is a rule about the provider rather than a list of the callers that
exist today.

**A channel's counter row is written once for the whole batch**, by
whichever record in it took the highest offset, carrying the totals of
every record before it. That is not only an optimisation - it is worth
about half of everything group commit buys, because the counter row is
otherwise a second statement on every single record - it is also the only
way several channels can share a transaction without one channel's totals
being written into another's row.

**A refusal is one record's; a failure is the batch's.** A record that
would cross its channel's `max_bytes` is refused before the transaction
opens, so the publishes beside it still commit and a full channel cannot
stop an unrelated one. A transaction that fails fails for everybody in it:
what can fail there is the provider being out of room or the file being
unwritable, and each of them is answered with what it would have been
answered alone.

**Near the bound that is not the same as being refused for the same
reason.** A batch of eight can be refused where a smaller one would have
fitted: a provider with room for exactly one more record refuses all
eight together, and one on its own immediately after is stored. The
answer is an honest `0x97` and the publisher's retry finds the room, so
nothing is lost or reported wrongly; but a publisher near a full provider
may be refused where it would have succeeded, and that is the price of
sharing a transaction rather than an equivalence.

**Where an interval is set, whether it helps at all is decided by
`publish_commit_max_records` against the traffic**, and RFC 0002 has the
measured table. A batch that fills on the record count commits at once; a
batch that does not fill waits out the interval whatever is in it: through
the wire, 256 records to a transaction is about three times *worse* than a
transaction per publish if only eight publishers are sending.

**The interval is not a wait for readers** - the leader waits it out
holding nothing and opens the transaction only afterwards, so a read on
the write connection waits for the commit alone, and one on a read
connection for nothing - and the wait is paid per connection rather than
per batch, which RFC 0002's `publish_commit_interval` section measures and
is why an interval above a second is refused.

### Several providers is how a write path is widened

A provider is one file and one writer, so channels on one provider do not
write concurrently however many channels there are - which is measurable,
and it measures the wrong way: spreading one publisher's load across 32
channels of a single provider is slightly *slower* than using one, because
there are more tables to touch per commit and no more concurrency to be
had.

Several `sqlite` providers is what adds writers: each is its own file, its
own writer, and its own `max_bytes`, `publish_commit_interval` and
power-cut window. Channels are assigned with `storage:` and nothing else
is needed. What stays inside one provider is a queue and its dead-letter
channel, because the move between them is one transaction and SQLite's
atomic commit does not span files.

### The queue's volatile half

Because a queue row is an unresolved record and nothing more, the delivery
state lives in memory: one entry per record currently out with a worker,
bounded by the in-flight windows of the live workers.

That is what makes the two operations the broker's clock drives cost
nothing. `Offer` is a bounded read and no write at all - records already
out with a worker cannot be excluded by the query, so it reads far enough
to see past them and skips them in memory. `ExpiredLeases` touches the
database not at all. `Lease` is one `UPDATE`, and it is there only because
the attempt count is the one thing that has to survive a restart.
Mirroring the memory store instead would have made both of the ticker's
operations into writes, on a timer, for state discarded at the next start.

Epochs are minted from a counter rather than kept per record; the memory
store keeps a per-record counter instead. Neither is a value any caller
may interpret - both answer only "is this delivery still the current one",
and both start again from zero after a restart. That is safe for one
reason, which holds for both: an epoch is compared only after a Delivery
ID has been found, and the broker's table of live deliveries is empty at
startup, so no identifier minted before the restart is ever looked up.

The dead-letter move is one transaction across two tables. The log grows
an unexported method that joins a transaction the queue opened, taking the
driver's own transaction type, so that only a store in the same database
can satisfy it - a queue can dead-letter into a log kept the way it is,
and into nothing else. If the move fails, neither half happened, the
record is still held with the attempt count it had, and the offset the
dead-letter channel had already taken is handed back; otherwise the next
record there would leave a hole that a stored consumer position reads
straight past.

In the memory store the same guarantee is the append happening while the
queue's lock is held, taking the log's lock inside it. Nothing anywhere
takes the two in the other order, so there is no cycle to deadlock on.

### Opening and closing

Opening applies the schema if the file is new, refuses it by name if it
states a schema version this broker does not read, and refuses it too if
it holds tables and states no version at all - adopting either is how a
broker starts with plausible-looking wrong state and says nothing. The
version is set in the same transaction as the tables, so a file cannot
exist with one and not the other.

Recovery at open has nothing to do about queue work, and that is the point
rather than an omission: none of the delivery state is stored, so there is
nothing to undo. What it does do is drop the positions whose sessions
expired while the broker was down. It runs unconditionally, including
after a clean shutdown, because there is no way to tell the two apart and
no reason to try.

The database file is created by Sagüin at 0600 before SQLite sees it,
rather than left to SQLite's 0644 less the umask. It holds whatever the
applications published.

Closing runs `wal_checkpoint(TRUNCATE)` first, and that is what makes a
copy of the main file on its own a complete one. **While the broker is
running it is not**: everything committed since the last checkpoint is in
the write-ahead log, and the main file copied alone can be a database with
no tables in it. Backing up a stopped broker is `cp`; backing up a running
one is SQLite's own backup - `.backup` in the shell, or `VACUUM INTO` -
which is what copes with a file being written to while it is read.

A failed checkpoint at shutdown is worth saying and not worth refusing to
shut down over: the data is committed either way, and the next open
replays the log.

### Reading it while the broker runs

An ordinary SQLite reader in another process can query a live provider,
including records committed since the last checkpoint, read-write or
read-only. `immutable=1` is the exception and it is a quiet one: it tells
SQLite to ignore the write-ahead log, so the reader sees an empty or stale
database rather than an error about it.

Nothing stops such a process **writing**, and the lock below does not
defend against it. Deleting records with `sqlite3` under a running broker
removes them without raising the floor, which is the one failure
invariant 1 exists to report, introduced by hand. The file is open for
reading; treat writing to it as editing the broker's memory.

**Read it where it is, and never copy the database file on its own.** This
is the same failure as `immutable=1` reached a different way, and it is
the way somebody actually reaches it, because copying a file is what a
backup script does. While the broker runs, `saguin.db` is one page with
no schema in it and `saguin.db-wal` holds the tables and every record. A
reader opening the real path sees both and answers correctly; `cp
saguin.db elsewhere` produces a database that reports **`no such table:
records`** - not a lock, not a permission error, an apparently empty file.
Copying the `-wal` alongside it reads back the records - and is still not
a backup: two `cp`s of a pair being written to are not one moment, so the
copy can hold a log that runs past the database beside it, and nothing
reports that either.

What makes this worth writing down rather than leaving to be discovered is
that it depends on whether the broker happened to be running. Shutdown
checkpoints, so the same `cp` on a stopped broker produces a complete
database. A backup taken while the broker is down works, the identical
command taken while it is up silently does not, and nothing reports the
difference until the copy is needed.

To take a copy of a live provider, use `VACUUM INTO 'somewhere.db'` or
`sqlite3`'s `.backup`, both of which fold the log in and produce one file
that stands alone. `cp` is not a backup of a WAL database.

**Where the copy belongs on another machine, `sqlite3_rsync` does the whole
job in one command** - and the underscore is part of the name, which is
worth knowing before a runbook is written against the wrong one:

```sh
sqlite3_rsync /var/lib/saguin/saguin.db backup@host:/backups/saguin.db
```

It reads a live database, sends only the pages that differ from the copy
already at the far end, and leaves a consistent one there: a point in
time, taken while the origin goes on publishing, and a second run sends
only the pages written since. A store measured in gigabytes is where it
pays.

Three things to know before it goes in a runbook. Its own usage describes
one side as `USER@HOST:PATH` and the other as a local path, and the remote
form is **SSH with the same program installed at both ends** - which is
what `--exe` and `--ssh` are for. Two local paths also work. It arrived in
**SQLite 3.47**, described as experimental there, so an older host does
not have it at all - and Sagüin's own version says nothing about this,
because the broker links `modernc.org/sqlite` rather than the C library an
operator installs. Where the destination is a mounted volume rather than
another host, `VACUUM INTO` remains the answer: it needs nothing installed
anywhere.

**Sagüin never runs `VACUUM` on its own database - not at startup, not at
shutdown, not on a timer - so a file that has grown stays that size.**
Pages that retention or a resolved job free are reused by the next writes,
never returned to the filesystem, and `saguin_provider_bytes` counts them,
so after retention deletes a week of records the figure stays where it
peaked; that is not retention failing. Giving the disk back is the
operator's decision and is done on a stopped broker:
`sqlite3 /var/lib/saguin/saguin.db 'VACUUM'`, which rewrites the file and
needs up to its own size free while it does. It is not a startup option
because startup is the wrong moment for it: the rewrite holds the write lock
for as long as it takes - minutes for a large file on a card - while a fleet
is reconnecting and a supervisor is timing the health check, and a disk too
full to hold the copy would stop the broker starting at all.

## One broker at a time: the lock

One broker holds a storage provider for as long as it has it open, and one
that finds a provider held refuses to start. RFC 0002 has that rule, the
two lock file names, and what goes wrong without it. Five things about the
mechanism belong here instead.

**The lock is a tiny SQLite database of its own**, not the provider's
database. Opening that exclusively would work and would lock out `sqlite3`
with it, and reading a live channel with ordinary tools is a reason to
choose Sagüin over a broker with a private format. A memory provider's
lock being a SQLite file is odd enough to say out loud: SQLite is what
makes the locking portable across the platforms Sagüin builds for, the
binary links it regardless, and a memory provider has no file of its own
to take a lock with.

**It is claimed by writing to that database, and the write is
unconditional.** Exclusive locking mode takes the file's exclusive lock on
the first write and never gives it back, while a read takes only a shared
lock - and shared locks do not exclude each other. A conditional write is
therefore not a lock at all on the second run: `CREATE TABLE IF NOT EXISTS`
writes nothing once the table is there, so two brokers meeting an existing
lock file both take shared locks and both start. A lock file exists on
every run but the first, which is the whole life of a deployment.

**It sits beside what it guards.** A temporary directory keyed by a hash
of the path is swept on a timer, is not shared between containers that
share a data volume, and does not notice that two configurations spelled
one path two ways. Beside the file, the filesystem resolves identity and
nothing sweeps a data directory.

**It is taken before the provider's database is touched at all**, so a
broker that may not have it does not write into a running one on its way
to finding out.

**It stops a second Sagüin and not a second writer.** Nothing in it
prevents another process opening the database and inserting or deleting
rows; SQLite is designed to allow exactly that. What defends the file from
everything except another broker is the filesystem, and the section above
says what a stray write costs.

## Moving a channel between providers

A channel names a provider in the configuration. Changing that name moves
the channel, and what happens to its records depends entirely on whether
the operator moved them too.

### What changing the name does on its own

**The channel starts fresh**, at `next = 1` and `floor = 1`, as though it
had never existed. The broker permits it and says nothing, because it is
the operator's decision to make: a channel may be abandoned deliberately,
and a broker that refused would be standing between somebody and their own
data.

Three consequences, none of them obvious from the one word that caused
them:

- **The old records are not deleted. They are unreachable.** They stay in
  the provider the channel used to name, and the only thing that reads them
  again is pointing the channel back at it.
- **Pointing it back makes the new records unreachable in the same way**,
  and does it to a channel that has meanwhile been reissuing offsets 1, 2,
  3 for different records. Two histories then exist under one name, and
  which one a consumer sees depends on a word in a file.
- **A durable consumer is not told.** Its position is stored beside the
  records it points into, so it does not cross either. Moving forward, the
  consumer has no position in the new provider and replays from the
  beginning, which at-least-once permits. Moving *back*, it finds its old
  position, resumes there, receives nothing, and reports itself caught up
  over every record written while the channel was elsewhere. That is the
  failure invariant 1 exists to report, arriving from a configuration edit
  instead of from retention.

There is a fourth for anything outside the broker. `saguin-offset` rides on
every delivered `PUBLISH`, so an application that recorded "processed up to
4500" holds a number that now names a different record.

Sagüin does not police any of this, and does not tidy up after it. The
files in the provider a channel has left are the operator's to keep or
remove.

### Moving the records with the channel

Two commands, run with the broker stopped:

```sh
saguin --snapshots-to-sqlite  /var/lib/saguin/snapshots  /var/lib/saguin/saguin.db
saguin --sqlite-to-snapshots  /var/lib/saguin/saguin.db  /var/lib/saguin/snapshots
```

They carry `next` and `floor` as they stand, every record at the offset it
already has, every queue record with its attempt count and delivery
history, and every consumer position. A channel that arrives this way means
what it meant: offsets are not reissued, and a consumer resumes where it
stopped rather than replaying or skipping.

The rules they enforce, and why each one is there:

| | |
|---|---|
| No `--config`, and it is refused rather than ignored | A migration reads and writes the paths it is given. A configuration is loaded whole or not at all, so requiring one would put this out of reach of an operator whose configuration had drifted - which is when they need it |
| The destination must not exist, or must be a directory holding no snapshot and no manifest | Writing beside channels that are already there is how two histories end up under one name |
| The broker must be stopped | Both take the provider's lock. A directory a broker is serving is one its next shutdown will rewrite |
| The source is never touched | It is the remaining copy until the destination has proved itself. Even the expired positions a starting broker would drop are left alone |
| Nothing is created when there is nothing to migrate | An empty destination would satisfy "must not exist" and lock the operator out of retrying that path |

A failure is reported and the conversion carries on, one snapshot file at a
time, so an operator whose disk filled learns every channel that did not
make it rather than only the first. A queue and its dead-letter channel are
one file and one transaction, so the move between them can never be
recorded by half.

Neither command reads the configuration, so both work on a copy, on another
machine, and on a directory from a backup.

**The order that works**, and the reason it is this order:

1. Declare the destination provider in the configuration. Nothing is
   created for it: a provider no channel names is inert, which is what
   leaves the migration a path to write.
2. Stop the broker.
3. Run the migration. It takes as long as it takes - this is why it is a
   command rather than something a shutdown does under a stop timeout.
4. Change the channel's `storage:` to the new provider.
5. Start the broker, and confirm the channel before removing anything.

## Backing up and restoring

Sagüin's answer to "the machine is gone" is one mechanism, a file copy of
whichever artifact the channel's provider keeps, and what the copy carries
depends on the provider rather than on the channel's type. "Restoring on
another machine" below is the procedure, and it is the same for both.

| | Carried by | Recovery point |
|---|---|---|
| Everything on a sqlite provider | A copy of the provider's database, taken by the operator while the broker runs | Whenever the last copy was taken |
| Everything on a memory provider | A copy of the provider's snapshot directory | The last graceful shutdown, which is when those files are written |
| Broadcast | Not recovered - nothing is stored | - |

**A bridge is not a backup**, and this is the sentence to take away. It
carries live traffic from another broker into channels here, and what
arrives is a new record at an offset this broker assigns - so two brokers
bridged to each other hold two independent sets of records that agree about
neither offsets nor positions. Neither is a copy of the other, and neither
can be promoted to stand in for the other.

**The difference between the two rows is when a copy may be taken, not
whether one exists.** A database can be copied from a running broker;
snapshot files are written at a graceful shutdown, so a copy taken while
the broker runs holds the state as of its last stop, and a broker lost to
power failure has written nothing since. That is the choice a provider
decides, and it is worth stating before somebody makes it rather than
after.

### A database copy carries the whole provider, positions included

**A provider is one database and every table in it travels together** -
`records`, `latest_values`, `queue_items` with their attempt counts, and
`positions`.

**That last one is the reason this section is worth reading twice.**
Consumer positions are broker state rather than records, so nothing but the
database copy carries them - and recovered from one, the same consumer
resumes exactly where it was.

Driven end to end rather than reasoned about, on a live provider with a
writer publishing throughout: 500 records, 50 latest values, 100 queue jobs
of which 25 were acknowledged and 15 returned so their attempt counts
advanced, and a durable consumer that read 200 records and stopped.
`sqlite3_rsync` to a replica, the origin killed, a broker started on the
replica with the same configuration. The replica held the same 3,965
records, 50 values and 75 queue items with their attempts, and the same
consumer, reconnecting with its own client id, **resumed at offset 201** -
the record after the last one it had acknowledged - rather than at the
floor.

**What it costs, and it is a sync interval rather than a rate.** The
transfer is whole 4KiB pages plus about 4KB of protocol, so the cost per
record falls as the interval lengthens. Measured on the same provider,
records of about 120 bytes:

| Written since the last sync | Sent | Per record |
|---|---|---|
| nothing | 4.1KB | - |
| 100 records | 53KB | 533 bytes |
| 1,000 records | 271KB | 271 bytes |

Syncing more often does not send less; it sends the same pages more times.
The interval is the recovery point, and it is the only knob.

**What it does not carry** is what is not in the database: a memory
provider's channels, which have only their snapshots, and the retained
broadcast store where that is on memory. And several providers are several
databases, each internally consistent and none consistent with the others,
which is a reason to keep a deployment's channels together where their
recovery has to be together.

### Back up the provider, never the channel

A queue and its dead-letter channel are two tables in one database so the
move between them is one commit. Copying one without the other produces
exactly the outcome dead-lettering exists to prevent, from a backup script
rather than from a crash.

**`cp` is not a backup of a running broker** - "Reading it while the
broker runs" above has the measurement and the three commands that are:
`VACUUM INTO`, `.backup`, and `sqlite3_rsync` for another machine. The
identical `cp` against a *stopped* broker works, which is what makes this
worth writing where a backup script gets written.

**Only the queue's provider is copied.** Append and latest channels are
already on the second broker, and backing them up as files too would leave
two copies of one channel with no rule for which is current - the bridge is
usually ahead, but a link down for a day and a copy taken five minutes ago
is the other way round, and nothing reports which. One channel, one
mechanism, and the question never arises.

### Give the queues a provider of their own

Not tidiness. It is what makes a restore surgical: a provider holding only
queues cannot overwrite a channel the bridge has been feeding, so step 3 of
the runbook below can never do damage that nothing reports.

It is cheaper too. A queue table holds unresolved work rather than a
history - bounded by the very `max_bytes` being checked - so `VACUUM INTO`
over it is small and can run often, which is most of what closes the gap
between the two recovery points above. **The dead-letter channel is what
spoils that**, because it *is* a history and accumulates: `dlq_retention_bytes`
is the number that actually decides what a backup costs.

### The lock files travel, and do not matter

Both providers leave a lock beside their data - `<file_path>.lock` and
`saguin.lock` in the snapshot directory - and both survive a shutdown, so
`cp -r` of a data directory carries them into the backup. A broker started
over stale lock files starts, and a restored directory holding both of
them starts with every channel loaded. The lock is held by a live
exclusive connection rather than by the file existing, so a lock whose
holder is gone is an ordinary file. A broker meeting one a live process
*does* hold is refused, naming the provider and the lock.

### Restoring on another machine

A restore is a file copy and nothing else - there is no import step and no
command to run. Both formats are self-describing, so a database or a
snapshot directory means the same thing on any machine that can read its
version:

1. **Put the file where the configuration says.** A provider's `file_path`
   or `snapshot_dir` is the only thing that decides what it opens, and the
   broker must not be running while it is written.
2. **Start, and confirm the channel before removing the backup.** The
   restored file is the remaining copy until the broker has proved it.

A memory provider restores the same way - its snapshot directory is
ordinary files, and a channel whose file is damaged, truncated, or in a
format this broker does not read is a startup error naming what was wrong,
never an empty start.

### When the active machine is gone

Manual, and it stays manual. Nothing here promotes anything.

1. **If the failed broker is still running, stop it first.** A graceful
   stop writes its memory snapshots and lets a last `VACUUM INTO` be taken,
   so a planned failover loses very much less than an unplanned one.
2. **Shut the second broker down gracefully, and read what the shutdown
   said.** A shutdown in which one channel's file could not be written
   reports the failure and leaves that channel's last good snapshot exactly
   as it was - so a channel named there has lost everything since that
   snapshot, and it is worth knowing before the promotion rather than
   after.
3. **Copy in the queue provider's backup, and only that one.**
4. **Start it as the new active.**

Two things keep step 4 small, and both are decisions taken long before it
runs. **Keep the two configurations identical apart from the bridge
block**, so a promotion is one edit rather than one improvised at three in
the morning; `--check-config --output` is what proves they are - it prints
each side's resolved configuration as one document, which is what makes a
`diff` between two deployments mean something. The other is declaring the
second broker's queue channels up front, so their dead-letter channels
exist and the provider a restore lands in is already named - which is
invisible until a promotion needs it.

Writing that second file is the step worth doing carefully. A channel
nobody remembered to declare is a channel the restore has nowhere to put,
and a `limits` block that drifted tighter refuses records the first broker
had already acknowledged to a producer - both quiet until the moment they
are not.

Pointing clients at the new broker is DNS, a virtual address, or whatever
the deployment already uses. Sagüin does not do it and does not know it
happened.

**There is no failing back.** The old machine comes back as a fresh copy
target - its stored data cleared - or it does not come back. Two histories
that both moved forward cannot be merged without a rule for which write
wins, and every such rule loses somebody's data silently.

### The one number nobody has

An operator gets two recovery points and Sagüin can only see one of them.
It knows how far behind the bridge is, to the record; it knows nothing
whatever about when the last database copy was taken. So a figure reported
for one **may never be presented as covering the other** - that is
invariants.md 14's reporting rule, reached from a direction it was not
written for, and the failure it prevents is a dashboard saying "three
seconds behind" about half a broker.

## Enforcing a size bound

RFC 0002 says which bound a channel or a provider may have and what
happens at it. How each is enforced belongs here, because no two of them
are the same mechanism.

**A channel's size is a counter, kept in the write that already runs.**
Every append updates the channel's row to advance `next`, so the bytes it
holds ride in the same statement - `SET next = ?` becomes `SET next = ?,
bytes = ?`, one more bound parameter in one transaction that was happening
anyway, and it adds no measurable cost to an append. So a size bound is
not a reason to hold a channel in memory rather than in a database, and an
append channel has it on either.

It is stored beside `next` and `floor`, but for a different reason than
they are. Those two must not be derived because deriving them is *wrong* -
the answer loses what was removed. Summing the bytes over the surviving
rows is perfectly correct; it is simply a scan of the whole channel, paid
at every open. Not having to read a channel at startup is most of what a
database buys over a snapshot, so a column keeps it.

**A queue's is summed at open instead, and kept only in memory.** Its table
holds unresolved work rather than a history, bounded by the very
`max_bytes` being checked, so the scan is small - and storing it would put
a second statement on the resolution path, which is the hot one. The rule
is the shape of the table rather than the kind of channel: a history must
not be scanned, and a working set may be.

**A `latest` channel keeps no counter of its own**, because RFC 0002 gives
it no size bound. That is decided on its shape, and the shape is the cost:
a replacement is a difference, so the value being overwritten would have
to be read before every write - a point read on the publish path, paid to
feed a number nothing bounds - where the same accounting on an append
channel rides a statement that was already running. On a memory
provider its values still count against the *provider's* bound, where the
value being replaced is already in hand and the difference is free.

**A sqlite provider's bound is SQLite's own.** `PRAGMA max_page_count`
bounds the database to a number of pages, which is `max_bytes` divided by
the page size. The engine refuses the write itself with `SQLITE_FULL`, so
there is nothing per-write to maintain and nothing for Sagüin to check
first - the store recognises that one error and turns it into `0x97`
rather than the `0x83` any other write failure gets. The database stays
readable at the bound, and freeing pages lets writes through again.

**The page size is pinned at 4KiB on a database Sagüin creates.** A bound
in pages means an operator's figure rounds down by up to one page, and
left to whatever default a build carried, two machines running one
configuration would round it differently. On a file made elsewhere the
size is read back rather than assumed, because SQLite ignores the pragma
outside a `VACUUM` and computing pages from a guess would bound the wrong
amount.

**So this bound is the file's, where a channel's is its records'.** A
channel's `retention_bytes` and `max_bytes` count record sizes, and the
file holding those records is larger than their count; RFC 0002 "Bounds on
what a channel holds" says by how much.

Three rules fall out of using the engine's own counter. A bound that
rounds down to no pages is refused at startup, because SQLite reads
`max_page_count = 0` as "tell me the current value" rather than "allow
nothing", and a provider given a few hundred bytes would otherwise come up
silently unbounded. A bound below what the database already holds is not
an error: SQLite keeps the pages it has and refuses to add more. And what
the bound actually came to - where publishes stop, what is reserved, the
page size it was rounded in - is stated once at startup, because it is no
longer quite the number the operator wrote and arithmetic is the wrong way
to discover that.

### The reserve

`SQLITE_FULL` does not distinguish a publish from the operations that
would relieve the provider, and RFC 0003 says those are never refused. So
a provider holds back a **reserve**: one largest possible record with its
headers, which publishes may not take and the acknowledgement, the attempt
count, a stored position, the dead-letter move and the release of a held
exactly-once publish may.

It is carved out of `max_bytes`, never added to it, so the file stays
inside the operator's figure. Its size comes from `max_message_size` plus
`max_header_bytes` and is not a configuration key of its own - a knob
nobody can reason about, and one that strands work when set low. A
`max_bytes` below twice it is refused at startup naming both numbers.

The two providers reach it by different mechanisms, which is the same
split as everything else here: a memory provider holds it back inside its
own counter, and a sqlite provider keeps a second page ceiling and raises
to it **inside the relief transaction**. Both pragmas go in that
transaction rather than around it, because the single write connection is
what makes a transaction exclusive - raising outside would leave a window in
which an ordinary publish took reserve it is not entitled to.
`max_page_count` is connection state rather than content, so a rollback
does not undo either pragma and the ceiling is restored on both paths.

**The reserve is spent, not lent, and only on sqlite.** SQLite will not
set a ceiling below the pages a file already holds, so a relief write that
grew the file keeps that room until the file is rebuilt. It is spent
*once* rather than per operation, because the pages a removal frees are
what the next move writes into. So the reserve is room to grow into on the
way to a full provider rather than an allowance that renews, and a
`VACUUM` is the only thing that returns it.

On a memory provider the reserve buys something slightly different, and
this is worth saying because the same word covers both. The move there
charges the counter rather than asking it, so it could never be refused;
what the reserve does is keep the resulting overshoot *inside* the
operator's figure instead of just outside it. Both providers then mean the
same thing by `max_bytes`.

**A memory provider's bound is a counter shared by its channels**, because
there is no engine underneath it. Reserving and checking are one
operation, so two channels cannot both be told yes for the last of the
room, and every removal gives its bytes back. It is charged for whatever a
store already holds when it is attached, since a store restored from a
snapshot arrives full of records that are in memory whether or not
anything counted them.

Two honest edges. The sqlite bound is on the main database file, and the
write-ahead log grows beside it until a checkpoint, so what is on disk
transiently exceeds `max_bytes` by the size of that log. A checkpoint
cannot pass a reader still in the log, so sustained reads - consumers
reading channels on the read connections beside the writer - widen that
transient: each read is one short snapshot, but with consumers reading
continuously under saturated load a checkpoint rarely finds the log
without a reader, and it was measured at up to about 300 MiB. And the two
providers measure different things - the file on one, what was published
on the other - so the same number means somewhat different amounts on
each.

## Removing a record

**The floor advances in the same operation as the removal.** On sqlite
that is one transaction, holding the deletion, the new floor and the new
byte total together. Two transactions leave a moment in which the rows are
gone and the floor still covers them, and a consumer reading across that
moment is served the oldest survivor and reports success over records it
never received - which is the failure the floor exists to report.

**The bytes go back in that same operation**, to the channel and to its
provider. This is the store where forgetting is permanent: an append
channel's total is a column, so a missed decrement survives every restart
and the channel refuses publishes against room it is no longer using, with
nothing anywhere saying why. A queue sums its own at open, so the same
mistake there is gone at the next start - which is exactly why the append
case is the one to watch.

**A record with no timestamp is never too old.** An unset time is stored
as 0 and taken at face value is a date in 1754, so a channel of them would
be emptied by the first sweep that ran - silently, because retention
removes behind a producer and never says so.

The rows to remove are found by walking from the oldest and stopping at
the first that has to stay, so a sweep reads what it removes rather than
what the channel holds. That is what makes a sweep cheap enough to run
often, and flat in the channel's length, which is the property that
matters. `BenchmarkTrimOnALongChannel` in each store - one record removed
from a channel of the size shown, at `-benchtime 2s -count 2`, the sqlite
file on the machine's own disk:

| records held | memory - Ryzen 7 260 | M1 Pro | sqlite - Ryzen 7 260 | M1 Pro |
|---|---|---|---|---|
| 10,000 | 65–80ns | 63ns | 100–103us | 66us |
| 100,000 | 51ns | 38–39ns | 48–95us | 66–67us |
| 800,000 | 32–37ns | 39–42ns | | |

The sqlite rows are wide because they are the disk's - a removal is a
transaction - and the 800,000 row is memory only.

Sizes are computed with the same function both stores count by, never in
SQL. Headers are stored as JSON here and that function counts their keys
and values, so a sweep measuring `length(headers)` would decrement by a
different number than the append had added.

**The memory store removes from the front and does not shuffle.** It keeps
an index of how far the dead prefix reaches and clears each removed record
as it goes, so the payload stops being reachable at the moment it stops
being counted - a memory provider's bound is a number about memory, and a
record counted out of it that is still held is that number lying.
Compacting on every removal instead costs what *survives* rather than what
went, under the channel's own lock, for removing one record - a stall a
publisher feels. It compacts once the dead prefix reaches half the
slice, and reallocates rather than copying in place when the array has
grown far past what is left in it.

**The broadcast log also lets a message go from the middle**, once nobody
owes it, and does not shuffle for that either. The message is cleared where
it stands and keeps only its offset, every read skips it, and it counts
towards the same half. A release therefore costs the same wherever in the
log the message stands.

Everything that reads a memory channel's records skips that prefix. A
snapshot written from the whole slice is the silent one: it brings back
records below the floor stored beside them, so the channel's own two
numbers disagree with its contents at the next start.

**Retention does not use the provider reserve.** It is an operation that
*frees* room rather than one that relieves a full provider on Sagüin's
behalf, so it may be refused when there is none to spare - and it simply
runs again on the next tick.

**Which is not the livelock it sounds like, and the difference is worth
stating rather than leaving a reader to work out.** "Refused, and runs
again next tick" describes a provider that never recovers if retention is
what needs the room. It is not: a delete frees pages rather than needing
them, so a provider already refusing publishes at its ceiling runs its age
sweep to completion - the rows go, the floor rises in the same
transaction, and the next publish is accepted. What the reserve is for is
the operations that must write while the provider is full, and retention
is not one of them because it does not have to grow the file to shrink
it.

## What is not built

A snapshot is written only at shutdown, so the window a crash loses is the
whole life of the process rather than an interval. That is invariant 14
holding rather than a defect, and shortening it costs the broker's lock for
the length of a write.
