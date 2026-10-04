# Invariants

Eighteen things Sagüin must never do.

Each is written as **the failure it prevents**, because nearly all of them
fail silently: a plausible-looking implementation reports success while
losing or duplicating data. A rule without its failure gets "simplified"
by the next reader, straight back into the bug.

The RFCs pick the mechanisms and may not drop a guarantee. Standard
MQTT 5 outranks every document here - where something in this repository
requires behaviour MQTT 5 forbids, the document is wrong, not the broker.

If this list ever reaches twenty-five, the design has grown past what one
person can hold, and the fix is to remove features rather than to add
invariants.

---

## Retention and loss

**1. A channel distinguishes "you are caught up" from "your position was
deleted."** Every durable channel stores a monotonically advancing floor -
the oldest offset still readable - and a read from below it is rejected,
never served. Otherwise the consumer is handed the oldest *surviving*
record and concludes it processed everything in between, silently.

**One case cannot be reported to the consumer**, and the rule is written
with it rather than around it. A consumer that is connected and reading
when the floor passes it has no CONNECT to carry the signal and MQTT
provides no other, so it restarts at the floor and the broker tells the
*operator* how many records went. Nothing below the floor is served in
that case either - the read is still rejected - but the consumer cannot be
told, which is this rule failing in the one place Sagüin cannot avoid it
without extending MQTT. RFC 0003 has the case and what it needs.

Three ways to reintroduce this bug, each sufficient on its own: advancing
the floor in a different transaction from the deletion; forgetting one of
the removal paths, of which size-based trimming is the one that gets
missed because it does not feel like deleting data; and recomputing the
floor as `MIN(offset)` over surviving rows, which reads as "nothing was
ever removed" on an emptied channel.

**2. Resolution, retention, and eviction are three different mechanisms.**
Resolution removes queue work because it is *done*. Retention is
operator-declared age- or size-based deletion. Eviction is dropping data
under memory pressure, and is never permitted for unresolved queue work.
Conflating them gives either a queue that grows until storage fills and
then refuses writes forever, or one that quietly drops work nobody
processed.

**A publisher's own clock is a fourth, and it is not retention.** A
Message Expiry Interval may remove MQTT's own state and nothing else: the
broadcast retained value, and a message a session is owed - that
session's own copy, not data leaving a channel. It never removes a record
from a channel of any type; otherwise a client deletes out of the middle
of somebody else's replay, the floor does not move, and the consumer
reads past the gap and reports success. What it does on a channel is
arrive on the delivery and be read (RFC 0003).

## Queue correctness

**3. A superseded delivery cannot resolve anything.** Every delivery
attempt carries a Delivery ID. An acknowledgement or return carrying a
Delivery ID that is no longer current changes nothing, and is logged. It
is logged rather than answered: the worker is told nothing, because by
then another worker holds the record and there is no decision the first
one could act on (RFC 0003). Otherwise: worker A stalls past its timeout,
the job goes to worker B, A acknowledges late, and B's work is deleted
underneath it - one job processed twice with the second result discarded,
reported as success.

The check runs on *every* operation naming a delivery, not only on
acknowledge, and consuming a Delivery ID advances the epoch so a client
retry cannot apply twice.

**4. Exactly one consumer holds a queue record at a time.** No
configuration, subscription form, or client behaviour may produce two live
deliveries of one record. Two spellings that both worked would be two
populations of workers, each receiving a copy of every job - both process,
both acknowledge, nothing reports a problem. This is why a queue admits
exactly one subscription form and the broker refuses all others, and why
the broker picks the recipient itself from its own index of who is
consuming which queue rather than leaving the choice to a matched filter.

**5. The dead-letter move is atomic.** A record leaves the queue and
appears in its dead-letter channel in one transaction, or neither happens.
The two halves are separate writes in every natural implementation, and a
failure between them either loses the record - the exact outcome
dead-lettering exists to prevent - or leaves it live in both places. A
move refused for want of capacity leaves the source record untouched,
including its delivery state.

**6. Transport acknowledgement is never application acknowledgement.**
`PUBACK` confirms MQTT moved a packet; it says nothing about whether the
work succeeded. Treating it as completion resolves the record the moment
it reaches the worker's socket, so a worker that crashes mid-job has
already had its work deleted - at-least-once silently degrades to
at-most-once, and only under load, which is when crashes happen.

**7. A record's visibility deadline never overlaps its transport in-flight
window.** The timeout starts at the transport acknowledgement, not at the
send. Otherwise a record can be redelivered while the first worker still
holds the packet identifier unacknowledged; the broker cannot reclaim that
identifier safely, so the worker's flow-control window leaks one slot per
timeout until it silently stops receiving work.

Starting the clock at the `PUBACK` makes the overlap impossible rather
than managed. The cost: a client that defers its transport acknowledgement
until after processing gets no mid-job redelivery, falling back to
redelivery on disconnect. That is a documented degradation, it never
duplicates work, and it is what every other MQTT broker already does.

The rule is about a deadline racing a packet the worker **holds**, so it
says nothing about a packet the worker was never sent - and a record
waiting for a `PUBACK` that can never arrive is stranded rather than
protected. Such a record is returned with its attempt unspent, and this is
not an exception: there is no transport in-flight window at the worker to
overlap with, because the worker has never seen the packet. Putting a
clock on the state instead would be the exception, and a costly one - with
the `PUBACK` deferred until processing ends, "no `PUBACK` yet" is what a
job in progress looks like, so a clock short enough to rescue the stranded
record takes the other worker's job away mid-run and has it done twice.

## Identity and position

**8. Message identity and stream position are distinct.** A Message ID is
stable for the life of the message; an offset is a channel-local position.
Neither substitutes for the other. Conflating them makes a dead-lettered
message a *new* message, so a consumer deduplicating on identity processes
it twice and an operator tracing a failure finds no link between the queue
and its dead-letter channel.

**9. Offsets are monotonic within a channel and never reused** - including
after removal, after a restart, and after a snapshot restore. A reused
offset makes a stored consumer position point at unrelated data, which the
consumer reads in order and reports as success. Restart is the dangerous
path: computing the next offset as `MAX(offset) + 1` over surviving rows
restarts at 1 on a channel retention has emptied.

One thing outside the broker's gift: an operator may point a channel at a
different storage provider, and the channel then starts fresh and reissues
offsets its old records already used. That is their decision and Sagüin
does not refuse it - but it is the one way this rule can be broken, so
RFC 0004 says what it costs and gives two commands that carry the offsets
across instead.

**10. The broker is the only enforcement point.** No rule may depend on a
Sagüin SDK, a convention, or a well-behaved client. A stock MQTT client
reaches the same broker with the same credentials, and a validation living
in an SDK makes correctness depend on which library somebody's contractor
chose.

**11. A wildcard never reaches a queue.** A queue admits exactly one
subscription form. A filter that merely crosses one is served everything
else it matches and none of the queue's records. Otherwise `SUBSCRIBE #`
quietly drains a work queue into somebody's debugging session, and the jobs
it took are gone from the workers that were meant to do them.

A filter does reach every `append` and `latest` channel it matches, and is
served each record under the semantics that record's topic has - replay
from a stored position, or the current value carrying RETAIN. That is what
a filter is for: a client subscribes the way MQTT already works and never
has to know where the channels are.

**A shared subscription is the one form served less than that, and it is
MQTT's rule rather than an exception to this one**: what is published
after its group began - no replay, and no pass of current state - split
across the group, and nothing from a queue. A group with a member whose
session outlives its connection holds that while its members are away,
returns what an ending member had not acknowledged where MQTT lets
another member have it, and keeps it across a restart, over a channel as
over broadcast (RFC 0002 "Shared subscriptions", RFC 0003 "Broadcast").
**A subscriber may also ask to be served less** - client-declared
partitioning, its own choice, with what it costs in RFC 0003. And the
alternative to serving a filter fully is quieter and worse: a filter
served only where it fits inside one channel would be a client that asked
for everything about one device, was told yes, and was given half (RFC
0002 "What a filter reaches").

**12. Which channel a topic belongs to is decided by the configuration,
never by the broker.** Where two channels' filters both match a topic, the
one that spells the topic out most exactly holds it: a spelled-out level
beats `+`, `+` beats `#`, and the first level at which two filters differ
decides. Two channels carrying the same filter are refused at startup,
naming both.

If the broker picks, it picks consistently only until a map iteration
order changes - at which point records land in two channels with two
retention policies and two delivery semantics, out of a configuration file
nobody edited. A rule an operator can read off their own filters is a pick
that never changes, and it leaves the broker no tie to break: two filters
that both match a topic and are equally exact at every level are character
for character the same filter.

## Bounds and durability

**13. Everything that accumulates is bounded, with defined behaviour at
the bound.** Enforcement happens *before* allocation, against the declared
size, not after the bytes are read. The broker is the memory-pressure
point for every client attached to it: one unbounded output buffer turns
one slow consumer into a dead broker, and on edge hardware "bounded by
available RAM" means bounded by the OOM killer, which arrives without a
log line. The outcome at the bound is a reason code, a disconnect, or
backpressure - never "grow a bit more".

**A session is one of the things that accumulates**, in three directions
at once - how many are held, how much each is owed, and how long a client
that never returns keeps one - each with its own bound and defined
outcome: the provider's `max_bytes`, `limits.session_queue_bytes`, and
`limits.max_session_expiry` (RFC 0002 has each). **What a session counts
against `max_bytes` is memory too**: the broker holds an away session's
client and filters for as long as the session is held, and charging its
strings alone admitted hundreds of times the figure. A sqlite provider's
`max_bytes` bounds its file rather than memory, so there how many are held
is bounded by `limits.max_session_expiry` and the rate new client ids
arrive, which RFC 0002 states. **What `limits.session_queue_bytes` counts is
memory**: each delivery its message and the measured cost of its entry, on
every path a delivery can wait on, so small messages cannot hold more than
the bound says. **A restart is not a way around any of them**: what a start
puts back it also counts, and a session whose expiry passed while the broker
was stopped is ended at that start. **A shared group's backlog accumulates
in the same three directions, in the same provider, and takes the same
bounds**, cannot outlive the sessions it belongs to - the rule that decides
where a Will lives - and `broker.share.expires_after` bounds the wait
itself.

**14. Memory durability is exactly the last successful snapshot, and no
document, metric, log line, or API response implies otherwise.** A
memory-backed channel survives a graceful shutdown and does not survive a
crash. Otherwise an operator sizes an edge deployment on memory storage
for its speed, loses power, and discovers the guarantee depended on a
shutdown path a power cut does not take.

Two consequences: the known-good snapshot is never overwritten before its
replacement is fully written and synced, and a snapshot failing its
version or integrity check is a startup error naming what was wrong -
never an empty start, which is indistinguishable from a fresh install and
destroys the evidence.

The same rule holds for a store that keeps its own records, because
"survives a crash" is not one guarantee. A committed record survives a
process crash, an OOM kill, and `kill -9`, whole. A power cut is
different: SQLite storage runs `synchronous=NORMAL`, which does not sync
on commit, so what was committed in about the last flush interval plus one fsync
(`flush_interval`, 150 ms by default) can be lost *after* it was
acknowledged. That is the trade it makes for its write rate, and it is
the right trade - but it is the same class of loss as the paragraph
above, so it is written wherever the durability is promised rather than
left for a power cut to teach.

**15. Restart never resurrects a delivery as live.** After a restart no
record is in flight; deliveries, Delivery IDs, and deadlines are not
restored. A restored deadline belongs to a session that no longer holds it,
so work stalls for the length of a timeout nobody is racing, and a
restored Delivery ID can be resolved by a client retrying across the very
restart that interrupted it. Attempt counts do survive, so a record that
has exhausted its attempts is still dead-lettered rather than starting
over.

**This is about a channel's records, and it holds although sessions come
back.** A channel keeps its records and a session holds a position in them; a
queue keeps its work and hands it out under a lease. Neither is session
state, so neither is restored with a session: the consumer resumes from its
stored position and the queue offers its work again, each by its own path. A
session's own messages - a broadcast or shared-group delivery it was owed, and
an exactly-once publish it had sent and not released - do come back with it,
and that is a different thing from a lease being resurrected.

**16. No client can withhold another client's acknowledgement.** A client
that stops reading its socket - while the connection stays open - must not
be able to hold whichever goroutine is delivering to it, and must not be
able to delay a publisher at all. Otherwise an unrelated publisher's
`PUBACK` is withheld for a record already stored, its library concludes the
broker is gone and re-sends, and the channel holds that record twice while
the publisher reports one successful publish.

**A record is durable before anybody is told about it, so a publisher
waits on storage and on nothing another client does.** A `sqlite`
provider makes that wait longer by collecting: by default a publish
arriving while a transaction commits waits for that commit, any already
queued behind it, and then its own, each of at most 256 records, and
`publish_commit_interval` lets an operator make a transaction wait up to
its interval for more. Other clients' session writes are collected too,
and a transaction of them gives way to a waiting publish after the write
in progress, so a publish waits behind one client's write and a commit,
not behind the group. Either way it is a wait on storage, bounded by
commits of a known size - Sagüin's ceiling or the operator's figures -
rather than by a stranger's socket. A delivery to a consumer runs on a
goroutine of its own - one per consumer and channel, claimed before it
starts so that two never write one consumer's records out of order. A
delivery written on the goroutine that published the record would make the
publisher's acknowledgement wait on a stranger's socket, and no bound on
that write removes it: a publisher less patient than the bound gives up
inside it and re-sends.

**Every write to a client is bounded by `limits.write_timeout`, and a
client that does not take one in time is disconnected.** Where an operator
sets it to `none`, the client's keepalive bounds the write instead, and one
with no keepalive is not bounded - except that a shutdown or a takeover
waiting on such a write cuts it, so neither waits for good. Two routes reach
that bound. A channel's records to an ordinary subscriber, a dead-letter
record and every control reply are written by Sagüin. A queue offer, a
broadcast delivery, and a channel's live records to a shared subscription
are written by the substrate, which arms the same deadline inside the
client lock it holds across the write.

**A refusal on the way out is not a delivery.** The substrate asks the
`acl_file` for the worker it has chosen, and a queue record refused there had
already been marked as being delivered - so it sat with no holder and no lease
clock, which invariant 7 only starts at the acknowledgement, and the offer
loop feeds only what is waiting. The job was acknowledged to its publisher and
then belonged to nobody: never processed, never returned, never
dead-lettered. So the grant is asked where the worker is chosen, before the
record is handed over, beside the two questions already asked there - is this
worker still live, and has it room. A record nobody may take goes back with
its attempt unspent, which is what that path already did for a worker that had
gone.

**Both routes ask the `acl_file` per delivery** - the substrate for each
delivery it makes, Sagüin's own pump for each record it feeds a consumer.
A grant checked at SUBSCRIBE alone would hold only while the file cannot
change under a running broker, and `SIGUSR1` re-reads it. The failure is
silent: an operator narrows a role, watches the broadcast stop, hangs the
client up - and the device resumes its durable session, sending no
SUBSCRIBE, so nothing is left to refuse, and goes on receiving channel
data. A consumer refused is stalled where it stands rather than stepped
over, so nothing is lost if the grant comes back - and the grant coming
back is itself what wakes it, rather than the next thing published, which
on an idle channel may be nothing at all.

**The second route is the one to know about, because the substrate holds
a client's lock across the write.** A packet identifier taken under that
lock, before the packet reaches the queue that sheds a slow client, hands
the lock to whoever delivers - so one client that stops reading freezes
the loop that offers every queue's records, expires jobs and runs both
retention sweeps, and hangs every publisher whose message matches it:
with 30,000 QoS 1 publishes to one deaf subscriber, a publisher held to
that lock waits 10.62-10.73s for an acknowledgement that takes 1.11-1.13s
against a subscriber that keeps up.

**So the identifier is taken without that lock, and a delivery is written
by the client's own write loop and by no other goroutine.** A publisher
hands the delivery over and moves on, so a client that stops reading holds
up its own write loop and nothing else - which `limits.write_timeout` then
ends. What this rule promises is that a *third party* is still served:
another queue's worker, another publisher, the sweeps.

**A publisher still waits on storage**, which is the wait this rule allows:
it waits for the one write that keeps the message in the provider's
broadcast log, however many sessions are owed it, and never for anything a
session does.

**Disconnecting rather than dropping is the half that is Sagüin's to
choose.** A consumer's position advances only on its own acknowledgement,
so hanging up costs it nothing. Broadcast has no position, so what its
outbound queue cannot hold is dropped and counted - instead of
*buffering*, never instead of the deadline: a deaf QoS 0 subscriber is
shed and then disconnected like every other, and one at QoS 1 or 2 is
held within its session's bound (RFC 0002). The deadline is what keeps a
deaf consumer from holding the broker's own work; the delivery goroutine
answers the publisher, and the bound answers everything else.

## Ownership

**17. Only the connection that owns a client id changes what the broker
keeps for it.** One connection owns a client id's session at a time, and
nothing else - a refused `CONNECT`, a connection that never became the
owner, one that has been taken over, a teardown running late - changes it.
Otherwise a device whose second connection fails halfway through connecting
has the session its first connection is still using ended underneath it: an
exactly-once publish the broker already acknowledged is dropped, its
positions go, and it stops being served while it stays connected, with
nothing reporting a problem. RFC 0003 "Sessions" has when ownership moves
and every way a session ends.

## What a client is told

**18. Nothing is told to a client before the state it describes is
stored, with two exceptions, both below.** An acknowledgement, a `CONNACK`'s
Session Present, and the close that answers a `DISCONNECT` each tell a
client that something is done, and the broker stores what makes it true - as
far as its provider promises - before it sends them. Otherwise a crash
between the two breaks a promise the client already holds: a publisher told
its message was kept finds it gone, a device told its session is new is
served the old one, and a consumer that acknowledged a record and
disconnected cleanly is sent it again. Invariant 16 is this rule for a
publish.

**The first exception is where a connected session is in what it reads** -
a channel consumer's position, and a session's acknowledgements of the
broadcast log. An acknowledgement is taken for writing at most
`broker.session.ack_commit_interval` after it arrives, not as it is read -
storing it there would hold every acknowledgement, and the client's window
with it, behind a store write - and it is stored when that write reaches
the provider. A `sqlite` provider has one connection, and the write waits
there behind the writes already queued for it: those a client's packet is
waiting on go first, and the rest take turns (RFC 0004 "Group commit"). So
an unclean stop can send again what a session acknowledged since its last
completed write. For a session's acknowledgements of the broadcast log
that is **never more than its Receive Maximum messages**: the drain sends
nothing more until what was acknowledged has been written. For a channel
consumer's position there is no such count: it is what the consumer
acknowledged in that time, a replay from the stored position and never a
skip. Either is about `ack_commit_interval`'s worth, 200 ms by default,
when storage keeps up, and longer under a storage backlog, such as a
reconnect storm or a slow disk; a `memory` provider has no queue, so there
the bound is the interval. RFC 0003 states that cost. A clean `DISCONNECT`
is not that case: both, and what the `DISCONNECT` changes - its Will
withdrawn, its expiry - are stored before the connection is closed. **The
second is a clean `DISCONNECT` whose writes the provider refuses.** A
`DISCONNECT` has no answer to refuse it with, and its client closes the
connection whether or not the broker does (MQTT-3.14.4-2), so where the
provider refuses those writes the broker closes all the same, keeps what
was asked, and writes it again every second until it lands; a crash before
then loses it, and the log names the client.
