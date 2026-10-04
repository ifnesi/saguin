# RFC 0003 - Delivery semantics

**Status:** Draft
**Authoritative on:** what Sagüin actually guarantees. Where any other
document implies something stronger, that document is wrong.

## The guarantee, in one table

| | Delivered | Survives restart | Redelivered |
|---|---|---|---|
| broadcast | to sessions connected at the time | no, unless it asked to be retained ("Retained messages") | no |
| `append` | to every subscriber | yes | on reconnect, from the stored position |
| `latest` | current value per topic, on subscribe | yes | on every subscribe; a resumed session is sent only what changed |
| `queue` | to one worker at a time - one *lease*, which is not one execution: a lease that runs out is reoffered while the first worker may still be working | yes | on return, timeout, or disconnect |

**Sagüin is at-least-once.** A `queue` record may be processed more than
once - a worker that finishes the job and dies before acknowledging will
see it again. Consumers must be idempotent, and the Message ID exists to
make that practical.

**QoS 2 does not change that, and the distinction is the one an operator
has to hold on to.** Exactly-once is a property of the *trip*: it says the
message a publisher sent arrives in its channel once, however many times
the packet is re-sent. It says nothing about the *job*: a worker that
processes a queue record and dies before acknowledging sees it again, at
QoS 2 exactly as at QoS 1, because what resolves a queue record is the
worker's reply and not the transport. Invariant 6 is the same sentence for
the acknowledgement, and it holds at every quality of service.

So a deployment that needs work done exactly once still needs idempotent
consumers. What QoS 2 removes is a duplicate *record*, which is worth
having on its own: it is the one an operator cannot deduplicate afterwards,
because the two copies are identical and both arrived legitimately.

### Exactly once, and where the unfinished ones wait

QoS 2 is a four-packet exchange: the client sends the `PUBLISH`, the broker
answers `PUBREC`, the client sends `PUBREL`, the broker answers `PUBCOMP`.
Ownership of the message transfers to the broker at the `PUBREC` (MQTT 5
section 4.3.3), a full round trip before the client learns the exchange is
complete.

**Sagüin writes the record when the `PUBREL` arrives, not when the message
does.** Between the two it holds the message in the store of the channel it
is for, without an offset: nothing that reads the channel sees it, and it
counts against the provider's `max_bytes`, not the channel's own, from the
`PUBREC`. **A held message is as durable as the channel that accepted it**,
and no more. Its release is one operation in that one store: the record is
written at the channel's next offset and the hold goes with it, so there is
never a moment with both or with neither.

Three things follow, and each of them is a failure that storing on arrival
produces:

- **A restart mid-exchange stores nothing.** Storing on arrival leaves the
  record kept while the publisher, never having received its `PUBCOMP`,
  believes the publish failed - so the application sends it again and the
  channel holds it twice, with nothing on the wire saying so.
- **No offset is assigned early.** One assigned on arrival could be passed
  by a consumer while the message still waited, and the record would then
  become readable below a position that had already moved over it, which is
  invariant 1's failure reached with nothing going wrong.
- **A message nobody releases is not a record.** It leaves when its session
  ends, or after `broker.qos2.expires_after`, and a `PUBREL` arriving
  afterwards is answered `0x92 Packet Identifier not found` - the honest
  answer, because there is nothing left to write.

**An accepted exchange can wait for its channel to make room.** The
provider's room is taken at the `PUBREC`, and the release is written in the
room a provider keeps back for the writes that relieve it (RFC 0004 "The
reserve"), so a provider that filled after the `PUBREC` does not refuse the
release. A channel's own `max_bytes` can. When an `append` channel or a
queue filled between the `PUBREC` and the `PUBREL`, the release is refused,
the message stays held, and the `PUBREL` gets no `PUBCOMP`: the connection
is closed with `DISCONNECT 0x97` (Quota exceeded). The client sends the
`PUBREL` again on its next connection, as MQTT has it do for any `PUBREL`
left unanswered, and the exchange completes once the channel has room - an
`append` channel's retention removing its oldest records, or a queue's
workers resolving what they hold. A `latest` channel has no `max_bytes` of
its own, so its release is never refused for room. Nothing is written
twice, and nothing is lost unless the hold outlives
`broker.qos2.expires_after`: then it is dropped, counted in
`saguin_qos2_abandoned_total`, and a `PUBREL` after that is answered `0x92`.
A release the store fails to write is refused the same way, with
`DISCONNECT 0x83`. A channel already at its `max_bytes` when the `PUBLISH`
arrives refuses it on the `PUBREC` with `0x97`, as it refuses a QoS 1
publish, and holds nothing.

**A `PUBCOMP` follows the release that stored the record, and nothing
else.** No `PUBCOMP` is sent for a refused release, or for a `PUBREL` read
on a connection another has taken its client id from. The exchange is found
where its `PUBLISH` put it, never by resolving the topic again. An expiry
and a release of the same exchange never interleave, so a `PUBREL` racing
the expiry is answered `0x92`, never completed.

**A shared subscriber is served a held publish at its release, not at its
arrival.** A shared group over an `append` or `latest` channel is served
records as they are written, and a held publish is not one yet: handed over at
the `PUBLISH`, a member would hold a message that an abandoned exchange never
stores.

**Only a client's publish is held.** Sagüin's own in-process publishers - a
Will, a bridge - send no `PUBREL`, so a publish of theirs at QoS 2 is written
at once. It is injected exactly once, which is what exactly-once asks.

**A broadcast is held as a channel's publish is**, in the store of the
broadcast log (RFC 0004), from its `PUBREC` to its `PUBREL`, and counted in
`max_inflight_per_client` with the rest. Its release is the log's one
append: the message takes the log's next offset and the hold goes with it,
and it is then delivered - from the log to the sessions that outlive their
connections, and directly to the others. **So a QoS 2 broadcast reaches its
subscribers a round trip later than one at QoS 1**: at the `PUBREL`, not
the `PUBLISH`. It is held whether or not any session will keep it, because
what has to outlive a restart is the exchange: a publisher that re-sends
the `PUBLISH` after one is answered as for a repeat, and the message is
delivered once. A retained broadcast is retained at its release too, just
before the message is written, so no subscriber is served the value of a
publish that was never released - except where a release was refused after
that write and never sent again before `expires_after` or the session's end:
the value then stays retained, the exchange is counted in
`saguin_qos2_abandoned_total`, and a warning names the client. The log has no
`max_bytes` of its own, so the log never refuses a release for room; a log
that has nothing left to give refuses the `PUBLISH` on its `PUBREC` with
`0x97`, before ownership is taken. A retained one's value can find no room
in the retained store's provider, and then its release is refused as a
channel's is, with `DISCONNECT 0x97`, and the message stays held.

**What a restart costs, stated plainly.** Where the session comes back and
the provider of the channel or the broadcast log kept the hold - each as
far as its provider promises - the client is answered
`Session Present = 1`, re-sends its unanswered `PUBREL`, and the record is
written by the exchange that started before the restart. Where the session
does not come back, `Session Present = 0` has [MQTT-3.2.2-5] discard the
half-finished exchange and [MQTT-4.4.0-1] forbid a re-send, so the
application's retry lands once, and the hold the session left is dropped
at the start and counted as abandoned. A restart after the release stored
the record and before its `PUBCOMP` reached the client keeps the record
and not the hold, so the `PUBREL` sent again is answered `0x92`, which
MQTT 5 says is no error during recovery. A broadcast released into the log
and not yet counted for its sessions when the broker stopped is counted at
the start, for every session one of whose subscriptions it matches, and
delivered once. Every way, the record lands once.

**A channel removed from the configuration takes the exchanges held in
it**, and a `PUBREL` for one of them is answered `0x92`. A database still
opened for other channels has those holds deleted at that start, each
counted as abandoned, because nothing could release them and they would
take its room for good. A memory provider does not read a removed channel's
snapshot, so what it held takes no room and is not counted then. A channel
that comes back from a snapshot older than the last shutdown comes back
without its holds, counted as abandoned: that shutdown's broker answered
their releases without them, and a publisher told `0x92` may have sent the
message again. A provider no channel names is not opened at all, and what
it held goes with it, uncounted.

## Sessions: one owner, one ending

A session is what the broker keeps for a client id between connections.
MQTT defines most of it - the subscriptions, the messages the session is
owed, its unfinished exchanges, its Will - and Sagüin adds three things of
its own: a position in each `append` channel, a mark in each `latest`
channel, and its membership of each shared group, which is what keeps that
group's backlog. **All of it follows the rules below**, and the table
after them is every way it moves or ends.

**One connection owns a client id at a time, and nothing else changes what
is kept for it** (invariant 17).

**A CONNECT is refused before it touches anything.** Every check that can
refuse one - its credentials, the `acl_file`'s features, room for its Will,
every limit - is made before the session it names is looked at, so a refused
CONNECT ends, cancels, arms, replaces and counts nothing.

**Ownership moves when the CONNACK is written.** A CONNECT that passes
every check claims the session under its client id's lock - it takes the
session over, or with Clean Start marks it to end - and is answered; the
claim becomes ownership once its CONNACK has been written. A connection it
replaces is disconnected with `0x8E` (Session taken over). Nothing is sent
to the new connection before its CONNACK. A CONNACK that cannot be written
gives the claim back to the session the broker already held under the id,
live or away: that session is as it was before that CONNECT, and nothing
the failed connection did outlives it. Where the store refuses to put that
session's record back, the broker writes it again every second until it
lands, and before anything else reads or writes the record. Where the id
held none, there is nothing to give back, and the session that connection
began ends with it, whatever its expiry: its client never heard it had one.
A clean start's ending of the session it replaces runs once its CONNACK is
written: the ending and the new session's record are one write ("A session
begun new", below). A session that ends with its connection - no Session
Expiry Interval, or a 3.1.1 Clean Session - ends at the takeover, which
closes that connection, so the CONNECT that takes it over begins a new one
and is told Session Present 0, whatever its Clean Start says.

**A CONNECT that a newer one is waiting to replace is answered without
taking the session over.** CONNECTs for one client id take turns under its
lock. One that resumes, with a newer CONNECT for the id waiting behind it,
is answered once a newer one owns the session: its CONNACK, with Session
Present as the newer one found the session, then `0x8E` - what taking the
session over and being taken over in turn would have sent it - with
nothing between them, and nothing it sent after its CONNECT is read. Its
Will goes exactly as a taken-over connection's does ("Last Will"). If the
newer one does not take ownership, the CONNECTs it passed rejoin the queue
under the same rule. A Clean Start, and a CONNECT whose session would end
with its connection, keep their turn, since each changes what the next
CONNECT finds.

**A session ends in one place, and all of it ends there.** Whichever way it
ends, everything in the table goes together, and a Will waiting on it is
published first. A session ended by half is worse than one kept whole: its
client is told Session Present 0 and then served from what the half that
stayed remembers.

**A session begun new holds nothing an ended one left.** Ending the session
under its client id and keeping the new one's record are one write, and
where no session is held for the id, what the store keeps there is an ended
session's and goes in that write too. An ending the store refuses is not
built on: the CONNECT is refused with `0x83`, or, where its CONNACK has been
written, the connection is ended with `0x83`, and the client's next CONNECT
begins the session again. So too with what an ended session left in its
channels - its positions, and its exactly-once publishes not yet released -
where the store refuses to drop them: they are dropped again every second,
and a CONNECT for the client id drops them before it is answered, refused
`0x83` while the store still refuses. Built on, the session begun new would
be resumed at the ended one's position, and a publish reusing a packet
identifier the ended one held would be answered from its exchange -
acknowledged, and never kept. This rests on the broker holding every session
that is kept: what the store keeps under an id the broker holds no session
for is what an ended session left.

**Expiry and reconnection cannot interleave.** A session whose expiry has
passed is ended, whole, before a CONNECT for its client id is answered,
however recently the expiry sweep last ran, and the client is told Session
Present 0.

**A read that fails writes nothing in its place.** Where a stored session
cannot be read, the operation that needed it is refused - a CONNECT with
`0x83` (RFC 0002 "Every session's state") - or left undone: a session saved
without what could not be read is a client told Session Present 1 and then
sent nothing.

| What the session holds | A new connection resumes it | Its connection ends, the session kept | The session ends | A restart |
|---|---|---|---|---|
| Subscriptions, with their options and partition declarations | move to it | kept | end | restored |
| QoS 1 and 2 deliveries it is owed, under their packet identifiers | move to it, and any unacknowledged are sent again | kept, within `limits.session_queue_bytes` | end, but those a shared group handed it, which go back to the group or are dropped ("Broadcast") | restored |
| Exactly-once publishes it received and has not released | move to it | kept | end, counted as abandoned | restored with the session as far as the provider of their channel, or of the broadcast log, keeps them, or dropped and counted where the session does not come back |
| Its position in each `append` channel | kept | stored | ends | kept, or dropped at the start where its session expired |
| Its mark in each `latest` channel | kept | kept | ends | lost, so the whole of current state is served |
| Its membership of a shared group, which keeps the group's backlog | moves to it | kept | ends, and a backlog left with no member is dropped and counted | kept, or dropped at the start where no member is left |
| Its Will | a waiting one is cancelled; a replaced connection's is published if its delay is 0 and dropped otherwise; the CONNECT's own replaces it | waits out its delay, unless a `DISCONNECT` with `0x00` discarded it | published first - at a start, only if it carries a due moment ("Last Will") | kept, with its due moment |
| A queue job its connection holds | returned to the queue | returned | returned | returned (invariant 15) |

**The session ends** at its disconnect where it asked for no Session Expiry
Interval, or on 3.1.1 for Clean Session; when its expiry passes - the
interval its CONNECT was granted, or the one its DISCONNECT carried where it
carried one, either held to `limits.max_session_expiry` - at the sweep or at
a CONNECT that finds it passed; at a CONNECT with Clean Start;
at a CONNECT the broker answers Session Present 0 because retention passed
one of its positions ("Resuming below the retention floor"), where positions
retention has not passed are kept, since nothing their client had not read
was removed from them; and at a start, where it expired while the broker was
stopped, ended with a connection the stop cut, or holds a subscription the
broker now refuses.

**The connection ends and the session is kept** when the client goes away
with a session that outlives it, including a connection the broker closes -
at `limits.write_timeout`, or for an operator's hang-up (RFC 0002).

**A restart brings back what each provider promises**: a database across a
crash, a memory provider with a `snapshot_dir` across a graceful stop, and a
memory provider without one nothing at all (RFC 0004).

## From publish to record: identity and headers

A `PUBLISH` to a topic a channel claims becomes a record:

| Field | From |
|---|---|
| Message ID | User Property `saguin-id`, or generated by the broker |
| Topic | the full MQTT topic name, exactly as published |
| Payload | the packet payload, opaque bytes |
| Headers | the MQTT 5 User Properties |
| Timestamp | broker receipt time |
| Offset | assigned by the broker, monotonic, never reused |

**A record keeps the publish properties too**: Content Type, Response
Topic, Correlation Data, the Payload Format Indicator and the Message
Expiry Interval, alongside the six fields above. A consumer replaying a
channel is given what the publisher sent.

**And the retain flag the publisher set**, which is stored rather than
acted on: the channel is already the store the flag was asking for, so
there is nothing further to do with it. It is kept because a subscriber
that asked for Retain As Published is owed the publisher's own answer, and
it is not what an ordinary delivery carries - see "The retain flag on a
delivery" under `append`.

**Two exceptions, where a delivery carries Sagüin's own value instead.**

*A queue delivery keeps its own Response Topic and Correlation Data*, which
carry where a worker answers and the Delivery ID it answers with. MQTT
provides one of each per packet, so the publisher's are not delivered there.
They are still stored, and are delivered everywhere else the record goes,
including the dead-letter channel.

*A Message Expiry Interval is delivered decremented* by the time the
record has waited, as MQTT requires, and is not sent once it has run out -
`saguin-expires` is what still says when that was, and "The retain flag on
a delivery" has it. An expired record is still readable, on every channel
type: retention is the operator's (invariant 2), and nothing a publisher
sets removes a record from a channel. On `append` and `latest` that
protects another consumer's replay; on a queue there is no replay to
protect and the reason is the other one - taking work out of a queue is
the operator's decision, and `job_expires_after` is where they make it.

**That deviation is a channel's, and it ends at the channel boundary.** A
retained value on a broadcast topic has no consumer position and no replay
to tear a hole in, so there MQTT's own rule applies unchanged: the value is
discarded at its expiry and a later subscriber is not served it
(MQTT-3.3.2-5, "Retained messages" below). Two answers because the two
stores promise different things - one holds a history somebody is reading,
the other holds what is current.

**A 3.1.1 client sends none of this and is delivered none of it.** Its
`PUBLISH` frame carries no User Properties and no publish properties at
all, so a record it writes has no headers and no `saguin-id` of its own -
the broker generates the id, as it does for any publisher that supplies
none, and the record is complete without them. Reading is where the
shortage shows: a 3.1.1 subscriber is delivered the topic and the payload,
so the headers an MQTT 5 publisher set, the timestamp, the id and the
offset are all absent from the frame it receives.

**The record is not changed by who reads it.** An MQTT 5 consumer of the
same record is still given the lot, and the stored record is identical
either way - what is missing is on the wire to one client, not in the
channel. What a 3.1.1 consumer cannot do is tell two deliveries apart by
id or place one against another by offset, and that shortage is why the
`append` rules below give it a different starting point rather than the
same one.

**Message ID.** A producer may supply a UUIDv7 as the User Property
`saguin-id`. When it does not, the broker generates one. Either way every
record has one, so a consumer can always deduplicate on it, and it is
unchanged by redelivery, dead-lettering, and replay. It is not the
MQTT packet identifier, which is per-hop, 16 bits, and reused constantly.

The broker does **not** deduplicate on it in v0.1. A bounded
deduplication index is a real cost and an easy thing to add later; taking
it on before anything runs is not.

**Headers.** User Properties become record headers and are delivered back
as User Properties: **all of them, in the order the publisher wrote them,
including a name that appears more than once.** MQTT 5 permits a repeated
name - it is the standard way to carry a list - and requires a server to
forward the properties unaltered and in order, so a record keeps them as a
sequence rather than as a set.

A topic no channel claims carries them the same way: a broadcast publish
reaches its subscribers as the packet it arrived as.

The `saguin-` prefix is reserved: the broker strips any client-supplied
property under it other than `saguin-id`, so that a publisher cannot forge
dead-letter metadata a consumer will read as the broker's. That is the one
place a publisher's properties are altered.

#### Which channel a record came from: `saguin-channel`

**A delivery carries `saguin-channel` only where the consumer's filter
reaches more than one channel.**

`saguin-offset` is the record's position *in its channel*. A consumer whose
filter reaches two channels receives two independent offset sequences,
interleaved, with nothing saying which is which - so one tracking "I have
seen up to offset 3" is silently wrong about one of them. It cannot recover
the channel from the topic either, without knowing the filters that placing
a channel with a filter exists to spare it from knowing.

**Only where it answers something.** Measured on the wire, the property is
26 bytes for a channel called `events`: 17% of a 150-byte packet carrying a
20-byte reading, and 8% of a 200-byte JSON record. A fixed-size reading on a
metered radio is the target rather than an edge case, and about 100 bytes of
that 150 is already `saguin-id`, `saguin-offset` and `saguin-timestamp`. A
consumer whose filter reaches one channel already knows which channel and
pays nothing.

**A queue offer never carries it.** A worker names one queue - the only
subscription form a queue admits - so it is never ambiguous.

**A dead letter carries two names and both are right**: `saguin-channel` is
where the record is now, the dead-letter channel, and `saguin-dlq-channel`
is the queue it came from. They are easy to misread as a contradiction and
are not.

**A consumer's filter can become ambiguous later**, when an operator adds a
channel that it now also reaches. The property then appears where it never
did before. That is correct - the consumer was silently wrong before and is
not now - but it is a change nobody asked for, and it is written here rather
than left to be discovered.

**The property is not stored.** It is built at delivery from the channel the
record is already in, so a publisher sends nothing new and there is no disk
cost. A publisher that sends one has it stripped like every other reserved
property, because a client naming the channel a record is in would be
answering, in the broker's own vocabulary, the one question this exists to
answer.

**Bounds** are enforced before allocation, against the declared packet
length, not after the bytes are read. `max_message_size`,
`max_topic_length`, `max_topic_levels`, `max_header_count`, and
`max_header_bytes` are in RFC 0002.

### The reserved prefix on a client's own packets

The `saguin-` prefix is the broker's on the way in as well as on the way
out, and what happens to an unrecognised name under it depends on what the
packet is asking for.

**An instruction Sagüin does not understand is refused; data it does not
understand is stripped.** That is the whole rule, and it decides the packets
one at a time:

| Packet | An unread `saguin-` property | |
|---|---|---|
| `CONNECT` | **refused**, `0x83` | the client is configuring a session on terms Sagüin cannot honour |
| `SUBSCRIBE` | **refused**, `0x83` on every filter | the property names no filter, so there is no per-filter answer to give |
| `UNSUBSCRIBE` | **answered on the `UNSUBACK`, and the packet goes through** | refusing would leave the client subscribed to what it asked to stop receiving |
| `PUBLISH` | **stripped from every delivery** | the property rides with a record as data, and what makes that safe is that no delivery carries it |

**`UNSUBSCRIBE` is answered rather than refused**: the unsubscribe
happens, and the `UNSUBACK` carries `saguin-unread` naming the property -
to a client that set Request Problem Information; the broker log carries
it for the rest (MQTT-3.1.2-29). It does not echo the client's own name
back: a packet Sagüin writes carries only Sagüin's names under the
prefix.

**Stripping means every delivery, not every channel**: a `saguin-`
property a publisher wrote reaches no subscriber - not through a channel,
not through broadcast's live fan-out, and not through the value a
`broker.retained` store hands a later subscriber, where it would be a
forgery by construction.

**Refusing is what lets this grow, not what constrains it.** A property
silently ignored means a client that asked for a slice, or deletions, is
served something else and told it succeeded - and every release shipping
that silence makes closing it later a breaking change.

**A hint an older broker should tolerate goes outside the prefix.** An
unprefixed User Property stays opaque exactly as MQTT intends, and Sagüin
does not look at it. That is the compatibility valve, and it is deliberate.

**What Sagüin reads today** is two names on `SUBSCRIBE` - `saguin-filter`
and `saguin-deletions` - and none on `CONNECT`. The `CONNECT` rule is
stated with an empty set on purpose: nothing can be broken by it now, and a
version that shipped the silence could not adopt it later without breaking
somebody.

## When a record is too large for a subscriber

A client may declare a **Maximum Packet Size** in its CONNECT, and MQTT
forbids the server sending it anything above that. This is the client's
own bound on what it will receive, and it is a different thing from
`max_message_size`, which is the broker's bound on what a client may
publish.

**On a channel, Sagüin disconnects that subscriber, with reason code 0x95
(Packet too large). It does not skip the record.**

Skipping is the tempting answer and it is the wrong one. The subscriber
would go on to receive everything after the record, in order, and conclude
it had seen the lot - reporting success over a record it never got, with
the only notice of it in the broker's log, where the party that needs it
is not looking.

Disconnecting keeps the subscriber's position honest: on an `append`
channel the position is not advanced past the record, and on a `latest`
channel the value is recorded as undelivered, holding the resume below
it. A client that reconnects with the same bound is disconnected again,
which is loud - and loud is the point, because the failure being
prevented is a consumer starved in silence.

A `latest` update replaced by a newer one while it waited for room in the
subscriber's window is a different thing and is not this: that value has
been superseded by the time the subscriber catches up, and this one never
is.

The bound belongs to the client, so the client is where it is changed. An
application whose records are too large for some of its subscribers wants
two channels rather than one. There is no broker setting for it, because
the setting one would offer is "skip silently".

Broadcast is the exception, and deliberately so: a message too large for
a subscriber is dropped for that subscriber, which stays connected -
there is no position and no claim to have delivered everything, so there
is no report of success to be made false.

On a `queue` the check happens before the job is handed over. The worker
chosen for a job it could not receive is disconnected and the record goes
back with its attempt unspent - it was never delivered - because a worker
that cannot hold a queue's records cannot do the work, and routing around
it quietly would leave it taking only the small jobs with nothing saying
so.

**A shared group - over a channel or over broadcast, and whether or not it
holds a backlog - hands a record only to a member it fits**, measured as a
queue's offer is. A member it does not fit is passed over: it is not
disconnected, since the group needs only one member to take the record, and
nothing is counted lost. Where no connected member it fits can take it, the
group does what it does when no member is connected: a group with a member
whose session outlives its connection holds it until a member it fits
arrives or the group's bound gives it up (`backlog_full`), and any other
drops it, counted as `no_shared_member` (RFC 0002 "Shared subscriptions").

## Broadcast

Delivered to sessions subscribed at the moment of publish. Not stored, not
replayed, not recoverable. A subscriber that was disconnected has missed
it permanently, and that is the whole contract.

**And one case where a broadcast message is kept after all: a shared group
whose members are all away.** A delivery that matched a `$share/` group and
that no member could take stays in the broadcast log below, behind the
group's cursor, and is served to the first member back, in the order it was
published. A channel's record a group over an `append` or `latest` channel
is owed is kept the same way, as a copy in that log for the groups alone,
and from there the group is served exactly as over a broadcast topic. Two
things make one held rather than dropped, and each narrows it:

- **QoS 1 or 2 only.** A QoS 0 delivery is owed to nobody by anything, and
  holding one would invent a promise MQTT does not make.
- **A member whose session outlives its connection.** A hold needs a session
  that can come back to collect it, exactly as a Will's delay needs a session
  to wait inside. A group whose members are all clean-start is owed nothing,
  and its deliveries are dropped and counted as `no_shared_member`.

**A delivery arriving while its group holds something joins the back of that
backlog**, even where a member could take it now, because [MQTT-4.6.0-6]
requires a server to send publishes to a consumer in the order it received
them from any given client.

**What a backlog survives is the promise of the provider
`broker.session.storage` names**, which keeps the log and the group's
cursor, exactly as a session's is: a database across a crash, a memory
provider with a `snapshot_dir` across a graceful stop, a memory provider
without one nothing at all. It ends when its last member session ends,
whichever way that session ends ("Sessions"), and when it has been held
longer than `broker.share.expires_after` where an operator set one. A
drain interrupted between sending a delivery and recording that it went
serves that delivery again, which is at-least-once rather than a
duplicate.

**One flag changes that too, and only for the client that sets it.** A publish
carrying RETAIN also has its value kept as the current value of that topic,
in the retained store every broker has, and handed to whoever subscribes
next. Nothing else about broadcast changes: a message without
the flag is stored nowhere, and a retained one is still delivered live to
everyone subscribed at the time. "Retained messages" below has the rules.

Each subscriber has a bounded outbound buffer, no more than half its
session's `limits.session_queue_bytes`, and **two different bounds reach a
subscriber that stops reading**. The queue is one: a delivery the buffer
cannot hold is **dropped** at QoS 0 and counted, where at QoS 1 or 2 it
waits in its session, within the bound below, until what is queued ahead
of it has been written. `limits.write_timeout` is the other, and it is not
about QoS at all - a socket that takes no write for that long is
**disconnected**, whoever is on it.

**So a deaf QoS 0 broadcast subscriber gets both, and in that order.**
Deliveries are shed for as long as the socket still accepts writes, and
when it stops accepting them the deadline hangs the client up: measured at
653,100 dropped and then `disconnected a consumer that stopped reading`
five seconds in. Broadcast is the only place Sagüin drops at all, because
it is the only place with no position a reconnect would replay from - but
dropping is what happens *instead of buffering*, never instead of the
deadline. A run shorter than the deadline sees only the drops, and reads
as a subscriber that stays connected.

Either way the subscriber is not buffered further and not allowed to slow
the channel down for anyone else, and the publish is never held back by it. A
QoS 0 delivery is never persisted on a slow subscriber's account, and a
QoS 1 or 2 one is kept in its session only within the bound below: a
history beyond that is what `append` is for.

**A session that outlives its connection is owed QoS 1 and 2 broadcast
through one log.** A message that matches such a session's subscriptions
is written once to a log in the provider `broker.session.storage` names,
and each session holds a cursor into it: what the session is owed is
everything after its cursor that its filters matched when it was
published, so a subscription made later does not reach back to a message
already in the log. It reads the log at its own pace, within its client's
Receive Maximum, so a slow session lags its own cursor and nobody else's.
QoS 0, and a session that ends with its connection, are served from memory
alone: MQTT promises neither anything once the connection is gone, so
there is nothing to keep.

**A channel's record a shared group over it is owed is kept in the same
log, for the groups alone.** A group over an `append` or `latest` channel
keeps no position on it, so its backlog and what it hands its members are
the log's: at QoS 1 or 2, where a group with a member whose session
outlives its connection matches it, the record is written to the log once
as a copy owed to those groups, before the publisher is answered, and each
group is served it as a broadcast. No session's own subscription is
matched against the copy: a durable subscriber to the channel has the
record from the channel. A member that also has its own subscription to
the record's topic is sent it twice, once from the channel and once from
its group, as MQTT has a client whose shared and non-shared subscriptions
overlap.

**The publisher is answered after that one write.** The `PUBACK` - or the
`PUBREC` at QoS 2 - follows the message's write to the log and nothing else:
not a session's write, not a subscriber's read, and not what any session
gives up. A publisher told its message was accepted is told something a
crash does not undo, as far as that provider promises, and a session that
comes back after a restart is sent it.

**What a session keeps of it is a cursor and a window.** The cursor is the
lowest offset it has not acknowledged; the window is what is on the wire,
each message under the packet identifier MQTT requires a re-send to a
resumed session to carry (MQTT-4.4.0-1) and, at QoS 2, how far its
exchange has got. Both are kept with the session, so a session resumed
after a link cut or a restart is sent its unacknowledged messages under
their original identifiers. An acknowledgement is taken for writing by the
session's drain at most `broker.session.ack_commit_interval` after it
arrives, however much the session is still owed, off the connection's read
loop, and before a clean `DISCONNECT` closes the connection (invariant
18); it is stored when that write reaches the provider, which on a
`sqlite` provider waits behind the writes already queued for its one
connection (RFC 0004 "Group commit"). Where the store refuses that write
the connection closes all the same, and the drain writes it again every
second until it lands, the session away. The drain sends nothing past its
window until what was acknowledged has been written, so an unclean stop
can send again **at most the session's Receive Maximum messages**: what it
acknowledged since its last completed write - about the last
`ack_commit_interval`, 200 ms by default, when storage keeps up, and
longer under a storage backlog such as a reconnect storm or a slow disk.
At QoS 1 that is the message, under its identifier, and at QoS 2 its
`PUBREL`, never a second message.

**At QoS 2 the `PUBREL` goes out once the `PUBREC` is stored.** The client
answers a `PUBREL` with `PUBCOMP` and forgets the identifier, so an
exchange still stored at its `PUBLISH` would have a restart deliver the
message again as a new one. Where the store refuses that write, no
`PUBREL` is sent and the connection ends - `DISCONNECT` with reason
`0x80`, or a plain close below MQTT 5 - and the exchange resumes when the
session does: its `PUBLISH` is sent again under its identifier, and the
client's `PUBREC` to it asks for the write again.

**A delivery carries what the session's subscriptions ask of it when it is
sent**: the lower of the message's QoS and the highest any matching
subscription grants, every Subscription Identifier they carry, and RETAIN
where the publish set it and any of them asks for Retain As Published - so
overlapping subscriptions that disagree about it are answered the same way
whichever order they are held in. A message no subscription reaches is not
sent, and neither is one the `acl_file` does not allow the client at that
moment: the session lets it go as it lets go of one acknowledged, since
broadcast has no position to wait at.

**Two bounds, and neither refuses the publisher.** A session may fall
behind by `limits.session_queue_bytes` of what it is owed, and past that
its cursor moves on past the oldest it is owed. The log as a whole holds
what its provider's `max_bytes` allows, and when it is full its oldest
messages go, except one a session has on the wire. A message leaves the
log once every session and shared group that wants it has had it. What a
session loses to either bound is counted against that session (RFC 0002
has both).

**A message's expiry is read, not enforced by deleting it.** One whose
Message Expiry Interval has run out when a session reaches it is skipped and
counted as expired, and stays in the log for as long as another session
still has to pass it (invariant 2).

**A shared group reads the same log through one cursor of its own**, kept
with the sessions, and hands each message after it to one member. A
delivery on the wire to a member is in that member's window, as any
session's is, and what lies after the group's cursor while its members are
all away is its backlog, above. **The cursor is made with the record of the
group's first member whose session outlives its connection**, in one write:
a provider that cannot keep both refuses that member's `SUBSCRIBE` - `0x97`
for room, `0x83` otherwise - and keeps neither (RFC 0002). A member joining
as the last one leaves joins a group that keeps its cursor or makes it
again, never one whose cursor is ending; and a resumed session holding a
group that has no cursor is given one before its `CONNACK`, or refused.

**A member's session that ends with a delivery its group handed it
unfinished** splits it as MQTT 5 section 4.8.2 does. At QoS 1 it goes back
to the group, which serves it to another member ahead of everything after
its cursor - the Server should - so a member that processed it without
acknowledging it has it processed twice, which is at-least-once. At QoS 2
it goes to no other member (MQTT-4.8.2-5): one the member had not answered
with a PUBREC is dropped and counted as `member_ended`, and one it had
answered has reached it. A delivery counts as sent once it is queued for
the member's connection, whether or not the socket has written it, so a QoS
2 message still in that queue when the session ends is dropped, not
returned; one that could not be queued at all goes back to the group at any
QoS. Where the provider has no room, even in its reserve, to keep what is
returned, it is dropped and counted as `storage_full`. Only an ending does
this: a takeover carries a member's deliveries to its new connection, and a
connection that closes while its session is kept leaves them in the
session, to be sent again when it resumes. A member whose session ends with
its connection is handed its deliveries from memory, and they are split by
the same rule when it ends - a restart ending it too.

**A message's identity is stated rather than inferred.** A publish is given
one Message ID where it is written, and every delivery of it carries that
ID: two publishes with the same bytes are two messages (invariant 8).

## No Local

MQTT 5's No Local option says a record is not forwarded to a connection
whose client id equals the publishing connection's [MQTT-3.8.3-3]. Sagüin
honours it on every delivery it makes, and how it decides differs by where
the record is kept:

| Where | How the publisher is known |
|---|---|
| Broadcast | the packet being fanned out |
| `append`, `latest` | the client id stored on the record |
| Retained values | the client id stored on the retained value |
| `queue` | the subscription is refused - below |

A broadcast message is forwarded as it passes, so the publishing connection
is still in hand. A channel is not: a subscriber is served from a position,
and the record it is handed may have been written minutes or a restart ago.
So a record keeps the client id that published it, and the comparison is
made against that. **That client id never reaches a subscriber** - it is not
a delivery property, a header, or anything else on the wire. Who published a
record is not something Sagüin tells the broker's clients; it exists for
this one comparison.

**The flag is read off the subscription that matched, not off the
connection.** A client may hold one subscription that asked for No Local and
another that did not, and a record reaching both is delivered: the
subscription that asked for nothing is owed it. On a channel that matters
more than it looks, because a consumer holds one position for the channel -
a record withheld from it is stepped over for every subscription at once.

**A consumer's position moves past a record withheld from it**, exactly as
it does past a record outside a declared partition slice: a channel consumer
holds one position, so a record it is not sent is stepped over rather than
waited on. A consumer that later resubscribes without the flag does not go
back for them.

**A Will is published by the client that armed it.** Its record, its
retained value and the broadcast packet carry that client's id, so a No
Local subscription of that client's is not sent its own Will - to a session
it resumes, or to a connection that took its client id over.

**A record written before the client id was stored has no publisher, and is
delivered.** Withholding it instead would mean a subscriber is never shown a
record and nothing anywhere says so; delivering it costs one message the
subscriber can recognise as its own.

**No Local on a shared subscription is a Protocol Error** [MQTT-3.8.3-4].
The connection is closed with `0x82`, as section 4.13 says a Protocol Error
is answered.

## `append`

### Live subscription

A subscriber receives records published while it is subscribed, in offset
order, at the QoS it was granted.

### The retain flag on a delivery

**An `append` record is never marked retained**, even on a replay of
something published last week: on this channel a consumer's *position* is
what says where it is, and a record is one of many rather than the current
value of a topic.

**Unless the subscriber asked for Retain As Published**, and then it is
sent the flag its publisher set (MQTT-3.3.1-13), which is what that option
asks for and what a broadcast topic already answers. The record is
unchanged either way - the flag is information about how it was published,
not an instruction - and a client that never sets the option sees exactly
what it saw before. A client holding two filters that both reach a record,
which is the ordinary shape of a replay, is served the publisher's flag if
either of them asked.

### Where a subscription starts

**A subscriber with no stored position is served the channel from its
retention floor** - everything it still holds, in order, before anything
published after the subscriber arrived. That is the replay this channel
type exists for: a consumer that did not exist yesterday reads yesterday's
events. It applies whether the subscriber discarded a position or never had
one.

**`start: tail` on the channel says the opposite**, and then a subscriber
with no stored position is served only what arrives next. A channel of
commands wants that: a fleet restarted together and handed a retained
channel would act on yesterday's instructions a second time, which is a
hazard for any device and a serious one for a device with no way to see an
offset and tell a replay from live traffic.

**The channel answers this and the client's protocol does not.** One
answer for every reader, whatever it speaks: a rule keyed on the
protocol would make one application behave two ways depending on which
client library it linked, and the operator knows which kind of channel
this is when the broker cannot.

**One SUBSCRIBE naming several filters is one arrival.** Every filter in
the packet is recorded before any of them is replayed, so a consumer
reaching one channel through two filters is served every record either of
them matches. Without that the answer would depend on which filter the
broker happened to handle first: a replay steps over records that match
none of the consumer's filters - it must, or a record for somebody else is
re-read for ever - so a filter replayed while it was the only one recorded
would step over the records only its neighbour matched, and the consumer
would receive everything after them and have no way to know.

**It is the ordinary shape rather than a corner.** A channel whose filter
carries a `{a,b}` level is read with one filter per alternative, since a
`+` there would also reach topics the channel does not claim.

**A filter added in a *later* SUBSCRIBE is a different thing and does not
reach back.** By then the consumer has a position, and a subscription made
now does not undo it - which is MQTT's own rule for a subscription and this
document's for a position. A consumer that wants a channel's history under
a new filter seeks.

**Retain Handling 2 asks for nothing older than now, and this channel type
honours it.** A subscriber with no stored position is served only what
arrives next, whatever the channel's `start` says; one with a stored
position has that position committed at the channel's head and the backlog
dropped, which is what a seek to `-1` already does. Retain Handling `0` and
`1` both replay as described above.

*This is Sagüin extending the option rather than applying it*: MQTT
defines Retain Handling only for retained messages, and what carries
across is the client's meaning - *send me what is happening, not what
happened* - which otherwise had no spelling short of a seek to `-1` on a
durable session.

`1` is treated as `0` rather than as "replay only if the subscription is
new". Its rule is about not re-sending one topic's current value on a
repeated subscribe, and a replay has no equivalent to not re-send; reading
it as `0` is the answer that invents no behaviour.

*The commit is the part to know about*: a position that moved only for
the connection would hand the whole backlog back on the next connect. A
session that does not outlive its connection stores nothing, and its
subscription is still served live only.

*A 3.1.1 subscriber is unaffected.* Its SUBSCRIBE carries no subscription
options beyond the QoS, so there is nothing for it to ask, and `start` goes
on deciding for it exactly as before.

**Historical replay is otherwise a deliberate seek**, and a seek to `0` is
everything the channel still holds. On a channel writing `start: tail` that
is how a reader asks for history, and it needs a durable session - so a
consumer that wants both the tail by default and history on demand connects
with a session that outlives its connection.

**Below the floor is refused whatever the setting.** A durable consumer
whose stored position has fallen below the channel's retention floor is
told its session was not found - `CONNACK` with Session Present = 0, which
3.1.1 carries as surely as MQTT 5 - and starts again wherever the channel
says a reader with no position starts. Invariant 1 forbids serving a gap in
silence, and it is not served one.

### Durable consumers

A subscriber connecting with **Clean Start = 0** and a non-zero **Session
Expiry Interval** is a durable consumer. Its position is stored per
`client_id` per channel, and on reconnect it resumes there - receiving
everything published while it was away, in order.

The position is a **cursor into the channel's log**, not a buffered copy
of the records. Storing it costs one offset per consumer regardless of how
far behind it is, which is why a durable session cannot overflow and why a
consumer offline for a week is not a problem the broker has to size for.

A consumer's position never causes a record to be removed, and never
affects another consumer.

**A 3.1.1 durable consumer is `cleanSession = 0` and has nothing else to
choose.** 3.1.1 has no Session Expiry Interval, so a client cannot name
how long its position should outlive its connection: it is given
`limits.max_session_expiry`, the same ceiling that caps what an MQTT 5
client may ask for, and there is no shorter value for it to name. With
`cleanSession = 1` it stores nothing and begins wherever the channel says
a reader with no position begins, every time.

**Its position advances on its `PUBACK`s exactly as an MQTT 5
consumer's does** - 3.1.1's own promise, kept with a cursor instead of a
per-session buffer. **And nothing is dressed as retained to fake a
replay**: a rebooting device re-sent the newest record under the RETAIN
flag would act on a stale event a second time.

**A position advances on the acknowledgement, and QoS 0 has none.** The
rest of this section is the rule at QoS 1, which is what a durable
consumer should ask for. A subscription granted QoS 2 follows the same
rule with that grade's own acknowledgement: the position advances when the
exchange completes, at the `PUBCOMP`, and a record whose exchange is
unfinished is still ahead of it. A subscription granted QoS 0 is served
the same records with no `PUBACK` behind them, so its position advances as
each record is written to the socket - and the two risks are opposite
ones. A position that lags costs a replay; a position that advances on the
write costs a record, because one that did not survive the link is already
*behind* it and the consumer resumes after it rather than being re-sent
it. A durable `append` consumer at QoS 0 is therefore at-most-once from
the socket onward, and the at-least-once promise rests on QoS 1. A
`latest` channel has the same shortage for the same reason: a QoS 0
subscriber has no acknowledgement to serve a difference from, so it is
sent the whole of current state every time.

**A stored position may lag, and never leads.** It is taken for writing
every `broker.session.ack_commit_interval`, 200 ms by default, rather than
as each record goes out, before a shutdown, and when the consumer
disconnects, and it is stored when that write reaches the provider - on a
`sqlite` provider behind the writes already queued for its one connection
(RFC 0004 "Group commit"). After a clean `DISCONNECT` it is stored before
the connection is closed (invariant 18), so a consumer that has seen its
connection close has had everything it acknowledged before the
`DISCONNECT` stored - unless the store refused it, when the connection
closes all the same and the position is written again with every flush
until it lands. A position a subscription with Retain Handling 2 moves to
the head is the same: written as it is made, and again until it lands. A
dropped link sends no `DISCONNECT`, and its position is written once the
broker notices the connection has gone. An unclean stop can therefore lose
the last moment of a consumer's progress: what it acknowledged since its
last completed write - about the last `ack_commit_interval` when storage
keeps up, longer under a storage backlog such as a reconnect storm or a slow
disk, and on a `memory` provider, which has no queue, the interval. Unlike a
broadcast session's, it is not bounded by a count: the consumer is sent more
while the write waits. What it costs is a replay: the consumer resumes
earlier and receives records it already had, which is at-least-once behaving
as promised.

The opposite cannot happen. A position is the lowest offset the consumer
has not acknowledged, so it never runs ahead of what the consumer received;
one that did would step over records the consumer never saw, and it would
then process everything after them in order and conclude it had seen the
lot - the one failure a position must never produce.

### Resuming below the retention floor

Retention may have removed records a stored position still points at. The
broker must never serve the oldest surviving record as though nothing were
missing - the consumer would process the remainder in order and
conclude it had seen everything.

MQTT has no way to say "your position expired" mid-session, so Sagüin says
it at the only moment MQTT provides: **`CONNACK` with Session Present =
0.** The stored session is discarded, the client is told its session was
not found, and it starts fresh at the channel's current retention floor.

Every client library already handles that signal as "I must resubscribe
and I may have missed things", so Sagüin needs no mechanism of its own.

The same signal is used when a session expires normally. Sagüin does not
distinguish the two on the wire, because the client's correct response is
identical.

The check happens at CONNECT, before the session is resumed: a client
whose stored position is below any of its channels' floors has that
position dropped and its session discarded, so the CONNACK reports Session
Present = 0 and it starts again at the floor.

### How a stored position is dropped

**With its session, and otherwise only when retention passes it.** A stored
position is session state, so it goes wherever the session ends and nowhere
else - "Sessions" has every way one does - and retention passing it is the
case reported rather than silently corrected, above. `limits.max_session_expiry`
is what makes every ending reach every row: a client may ask for a session
lasting 136 years, and without the cap no expiry would ever find that one
due.

Only an `append` consumer has one stored. A `latest` consumer's mark is held
in memory, so a restart takes it and the consumer is served the whole of
current state when it comes back - which is what a `latest` channel is for,
at the cost of a message per topic on the first connection after a restart.

**A position that outlives its session is a correctness failure**: a
replacement device taking over a departed id would be resumed at the old
consumer's offset - told it had missed nothing while missing everything
(invariant 1) - which is why the drop happens when the session expires
rather than at the next start.

**One case cannot be reported, and it is not the offline one.** A consumer
that is *connected and reading* when the floor passes it has no CONNECT to
carry the signal, and MQTT provides no other. It restarts at the floor and
the broker logs how many records went - the report going to the operator
rather than to the consumer, which is invariant 1's failure in the one
place Sagüin cannot avoid it without extending MQTT. It needs a channel
whose retention is shorter than the time a consumer takes to drain it, so
it is the slow-consumer case rather than the absent one.

### Moving a consumer's position: seek

A consumer sometimes needs to be somewhere other than where it stopped: to
replay a day after a bug, or to skip a backlog it no longer wants. Both are
ordinary, both are the consumer's own decision, and neither should require
an operator to touch storage. So a consumer may move its own position, by
publishing to a reserved topic:

```
$saguin/consumer/<channel>/seek
```

The payload names a position, in one of five forms:

| Payload | The position becomes | The consumer then receives |
|---|---|---|
| `0` | the channel's retention floor | everything still held |
| `-1` | the channel's next offset | only records published after the seek |
| a positive offset | that offset | from there onwards |
| a duration - `-12h`, `90m`, `7d` | the earliest offset at or after that moment | everything from then on |
| an RFC 3339 moment | the same | the same |

**A bare integer goes on meaning an offset**, and that is why a time is
told apart by shape rather than by a flag. `1763000000` is a plausible
offset and a plausible Unix time; a consumer that seeks to the wrong one
reads from there in order and reports success, which is a silent skip.

**A duration means *ago* whether or not it carries a minus sign.** `-12h`
and `12h` are one request - the last twelve hours - because a moment in the
future has no records and would mean the same as `-1`. Days are admitted
because `-7d` is what an operator types about a channel retaining for a
week.

**An absolute moment is RFC 3339 and nothing else.** `2026-08-14` alone is
a date in some places and nothing in others, and a broker guessing between
them would move a consumer somewhere it did not ask for. It is refused as
malformed, like any payload that is none of these shapes.

**The answer is always *at or after*, never an exact match.** The timestamp
is the broker's own receipt clock: many records share a millisecond, and
nothing guarantees the moment asked for is one any record carries.

**And it is the earliest *offset* carrying a time at or after the one
asked for**, which is not always the record closest to it. The clock is the
wall clock, so a step backwards from NTP can leave a short run of records
whose times run the other way from their offsets, and the closest record
would place a consumer past records inside the window it asked for. The
earliest offset may hand it a few records older than it wanted, which is
the safe direction: extra records, never missing ones.

**A time older than the oldest surviving record has two causes, and only
one is safe to answer.** If the floor is still 1 nothing was ever removed:
the channel does not go back that far, serving from the start is a
*complete* answer, and refusing it would be absurd. If the floor has moved,
retention took records the consumer asked for, and that is `below-floor`
carrying `saguin-floor` - the same refusal an offset seek gives, for the
same reason.

**Including when retention has emptied the channel.** There is no surviving
record to compare a moment with, and everything that existed is gone, so a
past moment is refused there too. A future one is not, because nothing was
removed after it.

**`0` and `-1` exist so that a consumer never has to know the floor or
the latest offset** - asking first would be a round trip and a race - and
they are safe as words because offsets start at 1 and are never reused.
Every other negative number is refused rather than given a meaning.

The payload lands on the definition of a position already given above,
rather than introducing a second one. A position is the lowest offset the
consumer has not acknowledged, so `-1` setting it to the next offset says
"nothing is outstanding", which is what starting from the end means.

**A seek takes effect where the consumer is.** It needs no reconnect, no
unsubscribe, and nothing else the consumer has to remember to do: the
records from the new position start arriving on the connection that asked
for them, and the reply naming the stored offset comes first.

What that costs is spelled out because it is the whole difficulty. Records
are already in flight to that consumer at QoS 1 when the seek arrives, and
a `PUBACK` for one of them landing afterwards would advance the position
past the place the consumer just asked to be - silently, and in the
direction that skips records, which is the one failure a position must
never produce. So the seek discards what is outstanding on that channel and
ends that consumer's *era* on it: every delivery is stamped with the era it
was sent in, and an acknowledgement from an era a seek has closed is
recognised and ignored. The record it was for is simply sent again if the
new position covers it.

**Records already written to the socket stay written.** The broker stops
counting them; it cannot unsend them. A consumer may therefore see a record
from before the seek arrive just after it, which is at-least-once behaving
as promised, and it is what the consumer asked for by moving.

**It reaches the seeking consumer and nothing else.** A position is held
per client id and per channel, so no other subscriber of the same channel
has its stream disturbed, and neither do the seeking consumer's other
channels.

A seek does not require a reconnect; one that reconnects afterwards is
unaffected. A consumer that has **not** started reading the channel has no
stream to move: its first read starts at the new position, in the same
session.

A consumer that seeks and then keeps reading does not have its seek quietly
undone by the position its own reading would otherwise store. Its position
follows the replay from the new place rather than the offset its stream had
reached before it.

**It applies to `append` channels, which includes every queue's
dead-letter channel** - that is an append channel in every respect, and
replaying one is a normal thing to want after fixing whatever dead-lettered
the work. A seek on a `queue` is refused: a queue is unresolved work with
exactly one consumer per record, and rewinding it would hand out work that
is already resolved. A seek on a `latest` channel is refused because there
is no *history* to move through - it holds current values, not a log. Its
consumers do have a position, and it is a different kind of thing: a mark of
what has already been delivered, used to work out what a resumed session
missed, and not a place in a sequence that could be rewound to.

**A consumer may move only its own position**, which is the one stored
under its own client id. Nothing in this lets one consumer move another's.

**A seek outside the channel is refused at both ends**, and the reply says
which bound was met. Below the floor the alternative would be to clamp,
which manufactures exactly the state the floor exists to report: a consumer
that asked for offset 500, was given 900, and has no way to know that 400
records it asked for are gone. Past the end is the same failure pointing
the other way - a position beyond the next offset skips every record
published between the seek and the reconnect, and the consumer would never
learn there had been any. `-1` is how a consumer says "the end" without
naming an offset, and it is exact by construction.

**A seek requires a durable session** - a Session Expiry Interval above
zero, the thing that gives a consumer a stored position at all. A client
whose session ends with its connection has nothing to move, and its seek
is refused rather than silently doing nothing. On 3.1.1 that is
`cleanSession = 0`; put beside "Where a subscription starts", **on a
channel writing `start: tail` a client with no durable session can never
be sent anything published before it subscribed** - a property of the
channel and the session, not the protocol. Clean Start does not come into
it: a consumer opening Clean Start = 1 *with* an expiry has a durable
session from that moment and may seek on it.

The reply goes to the Response Topic the client set on its seek, echoing
the Correlation Data it sent, exactly as a queue worker's acknowledgement
does.

**A client that set no Response Topic is answered on
`$saguin/consumer/<channel>/seek/reply`** instead: the same payload, the
same properties, the same echoed correlation data. Response Topic is an
MQTT 5 property, so a 3.1.1 client could never ask for the reply to go
anywhere - this is where its answer arrives, and an MQTT 5 client that
simply set none reads it here too. A Response Topic still wins where one
is set, so nothing that sets one answers anywhere new. It is one of the two
topics under `$saguin/` a client may subscribe to - the other is a queue's
`$saguin/queue/<channel>` (RFC 0002) - and the subscription is only how its
own library routes the packet: the answer is written to the seeking session
rather than published, so it carries that client's replies and never
another's, however many subscribe.

**A refusal also rides the acknowledgement**, as `0x83` naming the reason,
so that a client with no Response Topic can still tell a seek that moved it
from one the broker threw away. Without that the two are the same packet -
`PUBACK` with no reason - and a consumer that believes it moved and did not
reads on from somewhere other than where it thinks it is.

**It goes to the seeking session and to nobody else**, which is worth saying
because the reply looks like an ordinary publish and is not: a third party
subscribed to the same Response Topic sees nothing. The reply is an answer to
one client rather than an event about the channel, and a seek is a private
correction of one consumer's position - broadcasting it would tell every
other subscriber where somebody else is reading from.

The consequence to know before trying it: **a client that neither sets a
Response Topic nor subscribes to the reply topic reads nothing but the
acknowledgement**, and a seek that was accepted still applies. The
commands below do neither, so they produce a seek that worked and
nothing to read it by. A *refused* one `mosquitto_pub` does report -
`Warning: Publish 1 failed: Implementation specific error` and the reason
beneath it - because the refusal rides the acknowledgement. **It still
exits 0** (mosquitto_pub 2.1.2), so a script that checks the status and
not the output reads a refused seek as a successful one.

Setting a Response Topic is what shows the answer itself, and
`mosquitto_pub` can: `-D publish response-topic seek-reply` sets one, and
with `-d` the reply is printed as it arrives, which is before the `PUBACK`
the tool exits on - the same trick the point read uses (*Reading one
value* below). A client that stays connected sees the same thing without
the debug output - a dozen lines of Paho against the demo broker, and the
last line prints the offset it landed on:

```python
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

# A durable session: a client id, clean start off, and an expiry - the
# same three things `-c -x 300 -i my-consumer` say.
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "my-consumer",
                     protocol=mqtt.MQTTv5)
client.on_message = lambda c, u, m: print(m.topic, m.payload.decode())

connect = Properties(PacketTypes.CONNECT)
connect.SessionExpiryInterval = 300
client.connect("127.0.0.1", 1883, clean_start=False, properties=connect)

# The seek, with a Response Topic - the reason the reply arrives. No
# subscription: the answer goes to the session that asked.
publish = Properties(PacketTypes.PUBLISH)
publish.ResponseTopic = "seek-reply"
client.publish("$saguin/consumer/events/seek", "-12h", qos=1,
               properties=publish)

client.loop_start()
time.sleep(2)          # the reply prints: seek-reply 1
client.loop_stop()
```

It can also produce a seek that worked and nothing left to replay:
`mosquitto_pub`'s library acknowledges whatever reaches it before the
exit, so a seek sent that way is handed the rewound records itself and
the consumer that reconnects next receives nothing. A consumer seeks from
the connection that will do the reading.

| Reply payload | Meaning |
|---|---|
| a decimal offset | stored; the records start arriving from there |
| `below-floor` | refused; User Property `saguin-floor` carries the floor |
| `beyond-end` | refused; User Property `saguin-next` carries the next offset |
| `no-session` | refused; the client has no durable session to move, or another connection under its client id has taken the session over |
| `malformed` | refused; the payload was not one of the shapes above |
| `storage` | the position could not be written; the broker logs why |

**A channel with no history to move through is refused in the `PUBACK`, not
here.** Seeking a queue, a latest channel, or a name no channel has answers
**0x90 Topic Name invalid** with a Reason String saying which of the three
it was, because which channel a topic names is a property of the topic and
RFC 0002 refuses those before this handler runs. It is also the only
answer that always arrives: everything in the table above reaches only a
client that set a Response Topic or subscribed to the reply topic, and
the commands below did neither. A misdirected seek is the one failure a
client is most likely to make on its first attempt, so it is the one
that should not depend on having got the optional half right.

It is an ordinary `PUBLISH`, so a stock client can do it:

```sh
# replay this channel from the beginning
mosquitto_pub -V 5 -t '$saguin/consumer/events/seek' -m 0 \
  -q 1 -c -x 300 -i my-consumer

# or skip whatever has piled up and start from now
mosquitto_pub -V 5 -t '$saguin/consumer/events/seek' -m -1 \
  -q 1 -c -x 300 -i my-consumer

# or name a moment: the last twelve hours, or an exact one
mosquitto_pub -V 5 -t '$saguin/consumer/events/seek' -m -12h \
  -q 1 -c -x 300 -i my-consumer
mosquitto_pub -V 5 -t '$saguin/consumer/events/seek' -m 2026-08-14T09:00:00Z \
  -q 1 -c -x 300 -i my-consumer
```

**Each of those flags has a job.** `-c` keeps the session the client id
finds, `-i` names it across connections, and `-x` sets how long it lasts
after the connection closes; a session that expires on disconnect has
nothing to move, so a seek with no expiry is refused `no-session`. `-V 5` is
written so the command asks the same thing of every tool. mosquitto_pub
2.0.22 and later give `-c` an expiry of its own on MQTT 5, so there `-x`
only shortens it: `limits.max_session_expiry` caps what either asks for.

**What a seek never does.** It removes no records, so a consumer moving
forwards does not delete what it skipped and another consumer reading the
same channel is unaffected. It never moves the retention floor. And it
never moves another consumer, so one client rewinding to debug cannot
replay a day of work into everybody else.

## `latest`

A publish replaces the current value for its topic. A zero-length payload
deletes it, following MQTT's own retained-message convention: the delete is
delivered to whoever is subscribed, because that is how they learn the key
is gone, and **a later subscriber is told nothing about that topic** -
not an empty value, nothing at all, exactly as for a topic never set.

**The deletion is kept, and nothing that reads this channel can tell.** It
is stored as a value with no payload and an offset of its own, and it is
left out of the state a subscriber is sent, so the sentence above holds
unchanged. A point read finds it and answers with an empty payload, which is
already the answer for a topic never set - the two are the same
answer here.

It is held for the reader that was away. A subscriber is handed current
state when it subscribes, and a set of current values cannot say that a
topic is gone - so a topic deleted while a reader was disconnected would
live on in whatever that reader holds, for ever. Stored, the deletion is
part of what a subscriber asking for deletions is told.

It also makes a deletion an ordinary thing here: it has a position, so a
consumer can place it against the value it replaced and two deletions of one
topic order against each other. **Deleting a topic that has no value stores
nothing**: there is nothing to say is gone, and storing one would let a
client repeating itself fill a channel with rows for topics that never
existed.

**A subscription asks to be sent them**, with the User Property
`saguin-deletions` on its `SUBSCRIBE`. Presence is the switch. Without it
a subscriber is sent current state and nothing else; with it, the
deletions are included in that state, each carrying the empty payload and
the offset it was stored at.

The property is on the packet rather than on each filter - where MQTT 5
puts User Properties - so it applies to every filter named, and a client
wanting both shapes sends two `SUBSCRIBE`s. A resumed session does not
carry it, so a copy asks again on every connection, which a bridge does
anyway.

**A live deletion is delivered to every subscriber whether or not it
asked**: that is how a client learns a key it holds has gone, and it is
the one moment at which a deletion is not hidden. The property is about
the state a subscription is handed when it starts, which is the only place
absence cannot be told from silence.

**A deletion expires on its own clock**, `deletion_retention_period`, which
is a day when the file does not say. A value lives as long as it is the
truth; a deletion only has to live long enough for everything reading this
channel to have seen it. Held under the value period, a channel keeping its
values for ever would keep a row for every device ever decommissioned and
nothing would bring the topic count down again. What sets it is the longest
outage a subscriber may have and still be told a topic is gone -
and an outage longer than it loses the deletion, which is the cost of
bounding this at all.

A subscriber receives both. On subscribe, the current value of every topic
its filters reach, carrying the RETAIN flag - which is how a client tells
state it is catching up on from an update that has just happened. After
that, every change as it happens, without the flag - unless it asked for
**Retain As Published**, and then a change carries the flag its publisher
set (MQTT-3.3.1-13), exactly as a broadcast topic's does.

**The catch-up pass carries the flag either way**, including for a Retain
As Published subscriber and including where no publisher ever set one. That
flag is Sagüin saying *this is what you are catching up on* rather than
repeating a publisher, which is the whole reason this channel type sets it,
and broadcast makes the same distinction: a value served from the store at
SUBSCRIBE is flagged for everybody, and Retain As Published changes only
the live delivery. Each value carries an offset, so a consumer can tell
which of two it holds is newer; the offset is not a position, and replacing
a value gives it a new one.

The current state is delivered by the broker rather than through the
server's own retained-message store, for the same reason `append` records
are: the store fans a value out to whoever matches at the moment it is
set, and a `latest` channel owes a subscriber the current value of every
topic it reaches at the moment it *subscribes*. Delivering it here is also
what keeps the current-state pass out of a shared subscription crossing the
channel, which the store would have no way to know about: a shared
subscriber is served changes and nothing else.

### Every value says when it was set

A delivery carries `saguin-timestamp`: the moment the broker received the
publish, as Unix milliseconds in decimal. One integer, because what a
consumer does with it is compare it, and every language compares integers
without a date library. The seek request in "Moving a consumer's position"
takes human forms like `-12h` instead, and for the opposite reason: a
person types that one.

**It is on every channel delivery and not only this one.** The receipt time
is the same fact whichever channel holds the record, and two consumers of
one broker reading different metadata off the same kind of record is worse
than either answer alone. It is described here because this is where it
does the most work.

A record with no timestamp carries no header rather than a wrong one -
that is a record stored without one, and the zero time formats as a
number that reads as the year 1754.

**This is the failure retained messages are best known for, and the reason
to prefer a channel.** A retained message carries no age, so a dashboard
showing a reading from a sensor that died three weeks ago looks exactly
like one showing a reading from a second ago, and no MQTT client can tell
them apart. Every serious deployment builds a workaround in the
application, usually by putting a timestamp inside the payload, which then
only works for producers that agreed to a payload format.

Read with the channel's `retention_period`, it becomes a promise rather
than a diagnostic: **a value older than the period is not there at all**,
so a consumer that receives one knows it is no older than the period, and
knows exactly how old within it. That is a guarantee no retained store can
make, and it costs one header.

The broadcast retained store deliberately does not carry it - nothing
Sagüin adds rides on those, they go back out as they were published - so
"how old is this state" is one of the things a `latest` channel is for.

### What a reconnecting consumer receives

**On a new subscription: the current value of every topic the filter
reaches.** A client that clears its own cache and rebuilds from what
arrives is correct, which is what most of them do and what MQTT's retained
delivery already promises.

**On a session resumed without a new SUBSCRIBE: only the topics whose
value changed while it was away.** A consumer's position on a `latest`
channel is **the lowest offset not yet delivered to it**, and a resumed
session is served from above that. Four things can be below it and each is
covered: anything acknowledged has been received; anything sent and not
acknowledged is resent by the session itself; anything the broker could not
deliver at all - refused by the `acl_file`, or too large for the subscriber -
is held below the position until it is served, which is what the next
paragraph but one is about; and anything still waiting to be sent when the
consumer went away, a snapshot's remainder or a live value, is held below it
the same way, because values are sent a window at a time and a consumer can
leave in the middle of them.

That is the same rule an `append` consumer's position follows, and for the
same reason - a position must never step over a record that has not been
delivered, or the consumer resumes after the gap and reports success over
it. Both channel types issue offsets from a single counter per channel, so
the rule reads identically on both.

**An `append` consumer has a second number, and the two differ on
purpose.** What a resumed session is served from is the one above: the
lowest offset not yet delivered, so that the records the session is itself
re-sending are not sent a second time by the broker beside them. What is
*stored* - the number that survives a restart - is the lowest offset not
yet **acknowledged**, which is lower whenever anything is in flight.

The difference is what a restart does to a channel's in-flight records. A
resumed session re-sends its own unacknowledged packets, so the broker must
not; a restart leaves none of them to re-send - a channel's records are not
session state and are not restored with a session (invariant 15) - and a
stored position above an unacknowledged record would step over it for good.
So the higher number lives with the session and the lower one is written down,
and each is the safe answer to the question it is asked.

What a consumer sees, either way, is at-least-once behaving as promised: a
record it held unacknowledged when the link dropped arrives again, once
from the session on a resume and once from the channel after a restart.
Deduplicating on `saguin-offset` - ignore anything at or below the highest
you have processed on that channel - makes both cases exact, and is the
same five lines a consumer needs for at-least-once anyway.

**That position lives with the session and no longer**, which is the one
difference from an `append` channel, where it is stored with the records and
survives a restart. It is held in memory, so a restart takes it even where the
session itself comes back, and the consumer is served the whole of current
state on its next connection - complete, rather than a gap. Which moment that
is depends on whether its session came back: the resume where it did, the
SUBSCRIBE it sends where it did not.

That moment is one MQTT says nothing about, which is what makes it Sagüin's
to use: a standard broker sends **nothing at all** when a session resumes
with a subscription already in it, so a client that does not re-subscribe
stays stale until it does. The cost of the alternative is the thing this
design exists to avoid - ten thousand state topics re-sent in full on every
reconnect over a link that drops, when three of them moved.

Three consequences, each worth stating because each is a question somebody
will ask:

- **A QoS 0 subscriber gets the full state every time.** A position moves
  on an acknowledgement and QoS 0 has none, so there is nothing to serve a
  difference from - the same absence that makes a queue refuse QoS 0
  outright.
- **Retain Handling governs the SUBSCRIBE, not the resume.** A client
  asking for retained messages on subscribe gets the full state; asking for
  them only if the subscription is new gets nothing on a re-subscribe;
  asking for none gets none. All three are MQTT's own option and Sagüin
  does what each says. So a client can always ask to be sent the lot, and
  can always refuse to be sent any of it, without Sagüin having invented a
  way to ask.

  **On this channel type it changes nothing that outlives the connection**,
  which is where it differs from an `append` channel and is worth saying
  beside it. Here the option decides one pass and no more: a client that
  asked for none this time is sent the full state the next time it asks for
  it, because a `latest` channel keeps no position for the option to move.
  On an `append` channel the same option at 2 commits the consumer at the
  head - it is choosing to skip a backlog, and a skip held for one
  connection would hand the backlog back on the next. Two answers because
  the two channel types keep different things: current state that is
  re-sent on demand, and a position that is the consumer's place.
- **A topic deleted while a consumer was away is not reported to it on
  resume.** The deletion is stored (above), but a resumed session carries
  no `SUBSCRIBE` and so no `saguin-deletions`, and the difference it is
  served leaves deletions out as a subscribe snapshot does - so a consumer
  merging that difference into its own cache keeps an entry for a topic
  that no longer has one. A standard broker has the identical hole - it
  sends nothing on resume, so the stale entry survives there too - and the
  way out is the same in both: re-subscribe, and rebuild from the full
  state, with `saguin-deletions` if the deletions themselves are wanted.

**A subscriber is promised the current value, not every intermediate one.**
State is delivered within the subscriber's in-flight window, and what does
not fit waits for an acknowledgement to free a slot. A *live* update waits
the same way, and a newer value for the same topic takes the place of one
still waiting: a subscriber that has fallen behind is sent what is current
when it has room, rather than every value in between. What waits is at most
one value for each topic the subscription reaches - the bound the
current-state pass always had - and not every update a publisher makes,
which would be the unbounded per-subscriber buffer this design refuses
everywhere else.

**Never an older value after a newer one.** Two publishers can set one topic
at nearly the same moment, and their values are stored in one order and
handed to subscribers by two goroutines in whichever order those run - so
the older value, stored first, could reach a subscriber after the newer one
had been written to it, and be applied over it as current. A subscriber is
never sent a value of a topic older than one it has already been sent: each
topic's newest offset handed out is remembered, and a value found below it
on its way to a subscriber, live or in the state a subscription is served,
is not sent. That is what makes `saguin-offset` safe to act on as well as to
compare.

**The waiting is the subscriber's, never the publisher's.** A publisher's
acknowledgement waits for its value to be stored and for nothing a
subscriber does (invariant 16): each subscriber's values are written by a
delivery of its own, so one that has stopped reading holds nobody but
itself.

**"When it catches up" includes a resume**, and that is what holds the
position back. A value still waiting when the connection goes, and a live
value that is not going to reach the subscriber at all - the `acl_file`
refuses it, or it is larger than the subscriber's Maximum Packet Size - keeps
its own offset, which is lower than the next one the consumer acknowledges.
A position that moved to the highest acknowledged would step over a value
that was never delivered and the difference would never carry it again. The
consumer resumes from below it instead, and is re-sent values it already
holds along with the one it does not. That is the trade this document makes
everywhere: over-deliver rather than let a consumer believe it is current
when it is not (invariant 1).

A consumer that has ever missed a value therefore resumes from below it
every time. The saving is kept by the consumers that never miss one and
spent by the ones that have - and a subscriber is told nothing either way,
because MQTT has no signal for "you missed an update" outside CONNECT. The
broker logs each value it could not deliver, with the reason, which is the
operator's only notice of it. A value that waited for room and was then sent
is not missed, and is not logged.

**A value the session is still holding is subject to the same rule.** MQTT
restores a session's unacknowledged packets and re-sends them, and one of
those may be a `latest` value the store has written past while the client
was away. Such a value is removed as the session is resumed, **before
anything is re-sent**, so it is never put on the wire a second time: what
the subscriber is owed for that topic is whatever is current now, and the
catch-up immediately after the resume serves exactly that. Only a value the
store has actually superseded is removed - one that is still current is
re-sent as MQTT asks - so the session is never served less than it was
owed, and the position is held below the value that went, exactly as it is
for one still waiting when a consumer leaves.

**Before rather than after, and the difference is a false acknowledgement.**
A superseded value written and then withdrawn leaves its packet identifier
free while the client still owes an acknowledgement for it, and an
identifier that is free is reissued - so the current value can go out under
the identifier the stale one is still owed, and that acknowledgement is
credited to the wrong delivery. The broker would then record the subscriber
as holding current state it may never have read (invariant 1). Removing the
value before the session is resumed leaves nothing written, nothing owed and
no identifier to reissue.

The alternative is the same buffering this section refuses, arriving by a
different door: a superseded value re-sent as though it were current holds
a slot of the subscriber's Receive Maximum until it is acknowledged, and no
acknowledgement can arrive for a delivery re-sent to a connection that has
already gone. Such a value is therefore re-sent on every resume and never
cleared, one more with each reconnect that lands in the wrong moment, until
the window is full of them. A subscriber in that state is connected,
subscribed, and permanently behind: its current-state pass waits for a slot
that never frees, and it holds state nothing can correct (invariant 1).

This applies to `latest` alone. An `append` record is history and a queue
record is a lease; neither is superseded by a later write to the same topic,
and both re-send untouched.

**That mark is cleared by a new SUBSCRIBE and by nothing else**, so it is
paid for the life of the session rather than only while a consumer is
behind: a value dropped at nine in the morning still costs the same resume
at six in the evening, on a consumer that has kept up perfectly since.

That is deliberate and it is the safe direction. Clearing it once the
resume that stood in for it has been acknowledged would buy the saving
back, and it is more state to keep and more ways to be wrong about it, on
a session that has already shown it can fall behind. A client that wants
the saving back has a way to ask: a re-subscribe, which is answered with
the whole of current state and clears the mark by doing so.

What `latest` adds over a retained message, in one place: an age on every
value and a period beyond which none is served, so state cannot look live
when it is not; a position, so a consumer can tell which of two values it
holds is newer; and only the changes on a resumed session, so a reconnect
costs what moved rather than everything. Being bounded, durable and
readable with `sqlite3` is not on that list, because Sagüin's own retained
store has all three - those distinguish it from a conventional broker's
retained store rather than from this one. Durability is a snapshot
across a graceful shutdown, or SQLite storage across a crash; expiry is
the broker's sweep, below. There is no size bound: a latest channel holds
one value per topic, so what grows is the number of topics, and expiry is
what removes a topic nothing publishes to (RFC 0002).

**Expiry on a `latest` channel deletes the current value, and that is the
point rather than a defect.** A topic whose last publish is older than the
channel's retention period stops having a value at all: a subscriber that
arrives afterwards is told nothing about it, exactly as for a topic that
never existed. A stale reading is worse than none, and a consumer cannot
tell the difference between the two if the old one is still being served.

**A value waiting to be sent is held to the same period.** A subscriber
short of window has values waiting for it - the rest of its snapshot, and
changes since - and one the period passes while it waits is let go and not
sent, as one waiting to cross a bridge is ("Retained messages"). The
retained store's values waiting for a subscriber are held to
`broker.retained`'s period the same way.

It follows that a device reporting less often than the period loses its
state between reports, and that the broker-wide default reaches these
channels like any other (RFC 0002). A channel holding slow-moving state
says so with its own `retention_period`.

### A 3.1.1 subscriber, on a `latest` channel

**On subscribe: the current value of every topic its filters reach,
carrying the RETAIN flag** - the same as an MQTT 5 subscriber, for the
same reason. A value *is* current state, RETAIN is MQTT's own flag for
saying so, and 3.1.1 has it. There is no tail rule here: what this channel
type is for is the value you find when you arrive, and a device handed
current state is not a device acting twice on an old event.

**On a session resumed without a new `SUBSCRIBE`: only the topics whose
value changed while it was away**, again as for MQTT 5. The position that
makes that possible lives with the session rather than with the records,
and a 3.1.1 persistent session holds one exactly as an MQTT 5 session
does.

**Retain Handling is an MQTT 5 subscription option and 3.1.1 has no
equivalent**, so a 3.1.1 client cannot ask to be sent less than the whole
state on a re-subscribe. Every new `SUBSCRIBE` brings the lot - which is
what Retain Handling 0 does for an MQTT 5 client, and is what most of them
ask for anyway.

**It has no offset to compare**, so what it holds for a topic is whatever
arrived last on its connection. That is the ordinary MQTT contract and it
is enough for state, where an `append` consumer's need to place one record
against another in a log is not.

### Reading one value: the point read

A `latest` channel is a key-value store, and three of the four verbs are
already ordinary MQTT. **SET** is a publish to the topic. **DELETE** is a
publish with a zero-length payload. A **multi-get** is a subscription, which
receives the current value of every topic its filter reaches. The one that
is not is a **point read**: the current value of one topic, to a caller that
does not become a subscriber.

```
PUBLISH  $saguin/kv/get
  payload:          the topic to read
  Response Topic:   where the answer goes
  Correlation Data: optional
```

The reply is **sent to the client that asked**, on the Response Topic it
named, with the value as its payload. It is not published: a third client
subscribed to that topic receives nothing, which is the behaviour to want -
a value should not reach whoever guessed the reply topic - but "published"
reads as a fan-out, and somebody building a collector on the reply topic
would wait for ever.

**An empty reply means there is no value.** That is what a zero-length
payload already means on this channel type, so absence needs no new
vocabulary, and a topic never set and one whose value was deleted are the
same answer - exactly as this channel type already makes them.

**Which is why every refusal rides the `PUBACK` instead.** If a bad request
could answer on the Response Topic, an empty reply would mean two things and
the verb would lose the distinction it exists for. The Response Topic only
ever carries an answer.

| The request | |
|---|---|
| the key names no channel | refused `0x90`: a topic no channel claims is broadcast and has no stored value |
| the key names an `append` or `queue` channel | refused `0x90`, naming the channel and its type |
| the key holds a wildcard | refused `0x90`: "everything under X" is what a subscription already does, and a point read that returned an unbounded set would be a scan |
| the key is in a reserved space | refused `0x90`. No channel name may begin with `$`, so a key that does holds no channel's value - and it is not broadcast either, since a publish there is refused too |
| the key is empty, or longer than `max_topic_length` or deeper than `max_topic_levels` | refused, the first `0x83` and the others `0x90` |
| no Response Topic | refused `0x83`. A read with nowhere to send its answer has no effect at all, and accepting it would tell the client it had worked |
| QoS 0 | dropped. There is no `PUBACK` to carry any of the above |

The two codes are split by what is wrong rather than by where the check
happens, because the code is what `saguin_publish_refused_total` carries
(RFC 0005): `0x83` says the request cannot be acted on, `0x90` says the key
is not a topic this broker has a value for. An operator watching one number
can then tell a fleet mistyping keys from a client that forgot its reply
path.

**Every reply carries Correlation Data**, whether or not the caller sent
any: the caller's when it did, and otherwise the key it asked about. A
client with several reads outstanding can tell the answers apart without
keeping a table, and one that wants its own bookkeeping still has it.

**The reply goes to the asking session and to nobody else**, exactly as a
seek's does and for the same reason: it is written to the connection that
asked, needs no subscription, and a third party on the same Response
Topic sees nothing.

The consequence to know before trying it: `mosquitto_pub` publishes and
exits, so it asks and is gone before it can show you the answer - `-d`
prints the reply arriving, and anything more than that wants a client that
stays connected.

**A point read never makes the caller a subscriber**, which is the whole
reason it exists. Subscribing to ask returns silence for an absent key -
indistinguishable from a slow one - and enrols the caller in every later
update to a topic it wanted once.

It reaches `latest` channels only. An `append` channel is a log rather than
a set of current values, and a queue holds unresolved work that exactly one
consumer may take.

### Saying which schema deserializes a payload

A publisher can already say *how* its payload is serialized: Content Type
and Payload Format Indicator are MQTT 5's own fields and Sagüin carries
both. What it cannot say with them is *which schema* -
`application/x-protobuf` does not tell a WeatherReading from a WaterLevel,
and a consumer holding the bytes has no way to find out.

**This needs nothing from the broker.** A `latest` channel is already a
key-value store with delete, readable one key at a time by the point read
above, so a schema registry is a channel and a convention:

```yaml
channels:
  schemas:
    type: latest
    filter: schemas/#
```

- **Register** by publishing the schema text to a topic in that channel.
- **Retire** by publishing a zero-length payload, which is how this channel
  type deletes.
- **Produce** with Content Type saying the serialization format and a
  User Property named **`schema`** carrying the schema's topic.
- **Consume** by reading that property, point-reading the topic it names,
  and caching the answer until the pointer changes.

**The property is called `schema`**, and Sagüin recommends the name rather
than leaving it to each deployment for the reason a convention exists at
all: a consumer written against one name works against any broker that
follows this section, and a consumer written against a name somebody chose
locally works nowhere else. Nothing in the broker reads it - it is an
ordinary User Property and Sagüin never touches it - so an operator with a
reason may use another name, and pays for it in portability.

**The pointer is a whole topic rather than a bare id**, and that is what
makes the convention work without an allocator. A bare `weather-v1` has two
holes: two publishers in different domains choose the same name and the
second write silently replaces the first, and nothing in the message says
which channel or prefix to look under. A topic answers both - the consumer
reads exactly the string it was given.

**Collisions are refused rather than discouraged.** One ACL rule confines
each publisher to its own prefix, so two domains choosing the same name
cannot reach each other's key:

```yaml
roles:
  device:
    - channel: schemas
      filter: "schemas/%u/#"   # acme may write only under schemas/acme/
      allow: [write, read, delete]
  reader:
    - channel: schemas
      allow: [read]            # read any schema, write none
```

**The registry can live anywhere.** `filter:` is compared against the
whole topic, so a registry at `schemas/#` in a channel named `schemas` and
one nested at `iot/+/schemas/+` are written the same way - the second's
rule is `filter: "iot/%u/schemas/+"`, and nothing about the mechanism
changes.

**A consumer follows a pointer only where it lands inside the registry's
own filter.** The ACL governs who may *write* a topic and never who may
*name* one, so a publisher may point at anything - including a `latest`
channel holding device state. The check is one comparison and the filter is
readable from `saguin_channel_info`.

**`saguin-` is reserved on a publish**, so `saguin-schema` - the name that
looks most official - is the one that will not survive: it is dropped like
every other reserved name a client sets, silently, because a publisher
cannot be allowed to forge the broker's own properties.

**What Sagüin does not do**, said plainly because a registry usually does
it: it does not parse a registered schema, check that it is valid, or
enforce that a new version is compatible with the last. The payload is
opaque bytes here as everywhere else. A deployment that must refuse an
incompatible evolution still needs something that understands the format.

## When retention removes

Both retention rules are applied by a sweep on the broker's clock rather
than by the write that violates them, and neither interval is
configurable.

**A channel may therefore hold slightly more than it says, briefly.** Over
its `retention_bytes` until the next sweep, and past its
`retention_period` by about a tenth of that period. That is the difference
between retention and `max_bytes`: `max_bytes` is a refusal a producer is
told about with `0x97`, so it holds exactly, and retention is a
housekeeping target that removes behind a producer and never says
anything. A bound nobody is told about does not have to be exact; it has
to be reached.

The alternative was trimming on the publish that goes over, which is exact
and is paid on every publish to a channel at its size: a permanent tax on
the write path to make a number exact that nothing reports. Kafka makes
the same trade with `log.retention.check.interval.ms`, and for the same
reason.

**Age needs a clock whatever size does**, because a channel that has
stopped receiving writes still ages, and no publish will ever arrive to
notice.

The two run on separate clocks because they decide different things. The
size sweep decides how far above its number a channel may sit, so it is
frequent and fixed. The age sweep decides only how far past its deadline
a record may survive, so it is a tenth of the shortest deadline any channel
configured - a month's retention is served perfectly well by a sweep every
three days. **A queue's `job_expires_after` counts as one of those
deadlines**, since it is the same sweep that enforces it, and the whole is
floored at one second, which is also what a configuration carrying no
deadline at all gets. Both sweeps run once at startup too, so a broker that
was down while its records aged does not wait for a tick to notice.

None of this is exposed. What an operator would be choosing is how far
past their own policy they are willing to sit, and there is no answer to
that except "as little as it costs" - which is what the broker already
picks. It states both intervals at startup.

## `queue`

### States

```
                         ┌──────────────┐
   publish ────────────▶│  AVAILABLE   │◀──────────┐
                         └──────┬───────┘          │
                      PUBLISH   │                  │
                                ▼                  │
                        ┌──────────────┐           │
                        │  DELIVERING  │───────────┤ disconnect
                        └──────┬───────┘           │ before PUBACK
                               │                   │ never sent
                      PUBACK   │                   │
                               ▼                   │
                        ┌──────────────┐   return  │
                        │   LEASED     │───────────┤ timeout
                        └──────┬───────┘           │ disconnect
                               │                   │
                          ack  │                   │ (attempts remain)
                               ▼                   │
                        ┌──────────────┐           │
                        │   RESOLVED   │◀─────────┘ attempts exhausted
                        └──────────────┘             ⇒ dead-letter
```

`DELIVERING` and `LEASED` are separate on purpose: a record's visibility
deadline must never overlap its transport in-flight window. While
`DELIVERING`, the record occupies a slot in the worker's MQTT in-flight
window and no visibility deadline is running. The deadline starts at
`PUBACK`, when that slot is released. A record can therefore never be
simultaneously held unacknowledged at the transport layer and expired at
the application layer, which is the condition that would otherwise leak a
flow-control slot per timeout until the worker silently stopped receiving
work.

**`DELIVERING` has no deadline, so it needs the other exit: a record whose
`PUBLISH` was never put on the wire is returned to `AVAILABLE`, with its
attempt unspent.** Waiting for the `PUBACK` assumes one can arrive, and for
a record the connection never carried none ever can - so without this the
record stays `DELIVERING` until the worker's session ends, held by a worker
that has never seen it: not redelivered, not dead-lettered, and offered to
nobody else.

**It is not a deadline on `DELIVERING`** - "no `PUBACK` yet" is the
ordinary state of a job in progress, so any clock short enough to rescue
a stranded record takes a live worker's job away (invariant 7). Whether
the packet was sent is a question with an answer, so the broker asks it
rather than timing it, and a worker mid-job is never touched.

### Delivery

**A worker subscribes to `$saguin/queue/<channel>` at QoS 1**, and nothing
else consumes a queue. **A subscription asking for No Local is refused
`0x8F`**, naming the filter to change; the connection stands. The reason
is the one MQTT gives for making the same flag a Protocol Error on a
shared subscription: echo-suppression and work distribution cannot both be
honoured. A job withheld from the one worker whose client id published it
is never delivered - so it is never acknowledged, never times out, never
dead-letters, and nothing reports a job that has stopped moving. A single
worker publishing its own work is all it takes. The broker keeps its own
index of who is subscribed to which queue and selects one of them -
round-robin over the workers that are live, hold no job from this queue,
have room in their in-flight window, and are still allowed the channel by
the `acl_file` - then sends a `PUBLISH` at QoS 1 carrying:

| Property | Value |
|---|---|
| Response Topic | `$saguin/queue/<channel>/response` |
| Correlation Data | the Delivery ID for this attempt |
| User Property `saguin-id` | the Message ID |
| User Property `saguin-offset` | the record's position in the queue |
| User Property `saguin-timestamp` | when the broker received it, Unix milliseconds |
| User Property `saguin-expires` | when the publisher's expiry runs out, Unix milliseconds; only where one was set |
| User Property `saguin-attempt` | this attempt's number, from 1 |

The four `saguin-` properties before the attempt are on every channel
delivery, `saguin-expires` only where an expiry was set; Response Topic,
Correlation Data and `saguin-attempt` are the queue's own. On a queue
`saguin-expires` is the one a worker is expected to act on: nothing removes
a job on the publisher's clock, so a job whose moment has passed is still
offered, and whether stale work is worth doing is the worker's decision
rather than the broker's.

**The topic and the payload are the publisher's, unchanged, and so are its
own User Properties.** A delivery is the original message with the
properties above added to it - there is no envelope, nothing to unwrap, and
nothing rewritten. `orders/resize/thumbnails/42` is delivered on
`orders/resize/thumbnails/42`, and a header the publisher set arrives beside
`saguin-id` rather than inside anything. Only the `saguin-` prefix is
reserved (see "From publish to record").

**The topic under a queue channel does not select a worker, and this is the
one place a habit from ordinary MQTT misleads.** On an `append` or `latest`
channel a subscriber's filter chooses what it receives. A queue admits
exactly one subscription form, so every worker draws from the whole channel
whatever the topic says: it is description, not address. It survives storage
and redelivery, and it carries into the dead-letter channel - a record
published to `orders/resize/thumbnails/42` is dead-lettered to
`orders/__dlq/resize/thumbnails/42`, which is what lets an operator see
what failed.

Work that must go to different pools of workers is therefore **two channels,
not two filters**, and the broker says so: a narrower filter is refused with
`0x8F` rather than granted and quietly given a share of everything.

**A worker holds one job from each queue at a time.** A job is held from
the moment it is offered until the worker acknowledges or returns it, its
visibility timeout runs out, or the worker disconnects, and until then
that worker is offered nothing more from that queue. A worker consuming
two queues holds one job from each, and the two do not wait on each other.

**Receive Maximum does not change this.** It bounds what the connection
has in flight, a job included, and a worker whose window is full is passed
over as one holding a job is. More concurrency is more workers, and Sagüin
adds no configuration for it.

**An answer offers that worker its next job at once**, and so does the
acknowledgement that reopens a full window - a client may answer a job
before it acknowledges the packet, as Paho does. New work, and a worker
that has just subscribed, are offered on a fixed 200ms tick, so a first job
can wait that long. With `examples/support/worker` doing no work per job,
one worker drained a 300-job backlog in 53 to 213ms over six runs at
Receive Maximum 1, 2 and unset, the longest being those whose first offer
waited for the tick. A worker is bounded by its own processing, not by the
broker.

The tick is not configurable, for the reason the sweep intervals are not -
an operator has no way to reason about what to set it to, and the honest
answer is "as little as it costs".

The **Delivery ID** is opaque bytes, at most 32 of them. Clients echo it
exactly and never parse it. It is unique to the attempt: a redelivery of
the same record carries a different one.

### Acknowledgement and return

The worker publishes to the Response Topic it was given, echoing the
Correlation Data it was given, with a payload of exactly:

| Payload | Meaning |
|---|---|
| `ack` | the work succeeded; resolve the record |
| `return` | the work failed; make it available again now |

That is the entire protocol. It is an ordinary `PUBLISH`, and this is its
shape - though as a command it cannot work on its own: `mosquitto_pub` is
a second session, so rule 3 below has the broker acknowledge it and
ignore it; the job comes back to its worker as attempt 2 when the
visibility timeout passes.

```sh
mosquitto_pub -V 5 -q 1 -t '$saguin/queue/jobs/response' -m ack \
  -D publish correlation-data "$CORRELATION_DATA"
```

In a worker it is the message handler answering on the session the job
arrived on - Paho, which acknowledges once and is not redelivered:

```python
def on_message(client, userdata, job):
    props = Properties(PacketTypes.PUBLISH)
    props.CorrelationData = job.properties.CorrelationData
    outcome = "ack" if process(job) else "return"
    client.publish(job.properties.ResponseTopic, outcome, qos=1,
                   properties=props)
```

`PUBACK` is not acknowledgement. It confirms MQTT moved a packet and
says nothing about whether the work succeeded. A worker whose library
acknowledges on receipt has told Sagüin only that it received the job.

The broker accepts the response only when **all** of the following hold:

1. Correlation Data is present and decodes to a known delivery;
2. that delivery is the record's **current** one;
3. the session publishing the response is the session holding it.

Rule 3 is what stops one worker resolving another's work. Rules 1 and 2
are the fencing check, and it runs on every operation naming a delivery,
not only on `ack`.

A response failing any of them is ignored and logged. A payload that is
neither `ack` nor `return` is ignored and logged. Ignoring is safe: the
lease expires and the record is redelivered.

**A worker is not told whether its response was applied.** The `PUBACK` it
gets confirms the broker received the packet, nothing more. If the
response was too late, another worker already holds the record and there
is nothing useful the first worker could do about it. Adding a reply path
would be a second protocol for no decision anyone can act on.

### Attempts, timeout, and redelivery

The attempt count increments when the worker is known to have received the
record: at `PUBACK`, or when it answers for that delivery, whichever comes
first. A delivery that reaches neither burns nothing - the worker never
showed it had the job.

Both halves are load-bearing. `PUBACK` alone is not enough, because a
client library sends it when its receive handler returns, so a worker that
answers from inside the handler answers *before* it acknowledges. Counting
only at `PUBACK` would leave such a worker's `return` uncounted, and a job
it keeps handing back would be redelivered for ever instead of being
dead-lettered. Answering is the stronger evidence of the two: a worker
that replies about a delivery demonstrably had it.

A lease ends in one of four ways:

| | Result |
|---|---|
| `ack` | RESOLVED |
| `return` | AVAILABLE, after `retry.backoff` |
| deadline passes | AVAILABLE, after whatever is left of `retry.backoff` |
| worker's session ends | AVAILABLE, after whatever is left of `retry.backoff` |

**The last three are one rule and not three**, and the next section is what
it is: the gap runs from the moment the worker last had the record. A
worker that answers is measured from its answer, so it waits the whole gap.
A record taken back by the deadline or by a session ending is measured from
when that worker received it, so however long it was held counts toward the
gap - and at the default 30s visibility timeout, a gap under five attempts
at a 2s base is already spent.

In every case that returns the record, the delivery epoch advances, so the
Delivery ID just used is dead. A late `ack` from the previous holder
resolves nothing and changes nothing.

A worker's session ending returns every record it held, immediately. The
broker knows which records those are, so this needs no waiting for a
deadline.

When a record would become AVAILABLE but its attempt count has reached
`retry.max_attempts`, it is dead-lettered instead.

### Backoff

A returned record is offered again on the next tick, which is right for a
job that failed on its own and wrong for one failing because something
downstream is down: the faster the worker fails, the harder the queue
hammers whatever is already broken, and `retry.max_attempts` is spent
inside a second. `retry.backoff` puts a widening gap between the attempts.

| `retry.backoff` | The gap after `n` attempts |
|---|---|
| `none` - the default | none; the record is offered on the next tick |
| `linear` | `backoff_base x n` - 2s, 4s, 6s at a 2s base |
| `exponential` | `backoff_base x 2^(n-1)` - 2s, 4s, 8s, 16s at a 2s base |

**There is no maximum gap and there is no key for one.**
`retry.max_attempts` already bounds the total: five attempts at a 2s base
come to 30 seconds of waiting on the exponential shape, and a ceiling would
be a second bound on a quantity that already has one.

**One rule decides every case: the gap runs from the moment the worker last
had the record**, and not from the record's `saguin-timestamp`, which is
when the broker received it.

Three things follow from that one rule, and none of them is a rule of its
own:

**A record taken back by the visibility timeout waits no further.** The
worker had it from the moment it was delivered, and the timeout is exactly
how long ago that was - so a gap shorter than `visibility_timeout` is
already served and the record goes out on the next tick. That is the
intended answer and not a concession: the timeout is a detection delay, but
it is also real time in which nothing was retried. Where the computed gap
is *longer* than the visibility timeout, the difference is what such a
record waits, and at the default 30s timeout no gap under five attempts at
a 2s base reaches it.

**A worker whose session ends is the same case**, and it is the one where
the arithmetic can still leave something to wait: the record is measured
from when that worker received it, and a session that ends a second after a
delivery has served only a second of the gap.

**The backoff and the visibility timeout never compete.** They govern
opposite states. While a worker holds a record the timeout is what ends
that state, and the backoff is not consulted at all - the record is not a
candidate for delivery, so there is nothing to delay. Once the record is
back in the queue the timeout is over and the backoff is what decides when
it goes out.

**A record still waiting is skipped, and never waited on.** The offer walks
past it to the records behind it, so one job nobody can process cannot stall
the channel for the length of its own gap.

Two things about the shape of it. The gap is honoured to within one 200ms
tick, so the wait is the gap rounded up to the next one; and a record that
is skipped holds no worker, because it is never offered.

### At a size bound

A queue takes `max_bytes` and never retention, because deleting
unacknowledged work is eviction of unresolved work and is never permitted
(invariant 2). What a queue does at its bound is refuse the publish with
`0x97`, and that makes the bound **flow control rather than a limit**:
resolution is what frees the space, so a queue at its bound accepts work
again as its workers catch up, without an operator doing anything.

The rule that keeps it from being a deadlock is worth stating on its own,
because a bound that refuses every write has one:

> **A bound never refuses the operation that would relieve it.**
> Acknowledgement, return, the visibility timeout, **the attempt count**,
> a consumer's stored position, and the move into the dead-letter channel
> are not publishes and are never refused for want of capacity.

Without it a full queue could not dead-letter, so it could not drain, so
it would stay full - and the dead-letter move is exactly the path that
takes bytes out of a queue that nothing is succeeding at. A move that
fails for any *other* reason still leaves the record as it was, with its
attempt unspent, as above.

**The attempt count is in that list because it is a precondition.** A
record whose attempt count cannot be written never reaches
`retry.max_attempts`, so it never becomes eligible for the move that
would take it out of the queue: the bound blocks the relief indirectly,
by blocking the thing that has to happen first. It reads as bookkeeping
and it is a precondition.

**Every one of those operations is a write, so a bound has to hold room
for them.** A provider therefore keeps a **reserve** - one largest
possible record with its headers, carved out of `max_bytes` and derived
from `max_message_size` plus `max_header_bytes` rather than configured -
which publishes may not touch and the operations above may. A `max_bytes`
below twice the reserve is a startup error. How each provider keeps it,
and why the reserve is spent rather than lent on sqlite, is RFC 0004
"The reserve".

### Dead-lettering

The record leaves the queue and appears in `<channel>__dlq` in one
transaction, or neither happens. A move refused for want of capacity
leaves the record exactly as it was, including its delivery state, so the
attempt is not consumed by a failure that was the broker's.

Its topic gains one level, `__dlq` (RFC 0002 "The dead-letter channel"),
which the move writes directly: a record at `limits.max_topic_levels` is
kept one level past it in the dead-letter channel, and is read there through
a shallower filter ending in `#` (RFC 0002 "How deep a topic may be").

The dead-lettered record keeps its Message ID and gains headers:

```
saguin-dlq-channel     the queue it came from
saguin-dlq-offset      its offset there
saguin-dlq-attempts    how many attempts were made
saguin-dlq-first       first delivery time
saguin-dlq-last        last delivery time
saguin-dlq-at          dead-letter time
saguin-dlq-reason      attempts_exhausted | expired
```

They are ordinary User Properties, so a stock `mosquitto_sub` on
`jobs/__dlq/#` reads them without any Sagüin-specific tooling.

A record whose age exceeds `job_expires_after` is dead-lettered without
further delivery, with reason `expired`. It goes whatever its attempt count
says, because it is out of time rather than out of attempts - a job that
expires before any worker exists is dead-lettered on its first offer with
no attempt spent.

**`job_expires_after` is the only clock that ends a job.** A publisher's
own Message Expiry Interval does not, on this channel type or any other:
taking work out of a queue is the operator's decision, and this is where
they make it. A queue that writes no `job_expires_after` never expires work
at all.

**What a publisher's expiry does instead is arrive and be read.** The
delivery carries `saguin-expires`, the moment the interval runs out, and
the worker decides what to do about it - ignore the job and acknowledge, or
do it anyway and acknowledge. The broker delivers; the application judges.
That is the same division as everywhere else here: the broker enforces
rules an operator wrote, and does not make an application's decisions for
it.

**This is a deliberate deviation from MQTT and is worth naming.** The
specification deletes a message whose expiry has passed before it was
delivered. Sagüin does not, on any channel: a record is what an operator
asked it to keep, and a client does not get to remove it. The one place the
publisher's clock still deletes is a retained value on a broadcast topic,
which is MQTT's own store, has no consumer position to damage, and is
covered under "Retained messages".

**Two places check it, and they are not redundant**: a job is checked as
it is about to be handed to a worker, and the broker's own sweep expires
jobs that are waiting - a queue whose workers have all gone away is never
asked for anything, and expiry exists for exactly that queue.

The sweep leaves alone any job that is out with a worker. Removing one is
removing a record in flight to a consumer, and the worker's answer would
then arrive for a job that no longer exists. Such a job expires on its next
offer instead, or when the worker hands it back.

A job with no timestamp never expires. Storage keeps an unset time as zero,
which read as a date is in 1754, so a queue of them would be dead-lettered
whole on the first check.

The dead-letter channel is an `append` channel in every other respect:
independent consumers, replay, its own retention.

### Putting dead-lettered work back

"How do I retry the dead letters once the bug is fixed?" has an answer and
it is four lines of any MQTT client, because both halves already exist: a
dead-letter channel is an ordinary `append` channel, and a queue takes an
ordinary publish.

**Read the dead letters.** Subscribe to the dead-letter channel as to any
`append` channel. A durable session resumes where it left off; a seek to 0
replays everything the channel still holds, which is what to use after
fixing a bug that dead-lettered work over an afternoon.

**Take the `__dlq` level out of the topic.** A dead letter carries the
job's own topic with one level inserted, so removing it is the way back.
**Where it sits depends on the queue's filter**, and this is the part to
get right rather than assume: a filter ending in `#` takes the level at
the `#`, and any other filter takes it on the end.

| Queue filter | A job on | Dead-lettered to |
|---|---|---|
| `jobs/#` | `jobs/j1` | `jobs/__dlq/j1` |
| `iot/+/work/+` | `iot/site/work/j1` | `iot/site/work/j1/__dlq` |

`saguin-dlq-channel` names the queue it came from, so a redriver holding
the configuration can look the filter up rather than guessing from the
topic.

**Publish the payload back to that topic**, carrying the publisher's own
properties - Content Type, Payload Format, and any User Property of its
own.

**The failure metadata cannot ride along, and that is the broker's job
rather than the redriver's.** `saguin-` is a reserved prefix on a publish:
Sagüin drops every property in it from any client, so a redriver that
copies the whole property set back without thinking still cannot produce a
job carrying `saguin-dlq-reason`. A publish setting one reaches its
consumer without it, whoever sent it.

**`saguin-id` is the one reserved name a publisher may set**, and passing
the dead letter's back is the whole reason to look at properties at all.

**What it costs, said rather than discovered.** The redriven job is a
**new record**: a new offset in the queue, and a fresh attempt count that
starts at 1. What survives is its identity - the broker keeps a
publisher's `saguin-id` (invariant 8), so a consumer can deduplicate across
the whole round trip and tell that this is the same work rather than new
work that looks like it. Pass nothing back and the job gets a new identity
and is indistinguishable from work that had never failed.

**And the hazard, out loud.** Redriving into a queue whose bug is not
fixed dead-letters the job again, with a second set of `saguin-dlq-*`
values and one more offset in both channels. Doing that in a loop is a way
to spend an afternoon and fill a disk.

### Restart

No record is in flight after a restart. Deliveries, Delivery IDs, and
deadlines are not restored: a restored deadline belongs to a session that
no longer exists, and a restored Delivery ID could be resolved by a client
retrying across the very restart that interrupted it.

Every previously held record is available immediately. Attempt counts
survive, so a record that had already exhausted its attempts is
dead-lettered rather than starting over.

## Client-declared partitioning

**A subscriber may take a slice of what its filter reaches, chosen by
itself, with per-topic ordering intact and no coordination between
members.** It declares one MQTT 5 User Property on its `SUBSCRIBE`:

```
saguin-filter   topic_hash(3, 0)
```

The broker delivers a message only when
`topic_hash(topic) mod partitions == index`, where `topic_hash` is FNV-1a-64
over the topic followed by the mixing step *The hash, in full* writes out.
**Declare nothing and you get everything**, which is every client that has
never heard of this.

**A member wanting several slices repeats `saguin-filter`, and the calls
are an OR.** MQTT allows a User Property to appear more than once, which is
more idiomatic than a comma-separated list. `topic_hash(8, 1)` beside
`topic_hash(8, 5)` is a member holding two slices of eight - a member
covering a failed peer's share, most often. Asking twice for one slice
means what it says and is not an error.

**Every call on one `SUBSCRIBE` must name the same number of partitions.**
`topic_hash(4, 0)` beside `topic_hash(6, 1)` is expressible and refused: a
subscription has one partition space, which is what the operations listener
reports and what the broker compares when it warns that two members
disagree about the size of a space.

**The arguments are positional, and the order cannot be got wrong
silently.** An index must be below the partition count, so if
`topic_hash(8, 1)` is legal then `topic_hash(1, 8)` is not - which holds
for every valid pair, so a client that swaps them is refused rather than
served a slice it did not ask for.

**The property is on the packet, not on a filter**, as `saguin-deletions`
is, so a declaration applies to every filter named; different slices for
different filters are different `SUBSCRIBE` packets.

**MQTT 3.1.1 has no User Properties**, so a 3.1.1 subscriber cannot declare
a slice and always receives everything its filter reaches. That is a
protocol limit rather than a policy, and it is said here rather than left
to be discovered.

### Where it applies

| | |
|---|---|
| broadcast | allowed, on live traffic and on the retained pass a `broker.retained` store hands a new subscriber |
| `append` | allowed, on the replay from a stored position as well as on live records |
| `latest` | allowed, on the pass of current state as well as on later changes |
| `$share/…` | **refused** - a shared subscription already divides a stream between its members |
| `$saguin/queue/<name>` | **refused** - a queue already divides work between its workers |

Both refusals are `0x83`, with the reason naming which rule was broken.
They are decided by the two prefixes as strings: nothing here asks which
channels a filter reaches, because that question is the one *What a filter
reaches* exists to avoid asking at subscribe time.

**`latest` takes the predicate on both halves or neither.** A member sent
the whole of current state on subscribe and then only its share of the
changes holds a copy of the key space that starts complete and drifts,
which reads as correct on the day it is set up and as loss a week later.

**The same rule reaches broadcast, and for the same reason.** A configured
`broker.retained` store hands a new subscriber the current value of every
topic its filter reaches, which is a current-state pass however different
it looks from a channel's - so it takes the predicate too. Broadcast needs
no channel, no storage and no position, and the predicate still has to
reach that one handover: a group whose members each received every retained
topic is state processed once per member rather than once per group.

**A declaration applies to every filter in its SUBSCRIBE, and to no
subscription outside it.** A client holding a declared filter and an
overlapping undeclared one is served the union: the undeclared
subscription is owed everything it matches, and a record reaching this
client through either is delivered once. What a declaration narrows is the
subscription it was made on, never the client.

### The hash, in full

The hash is **FNV-1a, 64-bit, over the topic's bytes, followed by a
mixing step**, and it is a constant of the protocol rather than an
implementation detail: a client that wants to know which of its topics land
in its slice must compute the same value. Both halves are written out here
so that they can be reimplemented from this document alone.

```
offset basis = 14695981039346656037
prime        = 1099511628211

hash = offset basis
for each byte b of the topic, in order:
    hash = hash XOR b
    hash = hash * prime          (modulo 2^64)
```

The bytes are the topic's UTF-8 bytes, not its characters, and the
multiplication wraps at 64 bits.

Then the mixing step, on that value, with the same wrapping:

```
hash = hash XOR (hash >> 30)
hash = hash * 13787848793156543929     (modulo 2^64)
hash = hash XOR (hash >> 27)
hash = hash * 10723151780598845931     (modulo 2^64)
hash = hash XOR (hash >> 31)
```

`>>` is an unsigned shift right. The slice is taken from the value after
this step, never from the value before it.

**Both halves are given in the worked examples below**, so that an
implementation that disagrees can tell which of the two is wrong. An
implementation that stops after FNV-1a agrees with the *FNV-1a* column and
with nothing else.

| Topic | FNV-1a-64 | after mixing | `mod 3` | `mod 8` |
|---|---|---|---|---|
| `iot/depot/events/dev-1` | 9321193355118713112 | 16961261177379703000 | 1 | 0 |
| `iot/water/w-7/inspect` | 14147658934179859562 | 16406880391913018636 | 2 | 4 |
| `orders/resize/thumbnails/42` | 15020795679369718730 | 10596179872049748585 | 0 | 1 |
| `a` | 12638187200555641996 | 198367012849983736 | 1 | 0 |

**Why the mixing step is there, and why it is not optional.** FNV-1a stirs
the top of its accumulator thoroughly and the bottom hardly at all, and the
bottom is the half a modulus reads. Flip one bit of a topic and FNV-1a's
lowest bit changes 12.5% of the time where an even split needs 50%; the
next three change 19%, 25% and 31%. That is not noise that averages out.
Because the prime is odd, each byte either flips the lowest bit or does
not, so a byte appearing twice cancels itself out - and a topic scheme that
carries an identifier twice, such as
`devices/<id>/messages/devicebound/<id>`, sends every identifier to the
same low bits. Four members splitting 4000 such topics receive
2454, 0, 1546 and 0 of them: two members are sent nothing at all, at any
scale, and *Coverage is the client's responsibility* means nothing reports
it. With the mixing step the same 4000 split 1005, 996, 1011, 988.

**The step is the ordinary shape of a hash rather than a repair to this
one.** Every modern hash is a mixing loop followed by a finalizer; murmur3
and xxhash both end this way, and FNV-1a is the unusual one for stopping
early. The two constants are splitmix64's, and after the step every output
bit lands within half a point of the ideal 50%.

**Why FNV-1a and not something else**: xxhash needs a library in every
client language, murmur3 has incompatible variants people get wrong, and
SHA-256 truncated - the honest runner-up - is around twenty times the
work per topic for a distribution these ten lines already reach.

**Explicitly not a randomly seeded hash** such as Go's `maphash`: it is
faster than all of them and reshuffles every assignment on a restart, so a
client could never predict its own slice.

### What a subscriber may declare

A `saguin-filter` value is a call, and `topic_hash` is the only function
Sagüin reads:

```
topic_hash(<partitions>, <index>)
```

| | |
|---|---|
| `<partitions>` | 1 to 2147483647, and the same on every call of one `SUBSCRIBE` |
| `<index>` | 0 to `partitions - 1` |
| the property | repeated for several slices, which are an OR |

**An argument is one or more decimal digits**, `0` to `9` and nothing
else - no sign, no `0x`, no exponent - so `topic_hash(+3, 0)` is refused
for its spelling, and leading zeros mean what they say. **Whitespace is
the ASCII four alone**, ignored around the name, parentheses and comma; a
non-breaking space is not a space, stated because a value one broker
trims and another does not is a value the two answer differently.

Nothing else in the value is ignored: the spelling is exact, and
`TOPIC_HASH` is not this function for the same reason `$SHARE` is not a
shared subscription.

Anything else is `0x83` with the reason naming the property and stating the
form: a value that is not a call, a function Sagüin does not have, the
wrong number of arguments, an argument that is not decimal digits, an
argument above the bound, a partition count of 0, an index at or above the
count, or two calls naming different counts. **An argument that is not a
number and one that is too large get different sentences**, because they
send a reader looking in different places: told that 10^23 is "not a
number", somebody goes hunting for a stray character that is not there.

**The sentence travels on the SUBACK, as its Reason String**, rather than
only in the broker's log: the person who has to act on it is the author of
the refused client, who cannot read the log. MQTT's own rule is the one
exception - a client that connected with Request Problem Information 0 has
asked for no such text and is sent the reason code alone.

An empty value is refused rather than ignored - a client that asked for a
slice and was silently served the whole channel would have no way to
notice. A count of 1 is legal and degenerate - one slice, holding
everything.

**There is no operator syntax.** A client writes a function name and its
arguments, so `$topic_hash % 3 == 0` and every other expression is refused
rather than read. A name and two arguments admit exactly what the broker
implements, and a further predicate is a further function name rather than
a larger language.

**The upper bound is 2147483647 because that is a number every build
agrees on**: Sagüin runs on 32-bit gateways as well as 64-bit servers,
and a threshold taken from the platform's integer would make the same
`SUBSCRIBE` legal on one and refused on the other.

### What this does not build, and what that costs

There is no group object, no membership, no generation ids, no rebalancing
and no group cursor. Each member stays an ordinary session keeping its own
position, exactly as it does without a declaration.

**Coverage is the client's responsibility, and the broker cannot police
it.** Nothing stops one member declaring count 3 index 0 while another
declares count 2 index 0. A slice nobody claims is records nobody receives,
and every member looks healthy. The broker does not report a gap, because
it cannot tell one from a member that has not connected yet - any gauge
claiming to would be wrong during every rolling restart.

**What it does report is two subscribers on one filter declaring different
counts.** That is not a timing artefact and cannot be correct, so it is a
log line naming both clients. It is scoped to the identical filter string,
so it misses overlapping-but-different filters - `iot/+/events` against
`iot/site-a/events` - and that miss is deliberate: where the filters differ
the counts may differ on purpose.

**A member's position advances past records it never received.** The
position is one cursor into the channel, so a record outside a member's
slice is stepped over exactly as a record matching none of its filters is,
and the cursor moves past it. Two consequences follow and neither is a
defect:

- **Widening a declaration later does not recover what was skipped.** Those
  records are behind the consumer's position. A member that takes a larger
  slice tomorrow receives more of what arrives from tomorrow, and nothing
  of what it passed over yesterday.
- **A slice nobody claims is not held anywhere.** Those records age out
  under retention like any other, on the channel's own schedule.

That is the cost of having no group cursor, and it is the trade this design
makes deliberately: the 80% of a consumer-group protocol that is not paid
for.

**A declaration lasts as long as the session that made it**, and survives a
reconnect that resumes one. MQTT restores a resumed session's subscriptions
but not the `SUBSCRIBE` that made them, so a member that had to re-declare
would be served the whole channel - every other member's slice included -
between resuming and subscribing again.

**That is the opposite of the choice a `latest` channel's deletions make,
and the asymmetry is which way each fails.** A subscriber that must ask for
deletions and has not is served *less* than it wanted: it misses them.
A member that must re-declare and has not is served *more*: the whole
channel, including every slice belonging to somebody else. One degrades
safely and the other duplicates records across a group, so they are not
made to behave the same way for symmetry's sake.

A session that is *discarded* rather than resumed takes its declarations
with it, so a declaration never outlives the session it was made on.

## Ordering

| | Guaranteed |
|---|---|
| `append` | total order by offset; each subscriber receives records in that order |
| `latest` | per topic. No ordering between different topics |
| `queue` | records are *offered* in offset order |
| broadcast | per subscriber, for what that subscriber receives |

For a queue, offered order is not completion order and cannot be. With two
workers, or one redelivery, this is normal:

```
record A ─▶ worker 1
record B ─▶ worker 2
A times out
record A ─▶ worker 2      ← after B is already done
```

An application needing strict ordering needs one worker, and even then a
redelivery reorders against the records that passed it. `append` is the
primitive with an ordering guarantee.

## What Sagüin does not guarantee

Stated together, because each is a question someone will otherwise have to
find out by being surprised:

- **Exactly-once processing.** At-least-once, always. Consumers must be
  idempotent.
- **Duplicate suppression at publish.** Two publishes with the same
  `saguin-id` become two records in v0.1.
- **That an acknowledgement was applied.** The worker is not told.
- **Completion order on a queue.** See above.
- **That a retained message on a broadcast topic is kept without bound.**
  The retained store keeps it inside its provider's `max_bytes`, and the
  value dies at the publisher's expiry or the operator's
  `retention_period`, whichever runs out first ("Retained messages"
  below).
- **That a broadcast reached anyone.** A broadcast is delivered to whoever
  is subscribed at that moment, and stored nowhere unless it asked to be
  retained. `PUBACK` says the broker accepted the packet - at QoS 1 or 2,
  that every session owed it has kept it ("Broadcast") - and nothing about
  who received it, so a publish to a topic nobody holds is `0x00` -
  including a channel name with a typo in it, which is an ordinary broadcast
  topic as far as the broker can tell.
- **That memory-backed data survives a crash.** Only a graceful shutdown,
  and only as far as the last successful snapshot.
- **That a Will the broker accepted can still be delivered when it fires.**
  A Will whose topic could never work is refused at CONNECT, where the
  device is there to be told. What cannot be known until it fires - a full
  channel, a storage failure - is logged and no more, because the client is
  gone by then and there is nobody to answer.

## Retained messages

**The retain flag asks for a store, and Sagüin honours it wherever it has
one.** There are two stores, and the topic decides which of them applies or
whether neither does:

- **A topic an `append` or `latest` channel claims.** The channel is the
  store. On a `latest` channel the flag is redundant - that channel holds
  the current value of every topic beneath it whether or not anyone set it -
  and on an `append` channel the record is stored as any other and
  replayed to whoever reads the channel afterwards. Nothing is refused and
  nothing extra happens.
- **A topic a `queue` claims: accepted, and the flag dropped.** A queue
  holds work rather than state, and nothing on one could ever be sent a
  retained value - so the flag is the only part of the publish that
  cannot be honoured, the message becomes a job, and the `PUBACK` says
  the work was stored, which is true. `saguin_queue_retain_ignored_total`
  counts it, nothing on the wire saying a flag went missing.
- **A broadcast topic.** The retained store holds it (RFC 0002 "Retained
  messages on a broadcast topic"). One current value per topic, a
  zero-length payload deleting the entry, handed to whoever subscribes
  next with the RETAIN flag set, and delivered live to whoever is
  subscribed at the time as well, because it is still a broadcast. A value
  sent on subscribe at QoS 1 or 2 to a session that outlives its
  connection is kept in the broadcast log for that session alone, as a
  copy no other subscription or group is owed, so a restart sends it
  again under its identifier, with the RETAIN flag, until it is
  acknowledged (MQTT-4.4.0-1).

**A bridge forwards the live fan-out and never a stored pass.** An outbound
bridge rule carries what is published while it is running; it is not sent
the retained values a subscribe earns, nor a `latest` channel's current
state. Two reasons, and either alone would settle it. A stored pass is
served on every subscribe, so a bridge that took one would ship the whole
of current state again at each reconnect - deterministic duplication at the
far end, growing with every link drop. And a value a bridge itself brought
in is still in that store, so forwarding one would hand a peer its own
retained set back, once per restart, for ever. The record keeps the bridge
it arrived on for that second reason, in the retained store as everywhere
else.

**A link that drops is not a rule that stopped.** An outbound rule runs
beside its link rather than inside a connection, so it goes on taking a
`latest` channel's changes while the link is down, and each topic's newest
crosses when the link comes back - a newer value replacing one still
waiting, as it does for any subscriber. What a peer is not sent is what was
published before the rule started, which is the same answer a broadcast
topic gets, **and a value the channel's own retention removed while it
waited**: a topic that went quiet for longer than `retention_period`, or
a deletion older than `deletion_retention_period`, is let go by the rule
as the channel lets it go, and never crosses. A stale reading is worse than
none at the far end as it is here ("What a reconnecting consumer receives").

**That mark is what stops the echo, and it carries the weight because the
flag crosses.** A retained value arriving over a link is kept at this end,
and a `direction: both` rule reads what arrives - so the only thing between
a retained value and a pair of brokers handing it back and forth is the rule
that a record which came in over a bridge is never sent out over one. It is
a blanket rather than a comparison against the bridge's own name, because a
name permits a second hop and a ring of brokers would each see a name that
is not theirs. What an operator does instead is re-origination: a broker
meant to relay publishes the record again as its own.

**The RETAIN flag crosses as its publisher set it, in both directions.** A
record published retained here is carried to the peer retained, so the peer
keeps it and a subscriber arriving there afterwards is served it. Inbound is
the same: the bridge subscribes at the peer asking for Retain As Published,
because MQTT puts the flag down on a live delivery otherwise
[MQTT-3.3.1-12], and a value that reached the bridge looking ordinary would
be stored here as ordinary.

That is the crossing publish only. The paragraph above still holds whole: a
bridge ships what is published while it runs and never a stored pass, so a
link restart re-ships no stored set and neither end's retained set is handed
back to the other. What a link restart *can* repeat is an unacknowledged
record the upstream re-sends at session resume, which MQTT requires of it;
RFC 0002 "Bridges" has what the bridge does about that and where it stops.

**Retained-ness therefore extends one hop**, which is what "publish
retained, and whoever subscribes later sees current state" means across a
link, and it is what mosquitto's bridge already does - so a topology moved
from mosquitto behaves as it did. A `latest` channel at the receiving Sagüin
remains the stronger form of the same promise: current state per topic,
durably, bounded, and readable by a point read rather than only on
subscribe.

**Where it does not extend, and this is the degradation to know.** MQTT 5
lets a broker advertise that it does not accept retained messages, and a
peer that does is sent the value with the flag down: its subscribers receive
it live and a subscriber arriving there later finds nothing. The alternative
is worse and is not offered - a retained publish to such a peer is refused,
and a bridge that carried the flag regardless would hold that record, retry
it on every reconnect, and never move again. So the value crosses and only
the retained promise is lost. Sagüin says so once per link in its log rather
than once per record. Where the peer can advertise the limit (MQTT 3.1.1
has no way to), Sagüin honours it without being configured to.

**A value the receiving broker will not keep is refused as any publisher's
would be.** A bridge has no route of its own into a retained store: a
crossing record takes the ordinary publish path and meets the ordinary
answers - the store's bounds, the `acl_file`, a zero-length payload as a
deletion.

**On a broadcast topic the rules are MQTT's exactly, with nothing added.**
That is the whole point of it: an unmodified Home Assistant, Zigbee2MQTT or
piece of firmware works, and anything Sagüin invented here would be
something to discover the hard way. So, in the six places a broker is
tempted to differ:

- **The `SUBACK` is written first, then whatever the subscription
  earns.** MQTT permits either order (§3.8.4), so this is a choice rather
  than a rule, and Sagüin makes the one every client library is written
  against: a client that reads one packet expecting its acknowledgement
  gets one. It holds for everything a SUBSCRIBE brings back, a channel's
  replay and a `latest` channel's current state along with the stored
  values here. The subscription itself is recorded before the `SUBACK`,
  so a message published in the gap is delivered rather than missed.
- **Delivery happens at SUBSCRIBE, not at connect.** A client that resumes
  a session and does not subscribe again is sent nothing, exactly as a
  standard broker does. The difference from a `latest` channel is
  deliberate and is described there.
- **Retain Handling is honoured**: send the stored values at subscribe,
  send them only if the subscription did not already exist, or send none.
  A client asking for none and receiving them anyway is a broker breaking
  its own word.
- **Nothing is sent to a shared subscription**, which is what MQTT says of
  retained messages, and Sagüin adds no exception. A shared subscriber is
  served what is published while it is there and nothing that was stored
  before it arrived.
- **The RETAIN flag arrives as MQTT says it should.** A subscriber served
  the stored value at SUBSCRIBE sees it set. A subscriber that was already
  there sees it cleared on the live delivery, because that is an update
  rather than state - unless it asked for Retain As Published, which is a
  client saying "tell me what the publisher set", and then it sees it set.
  Sagüin's own store is still the only one there is: nothing is left in the
  substrate's.
- **The publisher's Message Expiry Interval discards the stored value**
  (MQTT-3.3.2-5), so the value dies at the earlier of that interval and
  the operator's `retention_period`, each clock deleting on its own. A
  subscriber arriving past the expiry is not served it, even before the
  sweep has reclaimed its bytes, and one arriving inside it is served the
  remaining countdown - the publisher's, decremented by the wait, and
  never the operator's period, which no client hears about. A value whose
  publisher set no expiry carries no countdown and waits on the
  operator's clock alone. Both deletions are silent, as retention is
  everywhere. This is the one MQTT rule that stops at the channel
  boundary: on a channel the record outlives its expiry, and only the
  countdown says so - the split is under "From publish to record".

A value goes back out as it was published, carrying no `saguin-id`, no
`saguin-offset` and no `saguin-timestamp`. So "how old is this state" is
not answerable here and is answerable on a `latest` channel, which is one
of the reasons to name a channel for a prefix that matters.

**A client whose roles deny `retained` has a retained publish to a
broadcast topic refused with a `DISCONNECT` carrying `0x9A` (Retain not
supported)** (RFC 0002 "Taking a feature away"). It is a disconnect rather
than a reason code because MQTT makes it a protocol error: `0x9A` is not
among the codes a `PUBACK` may carry, and at QoS 0 there is no `PUBACK` at
all. The alternative - dropping the flag and acknowledging - tells a
producer its state was kept when nothing kept it, and that is the failure
this whole document exists to prevent. The denial is about broadcast topics
only: a retained publish to a channel's topic is taken as it always is, and
that client's `CONNACK` still says Retain Available 1.

**What MQTT's own retained store lacks, both of Sagüin's have.** A broker's
retained store is conventionally broker-wide, unbounded, without retention
and without configuration, which is what invariant 13 refuses. Sagüin's
sits on a named provider, is bounded by that provider's `max_bytes`,
refuses rather than evicts at the bound, survives a restart to whatever
degree its provider promises, and can be read with `sqlite3` while the
broker runs. That is the whole of the difference.

**Which of the two you want.** A `latest` channel is the answer when the
topics are a fleet's state and you want them bounded, replayable as current
state, and named in the configuration file. The retained store is the
answer when a client insists on the flag and you would rather it worked
than have to name every prefix it uses - a device tree published under
`homeassistant/…` or `zigbee2mqtt/…` works either way, and works with no
Sagüin-specific configuration at all.

**The `CONNACK` carries Retain Available 1** to every client, a client
denied `retained` included, because MQTT has one answer for the whole
broker and no way to say "on these prefixes".

## Last Will

**A Will is delivered through the ordinary publish path**, so it may target
broadcast or any channel type and gets exactly what a publish gets: the
reserved `saguin-` prefix stripped from its headers, a Message ID and an
offset, storage in the channel that claims its topic, and every bound
applied.

**Including the publish properties it carried.** A Will may set a Content
Type, a Payload Format Indicator, a Message Expiry Interval, a Response
Topic and Correlation Data, and every one of them is delivered with it -
alongside its User Properties, and on a channel stored with the record like
any other publish's. That is the same list "From publish to record" gives
for an ordinary publish, which is the point: a Will is a publish whose
sender has gone, and a monitor reading one should not have to know it was a
Will to know what the bytes are or where to answer. A Will into a `queue`
is an ordinary job with a Delivery ID and a Response Topic, which is a
reasonable thing to want when a device dies.

That is not how the substrate publishes one - `sendLWT` calls
`publishToSubscribers` directly and never `OnPublish` - so a Will left to it
reaches a channel's live subscribers while being stored nowhere, hands a
queue worker something no timeout will ever reclaim, and skips the bounds
entirely. Routing it is what makes a Will an ordinary record instead of
one more special case.

### What is refused, and when

**At CONNECT, with `CONNACK 0x90`, for a Will that could never be
delivered:** a topic in the reserved `$saguin/` space, a topic in a
dead-letter channel - which takes records from its queue and from nothing
else - a topic longer than `max_topic_length` or deeper than
`max_topic_levels`, which nothing else bounds, since a Will topic arrives in
CONNECT and is not checked again until it fires, and **a topic name nothing
may publish to at all**: one holding `+` or `#`, one in the `$SYS` tree the
broker keeps about itself, or one beginning `$share/`, which is a
subscription form (RFC 0002 "Publishing").

That last one is asked here because the answer is worthless anywhere else.
A wildcard in a topic name is refused a publisher on the wire by the
protocol itself, but a Will arrives inside CONNECT where no such check
runs - and a Will is the one thing a client can leave behind that outlives
it. Stored, it would sit at the head of the channel for ever: MQTT forbids
a wildcard in a *delivered* topic name as firmly as in a published one, so
every conforming consumer stops on reaching it, and on a `latest` channel
no client could remove it, because deleting a value means publishing an
empty payload to its topic and that publish is refused too.

Refusing costs the connection, MQTT requiring a server to close after a
non-zero CONNACK - **and that is the point**: each of these is permanent
for that client's configuration, and the alternative is a device that
connects happily for months while its fleet monitoring was never armed.

**At CONNECT, with `CONNACK 0x87`, for a Will on a topic its client's
roles may not publish to** (RFC 0002 "What a client may do"). A Will is a
publish the client asks the broker to make for it, so it meets the rules
the client's own `PUBLISH` meets. Sagüin publishes it as itself, which no
rule is asked about, so without this a device whose role only reads a
`latest` channel could overwrite that channel's state by dropping its link.

**A retained Will follows the retained rule and nothing else.** It is
admitted wherever a retained publish to the same topic would be - an
`append` or `latest` channel, a queue where the flag is dropped, or a
broadcast topic - and refused at CONNECT with `0x9A` where a publish is
refused: a broadcast topic from a client whose roles deny `retained`. That
it is the same rule is the point of it, since a device that may publish a
message all day should not be turned away at CONNECT for promising to send
the same one. A retained Will aimed at a queue is still the availability
pattern aimed at the wrong channel type - a device means "tell a dashboard
I died", and one worker takes it once and is done - but that is the
operator's `filter` saying the topic is work, and refusing a connection is
not how they find out. A retained Will into a `latest` channel is the
standard availability pattern, a retained `online` and a retained Will
publishing `offline`, and it works.

**One question is asked again when the Will fires: may its client still
publish there.** The `acl_file` can be reloaded between a `CONNECT` and a
Will that fires days later, so the Will is judged as the client that armed
it - its client id and the name it authenticated as, which its session
keeps beside it - against the rules as they stand. A Will refused then is
not published: it is logged and counted in `saguin_publish_refused_total`
as `not authorized`, and not in `saguin_wills_published_total`. Nothing
else is refused when the Will fires. A full channel or a storage failure
is logged and no more: the client is gone by then, so there is nobody to
answer, which is exactly why everything that can be judged at CONNECT is
judged there.

### The delay is Sagüin's, not the substrate's

A client may ask for its Will to wait - a Will Delay Interval - so that a
link dropping for four seconds does not announce a device that is still
there. **Sagüin holds that Will itself**, delivers it when the delay
expires, and cancels it when the same client id resumes the session - a
connection as "Sessions" defines one, never a CONNECT that was refused. A
Clean Start ends the session instead, which publishes it. A Will with no
delay is published as its connection ends, a takeover included.

**The wait ends when the session does, whichever comes first**, which is
MQTT's own rule (5.0 section 3.1.3.2.2) and is two cases rather than one. A
session that ends with its connection - Clean Start with no Session Expiry
Interval, or a 3.1.1 client with Clean Session 1 - ends at the disconnect, so
its Will is due then however long its interval says: holding it would be
waiting to announce the death of a session the broker has already thrown
away, and a device asking for five minutes would be announced five minutes
after nothing was left to announce. A session shorter than the interval ends
first too, and what counts is the interval the session was **granted**: a
client asking for an hour on a broker whose `limits.max_session_expiry` is a
minute has a minute of session, and its Will is due at the end of it.

It is held by Sagüin, and the substrate holds no Will for later, so a
delayed Will is published through the ordinary publish path when it falls
due, under every rule above - and a delay is precisely what a flapping
link makes an operator configure.

**A delay needs a session to wait inside, and that is the recipe.** A device
that wants flap protection asks for a Session Expiry Interval at least as long
as its Will Delay Interval; one that asks for a delay on a session ending with
its connection is asking for a wait its own session will not allow, and its
Will is published at the disconnect. Sagüin says so in its log when it
happens, naming the client, the interval it asked for and the fix, because
nothing on the wire can carry that answer and the device is gone by the time
it would matter.

**The wait never outlives the session.** Sagüin publishes the Will at the
disconnect where the session ends with it, and caps the wait at a real
session expiry. A Will outliving its session is the one thing Sagüin's
storage rule forbids, because a Will belongs to a session, is discarded when
it ends, and therefore needs no home, bound or restart rule of its own. A
fleet migrating from a broker that waits out the whole interval, with Clean
Start and a delay, will see its announcements arrive at the disconnect
rather than after the interval; the log line above is where that shows.

**What the wait costs while it runs** is one small record per waiting client:
a client id, the moment the Will is due, and a timer. The Will itself is on
the session record, in the provider `broker.session.storage` names, where the
CONNECT that armed it wrote it - so a waiting Will is held once rather than
twice, and `saguin_wills_waiting` is how many are held (RFC 0005).

**A restart does not lose it.** The moment a Will becomes due is written
down when the wait begins, so a broker that comes back inside the wait
resumes what is left of it, and one that comes back after the moment has
passed publishes then - late by however long the outage was, which is the
honest cost of the outage rather than a lost announcement. A start ends
sessions as well as restoring them - one whose expiry passed while the broker
was stopped, one that ended with a connection the stop cut, one holding a
subscription the current rules refuse - and where the Will it holds carries
a moment, ending the session is that Will falling due: it is published
before its record goes. **So a Will is published at least once across a
crash, and can be published twice**: a Will that falls due is published
before the write that takes it off its record - when its delay runs out,
when its session expires, or at a start - so a broker that dies between
the two leaves the Will on its record, where the next start can find it
owed again and publish it again. The other order would lose it instead. A
Will with no moment on it is the next paragraph's case, and the two outage
cases below are what it costs.

**A client that was connected when the broker stopped keeps its Will**, and
that Will has no moment on it, because nothing has happened yet to make one: a
broker stopping is not a device dying. It waits on the restored session for
whichever comes first. The client returns, and its `CONNECT` replaces the
Will. Or the session expires at the running broker - which is that broker
watching the client stay away for the whole interval it was granted - and the
Will is published then. Stripping it at the start was the other option and it
is the worse one: every restart would quietly take Will protection away from
every connected device until each happened to reconnect.

**A clean `DISCONNECT` withdraws the Will from the session's record**, with
the expiry it carried, before the connection that answers it is closed
(invariant 18), so a crash once the client has seen its connection close
publishes no Will it withdrew. Where the store refuses that write the broker
holds the withdrawal itself and writes it again every second until it lands,
and as it stops - so only a crash while the store is still refusing leaves the
record reading as a connected client's, whose Will is then published when the
session expires. A `CONNECT` for that client id writes it first, and is
refused `0x83` while the store refuses it: the session it would claim would
read as though its client had never said goodbye.

**What an outage costs, stated plainly, because an operator will meet it.**
Two cases are announced by nobody, and both have the same shape - the broker
was not running to see the connection end.

- **A device with a session that ends with its connection**, Clean Start with
  no expiry. Its session is ended at the next start, and its Will goes with
  it: there is no session left for the announcement to belong to.
- **A device whose session expiry passed while the broker was stopped.** That
  session is ended at the next start too, and its Will carries no moment, so
  nothing says it was due rather than a client that simply stayed away.

Between them sits the case that *is* covered: a device that goes away, whose
granted session outlasts the outage, is announced when that session expires -
late by the outage, which is the honest cost rather than a lost announcement.
A fleet that needs deaths noticed through an outage of any length needs
something outside MQTT watching it, because a Will is a message a broker sends
when it sees a connection end, and a broker that is not running sees nothing.
