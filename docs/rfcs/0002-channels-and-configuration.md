# RFC 0002 - Channels and configuration

**Status:** Draft
**Authoritative on:** how a channel claims the topic space, which
subscriptions and publications the broker admits, and the configuration
file.

## The topic space

**A channel claims the topics its filter matches.** The filter is an
ordinary MQTT topic filter and it is written on the channel:

```yaml
channels:
  water-location:
    type: latest
    filter: iot/water/+/location/#
  water-measurement:
    type: append
    filter: iot/water/+/+/#
  weather-measurement:
    type: append
    filter: iot/weather/+/{data,events}
```

```
iot/water/w-7/location        water-location        latest
iot/water/w-7/location/gps    water-location        latest
iot/water/w-7/flow            water-measurement     append
iot/water/w-7/flow/raw        water-measurement     append
iot/weather/st-3/data         weather-measurement   append
iot/weather/st-3/data/raw     broadcast - that filter carries no #
iot/weather/st-3/status       broadcast - no filter matches it
```

Every topic no filter matches is broadcast (RFC 0001). There is no
configuration for broadcast and no way to disable it: it is the behaviour
of a topic no channel claims.

**A filter matches exactly what MQTT says it matches**, and nothing in
this document changes that. `+` stands for one level. `#` is the last
level only, and stands in for nothing as well as for everything - which is
why `iot/water/+/location/#` takes the bare `iot/water/w-7/location`. A
filter of N levels carrying no `#` matches topics of N levels and no
others, which is what puts `iot/weather/st-3/data/raw` in the broadcast
row above and is worth reading twice, because it is the one line of the
table an operator writes by accident.

**A channel that writes no `filter:` gets `<name>/#`**, so `events` takes
`events/orders/123`. Writing a filter replaces that entirely - the name
then claims no topic at all and is identity only, which is what the next
section is about.

### `{a,b}`: one level, several spellings

A level may be written as a comma-separated list in braces, and it is
expanded at startup into plain filters. `iot/weather/+/{data,events}` is
`iot/weather/+/data` and `iot/weather/+/events`, and nothing past the
loader ever sees a brace.

It exists so that a channel wanting two spellings of one level is still
one `filter:` line and not a list - a list has an order, and the rule two
sections down exists so that no order can matter.

The rules are short. Braces hold no `/`, so an alternative is one level
and never a subtree. An empty brace, or an empty alternative, is refused.
Two braced levels in one filter expand as every combination.
`saguin --route` prints what each filter expanded to, in the routing table
it ends with, so an operator reads the real list rather than counting it in
their head. `--check-config --output` does not: it prints a configuration
file, and a channel takes one `filter:` there, so the written form is the
only form that round-trips.

**There are no regular expressions**, and the reason is not taste. The
broker has to answer one question about a filter before it opens a
listener - does another channel already claim what this matches - and for
a regex that question is undecidable. A filter language costs an operator
almost nothing and moves that answer from "refused at startup" to "found
out in production".

### Channel names

A channel name:

- holds only `a`–`z`, `A`–`Z`, `0`–`9`, `-`, `_` and `.` - the
  characters a Kafka topic holds;
- is not `.` or `..`;
- is non-empty and at most 128 bytes;
- does not end with `__dlq`, which is reserved (RFC 0001).

**A name therefore holds no `/` and is one topic level**, and it is
identity rather than placement. The name is what a snapshot file, a
storage key, an ACL subject, a metric label and the `$saguin/` control
topics are built from - `$saguin/consumer/<name>/seek`,
`$saguin/queue/<name>/response`, and the `__dlq` a queue derives for its
dead letters. **Where a channel's records sit in the topic space is its
`filter:`, and the two are deliberately different things**: a channel
whose data lives four levels down still has one flat name to seek on.

This is an allow-list rather than a list of the characters that break
something, and the two are not the same size. Refusing `+`, `#`, NUL and a
leading `$` covers what MQTT reserves and nothing else, and every rule that
follows from a name then has to be re-derived for whatever is left. A `/`
is the case that shows why: it would make a name hierarchical, which makes
it a prefix another name could overlap, a file name that needs encoding, a
name inside this 128-byte limit still too long to store, and
`$saguin/consumer/site/events/seek` unreadable as a seek. An allow-list
answers all of that once.

`.` and `..` are inside the character set and refused anyway. Both name a
directory rather than a channel in every listing that will ever hold one,
including the snapshot directory where a channel's name becomes a file.

The 128-byte cap is a constant in the broker, not a configuration key.
A channel name is written by hand by the operator, once, in a file the
broker already trusts - it is not client input, so the limit protects
nothing and tunes nothing. Exposing it would add a knob whose only
reachable settings are "the default" and "wrong".

`max_topic_length` is exposed precisely because the opposite is true of
it: a topic arrives from an untrusted client on every publish, and an
operator on constrained hardware has a real reason to lower it.

`max_client_id_length` defaults to 256. A durable consumer's stored
position is keyed by its client id, so an unbounded id is durable state
whose size the client chooses; a longer one is refused at CONNECT with
`0x85 (Client identifier not valid)`, before a session exists. MQTT
requires a server to accept any id up to 23 bytes, so a configured value
below 23 is refused at startup.

`max_session_expiry` defaults to **30d** and stops a client choosing how
*long* a stored position lives, as the bound above stops it choosing how
large one is: MQTT's own ceiling is about 136 years, so a fleet taking a
fresh client id every boot would otherwise leave one row per boot, per
channel, that nothing an operator sets removes.

MQTT supplies the whole mechanism, so this is a cap and never a refusal:
the server shortens what the client asked for and the CONNACK carries the
shortened value, so a conforming client is told what it got. A client
asking for less than the cap is given what it asked for.

**A DISCONNECT is held to it too.** MQTT 5 lets a client change its
Session Expiry Interval as it leaves (section 3.14.2.2.2), except from 0 to
anything else, which is a protocol error. The value it sends is the one the
session is kept by from then on - across a restart as well - and one above
the cap is kept at the cap. A DISCONNECT has no answer to say so in, so the
client is not told, as it would not be told of a session ended early for
any other reason of the server's.

Thirty days because too short is the expensive direction: these
deployments go offline for weeks at a time, and a consumer whose session
expired resumes from the retention floor instead of where it stopped. A
long cap never costs correctness (invariant 1), but it does cost memory:
the broker holds a session whose client is away for the whole of its
expiry, a few kilobytes and more for each filter ("Every session's state:
`broker.session`" has the figures), and nothing else ends one that is
never resumed. A memory provider's `max_bytes` bounds how many are held.
A sqlite provider's `max_bytes` bounds its file instead, so on sqlite - and
on a memory provider with no `max_bytes` - how many are held is bounded by
this cap and the rate at which new client ids arrive: a fleet taking a fresh
id every boot holds every boot's session for thirty days.

`none` is accepted by the retention keys and refused here, because a
session that never expires is the unbounded table the key exists to close.

### Which channel a topic belongs to

Filters overlap, and that is the point rather than a mistake to refuse. A
domain holds a location, a reading and a command at once, and saying so
takes two filters that both match some of the same topics:

```
water-location      iot/water/+/location/#
water-measurement   iot/water/+/+/#

iot/water/w-7/location    matches both
```

**The filter that spells the topic out most exactly holds it.** Compare
the two filters level by level from the left. The first level at which
they differ decides, and at that level:

| | |
|---|---|
| a spelled-out level | beats `+`, and beats `#` |
| `+` | beats `#` |
| a filter that ends here | beats one continuing with `#` |

At level 3 above, `location` beats `+`, so `iot/water/w-7/location` is a
`latest` value; `iot/water/w-7/flow` matches only the second filter and is
an append record. Nothing further needs stating, because the rule reads
off the two lines the operator wrote.

**The order of the file has no effect on any of it.** Sorting `channels:`
alphabetically, or moving half of them into a file named by `!include`,
cannot change where one topic lands. That is deliberate, and it is why
placement is a key on the channel rather than a list somewhere else: a
list has an order, an order in a file is a thing the next reader changes
without meaning to, and routing that moves when somebody tidies a file is
exactly the silent class of defect this broker is built to refuse.

**Two channels may not carry the same filter**, and that is a startup
error naming both - the only tie the rule can be handed, since two
filters equally exact at every level of one topic are character for
character identical. Resolving by file order gives an order nobody can
see once `channels:` is split across files, and refusing overlap outright
refuses the three-line example at the top of this document.

### Changing a filter

**Records stay where they landed.** Editing a filter changes where new
records go and moves nothing already stored: a record written under
yesterday's filter is served from the channel it is in. Somebody will
change a filter and go looking for yesterday's records in the new place,
which is why this is written down rather than left to be worked out.

That is safe in one direction and worth checking in the other. A topic
that *starts* reaching a channel is straightforward - new records go
there, and a broadcast retained value whose topic now reaches a `latest`
channel is moved in at startup ("Retained messages on a broadcast topic").
A topic that *stops* reaching one leaves what was stored behind it
reachable by nothing: the records are all there, `$saguin/kv/get` answers
that the topic names no latest channel, and no counter anywhere is wrong.

**So at startup the broker counts the values in each `latest` channel
whose topic that channel's filter no longer matches, and logs the
number.** One value per topic is what makes that cheap. **Append channels
are not checked**, because it would mean reading the whole log - and that
is said here rather than left for an operator to conclude from silence
that there is nothing to find. `saguin --route` on the old topic answers
the question for either kind, before or after the change.

## What a filter reaches

**A subscriber is served every channel its filter touches, and each record
under the semantics its own topic has.** A filter touches a channel when
there is any topic both it and the channel's filter match. The client
never has to know where the channel boundaries are, which is the whole
reason placement is a filter and not a name.

With the three channels from the top of this document:

| Filter | Is served |
|---|---|
| `iot/water/w-7/location` | that device's current location, carrying RETAIN |
| `iot/water/+/location/#` | every device's current location |
| `iot/water/w-7/#` | the device's location as current state, its readings replayed from the subscriber's own position, and anything else beneath it live |
| `iot/weather/st-3/data/raw` | live broadcast only - no filter claims that topic |
| `#` | all of the above, across every device |

**A filter spanning two durable channels is two deliveries and not one
merged stream.** A durable consumer holds one position per channel
(RFC 0003 "Sessions"), so `iot/water/w-7/#` resumes independently in the
append channel and is served the latest channel's current values on every
subscribe.

**A queue is the exception, and it is the only one.** A filter crossing a
queue is granted and served everything else it matches, with the queue's
records left out - so `mosquitto_sub -V 5 -t '#'` at a new broker works,
which is the first thing anyone types, and takes nobody's work while doing
it. A queue's records are reachable through one subscription form and no
other, which is the next section.

Every row above is **accepted**, and so is a filter that matches nothing
at all: it matches nothing *yet*, and the topic it was written for may be
published a second later. The rule is enforced by the broker at
`SUBSCRIBE`, against the filter the client sent - an SDK that constructs
filters correctly changes nothing about it.

### What a wide filter costs, said rather than discovered

`#` with Clean Start = 1 is served every append channel from its
retention floor, and a client reconnecting twenty times reads it twenty
times. That is what an append channel promises and it is not a defect, but
it is a surprise if the first thing typed at a loaded broker is
`mosquitto_sub -t '#'`. `saguin --route <config> '#'` answers what a
filter would be served before anybody subscribes with it.

The resource shape is the same sentence from the other side: a filter is
one delivery goroutine per channel it touches, so `#` on a broker with N
append channels is N replays into one socket, each bounded by
`limits.write_timeout` and the client's Receive Maximum.

**A filter is not served only where it fits inside one channel's filter,
because of what that would hide.** It would make `iot/water/w-7/#` a
subscription to the device's commands and not its readings, answered
`SUBACK 0x00` with no indication that half of what was asked for is
missing. A client that asked for everything about one device, was told
yes, and was given half. Refusing loudly and serving fully are both
honest; a silent half is not.

## What each channel type admits: the SUBACK codes

### `append`

| | |
|---|---|
| Publish | `PUBLISH` to any topic the channel's filter matches, at any QoS. A QoS 2 publish is held and written at its `PUBREL` (RFC 0003 "Exactly once") |
| Subscribe | Any filter touching the channel's |
| Position | Per `client_id`, per channel |
| From 3.1.1 | Publish and subscribe both, with a durable position on `cleanSession = 0`. Where a reader with no position starts is `start` below, and is the same answer for every protocol |

A subscriber with Clean Start = 0 and a non-zero Session Expiry Interval
is a **durable consumer**: its position is stored and it resumes there on
reconnect.

**The rest is delivery semantics and RFC 0003 owns it**: the replay from
the retention floor, what `start: tail` changes, the position as a cursor
per `client_id` per channel, what Clean Start = 1 discards, and why a
consumer that changes its filter seeks rather than re-reads (RFC 0003
"Where a subscription starts" and "Durable consumers").

A shared subscription over this channel is served new records only, split
across the group - *Shared subscriptions* below.

### `latest`

| | |
|---|---|
| Publish | `PUBLISH` to any topic the channel's filter matches, at any QoS. A QoS 2 publish is held and written at its `PUBREL` (RFC 0003 "Exactly once") |
| Subscribe | Any filter touching the channel's |
| On subscribe | The current value of every matching topic is delivered |
| From 3.1.1 | Publish and subscribe both, and current state arrives with the RETAIN flag exactly as it does for MQTT 5 |

A publication replaces the topic's current value. A zero-length payload
deletes it, matching MQTT's own retained-message convention.

Current state is delivered carrying the RETAIN flag, so a stock client
can tell state it is catching up on from an update that has just
happened. RFC 0003 is authoritative on how it is delivered.

What a `latest` channel adds over a retained message - including over
Sagüin's own retained store, which is bounded and durable too - is that it
is named in the configuration file, that every value carries an age and a
position, and that a consumer reconnecting is sent only what changed. RFC
0003 is where those are described. Expiry is configured below as
`retention_period`, and it deletes the *current value* of a topic that has
gone quiet - which is the point rather than a defect. There is no size
bound, for the reason under "Bounds on what a channel holds": what grows
here is the number of topics rather than a history.

**`deletion_retention_period` is a second clock, for deletions.** A latest
channel keeps a deletion - a stored value with no payload, which nothing
reading the channel is shown - so that a broker mirroring it over a bridge
can be told a topic is gone; RFC 0003 has why, and this is how long it
is kept. A day when the file does not say, `none` to keep it as long as
the channel exists. A value and a deletion are not the same thing to
keep: a value lives as long as it is the truth, a deletion only until
everything reading this channel has seen it, so a channel keeping values
for ever does not have to keep a row for every device ever decommissioned.
It applies to no other channel type and is refused on one.

A shared subscription over this channel is served changes only, split
across the group - *Shared subscriptions* below.

### `queue`

| | |
|---|---|
| Publish | `PUBLISH` to any topic the channel's filter matches, at any QoS, from an MQTT 5 or a 3.1.1 client alike. A QoS 2 publish is held and written at its `PUBREL` (RFC 0003 "Exactly once") |
| Subscribe | `$saguin/queue/` + the channel's **name** - this form and no other |
| Subscription QoS | 1 only. QoS 0 on that form is `SUBACK 0x83`; QoS 2 is granted at 1 - MQTT grants the lower of what was asked and what is offered, and a queue's offers are sent at QoS 1 (RFC 0003) |
| From 3.1.1 | Publish yes, consume no. A delivery carries its Response Topic and Correlation Data as MQTT 5 properties, and 3.1.1 has neither |

**A worker names the channel, and needs nothing out of the configuration
file.** A queue called `inspections`, whatever its filter says, is consumed
through `$saguin/queue/inspections` - the same name its seek topic, its
response topic and its dead-letter channel are made of. The form matches
none of the queue's topics and is not resolved as a filter: the last level
is a channel name, compared as a name.

**The broker chooses which worker receives a job**, from its own index of
who is subscribed to which queue, and does not delegate that to a matched
filter. It already owns leases, offers, redelivery and attempt counts, so
the choice sits with the same component as the state it depends on - and
one exact string is one population of workers whatever MQTT would have made
of it. `$share` is an ordinary shared subscription here and reaches no
queue (*Shared subscriptions*).

**`$saguin/` is otherwise a space nothing is delivered under** - a queue's
form and a seek's reply are the two subscribable topics there, and the
whole space is set out under *Publishing*.

A queue's filter carries no `{a,b}`, and that is refused at startup.
Braces expand into two filters and two ways to place a topic, and the
ambiguity is not something a channel's name should have to answer for.

Exactly one subscription form is valid. Every other form is refused. With a
queue named `inspections`, filtered `iot/water/+/inspect`:

| Attempted | |
|---|---|
| `iot/water/+/inspect` | the channel's filter, which is not the form, and which lies inside the queue |
| `$saguin/queue/inspections/#` | straddles the work and the response topic |
| `$saguin/queue/inspections/w-7` | a channel name is one topic level |
| `$saguin/queue/#` | reaches every queue at once |
| `$saguin/queue/readings` | names a channel that is not a queue, or none |
| `$saguin/queue/inspections` at QoS 0 | see below |

**Everything under `$saguin/queue/` that is not a queue's form is refused
`0x8F`.** MQTT would grant such a subscription happily and Sagüin can never
feed it, so the subscriber would be connected and empty for ever. Four ways
to arrive there, each one keystroke from a correct line: a misspelt channel
name; a dead-letter channel's name, which is a *name* and not where its
records are; an `append` or `latest` channel's name, same reason; and a
wildcard or an extra level, which a channel name cannot contain.

**The dead-letter case is why this is a rule and not a warning**: failed
work lands in `<queue>__dlq` and a queue is consumed through
`$saguin/queue/<name>`, so putting the two together is the obvious
reading - and a dead-letter channel is an ordinary `append` channel, read
with an ordinary filter like any other.

**A plain filter lying entirely inside a queue's filter is refused `0x8F`
too.** A queue admits its form and nothing else, so a subscriber asking for
part of a queue's topics through an ordinary filter is refused rather than
served records that belong to a worker (invariant 11). One that merely
*crosses* a queue - `#`, `iot/#` - is contained by nothing, is an ordinary
subscriber, and is granted: it is served everything it matches except the
queue's records. Refusing the crossing form would break
`mosquitto_sub -t '#'`, which is the first thing anyone types at a new
broker.

A dead-letter channel's topics lie inside its queue's filter when that
filter ends in `#`, and a reader of them is not a worker asking wrongly.
The dead-letter filter is the more exact of the two, so it holds those
topics and the reader is granted - see "The dead-letter channel".

### Shared subscriptions

**`$share` is MQTT's and Sagüin does not take it.** A shared subscription
is granted on every channel type and over broadcast alike, and is served
what is published after its group began, split across the group:

| | |
|---|---|
| broadcast | everything, split across the group |
| `append` | new records only, split across the group. No replay, no offsets, and it never moves a stored position |
| `latest` | changes only, split across the group. No pass of current state on subscribe |
| `queue` | nothing. Queue records only ever leave through the queue's own form |

**A group is served alike over all three.** One with a member whose
session outlives its connection holds what it is owed while its members
are away, and a member's session ending gives back or counts what the
group had handed it (RFC 0003 "Broadcast"; "What a shared group is owed:
`broker.share`" below) - a channel's record exactly as a broadcast.

Over an `append` channel it is not load-balanced *consumption* of the
log's history, which would need a single position advanced by concurrent,
unordered acknowledgements - a consumer-group protocol Sagüin does not
have. A queue is the primitive for distributing work. The group holds no
position on the channel and never moves one - including the position of
the same client's ordinary subscription, if it holds one. And no pass of
current state on subscribe is MQTT's own rule: a server does not deliver
retained messages to a shared subscription, and a `latest` channel's
current state is delivered carrying RETAIN - a subscriber that wants
current state subscribes ordinarily.

**Each message goes to one member that can take it now**: a session that is
connected, whose outbound queue is not full, and - for a delivery at QoS 1 or
2 - with room in the in-flight window its Receive Maximum gives it. The
members that qualify take turns in the order of their client ids, one turn
per message, with a rotation kept for each group. A member whose client is
away is not given the group's share to hold, and neither is a connected one
that has stopped acknowledging, and neither is one whose Maximum Packet
Size the record is over, which is passed over rather than disconnected (RFC
0003 "When a record is too large for a subscriber").

**A message no member can take is never handed to a member that cannot
take it.** A group with a member whose session outlives its connection
holds it; any other drops it, counted as
`saguin_session_deliveries_dropped_total{cause="no_shared_member"}` (RFC
0005). An expired session is not a member at all: it leaves every group
it was in when it expires.

**It is never refused for the channels it reaches.** No ShareName is
reserved, no `0x9E` is sent for what it reaches, and the broker does not
work out which channels a shared filter touches. A shared group whose
filter lies inside a queue's is granted and receives nothing from that
queue, which is honest rather than lax: a queue's records leave through
its own form and no other, so there is nothing there for such a group to
be given.

Four refusals apply to a shared subscription, and none asks anything
about the configuration:

- **It is `$share/<ShareName>/<filter>` and nothing looser** (MQTT 5
  section 4.8.2): a ShareName of at least one character holding no `+` or
  `#`, then `/`, then a topic filter. A filter that begins `$share` again,
  in any case, is refused too: it matches only topics under `$share/`, and
  nothing can publish there. `0x8F`.
- **The filter inside is refused where it would be refused alone.**
  `$share/g/$SYS/#` is refused `0x87`, as `$SYS/#` is, and so is any filter
  under `$saguin/`, a queue's form and a seek reply included: a queue's work
  goes to the one worker Sagüin chose and a seek reply to one client's
  socket, so neither is ever delivered to a group.
- **`$share` is spelled in lower case and no other.** `$SHARE/grp/events/#`
  is an ordinary filter under a level called `$SHARE`, and Sagüin refuses
  the spelling rather than interpreting it, `0x8F`.
- **A 3.1.1 client cannot have one.** Its specification gives `$share` no
  meaning, so it has asked for topics under `$share/`, which nothing can
  publish to. Refused `0x80`, the one failure code a 3.1.1 `SUBACK`
  carries.

Each catches a filter that would otherwise be granted and then match
nothing for ever.

A client whose roles deny `share` is not refused a filter at all: its
`CONNACK` says Shared Subscription Available 0, and a `SUBSCRIBE` asking
for one anyway closes the connection, `0x9E` (*Taking a feature away*).

**Each is refused with the code that describes it**, and this is every
reason code a `SUBACK` from Sagüin carries - one per filter, so a SUBSCRIBE
naming four gets four answers:

| Reason code | When | To a 3.1.1 client |
|---|---|---|
| 0x00 / 0x01 / 0x02 Granted | Granted, at the QoS shown - never higher than the one asked for, and never above the broker's ceiling of 2 (RFC 0001), or of 1 for a client whose roles deny `qos2`. A queue's form is granted at most 1, its offers being sent at QoS 1 | the same |
| 0x83 Implementation specific error | QoS 0 on a queue's canonical form: there is no transport acknowledgement to start the visibility timeout from. Also, on every filter of the packet: a `saguin-` User Property Sagüin does not read, or a partition declaration that is malformed or self-inconsistent, neither of which names a filter. A valid declaration on `$share/…` or a queue's form (RFC 0003) refuses that filter alone, and the packet's other filters are granted with the declaration applied. And on every filter the packet would add or change, the provider `broker.session.storage` names failing to keep the session's record, or the cursor of a shared group kept with it, other than for room - the session is kept as it was | `0x80`. The store's failure is reachable; the rest is not: a 3.1.1 client is refused the form outright, and has no User Properties to carry the others |
| 0x87 Not authorized | A filter the client's roles do not allow (*What a client may do*), or one in a reserved space nothing is delivered under - `$SYS/`, or `$saguin/` other than the two topics Sagüin defines a subscription for. Either space inside `$share/<ShareName>/`, those two topics included | `0x80` |
| 0x8F Topic Filter invalid | A string that is not a topic filter at all, `+` or `#` taking less than a whole level (MQTT-4.7.1-1, -2); one with more levels than `limits.max_topic_levels`; a spelling under `$saguin/queue/` that is not a queue's canonical form; a plain filter lying entirely inside a queue's filter; a queue's form asked with No Local, which would withhold a job from the one worker whose client id published it (RFC 0003 "No Local"); a queue's form from a 3.1.1 client; `$share` written in any case but the one MQTT defines; or a shared subscription that is not `$share/<ShareName>/<filter>`, or whose filter begins `$share` again | `0x80`, and any filter beginning `$share/` at all - except the string that is not a filter, which closes the connection, 3.1.1 having no code for a malformed packet |
| 0x97 Quota exceeded | The provider `broker.session.storage` names is at its `max_bytes`, on every filter the packet would add or change - the session is kept as it was. The cursor a shared group is given by its first member whose session outlives its connection is kept in the same write as that member's record, so no room for the cursor is no room for the packet. Also a filter that would take the client past `limits.max_subscriptions` ("How many topic filters one client may hold") | `0x80` |

**A refusal is total and the client stays connected**, whichever code it
is: it is told which rule it broke and may subscribe again correctly - a
3.1.1 client included, the `SUBACK` being the one packet where 3.1.1 can
be refused in words rather than by a closed connection. The exceptions
are protocol errors rather than filters, and they end the connection
instead: a `SUBSCRIBE` carrying no filters at all - `0x82`, under "A
`DISCONNECT` from the broker" - a shared subscription asked with No Local
(MQTT-3.8.3-4; RFC 0003 "No Local"), and a malformed filter from a 3.1.1
client, which that protocol gives no code for.

**Except where the substrate granted it anyway**, and then the client is
disconnected - `0x9B`, `0x8F` or `0x83` matching the refusal that was
overridden (RFC 0003) - rather than left holding a subscription Sagüin
refused. Every *granted* filter is checked a second time because a hook
only advises: a substrate that ignored the reason codes would grant
exactly what was refused (invariant 10). On one that honours them this
never fires, and either way the broker never silently downgrades a queue
subscription into something that looks like it worked.

**A queue cannot be consumed by a 3.1.1 client at all** - a delivery
carries its Response Topic and Correlation Data as MQTT 5 properties, and
acknowledgement is a publish echoing both, so such a worker would be
handed jobs it can never resolve: each times out, redelivers and
dead-letters, with nothing reporting that the worker was incapable rather
than slow. It is refused `0x80`, the one failure code a 3.1.1 `SUBACK`
carries.

A 3.1.1 client publishing *into* a queue is untouched - it is an ordinary
publish to an ordinary topic, and the whole point of the feature.

**One form is one population of workers** (invariant 4): a second spelling
is not a second group, it is a subscription that names no queue, and it is
refused. Two independent work streams over the same records are two
queues; there is deliberately no way to express that with subscriptions.

## Publishing

A publish is resolved to the channel whose filter spells its topic out
most exactly, or to broadcast where no filter matches at all. The broker
replies with a `PUBACK` (QoS 1) carrying one of:

| Reason code | When | To a 3.1.1 client |
|---|---|---|
| 0x00 Success | Stored, or accepted for delivery | `PUBACK`, carrying no code - 3.1.1's has no room for one |
| 0x80 Unspecified error | On the `PUBREC` of an exactly-once publish from a connection whose client id another connection now holds: the session it would be held for is no longer this connection's | the connection is closed |
| 0x83 Implementation specific error | The channel's storage, or the broadcast log's, could not keep the record. Also a refused seek - the payload is not an offset or a time, or it is outside the channel - because a seek is a publish to `$saguin/consumer/<channel>/seek` and this is the answer every client gets, whether or not it set a Response Topic (RFC 0003) | the connection is closed |
| 0x87 Not authorized | The channel takes its records from one place and this is not it: a derived `<queue>__dlq`. Also a principal whose roles do not allow it here (*What a client may do*) | the connection is closed |
| 0x90 Topic Name invalid | Over `max_topic_length`, or deeper than `max_topic_levels`; in the reserved `$saguin/` space without being one of the topics Sagüin defines there; or beginning `$share/`, which is a subscription form rather than a topic | the connection is closed |
| 0x97 Quota exceeded | The channel is at its size limit and its policy is to reject; the channel's provider, or the retained store's, is at its `max_bytes`; the publish exceeds `max_header_count` or `max_header_bytes`; or the client is over its `publish_rate` or `publish_bytes` ("How fast one client may publish") | the connection is closed |
| 0x99 Payload format invalid | Payload Format Indicator says UTF-8 and the payload is not | cannot arise - 3.1.1 has no Payload Format Indicator |

**A refusal a 3.1.1 client cannot be told about closes its connection, and
is never acknowledged.** 3.1.1's `PUBACK` carries no reason code, so the
only answers the packet can express are success and nothing. Answering
success to a record the broker threw away is the one failure this project
will not ship - a publisher told its record is safe, on a channel
configured to keep it, and the loss visible in nothing but the broker's own
log. A closed connection says far less than a code, but what it says is
true, and a client that reconnects and re-sends loses no record. What it
costs is a misconfigured device in a reconnect loop, which is at least loud.

**A refused QoS 0 publish is still a silent, counted drop**, on 3.1.1
exactly as on MQTT 5: there is no reply packet to withhold and nothing
changes.

Every non-success carries a Reason String - but only to a client that
asked for one: MQTT lets a server withhold it when the client sent
Request Problem Information 0, and Eclipse Paho sends 0 by default. That
is MQTT working as specified, written here because measuring it the wrong
way round reports a broker that says nothing when it said everything.

**Every refusal is logged whatever the QoS**, so the one case with no
reply at all is still visible to the operator - and a queue producer has
one more reason to publish at QoS 1.

**A publish at QoS 2 is answered on its `PUBREC`.** Every refusal in the
table above is carried on the `PUBREC` instead, and for a reason MQTT
states: a receiver makes every check that could produce a forwarding
failure *before* it accepts ownership of the message, and reports the
outcome in that packet (section 4.3.3). One code is reachable only here -
`0x97` for a publisher already holding `broker.qos2.max_inflight_per_client`
unfinished exchanges - and it does not end the connection, because such a
client is inside a bound rather than misbehaving and its next exchange
succeeds as soon as one of its own completes.

**`0x87` is what a derived dead-letter channel answers a client** - it
takes records from its queue's own dead-lettering and from nowhere else,
and reading one is untouched ("The dead-letter channel" below).

The row's other cause - a client whose roles do not allow the publish -
needs an `acl_file`; without one every authenticated client may use every
channel, which is what a broker with no such file does.

**A publish beginning `$share/` is refused because nothing could ever
read it**: every subscription starting with those characters is a shared
subscription, not a topic, and no channel can claim them either. Left
accepted it is a record stored nowhere, delivered to nobody, and answered
Success. Only the exact lower-case spelling is refused - `$SHARE/x` is an
ordinary topic an ordinary filter can subscribe to, and publishing there
is allowed.

**Sagüin does not send `0x10 No matching subscribers`**, which MQTT 5
makes optional; a broadcast nobody hears is answered `0x00`, where
mosquitto answers `0x10`. The cost is a producer's only signal that it
mistyped a channel name: `event/orders`, one letter short of the `events`
channel, is an ordinary broadcast topic, acknowledged on every publish,
and nothing on the wire distinguishes it from a deliberate broadcast.

**Sagüin defines exactly five topics in the reserved space**, and a publish
to anything else under `$saguin/` is the 0x90 above:

| Topic | |
|---|---|
| `$saguin/consumer/<channel>/seek` | a consumer moves its own position |
| `$saguin/queue/<queue>/response` | a worker acknowledges or returns a delivery |
| `$saguin/kv/get` | a client reads the current value of one topic on a `latest` channel |
| `$saguin/sessions/disconnect` | an operator hangs up the client whose id is the payload |
| `$saguin/catalogue/<channel>` | a client asks what one channel is: its type, and the filter it claims |

**And two topics a client may *subscribe* to**, which are the whole of what
is delivered under `$saguin/`:

| Topic | |
|---|---|
| `$saguin/queue/<queue>` | a worker consumes that queue: one job at a time, at QoS 1 |
| `$saguin/consumer/<channel>/seek/reply` | a consumer reads the answer to its own seek when it set no Response Topic |

Publishing to either is refused with everything else under the prefix - the
five above are the whole of what a client may publish there. **A queue's two
topics sit one level apart and are opposites**: work is delivered on
`$saguin/queue/<queue>`, which may only be subscribed to, and answered on
`$saguin/queue/<queue>/response`, which may only be published to. A filter
of `$saguin/queue/<queue>/#` would straddle both, so it is refused with
every other spelling that is not a queue's exact form.

**Everything else in the space is a topic a client publishes to**, so a
subscription to one of those is a promise that can never be kept and is
refused rather than granted and left silent. Nothing reaches this space by
wildcard at all: MQTT already stops `#` matching a topic beginning with
`$`, so only a client spelling one of these two out arrives here.

**A subscription to the seek reply can only ever carry that client's own
answers**, and that is a property of how the reply is sent rather than a
rule to enforce: Sagüin writes it to the seeking client's socket rather
than publishing it, so nothing reaches a subscriber that did not seek.
Which is what lets the topic be one name rather than one per client - a
client id in a topic would be a string a stranger chose, and then a rule
about who may subscribe to whose.

**The first two name a channel that can answer them** - a seek an append
channel, a response a queue - and a name that matches nothing, or the
wrong kind, is refused with a Reason String saying which. **The third
names no channel**: its payload is the topic to read, and a topic already
resolves to a channel (RFC 0003 has the verb). **The fourth names none
either**, a hang-up being an act on nobody's records ("Hanging up a
client" below). **The fifth names a channel of any kind** ("Asking what a
channel is" below).

**The reason code judges the topic; the handler judges the request.** What
is *inside* the message is not judged here, and each handler answers it its
own way: RFC 0003 gives a seek a reply on the client's own Response Topic,
and says of a response that it is ignored and logged and that a worker is
never told whether it was applied.

The `PUBACK` is the only answer that always arrives, and for a seek it
carries the refusal too: a client like `mosquitto_pub` that publishes and
exits can read nothing else (RFC 0003).

**A publish asking for a retained message is kept where there is
somewhere to keep it.** On a topic an `append` or `latest` channel claims,
the channel is that store and the flag adds nothing. On a `queue` the
message is taken as ordinary work and the flag dropped -
`saguin_queue_retain_ignored_total` counts those, nothing on the wire
reporting a dropped flag. On a broadcast topic the value is kept in the
retained store ("Retained messages on a broadcast topic" under
*Configuration*). From a client whose roles deny `retained` the publish
is refused with a `DISCONNECT` carrying `0x9A` (Retain not supported) -
MQTT makes it a protocol error, and `0x9A` is no code a `PUBACK` may
carry. RFC 0003 "Retained messages" has the whole of it.

The consequence worth knowing before reaching for it: **nothing in the
reserved space can be watched from the wire.** Somebody debugging a
worker's acknowledgements watches the worker, not the response topic.

**A topic that is malformed rather than merely unwanted never reaches this
table at all.** A wildcard in a `PUBLISH` topic is a protocol error under
MQTT 5 rather than a refusable publish, and the answer is a **DISCONNECT
with 0x82 (Protocol Error)**; a publish into the `$SYS` tree the broker
keeps about itself is answered **`0x87` (Not authorized)**; and a topic
whose bytes are not valid UTF-8 fails to decode before any of this runs and
the connection closes with nothing sent. `mosquitto_pub` validates the topic
itself and sends neither of the first two, so through that tool this
behaviour is invisible.

**The broker asks the same questions of its own publishers.** A Will, an
inbound bridge record and a queue delivery do not arrive on a socket, so
the same checks run on the way into a channel and a refusal is logged
instead - or, for a bridged record, held against the bridge's own
acknowledgement (*Bridges*). A Will naming `events/+` would otherwise be
stored in a channel where it stopped every conforming consumer for ever.

A record the storage could not keep is refused rather than acknowledged:
a producer told its publish failed can retry or raise an alarm, and one
told it succeeded can do neither. The code is **0x83** because nothing
the client sent is wrong - MQTT 5 gives it for exactly that - where 0x80
declines to say why, and every refusal here says why.

Publishes above `max_message_size` never arrive. The limit is enforced
against the declared packet length before any buffer is allocated -
earlier than any hook - so no rule in this document applies to such a
packet. **The bound is advertised**: the `CONNACK` carries Maximum Packet
Size, so a conforming client never sends the packet, and one that does is
disconnected with **0x95 (Packet too large)**. The producer's own write
still fails part-way - the broker stops reading at the declared length -
and the reason arrives on the same connection, so a producer that reads
before it retries has it.

## The dead-letter channel

Every `queue` named `<name>` derives an `append` channel `<name>__dlq`
automatically. It is not configured, cannot be configured, and shares the
queue's storage provider so that the move out of the queue and into it is
one transaction.

**Its filter is derived too, and so is the topic each dead-lettered record
takes.** A record keeps the queue's name for its channel and gains one
level in its topic: `__dlq`, inserted where the queue's filter carries its
`#`, or appended where it carries none.

```
queue filter                  dead-letter filter
iot/water/+/inspect           iot/water/+/inspect/__dlq
iot/water/+/+                 iot/water/+/+/__dlq
iot/water/+/inspect/#         iot/water/+/inspect/__dlq/#
jobs/#      (the default)     jobs/__dlq/#

a record: iot/water/w-7/inspect  →  iot/water/w-7/inspect/__dlq
```

One rule for every shape of filter, and the `+` levels keep what they
captured because they are the same positions - one level is inserted and
nothing else moves.

**The rewrite is what makes a dead letter readable, not decoration.** A
subscriber finds a channel through the filters and nothing else, so a
record still carrying the queue's own topic would belong to the queue and
be refused to everyone who is not a worker. Where the queue's filter ends
in `#` the derived filter lies inside it, and the rule two sections up
settles which holds a topic without anything further being said: `__dlq`
is spelled out where the queue has `#`, so the dead-letter channel is the
more exact of the two.

`__dlq` is a reserved level in consequence. **No filter an operator writes
may carry a level equal to `__dlq`**, and that is refused at startup; every
filter that does is one the broker derived.

It is an ordinary `append` channel in every other respect. Consumers
subscribe to the topics above - `iot/water/+/inspect/__dlq`, or one
device's, or `iot/#` and read dead letters among everything else - replay
it, and hold independent positions. A publish *to* one of those topics is
refused `0x87`, because a dead-letter channel takes records from its queue
and from nowhere else: a record put there directly has no queue record
behind it, so a redrive that reads `saguin-dlq-channel` and
`saguin-dlq-offset` to put work back finds neither. The same rule refuses
a bridge rule naming one. Its seek topic is
`$saguin/consumer/<name>__dlq/seek`, by name, as every seek topic is.

## Every reason code, in one place

The answer to a publish is under "Publishing" and a subscription's under
"What each channel type admits"; what is left - the answers that arrive
before a session exists or that end one - is collected here.

**At `CONNECT`, in the `CONNACK`:**

| Code | Refuses | To a 3.1.1 client |
|---|---|---|
| *unacceptable protocol version* | Any version below `broker.mqtt.min_protocol_version`, and MQTT 3.1 always - answered in the refused protocol's own vocabulary, the broker working rather than a fault (RFC 0001) | `0x01` |
| `0x81` Malformed packet | A `CONNECT` with its reserved flag set or a Will QoS of 3, one whose Will QoS is set with no Will to govern, or one whose user name is not well-formed UTF-8 | `0x05` Not authorized; the user name `0x04`, being a broken credential |
| `0x82` Protocol error | A `CONNECT` with Will Retain and no Will, or a 3.1.1 one carrying a password and no user name, which that protocol forbids | `0x05` Not authorized |
| `0x83` Implementation specific error | A `CONNECT` carrying a `saguin-` User Property Sagüin does not read (RFC 0003 "The reserved prefix on a client's own packets"); or one the provider `broker.session.storage` names failed for, other than for room - a resumed session it could not read, a Will it could not write ("Every session's state: `broker.session`"), the ending of what a session begun new replaces (RFC 0003 "Sessions"), or what it still owes the client id from before - its last connection's disconnect (RFC 0003 "Last Will"), a record a connection that never completed wrote over, or what an ended session left in its channels (RFC 0003 "Sessions") - or the cursor of a shared group a resumed session holds and that has none | `0x03` Server unavailable, for the storage failure (3.1.1 has no User Properties to carry the other) |
| `0x85` Client identifier not valid | A client id over `limits.max_client_id_length`, or a zero-length one asking for a session that outlives the connection - Clean Start 0, `cleanSession = 0` (MQTT-3.1.3-8) - because a session cannot be kept under no name, or one holding a control character (U+0001-U+001F, U+007F-U+009F), which mosquitto and EMQX refuse too | `0x02` Identifier rejected |
| `0x86` Bad user name or password | A wrong credential and a missing one alike, so an unauthenticated caller cannot learn which user names exist; and a name holding U+0000 or a control character - a certificate's Common Name, a proxy's, or a `CONNECT` user name - which is nobody's ("TLS on a listener") | `0x04` Bad user name or password |
| `0x87` Not authorized | A Will from a client whose roles deny `will` (*Taking a feature away*), or on a topic its roles do not allow it to publish to (*What a client may do*; RFC 0003 "Last Will") | `0x05` Not authorized |
| `0x89` Server busy | `limits.max_connections` reached, on a tcp, tls or unix socket that found a place in the overflow budget and sent the opening bytes of a `CONNECT` ("How long a socket may wait to send CONNECT"); any other socket, and every ws one, is closed with nothing written | `0x03` Server unavailable |
| `0x8C` Bad authentication method | A `CONNECT` naming an Authentication Method. Sagüin runs no enhanced authentication, so every method named is one it cannot continue, and a client library answered this falls back to an ordinary `CONNECT` | - (3.1.1 has no property to name one) |
| `0x90` Topic name invalid | A Will that could never be delivered: the reserved `$saguin/` space, a dead-letter channel, over `max_topic_length` or deeper than `max_topic_levels`, or a topic name nothing may publish to - one holding `+` or `#`, or in the `$SYS` tree (RFC 0003 "Last Will") | `0x05` Not authorized |
| `0x97` Quota exceeded | A Will the provider `broker.session.storage` names has no room for: MQTT can tell a client its session ends with its connection, and has no way to say "your Will is not held" ("Every session's state: `broker.session`"); or a resumed session holding a shared group that has no cursor, which that provider has no room to give it (RFC 0003 "Broadcast") | `0x05` Not authorized |
| `0x9A` Retain not supported | A retained Will aimed at a broadcast topic from a client whose roles deny `retained` - where a retained publish is refused too (RFC 0003 "Retained messages") | `0x05` Not authorized |
| `0x9B` QoS not supported | A QoS 2 Will from a client whose roles deny `qos2`, which would be told Maximum QoS 1 (MQTT-3.2.2-12) | `0x05` Not authorized |

**A `CONNECT` that cannot be read is answered with the code the reading
stopped on**, once it has said it speaks MQTT 5: `0x81` Malformed packet
or `0x82` Protocol error - a property a `CONNECT` may not carry, a
property sent twice or with a value its definition forbids, a length that
runs past the packet, bytes left after the last field - and `0x95` Packet
too large for one over the Maximum Packet Size. One refused before it says
which version it speaks, or below MQTT 5, is closed with nothing written:
3.1.1 has no return code for either.

**3.1.1 has five return codes and Sagüin has more reasons than five**, so
`0x05` is shared: the Will refusals and the malformed-flag refusals all
land on it, because each is genuinely "this connection may not do what it
is asking to do" - the closest of the five, rather than a code picked to
fill the column. A `CONNACK` is all a 3.1.1 client gets: there is no
Reason String and no server `DISCONNECT` to follow it with.

**A zero-byte client id is admitted on both protocols when it asks for no
session** - `cleanSession = 1`, Clean Start 1 - because a session ending
with its connection needs no name to be kept under. MQTT 5 then assigns
an id and says so in the CONNACK's Assigned Client Identifier, a property
3.1.1 does not have. Asking to *keep* a session under no name is the
`0x85` row above, on either protocol.

**`0x04` and `0x05` are kept apart on purpose**: a credential that does
not work and a Will the connection may not register are fixed by
different people in different files. The credential refusal keeps `0x04`;
everything about what the connection may *do* is `0x05`.

**A `DISCONNECT` from the broker**, which is the answer where no reply
packet exists:

| Code | Ends the connection because |
|---|---|
| `0x80` Unspecified error | A `PUBREC` for a QoS 2 delivery that the store could not record: no `PUBREL` is sent, and the exchange resumes with the session (RFC 0003 "Broadcast") |
| `0x81` Malformed packet | A packet that cannot be read: a `PUBLISH` with both QoS bits set, flags MQTT reserves, a length that runs past the packet, bytes left after its last field, or a property the packet may not carry |
| `0x82` Protocol error | A packet MQTT makes a Protocol Error as it is read, such as a `SUBSCRIBE` asking for Retain Handling 3, or any packet carrying a property twice that MQTT allows once or with a value its definition forbids (a Payload Format Indicator of 2, a Receive Maximum of 0). A publish whose topic name carries `+` or `#`, or one naming a topic alias this connection never registered - which every client that reconnects and keeps using its aliases does, since a new connection starts with none. A `SUBSCRIBE` carrying no filters at all: a zero-length filter is not a filter the broker dislikes but a field the client did not send, which MQTT calls a Protocol Error rather than `0x8F`'s business. A shared subscription asked with No Local, which MQTT-3.8.3-4 makes a Protocol Error (RFC 0003 "No Local"). An `AUTH` packet, no connection here having a method to re-authenticate with |
| `0x83` Implementation specific error | A guard that should never fire: the substrate granted a SUBSCRIBE carrying a reserved property or a partition declaration Sagüin refused (RFC 0003), and the connection ends rather than deliver on a grant that lies. And a `PUBREL` whose release the store could not write: the message stays held (RFC 0003 "Exactly once"). And a session begun new whose ending of what it replaces the store refused after its `CONNACK` was written: the client's next `CONNECT` begins it again (RFC 0003 "Sessions") |
| `0x8E` Session taken over | Another client connected with the same client id - enforced where the session lives, in the substrate |
| `0x8F` Topic filter invalid | The same guard, for a shared subscription or a queue's form this connection cannot have, granted anyway |
| `0x93` Receive Maximum exceeded | More QoS 2 exchanges unfinished than the Receive Maximum its `CONNACK` gave (MQTT-3.3.4-7); an exchange is unfinished until its `PUBREL` is answered |
| `0x94` Topic alias invalid | An alias above the advertised maximum of sixteen, refused where the packet is parsed |
| `0x95` Packet too large | A packet above the advertised Maximum Packet Size - and a bridge reads this one from its source, where it stops the link ("The two brokers' `limits` blocks have to agree"). And the other way: a channel's record, or a queue's job, over the Maximum Packet Size of the subscriber or worker it was for; a shared group's member it does not fit is passed over instead (RFC 0003 "When a record is too large for a subscriber") |
| `0x97` Quota exceeded | A `PUBREL` whose release was refused for room: its channel filled after the `PUBREC`, or a retained broadcast's value finds no room in the retained store's provider. The message stays held, and the `PUBREL` sent again on the next connection completes it once there is room (RFC 0003 "Exactly once") |
| `0x98` Administrative action | Somebody hung this client up: an operator holding `disconnect` published its client id to `$saguin/sessions/disconnect`. The session is untouched, so reconnecting resumes it |
| `0x9A` Retain not supported | A retained publish to a broadcast topic, at either QoS, from a client whose roles deny `retained`: `0x9A` is not a code a `PUBACK` may carry, at QoS 0 there is no `PUBACK` at all, and acknowledging would claim a store the client may not use. A queue is not this case - there the flag is dropped and the work is taken |
| `0x9B` QoS not supported | A QoS 2 publish from a client whose roles deny `qos2`, which its `CONNACK` told Maximum QoS 1 (MQTT-3.2.2-11). The same guard, for a queue subscription granted against Sagüin's refusal of its form or its QoS |
| `0x9E` Shared Subscriptions not supported | A `SUBSCRIBE` containing a `$share/` filter from a client whose roles deny `share`, which its `CONNACK` told Shared Subscription Available 0 - a Protocol Error (MQTT 5 section 3.2.2.3.13). The same guard closes a connection the substrate granted one anyway |

#### Every reason code Sagüin sends, by code

The four tables above are organised by packet, which is the right shape
when you are reading about a feature. This one is the other question, and
the one somebody has at three in the morning: **I have a code, what could
it be?** It is a reference rather than a second account - the causes below
are the ones stated above, gathered under the number a client actually saw.
A publish at QoS 2 reads its `PUBACK` rows off its `PUBREC` ("Publishing").
Every filter a `SUBACK` refuses is counted in
`saguin_subscriptions_refused_total`, under the code decided for it - the
one below, even where a 3.1.1 client was sent `0x80` (RFC 0005).

| Code | Sent on | What it means here |
|---|---|---|
| `0x00` | `CONNACK`, `SUBACK`, `PUBACK` | Accepted. On a `SUBACK` it is also the granted QoS |
| `0x01` | `SUBACK` | Granted at QoS 1, and what a queue's form is always granted at |
| `0x02` | `SUBACK` | Granted at QoS 2 - never on a queue's form |
| `0x80` Unspecified error | `PUBREC`, `DISCONNECT` | On a `PUBREC`, an exactly-once publish from a connection whose client id another connection now holds. On a `DISCONNECT`, a `PUBREC` for a QoS 2 delivery the store could not record, so no `PUBREL` was sent |
| `0x81` Malformed packet | `CONNACK`, `DISCONNECT` | A `CONNECT` with its reserved flag set or a Will QoS of 3, one whose Will QoS is set with no Will, or one whose user name is not well-formed UTF-8; any packet that cannot be read - a `PUBLISH` with both QoS bits set, flags MQTT reserves, a length that runs past the packet, bytes left after its last field, a property the packet may not carry |
| `0x82` Protocol error | `CONNACK`, `DISCONNECT` | A packet MQTT makes a Protocol Error as it is read, such as Retain Handling 3, a property sent twice that MQTT allows once, or one with a value its definition forbids; a topic name carrying `+` or `#`, or an alias a connection never registered; a `SUBSCRIBE` carrying no filters at all; a shared subscription asked with No Local (MQTT-3.8.3-4); a `CONNECT` with Will Retain and no Will, or a 3.1.1 one with a password and no user name; an `AUTH` packet, which is answered on a `DISCONNECT` because no connection here has an authentication method to re-authenticate with |
| `0x83` Implementation specific error | `CONNACK`, `SUBACK`, `UNSUBACK`, `PUBACK`, `DISCONNECT` | QoS 0 on a queue's canonical form; a `saguin-` property Sagüin does not read, on a `CONNECT` or a `SUBSCRIBE`; a partition declaration it refuses (RFC 0003); storage that could not keep the record, or at a `CONNECT` could not read the session being resumed, write the Will, end what a session begun new replaces, write what it still owes the client id from before, or give a shared group a resumed session holds the cursor it has none of; on an `UNSUBACK`, a session store that could not forget the filter, which stays subscribed ("Every session's state: `broker.session`"); on a `DISCONNECT`, an exactly-once release the store could not write, the message kept held (RFC 0003 "Exactly once"), or that ending refused after the `CONNACK` (RFC 0003 "Sessions"); a seek whose payload is not an offset or a time |
| `0x85` Client identifier not valid | `CONNACK` | A client id over `limits.max_client_id_length`, a zero-length one sent with Clean Start 0, or one holding a control character |
| `0x86` Bad user name or password | `CONNACK` | A wrong credential, a missing one where the listener requires it, and a name holding U+0000 or a control character |
| `0x87` Not authorized | `SUBACK`, `PUBACK` | A filter or a channel the client's roles do not allow; a derived `__dlq`, which takes its records from the queue it failed in |
| `0x89` Server busy | `CONNACK` | `limits.max_connections` reached and the socket found a place in the overflow budget and sent a `CONNECT`; otherwise it is closed with nothing written, always on a ws door |
| `0x8B` Server shutting down | `DISCONNECT` | The broker is stopping |
| `0x8C` Bad authentication method | `CONNACK` | A `CONNECT` naming an Authentication Method. Sagüin runs no enhanced authentication, so every method named is one it cannot continue, and a client library answered this falls back to an ordinary `CONNECT` |
| `0x8E` Session taken over | `DISCONNECT` | Another client connected with the same client id |
| `0x8F` Topic filter invalid | `SUBACK`, `DISCONNECT` | A string that is not a topic filter at all - `+` or `#` taking less than a whole level (MQTT-4.7.1-1, -2) - from an MQTT 5 client; a filter deeper than `limits.max_topic_levels`, on a `SUBACK`; a queue subscription that is not the canonical form, one asked with No Local (RFC 0003 "No Local"), or one this connection cannot have; `$share` in any spelling but MQTT's own lower case, or not shaped `$share/<ShareName>/<filter>` |
| `0x90` Topic name invalid | `CONNACK`, `PUBACK` | Over `max_topic_length`; the reserved `$saguin/` space; a Will that could never be delivered |
| `0x92` Packet Identifier not found | `PUBCOMP` | A `PUBREL` for an exactly-once publish nothing holds any more: it outlived `broker.qos2.expires_after`, its session ended or did not come back, its channel was removed, or it was released before a restart its `PUBCOMP` did not survive (RFC 0003 "Exactly once") |
| `0x93` Receive Maximum exceeded | `DISCONNECT` | More QoS 2 exchanges unfinished than the Receive Maximum the `CONNACK` gave |
| `0x94` Topic alias invalid | `DISCONNECT` | An alias above the advertised maximum |
| `0x95` Packet too large | `CONNACK`, `DISCONNECT` | A packet above the advertised Maximum Packet Size, the `CONNECT` included; a channel's record or a queue's job over the Maximum Packet Size of the subscriber or worker it was for |
| `0x97` Quota exceeded | `CONNACK`, `PUBACK`, `PUBREC`, `SUBACK`, `DISCONNECT` | On a `PUBACK`, **five different things** - see below - and on a `PUBREC` the same five, joined by a publisher already holding `broker.qos2.max_inflight_per_client` unfinished exchanges. On a `DISCONNECT`, an exactly-once release its channel has filled too far to take since the `PUBREC`, or a retained broadcast's release whose value the retained store's provider has no room for: the message stays held, and the `PUBREL` sent again on the next connection completes it once there is room (RFC 0003 "Exactly once"). On a `CONNACK`, a Will the session store has no room for, or the cursor of a shared group a resumed session holds and that has none. On a `SUBACK`, the session store `broker.session.storage` names is at its `max_bytes`, for every filter the packet would add or change ("Every session's state: `broker.session`"), or a filter that would take the client past `limits.max_subscriptions` ("How many topic filters one client may hold") |
| `0x98` Administrative action | `DISCONNECT` | An operator hung this client up with `disconnect`; the session survives and reconnecting resumes it |
| `0x99` Payload format invalid | `PUBACK` | Payload Format Indicator says UTF-8 and the payload is not |
| `0x9A` Retain not supported | `CONNACK`, `DISCONNECT` | A retained Will, or a retained publish, aimed at a broadcast topic by a client whose roles deny `retained` |
| `0x9B` QoS not supported | `CONNACK`, `DISCONNECT` | A QoS 2 Will or a QoS 2 publish from a client whose roles deny `qos2`; a queue subscription granted against Sagüin's refusal of its form or its QoS |
| `0x9E` Shared Subscriptions not supported | `DISCONNECT` | A shared subscription from a client whose roles deny `share` |

**A 3.1.1 client sees none of these numbers.** Its `CONNACK` carries one of
the five return codes mapped above, its `SUBACK` carries a granted QoS or
`0x80` for a refusal, and its `PUBACK` carries nothing at all. That last
one is why a publish refusal it cannot be told about is a closed connection
rather than an acknowledgement.

**`0x97` is the one to know about.** MQTT 5 has no finer code, so Sagüin
answers all five of these with it: a channel at its `max_bytes`, its
provider at its `max_bytes`, a publish over `max_header_count`, one over
`max_header_bytes`, and a client over its `publish_rate` or
`publish_bytes` - and a QoS 2 publisher meets the same five on its
`PUBREC`, joined by a sixth: it already holds
`broker.qos2.max_inflight_per_client` unfinished exchanges. A publisher
cannot tell them apart from the code alone - the Reason String beside it
says which, but MQTT lets a client ask not to receive one and Eclipse
Paho asks not to by default.

**An operator can tell them apart**, and that is deliberate rather than
incidental: a metric is not bound by MQTT's code set, so
`saguin_publish_refused_total` labels a rate refusal `publish rate exceeded`
and the inflight refusal `inflight allowance exceeded`, where the rest are
`quota exceeded`. "Am I throttling my own devices" and "is a channel full"
are opposite problems with opposite fixes, and an operator watching refusals
climb should not have to guess between them. The same series is what makes a
QoS 0 refusal visible at all, since nothing reaches the publisher there.

**And what the `CONNACK` promises when it says yes**, so a client never
has to guess:

**Every row below is an MQTT 5 `CONNACK` property.** A 3.1.1 `CONNACK`
carries a session-present flag and a return code and nothing else, so a
3.1.1 client is told none of it. Every limit is still enforced against it -
that is what the refusals above are for - with one exception, Server Keep
Alive, because a ceiling that works by telling the client cannot be
enforced against a client that cannot be told. `limits.max_keepalive` says
what happens instead.

| Property | Value |
|---|---|
| Maximum QoS | **Absent**, which MQTT reads as 2 (section 3.2.2.3.4): the property carries only 0 or 1, so a broker offering 2 omits it. 1 for a client whose roles deny `qos2` (*Taking a feature away*) |
| Topic Alias Maximum | 16, per connection (RFC 0001) |
| Maximum Packet Size | `limits.max_message_size` |
| Session Expiry Interval | Capped at `limits.max_session_expiry`, and a client that asked for more is told the granted value. 0 for a client whose roles deny `persistent` |
| Server Keep Alive | Sent when `limits.max_keepalive` is set and the client asked for more, or for zero. A client must then use the value it is given (MQTT-3.1.2-21) |
| Retain Available | 1 where some channel or the retained store can keep one, 0 where nothing on this broker can - and where it is 1, one topic's fate is still its channel's business, which is what the refusals above are for. A client whose roles deny `retained` is told the same, because that denial is about broadcast topics only |
| Shared Subscription Available | 0 for a client whose roles deny `share`, and absent for every other, which MQTT reads as 1 |

## Configuration

One file. Example:

```yaml
broker:
  id: edge-1
  log_level: info

  mqtt:
    listen:
      tcp:
        address: 0.0.0.0:8883
        tls:
          cert_file: /etc/saguin/tls/cert.pem
          key_file: /etc/saguin/tls/key.pem
      ws:
        address: 0.0.0.0:8083
      unix:
        path: /run/saguin/saguin.sock
        mode: "0660"
    password_file: /etc/saguin/clients.passwd
    allow_anonymous: false
    min_protocol_version: "3.1.1"

  operations:
    listen:
      tcp:
        address: 127.0.0.1:9090
      unix:
        path: /run/saguin/operations.sock
        mode: "0660"
    min_scrape_interval: 60s

  storage:
    default: local
    default_retention_period: 3d
    default_retention_bytes: none
    providers:
      local:
        type: sqlite
        file_path: /var/lib/saguin/saguin.db
      volatile:
        type: memory
        max_bytes: 64MiB
        snapshot_dir: /var/lib/saguin/snapshots

  retained:
    storage: local
    retention_period: none

  limits:
    max_message_size: 1MiB
    max_topic_length: 1024
    max_client_id_length: 256
    max_header_count: 32
    max_header_bytes: 8KiB
    max_connections: 10000
    max_subscriptions: 10000
    max_topic_levels: 200
    max_session_expiry: 30d
    max_keepalive: none
    publish_rate: 200
    publish_bytes: 128KiB
    write_timeout: 5s
    connect_timeout: 10s
    max_connect_size: 100KiB
    max_connect_rate: 500
    session_queue_bytes: 1MiB

channels:

  events:
    type: append
    filter: iot/+/+/events/#
    storage: local
    retention_period: 7d
    retention_bytes: 20GiB

  device-state:
    type: latest
    filter: iot/+/+/{state,location}
    storage: volatile
    retention_period: 30d

  presence:
    # No filter, so this channel claims presence/#.
    type: latest
    storage: volatile
    retention_period: none

  jobs:
    type: queue
    filter: iot/+/+/work
    storage: local
    max_bytes: 128MiB
    visibility_timeout: 30s
    job_expires_after: 6h
    retry:
      max_attempts: 5
      backoff: exponential
      backoff_base: 2s
    dlq_retention_period: 30d
    dlq_retention_bytes: 1GiB
```

There is no `type: broadcast`. A topic no filter matches is broadcast, and
configuring that would only be a way to get it wrong.

`filter` is the one key above that decides what a channel *holds* rather
than how it holds it, and everything in "The topic space" applies to it.
Of the four channels here, three divide one `iot/<site>/<device>/`
hierarchy, which is the thing a channel name could not do: an event log,
current state, a work queue. `presence` claims a tree of its own, and
`iot/hq/dev-1/diagnostics` is left as ordinary broadcast because nothing
claims it.

### What this broker is called: `broker.id`

**There is no default and it is refused when absent.** The id names this
broker in its own log lines and on `saguin_build_info`'s `broker_id` label,
and a generated one would be worse than none: the first time two brokers'
output is read side by side, the question is which machine said a line, and
an invented name answers it with something nobody can look up.

### How much the broker says: `broker.log_level`

`debug`, `info`, `warn` or `error`. Absent is `info`.

```yaml
broker:
  log_level: debug
```

| Level | What it adds |
|---|---|
| `error` | Only what stopped working |
| `warn` | Refusals a client was given, a bridge that cannot reach its peer, **and the line that takes each of those back** |
| `info` | Startup, listeners, retention sweeps, connects and disconnects - **never a line per record** |
| `debug` | Every bridge connection attempt, the detail behind a retry, and a line per record: each job offered, leased, acknowledged or returned, and each value deleted |

**A line that retracts another is written at the level of the line it
retracts** - an operator owed the moment a problem began is owed the
moment it ended, or a log read a week later reports an outage that ended
in its first minute. `bridge link up again` is therefore a `warn` like
every other clear, while a first connection stays at `info` because it
retracts nothing.

**Some lines exist only at `debug`**: a bridge writes one line per failed
connection attempt there, and a bridge that cannot reach its peer at all
is reported once at `warn` - the fault visible either way, the attempts
behind it not.

**Nothing logs once per record at `info`**, because on a busy queue that
is gigabytes a day on an edge box's disk: a queue handing out a hundred
jobs a second writes 1.9 GB a day of them being offered and acknowledged.
The counters carry those numbers (`saguin_queue_delivered_total`,
`_acknowledged_total`, `_returned_total`; RFC 0005), and what an operator
acts on - a dead-letter, an acknowledgement for a superseded delivery - is
a `warn`.

Anything a value is refused for is named:

```console
$ saguin --check-config saguin.yaml
configuration is invalid:
saguin.yaml:
  - broker.log_level "verbose": it is one of debug, info, warn or error; omit the key for info
```

**`SIGUSR1` re-reads this key, the `ws` listener's `same_origin` and
`allowed_origins`, the credential files, and the TLS certificates and
client CAs.** Getting `debug` out of a misbehaving link otherwise costs a
restart, and a restart on a broker holding durable sessions costs every
connection and every record in flight - which is the same reason
withdrawing one device's password is on this signal too, under "Who may
connect". The broker logs the level it moved from and the level it moved
to, and says in the same breath that addresses and storage are
startup-only - so an operator who edited two keys and signalled is told
which one took. A file that does not parse changes nothing and does not
stop the broker, because the likeliest reason it does not parse is that
somebody is still typing.

`SIGHUP` is caught and does nothing - unhandled, its default disposition
would terminate the process with no shutdown and no snapshot - and it
says so at `warn`, because Mosquitto reloads on `HUP`, a closing terminal
sends one, and a silent no-op reads exactly like a reload that worked.

### Where the process writes its id: `broker.pid_file`

An absolute path, or absent for nowhere.

```yaml
broker:
  pid_file: /run/saguin/saguin.pid
```

Absent is the ordinary answer, and the demo ships it commented out for that
reason: systemd, Docker and runit each track the process themselves and want
no file. It is for the supervisor that reads one - an init script, a
`kill -USR1 $(cat …)` to raise the log level or to re-read a credential
file, a monitor that only knows a number.

The path is absolute, refused if it is not, for the reason every other path
here is: two copies started from two directories would each write a different
file and each believe it held the only one.

**If the file cannot be written the broker exits** rather than running
unmanaged - something reads a pid file an operator asked for, and a stale
pid is a pid that is now somebody else. It is written after the signal
handlers are installed, or a supervisor that signals immediately would
kill the broker it just started, and it is removed on a clean shutdown.

**There is no `log_file` beside it**, and the asymmetry is the point: a pid
file holds one number and is replaced, where a log accumulates - and
invariant 13 says everything that accumulates is bounded. Bounding a log
means a size, a retention count, compression and deletion: logrotate,
reimplemented inside a broker and worse. Sagüin writes to standard output and
lets the init system bound it. RFC 0005 has the table of what does that
where, and the entry worth reading before shipping is Docker's, which bounds
nothing until somebody sets `max-size`.

### How long a delivery may wait on a consumer: `limits.write_timeout`

The longest the broker will wait to hand a packet to a client. A client
that has not taken it by then is disconnected, and the packet is undone
rather than half-sent.

**It governs every write to a client**, by two routes. Sagüin writes an
`append` or `latest` delivery, a dead-letter record and every control
reply. The substrate writes a queue's offers and every broadcast, and is
given this same duration to arm on its own writes - which it does inside
the client lock it holds across them.

**That second route matters more than it sounds.** The substrate holds a
client's lock across the write, and a write that never completes holds it
for as long as that client stays connected. Unbounded, that write freezes
the loop that offers every queue's records, expires jobs and runs both
retention sweeps - this deadline is the bound.

A queue worker disconnected this way has its leases returned as any
disconnected worker does; what it does *not* get is a lease clock, for the
reason invariant 7 gives.

```yaml
broker:
  limits:
    write_timeout: 5s
```

**The failure it prevents is a duplicate on a durable channel** - a
publisher held to a stranger's socket past its own patience gives up
inside the window and re-sends, whatever the window is set to. So a
publisher is not one of the goroutines that can be held: it waits on
storage and on nothing another client does, lengthened only by
the commit in progress, the next one, and where set
`publish_commit_interval` below, the operator's own figure. Invariant 16
carries the mechanism and the measurement.

**Disconnecting is the answer rather than dropping**, because a timed-out
write has already put part of a packet on the wire and there is no way to
continue that stream. It costs the consumer nothing: a durable consumer's
position advances only on its `PUBACK`, so a consumer disconnected here
reconnects and resumes at the record it had reached. Nothing is lost and
nothing is duplicated.

That is the consumer granted QoS 1. One granted QoS 0 sends no `PUBACK`,
so its position advances on the write instead and what was on the wire
when the link broke is behind it (RFC 0003). Hanging up costs that
consumer the records it had not read, which is the trade QoS 0 is.

**Dropping is what a broker without a stored position has to do.**
Sagüin can hang up instead precisely because a channel consumer has a
position: the records are still in the channel, and a reconnect replays
them. The bound is the same; what it costs is not.

**Broadcast is the exception to the buffering, and to nothing else.** A
topic no channel claims has no cursor to resume from, so a subscriber
loses what could not be sent to it. Two counters, two different moments
(RFC 0005): `saguin_deliveries_dropped_total` is the outbound queue
overflowing, `saguin_deliveries_refused_total` the step before it, and
watching one without the other sees half of what a slow broadcast
subscriber costs.

**The deadline still reaches it.** Dropping is what happens instead of
*buffering*, never instead of `write_timeout`: a socket that takes no
write for that long is disconnected whoever is on it and whatever the QoS.
A deaf QoS 0 broadcast subscriber therefore gets both, in that order -
shed for as long as its socket accepts writes, hung up on when it stops.
Reading either half as the whole rule sends an operator looking for the
wrong symptom, and a short measurement shows only the first.

A broadcast subscriber served at QoS 1 or 2 never reaches the drop: it is
held within its session's bound instead (`limits.session_queue_bytes`
below).

**Why it is configuration.** It is the time a *slow link* is allowed, and
Sagüin's deployments are the ones where slow is normal - a ship, a
substation, a vehicle on a metered connection. The default of `5s` is
generous for any link that is working: a write only waits at all once the
kernel's own buffer is full, which means the far end has stopped reading
rather than merely being slow. `none` removes the ceiling and restores the
behaviour above, which is a choice an operator may make and should have to
write down.

### How long a socket may wait to send CONNECT: `limits.connect_timeout`

A connection is not a client until its `CONNECT` arrives. Until then there
is no keepalive to bound it, so without a bound a peer that opens a socket
and sends nothing holds a descriptor for as long as it likes.

**It holds a `max_connections` slot from the moment it arrives**, as
mosquitto, NATS and EMQX count connections, so everything a connection
costs before authentication is bounded by the same number as everything
after it. A peer holding slots with silent sockets holds them until this
bound closes them.

**Past the slots there is one overflow budget**: `max_connections` or 32
places, whichever is fewer, shared by every socket that arrives with
every slot taken and has not been admitted. A socket that finds a place
waits for a slot for up to 50 ms; a slot given back goes to the socket
that has waited longest, and one arriving meanwhile queues behind it
rather than taking the slot first. One whose wait ends with no slot keeps
its place while the opening bytes of its `CONNECT` are read, to be
answered `0x89` (Server busy; 3.1.1 `0x03`) with no credential checked.
Every socket that is not admitted is closed within 100 ms of its arrival,
or `connect_timeout` where that is sooner, its wait included. A socket
that arrives with the budget full is closed at once, with nothing read
and nothing written. So the sockets the broker holds are at most
`max_connections` plus the budget, besides the one a door has just
accepted and is closing for want of a place, and a connection whose end
is decided, whose socket outlives its slot as below; and a client that
hung up and connects again at once finds the slot it gave back. A stopping
broker admits nothing more and ends every wait at once.

**It gives the slot back the moment the broker decides the connection
ends**, not when the socket closes: before it writes the `CONNACK`
refusing a connection or the `DISCONNECT` ending one, and as it reads a
client's own `DISCONNECT` or close. A client turned away or disconnected
can connect again at once, as with mosquitto. From then on the
connection is ending: nothing more is read from it, and nothing is
written to it but the `CONNACK` or `DISCONNECT` that ends it. The socket
closes after that, so it outlives its slot by what comes between:
writing that last packet, which `write_timeout` bounds, and what must
happen before the close - storing what a client's `DISCONNECT` changed,
or publishing the Will of one that dropped.

**On a `ws` door a connection arrives as a socket before it is an MQTT
connection**, so its slot is taken when the socket is accepted, before the
HTTP upgrade. One that gets no slot is closed with nothing written, since
there is no MQTT connection yet to answer; mosquitto and EMQX close it
the same way. The door speaks HTTP/1.1 only. A request the door
answers instead of upgrading - one that is not an upgrade, from a page
whose origin is refused, or malformed - gives its slot back before that
answer is written. **This bound runs once, from the moment the
socket is accepted**, over its TLS handshake, its HTTP request - any body
the request declares included - and the `CONNECT` that follows, together
rather than each in turn: a socket that has not sent its `CONNECT`
`connect_timeout` after it arrived is closed, whichever of them it is
still in.

```yaml
broker:
  limits:
    connect_timeout: 10s
```

A connection whose `CONNECT` has not arrived within `connect_timeout` is
closed without a reply. Once it arrives the client's own keepalive governs
the connection instead. The default is `10s`.

It is a whole number of seconds, `1s` or longer. `none` is refused: this
bound is the only thing between an idle socket and the broker's
descriptors.

**A stopping broker does not wait for it.** Shutdown closes every
connection that has not sent `CONNECT` rather than waiting out this bound,
so the snapshot written after the listeners close is not delayed by a
peer that says nothing.

### The largest CONNECT before authentication: `limits.max_connect_size`

A `CONNECT` is read before its client has authenticated, so its size is
memory a stranger chooses. `max_connect_size` bounds the whole packet, fixed
header included:

```yaml
broker:
  limits:
    max_connect_size: 100KiB
```

The limit is checked against the length the `CONNECT` declares, before its
body is read. A larger one is answered `0x95` (Packet too large) and closed;
an MQTT 3.1.1 client, which has no such code, is closed with nothing written.
Either way the rest of the packet is not read.

The default is `100KiB`, or `max_message_size` when that is smaller, and the
key may not be set larger than `max_message_size`. A `CONNECT` whose fields
stay within MQTT's own limits - a Will payload of up to 65,535 bytes, a
password of the same - fits. A fleet sending larger ones raises it; a broker
short of memory is better served by a lower `max_connections`, because much
of what a handshake holds does not depend on the size of its `CONNECT`.

### How fast connections are accepted: `limits.max_connect_rate`

`max_connect_rate` is how many connections each MQTT listener accepts a
second, with bursts of up to twice that, or `none` for no limit:

```yaml
broker:
  limits:
    max_connect_rate: 500
```

**A connection over the rate waits; it is not refused.** It stays in the
operating system's accept queue until the listener takes it, so a fleet that
reconnects all at once after an outage is admitted more slowly rather than
turned away. What it bounds is the work done for connections that have not
yet authenticated, a password check each; memory is bounded by
`max_connections` and `max_connect_size`.

The default is `500`. At that rate 10,000 devices reconnecting together are
all admitted within about 20 seconds. A broker on a small processor lowers
it; one expecting large reconnect storms on ample hardware raises it or
writes `none`. The operations listener is not limited by it.

### How long a client may go quiet: `limits.max_keepalive`

A client chooses its own keepalive, and MQTT lets a server answer with one
of its own that the client must then use (MQTT-3.1.2-21). `max_keepalive`
is that answer, as a duration, or `none` - the default - for no ceiling.

```yaml
broker:
  limits:
    max_keepalive: 5m
```

A client asking for longer is given `5m` in its CONNACK, and so is a client
asking for **zero**, which MQTT defines as "never disconnect me for being
idle". Nothing is refused: the client is told the value and uses it.

A client already inside the ceiling is left alone and its CONNACK carries no
Server Keep Alive.

**A 3.1.1 client keeps whatever keepalive it asked for** - Server Keep
Alive is an MQTT 5 property, and a ceiling enforced against a client that
cannot be told it is a flap loop written into the configuration file - so
in a mixed fleet this ceiling is a property of the MQTT 5 half.

**What the ceiling is worth is how quickly a dead client is noticed.** A
session the broker cannot tell is gone lasts until `max_session_expiry` - a
much longer clock, and the one that releases a durable consumer's stored
position. A shorter keepalive makes the broker's own answer the answer.

The ceiling is a whole number of seconds and MQTT carries it in two bytes,
so anything above 65535 seconds is refused rather than truncated:

```console
$ saguin --check-config saguin.yaml
configuration is invalid:
saguin.yaml:
  - limits.max_keepalive "20h" is longer than MQTT can express, which is 65535 seconds or about 18 hours; write `none` for no ceiling
```

### How much a session may hold: `limits.session_queue_bytes`

The most one session may hold of deliveries its client has not acknowledged,
as a size. Absent is `1MiB`, or `max_message_size` where that is larger.

```yaml
broker:
  limits:
    session_queue_bytes: 4MiB
```

**What it counts is what a session is owed**: every QoS 1 and QoS 2
delivery on the wire and unacknowledged, every one waiting for room in the
client's Receive Maximum, and every one waiting while the client is away -
for broadcast to a session that outlives its connection, the messages
after its cursor in the broadcast log that its filters matched when they
were published (RFC 0003 "Broadcast"). Each is counted as its payload,
topic and properties, and beside them as the memory of its entry, measured:
about 600 bytes for one in the client's in-flight table, and 300 once for
a table that holds any; 80 for one owed from the broadcast log and waiting,
its entry on the session's list; and for one owed from the log and on the
wire, both of those and the 192 bytes that record its packet identifier,
and 600 once while the session has any of those on the wire. So what holding
them costs is inside the bound however small the messages: at 128-byte
messages a 1MiB bound is about 4,000 waiting for a session that is away, and
about 500 on the wire for one that is connected.

**Full means the oldest goes, and the publisher is never refused.** Past the
bound a session gives up the oldest it is owed that is not on the wire - for
broadcast from the log, its cursor moves past it - so a device that comes
back is sent the most recent of what was published while it was gone. Each
is counted as `session_queue_full`. The publisher is answered once its
message is written, and told nothing of what any session gave up, because
nothing about its publish failed (RFC 0003 "Broadcast").

**Some deliveries are never taken back.** A QoS 1 delivery on the wire to a
connected client stays until it is acknowledged: dropping it frees its packet
identifier while the client may still acknowledge it, and that
acknowledgement would complete another message. A QoS 2 delivery stays for
the same reason with a worse outcome, since a client holding an identifier
from an unfinished exchange discards a new message under it as a duplicate.
And a channel's own record or a queue's offer stays, because Sagüin tracks
each under its identifier - a position or a lease would be left pointing at
nothing.

**So no more than half the bound is written and unacknowledged.** A delivery
past that half waits to be written, and is written as acknowledgements make
room, earliest first; one is always written when nothing is on the wire,
however large. What waits can be taken back, so a client that stops reading,
or reads and never acknowledges, holds its bound as the oldest it was written
and the newest it was not, and is sent the newest once it recovers.

**It is not disconnected for holding its bound.** Its memory is bounded, and
`limits.write_timeout` hangs it up only once its socket stops accepting
writes - which, with a bound the kernel's socket buffers can absorb, it may
never do. It stays connected until it recovers or its session ends.

**A session at its bound holding nothing it can give up is refused the next
delivery** - deliveries at QoS 2 - and each refusal is counted beside the
drops. **A channel's own record is not refused: it waits where it is.** An
append consumer's position holds and a `latest` value stays pending, past
half the bound as any delivery is, and each is written as acknowledgements
make room - so a consumer that reads and never acknowledges holds half its
bound of a channel's records, and loses none of them. A session holding
nothing it cannot give up always has room, so a message as large as the
bound is never refused for its size alone. Otherwise a session passes its
bound only by the delivery being queued, and only until the oldest it can
give up is gone. There is no count of messages as well: a client's own
Receive Maximum is the window, up to the 65,535 MQTT allows.

**Sizing it.** The bound is memory the broker allocates for a session, and
the process's resident size runs about twice what it allocates, because the
runtime keeps room to collect in: measured at up to 1.3MB per session at the
1MiB default, with a thousand sessions each holding its bound, on both
session providers. A broadcast message is stored once in the log however
many sessions are owed it, and each session is charged its size and the
80 bytes above, so at 1MiB a session that is away holds 64 messages of 16KB
against the 4,000 of 128 bytes above. A deployment that broadcasts large
messages to subscribers that fall behind sizes the bound from the burst they
must ride out: half a second of 16KB messages at 500 a second is 4MiB.

**`none` is refused, and so is a size below `max_message_size`.** The first
is a session that stops reading holding everything it is sent, which is the
unbounded buffer invariant 13 refuses; the second could never queue the
largest message a client may publish.

`saguin_session_deliveries_dropped_total{cause="session_queue_full"}` counts
what was dropped, and `saguin_session_queue_messages` and
`saguin_session_queue_bytes` what every session holds (RFC 0005).

### How many topic filters one client may hold: `limits.max_subscriptions`

The most topic filters one client may hold at once. Absent is `10000`.

```yaml
broker:
  limits:
    max_subscriptions: 100
```

**A filter is memory that no other bound counts**: measured at about
2.3KB across the indexes that hold it. Without a count, one connection
subscribing to 100,000 filters held 201MiB. At the default, one client
holds at most about 23MB of them.

**The default is high on purpose.** Home Assistant subscribes once per
entity, so thousands of filters on one client is ordinary. An edge box
whose devices each hold a few filters can set `100`.

**What counts is what the client would hold after the SUBSCRIBE.** A filter
it already holds is replaced and adds nothing. A filter named twice in one
packet is one. A shared subscription is one filter like any other. A filter
given up by `UNSUBSCRIBE`, or by a refusal for any other reason, makes
room.

**Past it, each filter over the count is refused `0x97` (Quota exceeded)**,
with a Reason String naming the key, and the rest of the packet is answered
as it would be. The connection stays up. A 3.1.1 client is sent `0x80`,
that protocol's one failure code. The refusal is said at `WARN` once per
episode: the first refusal since the client last added a filter. A
re-subscribe to a filter it holds adds none.

A value below `1` is refused, since it would refuse every SUBSCRIBE.

**A lowered value bounds what is added, not what is held.** A session
restored holding more filters than a lowered `max_subscriptions` keeps them
all, since ending it would drop what it is owed; its next SUBSCRIBE of a
new filter is refused until `UNSUBSCRIBE` brings it under. A lowered
`max_topic_levels` is the same.

### How deep a topic may be: `limits.max_topic_levels`

The most levels a topic name or a topic filter may have. A level is what
lies between two `/`, so a string has one more level than it has `/`.
Absent is `200`. A 201-level topic a bridge brings in from a peer with a
laxer limit is dropped as a record Sagüin would never accept
(`saguin_bridge_unstored_total{cause="never_accepted"}`, RFC 0005).

```yaml
broker:
  limits:
    max_topic_levels: 32
```

**Every level of a filter is memory.** The topic index holds a node for
each, measured at about 600 bytes. `max_topic_length` does not bound
filters, and a SUBSCRIBE packet may carry a filter of 65,535 bytes. Without
this key, one filter of 30,000 levels took 17MiB and two seconds of CPU,
and a client could hold `max_subscriptions` of them. At the default, one
filter holds at most about 120KB.

**One rule, wherever a topic or a filter enters.**

- A publish over it is refused `0x90` (Topic Name invalid), as one over
  `max_topic_length` is. That includes a record a bridge brings in.
- A Will over it is refused at `CONNECT` with `0x90`.
- A `SUBSCRIBE` filter over it is refused `0x8F` (Topic Filter invalid),
  or `0x80` on 3.1.1, and the rest of the packet is answered as it would
  be.
- A point read's key over it is refused `0x90`.

A shared subscription is measured by the filter after `$share/<ShareName>/`.

**A dead letter is one level deeper than its record**, because its topic
gains `__dlq` ("The dead-letter channel"), so a 200-level record on a queue
dead-letters under a 201-level topic. The move writes it directly, not
through a publish, so it is kept. A subscriber reaches it with a shallower
filter ending in `#`, such as `jobs/__dlq/#`. A point read of it is refused,
and a Sagüin peer an `out` rule ships it to refuses it `0x90`, which the
rule counts as `peer_refused` and skips.

**The files are held to it at startup.** A channel's filter, a bridge
rule's filter and `topic:` template, and a filter or topic in the
`acl_file` deeper than this could match nothing a client may publish, so
`--check-config` refuses the file and names the line.

**A lowered value bounds what is added, not what is held**, as a lowered
`max_subscriptions` does. A session restored holding a filter deeper than
it keeps the filter, since ending the session would drop what it is owed,
and an `UNSUBSCRIBE` is not held to it at all: it allocates nothing, and
it is how a client gives up such a filter.

A value below `1` is refused, since it would refuse every publish and
every SUBSCRIBE. There is no `none`: a topic tree deeper than 200 levels is
not one anybody writes, and the bound is what stops one client from taking
the broker's memory one level at a time.

### How fast one client may publish: `publish_rate` and `publish_bytes`

Every other bound here is a **size**, and a size does not bound a
**rate**: one client publishing as fast as it can is limited only by what
it lands in. A channel at its `max_bytes` refuses with `0x97`, which is
backpressure and works - and **a broadcast topic has no size bound at
all**, so a device with a firmware bug looping on one would be answered as
fast as the broker could go, for as long as it liked.

That is invariant 13 arriving through the one thing no size key bounds -
work per second - and on the hardware Sagüin is for it is a likelier
failure than anything the size bounds catch.

**Two units, because they bound different things and either alone leaves
the other hole open.** A thousand one-byte publishes cost a kilobyte and a
thousand topic lookups, permission checks and store writes - which is what
actually saturates Sagüin, and why its own benchmark is in messages a
second. Ten one-megabyte publishes cost ten operations and ten megabytes. A
device can exhaust the broker either way, so both are counted and whichever
is reached first refuses. Either may be written alone.

**Two places, and they compose.** The broker-wide figures are the floor
every client takes:

```yaml
limits:
  publish_rate:  200      # messages a second
  publish_bytes: 128KiB   # bytes a second
```

and the `acl_file` names the exceptions, beside the roles rather than in a
block of their own:

```yaml
users:
  demo: [tour]                    # short form, unchanged
  "device-*":
    roles: [device]
    limits:
      publish_rate:  100
      publish_bytes: 64KiB
  "gateway-*":
    roles: [gateway]
    limits:
      publish_rate:  5000
      publish_bytes: 8MiB
```

One pattern, one entry. A separate top-level block keyed by the same
patterns would have every operator writing each device pattern twice - two
places to keep in step, and a typo in the second one silently meaning no
limit at all.

**The broker-wide figures apply to everybody**: anonymous clients, clients
no pattern matches, and brokers with no `acl_file` at all - which is most of
them. **A named client replaces them, up or down.** Tightest-wins would
make the broker-wide figure a ceiling nobody could exceed, and a gateway
that legitimately publishes faster than a sensor is most of why per-client
limits exist.

| The client | Takes |
|---|---|
| Anonymous, or no `acl_file` | the broker-wide figures |
| Authenticated, no entry applies to it | the broker-wide figures |
| The entry that applies carries `limits:` | that entry's figures, instead |
| The entry that applies carries none | the broker-wide figures |

**Which entry applies is settled once, under *Roles, and users matched by
pattern*,** and it is one rule for the whole entry rather than a rule of its
own for limits: where several patterns match a client's user name, the one
that spells it out most exactly supplies both its roles and its limits, and
the others do nothing. The figures follow the user name and the budget the
client id, so a client taking over another's id publishes at its own
figures, not the ones the id's last holder had.

**Absent means no bound, and that is the default.** There is no figure to
pick: what a deployment sustains is its storage's rate, and memory and
sqlite differ by an order of magnitude. A default drawn from either would
throttle a legitimate fleet on the other. A written `0` is refused at
startup rather than read as "unset" - those mean opposite things, and one
of them is a broker that refuses every publish.

**A message larger than the whole per-second byte budget is admitted**, and
spends it into deficit rather than being refused. Refusing it every time
would make `publish_bytes` a silent ban on large messages rather than a
rate, which is a different feature and not this one; `max_message_size` is
where a size ceiling belongs.

**Over the rate is `0x97` (Quota exceeded), and the connection is left
alone**: disconnecting turns a burst into a reconnect storm, and it is
not needed - the refusal happens before the topic is resolved, before any
authorization is asked and before anything is stored, so nothing
accumulates whatever the client does.

**A budget belongs to the client and survives its connection** -
otherwise a device could have as many second's-worth as it could open
connections, and a library that treats the `0x97` as fatal and reconnects
does exactly that by itself. The budget is dropped once it has refilled,
so a client that has genuinely been quiet loses nothing.

**At QoS 0 there is no reply**, so such a publish is dropped and the
publisher is told nothing - the same silence every refusal takes at that
QoS, since the only packet a server could send unsolicited is the
`DISCONNECT` this refusal exists to avoid.

The log line is written at debug rather than warn, because a client held to
its rate produces one per refused publish and a limiter that floods the log
under load has moved the problem rather than solved it. The metric
`saguin_publish_refused_total{reason="publish rate exceeded"}` is where an
operator sees it, and it is deliberately its own series - see the table
below.

### Channels split across files

One file stops being the right shape somewhere well below the ten thousand
channels a real fleet reaches. Channels belong to domains, those have
different owners and different rates of change, and a single file makes
every one of them a merge conflict. So `channels:` may also be written as
a list, whose entries are each a group of channels - a file named by
`!include`, or channels written in place:

```yaml
channels:
  - !include channels/telemetry.yaml
  - !include channels/vessel-07/jobs.yaml
  - events:
      type: append
      storage: local
```

An included file holds the same thing the block above holds: channel names
and their settings, and nothing else.

```yaml
# channels/telemetry.yaml
readings:
  type: append
  storage: local
  retention_period: 7d
```

The single mapping is still the whole of it for a configuration that needs
one file, and nothing about it changes. The rules the list follows:

- **Only under `channels`.** This is not a general preprocessor. A file
  that can include anything anywhere cannot be reasoned about, and the
  thing an operator wants to split is the list that grows.
- **Paths resolve against the including file, never the process
  directory.** Otherwise `make demo` and a systemd unit disagree about
  what the same configuration means, which is the worst class of
  configuration defect.
- **No nesting.** A master file names files, and those files name none -
  nesting buys little and costs cycle detection, a depth bound, and an
  error that explains a chain. Refused from both directions.
- **A channel name defined twice is a hard error naming both files** -
  two domains each defining `events`, one silently winning, is the real
  hazard of the shape - and two channels carrying the same `filter` is
  the same hazard refused the same way.
- **The order the files are named in decides nothing** - which channel
  holds a topic is the filters' own business ("Which channel a topic
  belongs to"), so a domain's file can be added, moved or sorted without
  moving anybody's records.
- **No globs.** `!include channels/*.yaml` makes what the broker serves
  depend on a directory listing, and an editor's backup file becomes
  configuration. Explicit names cost one line each and can be reviewed.
- **An unknown key stays a hard error**, and the message names the file it
  is in. Losing the position is most of what would make a split
  configuration worse than a single one.

One caveat on that last rule: an `!include`d file's positions are exact,
where a group written in the *master* reports a line counted from the
start of that group, so a comment inside the group moves the answer.

### Storage providers split across files

`broker.storage.providers` takes the same two forms and follows the same
rules, for the same reason: the team that owns a domain's channels usually
owns the disk they sit on, and splitting the channels while leaving every
provider in one file leaves that team editing the master anyway.

```yaml
broker:
  storage:
    default: local
    providers:
      - !include storage/local.yaml
      - !include storage/vessel-07.yaml
```

A channel in one file names a provider defined in another. Resolution is
by name and that is the point of it - a provider is a name a channel
refers to, not a place in the document.

One rule is stronger here than for channels. **Two provider names on one
`file_path` or one `snapshot_dir` are refused at startup, naming both
providers and both files** - two names on one store are two writers on
it, each with its own bound and its own retention. Paths are compared
after cleaning, so `/var/lib/x` and `/var/lib/./x` are the same store; a
symlink can still make two paths one store, and what Sagüin refuses is
the case visible in the text.

**One thing it cannot refuse: the total.** Two sub-operators each
declaring 8GiB of memory on a 2GB box is not a clash - each provider is
separately reasonable and only the sum is not, and Sagüin does not know
what the machine has. So the startup line states the total declared across
memory providers, in the one place somebody is looking, and the arithmetic
belongs to whoever owns the box.

### How a sqlite provider commits publishes

Committing has a cost that does not depend on how much is in the
transaction, so paying it once per record is what bounds the write rate of
a provider.

**When these two keys are absent, a `sqlite` provider collects without
waiting.** A publish that finds no transaction committing is stored at
once, in a transaction of its own. Publishes that arrive while one is
committing are stored together in the next, which starts the moment it
ends. Nothing waits for company, so a lone publisher is answered as fast
as with a transaction per publish, and under load a transaction holds
whatever arrived during one commit - the batch grows with the traffic
rather than with a setting. One transaction holds at most 256 records;
the next arrival starts another, committed after it.

`publish_commit_interval: none` gives every publish its own transaction.
An operator may instead ask for the publishes that arrive close together
to be stored in one transaction, which waits for them:

```yaml
  storage:
    providers:
      local:
        type: sqlite
        file_path: /var/lib/saguin/saguin.db
        publish_commit_interval: 2ms         # absent to collect without waiting; `none` for a transaction per publish
        publish_commit_max_records: 32       # required with it, and there is no default
```

**A transaction closes at whichever of the two is reached first** - when
`publish_commit_max_records` records have arrived, or when
`publish_commit_interval` has passed since the transaction opened,
whichever happens sooner. Under load the record count is what closes it
and the interval never fires; when little is being published the interval
is what closes it, and it is the only thing that can, because a lone
publisher never reaches the count.

#### What each key accepts, and why it stops there

Everything below is refused at startup, naming the key and the reason. A
setting whose wrong values are accepted in silence is a defect waiting for
somebody to write one.

| | `publish_commit_interval` | `publish_commit_max_records` |
|---|---|---|
| **Absent** | collect without waiting - the default | must also be absent |
| **Accepted** | `none`, or a duration from `1ms` to `1s` | a whole number from **2** to **`limits.max_connections`** |
| **Refused** | anything longer than `1s`; anything unparseable; either key on a `memory` provider | `1` or less; more than `limits.max_connections`; absent while `publish_commit_interval` is set; set while `publish_commit_interval` is not |

**Why `1s` is the ceiling on the interval**, and why single-figure
milliseconds is the useful range, is three things and the first surprises
people:

1. **The wait is paid per connection, not per batch.** Sagüin stores one
   record per connection at a time - a connection's next `PUBLISH` is not
   read off the socket until its last is stored - so a client that
   publishes and waits for its acknowledgement gets exactly one record per
   interval, whatever else is happening. At `1s` that is one message a
   second from that client: twenty publishes issued at once down one
   connection against a `50ms` interval take 1.019s.
2. **It is not a wait for readers, though it looks as though it should
   be.** A channel's reads run beside the writer ("How a sqlite provider
   reads", below), and even with `read_connections: 0`, where they share
   the one write connection and an open transaction does hold it, the
   leader waits out the interval holding *nothing* and opens the
   transaction only afterwards. So the connection is free for the whole
   interval and busy for the commit alone: with a publisher waiting out a
   `1s` interval, metrics scrapes on the same provider answer in under 3ms,
   and window reads on the collecting channel in at most 2.5ms with the
   wait doubled. What a reader does wait for is the transaction itself,
   which is longer with a batch in it - and that is bounded by
   `publish_commit_max_records`, not by this key.
3. **Nothing beyond a few milliseconds buys throughput.** The record count
   is what collects a batch; the interval only decides how long a batch
   that will not fill waits before giving up.

**Why `2` is the floor on the count.** A transaction that may hold one
record still waits out `publish_commit_interval` before committing it -
strictly slower than a transaction per publish, while reading in a file as
though collecting were on. `none` is a transaction per publish, and leaving
both keys out collects without waiting.

**Why `limits.max_connections` is the ceiling on the count**: a batch can
never hold more records than there are connections to have sent them, so
a larger count is unreachable by construction - every transaction would
wait out the interval and commit a handful. The ceiling is not a target:
what fills a batch is the connections publishing **at the same time**, so
a broker with `max_connections: 10000` and forty sensors wants a count
near forty.

`publish_commit_interval` is what makes a transaction wait.
`publish_commit_max_records` on its own is refused, because nothing would
wait for the count and the file would read as though it set one.

**And there is no default for `publish_commit_max_records`**, because the
figure that decides whether any of this helps is how many connections
publish at the same time, and Sagüin does not know it.

A batch that fills on the record count commits the moment the last record
arrives. A batch that does not fill waits out the interval and commits
whatever it has - so a count set above the traffic turns every commit into
a fixed wait and is **slower than not collecting at all**. Through the
wire, by `BenchmarkPublishConcurrently`: each publisher a connection of
its own, waiting for its acknowledgement before sending the next 128-byte
record at QoS 1, the database on the machine's own disk - an AMD Ryzen 7
260 on ext4 over NVMe, the broker and its clients held to 8 of its 16
threads - with the two right-hand columns at `publish_commit_interval:
2ms`:

| connections publishing | `none` | keys absent | `publish_commit_max_records: 256` | `publish_commit_max_records` at the publisher count |
|---|---|---|---|---|
| 1 | 7,100–10,100/s | 7,500–11,000/s | | |
| 8 | 10,100–11,000/s | 25,000–36,600/s | 3,400/s - count above the traffic | 32,900–33,400/s |
| 32 | 10,200–10,900/s | 56,000–69,500/s | 12,200/s - count above the traffic | 56,700–76,100/s |
| 256 | 10,300–10,800/s | 89,300–118,200/s | 77,200–79,900/s - count met by the traffic | 73,600–92,200/s |

Read the columns against `none`. A transaction per publish holds the
provider near ten thousand records a second however many connections
publish. With the keys absent the batch is whatever arrived during the
commit before, so it grows with the traffic, and the rate with it - as
fast as a count set to the publishers, without the deployment having to
know the number. Where a count is above the traffic it is never reached,
so every transaction waits the full 2ms and commits a handful of records:
at eight publishers that is **three times worse than `none`** and at
thirty-two no better.

**Set `publish_commit_max_records` at or below the number of client
connections you expect to be publishing at the same time.** Too low costs
a little - a batch closes on the count more often, and each is smaller -
and too high costs everything.

**Connections are what fill a batch, and in-flight depth is not** - one
connection is one record per interval whatever its Receive Maximum, and at
QoS 0 alike, since the record is stored on the way through. The
`mosquitto_pub -l` shape is the same trap from the other side: one
connection sending a file of lines is one record per interval, so it looks
as though collecting made the broker slower. It did, for that client.

Two channels that want different answers go on different providers. Each
`sqlite` provider is its own file, its own writer and its own setting,
so a channel carrying sensor readings can collect and a channel carrying
control messages beside it need not.

Nothing about the durability changes. A record is stored before its
publisher is told anything either way; the transaction is simply larger and
the wait for it is longer. A crash between the commit and the
acknowledgements leaves records stored and unacknowledged, so the publisher
re-sends and the channel holds them twice - which is the window a single
commit already has, widened by the interval.

Neither key means anything on a `memory` provider, which has no
transaction to collect into, and both are refused there.

### How a sqlite provider reads

A provider writes on one connection. **A consumer's read of a channel runs
on a read-only connection beside it**, so a window of records never queues
behind a commit, and a commit never waits for a window being read.
`read_connections` is how many of those a provider may open:

```yaml
  storage:
    providers:
      local:
        type: sqlite
        file_path: /var/lib/saguin/saguin.db
        read_connections: 2                  # absent for 2; 0 puts every read on the write connection
```

| | `read_connections` |
|---|---|
| **Absent** | `2` |
| **Accepted** | a whole number from `0` to `8`. `0` opens none: every read runs on the write connection, behind the writer |
| **Refused** | below `0`; above `8`; on a `memory` provider |

**What it buys is consumers reading while publishers write.** On one
connection every consumer's read waits for the commit in progress, and
every commit for the reads queued ahead of it. Fifteen consumers draining a
provider that 16KB publishers keep saturated, on eight CPUs, at `0` and at
the default `2`:

| | `read_connections: 0` | `read_connections: 2` |
|---|---|---|
| publishes a second | 1,291-1,374 | 1,940-3,013 |
| deliveries a second | 19,337-20,587 | 28,712-43,822 |
| `PUBACK` p50 | 94-96 ms | 42-62 ms |
| process peak memory | 76-81 MiB | 130 MiB |
| write-ahead log peak | 3 MiB | 111-315 MiB |

At sixty-four consumers `0` holds publishers to 359-375 a second, each
waiting a third of a second for its `PUBACK`, where `2` gives them
1,078-2,230.

**What it costs is memory and disk**, which is the reason to turn it down.
Each read connection is a connection to the same database with a page
cache of its own, opened when a read asks for it and kept for the next.
And a reader holds the write-ahead log open while it reads, so under
sustained reads the log grows beside the file until a checkpoint finds a
moment with no reader in it (RFC 0004). **A small box sets `1`**, which
still keeps reads off the writer, **or `0`**, one connection for
everything. More read connections trade publishers for deliveries: past
one or two, each reader added takes CPU the writer needed, so publishes
fall, while deliveries rise only where many consumers read at once. Above
eight a read waits for a CPU rather than for a connection, and each one is
still a page cache.

What a read returns does not change with it. Each read is one snapshot,
begun for that read and ended with it: the channel's floor and the records
above it come from the same instant, and every read sees every commit
before it, its own client's included. Only a channel's records are read
there. The broadcast log's reads decide what its deliveries acknowledge and
remove, so they stay on the write connection, as does any read made inside
a write. RFC 0004 has what a reader does to the write-ahead log.

### How often a sqlite provider forces its log to disk: `flush_interval`

A provider forces its write-ahead log to disk on an interval, so that a
power cut loses about one interval plus one fsync (RFC 0004 "WAL, and
`synchronous=NORMAL`"):

```yaml
  storage:
    providers:
      local:
        type: sqlite
        file_path: /var/lib/saguin/saguin.db
        flush_interval: 150ms                # absent for 150ms
```

| | `flush_interval` |
|---|---|
| **Absent** | `150ms` |
| **Accepted** | a duration from `10ms` to `1s` |
| **Refused** | `none` and `0`, since there is no setting that turns it off; below `10ms`; above `1s`; anything unparseable; on a `memory` provider |

**It trades acknowledged data lost to a power cut against disk wear.** A
shorter interval loses less and writes more; RFC 0004 has both figures. An
idle broker fsyncs nothing.

### Which web pages may connect: `same_origin` and `allowed_origins`

A browser opening a WebSocket sends an `Origin` header naming the site its
page came from. Two settings on the `ws` listener decide which pages may
connect, and a page that fails is refused with `403` before the upgrade:

```yaml
broker:
  mqtt:
    listen:
      ws:
        address: 0.0.0.0:8083
        same_origin: false
        allowed_origins:
          - https://dashboard.example.com
          - https://192.0.2.10:8443
```

| | |
|---|---|
| `same_origin` | The page's scheme, host and port must be the request's own: `https` on a TLS listener, and the host and port the browser connected to. Default `true` |
| `allowed_origins` | The page's origin must be one of these. Default empty, which checks nothing |

**When both are set, a page must pass both.** A dashboard served from
another site is therefore listed with `same_origin: false`. With
`same_origin: false` and no list, every page is admitted.

These are the NATS server's `same_origin` and `allowed_origins`, with the
same meaning. **`same_origin` defaults to `true`**, so with no list only a
page from the listener's own site is admitted.

**Why a page from another site is refused.** Without the check, any site the
operator's browser visits could open the listener as that browser: with the
client certificate the browser holds, or on a network where anonymous
clients are admitted.

**Behind a proxy that terminates TLS, `same_origin` refuses every page.**
The page is `https` and the request the listener receives is not, so the
schemes never match. Such a listener is configured with
`same_origin: false` and its pages listed.

**`same_origin` alone does not stop DNS rebinding.** A page whose domain is
re-pointed at the broker's address names that domain in both `Origin` and
`Host`, so it is its own site. A list stops it, because the attacker's
domain is never on it.

**A client that is not a browser is not affected.** paho.mqtt.golang,
autopaho and MQTT.js under Node send no `Origin`, and a request without one
is admitted. paho-mqtt for Python sends the broker's own scheme, host and
port, which `same_origin` admits; with a list, that address is listed too.

**An entry is an origin and nothing else**: `scheme://host` or
`scheme://host:port`, a different port a different site, compared as a
browser sends them. No wildcards, and an entry with a path, a query or a
user name is refused - a browser never sends one. `Origin: null` cannot
be listed: a sandboxed frame on any site sends it too.

A refused upgrade is logged at `warn` with its origin, bounded. Both
settings exist on the `ws` listener only and are re-read on `SIGUSR1`; a
file that is not valid changes nothing.

### TLS on a listener

A `tcp` or `ws` listener may carry a `tls` block naming a certificate and
its key, and so may the operations listener. Both paths are absolute, for
the reason every path here is: what a listener serves must not depend on
where the broker was started from. Making the files is three openssl
commands per certificate, and the recipe closes the client-certificates
section below.

```yaml
      tcp:
        address: 0.0.0.0:8883
        tls:
          cert_file: /etc/saguin/tls/cert.pem
          key_file: /etc/saguin/tls/key.pem
```

**Several listeners of a kind.** A kind with one door is written as a
mapping under `tcp`, `ws` or `unix`, named after its kind. A kind with two
or more is a list, and each entry then carries its own `name`, unique across
every MQTT door:

```yaml
      tcp:
        - name: local
          address: 127.0.0.1:1883
          allow_anonymous: true
        - name: fleet
          address: 0.0.0.0:8883
          tls:
            cert_file: /etc/saguin/tls/cert.pem
            key_file: /etc/saguin/tls/key.pem
```

So a plain port for local tools sits beside a TLS port for the fleet on
the same kind, each with its own address, TLS and auth - `password_file`,
`allow_anonymous`, `tls` with `client_ca_file`, `proxy_protocol` on a
Unix door, `ws`'s origins. `broker.mqtt.acl_file` stays one for the whole
broker, as does `limits.max_connections`, which is one count across every
door of every kind. `broker.operations.listen` takes the same shape (RFC
0005 "The operations listener").

**The name is what a door is known by beyond its own block**: it is the
listener id `/v1/operations/users` reports a session under, what a log
line's `listener=` names, and what a certificate error names -
`broker.mqtt.listen.tcp` for the door a single mapping leaves named after
its kind, `broker.mqtt.listen.tcp[fleet]` once a name says which of
several. The single-mapping form is exactly this with the name "tcp",
"ws" or "unix".

`--check-config` refuses, by name with both doors quoted: a second door
of a kind with none; two doors sharing one name - of any kind, including
a kind's own default name, so a `tcp` door named `ws` collides with a
bare `ws:` map. **That rule is asked within one listener's doors at a
time, never across the two**: the MQTT doors (`tcp`, `ws`, `unix`
together) are one closed set and the operations doors (`tcp`, `unix`)
are another, because an operations door's name keys nothing an MQTT
door's does - not `SetListenerCredentials`, not a route an MQTT client
reaches, only the operations listener's own credential lookup - so an
MQTT `tcp` door and an operations `tcp` door sharing a name are two
unrelated things, not a collision.

Ports and Unix paths are a physical fact rather than a naming one, so
those two checks *do* cross the two listeners: two TCP-speaking doors on
the same port, counting `tcp`, `ws` and the operations port together,
where the hosts are equal or either is a wildcard (`0.0.0.0`, `::`, or
no host at all - port `0` never clashes, since the kernel hands each
listener a different one); and two Unix doors, counting the operations
socket, at the same path - the kernel's port table and its filesystem
do not know which configuration block asked. Hosts are compared as addresses
(`::ffff:127.0.0.1` is `127.0.0.1`) and paths as cleaned absolute paths
(`/x/./r.sock` is `/x/r.sock`); a host name such as `localhost` is not
resolved at check time, so it clashes only at the bind. A list of one door
with no name is named after its kind, as the mapping form is. A tcp or ws
address must read as `net.Listen` reads it (`host:port`, IPv6 in brackets, a
port from 0 to 65535 or a service name `/etc/services` knows, `032010` and
`+32010` being port 32010); ports are compared as numbers, a host is empty,
an IP literal (a zone, if written, not empty; IPv6 multicast and zone-less
link-local refused) or an RFC 1123 host name, and a zone is ignored when
comparing hosts except on link-local addresses, where two zones are two
sockets; a Unix path may not hold a NUL, and its last element must be a file
name (not empty, `.` or `..`). **A plain start refuses exactly what
`--check-config` does**, because both read the same configuration through
the same check - a broker never binds half its doors and leaves the clash
for the connections that land on whichever port happened to come up second.
`SIGUSR1` re-reads every door's password file, certificate and client CA;
which ports are open is startup-only.

**Per listener rather than per broker.** The MQTT port faces a fleet whose
certificate is whatever their estate issues; the operations port faces a
monitoring system that is frequently somewhere else entirely. One
certificate for both would be one name for both, and a deployment with both
would have to choose which of them the name is wrong for. A Unix socket
takes none: the credential never leaves the machine, and a certificate there
is ceremony rather than security.

**There is no default and nothing is generated.** A self-signed certificate
Sagüin made for itself would be encryption no client can verify, so every
client would be told to skip the check - which looks secured, is not, and
stops anybody asking again.

**The certificate's key type decides most of a handshake's CPU.** Measured
in Go on x86, a whole handshake, both ends in one process, costs 0.9 to
1.1 ms with an RSA-2048 certificate and 0.24 to 0.43 ms with an ECDSA
P-256 one (the lower figure of each is TLS 1.2, the higher TLS 1.3); the
server's signature alone costs 0.74 ms against 0.021 ms. An operator who
expects many clients to connect at once - a fleet reconnecting after a
restart, or a small ARM board - should prefer an ECDSA P-256 certificate.

**The certificate is read at startup, and a bad one stops the broker**,
with an error naming the listener before a port opens - the alternative
is a broker whose log says it is listening while clients fail somewhere
else, or one that carries on in plain text, the failure that looks like
success.

**`SIGUSR1` re-reads it, and `client_ca_file` with it**, on every TLS
listener the operations one included, from the paths the broker started
with. A renewed certificate is served to every handshake after the signal;
a connection already open keeps the one it was handshaken with, so renewing
costs the fleet nothing - where a restart drops every connection, which is
six times a year for a certificate that renews every sixty days. A client
CA replaced the same way refuses, from the next handshake, a certificate
only the old authorities trusted.

**Nothing changes unless every listener's files load.** A renewal lands as
two files written one after the other, so a signal between them meets a key
that does not match its certificate: the broker logs which listener and why,
at `error`, and every listener goes on serving the pair it had. Each one
that is re-read logs its new certificate's expiry.

A bridge's own `cert_file` and `key_file` are read at every handshake, so a
renewed pair is presented on the next reconnect with no signal at all.

**`cert_file` holds the chain, not only the leaf**: a leaf issued by an
intermediate is `cat leaf.pem intermediate.pem > cert.pem`, as nginx's
`ssl_certificate` works. Getting it wrong fails on somebody else's
machine - "unable to verify the first certificate" at the client while
the broker's log says it is listening. The root itself is not included: a
client that does not already have it is not one this certificate can
convince.

`min_version` is the lowest version a listener accepts, `1.2` or `1.3`,
and 1.2 when it is not written. **It is a floor, not a pin** - a pin
excludes clients speaking something newer, the wrong way round for a fleet
that upgrades over years. There is nothing below 1.2 to choose.

#### Client certificates

`client_ca_file` names the authorities trusted when checking a certificate a
**client** presents, which is what Mosquitto's `cafile` does. It is named
for that rather than copied across, because `ca_file` reads to most people
as the chain a server sends, and here that is `cert_file`.

**Absent, no client certificate is asked for or looked at** - an ordinary
TLS listener. Present, mutual TLS, and `require_certificate` says whether
every client must have one:

| | |
|---|---|
| no `client_ca_file` | encrypted, nobody's certificate examined |
| `client_ca_file` | certificates verified; one is required |
| `+ require_certificate: false` | verified if presented, and a client without one falls through to the password file |

That last row is the mixed mode a fleet migrating in batches needs, and it
is the same shape as `allow_anonymous` beside `password_file`: presence is
the switch, and an explicit key relaxes it.

**The certificate's name becomes the client's user name**, which is
Mosquitto's `use_identity_as_username`, and it is not optional here: a
certificate *is* a name, and a different one beside it would be two
identities for one client. The password file is not consulted for a
certificate client at all - the authority already made the statement that
file exists to make.

**A certificate's name is its Common Name, or its first DNS name when it
has none**, taken exactly as the certificate states it - the rule the
operations listener names a certificate by (RFC 0005), so one certificate
is one identity at either door. CN is where an operator's own authority
usually puts a name, but many authorities now issue a subject alternative
name and nothing else. A verified certificate carrying neither names
nobody: it authenticates nothing, falls through, and says so in the log.

**A name holding U+0000 or a control character is nobody's** -
U+0001-U+001F or U+007F-U+009F, from a certificate, a proxy or a `CONNECT`
user name alike - and the connection is refused `0x86`, with a warning
saying why: such a name could rewrite the log line recording it or pass for
another in one. It is the set mosquitto and EMQX refuse, and MQTT forbids
U+0000 in a UTF-8 string outright [MQTT-1.5.4-2].

A `require_certificate` written where no authority is named is refused at
startup: it reads as though certificates were being demanded, and nothing
would be.

**Making the files is three openssl commands per certificate** (openssl 3;
`-copy_extensions` is what carries the SAN into the signed certificate).
One authority signs both sides: `ca.pem` goes in the listener's
`client_ca_file` and in every client's `--cafile`, and the same
device-shaped pair serves a bridge's `cert_file` and `key_file` when two
saguins meet with mutual TLS:

```sh
# One authority for the estate, kept off the broker.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -days 3650 -nodes -subj "/CN=my-estate-ca" \
  -keyout ca-key.pem -out ca.pem

# The broker's certificate. The subjectAltName must say what clients
# dial - a CN alone fails modern verification.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=broker" \
  -addext "subjectAltName=DNS:broker.example.com,IP:192.0.2.10" \
  -keyout key.pem -out broker.csr
openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -copy_extensions copy -out cert.pem

# A device's certificate. The CN is the client's name: it becomes the
# user name and matches ACL patterns, and the password file is not
# consulted for this client.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=device-7" -keyout device-7-key.pem -out device-7.csr
openssl x509 -req -in device-7.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 825 -out device-7.pem
```

and then a device connects with all three:

```sh
mosquitto_sub -V 5 -h broker.example.com -p 8883 --cafile ca.pem \
  --cert device-7.pem --key device-7-key.pem -t 'state/#' -q 1
```

A plain client on the TLS port fails its handshake, a client offering no
certificate is dropped where `require_certificate` is on, the certified
one connects - and an `acl_file` rule on `%u` matches `device-7` from the
certificate alone, which is the Common Name doing exactly what this
section says.

#### On a bridge, dialling out

A bridge takes `ca_file`, the authority that signs its **peer's**
certificate, for a `tls://` or `wss://` peer a public root does not
vouch for - which is what an estate running its own authority has. Absent,
the system roots are used.

**There is no way to turn verification off**: a link that accepts any
certificate accepts whatever machine answered first, with the records
going there. A `ca_file` beside an unencrypted peer is refused for the
same reason `require_certificate` without an authority is - it reads as a
link being verified when nothing is.

`cert_file` and `key_file` are the other half: the certificate Sagüin
**presents** when the peer asks for one, which is what Sagüin's own
listener asks for with `client_ca_file` set. Without them two saguins cannot
bridge to each other under the strongest setting either of them offers. They
go together - a certificate with no key cannot be presented and a key with
no certificate names nobody - and both are absolute, like every other path a
configuration names. The device-shaped pair the recipe above makes
serves here unchanged: a bridge is a client, and its certificate's Common
Name is its name at the far end.

The whole shape at once - a private authority checked, and an identity
presented, which is mutual TLS with Sagüin as the client:

```yaml
bridges:
  head-office:
    peer: tls://mqtt.example.com:8883
    client_id: vessel-07

    # The authority that signs the FAR END's certificate, for an estate
    # running its own. Leave it out and the system roots are used.
    ca_file: /etc/saguin/tls/peer-ca.pem

    # The certificate saguin PRESENTS when the far end asks for one, and
    # its key. This is the half a listener's client_ca_file asks for.
    cert_file: /etc/saguin/tls/bridge.pem
    key_file: /etc/saguin/tls/bridge-key.pem

    topics:
      - filter: fleet/+/telemetry/#
        topic: telemetry/$1/$#
        direction: in
```

**A pair without a `ca_file` is allowed**, which is not the rule a listener
follows, and the difference is that here the two keys answer different
questions: the authority checks the far end, the pair identifies this one.
A peer whose certificate a public root vouches for, reached with a
private client certificate, is an ordinary arrangement, and refusing it
would be a rule with no failure behind it. A `cert_file` beside an
unencrypted peer is refused for the same reason a `ca_file` is: there is
no handshake for it to take part in, and it reads as though there were.

**Sagüin presents what the configuration names, and lets the far end
refuse it** - handed to the TLS library as a candidate instead, a
certificate issued by the wrong authority is silently not sent, and the
link comes up carrying no identity wherever the peer takes one without.

**A file Sagüin cannot read stops the broker**, exactly as a listener's
certificate does: a configuration error is answered before a listener
opens, where a broker that started anyway would run a dead bridge for the
life of the process - the log saying up, the records not moving.

### Which versions may connect: `broker.mqtt.min_protocol_version`

The oldest MQTT version this broker admits. Two values and no others:

| Value | |
|---|---|
| `"3.1.1"` | the default. MQTT 3.1.1 and MQTT 5 clients are both admitted |
| `"5"` | MQTT 5 only. A 3.1.1 CONNECT is refused with `0x01`, 3.1.1's own "unacceptable protocol version" |

**MQTT 3.1 - protocol level 3 - is never admitted**, and no value admits
it: its `SUBACK` has no failure code, so a queue subscription could only
be granted or dropped, and its `CONNACK` has no Session Present flag, the
only way to tell a returning consumer its position is gone.

**It is written under `broker.mqtt`, once, for the whole broker.** Unlike
`password_file` and `allow_anonymous` it cannot be written on a listener,
because the version a device speaks is a property of the device rather
than of the door it arrives at: the same firmware reaching the same broker
over TCP and over a Unix socket is the same decision both times.

```yaml
broker:
  mqtt:
    min_protocol_version: "5"       # the fleet is MQTT 5; turn 3.1.1 away
```

Both values are accepted quoted or not - YAML reads `3.1.1` as text and
a bare `5` as an integer, and the loader takes both shapes.

**What changes for a 3.1.1 client once it is in is not here.** RFC 0001
says what the two protocols each get; this key decides only whether one
gets in at all.

### Who may connect: authentication and the password file

`broker.mqtt.password_file` names the clients that may connect, in
Mosquitto's format - the same file, hash for hash, so a deployment moving
from Mosquitto keeps the credentials it already has. `saguin --passwd`
manages it; RFC 0005 has the operations half, which is a **different file**
because an operator is not a device.

| Key | |
|---|---|
| `password_file` | Absolute path, Mosquitto's format. Absent means there is nothing to authenticate against |
| `allow_anonymous` | Whether a client offering no user name is admitted |

**Both keys may also be written on a listener, where they win.** A broker
with one answer writes it once under `broker.mqtt`; a broker with two - a
Unix socket whose file permissions guard it, beside a TCP port facing a
fleet - writes the difference on the listener it belongs to. Those
permissions are the socket's `mode`, `0660` if absent: owner and group,
nobody else, the same key the operations socket takes.

```yaml
broker:
  mqtt:
    password_file: /etc/saguin/clients.passwd
    allow_anonymous: false          # the fleet must authenticate
    listen:
      tcp:
        address: 0.0.0.0:1883
      unix:
        path: /run/saguin/saguin.sock
        allow_anonymous: true       # and this door is the file's to guard
```

**A listener naming its own `password_file` admits its own users and
nobody else**, unless it also writes `allow_anonymous` - it does not
inherit the broker's answer. **`allow_anonymous` takes its answer from
the password file when it is not written**: no file admits everybody, a
file admits only who it names. Written, it wins either way - `true`
beside a password file is the mixed mode a migrating fleet needs, where
named clients authenticate and the rest are still let in.

`false` with no password file is refused at startup. It is a broker nothing
can connect to, which is a configuration nobody means, and the alternative
is learning it from every client being refused at once.

**A wrong credential and a missing one are both `0x86`**, Bad User Name or
Password. Distinguishing them on the wire tells an unauthenticated caller
which user names exist. The broker's own log says which of the two it was,
because the operator holding the log is not the caller.

**A password file is read at startup and re-read on `SIGUSR1`.** Adding a
user does not admit that user until the signal arrives: `saguin --passwd`
writes the file, and the broker answers from the copy it holds. So
provisioning a device in the field is an edit and a signal - never a
restart - and an operator who skips the signal watches a tool report
success while the device it added is refused.

**The operations password file is re-read by the same signal, and so are
the certificates** ("TLS on a listener"). Which *file* a listener reads
stays a restart: the signal re-reads the contents of the files the broker
started with, and repointing `password_file` at another path is an
ordinary configuration change.

**Nothing is applied unless every file loaded**, because a signal arrives in
the middle of an edit as often as after one. A file that cannot be read, one
whose hashes Sagüin does not understand, one that names nobody on a listener
admitting nobody else - any of these leaves the broker running exactly what
it had, and says so at `error` naming the file.

Hashes are `$6$` (SHA512) and `$7$` (PBKDF2-SHA512) as Mosquitto writes
them. **argon2id is refused with an error naming the file and the line**,
not treated as a failed password: a migration from a file Sagüin cannot read
must not look like a fleet that has forgotten its passwords. **A user named
on two lines is refused the same way, naming both lines**, as Mosquitto
refuses it: only one of the two passwords could ever be the one checked,
and `--passwd delete` removing that one would leave the other admitting the
device it was run to withdraw.

#### Behind a proxy that terminated TLS

`broker.mqtt.listen.unix.proxy_protocol` reads a **PROXY protocol v2**
header from every connection: the client's real address, and the
certificate Common Name where the proxy verified one. Without it, a fleet
behind one proxy is a single local peer in every log line, every
`max_connections` count and every authorization decision.

**The name a proxy sends is an identity**, the way a certificate this
broker verified itself is one: it becomes the client's user name exactly
as a Common Name does, so an `acl_file` rule about `%u` works whether TLS
was terminated here or in front. **The header carries a Common Name and no
DNS name**, so a certificate the proxy verified that has no Common Name
names nobody here, even where Sagüin terminating TLS itself would name it
by its first DNS name.

| | |
|---|---|
| `proxy_protocol` | Read a v2 header. Unix listener only. Absent means none is expected |

**This key exists on a Unix socket and nowhere else**: the header is the
peer asserting who its client is, and a socket's file permissions already
decide who may assert it - the allowlist a TCP listener would need
first.

**With it set, a connection arriving without a header is refused.** The
socket exists because a proxy is in front of it, and serving one anyway
would mean the address in every log line depended on whether the proxy was
working.

**A connection that has not sent its header within 5 seconds is closed**:
a proxy writes the header the moment it connects, so only a peer that is
not a proxy takes longer. This holds on the operations socket's
`proxy_protocol` too, and the bound is fixed rather than configured.

**v2 and not v1.** A v1 header carries addresses and no TLVs, so it cannot
say which certificate was verified - half of what this is for. A v1 header
is refused naming the setting to change rather than reported as an absence,
because `proxy_protocol on` is the natural thing to write and is v1.

| | sends |
|---|---|
| HAProxy, `send-proxy-v2-ssl-cn` | addresses and the SSL TLV carrying the Common Name |
| nginx 1.31.4, `proxy_protocol v2` | addresses, an authority TLV, the SSL TLV - version, Common Name, cipher, signature and key algorithm - and a CRC32C |
| nginx, `proxy_protocol on` | a v1 header, which Sagüin refuses |

nginx needs 1.31.4 or later for `v2` at all. Earlier versions have only
`on`, and behind one of those Sagüin's answer is the refusal above.

**What a correct pair looks like.** The proxy verifies the client and hands
the connection to the socket; the broker reads the header and takes the
name. Four things have to line up, and each of them fails differently:

```nginx
stream {
    server {
        listen 8883 ssl;
        ssl_certificate        /etc/saguin/tls/cert.pem;
        ssl_certificate_key    /etc/saguin/tls/key.pem;
        ssl_client_certificate /etc/saguin/tls/clients-ca.pem;
        ssl_verify_client      on;          # 1

        proxy_pass unix:/run/saguin/saguin.sock;
        proxy_protocol v2;                  # 2
        proxy_timeout 20m;                  # 3
    }
}
```

```yaml
broker:
  mqtt:
    listen:
      unix:
        path: /run/saguin/saguin.sock
        mode: "0660"                        # 4
        proxy_protocol: true
```

1. **`ssl_verify_client on`**, and this is the one that is not merely a
   misconfiguration. The proxy is the only thing that sees the certificate,
   so whatever it forwards is what Sagüin believes - the whole contract is
   that an authority checked the name and Sagüin is trusting the hop.

   nginx sends the Common Name whenever a client presented a certificate,
   **including one it did not verify**, and sends beside it whether it
   did. Sagüin believes the name only when the SSL TLV's client flags say
   the connection was TLS and a certificate was presented, and its verify
   result is `0`. With `ssl_verify_client optional_no_ca`, a certificate
   carrying `CN=device-7` and signed by an authority nginx does not trust
   arrives with verify result `21`: it names nobody, the connection is
   answered as one with no certificate - the password file, or anonymous
   where the listener allows it - and the broker logs `the proxy could not
   verify this client's certificate … verify_result=21`. The operations
   socket's `proxy_protocol` believes a name by the same rule.

   With `on`, the same certificate does not get past the handshake: nginx
   answers `client SSL certificate verify error: (21:unable to verify the
   first certificate)` and the broker never sees a connection. `optional`
   with a `ssl_client_certificate` that actually verifies is equivalent for
   clients that present one; `optional_no_ca` is the setting to refuse
   outright.
2. **`v2`, never `on`.** `on` is v1 and is refused, naming the setting.
3. **`proxy_timeout` above the largest keepalive in the fleet.** It
   defaults to ten minutes and severs an idle connection whatever keepalive
   the client negotiated; the broker sees a disconnect and a new session,
   and a fleet sees reconnect churn with nothing in its own logs to explain
   it.
4. **The socket's permissions are the access control**, so the proxy's user
   must be in the group that owns it. Otherwise the proxy logs `connect()
   to unix:… failed (13: Permission denied)` and the broker logs nothing at
   all, because nothing reached it.

With a device holding `CN=device-7` and a role granting
`topic: alerts/%u/#`, `alerts/device-7/fire` is answered `0x00`,
`alerts/device-8/fire` and a channel the role does not name are answered
`0x87`, and the broker logs `a proxy named this client …
principal=device-7`. The rule is bound to the MQTT client, rather than
enforced only at the proxy's door.

**Certificate revocation stays the proxy's business**, because the proxy
holds the handshake. Sagüin is told a name that was verified; it cannot
re-check what it never saw.

### What a client may do: authorization and the ACL file

Authentication says who a client is; this says what it may do. Without an
`acl_file` every authenticated client may publish into every channel,
subscribe to every channel, drain a work queue, and hang any other client
up.

`broker.mqtt.acl_file` names an authorization file. It sits beside
`password_file` because it answers the second half of the same question, and
it is a **different file** from the password one so that the password file
stays Mosquitto's format hash for hash.

| Key | |
|---|---|
| `acl_file` | Absolute path. Absent means every authenticated client may do anything |

**An `acl_file` requires something that authenticates a client, and is
refused at startup without one.** Authorization is a statement about an
identity, and with nothing to authenticate against there is no identity to
make it about - a file full of rules governing nobody reads as protection
and is none.

Two things satisfy it, because Sagüin has two ways of naming a client: a
`password_file`, or a listener with a `client_ca_file`, where the identity
is the certificate's name. **A pure-certificate estate needs no
password file**, which matters because a certificate client is
deliberately not looked up in one ("Client certificates" above): requiring
one anyway would mean writing a file that authenticates nobody in order to
have any authorization at all, which is the very shape this refusal exists
to prevent. For the same reason `allow_anonymous: true` beside an
`acl_file` is refused, and the failure is worse than "no rule applies": an
anonymous client's identity is the empty string, and a `*` pattern under
`users:` matches it - so such a client is admitted and granted whatever
that wildcard grants, silently. With a password file and `allow_anonymous`
unwritten an anonymous client is already refused, so this error fires only
where an operator has said something contradictory out loud.

**The same exposure reached without writing it is warned about, at the start
and by `--check-config`**: a listener beside an `acl_file` that has no
password file and does not require a client certificate - a pure-certificate
estate's Unix socket, a plain `ws` door, a `tcp` door with
`require_certificate: false` - admits a client nothing identifies, whose
identity is the empty name and so gets whatever `*` grants. A user name such
a client types is not its identity: a name nothing checked is never one.

#### Roles, and users matched by pattern

Rules are written into a **role**, and roles are given to **user names** -
the name a client authenticates under, matched by pattern. A fleet of ten
thousand devices that are all the same kind of thing needs one rule, not ten
thousand copies of it, and every copy is a chance to get one wrong.

**The block is `users:` because its keys are user names** - never client
ids, the other string in the same packet, the one nothing proves, and the
one no rule here reads. A file that names the block `clients:` is refused
at load, saying what to write instead.

```yaml
roles:
  telemetry-publisher:
    - channel: events
      filter: events/telemetry/%u/#
      allow: [write]
  job-worker:
    - channel: jobs
      allow: [consume]
  reader:
    - channel: events
      allow: [read, seek]
    - topic: alerts/#
      allow: [read]

users:
  "vessel-*": [telemetry-publisher, job-worker]
  dashboard:  [reader]
```

`%u` stands for the client's identity: the password-file user name, or a
client certificate's name, which arrive in the same field.

**`%c` stands for its client id**, and the two are not interchangeable.
`%u` is proved - a password or a certificate stands behind it - so a rule
resting on it rests on something. `%c` is whatever the device typed.

**`%c` is for the deployment `%u` cannot serve**: a cohort sharing one
credential, where the proved name is the same for two hundred devices and
the only thing telling them apart is the id each chose. Scoping by it gives
each device its own topics. It does not stop one taking another's, because
devices sharing a secret are one principal - what it costs the taker is
that MQTT hands the session over, so the device it displaced reconnects and
somebody sees it.

**Written together they say two halves of one sentence**: the proved name
fixes which cohort, and the chosen id picks the device inside it.

```yaml
roles:
  sensor:
    - topic: iot/%u/health/%c
      allow: [write, read]
```

`%c` alone would let any credential reach any device's topic. `%u` alone
cannot tell two devices in one cohort apart. With `client_ids:` on the
entry, the credential cannot even be carried by a device outside its
cohort.

**A name goes into a rule as one level, never as the rule's syntax.** Where
the name `%u` or `%c` would put in holds `+`, `#` or `/` - which a topic
filter reads as a wildcard or a level - that rule grants the client nothing,
its other rules still apply, and the broker says so once, at connect;
`saguin --acl` names the rule as withheld. A client id is whatever the device
typed, and `#` under `iot/health/%c` would otherwise be every device's topic,
for reading and for writing. mosquitto and EMQX refuse the same names. Each
substitution is one pass, so a name holding `%c` is put in as written.

**`saguin --acl` says what that scoping is worth**, wherever a rule uses
`%c`, because a grant reading `iot/cohort-north/health/north-17` looks like
per-device isolation and is not. It is separation by mistake rather than by
force, and only a file with `%c` in it is told so - a note printed on every
file is one nobody reads by the third time.

**A deployment with one credential per device needs neither.** `%u` is
already the device, and `%c` adds nothing.

**It is substituted wherever a rule names a filter - `topic:` and
`filter:` alike - and never in a channel name.** A channel is declared once
in `channels:` and cannot vary by client, so `%u` there would name a
channel per device, which is the fleet-sized copying roles exist to avoid.
A rule whose `channel:` holds a `%` is a **startup failure** saying so,
rather than the "channel is not configured" it would otherwise get, which
is true and sends an operator to declare one.

**A channel rule narrows with `filter:`, compared against the whole
topic** - the same comparison a `topic:` rule makes, and the same one a
channel's own filter makes. One mechanism, so a rule says what it grants on
its own line rather than meaning something a reader has to work out from
the channel it names.

**A filter matching none of its channel's topics is a startup failure**,
naming the rule's filter and the channel's. It grants nothing and reads as
though it granted part of the channel, which is the shape this document
refuses everywhere else. `%u` is compared as an ordinary spelled-out level,
because the rule has to be decidable before any client connects.

**A rule's filter is a well-formed MQTT topic filter**, checked at startup
the way a channel's is: `#` is only ever the last level, and `+` and `#`
each take a whole level. A `#` in the middle is refused rather than read,
because the matcher stands it in for everything that follows - so
`alerts/#/page` would grant every `alerts` topic to a rule naming three,
and read on the page as the narrowing it is not. A `topic:` rule is checked
the same way less the rule about the first level, which is a channel's
alone: `topic: "#"` is how an operator says "any broadcast topic".

**`{a,b}` works in a rule's filter and expands the way a channel's does**,
so a rule may narrow a braced channel to one of its spellings or carry the
channel's own filter, and both mean what they look like. One braced rule is
one grant per spelling, which is what `--acl` prints.

**It expands before `%u` and `%c` are substituted** - the other way
round, a device calling itself `{s1,s9}` would turn one device's grant
into two. Expanding what the file says settles every grant before any
client is named.

**Assignment by pattern is the half that saves the work.** With it,
provisioning a device is one line in the password file and no authorization
edit at all - which matters here more than in most brokers, because neither
file means anything to a running broker until it is told to re-read them,
and an avoided edit is an avoided signal.

##### Which entry applies

**Where several patterns match a client id, the one that spells it out
most exactly supplies the whole entry - roles and limits together - and
every other matching entry does nothing.** `device-7` beats `device-*`,
counted in literal characters, so `device-*` beats `*-7` for `device-7`.
**Two patterns spelling out the same number and both matching one id are
refused at startup naming both** - which applies would otherwise be
decided by nothing an operator can read.

One entry, one rule: an operator reading a four-line entry can take it as
the whole of what that client gets.

**The cost is that a narrow entry can take something away.** A file with a
broad `*` giving everyone a `base` role and a `device-*` beside it does
not give devices `base`; a `device-7` written to add one role drops the
fleet's second; and a `device-7` carrying no `limits:` takes the fleet's
bound off that device rather than inheriting it.

That is not a defect - it is the only way an exception can be expressed:
a grant-only model with no precedence cannot subtract. Every entry
therefore reads as the complete statement about the clients it names,
which is why an entry with no `roles:` is still refused.

**Nothing can refuse a shadowed entry at startup**, because shadowing is the
intended behaviour rather than a mistake, and from inside the file a pattern
that matched and lost looks exactly like one that applied. So
`saguin --acl <config-file> <user-name>` names the entry in force and the
entries it shadows, and says when a shadowed entry carried limits the
applying one does not - the one form of this rule that loosens a bound
rather than removing a permission, and the one that shows up as a device
flooding the broker rather than as a device being refused.

##### Which devices may carry a name

Every MQTT `CONNECT` carries two strings, and only one of them is proved:

| | Chosen by | Proved by | What it names | Does authorization read it |
|---|---|---|---|---|
| Client Identifier | the client, freely | nothing | the **session**, so it can resume | never |
| User Name | the client | its password, or the certificate carrying it | **who you are** | always |

Everything above is about the second. A client id is whatever the device
typed, so a rule resting on one would rest on nothing.

**`client_ids:` is where an operator ties the two together.**

```yaml
users:
  "cohort-north":
    roles: [sensor]
    client_ids: "north-*"
```

The name `cohort-north` may then be used only by a device whose client id
matches `north-*`. Anything else is refused at the door. One pattern or a
list of them, `*` standing for any run of characters as it does in the
entry's own key, and `%u` substituted for the proved name - so
`client_ids: "%u"` says "connect under the name you logged in as", which is
one line for a fleet of any size and is what makes a client id in a log
line worth reading. A name holding `*`, the pattern's own wildcard, is never
substituted: such a pattern admits no client id for that name.

**Absent means any client id**, which is what most deployments want: a
credential per device already ties the two together and there is nothing
for this to add.

**What it buys is the boundary between credentials, and that is the
whole of it**: a device holding the northern credential cannot connect as
a southern one. Inside a cohort it buys nothing and cannot - devices
sharing a secret are one principal, and that impersonation stays noisy is
already the `%c` paragraphs' point.

**The refusal is `0x86` (Bad user name or password)**, the same code a
wrong password gets - "your credential is good, your client id is wrong"
would tell an unauthenticated caller the credential is good. The log line
carries the distinction, and that is where an operator looks.

**A `client_ids:` that admits nobody is a startup failure** - an empty
entry, or the key written with nothing under it. It reads as a restriction
and is a locked door: every device holding the credential is refused at
connect, which looks like a broken fleet rather than a configuration
mistake.

**It can only refuse.** The user name decides which entry applies and what
that entry grants; this decides whether the connection happens at all. So a
client id, which nothing proves, never widens anything - it is asked after
the name is proved, and only ever subtracts.

#### Three rule kinds, and only two of them are about topics

Every topic resolves to exactly one channel or to broadcast - filters
overlap deliberately and the most exact of them holds a topic, which is
invariant 12's rule and is settled before any client connects. Two of the
rule kinds hang off that partition, which is why they cannot conflict:

- a **`channel:` rule** governs topics that resolve to that channel;
- a **`topic:` rule** governs broadcast topics - the ones no channel claims.

The third is outside the partition because it is not about the topic tree
at all:

- a **`broker:` rule** governs a facility of the broker itself, and there is
  one of those today: `sessions`.

Nothing a `broker:` rule grants can be reached by any filter, and no
filter can confer it: **a role holding every verb on every channel and
`topic: "#"` beside them still cannot hang anybody up**, which is the
point of a separate kind rather than a reserved topic somebody could
match.

**A `topic:` rule that lies wholly inside a channel is refused at startup,
naming both** - "topic `iot/hq/+/events` is inside channel `events`, whose
filter is `iot/+/+/events/#`; write it as a channel rule". That is
invariant 12's own shape one level up: refuse the ambiguity while the
operator is looking, rather than resolving it consistently until something
changes underneath. `%u` is treated as an ordinary spelled-out level for
that check, since the rule has to be decidable before any client connects.

**A rule that merely crosses a channel is not refused**, and the difference
matters more than it looks. `#` and `+/telemetry` reach every channel on
the broker and are still how an operator says "any broadcast topic". What
the refusal is for is the rule that could only ever have been about a
channel's own topics, which grants nothing and reads as though it granted
the channel. Nothing is left ambiguous by accepting the crossing form,
because a topic is resolved before any rule is asked: a channel topic never
reaches a topic rule, whatever that rule's filter covers.

**A subscription is granted whole or refused** (*What a filter reaches*):
its filter needs the grant for every channel it touches, and a filter that
lies inside no one channel's filter needs a `topic:` rule covering it as
well, because it can match topics no channel claims. With the channels at
the top of this document, `iot/water/w-7/#` needs `read` on both water
channels and a `topic:` rule: `iot/water/w-7` itself is broadcast. A filter
that several channels together cover, and none alone, is judged the same
way.

#### The verbs

| | verbs |
|---|---|
| `append` | `write`, `read`, `seek` |
| `latest` | `write`, `read`, `delete` |
| `queue` | `write` (submit work), `consume` |
| `topic:` (broadcast) | `write`, `read` |
| `broker: sessions` | `disconnect` |
| `broker: features` | none: it takes `deny:` and nothing else (*Taking a feature away*) |

**One pair everywhere.** `write` puts something in and `read` takes it out,
on every channel type and on broadcast alike, so a rule can be written and
read without first looking up which type the channel it names happens to be.
`seek` and `delete` are the two acts that are not a direction - moving a
consumer's position, and removing a key - and a client may hold either
without the other.

**`consume` is the one irregular word, and it is there to stop a
mistake.** Reading an `append` channel costs nobody anything; taking a
job holds the record for one worker. Spelled `read`, an operator granting
a dashboard `read` on a queue to watch the work would drain it instead -
invariant 11's failure through the ACL. **So `read` on a queue is refused
at startup**, and the refusal says a queue cannot be watched at all - its
depth is a gauge on the metrics listener (RFC 0005) - because an operator
told only that `read` is not one of `write, consume` picks the nearer
word, which is the grant the list was protecting them from.

**The `$saguin/` verbs ride on the channel they name**, and get none of
their own: a seek is `seek` on the append channel it moves, a point read
is `read` on the latest channel it reads, and **a queue's response topic
is part of `consume`** - separated, an operator could grant `consume` to
a worker that may never acknowledge, and every job it took would time
out, retry and dead-letter.

**`disconnect` is the one that rides on nothing**, and it is an exception
because hanging up a client is not an act on anybody's records: there is no
channel to name, so there is no channel verb to hang it on. It is granted by
the `broker:` rule kind instead, which is why that kind exists at all - a
verb with no subject would otherwise have to be spelled as a topic, and a
topic is something a wide filter can reach by accident.

##### Hanging up a client: `broker: sessions`

**A password file reaches a client at CONNECT and nowhere else**, so a
credential withdrawn and re-read is still a credential a connected device is
using: it was checked when the device arrived and is not checked again. The
device goes on until something ends the connection, and a device with a good
keepalive has no reason to end it. **Hanging it up is the act that makes the
device come back and be asked again** - and it is the only act in this
document that is not about a topic.

**On its own it withdraws nothing.** A hang-up costs the device one
reconnection; what it may do when it returns is whatever the files say then.
So it is the second half of an act whose first half is editing those files
and signalling the broker to re-read them, and the order matters: hung up
first, the device is back with everything it had before the edit lands.

```yaml
roles:
  on-call:
    - broker: sessions
      allow: [disconnect]

users:
  oncall: [on-call]
```

**It is asked for by publishing to `$saguin/sessions/disconnect`, with the
client id to hang up as the payload.** In the payload rather than in the
topic, for the reason a point read puts its key there: a client id is
whatever the device typed - `/` and all - and a topic carrying one cannot
be parsed back into it.

**The `PUBACK` says whether the request was taken**, as every reserved-space
verb's does:

| | |
|---|---|
| `0x00` | authorized, and acted on |
| `0x83` (Implementation specific error) | no Response Topic to answer on; or the payload named no client id, or named the client that sent it |
| `0x87` (Not authorized) | this client's roles do not carry `disconnect` |

**What happened comes back on the Response Topic**, the way a seek's answer
does - one word, on the topic the request named:

| | |
|---|---|
| `hung-up` | it was connected; its DISCONNECT is on its way, and the connection ends within `limits.write_timeout` |
| `no-such-client` | nothing is connected under that id |

**Those two are worth telling apart**, which is the whole reason there is a
reply at all: otherwise "that device disconnected an hour ago" and "you have
misspelled the id" are the same answer, and the second is the one an
operator actually sends.

**And they are told apart in a payload rather than by a reason code**:
`0x10` would fit, and it is not dependable - a `PUBACK` may leave the
reason-code byte out, so a code that means something can arrive as the
absence that means `0x00`. A payload cannot be omitted.

**A client cannot hang itself up**: a connection ended by its own request
could never receive the acknowledgement, and every client can already end
its own connection with the `DISCONNECT` its protocol defines.

**At QoS 0 it works and is answered**, the answer being a reply rather
than a code; what QoS 0 cannot carry is a *refusal*, which is dropped in
silence - the trade the reserved space makes everywhere.

**A request that names no Response Topic is refused**, `0x83`, and
nothing is hung up - acting on it anyway would end somebody's connection
and tell nobody it had happened.

The property is MQTT 5's, so **a 3.1.1 client cannot ask for this verb at
all**: the refusal ends its connection - the rule every refusal a 3.1.1
client cannot be told about already takes - and the client hung up is the
one that asked, never the one it named. No fallback topic is kept here the
way a seek keeps one, because no data depends on an operator's tool being
a 3.1.1 client.

**It ends the connection and does not touch the session**: the device
reconnects, resumes at its stored position, and loses nothing - the whole
cost is one reconnection, which is what makes the verb safe to put in
front of an operator. Ending a session is a different act with a
different cost, and this document does not offer it.

**An MQTT 5 client is told why**: a `DISCONNECT` carrying `0x98`
(Administrative action). A 3.1.1 connection is closed with nothing on it,
3.1.1 having no server-to-client `DISCONNECT`.

**The broker's own in-process publisher cannot be hung up.** It is how the
broker publishes on its own behalf - a Will, a queue delivery, a bridge -
rather than anybody's session, and a request naming it is answered
`no-such-client` like any other id nothing is connected under.

**Without an `acl_file` every authenticated client may do this** - a
broker with no authorization file has no privilege boundary, and this
verb does not invent one to stand alone in.

##### Taking a feature away: `broker: features`

**MQTT gives every client five features a fleet's operator may not want
every device to have**: a session that outlives its connection, a Last
Will, a shared subscription, a retained value on a broadcast topic, and
exactly-once delivery. A `broker: features` rule takes any of them away
with `deny:`, each named for the broker block that configures it:

```yaml
roles:
  sensor:
    - channel: events
      allow: [write]
    - broker: features
      deny: [persistent, will, share, retained, qos2]
```

**A denied client is answered as a broker without that feature would
answer it**, in MQTT's own signal where there is one:

| Denied | An MQTT 5 client | A 3.1.1 client |
|---|---|---|
| `persistent` | accepted, with a Session Expiry Interval of 0 in its `CONNACK`: its session ends with the connection | accepted, with its session made clean |
| `will` | a `CONNECT` carrying a Will is refused `0x87` (Not authorized). One without a Will connects as usual | refused `0x05` |
| `share` | told Shared Subscription Available 0 in its `CONNACK`. A `SUBSCRIBE` containing a `$share/` filter anyway is a Protocol Error, and the connection is closed `0x9E` (MQTT 5 section 3.2.2.3.13) | nothing changes: a 3.1.1 client is refused every `$share/` filter already |
| `retained` | a retained publish to a broadcast topic is closed `0x9A`, and a `CONNECT` carrying a retained Will aimed at one is refused `0x9A` - what MQTT has a broker without retained messages answer. On a channel's topic nothing changes and Retain Available stays 1: the channel is the store, so the flag keeps nothing extra there, and a 0 would make every retained publish a Protocol Error on every topic | a retained broadcast publish closes the connection, and a retained Will aimed at broadcast is refused `0x05` |
| `qos2` | told Maximum QoS 1 in its `CONNACK`. A `SUBSCRIBE` asking for QoS 2 is granted 1 and delivered at no more than 1; a QoS 2 publish anyway is closed `0x9B` (MQTT-3.2.2-11); a `CONNECT` carrying a QoS 2 Will is refused `0x9B` | a QoS 2 subscription is granted 1, a QoS 2 publish closes the connection, and a QoS 2 Will is refused `0x05` |

**A feature is denied when any of the client's roles denies it**, and the
denial is broker-wide rather than per topic: a role written to keep a
fleet's sessions short is not undone by another role that says nothing
about features.

**It is asked once the client has authenticated** - asked of a name a
`CONNECT` merely claims, the answer would tell an unauthenticated caller
which names exist - so a wrong password is still `0x86`, whatever the
name's roles deny.

**A Will is refused rather than dropped**: quietly not arming an
announcement the device asked for is a half-grant nothing would report. A
persistent session is not refused, MQTT having a way to say "your session
ends with this connection".

**`deny:` is written only on a `broker: features` rule, and that rule
takes nothing else** - a feature is something every client has until a
role takes it away, so there is nothing for `allow:` to add. A denial
anywhere else, an `allow:` here, a feature not among the five, or one
written twice is refused at startup, `retain` by name because the feature
is `retained`. `saguin --acl` and `/v1/operations/acl` show each denial
beside the rule it is written on.

##### Asking what a channel is

**A client addresses a channel by name everywhere except when it
publishes or subscribes**, and only the configuration says which topics a
name claims - so a library offering `publish(channel, key)` rather than
`publish(topic)` has to learn the filter from somewhere that is neither
this file nor the operations listener, whose credential must not be a
device's (RFC 0005). **`$saguin/catalogue/<channel>` is where it asks**,
over the MQTT credential it already has:

```
PUBLISH  $saguin/catalogue/readings
  Response Topic:   where the answer goes - required
  Correlation Data: optional, echoed
```

The answer is written to the asking connection, exactly as a point read's
and a seek's are, so **no subscription is needed and no other client can
receive it**. It is JSON:

| Field | |
|---|---|
| `name` | the channel's name, as asked for |
| `type` | `append`, `latest` or `queue` |
| `filter` | the filter this channel claims, **as written** - braces and all |
| `verbs` | the verbs this client holds on it |
| `pin` | a queue's subscription form, and only a queue has one |

**The filter goes back as written rather than expanded.** `{device,sensor}`
is a level with a fixed set of spellings, which is exactly what a caller
composing a topic needs: a `+` takes any one level, a braced level takes one
of these, and a trailing `#` takes the rest or nothing. The plain filters it
expands to are the broker's own business and would tell a caller less.

**The answer is scoped to what the asking client may use, and the two kinds
of no are the same no.** A channel this client holds no verb on and a
channel that does not exist are both an empty reply, so asking cannot be
used to discover what is there. That is what makes this safe to answer over
a device's credential: a client already holding `write` on a channel learns
the topics it was already publishing to, and one holding nothing learns
nothing at all.

**A grant narrowed by `filter:` still learns the whole filter** - this is
a disclosure rule rather than a permission one, and the narrowing is
enforced where it decides something: on the publish that leaves it.

**And it is a permission answered as it stands, never a promise** - an
`acl_file` can be re-read under a connected client. The filter itself
cannot change under a live connection, channels being startup-only, so a
client may cache it for as long as the connection lasts.

**The `PUBACK` carries every refusal**, for the reason a point read's does:
the reply carries an answer and nothing else, so an empty one can mean one
thing.

| | |
|---|---|
| `0x00` | answered - with the channel, or with the empty reply |
| `0x83` (Implementation specific error) | no Response Topic to answer on |
| `0x90` (Topic name invalid) | the name held a `/`, so it names no channel that could exist, and the request is not one |

**Which means the request must be QoS 1**, since QoS 0 has no `PUBACK` to be
refused in; at QoS 0 it is dropped, as every other request in this space is.
A 3.1.1 client cannot ask at all, the Response Topic being MQTT 5's.


##### Withdrawing a device's access

Four steps, in this order, and none of them restarts the broker:

1. **Edit the two files.** `saguin --passwd delete <file> <user>` takes the
   credential away; removing the client's entry from the `acl_file`, or
   narrowing the role it names, takes the grants away. Either alone is
   worth doing and neither is complete without the signal below.

2. **Signal the broker to re-read them**: `kill -USR1 $(cat
   /run/saguin/saguin.pid)`. The broker logs what it now holds - the paths,
   the number of users, the number of roles - or, if a file cannot be used,
   logs that at `error` and goes on running what it had. **Read the line
   before going on**: a signal that refused the file leaves the device with
   everything it started with.

3. **Hang the device up**, by publishing its client id to
   `$saguin/sessions/disconnect`. The `acl_file` is already in force at this
   point - the device's next publish is refused whether or not it has
   noticed - but the password file is only read at CONNECT, so this is what
   makes the device connect again and be refused.

4. **Read the reply.** `hung-up` means the device was connected and its
   DISCONNECT is on its way: the connection ends within
   `limits.write_timeout`, whether or not the device is still reading - the
   reply does not wait on a stranger's socket. `no-such-client` means
   nothing was connected under that id, which is either a device that had
   already gone or an id spelled wrong, and those are not the same thing to
   check next.

**What each step alone leaves behind** is the whole reason to write them
down. Steps 1 and 2 without step 3 stop the device doing anything, but
the connection survives - authenticated on a credential that no longer
exists - and a lease it holds goes back only when the connection ends.
Step 3 without 1 and 2 costs the device one reconnection and changes
nothing. A restart in place of step 2 works and ends every other
connection with it.

**That same property is the verb's other use**, and it has nothing to do
with credentials: hanging up a worker that has wedged returns the jobs it was
holding for another worker to take, rather than waiting out their visibility
timeout.

**A device that authenticates with a certificate is withdrawn the same
way, with one step missing and one limit.** Step 1 is the `acl_file` edit
alone - there is no password-file entry to delete - and steps 2 and 3 are
unchanged. **The limit is that it can still connect**: Sagüin has no
certificate revocation, so the handshake still succeeds and the device
comes back to no grants. Where the connection itself must be refused,
`client_ids:` on the entry is what does it, and replacing the client CA
is the same signal again ("TLS on a listener").

#### Grant-only, for records

Default refuse; rules add; any match allows; a client's grants are the
union of the roles its entry names. A rule that can only grant cannot
disagree with another, so there is no precedence between rules to
explain - exactness decides one level up, and only which entry supplies
the roles.

**The one denial is a feature**, on a `broker: features` rule, and it has
one rule of its own: any role denying it denies it, whatever the other
roles say. It is not a refusal of an act on anybody's records - it takes
away something MQTT gives every client - which is why it cannot collide
with a grant. *Taking a feature away* has the five there are.

#### Authorization may only narrow

The substrate ORs its ACL hooks, and `OnACLCheck` carries Sagüin's
structural refusals - the queue form (invariant 4) and the wildcard
boundary (invariant 11) - so a permission model able to widen them would
put two workers on one job. **The structural rules are evaluated first
and refuse regardless of any grant**: a rule may take permission away and
may never give it.

#### What a refused client sees

Nothing new on the wire, and no code that did not already have a meaning:

| | |
|---|---|
| a publish | `0x87` (Not authorized) on the `PUBACK` - the row the table under *Publishing* already carries |
| a subscribe | `0x87` (Not authorized) in the `SUBACK`, per filter |
| a Will | `0x87` (Not authorized) on the `CONNACK`, for a Will on a topic the client may not publish to. It is asked again, as that client, when the Will fires, because the file may have been reloaded since: refused then, it is logged and not published (RFC 0003 "Last Will") |
| a QoS 0 publish | dropped: there is no acknowledgement to carry a refusal, the same trade the reserved space and `max_message_size` already make |

**`0x87` on a subscribe says one thing**: this client, not this filter. A
filter of the wrong shape is answered `0x8F` or `0x83` instead, so
a developer is pointed at the filter rather than at their credentials.

**The file is read at startup and re-read on `SIGUSR1`**, as the password
file is - and a re-read takes effect on connections already open, because
the file is asked on every publish and on **every delivery**, never only
at CONNECT or SUBSCRIBE: a resumed session sends no SUBSCRIBE for the
broker to refuse. A subscriber refused mid-channel is stalled, not
skipped, and the grant coming back is itself what wakes it; a queue job
offered to a worker that may no longer take it goes back with its attempt
unspent. Invariant 16 carries the mechanism and what each alternative
loses.

#### Explaining a decision: `--acl`

```sh
saguin --acl /etc/saguin/saguin.yaml device-7
saguin --acl /etc/saguin/saguin.yaml cohort-north north-17
```

prints the roles that matched, the rules they carry, and the effective
grants. **The first argument after the configuration is the user name** -
what a client authenticates as, and what every rule is written about - and
never the client id, which is a different string in the same packet and
which no rule reads.

**A client id is given only where a rule uses `%c`**, and then it must be:
without one the substitution cannot be made, so the literal is left
standing and the grants printed below it match nothing. The command says
that in a sentence rather than printing lines that look resolved - a rule
written on purpose, a configuration reporting ok, and a fleet refused
`0x87` is the failure that would otherwise hide. It takes the **broker
configuration** rather than the acl_file: that is where the `acl_file` is
named and where the channels its rules are about are defined, and a rule
naming a channel is only answerable against them. Indirection has one cost -
"why can device-7 not publish?" is two lookups and a pattern match
instead of a line in a file - and this is the answer to it, built with the
feature rather than after it. It is the same instinct as `--check-config
--output`: where behaviour is resolved from several places, the resolved
form is printable.

**A list of user names on standard input answers them a line at a time**,
which is the case the paragraph form is worst at. Seven tab-separated
columns under a header naming them: the user name asked about, the role that
granted it, whether the rule is about a channel or a broadcast topic, which
one, the verbs, and the two figures that user is held to per second -
messages and bytes.

**The two limit columns repeat on every row of a user**, because the figures
belong to the user rather than to the grant. That is the same reason the
first column is always the input: a line stands on its own however the
output is cut about, and "which of these forty devices is bounded
differently" is answered with a `cut` rather than forty runs of the
paragraph form.

**They carry `no bound` in words where nothing bounds one**, never `0` -
the one figure the configuration refuses - and never `-`, which this
format defines to mean granted nothing. **A line may carry a client id
after the user name**, separated by a tab or a space, for a file whose
rules use `%c`; a line with one field is a user name alone. A channel
rule that narrows with `filter:` carries both in the fourth column -
`events (iot/+/events/device-7)` - because the filter alone would not
say which channel, and the channel alone would not say which of its
topics. `%u` is already substituted, because the substituted
form is what the broker compares against. Against the files this
repository ships, so that it can be run - `examples/acl.yaml`, which the
demo stages where `examples/saguin.yaml` names it:

```sh
$ printf 'demo\ndevice-7\nnobody\n' | saguin --acl examples/saguin.yaml
# user    role    kind     subject                verbs              rate      bytes
demo      tour    channel  events                 write,read,seek    no bound  no bound
demo      tour    channel  state                  write,read,delete  no bound  no bound
demo      tour    channel  presence               write,read,delete  no bound  no bound
demo      tour    channel  jobs                   write,consume      no bound  no bound
demo      tour    channel  jobs__dlq              write,read,seek    no bound  no bound
demo      tour    topic    #                      write,read         no bound  no bound
device-7  device  topic    iot/+/health/device-7  write,read         200       65536
nobody    -       -        -                      -                  no bound  no bound
```

**A role carrying six rules is six lines**, as `tour` is, and a client
holding two roles is a line per rule of each. **`device-7` is granted by
its own entry**, not by `device-*`: the spelled-out pattern supplies the
entry, and `%u` resolves to the name it authenticated under - a client
certificate's Common Name here, that device having no password anywhere.
`jobs__dlq` is the dead-letter channel a queue derives; nobody configured
that name. **The list form does not say which entry applied**: its
question is which of forty devices may publish, and one client id gives
the paragraph or the object below, where the entry in force and the
entries it shadows are named.

A client no pattern matches gets a row of `-` rather than no row. Silence
would make "granted nothing" and "I never asked about that one" the same
output, and those are the two answers somebody checking a fleet most needs
to tell apart.

**The header starts with `#`** so that it is skipped by the same rule that
skips a comment in the list going in: the input is always the first column,
so a kept run is asked again by cutting that column and piping it back, and
the header must not become one more question.

**A configuration naming no `acl_file` is answered in one sentence**, in
both forms: it names the file it read and says that every authenticated
client may do anything. No rows at all. A row of `-` there would answer the
most permissive setting there is with the output this document defines to
mean granted nothing, which is the reverse, and in the most emphatic form
the format has.

#### `--json`, for a program rather than a person

`--json` answers either form as JSON instead of columns. One object per
line, compact, and no header - the keys are the header, and a `#` line is
not JSON:

```sh
$ printf 'device-7\nnobody\n' | saguin --acl examples/saguin.yaml --json
{"user":"device-7","role":"device","kind":"topic","subject":"iot/+/health/device-7","verbs":["write","read"],"denies":[],"publish_rate":200,"publish_bytes":65536}
{"user":"nobody","role":null,"kind":null,"subject":null,"verbs":[],"denies":[],"publish_rate":null,"publish_bytes":null}
```

**A line per answer rather than one array**: an array is not valid until
it is closed, so a run over a fleet would say nothing until it finished
all of it, and `grep` stops working. `jq -s` collects an array for
anybody who wanted one.

**Where a column holds `-`, the field is `null`** - not the string `"-"`,
which a consumer testing for absence would not recognise. `verbs` is a list
and is `[]` rather than null, so that iterating it never has to test first,
and so is `denies` - the feature denials a `broker: features` rule carries
(*Taking a feature away*), and `[]` on every other rule.

**`publish_rate` and `publish_bytes` are numbers, and `null` where
nothing bounds one** - null is the permissive end there and the absent
end in the four fields above it, there being no number that means
"unbounded".

One client id gives one object instead, carrying what the paragraph form
adds around the same grants - which entry applied, which patterns matched,
which patterns the file holds, `grants_withheld`: the rules that grant this
pair nothing because the name they would put in holds `+`, `#` or `/`, as
the file writes them, and `client_id_allowed`: whether the entry's
`client_ids:` admits the client id given, or `null` where none was. All
five are always present, because a shape that changes with the answer is
one a script has to branch on before it can read it; `pattern_applied` is
`null` where nothing matched.

**`patterns_matched` is not the answer to what a client gets**, and this is
the field to read instead. Every pattern that matches is listed, and one of
them is in force - a script testing whether a device matched its fleet
pattern will find that it did, while the entry actually supplying its roles
is a narrower one beside it:

```sh
$ saguin --acl examples/saguin.yaml device-7 --json
{"user":"device-7","acl_file":"/tmp/saguin-demo/acl.yaml","pattern_applied":"device-7","patterns_matched":["device-*","device-7"],"patterns_in_file":["demo","device-*","device-7"],"grants":[{"role":"device","kind":"topic","subject":"iot/+/health/device-7","verbs":["write","read"],"denies":[]}],"grants_withheld":[],"client_id_allowed":null}
```

A configuration naming no `acl_file` answers `{"acl_file":null,
"everything_allowed":true}`. An empty grant list there would be the JSON
spelling of the row of `-` above, and would read as the reverse of the
truth.

`--json` is how these two commands answer rather than a flag of its own:
given without `--acl` or `--route` it is refused, rather than ignored while
a broker starts.

### Where a topic lands: `--route`

```sh
saguin --route /etc/saguin/saguin.yaml iot/depot/state/device-1
```

**Filters overlap deliberately, and which one holds a topic is settled by
a rule rather than read off the name.** So the broker answers it, against
the configuration, without publishing anything - for a topic, the channel
that holds it and the filter that claimed it; for a filter, every channel
a subscriber would be served from and what each would send.

It calls the functions the broker calls. A second implementation here
would agree on the day it was written and drift silently afterwards, which
is the failure this command exists to prevent rather than to reproduce.

**A list on standard input answers a line at a time**, in four
tab-separated columns under a header naming them, the input first. Against
the configuration this repository ships, so that it can be run:

```sh
$ saguin --route examples/saguin.yaml < fleet-topics.txt
# topic-or-filter          channel    type       why
iot/depot/events/order-1   events     append     iot/+/events/+
iot/depot/state/device-1   state      latest     iot/+/state/+
iot/depot/health/device-1  -          broadcast  -
iot/depot/#                events     append     replayed
iot/depot/#                presence   latest     current
iot/depot/#                state      latest     current
iot/depot/#                jobs       queue      excluded
iot/depot/#                jobs__dlq  append     replayed
```

**`jobs__dlq` is in that list and is in no configuration file**, because a
queue derives a dead-letter channel. A wide filter reaching one is the sort
of thing an operator finds out here rather than by reading, which is most of
why the command exists.

The header starts with `#` so that it is skipped as a comment when a kept
run is asked again - cut the first column and pipe it back. A bare `#` is a
legal filter and is answered as one; a `#` with anything after it is a note.

The fourth column answers "and so?": for a topic, the filter that
claimed it - the line an operator goes and edits - and for a filter, what
a subscriber would actually be served. A filter reaching several channels
is several lines rather than an invented summary.

`awk -F'\t' '$3 == "broadcast"'` is every topic no channel claims, and a
diff of two runs against two configurations is every topic a filter edit
moved. That is what the shape is for.

**`--json` answers the same thing as JSON**, one object per line, on the
same terms `--acl` sets out above - a `-` column is a `null` field, there
is no header, and one subject given as an operand gives one object instead,
carrying the routing table the paragraph form prints:

```sh
$ printf 'iot/depot/state/device-1\niot/depot/health/device-1\n' |
    saguin --route examples/saguin.yaml --json
{"subject":"iot/depot/state/device-1","channel":"state","type":"latest","why":"iot/+/state/+"}
{"subject":"iot/depot/health/device-1","channel":null,"type":"broadcast","why":null}
```

**A blank line is skipped and a `#` line is a comment - except a line
that is only `#`**, which is the widest filter MQTT has and the thing
somebody most wants an answer about.

### The operations listener

`broker.operations` is the HTTP listener an operator reads: `/health`,
`/metrics`, and the `/v1/operations` routes that answer *which one* where a
metric can only answer *how many*. RFC 0005 specifies what it serves and who
reaches which route; this section is the keys that configure it.

**`listen.tcp` and `listen.unix` are each the list-or-map shape "Several
listeners of a kind" gives every listen block**: the table below is a
single door, named after its kind, and a second door of a kind is a list
entry with a `name` of its own - RFC 0005 "The operations listener" has
the example. Every row below is already a per-door rule and stays one
once there are several: each door keeps its own `password_file`, TLS and
`proxy_protocol`, and the loopback rule is asked of each TCP door in
turn.

| Key | |
|---|---|
| `listen.tcp.address` | `host:port`. Loopback only unless something there authenticates: a `password_file` naming who may read it - this listener's own, or the block's - or a client certificate required by `tls.client_ca_file` |
| `listen.unix.path` | A socket file. Not reachable off the box, so it is the answer for a reader on this machine |
| `listen.unix.mode` | Optional, `0660` if absent. The socket file's permissions, which are the access control |
| `listen.tcp.tls` | Optional. A certificate and key, and `client_ca_file` for mutual TLS - the same block as an MQTT listener (*TLS on a listener*) |
| `listen.tcp.password_file`, `listen.unix.password_file` | Optional. That door's own operators, instead of the block's `password_file` below |
| `listen.unix.proxy_protocol` | Optional. The socket is fed by a proxy sending PROXY v2, which carries the Common Name it verified |
| `min_scrape_interval` | Optional, `60s` if absent, and refused below `60s`. The shortest interval at which the metrics are recomputed |

`listen` takes either entry or both, and at least one if the block is
present: a block that opens nothing is a configuration asking for
`/metrics` and then not serving it anywhere. There is no default address,
because a port opened by a default is a port nobody chose.

**Omitting the block omits the listener.** No `/health`, no `/metrics`, no
port. That is a deployment which is not scraped and does not want the
surface, and it is why `address` has no default to fall back on.

**A TCP address that is not loopback has to authenticate somebody, and
the broker refuses to start otherwise**: a `password_file` - this
listener's own, or the block's - or `tls.client_ca_file` requiring a
client certificate (`require_certificate: false` does not count).
`/metrics` names channels, volumes and consumer positions, and without
the refusal "authentication later" becomes for ever the first time
somebody edits the address to `0.0.0.0` because that made the scraper
work. Give it a password file, a tunnel, or the Unix socket.

`password_file` is an absolute path to a file in Mosquitto's format,
managed with `saguin --passwd`, holding the operators who may read
`/metrics` - **not** the MQTT clients, which are a different set of people
and a different file (RFC 0005). It is absolute for the reason every other
path here is: which operators may read the metrics must not depend on where
the broker was started from.

**A door may name its own instead**, and `listen.tcp.password_file` or
`listen.unix.password_file` wins where it is written - the same rule an
MQTT listener follows. The block's file is what a door that names none
takes, so the common deployment writes one file once and the deployment
this exists for writes two: a monitoring system on the port, whose
credential rotates with the estate's secrets, and a local agent on the
socket, which belongs to this machine. One file for both makes those two
rotate together, which is what an operator splitting them is trying to
stop.

**A door that has no file anywhere has no credential**, which for the
socket is the ordinary arrangement - its file permissions are the gate -
and for the port is what confines it to a loopback address. The rule below
is asked of each door's own answer, so a listener naming its own operators
may bind a routable address, and a file on the *socket* says nothing about
the port.

**There is no `allow_anonymous` beside it**, and one written under either
listener is refused at startup - the single place this differs from an
MQTT listener. On the port `true` would open `/metrics` to whoever can
reach it, and on the socket it would only strip the name off a reader
already getting in. A key whose settings are the default and a hole is
not a key.

**`min_scrape_interval` is held in whole seconds**, so `500ms` is refused
rather than rounded, and a scrape arriving sooner than the interval is
answered from the previous one - RFC 0005 "The observer does not set the
cost" is why.

**Sixty seconds is a floor as well as a default, and a smaller value is
refused rather than quietly raised.** A number the broker accepts and does
not honour is worse than one it turns down, because the scraper goes on
asking every five seconds and the operator goes on believing it. Confluent
Cloud publishes its metrics on the same floor, and Prometheus's own default
scrape interval is a minute - so this is the interval the ecosystem already
assumes rather than a limitation of this broker. A deployment that wants
finer resolution than a minute wants a different instrument: the counters
here are cumulative, so a rate over a minute loses nothing but the shape of
a burst inside it.

### Bridges

A **bridge** is Sagüin as a client of another broker, in either direction:
it subscribes at the far end and brings what arrives in, and it reads its
own channels and publishes what it finds out. The peer is Mosquitto, EMQX,
another Sagüin, whatever is already there, and it needs no bridge
configuration of its own - what arrives there is an ordinary MQTT client's
publish, and what Sagüin reads is an ordinary subscription.

**It is a translator rather than a copy.** The far end has never heard of
a channel or an offset, so everything that made a record Sagüin's is lost
on the way across and what arrives is a new record at an offset this
broker assigns. Disaster recovery is the database copy in RFC 0004, not
anything a bridge does.

**A bridged record enters this broker through its own publish path**:
channel resolution, size bounds, header limits and every reason code
above apply to it exactly as to a local client's, so a rule for a foreign
feed puts no restriction on channel type. **There is no second way into a
channel** - no bridge writes to storage behind the broker's back.

**A record's identity crosses; its position does not.** A record leaving
carries its `saguin-id` as a User Property, and a Sagüin at the far end
lifts that into the record it stores - so the same message has one identity
on both brokers (invariant 8), and a consumer can recognise a record it has
already seen. Nothing else of Sagüin's crosses: the offset is this broker's
own count and the far end assigns its own, because what goes over the wire
is MQTT and MQTT has no offsets.

A record arriving from a **foreign** broker carries no `saguin-id`, because
nothing there sets one - so it is a new message here and is given an
identity on arrival, as any publish is.

**The RETAIN flag crosses, both ways** - and a bridge still ships records
as they are written and never a stored pass, so a link restart re-ships no
stored set. RFC 0003 "Retained messages" has the whole of it, the peer that
advertises no retained messages included.

**One thing does cross twice, and MQTT is what requires it.** A QoS 1
publish whose acknowledgement the upstream never saw is re-sent when the
session resumes - before any `SUBSCRIBE`, so no retained pass is involved -
and a link that drops with an acknowledgement in flight produces exactly
that. Stored a second time it would take a second offset and carry no
`saguin-id` either time, which is two records downstream that nothing can
tell are one.

**So the bridge remembers what it settled.** A record it stored and
acknowledged is held as its packet identifier and a digest of its topic and
payload, and an arriving publish that carries `DUP` and matches one is
acknowledged again and not stored - the acknowledgement being the thing the
upstream is missing. The memory is bounded by the `receive_maximum` this
bridge advertises, which is exactly what limits what the peer may hold
unacknowledged toward it, and the oldest is overwritten.

It is asked only of a packet carrying `DUP`, which is what makes a recycled
packet identifier safe: an identical payload published again arrives as a
first delivery, where MQTT forbids `DUP` [MQTT-3.3.1-1], so it is stored as
the new record it is.

**It does not survive this process.** The memory is held in RAM, so a bridge
that restarts has forgotten what it acknowledged and a redelivery then
crosses as a second record. Between a link recovery and the next restart the
promise holds; across a restart the bridge is at-least-once, as the rest of
this document already says delivery is.

```yaml
bridges:
  head-office:
    peer: tls://mqtt.example.com:8883
    client_id: vessel-07          # saguin's identity at the far end
    session_expiry: 1d            # how long the peer holds the backlog
    receive_maximum: 20           # how much may be in flight to saguin
    ack_interval: 5ms             # how long an acknowledgement may wait
    topics:
      - filter: fleet/+/telemetry/#
        topic: readings/telemetry/$1/$#
        direction: in
      - filter: alerts/#
        direction: both
```

**`filter` is what goes on the wire** as Sagüin's `SUBSCRIBE`, and it is an
ordinary MQTT topic filter - not a pattern evaluated locally over a broader
subscription. Asking the peer for more than is wanted and discarding
the rest pulls the difference across the link, which on the metered
connection a bridge exists for is exactly backwards.

**`direction` is which way the rule carries, and it is required.** There is
no default: an omitted key deciding whether this broker's records leave the
building is not something an operator could see in the file, so the rule is
refused and the refusal names the three words.

| | What the rule does |
|---|---|
| `in` | subscribes to `filter` at the peer, and publishes what arrives into this broker |
| `out` | reads what this broker holds under `filter`, and publishes it at the peer |
| `both` | both of the above, on one filter |

`out` reads a filter the way any client does: every `append` and `latest`
channel it matches, and every broadcast topic - **a queue is never crossed**,
which is invariant 11 and the same answer a subscriber gets. An outbound rule
draining an `append` channel keeps a position in it, so a link that comes back
resumes where it stopped rather than re-sending or skipping. **Each rule keeps
its own**, named by its bridge, its filter and its topic: a peer refusing one
rule's records holds that rule and no other, and retention passing it is
counted against that rule alone. A rule whose filter or topic is edited is a
new reader and starts where a new reader starts. A broadcast topic
has no position to keep, and what is published while the link is down is gone,
which is what broadcast promises everywhere else. A `latest` channel is carried
as its changes happen, never as a pass over its current state (RFC 0003): a
change made while the link is down waits in the rule, the newest per topic, and
crosses when the link returns.

**An `out` rule publishes one record at a time per topic, and several
topics at once.** What it may have in flight altogether is
`min(the peer's Receive Maximum, receive_maximum)` - the same key that
bounds what the peer may have in flight *to* Sagüin, spent in both
directions.

**Per topic is serial, and that is deliberate.** A record crossing a bridge
is given a *local* offset where it lands, so at the far end currency is
arrival order: two writes to one topic overtaking each other would leave a
`latest` channel there holding the older one for good, and a device reading
it stale with nothing to correct it. Order across *different* topics is not
promised and never was - RFC 0003 promises order per topic.

So **a stream on a single topic still costs one round trip per record.**
That is the residual rather than a shortfall: it is what per-topic order
costs, and no window can buy it back.

**What it costs, measured rather than reasoned.** Two Sagüins with a
latency-injecting proxy between them, one `out` rule, records already
waiting in the channel:

| Round trip | One topic | Eight topics |
|---|---|---|
| ~20µs (loopback) | 10,971/s | 11,721/s |
| ~5ms | 148/s | 552/s |
| ~10ms | 89/s | 299/s |

On one topic the rate is one record per round trip and nothing else: a 20ms
link - an ordinary satellite or cellular figure, and the deployment a bridge
exists for - carries about 47 records a second however fast either broker
is. Spread over eight topics the same link carries three to four times
that, because the window is spent across them.

**Loopback hides all of this**, which is worth stating for anyone measuring:
at 20µs a round trip costs nothing and both columns look like no limit at
all. The shape only appears once the link has latency, which is where
bridges live.

Sagüin keeps in-order delivery per topic and spends the window across
topics.

**What a window costs, and what bounds it.** Duplication across the hop
already existed: a link that drops with records unacknowledged re-sends them
on the next connection, because the position never moved past them. A window
widens that bound from one record to the window, and only on a refusal - a
pass cannot know which record the peer will refuse until the ones behind it
are already in flight, and those are offered again on the next pass. The
first refusal stops the pass issuing anything further, so what may be sent
twice is what was in flight and not the whole batch.

The bound is therefore `receive_maximum`, which an operator sets. And the
record's identity crosses with it - `saguin-id`, which a receiving Sagüin
lifts into the record it stores - so a consumer that deduplicates on message
identity sees each record once whatever the link did. The position itself is
unaffected: it advances only through the contiguous run of acknowledged
offsets and stops at the first the peer did not take, so nothing is ever
skipped.

**`topic:` is required on an `in` or an `out` rule, and refused on a
`both`.** The two halves of that are one rule read from either side. A
one-way rule states the whole topic its records take at the other end, so
leaving it out would be a rule that does not say where anything lands. A
`both` rule cannot carry one at all: a template is a one-way rewrite with
nothing to invert it, so `both` plus a template would mean "rewrite going
out, do not coming back" - two rules wearing one name. `both` therefore maps
a topic to itself, and that identity is what makes it reversible.

**`topic` is the whole topic a record takes, and nothing else decides where
it lands.** A channel is a topic filter rather than a prefix, so there is no
suffix that generally sits inside one - `iot/+/+/events/#` has nothing to
hang a suffix off. The topic resolves exactly as it does for a publish from
any client: to whichever channel's filter matches it, or to broadcast when
none does.

**There is no key naming the channel, and that is deliberate.** A client
never has to know where the channels are - it publishes a topic - and a
bridge is a client. A rule that named one would be a second routing table
beside the channel filters, and two routing tables disagree eventually.
`--check-config` prints what each rule reaches, which is where an
operator checks where a rule's records will land:

```
saguin.yaml: ok
  alerts                   latest
  readings                 append
  bridge "head-office", in "fleet/+/telemetry/#": lands in readings (append)
  bridge "head-office", both "alerts/#": lands in alerts (latest)
```

A filter that crosses a queue says so too, because nothing else ever will -
such a rule is served everything it matches *except* the queue, exactly as
any subscriber's is (invariant 11), so there is no refusal at startup and no
line at runtime:

```
  bridge "head-office", both "iot/#": lands in events (append), jobs__dlq (append), state (latest); crosses jobs (queue), never served
```

**A rule may not publish into the reserved `$` space**, where MQTT keeps its
own topics and Sagüin keeps a queue acknowledgement and a consumer's seek. A
bridge able to publish there would forge them on behalf of whatever it
carries. A template cannot express it - a `$` always begins a substitution,
so `$saguin/…` is refused as a `$` that is neither `$#` nor a wildcard
number - so what is actually stopped is the topic a substitution builds out
of what the peer published, and it is dropped with a warning. An inbound
`filter` beginning `$saguin/` is refused at startup for the same reason.

**`topic` is that whole topic, with substitutions**: `$1`, `$2` … for the
filter's `+` levels in order, and `$#` for its `#` tail.

| Filter | Topic | `fleet/vessel-07/telemetry/hold/psi` becomes |
|---|---|---|
| `fleet/+/telemetry/#` | `readings/telemetry/$1/$#` | `readings/telemetry/vessel-07/hold/psi` |
| `fleet/+/telemetry/#` | `readings/telemetry/$#` | `readings/telemetry/hold/psi` |
| `fleet/#` | `readings/$#` | `readings/vessel-07/telemetry/hold/psi` |

The numbered captures are what a prefix strip cannot do: they **reorder**,
so `fleet/<vessel>/telemetry/<metric>` can become
`telemetry/<vessel>/<metric>` rather than merely a shorter version of
itself.

There are no regular expressions, and the reason is worth stating because
the question will be asked again. A regex cannot be sent to a peer
broker, so a regex filter forces a broad subscription and local matching,
which is the waste above. It also admits patterns that look right and are
not: `fleet/(.)+/telemetry/(.)+` compiles, runs, and captures **one
character** - `$1` is `7` and `$2` is `p` - so every record lands on a
topic made of stray characters and nothing reports it. A `+` is one topic
level by construction and cannot mean anything else.

**A `#` matches zero levels as well as many** (MQTT 5 section 4.7.1.2), so
`fleet/#` reaches the bare `fleet`. The tail is then empty and the
separator that was joining it goes with it, so `telemetry/$1/$#` gives
`readings/telemetry/vessel-07` rather than a topic with an empty last
level. Where the whole suffix would be empty there is nothing to publish -
a channel topic has a non-empty suffix - and the record is dropped with a
warning rather than published somewhere unexpected.

**Rules are tried in the order they are written and the first match wins.**
A message matching no rule is dropped with a warning. That makes the order
of the list significant, which is worth knowing before a merge reorders two
entries and changes what the broker does without changing what any line
says.

Matching is a linear scan, and the cost is the reason it is allowed to be.
`BenchmarkInboundMatch` and `BenchmarkInboundMatchMiss`, at `-benchtime 2s
-count 2`:

| one rule, against one message | Ryzen 7 260 | MacBook Pro M1 Pro |
|---|---|---|
| that does not match | 54ns | 64ns |
| that matches | 143ns | 162ns |

Only a message that matches *nothing* runs every rule, so fifty rules is
under 3us on traffic nobody asked for. An index earns its keep a long way
above that.

#### Loops, and what stops one

**A record that arrived over one of this broker's own bridges is never
sent out over another** - a blanket mark on the record, not a comparison
against the bridge's own name (RFC 0003 "Retained messages" has why, and
re-origination for a broker meant to relay).

**Per link, that is the whole of it.** Two brokers, one `direction: both`
rule, and nothing is duplicated: No Local [MQTT-3.8.3-3] stops the peer
echoing this bridge's own publish back down the connection it arrived on, and
the mark stops what the bridge pulled in from going back out. Against a peer
that ignores No Local the worst case is one duplicate per record - bounded,
and never a loop.

**Across links it is topology rather than mechanism, and the obligation is
the operator's: the bridge graph must be a tree.** The mark cannot cross the
wire. A record a peer *pushed* here arrives as an ordinary client publish -
which is the whole of the promise that the far end needs no bridge
configuration - and nothing local can tell it from a record a device
published. So relay depends on which way each link was dialled:

| The record was | Onward over another bridge |
|---|---|
| pulled in by a rule here | stopped |
| pushed here by a peer's rule | forwarded |

Edge to hub to cloud therefore works when the links dial toward the cloud,
and a hub that pulls from the edge does not relay onward - which is the case
for re-origination: a broker meant to relay publishes the record again as its
own, the way dead-lettering already does.

A cycle loops. Three brokers each dialling the next multiplied one record
forty thousand times in three seconds, and no broker in the ring can decide
locally that it should not have. Mosquitto states the same obligation for the
same reason. Sagüin cannot refuse a topology it cannot see: it knows its own
rules and nothing about the peer's.

#### What running a bridge means

**Sagüin does not acknowledge the peer until its own store has taken
the record.** That is the rule the rest of this follows from. A channel at
its `max_bytes` answers `0x97`, and a bridge that had already sent its
PUBACK would have discarded a record the peer believes it delivered -
acknowledged and lost, which this document refuses everywhere else, one hop
further out than Sagüin can otherwise produce it.

**So a full local channel becomes backpressure on the peer
subscription, and that is intended.** Sagüin subscribes with a Receive
Maximum, so a peer that stops being acknowledged stops sending that
many records later. It is worth saying out loud because of what it means
for the other broker: an inbound bridge into a channel nobody is draining
will stop draining a broker other people are using. Against mosquitto
2.1.2, a subscriber asking for a Receive Maximum of 5 and never
acknowledging receives exactly 5 of 50 published.

**The backpressure holds the peer's records, and nothing else.** The bridge
stores what arrives on a worker of its own, in order, rather than on the
connection that brought it, so a channel refusing a record never keeps the
bridge from noticing that its link has gone - `saguin_bridge_connected`
falls when the link does, and it reconnects. A QoS 1 or 2 record waits
unacknowledged, as above. A QoS 0 record has no acknowledgement to hold
back, so while the channel refuses, those past one Receive Maximum of
waiting records are dropped and counted
(`saguin_bridge_unstored_total{cause="queue_full"}`, RFC 0005):
at-most-once, which is what their publisher asked for, and mosquitto's
rule for QoS 0 over a full queue.

**A record that could never be accepted is dropped rather than retried.**
A refused record is published again until it gets in, without a limit,
because a full channel and a storage that is not working can both stop
being true. A record that carries more topic or more headers than Sagüin
allows cannot, and retrying it would hold the link for ever behind
something that will never move. So it is dropped with a warning, and it is
the fourth of them. Each is counted under its cause in
`saguin_bridge_unstored_total` (RFC 0005), because each was finished with
at the peer and is lost at this hop:

| Dropped, with a warning | Because | `cause` |
|---|---|---|
| No rule covers the topic | Nothing asked for it | `no_rule` |
| A rule covers it and produces no topic | A `#` matched zero levels, and a channel topic has a non-empty suffix; or the peer sent an empty topic, which nothing may publish | `unmappable` |
| A rule produces a topic in the reserved `$` space | A bridge may not forge Sagüin's own control topics on behalf of what it carries | `unmappable` |
| Sagüin would refuse the record whatever the channel | It can never be accepted, and holding it stops everything behind it | `never_accepted` |

**An outbound rule drops for the middle two of those, and says so the
same way** - the check is one piece of code for both directions, so a
rule that would publish `$saguin/…` at the peer is refused here whatever
the peer is. The record stays at its offset under this channel's
retention, and the drop is counted and logged: nothing at the far end can
notice a record that never arrived.

**An inbound bridge is only as durable as the broker it reads from.**
Sagüin connects with a persistent session, so the peer holds messages
while the link is down - the buffer is somebody else's broker, bounded by
their rules. Mosquitto's `max_queued_messages` defaults to 1000: a
session offline while 1200 are published comes back with exactly 1000,
the peer logs it, and Sagüin cannot see it. **Sagüin's guarantees do not
extend one hop out**, and somebody will assume they do. An *outbound*
rule keeps the property: what it drains is this broker's own channel,
bounded by retention rather than by somebody else's session queue.

**A duplicate on the link is recognisable between two Sagüins and not
otherwise.** MQTT is at-least-once, so a lost acknowledgement makes the
peer re-send; between Sagüins the copy carries the same `saguin-id` at
its own offset, so a consumer keeping identities can tell (invariant 8).
A foreign broker's record carries none, so Sagüin mints one and the
duplicate is a separate message nothing can match: on an append channel
one a consumer cannot tell from a record, on a **queue** two jobs, done
twice, outside protections that are about one record and two workers.

**A rule may target a queue and this is the cost of it**, stated here as a
warning rather than as a preference. A queue is the one channel where the
mitigation below does not exist: pointing the rule at a `latest` channel
works because the same record replaces the value already there, and work
does not collapse - two jobs are two jobs. What is left is `max_attempts`
and the dead-letter channel, which bound how often a duplicate is *retried*
and catch what fails, and neither of them can tell a duplicate from a job.
So a fleet whose work arrives over a bridge needs idempotent workers, which
is what at-least-once already asked of it, and needs them for a reason that
is the link rather than the queue.

**The largest duplicate is not a lost acknowledgement, it is a lost
session.** A lost acknowledgement re-sends one record, occasionally. A
resubscribe re-sends the peer's **entire retained set**, because that is
what a broker answers a subscription with - so every reconnect that finds
Session Present = 0 copies all of it in again, deterministically. That is
the case the bridge already warns about, and it is what an edge box down for
longer than `session_expiry` produces. On an `append` channel those
accumulate for the life of the deployment.

**Pointing the rule at a `latest` channel is the mitigation**, and it is the
one an operator can choose in advance: the same records replace the values
already there, so the channel holds what the peer holds however many
times the link is rebuilt. It is the right shape for the traffic anyway,
since a retained message *is* a current value per topic. An `append` channel
is for a stream of events, where re-reading the retained set is a duplicate
with nothing to collapse it.

**One log line when the link goes and one when it returns**, saying how
long - not one per attempt, which scrolls a bad connection over the record
of everything else that happened. Reconnection is capped and jittered.

#### The three keys that tune a link

They are per bridge, because the thing they describe is the far end and no
two peers are alike: a bridge to a broker on the same LAN and one to a
broker over a metered satellite link want different answers to all three.

Each describes the subscription: how long the peer holds Sagüin's
session, how much it may have in flight *to* Sagüin, and how long Sagüin
may wait before acknowledging it.

| | Default | What it decides |
|---|---|---|
| `session_expiry` | `1d` | How long the peer keeps Sagüin's session, and so how long an outage it holds a backlog across |
| `receive_maximum` | `20` | How many records the peer may have in flight to Sagüin at once |
| `ack_interval` | `5ms` | How long a record Sagüin has already stored may wait before the peer is told |

**`session_expiry` spends somebody else's storage** - the peer holds the
backlog, so raising this asks another operator to hold a week of records
for a broker they may not know exists, and it only stops the *session*
being discarded before the queue is. Lower it, and a Sagüin down longer
comes back to Session Present = 0, warned about, having lost whatever was
queued. A day is the outage a bridge exists to survive.

**`receive_maximum` is the backpressure, and it bounds what a bridge holds
unacknowledged** (invariant 13). Sagüin acknowledges a record only once its
own store has taken it, so a channel that stops accepting stops the
acknowledgements, and the peer stops sending this many records later.
Setting it to 1 makes the link a round trip per record. Setting it high
makes a full channel take that much longer to become backpressure, and holds
that many records in memory in the meantime.

**`ack_interval` is the one that is not obviously a knob until the
arithmetic is written down.** An acknowledgement is not sent the instant
Sagüin stores a record; it is marked, and a ticker sends what has been
marked. Since the peer will not send past `receive_maximum`
unacknowledged records, **the two together are a ceiling of
`receive_maximum / ack_interval`**.

It is a ceiling and not a rate, and the difference matters at the short
end. Measured at `receive_maximum: 20` against a mosquitto peer on the
same machine: 200 records published to it at QoS 1 from one connection,
counted as they reach a subscriber of the memory-backed channel they land
in - so the publisher is part of what is measured, as it is on any link:

| `ack_interval` | the ceiling says | Ryzen 7 260, mosquitto 2.0.22 | M1 Pro, mosquitto 2.1.2 |
|---|---|---|---|
| `50ms` | 400/s | 412–413/s | 444–454/s |
| `5ms` | 4,000/s | 2,800–3,000/s | 3,600–4,300/s |
| `1ms` | 20,000/s | 10,300–11,800/s | 5,000–5,700/s |

At `50ms` the interval is the whole story. Below it something else is the
slower half, and shortening the interval further buys less each time. So
the arithmetic says where the ceiling is, not what the link will do, and
the useful reading is that the default is comfortably off its own ceiling
while a much longer interval would not be.

It is also the window in which a record Sagüin has stored is not yet
acknowledged at the peer. A link cut inside that window makes it
redeliver, and that is the duplicate above: a second record at its own
offset, carrying the original's `saguin-id` where the peer is a Sagüin and
a fresh one where it is not. Lowering it narrows that window and raises
the ceiling, and costs a timer wake-up per interval for the life of the
bridge, on hardware where that is not free.

It is one of the five keys in this file that may be given a value below a
second - a `sqlite` provider's `publish_commit_interval` and
`flush_interval`, `broker.session.ack_commit_interval` and
`limits.write_timeout` are the others - and they are why the duration form
admits `ms` at all.

**What a bridge does not do yet.** It authenticates with a certificate and
nothing else: `cert_file` and `key_file` are the certificate Sagüin presents
when the peer asks for one, and there is no username and no password. A
bridge cannot be given either, and the password file Sagüin reads is not the
place to look for one - that file is for connections arriving at this
broker, and a bridge is Sagüin connecting outward to somebody else's.

### Where a reader starts: `start`

Where a subscriber with no stored position begins reading an `append`
channel. Two values:

| Value | A reader with no position is served |
|---|---|
| `floor` | the default - everything the channel still holds, then what follows |
| `tail` | only what arrives after it subscribed |

**It is an `append` key and is refused on the other two types**, rather
than accepted and ignored. A `latest` subscriber is always served current
state, which is what that channel type *is*; a `queue` has no per-consumer
position at all, because work is claimed rather than read from a place. A
key that quietly does nothing on two of the three is a key somebody sets
and then trusts.

**What decides it is what the channel holds** - a channel of events wants
the floor, a channel of commands wants the tail - and the operator knows
which when the broker cannot. One answer for every reader whatever
protocol it speaks, and the rest, history on a `tail` channel by a seek
included, is RFC 0003 "Where a subscription starts".

```yaml
channels:
  commands:
    type: append
    filter: iot/+/+/commands
    start: tail          # a rebooting fleet must not act on old orders
```

### Bounds on what a channel holds

Three keys, and the two size bounds are not two spellings of one thing:

| | `retention_period` | `retention_bytes` | `max_bytes` |
|---|---|---|---|
| `append`, and a queue's `__dlq` | yes | yes | yes |
| `latest` | yes | - | - |
| `queue` | - | - | yes |
| `broker.retained` | yes | - | - |
| a provider | - | - | yes |

`retention_bytes` **removes** the oldest records to stay under. `max_bytes`
**refuses** the publish that would exceed, with `0x97`. The difference is
what happens to a producer: retention deletes behind it and never says so,
which is right for telemetry that ages out and wrong for anything an
auditor will ask about, while `max_bytes` says no and keeps everything,
which is right when losing a record is worse than refusing one. The one
thing a provider gives up to make room is the broadcast log of the
sessions it holds, never a channel's record ("Every session's state:
`broker.session`").

**What a size bound counts is a record's size, not the file's.** A
channel's `retention_bytes` and `max_bytes`, and a memory provider's
`max_bytes`, count each record as its payload, its topic, its message id,
and the key and value of each of its User Properties (`store.RecordSize`),
and a memory provider counts the sessions it holds as well, each as the
memory the broker holds for it ("Every session's state: `broker.session`").
A sqlite provider's `max_bytes` is different: it bounds the file itself, in
pages (RFC 0004 "A sqlite provider's bound is SQLite's own"). So a sqlite
file is larger than what its channels count. Measured on one append channel,
after a clean close, a record took about 75 bytes more on disk than it was
counted: 280 bytes against 205 at a 128-byte payload (1.37x), and 16,963
against 16,461 at 16KB (1.03x). While the broker runs the write-ahead log
adds to that (RFC 0004 "WAL, and `synchronous=NORMAL`"), and pages that
retention frees are reused rather than returned, so the file does not shrink
(RFC 0004 "Reading it while the broker runs"). Size a sqlite provider's
`max_bytes` above the sum of its channels' bounds by that margin.

**A `latest` channel takes neither size key**, and the reason is its shape
rather than its cost. It holds one value per topic, so it is not a history
that grows - what grows is the number of topics, and the tool for a topic
that has gone quiet is the period. A byte cap there would answer a question
the channel does not pose.

**A queue takes `max_bytes` and never retention.** An age- or size-based
rule that deletes unacknowledged work is eviction of unresolved work,
which is never permitted (invariant 2). A queue's size bound is therefore
backpressure and not a limit: resolution is what frees the space, so a
queue at its bound refuses new work with `0x97` until its workers catch
up, and recovers on its own.

A queue's dead-letter channel is an ordinary `append` channel and takes
ordinary retention - but it cannot be configured directly, because derived
names are part of the topic contract. Its two keys live in the queue's
block instead, spelled the same with a prefix: `dlq_retention_period` and
`dlq_retention_bytes`. They are copied onto the derived channel, which
already inherits the queue's storage provider the same way.

### Retention is stated once, and may be `none`

`broker.storage.default_retention_period` and `default_retention_bytes`
are what a channel gets when it does not say. Both are **required**,
exactly as `broker.storage.default` is, and both accept `none`.

There is no built-in figure, because both possible defaults are wrong in
the way this file already refuses elsewhere. A built-in period makes a
durable, replayable log quietly mean "the last three days of one", and a
reader who did not exist yesterday finds yesterday's events missing with
nothing to tell them - the retention floor only reports the gap to a
consumer that already had a position. A built-in `none` makes every
channel grow until the disk does not, which is invariant 13's failure with
no defined behaviour at the bound. So the operator writes the number, once
for the broker, and a channel that differs overrides it.

**The default applies to `latest` channels too, and means something else
there.** On an `append` channel a period drops old events. On a `latest`
channel it deletes the *current value* of any topic that has gone quiet
for longer than the period, so a device reporting weekly loses its state
under a three-day default. That is deliberate - a stale reading is worse
than none, and RFC 0003 says so where `latest` expiry is described - but
it is the one place where one rule across two channel types produces two
different outcomes, and a fleet with slow devices will meet it. Such a
channel states its own period.

Set the period to what makes a value worthless rather than to what makes
the disk comfortable; the consumer's half - nothing older than the period
is served, and every value says when it was set - is RFC 0003's.

### Retained messages on a broadcast topic

A client may ask for a message to be kept as the current value of its
topic - the RETAIN flag. What each channel type answers it is under
*Publishing*; on a broadcast topic Sagüin keeps the value in the retained
store, which every broker has, and one optional block changes where it is
kept and for how long:

```yaml
broker:
  retained:
    storage: local          # defaults to broker.storage.default
    retention_period: none  # the default
```

**Absent means the defaults, never refused.** The store is bounded by its
provider's `max_bytes`, like everything else on that provider, so a client
sending the flag cannot grow it past what the operator wrote. Taking
retained values away from a client is the `acl_file`'s job ("Taking a
feature away: `broker: features`"); there is no broker-wide switch.

**It is one store, and it is not a channel.** It claims no prefix, has no
name, and holds only topics that no channel claims - one store rather
than channels a client's publishes could bring into existence unbounded,
which the configuration file would not describe.

**What it holds, and what it hands back.** One current value per topic,
with a zero-length payload deleting the entry, which is MQTT's own
convention. A subscriber is sent the stored value of every topic its
filters reach at the moment it subscribes, carrying the RETAIN flag so that
it can tell state it is catching up on from an update that has just
happened. A retained publish is *also* delivered live to whoever is
subscribed at the time, because it is still a broadcast.

Sagüin delivers those values itself, from this store, and the server's own
retained store is left holding nothing: a broadcast publish passes through
it and is taken straight back out, and a channel's record never enters it
at all. That is the same arrangement `latest` channels already use, and for
the same reason - a subscriber is owed what is current when it subscribes,
which is not what a fan-out at publish time delivers.

The value goes back out as it was published. It carries no `saguin-id` and
no `saguin-offset`: a broadcast message has no record identity and no
position, and stamping one on the way out would make broadcast look like a
channel to whichever client happened to subscribe late.

**`retention_period` defaults to `none`**: a period deletes the current
value of any topic quiet for longer than it, so a device reporting weekly
would lose its state under any defaulted figure. Expiry that belongs to
one value is its publisher's Message Expiry Interval, which applies to
the stored value; an operator who wants a blanket period states one.

**The two clocks delete independently, and the value dies at whichever
runs out first.** A publisher's interval removes the value whether or not
a period is configured, which is what makes the default above safe; a
period removes a value whose publisher set no interval, which is most of
them. What a subscriber is told is the publisher's countdown alone,
decremented by the wait - the operator's period is not a client's
business, and a value already past its expiry is not served at all.
RFC 0003 "Retained messages" has the delivery half.

**Size is the provider's `max_bytes`, and the answer at the bound is a
refusal**, `0x97 (Quota exceeded)`, with what is already stored untouched:
deleting one device's current state to make room for another's would be a
broker losing something still true, silently. The store takes neither
`retention_bytes` nor a `max_bytes` of its own, for the reason a `latest`
channel takes neither.

**The `CONNACK` says Retain Available 1**, to every client. A client the
`acl_file` denies `retained` is told 1 as well, because channels still take
the flag from it, and MQTT gives one answer for the whole broker with no way
to say "on these prefixes".

**A topic that becomes a channel's is moved or dropped at startup, and is
never held in both places.** Configuration changes only at startup, so that
is where it is settled, for every stored topic a channel now claims:

- Into a **`latest`** channel the value is **moved**, and the startup line
  says how many went where. The two are the same thing under different
  names - one current value per topic - so nothing is lost and nothing
  changes meaning.
- For an **`append`** channel, a **`queue`**, or a dead-letter channel the
  values are **discarded**, and the startup line names the channel and
  counts them. Keeping them would not be a move: in an append channel a
  current value becomes an event that never happened, carrying a timestamp
  older than records already in the log, which is the ordering every
  consumer and every seek by time depends on; in a queue it becomes work
  nobody sent.
- A move the destination cannot take - its provider is at `max_bytes` - is
  a **startup error** naming the channel, the provider and how many values
  it was, rather than a broker that starts having quietly dropped state an
  operator asked it to keep.

### Exactly-once publishes: `broker.qos2`

QoS 2 transfers ownership of the message to the broker at the `PUBREC`,
a full round trip before the client is told it is done, so a broker
offering it holds messages nobody has finished sending. Every Sagüin
offers it, and one optional block changes how many one client may hold and
for how long:

```yaml
broker:
  qos2:
    max_inflight_per_client: 20  # the default
    expires_after: 5m            # the default
```

**Absent means the defaults.** There is no `qos2: false` and no broker-wide
switch: taking QoS 2 away from a client is the `acl_file`'s job ("Taking a
feature away: `broker: features`").

**Each held message sits in the store of the channel it is for**, or the
broadcast log's for a broadcast, inside that store's provider's `max_bytes`
and inside `max_inflight_per_client`, which counts one client's held
messages across every channel and broadcast - never in memory nothing
bounds. The release is one operation in the store that keeps the record,
so a refusal or a crash cannot fall between taking the message out of one
store and writing it to another.

**The record is written when the `PUBREL` arrives**, and RFC 0003 has
what that buys. A held message outlives a restart as far as the provider
of its channel, or of the broadcast log, does and is handed back to the
session that sent it; an exchange whose session did not come back is
dropped at that start and counted.

| Key | Default | |
|---|---|---|
| `max_inflight_per_client` | `20` | How many exactly-once publishes one client may have unreleased at once. Past it the next is refused on its `PUBREC` with `0x97 (Quota exceeded)`, before ownership is taken, and the connection is left alone. **Per client**, so one publisher that opens exchanges and never releases them stalls itself rather than taking room from publishers that are behaving. A count rather than a size, because `limits.max_message_size` already caps what one can be |
| `expires_after` | `5m` | Drops a publish that has waited this long for its `PUBREL`, and the release arriving afterwards is answered `0x92`. A connected client releases within a round trip, so what this covers is one whose link dropped after the `PUBREC` and comes back to the same session to finish. Without it, such a message waits until the session ends, which `limits.max_session_expiry` puts at 30 days |

There is no `max_bytes` here, for the reason `broker.retained` has none: a
held message counts against the **provider's** `max_bytes` - its
channel's, or the broadcast log's - and what bounds one client is
`max_inflight_per_client`. Writing the key is a startup error rather than
a bound somebody believes they set.

`saguin_qos2_held`, `saguin_qos2_abandoned_total` and
`saguin_qos2_max_inflight_per_client` are what an operator watches (RFC
0005), and the second of those is the only place a publisher that opens
exchanges and never finishes them is visible at all.

### What a shared group is owed: `broker.share`

A shared group whose members are all away is owed the deliveries that
matched it while nobody was there to take them. They stay in the broadcast
log, in the provider `broker.session.storage` names, behind a cursor the
group keeps with the sessions (RFC 0003 "Broadcast"), so a backlog keeps
across a restart what the sessions keep. One optional block says how long
one waits:

```yaml
broker:
  share:
    expires_after: 1h       # omitted, a backlog lives as long as a member session
```

| Key | Default | |
|---|---|---|
| `expires_after` | none | Drops a backlogged delivery that has waited this long. Omitted, a backlog lives exactly as long as a member session that could collect it, which is the conservative default: the alternative is a broker quietly discarding work nobody asked it to bound |

**A group holds nothing unless one of its members has a session that
outlives its connection** - a backlog for an all-clean-start group is
memory nothing will ever collect, so those deliveries are dropped and
counted. **Only QoS 1 and 2 are held**: a QoS 0 delivery is owed to
nobody by anything.

What one group's backlog may cost is `limits.session_queue_bytes`, and when
it is reached the oldest goes, as a session's does - a group is owed
messages the way a session is, and bounding it by a second figure that had
to agree with the first is two numbers where one will do.

### Every session's state: `broker.session`

A session is what MQTT keeps for a client between packets: its
subscriptions, the messages it is owed or has in flight, and the Will it
armed. One optional block says which provider keeps it, for every session,
persistent or not:

```yaml
broker:
  session:
    storage: local              # defaults to broker.storage.default
    ack_commit_interval: 200ms  # the default
```

**Each kind of data has one storage key, named after the data**, and this is
the one for sessions: a channel's records go where the channel's `storage`
says, retained messages where `broker.retained.storage` says, and session
state here, whatever the client asked for.

**What survives is what the provider survives**: a persistent session on a
`sqlite` provider survives a crash, on a memory provider a graceful
shutdown. What comes back with a session and what ends with it is RFC 0003
"Sessions".

**What it costs is one write per publish.** A QoS 1 or 2 broadcast owed to
any session that outlives its connection is written to the provider's
broadcast log once, however many sessions are owed it, and each session
keeps only its cursor and its window (RFC 0003 "Broadcast"). Crash survival
or throughput is the operator's pick of provider; QoS 0 has nothing to keep
and costs nothing either way.

**`ack_commit_interval` is how long a connected session's acknowledgements
may wait to be taken for writing.** A channel consumer's position and a
broadcast session's acknowledgements are both taken for writing at most
this long after they move, however much the session is still owed, off the
connection's read loop, so no client's acknowledgements wait on the write;
they are stored when that write reaches the provider. On a `sqlite`
provider the write waits behind those already queued for its one
connection - the writes a client's packet is waiting on first, the rest in
turns (RFC 0004 "Group commit") - so under a storage backlog, a reconnect
storm or a slow disk, they are stored later than the interval; on a
`memory` provider there is no queue. It is what an unclean stop sends
again: invariant 18's first exception, never more than a session's Receive
Maximum messages of the broadcast log, and RFC 0003 "Broadcast" and
"Durable consumers" say what that is on the wire. A clean `DISCONNECT` and
a graceful stop store them whatever this says.

| | `ack_commit_interval` |
|---|---|
| **Absent** | `200ms` |
| **Accepted** | a duration from `10ms` to `1s` |
| **Refused** | `0` and anything negative; `none`, since nothing stops acknowledgements being stored; below `10ms`; above `1s`; anything unparseable |

Past a second, the last moment an unclean stop sends again is seconds of
duplicate traffic for every busy session, and acknowledgements wait that
long in memory and in the store. Below ten milliseconds nothing is gained:
storing after every batch measured no faster than the default.

**A Will is session state, and it is kept here from the `CONNECT` that
armed it** (section 3.1.2.5). It has to survive a restart because of the
Will Delay Interval: a Will waiting out its delay is an announcement
nobody has made yet, and a broker restarting inside that wait would
restart into silence - the device gone, and nothing ever said.

**A Will the provider has no room for refuses the connection**, `0x97 (Quota
exceeded)`, with a Reason String naming what filled - where a *session* it has
no room for is accepted and told in its `CONNACK` that it ends with this
connection. The difference is what MQTT lets a server say: a session ending
with its connection has a property for saying so, and there is none for "your
Will is not held", so a device that armed one and was accepted would go on
believing the broker would speak for it if it died. It is counted in
`saguin_connections_refused_total{reason="session store full"}` (RFC 0005),
and not as a dropped session: it was never accepted.

**A provider that fails for another reason refuses rather than guessing**,
`0x83`, counted in `saguin_storage_errors_total` and logged: a Will it could
not write is not held any more than one it had no room for, and a resumed
session it could not read cannot be written back without the subscriptions
it could not read. The client connects again. **An `UNSUBSCRIBE` it cannot
store is refused the same way**, `0x83` for each filter, and the client stays
subscribed to them: the `UNSUBACK` is sent once the session is stored without
them, since a subscription the client was told was gone would otherwise be
back after a crash (invariant 18). A 3.1.1 `UNSUBACK` has no code to say it
with, so that client is disconnected instead.

There is no `max_bytes` here, for the reason `broker.retained` has none: the
**provider's** `max_bytes` bounds the store, and what bounds one session is
`limits.session_queue_bytes`. Taking a persistent session away from a client
is the `acl_file`'s `persistent` denial.

**What a session counts against its provider is the memory the broker
holds for it**, because that is what a session costs while its client is
away, for up to `limits.max_session_expiry`: 3,500 bytes and its client
id's length; for each filter 800 bytes, 400 more for a shared one, 600 for
each level after its first, and the filter's length; and its Will's topic,
payload and properties. A level two sessions' filters share is charged to
each, so this is a bound on what they take rather than a measurement of it.
At a memory provider's 64MiB that is about 12,000 sessions of one
three-level filter each, or about 2,250 of ten four-level filters. A sqlite
provider's `max_bytes` bounds its file rather than this memory ("Bounds on
what a channel holds"), and `limits.max_session_expiry` says what bounds it
there.

**Sessions that fill a memory provider are refused room like anything
else, and they leave only as they expire.** A snapshot restored over the
provider's `max_bytes` - one written under a larger bound - is loaded whole
and nothing in it is dropped. Then, until there is room, the provider
refuses every publish into a channel it holds (`0x97` where the publish has
an acknowledgement to carry it), every `SUBSCRIBE` (`0x97`), and every
resume that carries a Will (`0x97` at `CONNACK`); a broadcast its sessions
are owed is not kept for them, counted as `storage_full`. A resume without a
Will is not refused. Retention frees none of that room, because it removes
records and not sessions; the sessions go when they expire, which
`limits.max_session_expiry` can put a month away. The start says so once,
naming the provider and how much of it the sessions hold. The remedy is a
larger `max_bytes`, or a sqlite provider for `broker.session.storage`.

**A full session store answers in MQTT's own signals, and keeps nothing by
half.**

| When the store has no room for | The client is told | What is kept |
|---|---|---|
| A session asking to outlive its connection, at `CONNECT` | Accepted, with Session Expiry Interval 0 in its `CONNACK` (3.1.1: the session is made clean) | Nothing: the session ends with the connection. Counted in `saguin_sessions_dropped_total{cause="storage_full"}` |
| A `SUBSCRIBE`, from any client | `0x97` (Quota exceeded) for every filter the packet would add or change (3.1.1: `0x80`) | The session as it was: an existing subscription the packet tried to change goes on being served as it was granted |
| A QoS 1 or QoS 2 broadcast to sessions that outlive their connections | Nothing: the loss is the sessions', not the publisher's, which is answered `0x00` (RFC 0003 "Broadcast") | The broadcast log's oldest messages go to make room, the fewest that make it, and each session is counted once for each message it was owed and lost, in `saguin_session_deliveries_dropped_total{cause="storage_full"}`. A message a session has been sent and has not acknowledged stays, for every session owed it: it is that session's to finish. Where nothing in the log can go, the new message is not kept, and is counted the same way for each session it was for. Both are logged too, as a warning at most every ten seconds with the totals since the last - messages, sessions, deliveries - and the remedy: a provider of its own for `broker.session.storage`, or a larger `max_bytes` |
| A QoS 1 or QoS 2 broadcast a shared group is owed, or a channel's record a group over the channel is owed | Nothing: the group's loss is not the publisher's, which is answered `0x00` (RFC 0003 "Broadcast") | The group's backlog is in the same log, so the row above is what happens: when the log's oldest part goes, what the group was owed from it is counted in `saguin_shares_dropped_total{cause="storage_full"}` |

**The broadcast log gives way to everything else on its provider.**
`broker.session.storage` is `broker.storage.default` unless it names
another, so by default the log shares one `max_bytes` with the channels
there, the exactly-once publishes held in them, the retained store and the
sessions' own records. A write any of them makes that finds the provider
full takes its room from the log: the log's oldest messages go as they do
for its own next message, counted and logged the same way, the warning
naming what the room was for. Only when the log has nothing left it can
give is the write refused, answered as its own store answers a full
provider. A store's own bound - a channel's `max_bytes` - is not the
provider's, and the log gives nothing for it. Channels come first because
they are what an operator configures and bounds, where the log holds what
sessions were owed while they were away; a provider of its own for
`broker.session.storage` keeps the two apart.

**A `sqlite` provider makes room by the page, not by the message.** Its
bound is a page count, so what goes frees room only once about a page's
worth has gone: making room for one message can take several older ones,
where a memory provider gives up exactly the bytes it needs.

A subscription is session state for a client that keeps no session too, so
a full store refuses a new one from every client alike; what is already
subscribed goes on matching, at QoS 0 always and at QoS 1 and QoS 2 as the
row above allows.

### Validation

**Only the keys this document defines are accepted.** Any other is a
startup error that names it, whatever its value, rather than a key the
broker ignores while the operator believes it is in force.

The configuration is validated in full before the broker opens a
listener, and every finding is reported, not only the first. An invalid
configuration is a startup failure; there is no partial start in which
some channels work.

Rules:

- Every channel name satisfies the name rules above.
- **Every `filter` is a topic filter MQTT would accept**: `#` appears at
  most once and only as the last level, and no level carries a NUL. A
  channel writing no filter is validated as though it had written
  `<name>/#`.
- **A filter's first level is spelled out and does not begin with `$`.** A
  `+` or a `#` there claims `$SYS/…` and `$saguin/…` along with everything
  else, and a channel that swallows the broker's own control topics is a
  broker with no control topics. This is the one restriction on a filter
  that MQTT itself does not make, and it is here rather than in the matcher
  so that the matcher stays the ordinary MQTT one.
- **No two channels carry the same filter**, compared after `{a,b}` is
  expanded. The error names both channels and the expansion that collided,
  because with braces the two lines in the file need not look alike.
- **No filter an operator writes carries a level equal to `__dlq`.** Every
  filter that does is one the broker derived for a queue's dead letters.
- **A queue's filter carries no `{a,b}`.** Its workers pin one exact
  string, and two strings are two consumer groups that each take a copy of
  every job (invariant 4).
- **Braces hold no `/`**, and neither a brace nor an alternative inside one
  is empty. An alternative is one topic level and never a subtree.
- **A filter leaves room for a suffix under `limits.max_topic_length`**, so
  that no channel can be configured to claim topics that no publish would
  be allowed to carry.
- **`broker.storage.providers` defines at least one provider, and
  `broker.storage.default` names one of them.** Sagüin keeps nothing in a
  store the operator did not name, so a configuration without both does
  not start. A memory provider with `snapshot_dir: none` is how to keep
  nothing across a restart.
- Every `storage` names a defined provider.
- `retention_bytes` and `max_bytes` are rejected on `latest`, and
  `retention_period` and `retention_bytes` on `queue`, per the table under
  "Bounds on what a channel holds" and invariant 2.
- `dlq_retention_period` and `dlq_retention_bytes` are rejected on any
  channel that is not a `queue`, the only kind that derives one.
- `broker.retained.storage` names a defined provider, and defaults to
  `broker.storage.default`; `retention_bytes` and `max_bytes` are rejected
  there, per the same table.
- `default_retention_period` and `default_retention_bytes` are always
  stated. `none` is a value, not an omission: a channel that keeps
  everything says so.
- A period is a duration and a bytes bound is a size, both under the rules
  below, or the word `none`. Zero is refused for either, and the reason is
  that it has no honest meaning here. Read as a period it says "keep
  nothing", which is not a channel; read as Sagüin stores it internally it
  is indistinguishable from `none`, which says keep everything. A value
  that can be argued into meaning either of two opposite things is a value
  to refuse rather than to define, so `none` is the only way to say
  "keep everything" and there is no way at all to say "keep nothing".
- A provider's `max_bytes` is at least twice what it holds back for the
  operations which free it - `max_message_size` plus `max_header_bytes`,
  around a megabyte at the defaults; RFC 0003 has why the reserve exists
  and RFC 0004 how each provider keeps it. The error names both figures
  and says which limit to lower.
- **A `sqlite` provider's `max_bytes` is also above what its empty database
  takes**, 88KiB, plus twice that reserve. The file's tables take pages
  before any record does, and SQLite will not set a ceiling below the pages
  in use, so a smaller figure opens a provider over its own bound that can
  never hold a record.
- `broker.session.storage` names a defined provider, and defaults to
  `broker.storage.default`. `max_bytes` is rejected there: the provider
  bounds the store.
- `broker.share.expires_after` parses as a duration in whole seconds.

  A *channel's* `max_bytes` takes no such rule. The dead-letter move does
  not check a channel bound at all - a queue and the channel it
  dead-letters into are two different channels, so there is nothing there
  for a reserve to protect.
- `visibility_timeout`, `job_expires_after` and `retry` are rejected on any
  channel that is not a `queue`.
- `visibility_timeout` is greater than zero and less than `job_expires_after`,
  and is `30s` where none is written.
- `retry.max_attempts` is at least 1, and 5 where none is written.
- `job_expires_after` has no default and may be omitted: a queue that does
  not write it never expires work. It is the only clock that ends a job - a
  publisher's own Message Expiry Interval does not, and reaches the worker
  as `saguin-expires` to decide on instead (RFC 0003 "`queue` -
  Dead-lettering").
- `retry.backoff` is `none`, `linear` or `exponential`, and defaults to
  `none`. `retry.backoff_base` is a duration in whole seconds like every
  other interval here.
- **`retry.backoff` and `retry.backoff_base` are written together or
  neither is written.** A shape with no base has no gap to build from and a
  base with no shape is read by nothing, so either alone is refused at
  startup rather than starting a broker that looks configured.
- A sqlite provider declares the `file_path` of its database, and a
  memory provider declares either a `snapshot_dir` or
  `snapshot_dir: none`. Neither takes the other's key: they name
  different kinds of thing and promise different amounts, and accepting
  the wrong one would leave an operator believing they had configured
  something. There is no default, because both defaults are
  wrong: defaulting to a path invents a filename in the operator's
  filesystem, and defaulting to none makes "durable channel, memory
  provider" silently mean "discarded on shutdown" - the durability
  surprise. The operator states which they meant.
- A provider no channel names is inert - nothing is created for it, and
  the broker says so once at startup. Declaring one is not an error:
  staging a migration needs the destination provider named before
  anything writes to it (RFC 0004 has the sequence).
- One broker at a time writes a storage provider. A sqlite provider is
  held by `<file_path>.lock`, a memory provider by `saguin.lock` in its
  `snapshot_dir`, and a broker that finds one held refuses to start, naming
  the provider and the lock. Two configurations naming one provider is the
  way this happens, since the listener only catches brokers that also want
  the same port.

  Both would fail without it, and the memory one fails worse: two brokers
  on one database collide on the primary key - loud, and nothing lost -
  where two on one snapshot directory silently replace each other's
  channels at shutdown and reuse offsets, invariant 9's failure.

  The lock is a file of its own so that a sqlite database stays readable
  while the broker runs: `sqlite3` can query a live channel, which locking
  the database itself would prevent. The kernel releases the lock when a
  process ends, so a broker that was killed leaves nothing to clean up -
  but deleting a lock file underneath a running broker does defeat it.
- One broker at a time serves a Unix socket path, the MQTT one and the
  operations one alike. The socket is held by `<path>.lock` for as long as
  the broker runs, and what a starting broker does depends on it:

  | At the path | The lock | What happens |
  |---|---|---|
  | nothing | free | the socket is created |
  | a socket | free | it was left behind by a broker that crashed, was killed or lost power, and it is replaced |
  | a socket | held | another broker is serving it, and this one refuses to start, naming the lock |
  | anything that is not a socket | either | refused and left alone: a file or directory there is a mistake in the configuration |

  A leftover socket is replaced rather than refused because a broker
  restarting unattended after a power cut must come back without anyone
  removing a file. At shutdown a broker removes its socket before
  releasing the lock, so it only ever removes its own, and the lock file
  is kept for the reason above. An abstract name, beginning with `@`, has
  no file to leave behind and takes no lock.
- Each listener is a block of its own, so a setting belonging to one
  cannot be written beside another. A listener block with no address, or
  a Unix socket with no path, is a mistake rather than a default.
- `allow_anonymous` is rejected under `broker.operations.listen.tcp` and
  `broker.operations.listen.unix`, and each door's own `password_file` is
  honoured and absolute - "The operations listener" above has both
  arguments.
- **A configuration naming no listener at all gets `tcp` on `:1883`**,
  every interface - reachable by anybody who can route to the box, and
  with no `password_file` it admits them. One listener named is enough to
  turn the default off: a configuration naming just a Unix socket must
  not silently also open the TCP port its author was avoiding.
- A key naming a filesystem location says which kind it is: `file_path`
  names a file, `snapshot_dir` names a directory. They are not the same
  kind of thing, and one name for both would imply one guarantee.
- Durations are written as a whole number and one unit, from `ms`, `s`,
  `m`, `h` and `d` - `90m` rather than `1h30m`.

  **`ms` is accepted by the form and refused by almost every key that
  uses it.** Nearly every duration in this file is held in whole seconds,
  and the five places a sub-second value is honoured are a bridge's
  `ack_interval`, which is a handful of milliseconds by default; a `sqlite`
  provider's `publish_commit_interval`, which is only useful at that scale,
  and its `flush_interval`, which is 150 milliseconds by default;
  `broker.session.ack_commit_interval`, which is 200 milliseconds by
  default; and `limits.write_timeout`, where a deadline of a few hundred
  milliseconds is a reasonable thing to want on a local network.

  What the refusal prevents: a sub-second duration divided into whole
  seconds is **zero**, and zero is a sentinel - `retention_period: 500ms`
  would mean what `none` means, and `visibility_timeout: 500ms` a
  redelivery loop. The refusal is the *key's* - "held in whole seconds" -
  not the form's, so an operator is not sent looking for a typo they did
  not make.
- `snapshot_dir` is a directory, and it is absolute. A memory provider
  writes one file per channel - a queue and its dead-letter channel share
  one, so the move between them cannot be recorded by half - plus a
  manifest naming what was written and when. One file per channel is what
  makes a channel dropped from the configuration a file nobody opens
  rather than a startup error. A relative path is refused, because a
  snapshot must not depend on the directory the broker was started from.
- A channel name fits a snapshot file name. The encoding percent-encodes
  a dot - so no name can reach `.` or `..` however it is spelled - which
  trebles it: a hundred dots is a legal channel name and a 309-character
  file. It is refused here, where it costs a restart, rather than at the
  shutdown that would lose the channel.

Bridges, where a configuration has any:

- A bridge names a `peer`, whose scheme is one of `tcp`, `tls`, `ws`
  or `wss` and which carries a host and a port. `mqtt://` is not a scheme
  anything can dial, and it is refused here rather than at the first
  connection.
- A bridge names a `client_id`, and there is no default. It is Sagüin's
  identity at the far end - it decides which session the peer resumes,
  and once there is authentication, which principal it is. An invented one
  means two edge boxes silently share a session and take half each other's
  messages.
- **No two bridges share a `client_id` at one `peer`.** MQTT closes the
  older session when a new one arrives with the same identifier, so two
  such bridges disconnect each other as fast as they can reconnect and
  neither delivers anything, with nothing to show for it but a connection
  log that scrolls. The same identifier at two *different* peers is
  fine: the sessions are at different brokers and never meet.
- A bridge carries something. That is at least one rule under `topics`. A
  bridge carrying nothing connects, asks for nothing, and holds a session
  open at somebody else's broker for ever.
- `session_expiry`, `receive_maximum` and `ack_interval` are optional and
  each has a default. Where one is written:
  - `session_expiry` is a duration, no longer than MQTT's own ceiling of
    136 years, and it is **not** `none`, which every other duration in
    this file accepts: a session that never expires sits on somebody
    else's broker after the edge box is decommissioned, and only that
    operator can clear it. A bridge is a guest, and `none` is the one
    value that makes it a permanent one.
  - `receive_maximum` is between 1 and 65535, which is MQTT's own range for
    it. Zero is not "unlimited", it is a subscriber that may receive
    nothing, and MQTT forbids sending it.
  - `ack_interval` is a duration between `1ms` and `1s`. It is one of the
    five keys in this file that may be given a sub-second value, the others
    being a `sqlite` provider's `publish_commit_interval` and
    `flush_interval`, `broker.session.ack_commit_interval` and
    `limits.write_timeout`, and the ceiling is there because the only reason
    to delay an acknowledgement is to batch the ones behind it - which gains
    nothing after a few milliseconds, and past a second is indistinguishable
    from a bridge that has stopped.

  Each is refused with what it costs rather than only what the range is,
  because all three are the sort of number somebody raises to make a
  symptom go away: the ceiling arithmetic for `ack_interval`, what is held
  unacknowledged for `receive_maximum`, and whose storage is being spent
  for `session_expiry`.
- Every rule names a `filter` and a `direction` - `in`, `out` or `both`,
  with no default: an omitted key deciding whether this broker's records
  leave it is not something you could see in the file.
- `topic:` is required on an `in` or an `out` rule, and refused on a
  `both`, which is the identity mapping ("Bridges").
- **The topic decides where a record lands**, as it does for every
  publisher: a rule names no channel.
- A rule's `filter` may not name the reserved `$saguin/` space, in any
  direction: inbound it would carry a queue's deliveries across the link,
  taking leases nothing acknowledges (invariant 6). `$SYS/#` is allowed:
  Sagüin refuses the space it defines, not the character. It is allowed
  `in` with a template; `both` on a filter beginning with `$` is refused,
  because it maps a topic to itself and nothing may publish a `$` topic
  here.
- A filter is a well-formed MQTT topic filter: `#` is only ever the last
  level, and `+` and `#` each take a whole level.
- **Two outbound rules with one filter and one topic are refused**, a
  braced spelling meeting a written one included. They would send every
  record to the same topic at the peer twice, and share the one position
  their filter and topic name.
- A `{a,b}` level is **expanded into one rule per spelling**, and every
  rule above is asked of what it expanded to. The notation is Sagüin's own
  and the far end is an ordinary broker: a brace left in place goes on the
  wire as one literal level, is granted, and receives nothing - a link
  that is configured, reports no error and carries no records. The
  template is unaffected, because `$1`, `$2` … number the filter's `+`
  levels and a braced level is not one.

  `fleet/a+b/#` is **refused**: MQTT-4.7.1-2 makes the single-level
  wildcard a whole level, and a publish carrying `+` or `#` in its topic
  is refused everywhere, so no conforming broker can ever hold a level
  spelled `a+b` - the filter is dead by construction and a bridge built on
  one silently carries nothing. A filter that merely has nothing
  publishing to it yet stays legal: `fleet/vessel-99/#` must keep working
  when the vessel comes back, and no startup check can tell it from one
  that never matches.
- A `topic` uses only substitutions its filter provides: `$1` … `$N` for
  the `+` levels it has, and `$#` only where it ends in `#`. `$#` appears
  only at the end, as `#` does in a filter, because a tail is any number of
  levels and one in the middle makes the shape of the result depend on how
  deep the peer published.
- **A filter ending in `#` has a topic that uses `$#`.** A discarded tail
  collapses an unbounded number of the peer's topics onto one local topic. On
  an append channel that is a mess; on a `latest` channel it is
  destructive, because two topics become one value overwriting itself while
  the channel does exactly what it is designed to do.

  A discarded `$1` is *not* refused. It collapses finitely many topics and
  is a choice an operator can reasonably make - one bridge per vessel drops
  the vessel id on purpose - so only the unbounded case is an error.

  **`--check-config` names each rule that discards one**, saying which
  substitution went unused and what it costs, and says separately when the
  channel is `latest`, where the collapse overwrites a value rather than
  crowding a topic. It is a note rather than a finding: the exit code does
  not move and the configuration is still reported `ok`.
- A `topic` holds no `+` or `#` of its own, does not begin or end with `/`,
  and has no bare `$`. A published topic carries no wildcard, a suffix does
  not lead with a separator, and `$x` is a typo that would otherwise become
  literal text.

`--config` names the file and is required: a broker that read whatever
`saguin.yaml` was in the working directory would serve a different
configuration depending on where it was started from.

`--check-config` performs the whole of the above, touches no storage,
opens no socket, and exits non-zero on any finding. That exit code is what
makes it usable as a systemd `ExecStartPre` and in CI: a bad configuration
fails the unit rather than the broker. On success it prints the file it
read and the channels it found, to standard output, so redirect it where
silence is wanted.

**It also opens every file the configuration names** - password files,
certificates and keys, client and bridge authorities - and reports all of
them at once: a schema that parses is not a configuration that starts,
and a check that read none of those files would pass before the unit died
anyway, at a restart. Startup reads them the same way, one message naming
every unreadable file.

It still only reads. Nothing is created, nothing is written, no socket is
opened, so this runs against a configuration from a backup and while a
broker is up - but on a machine that does not have the files, it says so,
which is true rather than convenient. `--output` still prints its
document there, because somebody validating a configuration from elsewhere
wanted the document; the findings go to standard error and the exit code is
non-zero.

`--passwd` manages a password file and exits, in the broker's own binary
because Sagüin ships as one file and "install this other tool to add a user"
is not something a single binary gets to say:

```sh
saguin --passwd list   /etc/saguin/clients.passwd
saguin --passwd add    /etc/saguin/clients.passwd device-7 [password]
saguin --passwd delete /etc/saguin/clients.passwd device-7
```

The verbs are `mosquitto_passwd`'s, so an operator who has managed a
Mosquitto fleet already knows them; `list` is an addition, because a
hashed file cannot be read by eye, and `scope`, which narrows an operator
to some routes, is the other (RFC 0005 "Which routes a credential
reaches"). With no password on the command line it is prompted for
twice, with the terminal's echo off. Like a migration, it reads and
writes the file it is given and consults no configuration - which is
what lets a file be prepared before there is a broker to run it, and
lets one be repaired when the configuration is the thing that is wrong.

New entries are written `$7$` with 1000 iterations, rather than a
modern-looking number: the file is read on every CONNECT, and a fleet
reconnecting after a link drop pays that cost at once. `BenchmarkVerify`
puts one check at 0.33ms on a Ryzen 7 260 and 0.27ms on a MacBook Pro M1
Pro, and the cost is linear in the count, so a hundred times the
iterations is a hundred times that on every CONNECT. What it costs is
resistance to offline cracking of a stolen file, and the answer there is
the file's permissions - a file Sagüin creates is `0600`, and one that
exists keeps its mode and owner, as `mosquitto_passwd` keeps them.

`--licenses` prints the licences of the code inside the binary and exits,
before any configuration is read: somebody asking whose code they are
running should not need a configuration file to find out. Sagüin ships as
one file, so there is no `go.mod` beside it and this is the only place that
answer exists. The text is generated from the packages that actually reach
the binary rather than from everything the module file names, and it
travels inside the binary rather than beside it.

**`--output` prints the configuration as it actually resolved** - every
`!include` expanded into one document, every default filled in, and the
file each channel came from written beside it as a YAML comment.

```sh
saguin --check-config saguin.yaml            # exit 0, prints a summary
saguin --check-config saguin.yaml --output   # the resolved file, to stdout
```

It goes to stdout while the summary and any findings go to stderr, so a
redirect produces a clean document whatever else was said. What it buys is
the one question a configuration split across a dozen files cannot
otherwise answer - what the broker will actually serve - which is worth
having before a start rather than after one. Nothing is created and nothing
is opened, so it runs while a broker is up, on another machine, and against
a configuration from a backup. `--output` is given with `--check-config`
and refused without it: a broker that started and also wrote its
configuration to stdout would be writing into whatever its unit's output
happens to be.

**What comes out loads again**, and everything else follows from that: a
channel that named no provider says which one it got, a queue's derived
dead-letter channel is left out (a document carrying it would not start),
and a key nobody wrote is not printed - absence takes the broker-wide
default where the literal `none` is a value and stays. The provenance is
a comment because a reader wants it and a parser must not.

**Credentials are printed as they stand** - no key in the schema holds
one, authentication being configured as paths - and the rule is stated
for the first key that does: a document with its secrets starred out
would not load. The output is exactly as sensitive as the files it
flattens, and belongs in the same place with the same permissions.
