---
name: saguin-config
description: Work out the best saguin configuration for somebody's use case in a guided discovery session - their machine and the loss it can bear, their clients, each data flow and the channel type it fits, access control, listeners, TLS, operations and bridges - starting from scratch or from their existing file, then write the configuration and access files to a folder they name, check them with saguin --check-config, and explain every choice with a reference. Use when somebody wants a saguin configuration designed for them, wants to review or adapt the one they have, or asks which channel type or storage mode suits their deployment.
allowed-tools: Read, Grep, Glob, Bash, Write
argument-hint: [path to an existing configuration file]
---

# Finding somebody's saguin configuration

You sit down with somebody who is about to run saguin, or runs it already,
and work out their configuration with them. They know their devices, their
data and what a lost hour of readings would cost them; you know saguin's
documents. You ask about the first, one topic at a time, and turn the
answers into a configuration. At the end they have a configuration file
that `--check-config` accepts, an access file where they need one, and a
reason under every choice that they can check.

**The person is an operator, not a contributor.** Ask about their
deployment in their words - how many devices, how long one may be offline,
whether a job may run twice - and never ask them for a setting. Turning
the answer into the setting is your job.

## The rules you work under

This is saguin-ask's "Writing somebody a configuration", run as a
conversation. Read `.claude/skills/saguin-ask/SKILL.md` before the first
question, and hold to these sections of it, which are not repeated here:

- **"The one failure this job has"**: nothing is written that you have not
  read in this session, from a file.
- **"The corpus"** and **"Retrieval: count first, read second"**: what
  answers a question, and how to read the design without reading all of it.
- **"Saying where it came from"**: how to cite a document and section.
- **"When the documents do not say"**: refuse a gap; answer a deliberate
  absence with its citation.
- **"Writing somebody a configuration"**: only what config.md lists,
  `--check-config` before handing anything over, the one excusable
  failure, and never inventing a knob.
- **"What you do not do"**: no `git`, no `gh`, no change to a file the
  repository tracks.

**This skill names no setting, value or default, on purpose.** Each topic
below links the sections that own them. Read those sections at that point
of the session and take every setting, its values and its default from
them, so the configuration follows the documents as they are today. The
links are relative to this file; from the checkout's root they are
`docs/config.md`, `docs/design.md`, `docs/storage.md`, `README.md` and
`examples/`.

**Where this differs from saguin-ask.** saguin-ask writes nothing; this
skill writes the result, and only to the folder the operator names, never
inside this checkout. Check copies follow "Checking a file" below.

Read [config.md](../../../docs/config.md#sagüin-configuration) whole at
the start: it is short, and its opening section holds the rules every
value follows, such as how paths and durations are written.

## Checking a file

What `--check-config` and its `--output` print is in
[design.md › Operations](../../../docs/design.md#operations). Two things
about them matter in a session:

- **It prints nothing when it accepts a file.** An empty result with exit
  status 0 is the success to report, not a missing answer.
- **`--output` prints no configuration when the check reports any
  problem**, a missing certificate or password file included: only the
  problem lines, and exit status 1. An operator's file names their
  machine's paths, so on this machine it usually fails that way.

The way round is a **check copy**: a copy of the file in
`.tmp/saguin-config/` in this checkout (ignored; never `/tmp`), whose file
paths point at files present here - an empty placeholder in that folder
for each password file, the demonstration certificates in
`examples/support/certs/` for certificates, and the access file just
written for theirs. Run the check, `--output`, `--route` and `--acl` on the
copy; its paths are the check's, never the operator's. Where a
placeholder is refused, take the defaults from config.md instead and say
so.

## How it starts

**From a file.** A path came with the invocation, or they mention one.
Read the file, then check it, and print it with every default filled in:

```sh
./bin/saguin --check-config <their file>
./bin/saguin --check-config <their file> --output
```

Read "Checking a file" above: when the first command reports any
problem, the second prints no configuration until it runs on a check
copy. If `bin/saguin` is not built, offer `make build` and wait for a yes; if
they have saguin installed elsewhere, use theirs.

Then say back, in plain words, what that configuration does today: what a
power cut costs in its storage mode; where clients connect and how they
prove who they are; each channel, which topics it holds and what a reader
gets from it; what lands on `$broadcast` and how long it stays; any
bridges, the operations endpoint, and the limits moved from their
defaults. Name every problem the check reported. Then ask what they want
changed, and run the discovery below against the file:

- a topic the file settles and the change does not touch is confirmed in
  one line - what the file does there - and not asked again;
- a topic the change touches is asked, but only the questions that change
  needs;
- a fact no file can hold - the drive, the loss they can bear, the longest
  absence, a link's longest outage - is asked where the change depends on
  it, since the file shows what was chosen, not why.

**From scratch.** No file came. Ask what the broker is for, in a sentence,
and what to call it: the broker's name has no default
([config.md › `broker`](../../../docs/config.md#broker) says what it may
hold). Then begin with the machine.

## The discovery

**One topic at a time, and wait for the answer.** Within a topic, ask the
two or three questions it needs together, plainly.

**Never ask what is already settled**, by the documents or by an earlier
answer. Where one answer decides another, say so instead of asking. Queue
workers must speak MQTT 5
([design.md › `$saguin` topics](../../../docs/design.md#saguin-topics)).
An access file needs every client to prove who it is
([config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt)).
[design.md › Store](../../../docs/design.md#store) names a storage mode
one operating system refuses.

**Keep a running list** of every setting decided, its value, the answer
that decided it and the section that backs it. That list becomes the
explanation at the end. Where they do not know, read what the default
does, tell them, and take it. **Never guess a number they did not give**:
a retention, a timeout or a size comes from their answer or stays at its
default, and you say which.

### 1. The machine and the loss it can bear

Ask: what machine it runs on (board or server, operating system, the
drive: SD card, SSD or spinning disk), how much room the data may take,
and what a power cut may cost them - nothing they were told was stored,
the last second or so, or the last half hour.

Read [design.md › Store](../../../docs/design.md#store). For each storage
mode it says what a broker crash, an operating-system crash and a power
cut lose, and what else the mode needs: a RAM-backed directory, an
operating system, room on the disk; how much memory is in
[storage.md › Choosing a machine](../../../docs/storage.md#choosing-a-machine).
Set their answer against it and
choose the mode that loses no more than they can bear; tell them, in the
Store section's words, what that mode can still lose. Where two modes
fit, say what each costs and let them choose. How the drive weighs in is
in [README.md › Bounded, inspectable storage](../../../README.md#bounded-inspectable-storage),
[storage.md › Drive and filesystem](../../../docs/storage.md#drive-and-filesystem)
and storage.md › Choosing a machine.

Then read [config.md › `broker.storage`](../../../docs/config.md#brokerstorage).
Some of its settings belong to one mode alone and are refused in another.
It also needs the database's path - ask where the database will live -
and a default retention.

**The default retention governs every log that omits its own**: each
`append` channel, each queue and its dead-letter channel, and
`$broadcast`. On a `latest` channel it becomes the values' lifetime, and
on a queue the age at which an unfinished job is dead-lettered
([config.md › `channels.<name>`](../../../docs/config.md#channelsname)).
A default chosen for history therefore also ends current values and jobs:
say so, and give those channels their own wherever that is wrong for
them.

**Sizing against the disk.** There is no limit on the whole store, only
each log's own and the disk
([config.md › Dropped](../../../docs/config.md#dropped);
[examples/home-automation/README.md](../../../examples/home-automation/README.md)).
A default sized to the room they gave lets every log that takes it fill
that room, so it allows that many times the room: share the room among
the logs instead. The database file grows to its busiest size and never
shrinks ([storage.md › Capacity](../../../docs/storage.md#capacity)).
The documents do not give a record's stored size beside its payload, so do
not turn a disk size into a count of records or days: say the
documentation does not say, size retention from the room they gave, and
tell them to watch the file's size in its first days.

### 2. The clients

Ask: how many clients connect at once; whether any speak MQTT 3.1.1;
whether they keep a session across disconnects, and the longest any of
them is away; which connect with a fresh session each time (a phone app,
a dashboard, a script); and whether one of them is a gateway publishing
for many devices.

- **How many**, and how long sessions are kept, are limits
  ([config.md › `broker.limits`](../../../docs/config.md#brokerlimits)):
  change one only when their answer is far from the default.
- **MQTT 3.1.1** clients can publish into and read every channel type;
  only a queue's worker subscription and shared subscriptions need MQTT 5
  ([design.md › SUBSCRIBE](../../../docs/design.md#subscribe), its table;
  [README.md › Deliberate constraints](../../../README.md#deliberate-constraints),
  which also says what else they do not receive).
  The oldest version admitted is set in
  [config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt).
- **Away time is bounded twice.** A client away longer than its
  session's expiry comes back as a new reader with no position, which
  starts where the channel's start point says, not where it left off
  ([design.md › Words](../../../docs/design.md#words), Session;
  design.md › SUBSCRIBE, UNSUBSCRIBE); the broker caps that expiry
  (config.md › `broker.limits`). Within it, each channel's retention
  bounds what comes back: retention shorter than the longest absence loses
  records, and the loss is counted (design.md › Words, Retention;
  [README.md › A position, not a copy](../../../README.md#a-position-not-a-copy)).
  Carry the longest absence into topic 3.
- **Where a reader with no position begins** - the oldest record kept,
  or only what comes next - is set per channel
  ([config.md › `channels.<name>`](../../../docs/config.md#channelsname)),
  and what each type starts with is the table in design.md › SUBSCRIBE. It
  applies to every reader with no position: a client with a fresh session,
  every time; a durable session's first subscribe; a new outbound bridge
  rule ([design.md › Bridges](../../../docs/design.md#bridges), Out). A
  phone connecting fresh to a channel keeping 90 days and starting at the
  oldest record replays 90 days each time. A durable session may seek
  before it first reads ([design.md › `$saguin` topics](../../../docs/design.md#saguin-topics)),
  so one channel can serve fresh dashboards and a durable archiver both:
  new readers start live, and the archiver seeks back before its first
  read. Offer that as a choice, and carry it into topic 3.
- **QoS** matters only where an answer turns on it - a device that may
  not use QoS 2, a busy gateway - so ask it there, not of every client.
  QoS 2 is always offered and can be denied per role (topic 4). A busy
  gateway publishing at QoS 1 may need a larger incoming window once it
  runs: leave the default, and point them to
  [storage.md › Settings](../../../docs/storage.md#settings), which sizes
  it from what a running broker measures.

### 3. Each data flow

Ask them to list what flows through the broker: for each, the topics, who
publishes, who reads, and what a reader needs. **Every flow needs its
topics before it can become a channel**: when they say a reader needs the
current state, ask which topics carry that state, before going on.
**When a flow's topics are not known yet**, ask once more who could tell
them; if they are still unknown, say the channel cannot be placed yet and
leave that flow open in the result. Never write a default or guessed
filter for it: it may claim the wrong topics, or none.

Then take one flow at a time, match it to a channel type, and say why it
fits as [design.md › Channels](../../../docs/design.md#channels) and
[README.md › Core concepts](../../../README.md#core-concepts) define the
types. The settings each type takes, and their defaults, are in
[config.md › `channels.<name>`](../../../docs/config.md#channelsname).

- **History, replay, "what happened"** - telemetry, events, audit - is an
  **`append`** channel: a log with delete retention, a stored position per
  durable consumer, and seek by offset or time. Ask how far back a reader
  must reach and how much it may hold. Its retention removes the oldest
  records; its hard limit is a different thing, refusing publishes once
  full ([design.md › PUBLISH](../../../docs/design.md#publish)).
- **Current state, "what is it now"** - device state, settings, a
  setpoint - is a **`latest`** channel: one value per topic, sent on
  subscribe and readable by a point read. An empty payload deletes a value
  and leaves a tombstone; values and tombstones each have their own
  lifetime, and the values' lifetime is the default retention unless the
  channel sets its own (topic 1): ask how long a value may go unchanged
  before it should end, if ever.
- **Work, "do this once"** - jobs a worker takes, does and acknowledges -
  is a **`queue`** channel: each job goes to one worker at a time, the
  worker answers that it is done or hands it back, and failures retry,
  then go to the queue's dead-letter channel. A queue is not a shared
  subscription ([README.md › A real work queue](../../../README.md#a-real-work-queue)).
  Its lifetime follows the default retention unless it sets its own
  (topic 1). Ask how long one job takes, how many tries it deserves and
  how the wait between them should grow. **The time a job takes sets the
  queue's hold**: the hold starts at the worker's PUBACK, and a job still
  running when it ends spends that attempt and is offered again
  ([design.md › Delivery](../../../docs/design.md#delivery), Queue jobs;
  the hold's setting in config.md › `channels.<name>`), so the hold
  must outlast the longest job. **"Retried three times" is four
  attempts**: the queue counts attempts, the first included (Queue jobs,
  and the retry settings in config.md › `channels.<name>`), so ask which they
  mean. A job may reach a worker more than once, so workers must be
  idempotent (README.md › A real work queue).
- **Everything else** - plain publish and subscribe, retained messages as
  MQTT knows them - stays on **`$broadcast`**, the reserved `append`
  channel that follows MQTT and stores every message, whatever its QoS
  (design.md › Channels). Its retention and the retained values' own
  limits are in
  [config.md › `broker.broadcast`, `broker.retained`, `broker.qos2`](../../../docs/config.md#brokerbroadcast-brokerretained-brokerqos2).
  A retain flag on a configured channel's topic is ignored (design.md ›
  Channels), so a device that relies on retained state belongs on a
  `latest` channel or on `$broadcast`.

**Commands to devices that cannot be workers.** A device that speaks MQTT
3.1.1, or a gateway that subscribes to plain topics, cannot take jobs from
a queue, so its commands are not a queue. Choose from what each type
gives a reader, in the table of design.md › SUBSCRIBE: a fresh subscriber
to `$broadcast` gets retained values and then live messages, not the
history, and a
durable session resumes from its stored position within `$broadcast`'s
retention. The home-automation example carries its device commands on
`$broadcast` topics, not replayed to a fresh subscriber
([examples/home-automation/README.md](../../../examples/home-automation/README.md)).
A command replayed from history can act late, or twice: put commands in an
`append` channel only if they want that, with a retention they give.

**Live data that a bridge forwards.** `$broadcast` stores every message,
and an outbound bridge rule is a durable consumer with its own position
([design.md › Bridges](../../../docs/design.md#bridges), Out). So data
that is live-only for local readers can still be caught up by the bridge
after the link drops, as far back as `$broadcast`'s retention reaches: size
that retention for the longest outage of the link (topic 5), not for the
live readers.

For each channel, write its topic filter by the rules in config.md ›
`channels.<name>`, and its retention, measured against the longest absence from
topic 2. Where filters overlap, the topic goes to the one that spells it
out most exactly (design.md › Words, Channel). Set where a new reader
begins for the readers with no position of topic 2. A channel's name and type
are worth getting right now: changing the type later means
`saguin --drop-channel`, which deletes that channel's records
(design.md › Operations).

**A flow that lands in a channel that exists** - in their file, or one
made for an earlier flow - shares that channel's retention and limits
with every topic the channel claims: changing them for this flow changes
them for all of them. Say so, and offer the choice: keep the shared
settings, or give the flow a channel of its own with a filter that spells
its topics out more exactly (design.md › Words, Channel).

### 4. Access

Ask: who connects - devices, apps, people - and how each proves who it is
(a password, a client certificate); what each may do: which topics it may
publish to, read, replay, or take jobs from; and how their users and
client ids are named.

- Read what an absent password file means for anonymous clients
  ([config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt)), and say
  it plainly whenever they have not asked for passwords.
- The identity sources are in
  [config.md › A listener](../../../docs/config.md#a-listener), and what a
  verified certificate proves in
  [design.md › Access and reload](../../../docs/design.md#access-and-reload).
- **Certificates and passwords together.** Read the TLS row of config.md
  › A listener and design.md › Access and reload: how a listener asks for
  a certificate, whether it requires one, and what a client that presents
  none may do. Give them what the documents allow - separate listeners,
  or one listener on which a certificate is optional - as a choice, with
  what each means. Where the documents do not say, say so.
- An access file only narrows: without one, everything is allowed.
  config.md › `broker.mqtt` says what it needs beside it.

When they need one, write the access file from design.md › Access and
reload and [examples/acl.yaml](../../../examples/acl.yaml): rules in
roles, roles given to users by name pattern, and the user name or client
id substituted into a rule's topics so one role serves a whole fleet. The
verbs each channel type and `$broadcast` take, the features a role can
deny and the per-user limits are all in that section.

**Access by group.** A rule can follow a user only through the user's name,
its client id, or a topic level. When the group a person belongs to is not
a topic level - a dashboard account per person who may see only their
production line, phones that may command only lights and plugs - ask for
the names it needs: how users are named, and which topic levels carry the
line, the room or the kind of device. Then give each group a role, by a
name pattern or by listing the users (design.md › Access and reload says
how patterns compete). If no topic level tells a light from anything else,
say that no rule can tell them apart, and offer the choice: list those
devices' topics in a role, or change the topic layout.

**Never handle a password.** Name the password file's path; the operator
fills it with `saguin --passwd add <file> <user>`, which asks for the
password without echo (design.md › Operations).

### 5. Listeners, TLS, operations and bridges

Ask: where clients connect from (this machine, the local network, the
internet), whether any are browsers, whether anything should watch the
broker, and whether data goes to or comes from another broker.

- **Listeners** - plain, TLS, WebSocket for browsers, a Unix socket for
  local processes - and their TLS settings are in
  [config.md › A listener](../../../docs/config.md#a-listener), and what
  replaces the default listener is in
  [config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt). Use TLS
  wherever traffic crosses a network they do not control; which TLS
  version to ask for is in
  [README.md › Security and operations](../../../README.md#security-and-operations).
- **Operations** ([config.md › `broker.operations`](../../../docs/config.md#brokeroperations);
  [design.md › Operations](../../../docs/design.md#operations)): the
  health, metrics and read-only routes. Read which addresses count as
  loopback and what any other address needs. **A scraper gets a
  credential of its own, limited to the metrics**: a line of the
  operators' password file can carry a scope of routes, set with
  `saguin --passwd scope` ([design.md › HTTP](../../../docs/design.md#http)).
- **Bridges** ([config.md › `bridges.<name>`](../../../docs/config.md#bridgesname);
  [design.md › Bridges](../../../docs/design.md#bridges)): saguin connects
  to the peer as an MQTT client. Ask the peer's address; the user name the
  peer gave this broker, and that its password goes in a file the operator
  fills; which authority signed the peer's certificate, or whether the
  machine's own trusted roots cover it; what goes out, what comes in, which
  topics they take on the far side, and the QoS; and **the longest outage
  the link must survive**. That outage sets how long the peer keeps the
  bridge's session, and the retention of every local channel the bridge
  sends from, `$broadcast` included (topic 3). The peer's certificate is
  always verified; there is no switch to turn that off (config.md ›
  `bridges.<name>`).

### 6. Limits, only where the use case needs them

Do not walk [config.md › `broker.limits`](../../../docs/config.md#brokerlimits).
Read it when an answer calls for a limit: messages larger than the packet
limit (the memory budget must follow it, by the rule in that table), a
publisher to slow down (per connection there, per user in the access
file, as design.md › Access and reload says), far more or fewer
connections, or sessions kept shorter. Everything else stays at its
default.

## The result

**Ask where to write the files.** Resolve the folder with `realpath`; if
it is inside this checkout, say so and ask for another. Write the
configuration file there, and the access file where topic 4 needs one.
Paths inside the configuration are where the files will live on the
machine, which they gave in topic 1; put a short comment beside each
choice, as the files in `examples/starting-points/` do.

**Check it before saying anything is done:**

```sh
./bin/saguin --check-config <folder>/<configuration file>
```

When the access file at the path the configuration names is not the one
you just wrote - nothing is there yet, or an older file is - also run
the check on a check copy ("Checking a file"), so the access file you wrote is the
one checked. Fix every problem and run it again, until the only ones left
are files that do not exist on this machine - certificates, password
files - which saguin-ask's "Writing somebody a configuration" accepts.
Then show each flow landing where it should, with one sample topic per
channel, and what one user of each kind may do, **on the check copy**
whenever it was needed:

```sh
./bin/saguin --route <configuration or check copy> <a sample topic>
./bin/saguin --acl <configuration or check copy> <user> [client-id]
```

**Then explain it**, in the order of the topics:

- each choice: the setting, its value, the answer that decided it, and
  its reference - config.md and the section, design.md and the section;
- what is left at its default, named by group where a whole group stays,
  and why the default suits them;
- what they do next: fill the password files with `saguin --passwd`, put
  the certificates in place, start the broker
  (design.md › Operations). What reloads without a restart, and how, is
  in design.md › Access and reload; any other change needs a restart;
- every question the documents left open, worded as saguin-ask's "When
  the documents do not say" words it.

Hand over the path of each file written and the check's result, as it
printed it.
