---
name: saguin-config
description: "Work out the best saguin configuration for an operator's use case in a guided discovery session, starting from scratch or from their existing configuration file. Covers the machine and the loss it can bear, the clients, each data flow and its channel type, access control, listeners, TLS, operations and bridges. Writes the configuration and access files only to a folder the operator names, outside the repository, checks them with saguin --check-config, and explains every choice with a document reference. Use when someone wants a saguin configuration designed for them, wants to review or adapt theirs, or asks which channel type or storage mode suits their deployment."
---

# Find an operator's saguin configuration

Work out the operator's configuration with them in a conversation: ask about their deployment in plain language, one topic at a time, turn the answers into settings, and finish with files that `--check-config` accepts and a cited reason under every choice. The operator knows their devices and data, not saguin's settings: never ask them for a setting.

The user's instructions take precedence over this skill.

## Rules reused from saguin-ask

This is saguin-ask's "Draft configurations" workflow run as a conversation. Read `.agents/skills/saguin-ask/SKILL.md` before the first question and follow these sections of it, which are not repeated here:

- "Boundaries": no change to repository files, no `git` or `gh`, no claim sourced from memory, other brokers or Go source alone.
- "Documentation corpus" and "Retrieve evidence": what answers a question and how to read the design.
- "Cite answers": document and section for each claim.
- "When documentation is silent": say "The documentation does not say"; answer a deliberate non-goal with its citation.
- "Draft configurations": only what config.md lists, validation with `--check-config`, missing external files, never inventing a setting.

This skill names no setting, value or default. Each topic links the sections that own them: read those sections at that point of the session and take every setting, value and default from them. Links are relative to this file; from the checkout's root they are `docs/config.md`, `docs/design.md`, `docs/storage.md`, `README.md` and `examples/`.

Differences from saguin-ask: this skill writes its result, only to the folder the operator names, never inside this checkout. Check copies follow "Checking a file" below.

Read [config.md](../../../docs/config.md#sagüin-configuration) whole at the start; its opening section holds the rules every value follows.

## Checking a file

What `--check-config` and `--output` print is in [design.md › Operations](../../../docs/design.md#operations). Two points matter in a session:

- It prints nothing when it accepts a file: an empty result with exit status 0 is the success to report.
- `--output` prints no configuration when the check reports any problem, a missing certificate or password file included: only the problem lines and exit status 1. An operator's file names their machine's paths, so here it usually fails that way.

The way round is a check copy: a copy of the file in `.tmp/saguin-config/` in this checkout (ignored; never `/tmp`), whose file paths point at files present here: an empty placeholder in that folder for each password file, the demonstration certificates in `examples/support/certs/`, and the access file just written. Run the check, `--output`, `--route` and `--acl` on the copy; its paths are the check's, never the operator's. Where a placeholder is refused, take the defaults from config.md instead and say so.

## Start

**From an existing file** (passed as an argument or mentioned): read it, then run:

```sh
./bin/saguin --check-config <their file>
./bin/saguin --check-config <their file> --output
```

When the first command reports any problem, the second prints no configuration until it runs on a check copy ("Checking a file"). If `bin/saguin` is not built, offer `make build` and wait for approval, or use the operator's installed saguin.

Say back in plain words what the configuration does today: what a power cut costs in its storage mode, where clients connect and how they prove identity, each channel with its topics and what a reader gets, what lands on `$broadcast` and how long it stays, bridges, the operations endpoint, and limits moved from their defaults. Name every reported problem. Ask what they want changed, then run the discovery against the file:

- a topic the file settles and the change does not touch: confirm in one line what the file does there, and do not ask again;
- a topic the change touches: ask only the questions that change needs;
- a fact no file holds (the drive, the tolerable loss, the longest absence, a link's longest outage): ask where the change depends on it, since the file shows what was chosen, not why.

**From scratch:** ask what the broker is for, in a sentence, and what to call it: the broker's name has no default ([config.md › `broker`](../../../docs/config.md#broker) says what it may hold). Then begin with the machine.

## Discovery

- Ask one topic at a time and wait for the answer. Within a topic, ask its two or three questions together.
- Do not ask what the documents or an earlier answer settle. Where one answer decides another, state it: queue workers speak MQTT 5 ([design.md › `$saguin` topics](../../../docs/design.md#saguin-topics)); an access file needs every client to prove its identity ([config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt)); [design.md › Store](../../../docs/design.md#store) names an operating system one storage mode refuses.
- Keep a running list of every decided setting, its value, the answer behind it and its citation. Where the operator does not know, read what the default does, state it and use it.
- Never guess a number the operator did not give: a retention, timeout or size comes from their answer or stays at its default, and you say which.

### 1. Machine and tolerable loss

Ask the machine (board or server, OS, drive: SD card, SSD or disk), the room the data may take, and what a power cut may cost.

Read [design.md › Store](../../../docs/design.md#store): for each storage mode it says what a broker crash, an OS crash and a power cut lose, and what else the mode needs (a RAM-backed directory, an operating system, disk room; how much memory is in [storage.md › Choosing a machine](../../../docs/storage.md#choosing-a-machine)). Choose the mode that loses no more than the operator can bear and state, in the Store section's words, what it can still lose. Where two modes fit, give the cost of each and let them choose. For the drive, see [README.md › Bounded, inspectable storage](../../../README.md#bounded-inspectable-storage), [storage.md › Drive and filesystem](../../../docs/storage.md#drive-and-filesystem) and storage.md › Choosing a machine.

Then read [config.md › `broker.storage`](../../../docs/config.md#brokerstorage): settings that belong to one mode alone are refused in another; it also needs the database path (ask where it will live) and a default retention.

The default retention governs every log that omits its own: each `append` channel, each queue and its dead-letter channel, and `$broadcast`. On a `latest` channel it becomes the values' lifetime, and on a queue the age at which an unfinished job is dead-lettered ([config.md › `channels.<name>`](../../../docs/config.md#channelsname)). A default chosen for history also ends current values and jobs: say so, and give those channels their own wherever that is wrong for them.

Sizing: there is no limit on the whole store, only each log's own and the disk ([config.md › Dropped](../../../docs/config.md#dropped); [examples/home-automation/README.md](../../../examples/home-automation/README.md)). A default sized to the room given lets every log taking it fill that room, allowing that many times the room: share the room among the logs. The database file grows to its busiest size and never shrinks ([storage.md › Capacity](../../../docs/storage.md#capacity)). The documents do not give a record's stored size beside its payload: say so, do not convert disk size into records or days, size retention from the room given, and tell them to watch the file in its first days.

### 2. Clients

Ask how many connect at once, whether any speak MQTT 3.1.1, whether they keep sessions and the longest absence, which connect with a fresh session each time (phone app, dashboard, script), and whether one is a gateway publishing for many devices.

- Connection count and session lifetime are limits ([config.md › `broker.limits`](../../../docs/config.md#brokerlimits)); change one only when far from its default.
- MQTT 3.1.1 clients publish into and read every channel type; only a queue's worker subscription and shared subscriptions need MQTT 5 ([design.md › SUBSCRIBE](../../../docs/design.md#subscribe), its table; [README.md › Deliberate constraints](../../../README.md#deliberate-constraints), which also says what else they do not receive). The oldest version admitted is in [config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt).
- Away time is bounded twice. A client away longer than its session's expiry comes back as a new reader with no position, which starts where the channel's start point says, not where it left off ([design.md › Words](../../../docs/design.md#words), Session; design.md › SUBSCRIBE, UNSUBSCRIBE); the broker caps that expiry (config.md › `broker.limits`). Within it, each channel's retention bounds what comes back; retention shorter than the longest absence loses records, and the loss is counted (design.md › Words, Retention; [README.md › A position, not a copy](../../../README.md#a-position-not-a-copy)). Carry the longest absence into topic 3.
- Where a reader with no position begins (the oldest record kept, or only what comes next) is set per channel ([config.md › `channels.<name>`](../../../docs/config.md#channelsname)); what each type starts with is the table in design.md › SUBSCRIBE. It applies to every reader with no position: a fresh-session client every time, a durable session's first subscribe, a new outbound bridge rule ([design.md › Bridges](../../../docs/design.md#bridges), Out). A phone connecting fresh to a 90-day channel that starts at the oldest record replays 90 days each time. A durable session may seek before it first reads ([design.md › `$saguin` topics](../../../docs/design.md#saguin-topics)), so one channel can serve fresh dashboards and a durable archiver both: new readers start live, the archiver seeks back before its first read. Offer that as a choice and carry it into topic 3.
- Ask QoS only where an answer turns on it (a device that may not use QoS 2, a busy gateway), not of every client. QoS 2 is always offered and can be denied per role (topic 4). A busy QoS 1 gateway may need a larger incoming window once it runs: leave the default and point to [storage.md › Settings](../../../docs/storage.md#settings), which sizes it from what a running broker measures.

### 3. Data flows

Ask for each flow its topics, publishers, readers, and what a reader needs. Every flow needs its topics before it can become a channel: when the operator says a reader needs current state, ask which topics carry it before going on. When a flow's topics are not known, ask once more who could tell them; if still unknown, say the channel cannot be placed yet and leave that flow open in the result. Never write a default or guessed filter for it: it may claim the wrong topics, or none.

Match each flow to a type and explain why it fits, as [design.md › Channels](../../../docs/design.md#channels) and [README.md › Core concepts](../../../README.md#core-concepts) define the types. Each type's settings and defaults are in [config.md › `channels.<name>`](../../../docs/config.md#channelsname).

- History and replay (telemetry, events, audit): `append`, a log with delete retention, a stored position per durable consumer, and seek by offset or time. Ask how far back and how much. Retention removes the oldest records; the hard limit instead refuses publishes once full ([design.md › PUBLISH](../../../docs/design.md#publish)).
- Current state (device state, settings, setpoints): `latest`, one value per topic, sent on subscribe and readable by a point read. An empty payload deletes a value and leaves a tombstone; values and tombstones have separate lifetimes; the values' lifetime is the default retention unless the channel sets its own (topic 1), so ask how long a value may go unchanged before it should end, if ever.
- Work done once (jobs a worker takes, does and acknowledges): `queue`. Each job goes to one worker at a time; the worker reports it done or hands it back; failures retry, then go to the queue's dead-letter channel. A queue is not a shared subscription ([README.md › A real work queue](../../../README.md#a-real-work-queue)). Its lifetime follows the default retention unless it sets its own (topic 1). Ask how long one job takes, how many tries, and how the wait grows. The time a job takes sets the queue's hold: the hold starts at the worker's PUBACK, and a job still running when it ends spends that attempt and is offered again ([design.md › Delivery](../../../docs/design.md#delivery), Queue jobs; the hold's setting in config.md › `channels.<name>`), so the hold must outlast the longest job. "Retried three times" is four attempts: the queue counts attempts, the first included (Queue jobs; the retry settings in config.md › `channels.<name>`), so ask which they mean. Workers must be idempotent (README.md › A real work queue).
- Everything else: `$broadcast`, the reserved `append` channel that follows MQTT and stores every message whatever its QoS (design.md › Channels). Its retention and the retained values' limits are in [config.md › `broker.broadcast`, `broker.retained`, `broker.qos2`](../../../docs/config.md#brokerbroadcast-brokerretained-brokerqos2). A retain flag on a configured channel's topic is ignored (design.md › Channels), so a device relying on retained state belongs on `latest` or `$broadcast`.

Commands to devices that cannot be workers (MQTT 3.1.1 devices, gateways subscribing to plain topics) are not a queue. Choose from what each type gives a reader, in the table of design.md › SUBSCRIBE: a fresh subscriber to `$broadcast` gets retained values then live messages, not history; a durable session resumes from its stored position within `$broadcast`'s retention. The home-automation example carries device commands on `$broadcast` topics, not replayed to a fresh subscriber ([examples/home-automation/README.md](../../../examples/home-automation/README.md)). A replayed command can act late or twice: use an `append` channel for commands only if the operator wants that, with a retention they give.

Live data a bridge forwards: `$broadcast` stores every message and an outbound bridge rule is a durable consumer with its own position ([design.md › Bridges](../../../docs/design.md#bridges), Out), so the bridge catches up after a link drop as far back as `$broadcast`'s retention reaches. Size that retention for the link's longest outage (topic 5), not for live readers.

Write each channel's topic filter by config.md › `channels.<name>`, and its retention against the longest absence. Overlapping filters give a topic to the one that spells it out most exactly (design.md › Words, Channel). Set where a new reader begins for topic 2's readers with no position. Changing a channel's type later means `saguin --drop-channel`, which deletes its records (design.md › Operations).

A flow landing in a channel that exists (in their file, or made for an earlier flow) shares that channel's retention and limits with every topic it claims: changing them for this flow changes them for all. Say so and offer the choice: keep the shared settings, or give the flow its own channel with a filter that spells its topics out more exactly (design.md › Words, Channel).

### 4. Access

Ask who connects, how each proves identity (password, client certificate), what each may publish, read, replay or take as jobs, and how users and client ids are named.

- Read what an absent password file means for anonymous clients ([config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt)) and say it plainly whenever passwords were not requested.
- Identity sources are in [config.md › A listener](../../../docs/config.md#a-listener); what a verified certificate proves, in [design.md › Access and reload](../../../docs/design.md#access-and-reload).
- Certificates and passwords together: read the TLS row of config.md › A listener and design.md › Access and reload (how a listener asks for a certificate, whether it requires one, what a client presenting none may do). Present what they allow, separate listeners or one listener with an optional certificate, as a choice with what each means. Where they do not say, say so.
- An access file only narrows; without one everything is allowed. config.md › `broker.mqtt` says what it needs beside it.

Write the access file from design.md › Access and reload and [examples/acl.yaml](../../../examples/acl.yaml): rules in roles, roles given to users by name pattern, the user name or client id substituted into a rule's topics so one role serves a fleet. The verbs per channel type and `$broadcast`, the deniable features and the per-user limits are all in that section.

Access by group: a rule follows a user only through its name, its client id or a topic level. When the group is not a topic level (a dashboard account per person who may see only their production line; phones that may command only lights and plugs), ask for the names it needs: how users are named, and which topic levels carry the line, room or device kind. Give each group a role by name pattern or by listing users (design.md › Access and reload says how patterns compete). If no topic level tells a light from anything else, say no rule can tell them apart and offer the choice: list those devices' topics in a role, or change the topic layout.

Never handle a password. Name the password file's path; the operator fills it with `saguin --passwd add <file> <user>`, which prompts without echo (design.md › Operations).

### 5. Listeners, TLS, operations, bridges

Ask where clients connect from, whether any are browsers, whether anything watches the broker, and whether data goes to or comes from another broker.

- Listeners (plain, TLS, WebSocket for browsers, Unix socket for local processes) and their TLS settings: [config.md › A listener](../../../docs/config.md#a-listener); what replaces the default listener: [config.md › `broker.mqtt`](../../../docs/config.md#brokermqtt). Use TLS wherever traffic crosses a network the operator does not control; the TLS version advice is in [README.md › Security and operations](../../../README.md#security-and-operations).
- Operations ([config.md › `broker.operations`](../../../docs/config.md#brokeroperations); [design.md › Operations](../../../docs/design.md#operations)): read which addresses count as loopback and what any other address needs. Give a scraper its own credential limited to the metrics: an operators' password file line can carry a scope of routes, set with `saguin --passwd scope` ([design.md › HTTP](../../../docs/design.md#http)).
- Bridges ([config.md › `bridges.<name>`](../../../docs/config.md#bridgesname); [design.md › Bridges](../../../docs/design.md#bridges)): saguin is an MQTT client of the peer. Ask the peer's address; the user name the peer gave this broker, its password going in a file the operator fills; which authority signed the peer's certificate, or whether the machine's trusted roots cover it; what goes out and comes in, the far-side topics and the QoS; and the longest outage the link must survive. That outage sets how long the peer keeps the bridge's session and the retention of every local channel the bridge sends from, `$broadcast` included. The peer's certificate is always verified, with no switch to disable it (config.md › `bridges.<name>`).

### 6. Limits, only where needed

Do not walk [config.md › `broker.limits`](../../../docs/config.md#brokerlimits). Read it when an answer calls for a limit: messages above the packet limit (the memory budget must follow it, by that table's rule), a publisher to slow (per connection there, per user in the access file per design.md › Access and reload), far more or fewer connections, or shorter sessions. Leave everything else at its default.

## Result

1. Ask where to write the files. Resolve the folder with `realpath`; if it is inside this checkout, say so and ask for another. Write the configuration, and the access file where topic 4 needs one. Paths inside the configuration are where files will live on the machine (topic 1). Add a short comment beside each choice, as `examples/starting-points/` does.
2. Validate:

   ```sh
   ./bin/saguin --check-config <folder>/<configuration file>
   ```

   When the access file at the path the configuration names is not the one just written (nothing is there yet, or an older file is), also run the check on a check copy ("Checking a file"), so the written access file is the one checked. Fix and rerun until the only problems left are files absent on this machine (certificates, password files), as "Draft configurations" allows. Then show one sample topic per channel landing where intended, and what one user of each kind may do, on the check copy whenever it was needed:

   ```sh
   ./bin/saguin --route <configuration or check copy> <a sample topic>
   ./bin/saguin --acl <configuration or check copy> <user> [client-id]
   ```

3. Explain, in topic order:
   - each choice: setting, value, the answer that decided it, and its reference (document and section);
   - what stays at its default, by group where a whole group stays, and why the default suits them;
   - next steps: fill the password files with `saguin --passwd`, place certificates, start the broker (design.md › Operations). What reloads without a restart, and how, is in design.md › Access and reload; any other change needs a restart;
   - every question the documents leave open, as "When documentation is silent" words it.

Return the path of each file written and the check's result as printed.
