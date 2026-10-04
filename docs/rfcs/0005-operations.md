# RFC 0005 - Operations

**Status:** Draft
**Authoritative on:** what Sagüin exposes to the people running it rather
than to the clients using it - the operations listener, the health
endpoint, the metrics, and the interfaces it does not serve - neither a
`$SYS` tree nor anything that returns a record.

**A metric name is an interface that cannot be withdrawn.** Once
somebody's dashboard reads `saguin_queue_depth`, renaming it breaks their
alerting silently - the panel goes blank and nothing anywhere says why. So
the catalogue is decided in full before any of it is published, which is
the same argument the health endpoint's body already makes: adding a field
later is easy, taking one back after monitoring has started parsing it is
not.

The division with the other documents. A client's guarantees are RFC 0003,
and where this could be read as promising a client anything, RFC 0003 is
right. The configuration file's shape is RFC 0002; the keys named here are
part of that file and follow its rules, and this document says what they
do rather than restating how the file is parsed.

## The operations listener

One HTTP listener carries everything Sagüin says to an operator, and the
table below is every path it answers.

```yaml
broker:
  operations:
    listen:
      tcp:
        address: 127.0.0.1:9090
      unix:
        path: /run/saguin/operations.sock
        mode: "0660"
    min_scrape_interval: 60s
```

**`listen` takes either entry or both**, spelled as `broker.mqtt.listen`
spells them, so an operator learns one schema rather than two. There is no
`ws`: WebSocket exists on the MQTT listener because a browser cannot open a
raw MQTT socket, and a scraper speaks HTTP already. A block present with
neither entry is a startup error - absent is how an operator says no
listener at all.

**Each of `tcp` and `unix` is the same list-or-map shape a kind of
`broker.mqtt.listen` takes (RFC 0002 "Several listeners of a kind").** A
kind with one door is the mapping above; a kind with two or more is a
list, each entry named:

```yaml
broker:
  operations:
    listen:
      tcp:
        - name: local
          address: 127.0.0.1:9090
        - name: fleet
          address: 0.0.0.0:9443
          tls:
            cert_file: /etc/saguin/tls/ops.pem
            key_file: /etc/saguin/tls/ops-key.pem
            client_ca_file: /etc/saguin/tls/operators-ca.pem
```

Each door keeps every rule below on its own: `password_file`, TLS with
`client_ca_file` authenticating the port, `proxy_protocol` on a Unix
door, and the loopback-needs-a-credential rule, asked of that door's own
answer. A door's name is what a certificate error and the startup line name it
by - its own id space, separate from the MQTT listener's: it names nothing
an MQTT door does (not `SetListenerCredentials`, not a route an MQTT client
reaches), only this listener's own credential lookup, so a tcp door named
`fleet` beside an MQTT tcp door also named `fleet` is two unrelated things
sharing a word, not a collision. `--check-config` refuses, within the
operations doors alone, a second door of a kind with no name and two doors
sharing one name - the same rule RFC 0002 states for the MQTT doors - and
refuses a port or a Unix path two operations doors, or an operations door
and an MQTT door, would both try to bind (RFC 0002 "Several listeners of a
kind" has the exact rule: equal hosts or a wildcard, port 0 excepted).
`SIGUSR1` re-reads every operations door's own certificate and password
file, the same signal that already re-read the block's shared one.

Both answer the same paths with the same handler, **and a credential
each**: `broker.operations.password_file` applies to whichever door a
reader arrives on, and either listener may name a `password_file` of its
own, which wins for that door alone. Which door a request arrived at is
read off the connection, never from anything the caller sent. The doors
differ in who can reach Sagüin at all - they are a gate in front of the
credential rather than one instead of it - and RFC 0002 "The operations
listener" has the keys, the loopback rule and the socket's lock.

| Path | Authentication | What it is |
|---|---|---|
| `GET`, `HEAD` `/health` | none, ever | Would restarting this process help? |
| `GET`, `HEAD` `/metrics` | a password, a client certificate or a proxy's word (*Who Sagüin thinks you are*), once `password_file` names anybody or a certificate is required | The metrics catalogue, in Prometheus text format |
| `GET` `/v1/operations/acl` | the same | What one user may do, and which pattern decided it |
| `GET` `/v1/operations/config` | the same | The configuration this process resolved, as JSON |
| `GET` `/v1/operations/consumers` | the same | Which consumers are behind, and by how much |
| `GET` `/v1/operations/queues/<channel>` | the same | What that queue is holding, and how many attempts each has had |
| `GET` `/v1/operations/position-lost` | the same | Which readers the retention floor passed, and how many records each lost |
| `GET` `/v1/operations/refused` | the same | Which clients this broker refused, at CONNECT or by ending the connection, and why |
| `GET` `/v1/operations/sessions` | the same | Who is connected, and who is holding a session with nothing behind it |
| `GET` `/v1/operations/users` | the same | Which client names may connect, and where anonymous is allowed |
| anything else | - | `404`, and a method a path does not take is `405` |

#### Which routes a credential reaches

**One password file, and a user may be narrowed to some of the routes.** A
third colon-delimited field names them; a line without one reaches every
route.

```
prometheus:$7$1000$…:/metrics
alice:$7$1000$…:/metrics,/v1/operations
oncall:$7$1000$…:/v1/operations
```

**The colon is what makes that parseable.** A user name may not contain one
and a stored hash is `$`-prefixed base64, whose alphabet has none, so the
second colon ends the hash and no realistic value is misread. A delimiter
inside the name would not survive `alice@example.com`, which this file
accepts.

**The routes are paths rather than names for groups of them**, so there is
no mapping in the code that an operator cannot read. The price, said rather
than discovered: if a path ever changes, the files change with it.

**Matched at a segment boundary.** `/v1/operations` grants
`/v1/operations/consumers` and does not grant `/v1/operationsomething` - a
route nobody wrote, reached by a name that merely starts the same way.

**A scope naming no route Sagüin serves is a startup error**, and
`--check-config` reports it: it denies that user everything while reading,
from the file, as though it granted something.

**A third field on an MQTT password file is a startup error too.** An MQTT
client reaches topics rather than paths, its permissions are the
`acl_file`'s, and a scope there would govern nothing. It is also what keeps
that file Mosquitto's format hash for hash, which RFC 0002 promises anybody
migrating.

**A credential that is good but does not reach a route is answered `403`,
not `401`.** A `401` says "try again with a credential", so a scraper
retries and records an authentication failure - sending an operator to check
a password that is correct, when the answer is that this credential does not
reach here.

`saguin --passwd scope <file> <user> <routes|all>` writes the field. It sets
the whole list rather than adding to it: a user with no routes named reaches
every one, so an "add" against such a user would have to narrow it, which is
an add that removes.

**A `/metrics` with no credential is a loopback-only `/metrics`** - the
rule below refuses to start on a routable address until something there
authenticates. **The credential applies to every transport the listener
answers on**, the Unix socket included: its permissions are a second gate
rather than an alternative, so a local agent reading through the socket
needs the credential from the day one is configured.

**Two contracts Sagüin conforms to, and one it owns.**

`/health` answers an orchestrator's probe and `/metrics` answers a
scraper. Neither shape is Sagüin's to choose - one is a liveness
convention, the other Prometheus's exposition format - so neither carries
a version and neither ever changes. `/v1/…` is Sagüin's own, will grow,
and will one day change, which is why it says which version it is: a
`/v2` can stand beside a `/v1` while people move.

**No route writes anything, no user interface, and no payloads - ever.
And no secret: not a password, not a hash, not the contents of a key
file.** That is the rule, and it is bounded on what comes out rather than
on how coarse it is. The records themselves come out the way every other
client reads them: through MQTT, held to the same permissions as everybody
else.

**No secret can reach these routes, and that is the schema's doing
rather than a filter's.** Nothing in the configuration holds a
credential - every one is a path - so there is no redaction pass to keep
in step with the schema, and a key that carried a secret would be a
change to what Sagüin stores, refused on those grounds. What holds it
there is a test that reads every file the response names and requires
that none of its contents came back. The promise is bounded on what
comes out: nothing that leaves this listener is a credential, and
nothing that goes into it changes anything.

**The `/v1` routes are not metrics and must not be scraped.** They answer
with a list rather than a number, on demand rather than on a timer, and
they carry exactly what the catalogue is closed against - a client id is a
string a client chose, so a series keyed by one is a series count chosen by
whoever connects. A scraper pointed at them writes an unbounded number of
series into somebody's monitoring system, which then falls over, some
distance from Sagüin and long after the cause.

**Which is why they exist as a route rather than as metrics.** "Which of my
three hundred devices is behind" and "what is actually stuck in that queue"
are the two questions an operator arrives with, and the catalogue can answer
neither: `saguin_channel_consumer_position_min` is one number per channel,
so one straggler reads exactly like a fleet that has stopped, and
`saguin_queue_depth` says four hundred while nothing says what. Both answers
are in the store already. The only other way to see a queue's contents is to
consume them, which takes work from the workers the viewer was sent to
diagnose.

**Every row is capped, and the cap is a hundred.** Invariant 13 is about
everything that accumulates and a response body accumulates: without a cap,
a route that lists positions can be asked to read a fleet of ten thousand
into memory and out over a socket. **Worst-first ordering is what makes the
cap safe** - the rows that fall off the end are the ones nobody was looking
for - and every response carries how many exist beside how many it returned,
because a caller shown a hundred of three hundred and not told so believes
it has seen the fleet.

**A queue that is not configured answers 404 and does not name the ones that
are.** These routes are behind a credential, but a 404 that lists the
channels is a route that returns the configuration.

**JSON, which is the first thing on this listener that is not
`text/plain`.** `/metrics` is Prometheus text because a scraper reads it;
these are read by a person and by `jq`.

**`/health` needs no credential, and that is the point of it** - a
liveness probe that authenticates is a probe that restarts a healthy
broker over a secret; "The health endpoint" below has the whole of it.

**No block means no listener.** A `broker.operations` that is absent opens
nothing, exactly as a `listen` with no `unix` block opens no socket. The
address is bound at startup with the MQTT listeners, so a port already in
use is a startup error naming it rather than a surprise later.

**The block is named for what every path on it has in common**, which is
that each is a read-only fact for the person running the broker rather
than anything a client can use - and it is this document's own title, so
the file and the specification name the same thing the same way.

## Authentication and TLS

What is written below is the shape of each, and the one rule that stops
authentication being deferred for ever.

**Authentication is required on `/metrics`**, as HTTP Basic in the request
header. An operator reaches Sagüin from somewhere else on the network, the
same way an MQTT client does, so this cannot be a localhost-only interface
with the question deferred. Basic is what every scraper already sends and
what every proxy already understands; it carries the credential in clear,
which is what the paragraph on TLS below is about.

The operators are `broker.operations.password_file` - or a listener's
own, which wins for that door alone - in Mosquitto's format, managed with
`saguin --passwd`, read at startup and re-read on `SIGUSR1` (RFC 0002
"Who may connect"). A file naming no users is a startup error rather than
a listener nobody can read.

**The credentials are not the MQTT credentials.** An operator is not a
device. One credential set granting both topic access and broker
statistics means every device that can publish can also enumerate the
channels, their volumes, their consumers' positions and the address of
every broker this one dials - and a fleet's credentials are on the fleet,
where they are readable by anyone holding one device.

**TLS is optional**, and configured as `broker.operations.listen.tcp.tls`
with a certificate and its key (RFC 0002 "TLS on a listener"). On an edge
box the operations port is commonly reached across a management network or
an SSH tunnel that already carries the transport security, and requiring a
certificate there produces a self-signed one that nobody rotates and every
client is told to ignore - which is worse than plain HTTP on a trusted
link, because it looks secured. Where the port crosses a network the
operator does not own, TLS. The Unix socket takes none: it does not leave
the machine.

### Who Sagüin thinks you are

**Five ways to be named, and the last two are believed only at a door the
operator chose who may open.**

| Named by | Where it counts |
|---|---|
| A password this broker checked | anywhere |
| A client certificate an authority named in `tls.client_ca_file` signed | anywhere |
| A certificate that carries no name at all | anywhere, and it is *not* nobody - see below |
| `X-Saguin-Principal`, set by a reverse proxy that did the checking | a Unix socket, and nowhere else |
| PROXY protocol v2, carrying the Common Name the proxy verified | a Unix socket with `proxy_protocol: true` |

The header is whatever the caller typed. On a routable address anybody who
can reach the port can write it, so it is not read there at all rather than
read and weighed - a bypass that needs a rule to be safe is a bypass.

**A Unix socket and a loopback port are not equally strong doors, and only
the socket carries a name.** A socket has file permissions: the operator
decides which user or group may speak to it. A loopback port has none -
unreachable from the network is not the same as reachable only by whoever
the operator chose, because *every* process on the machine may connect. A
header there would let any local process name itself any operator and read
which devices are behind, without the password the operators file exists to
check, so it is not read there.

**This is the rule the MQTT listener's `proxy_protocol` already
states** (RFC 0002): a socket's file permissions decide who may assert a
name, and a TCP listener would need a trusted-proxy allowlist first.

So a reverse proxy that names its caller reaches Sagüin through the socket:
`proxy_pass http://unix:/run/saguin/operations.sock:/v1/` for the header -
the trailing `/v1/` matters, because a `proxy_pass` carrying a URI
replaces the matched location prefix and a bare `/` would strip it - or
the stream module with `proxy_protocol v2` for the Common Name. A loopback
port is still the right door for a scraper with a password, or for a local
reader with none - it just cannot carry somebody else's word for who is
asking.

**A name with no entry in the password file reaches `/metrics` and nothing
else.** Being scraped needs no entry, which is what a certificate or a
proxy is almost always arranged for; reading which device is behind names
devices, and that is a thing an operator says out loud by putting the name
in the file. Widening it is `saguin --passwd scope`, the same answer as for
everybody else.

**A name is the Common Name, or the first DNS name when there is none.**
CN is where an operator's own authority usually puts it and what an
acl_file is written against, but it has been deprecated as an identifier
for years and many authorities now issue a subject alternative name and
nothing else. **A name is taken exactly as the certificate, the proxy or
the header states it**, spaces and all, as the MQTT listener takes a
certificate's name: one certificate is one identity at either door. A
header has no edge spaces to keep: HTTP does not count them as part of its
value. **A certificate this broker verified and cannot name is still not
nobody**: it came through a door that admits nobody without one, so it
gets what a named stranger gets - `/metrics` and no more. **A name holding
U+0000 or a control character is nobody's**, from any of the four sources,
and the request is answered `403` with a line in the log, as the MQTT
listener refuses the same name (RFC 0002 "TLS on a listener").

**A credential that was offered and failed ends there.** A wrong password
is a refusal, not a fall-through to whatever else might name the caller -
otherwise a stale password beside a proxy's header answers 200, which is a
rejected credential that was not rejected.

**Nobody is not the same as somebody unscoped.** A request that carries no
name at all - no password, no certificate, no proxy - is trusted by the
door it arrived at, which validation has already made a loopback address or
a socket. That is the local reader with no credential to give, and it
reaches everything.

**And that local reader answers to an address, not to a name.** Unreachable
from the network is not unreachable from the internet. A page on another
site whose domain re-resolves to `127.0.0.1` is *same-origin* to this
listener and reads it through whatever browser has it open - the catalogue,
the sessions, the ACL, the user list, the resolved configuration. Every
route here is a `GET`, so nothing can be changed that way; all of it can be
read, and the operator's own browser is the one asking. The one thing that
page cannot forge is the `Host` header, because a browser sends the name it
was loaded by. So a request carrying no name at all is served when it
addressed this broker - an IP literal, or `localhost` - and refused with
`403` naming the setting that lifts it when it asked for this broker by a
name.

**Only there.** A listener with a password file, a client certificate or a
proxy that names its caller never reaches that branch, so a scraper or a
viewer calling a real deployment by its hostname is untouched - and by the
rule above, anything off this machine has one of those. A Unix socket is
exempt: no browser can open one, and whatever proxy is in front of it sends
the name it was asked for. `/health` is not behind it either, being never
authenticated and carrying nothing about this broker's contents, precisely
so that a probe which cannot hold a credential can read it from wherever it
runs.

**A required client certificate takes `/health` with it, and that is the
one thing to arrange around.** A liveness probe which authenticates is a
probe that fails when the credential is wrong, expired or not yet mounted -
so a broker in perfect health gets restarted. Mutual TLS on the only TCP
listener produces exactly that: the probe has no certificate, the handshake
ends, and the probe sees a connection error rather than a 200. Give the
probe its own door - the Unix socket, or a second loopback listener - rather
than a certificate.

**And a required client certificate lifts the loopback rule**, for the
reason the socket already lifts it: nothing without a certificate this
broker's authority signed completes the handshake, so the port is not
reachable by whoever can route to it. `require_certificate: false` does not
lift it - that is the mixed mode where a client presenting nothing still
connects.

**Behind a reverse proxy**, where a deployment already terminates TLS
centrally, the broker's own listeners stay on a socket and loopback and
the proxy carries the credential and the certificate. What follows is
the whole working pair. The stream half is the MQTT listener's
arrangement, specified in RFC 0002 under "Behind a proxy that terminated
TLS", and is shown here so the pair is one file nginx can load:

```nginx
# Raw MQTT. proxy_protocol v2 is what puts the client's address and its
# certificate's Common Name back in the broker's hands - `proxy_protocol
# on` is v1, carries no TLVs, and saguin refuses it by name.
stream {
    server {
        listen 8883 ssl;
        ssl_certificate        /etc/saguin/tls/cert.pem;
        ssl_certificate_key    /etc/saguin/tls/key.pem;
        ssl_client_certificate /etc/saguin/tls/clients-ca.pem;
        ssl_verify_client      on;

        proxy_pass unix:/run/saguin/saguin.sock;
        proxy_protocol v2;

        # Above the largest keepalive in the fleet. See below.
        proxy_timeout 20m;
    }
}

http {
    # The CN of the certificate nginx verified. Stock nginx has no
    # variable for it - $ssl_client_s_dn is the whole subject.
    map $ssl_client_s_dn $client_cn {
        default                 "";
        "~(^|,)CN=(?<cn>[^,]+)" $cn;
    }

    # MQTT over WebSocket, for browsers.
    server {
        listen 8443 ssl;
        ssl_certificate     /etc/saguin/tls/cert.pem;
        ssl_certificate_key /etc/saguin/tls/key.pem;
        location /mqtt {
            proxy_pass http://127.0.0.1:8083;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_read_timeout 3600s;
        }
    }

    # /metrics behind a credential. /health is deliberately not here:
    # a liveness probe that goes through the proxy reports on the proxy.
    server {
        listen 9443 ssl;
        ssl_certificate     /etc/saguin/tls/cert.pem;
        ssl_certificate_key /etc/saguin/tls/key.pem;

        # Without these, nginx asks for no certificate, $client_cn is
        # empty and proxy_set_header sends nothing - so nothing names the
        # caller and saguin answers 401 rather than serving a stranger.
        # `optional` rather than `on`, so /metrics below still works for a
        # scraper that has a password and no certificate.
        ssl_client_certificate /etc/saguin/tls/operators-ca.pem;
        ssl_verify_client      optional;
        location /metrics {
            auth_basic           "saguin";
            auth_basic_user_file /etc/saguin/metrics.htpasswd;
            proxy_pass http://127.0.0.1:9090;
        }

        # Or let the client certificate be the credential and tell saguin
        # whose it was. `$ssl_client_s_dn_cn` does not exist in stock
        # nginx; the subject DN does, and a map takes the CN out of it.
        # Through the socket, because that is the only door that carries a
        # name: its file permissions decide who may claim one.
        #
        # The `:/v1/` on the end is load-bearing. A proxy_pass carrying a
        # URI replaces the matched prefix, so `:/` would send
        # /v1/operations/consumers on as /operations/consumers and saguin
        # would answer 404 to every request through here.
        location /v1/ {
            proxy_pass       http://unix:/run/saguin/operations.sock:/v1/;
            proxy_set_header X-Saguin-Principal $client_cn;
        }
    }
}
```

**What each door answers**: `/metrics` with a password and no
certificate, 200; with a wrong one, 401. `/v1` with a certificate the
authority signed, 200 when that name has an entry in Sagüin's operators
file and 403 when it does not. `/v1` with nothing at all, **401** -
nginx sends no name, so Sagüin has none and refuses. And a name a
client sends itself never survives, because `proxy_set_header` replaces
it: a caller presenting no certificate and writing
`X-Saguin-Principal: operator` is answered 401.

That 401 row is the one to check after any change to this block: a
block without `ssl_client_certificate` is syntactically perfect and
open, and `nginx -t` cannot see that - `make nginx`, which drives this
block against a real nginx and asserts every row above, can.

The broker beneath it:

```yaml
broker:
  mqtt:
    listen:
      unix:
        path: /run/saguin/saguin.sock
        mode: "0660"
        proxy_protocol: true
      ws:
        address: 127.0.0.1:8083
  operations:
    listen:
      tcp:
        address: 127.0.0.1:9090        # /metrics, behind nginx's auth_basic
      unix:
        path: /run/saguin/operations.sock   # /v1, where a name can be carried
        mode: "0660"                        # nginx's worker needs this group
    password_file: /etc/saguin/operations.passwd    # and this is not optional
```

**`/health` is deliberately not proxied.** A liveness probe routed through
the proxy reports on the proxy: it answers while the broker is gone, and
stops answering when the proxy restarts under a broker that is fine. It is
reached on loopback, which is also why it carries no credential - the
paragraph on the two endpoints above says why that asymmetry is the rule
rather than an oversight.

**That `password_file` is what makes the arrangement fail closed, and
leaving it out is not a smaller version of it.** The proxy's credential
is on top of Sagüin's rather than instead: without the file, Sagüin has
nobody to refuse - a request carrying no name is read as a local reader
with no credential to give, which reaches every route. With `optional`
verification above, a caller presenting no certificate is exactly that:
nginx sends an empty header, and the door opens. Without the file,
`/v1` with no certificate answers **200**; with it, **401**.

**Note the two files.** `auth_basic_user_file` is nginx's own, in
htpasswd format - not Sagüin's operators file, which is Mosquitto's
PBKDF2 format and which nginx answers 500 on. The scraper goes in both,
because nginx passes the `Authorization` header through: a scraper
nginx authenticated and Sagüin does not know is answered 401 by Sagüin.
If one gate is enough for a deployment, it is Sagüin's: drop
`auth_basic` and let the broker answer.

**The rule that makes "authentication later" impossible: Sagüin refuses to
start when the operations listener's TCP address is not a loopback address
and no `password_file` governs it - neither its own nor the block's -
naming both.** The failure it prevents is the ordinary one, not a careless
one. The port starts on localhost with no credential configured, which is
correct; then a scraper arrives on another box, somebody changes the
address to `0.0.0.0:9090` because that is what makes it reachable, and the
broker begins serving channel names, message volumes, consumer positions
and its bridges' peer addresses to anyone who can route to it - with
nothing anywhere saying that a decision was made. A startup error is where
that gets caught, because it is the one moment the operator is looking.

A rejected credential is `401` with a `WWW-Authenticate` header, and the
log line says a credential was rejected and does not contain it.

## The health endpoint

`GET /health` answers `200` and `{"status":"ok"}`; `HEAD` gets the same
status and no body; another method on it is `405`.

**It answers one question: would restarting this process help?** That is
the only question a liveness probe is entitled to ask, and it is why the
endpoint deliberately reports nothing about the world outside the process.

An inbound bridge whose far end is down is a real thing an operator wants
to know and a terrible thing to fail a probe on: the broker would be
restarted, repeatedly, because somebody else's broker is unreachable - and
the restart cannot fix it. Storage errors, bridge state, and how far behind
each bridge is are all worth reporting and none of them belongs here. They
belong to the metrics below.

**The metric is the state and the log is the narrative**:
`saguin_bridge_connected` is where an operator reads whether a link is up
now, whatever their log level, and a line that retracts another is
written at the level of the line it retracts (RFC 0002 "How much the
broker says").

What is left is not nothing. Sagüin has one broker-wide mutex on the
publish path, and a broker that cannot take it is one whose publishes have
stopped - which is the failure a restart does fix. So the handler takes
that lock, with a timeout, and `503` and `{"status":"unavailable"}` means
it could not.

**No path in Sagüin holds that lock across disk I/O**, which is what makes
the answer unambiguous. A snapshot builds its state under the lock and
releases it before writing the file; the retention sweep copies the store
maps under the lock so that no removal is done holding it. A hold long
enough to fail this probe is therefore not slow hardware and not a large
channel - it is a defect. **That is also a constraint on everything else on
this listener**: a scrape that formatted its output while holding the lock
would make this endpoint flap on the same port, and the flapping would be
telling the truth. The metrics section repeats the rule where it bites.

**The timeout is a constant rather than a key.** An operator has no real
reason to change it. A publish through the wire costs tens of microseconds
("What it costs, measured" below), so the lock is held for less than that
at a time, and any threshold separating "busy" from "wedged" is four orders
of magnitude away from both. A knob whose only settings are the default
and wrong is not a knob.

**There is no `/ready`.** Snapshots are loaded before any listener binds,
so there is no moment at which Sagüin is running but not ready to serve,
and a second endpoint that always answers what the first one did is that
same non-knob in another shape. If bridges ever have to be connected
before the broker serves, that stops being true, and this is the paragraph
that would change.

**The body carries nothing else, and that is a decision rather than an
omission.** It is the one path on this listener with no authentication, so
whatever it returns is readable by anyone who can reach the port: a
version, a channel name, a record count, all of it. Adding a field later
is easy. Taking one back after somebody's monitoring has started parsing
it is not.

## Metrics

`GET /metrics` answers the catalogue below in the Prometheus text
exposition format, which every scraper in common use reads and with which
OpenMetrics is compatible.

**Pulled, never pushed, and that is the resource decision.** Sagüin runs
on a gateway, a Pi, or a vehicle, and its job is MQTT. A broker that
computes its own statistics on a timer pays for them every interval for
ever, whether or not a single person is looking; a broker that answers
when asked pays only when somebody asks, and the cadence is set in the
scraper the operator already runs. A `$SYS` tree is the timer version of
the same idea, and the section below says why Sagüin has none.

### What a metric is allowed to cost

**Where the substrate already counts something, that counter is the one
read.** It maintains connections, subscriptions, messages, packets and
bytes as atomics on the paths that change them - the numbers its own `$SYS`
tree publishes, though Sagüin silences that tree. Silencing stops the
publishing and not the counting, so reading them here costs an atomic load
and duplicates nothing. Keeping a second tally beside one of those would be
two numbers to keep in step and one of them going quietly wrong, which is
the reasoning that refused the `$SYS` tree in the first place.

**A metric is a number the broker already holds, or it does not ship.**
This is the rule the catalogue was built against rather than a note about
the implementation, because the failure it prevents is one nobody
attributes correctly: a monitoring tool, added to find out why the broker
is slow, is what is making it slow.

Three costs, measured against the stores as they are:

**Free - the number is a field.** The offset a channel will assign next,
the retention floor, the bytes an `append` or `queue` channel holds, the
count of records out with queue workers, and every counter. Both storage
providers keep these in memory and advance them from committed
transactions.

**Free because the store was asked to keep it, which is not the same as
free to ask for.** How many topics a `latest` channel holds a current value
for is the size of a map the `memory` store is already keeping. The
`sqlite` store counts its rows once, when the channel opens, and advances
that number from committed transactions - the arrangement a queue's depth
already has, and for the same reason: counting rows on a scrape is the one
cost this document refuses outright.

**What makes it free on the write path is the order of the write.** An
upsert reports success whichever branch it took, so a store using one
cannot tell a new topic from a replacement without asking again. Updating
first and inserting only where nothing matched gives that answer as a side
effect, and costs nothing over an upsert - it goes straight to the row
rather than attempting an insert and hitting the conflict:
`BenchmarkLatestSet`'s sqlite row is 18.6us. A topic seen for the first
time costs 27.5us, once in that topic's life - ten thousand devices pay
27ms between them, ever.

**A `latest` channel's bytes are free on neither provider**, for a reason
the count does not share. A write replaces a value, so the change in size
is a difference and measuring it means reading the outgoing value first.
The count escapes that because a replacement does not change it: only a
topic arriving or leaving does, and both are already known where they
happen. The catalogue says where the bytes are absent.

**Proportional to the number of consumers - and this one is paid.** The
worst lag on a channel means the lowest stored position on it, and there is
one position per durable consumer, so a fleet of ten thousand devices
reading one channel is ten thousand positions to take a minimum over, per
channel, per scrape. The memory store walks its map; the sqlite store asks
its table for `min("offset")`.

`BenchmarkLowestPosition`, at `-benchtime 2s -count 2`, one channel:

| consumers on one channel | memory - Ryzen 7 260 | sqlite - Ryzen 7 260 |
|---|---|---|
| 100 | 531ns | 25.6us |
| 1,000 | 5.7us | 174us |
| 10,000 | 59.6us | 1.69ms |

**Those are the cost of the minimum and the count together.** They are one
pass: the memory store reads the size of the map it is already walking, and
the sqlite store asks for `min()` and `count()` in one statement over the
same rows, where a second method, asking separately, would double it.

**The minimum is computed on every scrape rather than cached**, because a
cached minimum that only falls never rises when consumers catch up, and
`floor > position_min` - the one alert below - would then fire on a broker
where every consumer had caught up and nothing had been deleted unread. So
the reading above is the price of the alert being usable: **1.69ms, once
per channel, per scrape**, at a fleet size on the large side of what a
single-node broker is for, against a scrape interval with a configured
floor (`min_scrape_interval`).

**Proportional to the number of records - never.** Counting the rows of a
history is a table scan, and a scraper doing it every fifteen seconds
across every channel is a load source wearing a monitoring tool's clothes.
Two consequences, and both are why the catalogue looks the way it does:

- **A channel's record count is `next − floor`**, which is two of the free
  numbers above. Retention only ever removes a prefix and advances the
  floor past it, so no record is ever missing from the middle of a log and
  the subtraction is exact.
- **A queue's depth is not derivable**, because resolution removes a
  record from the middle rather than the front. It is the one number in
  the catalogue that a store does not already hold, so the store keeps a
  count in memory the way it already keeps the byte total, and the scrape
  reads that instead of counting rows.

**And the scrape does not hold the publish path.** The numbers are copied
under the broker's lock and formatted outside it, which is the discipline
the snapshot and the retention sweep already follow. A scrape that
formatted under the lock would stall publishes and fail the health probe
sitting on the same listener.

### What it costs, measured

One scrape, at four configuration sizes. A tenth of the channels are queues
and a tenth are `latest`, which is the shape of a real file rather than a
thousand of one kind - the queue series are the widest part of the
catalogue, so measuring over append channels alone would understate it.
`BenchmarkScrape` takes it, every channel holding one record, at
`-benchtime 2s -count 2` on an AMD Ryzen 7 260 (8 cores, 16 threads):

| Channels | Response | Gzipped | CPU, plain | CPU, gzipped |
|---|---|---|---|---|
| 10 | 12.3 KiB | 2.9 KiB | 33 µs | 291 µs |
| 100 | 55.2 KiB | 6.0 KiB | 131 µs | 736 µs |
| 1,000 | 493 KiB | 34 KiB | 1.65 ms | 4.06 ms |
| 10,000 | 4.85 MiB | 291 KiB | 15.4–16.5 ms | 32–33 ms |

**Each series per channel adds to both columns** - roughly 5–13% to the
body and 12–35% to the CPU per row of the catalogue - which is the
standing cost of adding one, worth knowing before the next is.

A publish through the wire, one publisher waiting for each acknowledgement
(`BenchmarkPublishThroughMQTT`), costs 30–32µs on a memory channel and
**92–106µs** on a sqlite one - so a hundred-channel scrape is about four
memory publishes, once per scrape interval, and gzip is most of the cost of
a scrape that asks for it.

**Two conditions on the sqlite figure.** `TMPDIR` must be on a real disk:
`/tmp` is a tmpfs on most machines, so an unset one measures the page cache
and returns about 78µs, which is not a number any deployment sees. And the
row varies by about 15% between runs on one machine and one disk, so take
several counts and quote the range, not the run. The memory row does not do
this: it repeats to the microsecond.

**Both the response and the CPU grow with the channel count**, which is
stated here so that an operator can predict it from the number of channels
they configured rather than discover it. RFC 0004 measures the other two
costs that grow the same way: the ten thousand snapshot files at a
graceful shutdown, and the retention sweep. The sweep is two different
costs - its size half reads nothing and is the same at ten thousand
channels as at ten, and its age half reads once per channel on a `sqlite`
provider, on an interval the shortest configured retention decides. So the
figure to plan against is this scrape and that age sweep, and which of
them dominates depends on how often each is asked for.

Gzip is offered when the client asks for it, which Prometheus does by
default. It returns about a fifteenth of the bytes at a thousand channels,
which is the right trade on any link an operator would scrape across and
the wrong one on none.

### The observer does not set the cost

`min_scrape_interval` is the shortest interval at which the catalogue is
recomputed. A scrape arriving sooner is answered from the previous one.

**The cost of being observed is bounded by Sagüin rather than by the
observer.** Without it, the load is chosen by whoever configures the
scraper - and by *how many* of them, since five monitoring systems at one
second each is five times the bill, which is not a number this broker got
to agree to. With it, one interval is one computation however many people
ask and however fast.

**A scrape arriving early is answered, not refused.** Prometheus records a
refused scrape as a *failed* one: a gap in the graph and, usually, an
alert. A defence that turns into the operator's incident is not a defence,
so the early scrape gets the previous bytes and nobody is told off.

What it costs is that a sample can be up to one interval old while the
scraper stamps it with the time it asked.

**A minute is the floor and the default, and a shorter one is a startup
error.** Not raised quietly: an operator who writes `10s` and is silently
given a minute reads their graphs believing they have ten-second
resolution, and every conclusion they draw about the shape of a spike is
drawn at the wrong scale. Naming the floor costs one restart.

**A minute is what a broker's metrics are worth.** The numbers do not change
usefully faster than that, and reading them costs the broker rather than the
reader. Against a scraper set to Prometheus's own default of fifteen seconds
it is that bill cut fourfold, and the bill is measured above: 1.69ms per
channel per scrape at ten thousand consumers on the sqlite store.

**It bounds how late the alert below can be**, which is the figure to weigh
if it is ever raised: retention removes records on a scale of days, so a
minute is nowhere near it.

**Set the scrape interval at or above this one.** A scraper running faster
sees the same numbers repeated until the interval passes, and a counter
that repeats and then jumps makes a rate calculation look like a staircase.
It averages out correctly over any window worth alerting on, and it is
confusing to look at, so the two intervals should agree.

### What a scrape looks like

Taken from a running broker with an `append`, a `latest` and a `queue`
channel on a memory provider, and a fourth channel, an `append` on a
`sqlite` provider, that one inbound bridge fills from its peer. A durable
consumer is three records behind the head, a worker holds one of three
jobs, one publish went to a topic no channel claims and nobody subscribes
to, one publish into the dead-letter channel was refused, one device lost
its link and its Will was published, and one connection was refused for
naming an authentication method. `HELP` and `TYPE` precede every name and
are elided here after the first, and so are the response headers other
than the content type and the value of the `version` label, which is
whatever the build says it is. Every name in the catalogue has a line but
`saguin_storage_errors_total`, which has none until a provider fails, and
`saguin_subscriptions_refused_total`, which has none until a filter is
refused:

```
$ curl -s -D - http://127.0.0.1:9090/metrics

HTTP/1.1 200 OK
Content-Type: text/plain; version=0.0.4; charset=utf-8

# HELP saguin_build_info Always 1. This is where saguin's own version is stated.
# TYPE saguin_build_info gauge
saguin_build_info{version="…",broker_id="edge-1"} 1
saguin_uptime_seconds 9.613964048
saguin_connections 1
saguin_connections_by_protocol{protocol="5"} 1
saguin_subscriptions 2
saguin_max_session_expiry_seconds 2592000
saguin_channel_info{channel="events",filter="iot/+/events/+",type="append",provider="mem"} 1
saguin_channel_info{channel="jobs",filter="iot/+/work/+",type="queue",provider="mem"} 1
saguin_channel_info{channel="jobs__dlq",filter="iot/+/work/+/__dlq",type="append",provider="mem"} 1
saguin_channel_info{channel="readings",filter="iot/+/readings/+",type="append",provider="disk"} 1
saguin_channel_info{channel="state",filter="iot/+/state/+",type="latest",provider="mem"} 1
saguin_channel_bytes{channel="events"} 390
saguin_channel_bytes{channel="jobs"} 195
saguin_channel_bytes{channel="jobs__dlq"} 0
saguin_channel_bytes{channel="readings"} 126
saguin_channel_next_offset{channel="events"} 7
saguin_channel_next_offset{channel="jobs__dlq"} 1
saguin_channel_next_offset{channel="readings"} 3
saguin_channel_floor_offset{channel="events"} 1
saguin_channel_floor_offset{channel="jobs__dlq"} 1
saguin_channel_floor_offset{channel="readings"} 1
saguin_channel_records{channel="events"} 6
saguin_channel_records{channel="jobs__dlq"} 0
saguin_channel_records{channel="readings"} 2
saguin_channel_records{channel="state"} 2
saguin_channel_consumer_position_min{channel="events"} 4
saguin_channel_consumers{channel="events"} 1
saguin_channel_consumers{channel="jobs__dlq"} 0
saguin_channel_consumers{channel="readings"} 0
saguin_channel_partitioned_consumers{channel="events"} 0
saguin_channel_partitioned_consumers{channel="jobs"} 0
saguin_channel_partitioned_consumers{channel="jobs__dlq"} 0
saguin_channel_partitioned_consumers{channel="readings"} 0
saguin_channel_partitioned_consumers{channel="state"} 0
saguin_provider_info{provider="disk",type="sqlite"} 1
saguin_provider_info{provider="mem",type="memory"} 1
saguin_provider_bytes{provider="disk"} 90112
saguin_provider_bytes{provider="mem"} 768
saguin_provider_max_bytes{provider="disk"} 0
saguin_provider_max_bytes{provider="mem"} 0
saguin_storage_commits_total{provider="disk",closed_by="records"} 0
saguin_storage_commits_total{provider="disk",closed_by="interval"} 0
saguin_storage_commits_total{provider="disk",closed_by="commit"} 2
saguin_storage_commits_total{provider="disk",closed_by="unbatched"} 0
saguin_storage_committed_records_total{provider="disk"} 2
saguin_provider_publish_commit_max_records{provider="disk"} 256
saguin_bridge_info{bridge="head-office",peer="tcp://127.0.0.1:38884"} 1
saguin_bridge_connected{bridge="head-office"} 1
saguin_bridge_stopped{bridge="head-office"} 0
saguin_bridge_sent_total{bridge="head-office"} 0
saguin_bridge_loops_skipped_total{bridge="head-office"} 0
saguin_bridge_unsent_total{bridge="head-office",cause="peer_refused"} 0
saguin_bridge_unsent_total{bridge="head-office",cause="broadcast_refused"} 0
saguin_bridge_unsent_total{bridge="head-office",cause="unmappable"} 0
saguin_bridge_unsent_total{bridge="head-office",cause="live_queue_full"} 0
saguin_bridge_unsent_total{bridge="head-office",cause="superseded"} 0
saguin_bridge_received_total{bridge="head-office"} 2
saguin_bridge_unstored_total{bridge="head-office",cause="no_rule"} 0
saguin_bridge_unstored_total{bridge="head-office",cause="unmappable"} 0
saguin_bridge_unstored_total{bridge="head-office",cause="never_accepted"} 0
saguin_bridge_unstored_total{bridge="head-office",cause="queue_full"} 0
saguin_bridge_reconnects_total{bridge="head-office"} 0
saguin_connections_total 4
saguin_bytes_received_total 664
saguin_bytes_sent_total 909
saguin_publishes_received_total 14
saguin_deliveries_sent_total 4
saguin_deliveries_refused_total 0
saguin_sessions_offline 1
saguin_wills_published_total{cause="immediate"} 1
saguin_wills_cancelled_total 0
saguin_wills_waiting 0
saguin_sessions_restored_total 0
saguin_sessions_dropped_total{cause="storage_full"} 0
saguin_sessions_dropped_total{cause="expired_while_stopped"} 0
saguin_sessions_dropped_total{cause="subscription_refused"} 0
saguin_retained_messages 0
saguin_qos2_held 0
saguin_qos2_abandoned_total 0
saguin_qos2_max_inflight_per_client 20
saguin_session_expiry_shortened_total 0
saguin_published_total{channel="events"} 6
saguin_published_total{channel="jobs"} 3
saguin_published_total{channel="jobs__dlq"} 0
saguin_published_total{channel="readings"} 2
saguin_published_total{channel="state"} 2
saguin_published_total{channel="broadcast"} 1
saguin_broadcast_unmatched_total 1
saguin_deliveries_dropped_total 0
saguin_deliveries_expired_total 0
saguin_session_deliveries_dropped_total{cause="session_queue_full"} 0
saguin_session_deliveries_dropped_total{cause="packet_ids_exhausted"} 0
saguin_session_deliveries_dropped_total{cause="no_shared_member"} 0
saguin_session_deliveries_dropped_total{cause="storage_full"} 0
saguin_session_deliveries_dropped_total{cause="too_large"} 0
saguin_shares_held_total 0
saguin_shares_drained_total 0
saguin_shares_dropped_total{cause="backlog_full"} 0
saguin_shares_dropped_total{cause="no_member_left"} 0
saguin_shares_dropped_total{cause="expired"} 0
saguin_shares_dropped_total{cause="storage_full"} 0
saguin_shares_dropped_total{cause="member_ended"} 0
saguin_shares_dropped_total{cause="not_authorized"} 0
saguin_session_queue_messages 0
saguin_session_queue_bytes 0
saguin_publish_refused_total{reason="not authorized"} 1
saguin_connections_refused_total{reason="bad authentication method"} 1
saguin_channel_retention_removed_total{channel="events"} 0
saguin_channel_retention_removed_total{channel="jobs"} 0
saguin_channel_retention_removed_total{channel="jobs__dlq"} 0
saguin_channel_retention_removed_total{channel="readings"} 0
saguin_channel_retention_removed_total{channel="state"} 0
saguin_channel_position_lost_total{channel="events"} 0
saguin_channel_position_lost_total{channel="jobs"} 0
saguin_channel_position_lost_total{channel="jobs__dlq"} 0
saguin_channel_position_lost_total{channel="readings"} 0
saguin_channel_position_lost_total{channel="state"} 0
saguin_latest_superseded_total{channel="state"} 0
saguin_queue_depth{channel="jobs"} 3
saguin_queue_inflight{channel="jobs"} 1
saguin_queue_delivered_total{channel="jobs"} 1
saguin_queue_acknowledged_total{channel="jobs"} 0
saguin_queue_returned_total{channel="jobs"} 0
saguin_queue_redelivered_total{channel="jobs"} 0
saguin_queue_dead_lettered_total{channel="jobs"} 0
saguin_queue_expired_total{channel="jobs"} 0
saguin_queue_retain_ignored_total{channel="jobs"} 0
saguin_go_heap_live_bytes 1.0514432e+07
saguin_go_heap_goal_bytes 2.1106112e+07
saguin_go_gc_cycles_total 3
saguin_go_gc_cpu_seconds_total 0.004718377
saguin_go_gc_assist_cpu_seconds_total 0.000125117
saguin_go_stack_bytes 917504
saguin_go_allocated_bytes_total 2.4389552e+07
saguin_go_allocated_objects_total 118004
saguin_go_gogc_percent 100
saguin_go_memory_limit_bytes 9.223372036854776e+18
```

**One line in it does not read as you might expect.**
`saguin_provider_bytes{provider="mem"}` is 768 where the channels on it
sum to 585. The provider holds `state` as well, which `saguin_channel_bytes`
does not measure, and it is `broker.session.storage` here, so the session
the durable consumer left is in it too.

### The catalogue

Closed. A name here is a promise; a name not here is not published.

**The broker**

| Metric | Type | |
|---|---|---|
| `saguin_build_info{version,broker_id}` | gauge | Always 1. This is where Sagüin's own version is stated, and the reason `$SYS/broker/version` is not to be believed. |
| `saguin_uptime_seconds` | gauge | |
| `saguin_connections` | gauge | Connected now. Read from the substrate's own counter, which it maintains whether or not anything asks |
| `saguin_connections_by_protocol{protocol}` | gauge | Connected now, by the MQTT version each client connected with - `5` or `3.1.1`, and no other label is possible. It is the fleet mix: how much of the estate is still on the older protocol, and whether that number is coming down. **Counted from the client table at each scrape rather than kept by increments**, because a gauge maintained on connect and disconnect is a gauge that drifts. **It sums to `saguin_connections`**: the substrate keeps an inline client of its own in that table - the one it publishes on its own behalf with - and the walk leaves it out, or a broker with one MQTT 5 subscriber would report one connection and two clients speaking MQTT 5. One client is a small and constant error, which is what lets one stand: three hundred against three hundred and one has nothing about it that looks wrong |
| `saguin_connections_total` | counter | Accepted since start |
| `saguin_sessions_offline` | gauge | Sessions this broker holds that nothing is connected to. **A session outliving its connection is what a session is for**, so this is a fleet's shape rather than a fault - but it is the question `saguin_connections` cannot answer: three hundred devices with two hundred connected is either a rota or a hundred that have stopped calling, and the connection count alone cannot tell them apart. A number that only climbs is sessions nothing is coming back for, held until their expiry. **A session this broker put back when it started counts here too**, because it is the same thing: a session held with nothing connected to it, whether its client left a minute ago or before the last restart. **Derived rather than read**, because the substrate derives it inside the `$SYS` tree Sagüin silences - the inputs are a map length and a counter, and this scrape already walks that table for the protocol split. |
| `saguin_wills_published_total{cause}` | counter | Wills this broker published on a client's behalf, by what made each due. `immediate` is a connection that ended without a `DISCONNECT` and no Will Delay Interval; `delayed` is one whose delay ran out; `session_ended` is a session that ended while its Will was still due - it expired, a clean start under its client id ended it, or its client came back to find retention had passed one of its positions (RFC 0003 "Sessions"); `start` is a Will this broker found already owed when it started, because the delay passed or the session ended while it was stopped. **Nothing dies because the broker stopped**, so a restart never adds to `immediate` for the clients it disconnects - a fleet's worth of these at a restart would be the broker announcing its own maintenance as a mass outage. |
| `saguin_wills_cancelled_total` | counter | Wills a client cancelled by resuming its session inside its Will Delay Interval [MQTT-3.1.3-9]. **This is the delay working rather than a fault**: a number climbing here beside a flat `saguin_wills_published_total` is a fleet on a bad link that is correctly not being announced dead, which is the whole reason the interval exists. |
| `saguin_wills_waiting` | gauge | Wills waiting out their Will Delay Interval now: clients that have gone and whose deaths have not been announced yet. Each is held in `broker.session.storage` with the moment it becomes due, so a restart inside the wait publishes it when it lands rather than losing it - and a number that only climbs is devices leaving faster than their delays run out. |
| `saguin_sessions_restored_total` | counter | Sessions this broker put back when it started, from what `broker.session.storage` kept while it was stopped. Each is answered `Session Present = 1` when its client comes back and is sent what it was owed. **It moves once, at the start, and then stands still**, so what it reports is what the last start found - read beside `saguin_sessions_dropped_total`, the two say what a restart did to a fleet. A start that restored none, on a broker whose clients ask for persistent sessions, is a session store that did not keep them: a `memory` provider with no `snapshot_dir`, or one that was stopped by a crash rather than a signal. Nothing else says so. |
| `saguin_sessions_dropped_total{cause}` | counter | Sessions the session store could not keep, by cause. `storage_full` is a client that asked for its session to outlive its connection while the provider `broker.session.storage` names was at its `max_bytes`: it was accepted, and told in its `CONNACK` (Session Expiry Interval 0) that the session ends with the connection. **Nothing else shows a fleet losing its sessions to a full provider**, because the clients connect as usual and only find out when they come back to nothing. `expired_while_stopped` is a session whose expiry passed while the broker was stopped, ended when it started with the messages it was owed. `subscription_refused` is a session ended at a start because it held a subscription this broker's rules refuse - a channel that became a queue while it was stopped, most often, whose records must reach a worker and nobody else. The session goes rather than the subscription, because MQTT has no way to tell a resuming client that one of its subscriptions is gone: it comes back to `Session Present = 0` and is answered for each filter it asks for again. |
| `saguin_session_queue_messages` | gauge | Deliveries every session holds that its client has not acknowledged, connected or not: on the wire, waiting for room in the client's Receive Maximum, or queued while it is away - for broadcast from the log, every message a session is owed after its cursor (RFC 0003 "Broadcast"). **A channel's records waiting to be sent are not in it**: an `append` consumer waits at its position, and a `latest` subscriber's waiting values - at most one per topic its subscriptions reach, a newer one replacing an older - belong to its connection rather than its session, and are bounded by those topics rather than by `limits.session_queue_bytes`. **Summed over sessions and never split by one**, because the only label that would divide it is the client id - a string the client chose. One session's share is bounded by `limits.session_queue_bytes`, and the log names a session when it reaches that. Read from counts each session keeps as deliveries arrive and leave - its in-flight table's, in the same walk as the row above, and its list on the broadcast log - each delivery counted once. |
| `saguin_session_queue_bytes` | gauge | The memory those deliveries take, as `limits.session_queue_bytes` counts it for one session: payload, topic and properties, and beside them about 600 bytes for each delivery in the client's in-flight table and 300 once for a table holding any, 80 for each message a session is owed from the broadcast log, and both and 192 more for one of those on the wire, 600 once while any is (RFC 0002). |
| `saguin_retained_messages` | gauge | Retained messages held on broadcast topics, and **Sagüin's own store rather than the substrate's**, whose count is wrong here for the reason given under "There is no `$SYS` tree". They accumulate until something publishes an empty payload to the topic or `broker.retained.retention_period` removes them, and nothing else says how many there are. Always reported: every broker keeps the store. |
| `saguin_qos2_held` | gauge | Exactly-once publishes received and not yet released - messages this broker has taken ownership of at their `PUBREC` and that no client has finished sending. A figure that climbs and stays is publishers not completing exchanges. Each is held in the store of the channel it is for, or the broadcast log's for a broadcast, and the broker answers this from its own count of them, kept as each is held and released, rather than asking every channel's store; their bytes are in their provider's `saguin_provider_bytes`. Always reported, with `saguin_qos2_abandoned_total` and `saguin_qos2_max_inflight_per_client`: every broker offers QoS 2. |
| `saguin_qos2_abandoned_total` | counter | Exactly-once publishes taken in and never released, because their session ended, they aged past `broker.qos2.expires_after`, or the start found them holding for a session that did not come back or in a channel the configuration does not keep. **Nothing else can show them**: the publisher has had its `PUBREC` and is simply never answered, and one that completes is already counted where its record lands. One series rather than one per reason, because both are the same thing seen by a publisher - an exchange that did not finish. |
| `saguin_qos2_max_inflight_per_client` | gauge | The configured `broker.qos2.max_inflight_per_client`, published for the reason `saguin_provider_max_bytes` and `saguin_max_session_expiry_seconds` are: a dashboard should be able to read what is held against what was allowed. |
| `saguin_subscriptions` | gauge | Every subscription the broker holds, not only the ones on channels. The substrate's own counter |
| `saguin_session_expiry_shortened_total` | counter | CONNECTs that asked to keep their session for longer than `max_session_expiry` and were given the cap. The client is told in its CONNACK; this is what tells the operator, and nothing else does. |
| `saguin_max_session_expiry_seconds` | gauge | The configured cap, so a dashboard can read what a fleet asks for against what it gets - the same reason `saguin_provider_max_bytes` is published beside the bytes held. |

The counter is Sagüin's own comparison of the requested Session Expiry
Interval against the limit, at connect time, on a path that is not the
hot one.

**What the box moved.** Five counters about the wire rather than about a
channel, and the only ones here a throughput is drawn from: everything else
counts records Sagüin decided to keep or deliver. Every one of them is read
from a counter the substrate already maintains as it reads and writes
sockets - the numbers its own `$SYS` tree publishes, which Sagüin silences,
and silencing stops the publishing and not the counting. Keeping a tally
beside one of them would be a second number for one fact.

**They are not split by listener**, and that is a decision rather than a
gap. The substrate keeps one set of totals for the whole broker, so a
figure per door would mean Sagüin counting bytes itself as they cross each
connection - the second tally the paragraph above refuses, to answer a
question an operator can usually settle from which addresses are in use.

| Metric | Type | |
|---|---|---|
| `saguin_bytes_received_total` | counter | Bytes read from MQTT connections since start. **Everything on the wire**: a payload, the acknowledgement behind it, a keepalive, a connection being set up. That is what sizes a link, and it is not the same question as how much payload a fleet sent. A bridge's traffic to its peer is in none of these four - that link is Sagüin acting as a client through another library, which the substrate's counters never see. |
| `saguin_bytes_sent_total` | counter | Bytes written to MQTT connections since start, on the same terms. |
| `saguin_publishes_received_total` | counter | Publishes that arrived, **the ones Sagüin went on to refuse included**: every PUBLISH a client sent, and every record an inbound bridge carried in over its upstream connection. Not a queue's offer to its worker, and not a Will the broker publishes on a client's behalf, which arrived as nothing. Against `saguin_published_total` summed, the pair says how much of what a fleet sends is landing, which neither answers alone. |
| `saguin_deliveries_sent_total` | counter | Publishes Sagüin sent to subscribers, a queue record handed out a second time included. **This is the read side**, and nothing else in this catalogue counts what leaves a channel that is not a queue. |
| `saguin_deliveries_refused_total` | counter | **Broadcast** deliveries Sagüin could not send because the subscriber already held `limits.session_queue_bytes` it had not acknowledged, or had run through its packet identifiers. **Every one is also counted in `saguin_session_deliveries_dropped_total`**, under `session_queue_full` or `packet_ids_exhausted`: this is the refusal seen from the delivery, those are the same losses seen from the session, and adding the two counts each twice. A different loss from `saguin_deliveries_dropped_total`, which is that client's outbound queue overflowing: the same complaint one layer apart, and an operator watching only the other sees half of what a slow subscriber costs. **A channel record meeting a full window is neither of them and is not lost** - Sagüin delivers those itself: an `append` record waits at the consumer's position, and a `latest` value waits for that subscriber, replaced by any newer value for its topic (`saguin_latest_superseded_total`), and each is sent when a slot frees. A consumer that leaves first has its position held below what it was not sent, so a resume carries it (RFC 0003). Broadcast promises nothing beyond delivery to whoever is connected at the time, so there is nothing to come back for, which is what makes this half the half worth counting. |

**Publishing**

| Metric | Type | |
|---|---|---|
| `saguin_published_total{channel}` | counter | One series per channel, plus one for broadcast. A dead-letter channel is always zero: a record arrives there by the queue's own move and a publish into it is refused, so `saguin_queue_dead_lettered_total` is where its arrivals are counted. |
| `saguin_connections_refused_total{reason}` | counter | Connections this broker refused, by reason, whatever protocol the client speaks: a CONNECT turned away before a session existed, a connection ended for a refusal, and a socket closed for want of a `max_connections` slot before it sent a CONNECT that could be answered: a websocket before its upgrade, and on any door one arriving with the overflow budget full (RFC 0002). That last is counted as `server busy` whether or not the socket would have sent a CONNECT, and makes no row on `/v1/operations/refused`, since it has no client id; a tcp socket read to be refused is counted when its CONNECT is. An MQTT 5 client is sent a `DISCONNECT` naming the code; one below MQTT 5, whose `PUBACK` carries no reason code, has its connection closed rather than a record acknowledged that Sagüin threw away. Labelled with the same reason vocabulary as the row below, so a rate of one against the other reads directly - of the publishes refused for this reason, how many cost a device its connection. **This is the series behind a fleet in a reconnect loop**, and the one to alert on. `session store full` appears here and nowhere else: a client that armed a Will the provider `broker.session.storage` names had no room for is refused `0x97`, because MQTT has a way to tell a client its session ends with its connection and none to tell it its Will is not held |
| `saguin_publish_refused_total{reason}` | counter | Labelled with the MQTT specification's name for the code the client was answered with - and never the sentence Sagüin sent beside it. The set is closed but it is not only the specification's: on the publish path `0x97` is the wire answer for six different things, so `publish rate exceeded` and `inflight allowance exceeded` are each split out from `quota exceeded`, and a refusal that carries no code at all is `rejected`. The second of those is a publisher holding as many unfinished exactly-once exchanges as `broker.qos2.max_inflight_per_client` allows it, which is a client to look at where a full channel is storage to look at. This is what answers "the fleet's data is not arriving" without reading a log - including the case an operator is most likely to have caused, a rule that refuses the fleet `0x87`, which is counted where the acl_file answers rather than on the publish path it never reaches. The one refusal missing from it is the `$SYS` publish, for the reason given at the end of this document. |
| `saguin_subscriptions_refused_total{reason}` | counter | Topic filters a `SUBACK` refused, one for each filter, labelled with the MQTT specification's name for the code decided for it. **Counted before a 3.1.1 client's code is written as `0x80`**, the one failure that protocol has, so a 3.1.1 fleet refused by an acl_file reads `not authorized` here and not `unspecified error`. Counted once, where the codes of a `SUBACK` are all known, so every refusal is here whichever part of Sagüin decided it: `topic filter invalid` for a filter that is not one or is deeper than `limits.max_topic_levels`, `not authorized` for a filter the client's roles do not allow, `quota exceeded` for a session store at its bound or a client at `limits.max_subscriptions`, `implementation specific error` for a queue's form, a reserved property or a partition declaration Sagüin refuses, or a session store that could not keep the subscription other than for room, `packet identifier in use`, and `shared subscriptions not supported`. No series until a filter has been refused, as the row above. What answers "why is this device not getting anything" when its SUBSCRIBE was answered and nobody read the codes |
| `saguin_broadcast_unmatched_total` | counter | Publishes that matched no subscription and no channel. **This is what answers a mistyped channel name**, which nothing else can: a publish to `event/x` where the channel is `events` is ordinary broadcast to nobody, and MQTT has no reason code for it. |
| `saguin_deliveries_dropped_total` | counter | Broadcast deliveries at QoS 0 discarded because a subscriber's outbound queue was full; one at QoS 1 or 2 waits in its session instead. **This is what says a subscriber is being shed**, and nothing else does. It does not count channel deliveries - see below. |
| `saguin_deliveries_expired_total` | counter | Deliveries discarded because the publisher's Message Expiry Interval ran out before they were sent. One already on the wire is never discarded by expiry, whatever its session, as MQTT has the sender of an exactly-once delivery do (MQTT-4.3.3-7): a resumed session is sent it again under its identifier (MQTT-4.4.0-1), and a session that ends with its connection takes it with it when it ends, which is not counted here. |
| `saguin_shares_held_total` | counter | Deliveries put on a shared group's list (RFC 0003 "Broadcast"): every QoS 1 or 2 delivery a group with a member whose session outlives its connection is owed - one its bound or its provider has no room for as well, counted dropped as it arrives - every one returned to it by a member's ending or dropped there once it had been handed out (`member_ended`, or a return its provider has no room for), and at a start every one put back on a group's list or dropped with a group no session holds. A group over an `append` or `latest` channel is counted exactly as a group over a broadcast topic is: the records it is owed are held as copies in the broadcast log. Everything else is counted as `no_shared_member` below. |
| `saguin_shares_drained_total` | counter | Deliveries a shared group handed to a member, which is the only way one leaves its list except by being dropped. **Held minus drained minus dropped is what the groups are holding**, exactly; **drained flat while held climbs is a fleet that is not coming back**, and the backlog is spending its provider until one of them does. |
| `saguin_shares_dropped_total{cause}` | counter | Deliveries a shared group dropped, by a closed set of six causes. `backlog_full`: at `limits.session_queue_bytes` for that group, the oldest given up for a newer one. `no_member_left`: the last member session that could have collected them ended, so nobody was owed them any more - which is the rule that a backlog cannot outlive the sessions it belongs to. `expired`: held longer than `broker.share.expires_after`, dropped by the running sweep or by a start, or past the publisher's own Message Expiry Interval when the group reached it - **a cause of its own, because the label names the knob**, and an expiry counted as a full queue sends an operator to grow a bound that was never reached. `storage_full`: `broker.session.storage` had no room, so the oldest part of the broadcast log went while the group was owed something in it, or a delivery a member's ending returned could not be kept even in the provider's reserve (RFC 0002 "Every session's state: `broker.session`"). `member_ended`: a QoS 2 delivery a group handed a member whose session ended before the member answered it with a PUBREC, which MQTT forbids sending to another member (MQTT-4.8.2-5; RFC 0003 "Broadcast"). `not_authorized`: the `acl_file` allowed it to none of the members that could take it. **All six series from the start**, at zero until their cause happens, for the reason the five below are. |
| `saguin_session_deliveries_dropped_total{cause}` | counter | Deliveries a session never received, by a closed set of five causes Sagüin names. `storage_full`: the provider `broker.session.storage` names was full, so the oldest of the broadcast log went and this session was owed it - to make room for the log's next message, or for any other write on a provider the log shares, a channel's record among them (RFC 0002 "Every session's state: `broker.session`"). `session_queue_full`: at `limits.session_queue_bytes`, the oldest the session was owed that was not on the wire given up, or a new delivery refused because the session already held that much unacknowledged (RFC 0002). `no_shared_member`: a shared group had no member that could take the message - every member offline, or connected with no room for it. `packet_ids_exhausted`: refused with no packet identifier left. `too_large`: larger than the client's Maximum Packet Size, so discarded rather than sent, as MQTT has the server do [MQTT-3.1.2-25]: a broadcast delivery, whichever way it goes out - live, from the broadcast log, or re-sent to a resumed session - and its subscriber stays connected. A channel's record or a queue's job too large for its client is not counted here: that client is disconnected instead, and a shared group's member it does not fit is passed over (RFC 0003 "When a record is too large for a subscriber"). **Five series from the start**, each at zero until its cause happens, because every one is a thing to alert on and a series that appears only once it fires is one a rule cannot be written against in advance. |

**Shedding a slow subscriber is a bound being enforced, not a fault.** A
client's outbound queue holds a fixed number of packets, and no more bytes
than half its session's `limits.session_queue_bytes`, and the broker
discards past it rather than growing - invariant 13, and the reason one
subscriber that has stopped reading cannot become a dead broker. What the
counter adds is that it can be seen: it is the only thing that says a
fleet is being shed.

**It counts broadcast at QoS 0, and that is the whole of it.** A QoS 0
delivery reaches the substrate's bounded outbound queue and is discarded
there; everything else is held within its session's
`limits.session_queue_bytes` and given up by that bound's own rules,
counted as `session_queue_full` below (RFC 0002). A queue cannot
contribute at all - QoS 0 on its form is refused.

**So this counter staying at zero says nothing about a slow consumer.**
What reports one is `session_queue_full`, or
`disconnected a consumer that stopped reading` at WARN, naming the client
and the deadline it exceeded - written in `OnDisconnect`, where every route
ends, rather than beside either write. An `append` or `latest` delivery, a
dead-letter record and every control reply never reach the substrate's
outbound queue at all: Sagüin writes them to the consumer's socket under
`limits.write_timeout`, and a consumer that does not take one in time is
**disconnected** rather than dropped (RFC 0002). So a consumer that stops
reading those shows up as a disconnection in the log and never here. **A
`latest` subscriber that reads, but more slowly than its topics change,
shows up in neither**: each topic's value waits for it and a newer one
replaces a waiting one - RFC 0003's promise, the current value rather than
every intermediate one - and `saguin_latest_superseded_total` counts what
was replaced.

**A queue worker is on that route too, and is disconnected like any other
client.** Its offers are written by the substrate, under the same
`limits.write_timeout`, so a worker that stops reading is hung up on and
its leases return as any disconnected worker's do. What it does not get is
a lease clock, for the reason invariant 7 gives - a deaf worker and a busy
one look the same from here.

**The two are separate numbers because an operator does a different
thing about each.** A drop means something is wrong at the other end; an
expiry is the publisher's own instruction being carried out, and added
together they would be a figure nobody could act on.

**Neither carries a label, and the useful one is the one that cannot
exist.** The question is which consumer, and a client picks its own id -
see "Labels, and where the catalogue stops". The consumer's name is in the
broker's log beside the drop instead, bounded the way every client-chosen
string is, and written at debug because a consumer thousands of packets in
arrears produces one line per packet.

**Channels.** A channel carries a series here only where the number
exists. `next_offset` and `floor_offset` are offsets, and a `queue` and a
`latest` channel have neither - a queue holds work rather than a log, and a
latest channel's offsets are sparse, because replacing a value gives it a
new one. `records` outlives them both: on an `append` channel it is the
subtraction of the two, and on a `latest` channel it is how many topics
hold a current value. A queue has neither derivation and carries
`queue_depth` instead. An absent series is deliberate: a zero would read as
an empty channel rather than as one nobody counts, and a dashboard reading
a wrong number is worse off than one reading nothing.
`consumer_position_min` follows the same rule for a channel nobody holds a
position on, and `records` and `bytes` have their own exceptions below.

| Metric | Type | |
|---|---|---|
| `saguin_channel_info{channel,filter,type,provider}` | gauge | Always 1. Carries the topic filter the channel claims, and which storage provider holds it so a dashboard can tell a memory-backed channel from a durable one - see the notes below. |
| `saguin_channel_records{channel}` | gauge | Records held, by whichever derivation the channel's shape allows. On an `append` channel it is `next − floor`, exact because retention only ever removes a prefix. On a `latest` channel it is how many topics hold a current value - one value per topic, so that count *is* the records - and `next − floor` cannot stand in for it, because a replacement takes a new offset and the subtraction would count writes instead. Both providers answer it from a number they hold rather than by counting rows for the scrape; a stored deletion is a row and counts as one, on both. A `queue` has neither derivation and carries `saguin_queue_depth`. |
| `saguin_channel_bytes{channel}` | gauge | `append` and `queue` channels. A `latest` channel is absent: one value per topic means a write is a replacement, so measuring it would have to read the value going out first - half again the cost of the write, paid on every publish, to feed a metric. |
| `saguin_channel_next_offset{channel}` | gauge | |
| `saguin_channel_floor_offset{channel}` | gauge | The oldest offset still readable |
| `saguin_channel_retention_removed_total{channel}` | counter | Records retention has deleted |
| `saguin_channel_consumer_position_min{channel}` | gauge | The lowest stored position. Against `next` it is the worst lag; against `floor` it is the alert below. |
| `saguin_channel_consumers{channel}` | gauge | How many durable consumers hold a stored position, which is what the lowest one is a reading *from*. Zero carries a series: it is a fact about a channel nobody reads, where an absent one would read as a channel that cannot have consumers. |
| `saguin_channel_partitioned_consumers{channel}` | gauge | How many subscribers have declared a partition slice on this channel (RFC 0003). It is a denominator and nothing more: it does not say which slices, and it deliberately does not report whether they cover the space - a missing index cannot be told from a member that has not connected yet, so a coverage gauge would be wrong during every rolling restart. The count and the index are not labels, because a client chooses them. |
| `saguin_channel_position_lost_total{channel}` | counter | Readers whose stored position the retention floor passed, so records they had a claim on were unreadable - invariant 1 firing. **It counts occurrences rather than readers**: the floor passes a lagging reader repeatedly, and a fleet of one straggler can move this number thousands of times. Every kind of reader and every way the loss is discovered, because the question it answers - are records being lost - does not change with which door the loss came through: a consumer overtaken while connected and reading, which MQTT gives no way to tell; one whose stored position had already been passed when it came back, which is told with Session Present = 0; and an outbound bridge rule whose position retention passed. |
| `saguin_latest_superseded_total{channel}` | counter | On a `latest` channel only: values a subscriber was not sent because a newer value for the same topic took their place while they waited for it - the subscriber had no room, or its delivery was behind (RFC 0003, "the current value, not every intermediate one"). **Not a loss**: every subscriber is sent what is current, and settles on it. It is what says a `latest` subscriber reads more slowly than its topics change, which nothing else does - no log line is written for a value replaced, because one per value is how a log stops being read. Against `saguin_published_total{channel}` times the subscribers, it closes the account of what was sent. |

**Queues** - one series per queue channel.

| Metric | Type | |
|---|---|---|
| `saguin_queue_depth{channel}` | gauge | Unresolved work |
| `saguin_queue_inflight{channel}` | gauge | Out with a worker now |
| `saguin_queue_delivered_total{channel}` | counter | Records handed to a worker, once per hand-over. A record the broker gives back before any worker holds it - no live worker with room, or larger than the chosen worker's Maximum Packet Size - is not counted |
| `saguin_queue_acknowledged_total{channel}` | counter | |
| `saguin_queue_returned_total{channel}` | counter | Handed back by a worker |
| `saguin_queue_redelivered_total{channel}` | counter | Taken back on a visibility timeout |
| `saguin_queue_dead_lettered_total{channel}` | counter | |
| `saguin_queue_expired_total{channel}` | counter | Work that aged out unresolved |
| `saguin_queue_retain_ignored_total{channel}` | counter | Retained publishes this queue took as ordinary work, with the flag dropped. A queue holds work rather than state and grants no subscription a retained message could be delivered to, so there is nothing for the flag to ask for - and nothing on the wire says so, since the publish is acknowledged like any other. It rises when a producer believes it is setting state on a topic a queue claims, which is a question about the operator's own `filter` rather than a broker fault, and this series is the only place it is visible |

**A release lands in exactly one of `returned`, `redelivered` and
`dead_lettered`, and dead-lettering wins.** A worker's return that spends
the last attempt is counted as dead-lettered and not as returned, so
`delivered = acknowledged + returned + redelivered + dead_lettered` holds
with nothing double-counted and nothing missing. `expired` is not one of
the three: work that aged out is counted there as well as in whichever of
the three released it.

**The identity is over deliveries that have ended**, which is the part to
read before wiring an alert on it. A delivery in flight has been counted
in `delivered` and in none of the four, so the two sides differ by
`saguin_queue_inflight` - which is why the scrape above shows `delivered`
at 1 beside four zeros and is not a broker miscounting. Subtract the
in-flight gauge, or compare the two sides on a queue that is idle.

**Storage** - one series per provider.

| Metric | Type | |
|---|---|---|
| `saguin_provider_info{provider,type}` | gauge | Always 1; `type` is `memory` or `sqlite` |
| `saguin_provider_bytes{provider}` | gauge | Everything the provider holds, across every channel on it - including a `latest` channel, which `saguin_channel_bytes` does not measure, so this figure is larger than those summed. On a `sqlite` provider it is the database's own size, which is **not** its disk usage: a write-ahead log waiting to be checkpointed is real bytes on that disk and is not counted here, so do not size a volume from it. It counts freed pages too: retention reuses them rather than returning them, so the figure holds at its peak after a sweep, and that is not retention failing - RFC 0004 says how an operator gives the disk back, on a stopped broker |
| `saguin_provider_max_bytes{provider}` | gauge | Zero for no bound |
| `saguin_storage_errors_total{provider}` | counter | Storage calls that failed, and a sqlite provider's periodic fsync of its write-ahead log (`flush_interval`) that failed, which is also logged at ERROR once per streak of failures. No series until a provider has failed, the rule `saguin_publish_refused_total` follows for a code nobody has been answered: a rate over a series that does not exist is nothing, and nothing has gone wrong |
| `saguin_storage_commits_total{provider,closed_by}` | counter | Transactions that stored publishes, by what closed them: `records` where the transaction filled, `interval` where it did not and waited out `publish_commit_interval`, `commit` where the provider collects without waiting and the transaction held what arrived while the one before it committed, or the one publish that found none committing, `unbatched` on a provider that commits one publish at a time (`none`). A retention sweep and a queue resolution are transactions too and are not counted here. **`sqlite` providers only** - a memory provider has no transaction to collect into |
| `saguin_storage_committed_records_total{provider}` | counter | Records those transactions carried. `sqlite` only |
| `saguin_provider_publish_commit_max_records{provider}` | gauge | The provider's `publish_commit_max_records`; 256 where the key is absent and publishes are collected without waiting; zero with `none`, where they are not collected. `sqlite` only |

**Whether collecting publishes is doing anything is the one thing an
operator cannot see any other way**, which is what those three are for. A
provider collecting into shared transactions and one committing singly hold
identical records, identical offsets and identical counter rows - the only
difference is how long a publisher waited. So:

```
rate(saguin_storage_committed_records_total[5m])
  / rate(saguin_storage_commits_total[5m])
```

is the average batch size, and it is read against
`saguin_provider_publish_commit_max_records` beside it. Where
`publish_commit_interval` is set, a batch size near one under a ceiling of
hundreds means every transaction is waiting out the interval and
collecting almost nothing, which is **slower than not collecting at all** -
measured in RFC 0002. Without the key, a batch size near one means only
that little arrives at once, and it costs nothing: no transaction waits.
The same thing shows directly in `closed_by`: where nearly every commit is
closed by `interval` rather than by `records`, the count is set above the
traffic.

**Bridges** - one series per bridge.

| Metric | Type | |
|---|---|---|
| `saguin_bridge_info{bridge,peer}` | gauge | Always 1 |
| `saguin_bridge_connected{bridge}` | gauge | 1 or 0 |
| `saguin_bridge_stopped{bridge}` | gauge | 1 when the bridge halted itself rather than losing its link: the peer holds a record larger than this broker's `max_message_size`, so it disconnects rather than deliver it and every reconnection ends the same way on the same record. `saguin_bridge_connected` is 0 beside it - **which is why this gauge exists.** A link that is merely down reads the same on that one, and only this tells an operator whether they are waiting for a reconnection or for a person |
| `saguin_bridge_received_total{bridge}` | counter | Records that arrived from the peer |
| `saguin_bridge_unstored_total{bridge,cause}` | counter | Records from the peer the bridge did not store, by a closed set of causes, each a record the peer was told was finished and so lost at this hop, and each logged when it happens - a run of `queue_full` drops once as it begins and once as it ends (RFC 0002 "Bridges"). `no_rule`: no inbound rule covers the peer's topic. `unmappable`: a rule covers it and built no topic, or only one in the reserved `$` space - the bridge's own configuration, the word `saguin_bridge_unsent_total` uses for the same two on the way out. `never_accepted`: Sagüin would refuse the record whatever the channel - more topic or more headers than it allows - so it is dropped rather than retried for ever. `queue_full`: a QoS 0 record dropped because the bridge's queue was full behind a channel that was not taking what it was given. The bridge stores off the connection's read loop, so that a refusing channel cannot keep it from noticing a lost link, and the queue between is bounded: a QoS 1 or 2 record is never dropped, because the peer may hold no more of them unacknowledged than the bridge's Receive Maximum, and a QoS 0 record - which nothing bounds - is dropped past its share of it, at-most-once as its publisher asked. |
| `saguin_bridge_sent_total{bridge}` | counter | Records this broker forwarded to the peer |
| `saguin_bridge_loops_skipped_total{bridge}` | counter | Records not forwarded because they arrived over a bridge. **Read it beside `sent_total` or not at all**: a bridge that is connected, sending nothing and skipping everything looks on `sent_total` alone exactly like one with nothing to send, and the two want opposite actions. This one climbing is a topology somebody built - two brokers pointed at each other - where the guard is working and the records are going nowhere |
| `saguin_bridge_unsent_total{bridge,cause}` | counter | Records an outbound rule did not send the peer, by a closed set of causes split by what an operator does about each. `peer_refused`: the peer refused it for good - its ACL or its limits - logged with the reason code. `broadcast_refused`: the peer refused a broadcast record, or the link was down when it was published - either way it has no store to retry from, so that is the loss. `unmappable`: the rule could build no topic for it, or only one in the reserved `$` space - the bridge's own configuration, logged. `live_queue_full`: a broadcast record dropped because the rule's queue was full, rather than hold up every publisher behind one slow link. `superseded`: a `latest` value replaced by a newer one for its topic while it waited to cross - **not a loss**, the newer one crosses; it is what says a link ran behind. |
| `saguin_bridge_reconnects_total{bridge}` | counter | |

**There is no metric for when a memory channel was last written to disk**,
and its absence is deliberate. Memory durability is exactly the last
successful snapshot (invariant 14), snapshots are taken at shutdown, and a
gauge reading "never, since start" for the whole life of a healthy process
would be read as a fault. `saguin_provider_info{type="memory"}` is the
honest form of the same warning: it says which channels are only as
durable as the next clean shutdown, which is the fact an operator needs.

### The one alert this exists for

Two of the channel gauges are the reason the catalogue is worth building:

```
saguin_channel_floor_offset > saguin_channel_consumer_position_min
```

**A consumer's data has already been deleted.** Retention has passed a
stored position, so when that consumer returns it is refused rather than
served the oldest surviving record - invariant 1 holding, which is correct
and is also a customer's missing afternoon of telemetry. MQTT has no way
to express it, and a broker that keeps no per-consumer position has no way
to compute it - Sagüin can because it keeps the two numbers anyway. Its
warning shot is `next − consumer_position_min` growing, which is a device
that has been offline long enough to be worth looking at before retention
reaches it.

**The `filter` label is what makes the catalogue usable by a reader rather
than only by a dashboard**, because placement is not derivable from the
name. A tool that scrapes this endpoint learns which channels exist, what
each is, and which topics each holds. Without it a reader knows a channel
called `water-location` exists and has no way to find out that its records
are at `iot/water/location/+`, so it cannot subscribe, cannot attribute a
record it receives, and cannot tell an operator where to look. A derived
dead-letter channel carries its derived filter here for the same reason:
nobody wrote that one down either.

**It is the filter as written, braces and all**, and that is a choice
rather than an oversight. A channel keeps one series here - that is what
makes this metric a channel list - and a `{a,b}` filter stands for several
plain ones, so the two cannot both be true of one label. The written form
is the one that matches the configuration file and the one an operator
recognises.

**So a `{a,b}` filter is not something to put on the wire.** A brace is
configuration syntax and not MQTT: subscribing with
`iot/+/{status,location}/+` is granted, treated as one literal level, and
matches nothing for ever - the silent kind. A reader that means to
subscribe expands the braces first, as `saguin --route` prints them
expanded and as the connectors viewer does before it subscribes.

Two ways to read the distance, and they answer slightly different questions:

```
on the broker being copied, where the link is a durable consumer:
  saguin_channel_next_offset − saguin_channel_consumer_position_min

across the pair, comparing one channel on both brokers:
  next_offset{on the source} − next_offset{on the copy}
```

The second is only meaningful because a copy stores each record at the
offset it was given, so the two counters mean the same thing on both
brokers - which is the whole reason offsets are preserved. The first is
sharper where the link is the furthest-behind consumer and needs only one
broker scraped, and it says nothing useful where an application consumer is
further behind than the link.

A `latest` channel answers neither, because it keeps no positions and its
offsets are sparse by nature. What says a copy of one is current is
`saguin_bridge_connected` and the rate of `saguin_bridge_received_total`.

**The Go runtime**

**The runtime's own figures, not Sagüin's**, read with `runtime/metrics`
when a scrape recomputes the catalogue and at no other time: a few
microseconds a scrape, and nothing while nobody asks. Never with
`runtime.ReadMemStats`, which stops the world to answer. They are what
sizes the collector's work: how often it runs, what it costs, and the heap
it runs against. The live heap counts the 8 MB heap floor ("The runtime's
own pauses, and the two variables that move them"), so a broker holding
little reads about 8 MB there.

| Metric | Type | |
|---|---|---|
| `saguin_go_heap_live_bytes` | gauge | Heap the last garbage collection found reachable, the 8 MB heap floor included. From `/gc/heap/live:bytes`. |
| `saguin_go_heap_goal_bytes` | gauge | Heap size at which the next garbage collection starts. From `/gc/heap/goal:bytes`. |
| `saguin_go_gc_cycles_total` | counter | Garbage collections completed since start. **Its rate is the figure to watch**: a small heap collecting dozens of times a second costs a fifth of the broker's CPU, which is what the heap floor is for. From `/gc/cycles/total:gc-cycles`. |
| `saguin_go_gc_cpu_seconds_total` | counter | CPU seconds spent collecting garbage since start, as the Go runtime estimates it. It counts a stop-the-world pause as every processor's time, so it is an upper bound. From `/cpu/classes/gc/total:cpu-seconds`. |
| `saguin_go_gc_assist_cpu_seconds_total` | counter | The part of that the broker's own goroutines spent helping the collector: work done on a client's or a publisher's path instead of its own. From `/cpu/classes/gc/mark/assist:cpu-seconds`. |
| `saguin_go_stack_bytes` | gauge | Memory held for goroutine stacks. Two a connection at most - its reader and, once it has been sent something, its writer - so this is most of what an idle connection costs. From `/memory/classes/heap/stacks:bytes`. |
| `saguin_go_allocated_bytes_total` | counter | Heap bytes allocated since start. From `/gc/heap/allocs:bytes`. |
| `saguin_go_allocated_objects_total` | counter | Heap objects allocated since start. From `/gc/heap/allocs:objects`. |
| `saguin_go_gogc_percent` | gauge | The effective `GOGC`: 100 unless set, and -1 when garbage collection is off. From `/gc/gogc:percent`. |
| `saguin_go_memory_limit_bytes` | gauge | The effective `GOMEMLIMIT`. With none set it reads `math.MaxInt64`, written `9.223372036854776e+18`, which a dashboard can take as no limit. From `/gc/gomemlimit:bytes`. |

### Labels, and where the catalogue stops

**Every label is bounded by something the operator typed.** Channel names,
provider names and bridge names come from the configuration file; MQTT
reason codes are a closed set in the specification. Nothing else qualifies.

**A client id does not qualify**, and this is the line that keeps the
catalogue closed for good. A client chooses its own id, so a metric keyed
by one is a series count chosen by whoever connects - a fleet that
reconnects with a fresh id per boot writes an unbounded number of series
into the operator's monitoring system, which then falls over, some distance
from Sagüin and long after the cause. The same is true of topics and of
filters.

So there is no series per consumer. `saguin_channel_consumers` is
published - a count rides the same aggregate - and it is the denominator
rather than a stand-in: it says a fleet is three hundred, never which of
them is behind.

**That distinction is the whole of what a bounded metric can do here.** One
straggler in three hundred and a single consumer that has stopped read as
the same `consumer_position_min`, and they are a device to replace in the
morning against a page tonight. The count separates those two. It does not
say *how many* are behind, and no bounded metric can: that needs a
threshold nobody can choose on an operator's behalf, or a series per
consumer, which is the line above.

### What the `/v1` routes answer with

**This is the one shape Sagüin promises and versions, so it is written
down.** `/v1` is in the path because these bodies are a contract: a field
here may be added, and a field may not change meaning or quietly disappear -
that is what the next number would be for. A dashboard reading `behind`
today has this paragraph to hold the next release to.

`GET /v1/operations/acl?user=<name>` answers the question `saguin --acl`
answers at a shell: what may this client do, and which entry decided it.
`&client_id=<id>` is needed only to resolve a rule written with `%c`.

```json
{"user": "device-7", "acl_file": "/etc/saguin/acl.yaml",
 "pattern_applied": "device-*", "patterns_matched": ["*", "device-*"],
 "patterns_in_file": ["*", "device-*"], "client_id_allowed": true,
 "grants": [{"role": "sensor", "kind": "channel",
             "subject": "events (iot/+/events/+)", "verbs": ["write"], "denies": []},
            {"role": "sensor", "kind": "broker",
             "subject": "features", "verbs": [], "denies": ["will"]}],
 "grants_withheld": []}
```

**`denies` is on every grant**, `[]` where the rule takes nothing away, so a
consumer ranges over it without checking. Only a `broker: features` rule can
fill it (RFC 0002 "Taking a feature away"), and a denial there holds
whatever the client's other roles allow.

**`grants_withheld` is the rules that grant this pair nothing** because
the name `%u` or `%c` would put in holds `+`, `#` or `/`, which a topic
filter reads as a wildcard or a level (RFC 0002 "Roles, and users matched
by pattern"), as the file writes them; `[]` where none is. Listed rather
than left out, so a rule an operator wrote is never silently absent from
the answer.

**`client_id_allowed` outranks everything beside it**, and is null where no
`client_id` was given. An entry's `client_ids` refuses at CONNECT with
`0x86` - the same code a wrong password gets - before a single rule is
consulted, so a body listing what a pair may publish while that pair cannot
connect at all answers the wrong question. The reader is somebody whose
device is not working.

**`pattern_applied` is the one that decides**, and the others matched and
did nothing: one entry applies, the one spelling the name out most exactly.
That is the only way an `acl_file` can take something away without saying
so, because a shadowed entry is the intended behaviour and cannot be
refused at startup - so it is named here, where somebody is already looking
because a device is not doing what its file appears to say.

**A name nothing matches is granted nothing, and that is an answer rather
than a `404`.** Users are patterns, so there is no register of real names
to check a query against - `printer-3` against a file naming `device-*` is
a legitimate question with the answer "nothing". The three pattern fields
are always present for the same reason, null or empty where there is
nothing to say: a caller reading an empty `grants` can see from
`patterns_matched` whether it asked about a name the file has no opinion
on, and a shape that changed with the answer would have to be branched on
before it could be read.

**A broker with no `acl_file` answers `{"acl_file": null,
"everything_allowed": true}`** rather than an empty grant list, which would
be the JSON spelling of "granted nothing" about a broker where every
authenticated client may do anything.

**The publish limits are not in this body**, although `saguin --acl` prints
them. They are a property of the user rather than of a grant, they are in
the configuration this listener already serves, and repeating them here
would be a second place for them to be wrong.

`GET /v1/operations/config`, `Content-Type: application/json`, is the
configuration this process resolved: the master file and every `!include`
in one document, every default filled in, and each channel carrying a
`configured_in` naming the file it was written in.

```json
{"broker": {"id": "edge-1", "storage": {"default": "local", "providers": {
   "local": {"type": "sqlite", "file_path": "/var/lib/saguin/saguin.db"}}}},
 "channels": {"events": {"type": "append", "filter": "events/#",
   "storage": "local", "configured_in": "/etc/saguin/saguin.yaml"}}}
```

**The resolved values, not the written ones**, which is the whole reason to
ask a running broker rather than read the file: a channel that wrote no
`filter` has `<name>/#` here, and one that named no storage has the
broker-wide default. The configuration file is read once at startup and
never again, so this is what the process is running - editing it underneath
does not change this answer.

**What `SIGUSR1` re-reads is not in here.** The log level is a key of this
document and may have moved since it was rendered; the credential files are
named here by path rather than quoted, and their contents are what the
signal replaces. `/v1/operations/acl` answers from the file the broker is
holding, so that route follows a re-read and this one does not.

**`?section=` narrows it, and repeats compose.** `?section=providers`
answers `{"providers": {…}}`, and `?section=providers&section=channels`
answers with both. The names are `bridges`, `channels`, `limits`,
`listeners`, `operations`, `providers` and `storage` - an operator's
vocabulary rather than the document's tree, which is why `providers` is
one of them although it sits under `broker.storage`. **A name that is
not one of those is `400`** listing the ones that are, rather than an
empty object: a section that answered nothing would read as "this broker
has no providers" to a caller with a typo. A section that is simply
unused answers with an empty object, which is how those two are told
apart.

**This route is the one place a whole configuration comes out**, so what it
cannot carry is worth stating twice: no credential is in it, because none
is in the schema - a password file, an `acl_file` and a bridge's client key
are all paths. It is behind the same credential as every other `/v1` route,
and it can be named on its own in a password file's scope field, so a
scraper can be given `/metrics` without being given this.

**An operator whose entry has no scope field reaches it the day it
exists**, which is worth saying out loud rather than leaving to be found: a
user with no scopes reaches every route, deliberately, so that no password
file has to be migrated. On a deployment that has narrowed nobody, adding
this route widens what every existing operator can read.

`GET /v1/operations/consumers`, `Content-Type: application/json`:

```json
{"channels": [
  {"channel": "events", "next_offset": 75, "consumers": 312, "returned": 100,
   "positions": [
     {"reader": "mqtt:north-17", "offset": 12, "behind": 63,
      "last_seen": "2026-08-28T16:27:27Z"}
   ]}
]}
```

`GET /v1/operations/sessions`, `Content-Type: application/json`:

```json
{"sessions": [
  {"client_id": "north-17", "connected": false, "user": "cohort-north",
   "protocol": 5, "listener": "tcp", "remote": "10.0.0.7:52104",
   "keepalive": 60, "expires_after": 3600, "clean_start": false,
   "subscriptions": 2,
   "partitions": [{"filter": "iot/+/events/+", "count": 3, "indices": [0, 2]}]}
 ], "total": 312, "connected": 310, "offline": 2, "returned": 100}
```

**`partitions` is the one place the client-chosen numbers appear**, and it
is here rather than in a metric because a partition count is declared by
the client: *Labels* keeps every series bounded by something the operator
typed, and this route is not a series. One entry per filter the
session declared a slice on, so the row above reads as "three slices, and
this member holds 0 and 2" (RFC 0003 "Client-declared partitioning").

**A list of objects rather than a map keyed by filter**, so that every key
in this response is a field name Sagüin chose and every client-chosen
string is a value.

The field is **absent for a session that declared nothing**, which is
almost every session, rather than answered as an empty object.

**This is the question the two counts cannot answer.** `saguin_connections`
beside `saguin_sessions_offline` says three hundred devices and two hundred
connected, which is either a rota or a hundred that have stopped calling.
The counts read the same for both; this names them.

**Held sessions come first**, and the list is capped like every other here,
so what falls off the end is the fleet behaving normally. `expires_after` is
the row that matters on one of them: a session nothing is coming back for is
held until it runs out, and that is when the number an operator is watching
will fall on its own.

**It answers nothing about positions.** `/v1/operations/consumers` already
does, keyed by the same client id, and a second route answering it would be
a second answer to one question kept in step with the first for ever - the
rule this document draws around records, applied to a number.

**Nothing here acts.** Hanging a client up is a publish to
`$saguin/sessions/disconnect`, gated by the ACL like every other thing a
client may do (RFC 0002 "Hanging up a client"), because whether somebody may
hang up a client is an authorization question and the broker answers those
in exactly one place. A verb on this listener would need a second answer to
it.

`GET /v1/operations/users` is who may connect:

```json
{"users": ["device-7", "gateway-1"], "anonymous_allowed": false,
 "listeners": {
   "tcp":  {"users": ["device-7", "gateway-1"], "anonymous_allowed": false,
            "certificate": "required"},
   "unix": {"users": ["local-agent"], "anonymous_allowed": true,
            "certificate": "none"}}}
```

**The names are what this broker is admitting**, taken from the files it
loaded at startup rather than read again - so the answer cannot disagree
with who actually gets in, which a re-read would the moment somebody edited
a file under a running process.

**The names alone are never the answer, which is why two fields travel with
them.** A door admitting anonymous clients lets in names that are in no
file at all. A door requiring a client certificate is wrong in both
directions at once: none of the names listed can connect, because they hold
passwords and the handshake wants a certificate, and whoever the authority
signed can, with no password and no entry anywhere. A body carrying only
the names would read "closed to all but these two" about a door standing
open to a certificate authority.

**`certificate` is `required`, `accepted` or `none`** - respectively a door
nothing reaches without one this broker verified, one that checks a
certificate where it is offered and takes a password otherwise, and one
that examines none. A Unix socket is always `none`: it carries no TLS, and
its file permissions are what decide who may reach it. What this route
cannot do is list who holds a certificate, because Sagüin does not know -
the authority mints those. Naming the kind of door is the honest answer,
and it is the one an operator needs to tell "these names, by password" from
"whoever your CA signed".

**`listeners` carries every configured door**, each with its own complete
answer. A rule listing only the doors that override something reads well
and is a trap: a door writing `allow_anonymous: true` and no password file
of its own would be absent under it, and the reader sent to the fields
above - which say the opposite about that door. A row per door has no
fallback to get wrong.

**These are the clients, never the operators.**
`broker.operations.password_file` names who may read this broker and which
routes each of them reaches, and handing that out over the interface it
guards widens what one leaked credential is worth. No password or hash
appears on any route, and none can: what is held here is a list of names.

`GET /v1/operations/queues/<channel>`:

```json
{"channel": "jobs", "unresolved": 101, "returned": 100,
 "records": [
   {"offset": 4, "topic": "iot/depot/work/w1", "attempts": 2,
    "state": "leased", "holder": "north-19",
    "lease_until": "2026-08-28T16:28:02Z",
    "first_seen": "2026-08-28T16:20:00Z", "last_seen": "2026-08-28T16:27:44Z"}
 ]}
```

`unresolved` is the total; `returned` is how many rows this body
carries, and it stops at a hundred. **The two numbers are separate on
purpose**: a body that returned a hundred rows and said nothing about
the total would read as a queue holding a hundred, and the difference
between that and ten thousand is the whole question being asked. `state` is
`waiting`, `delivering` - handed to a worker whose `PUBACK` has not arrived,
RFC 0003's `DELIVERING` - or `leased`, and `holder` and `lease_until` are
present only when it is `leased`.

**No payload appears in either body**, on any store - the queue route never
selects one. An operator is asking which records are stuck and what has
been tried, and a body carrying the records themselves would put a fleet's
data through an HTTP endpoint that exists to be scraped and logged.

A `reader` is prefixed by what kind of consumer it is, so a durable MQTT
session and a bridge are told apart rather than sharing a namespace.

`GET /v1/operations/refused` answers which clients this broker refused, at
CONNECT or by ending the connection, on any protocol, and why:

```json
{"clients": [
   {"client_id": "tasmota-c4f1", "user": "fleet", "reason": "topic name invalid",
    "count": 47, "last_seen": "2026-08-31T09:14:02Z"}
 ],
 "returned": 1, "tracked": 1, "beyond": 0}
```

**What these two cover is every connection this broker refused or
ended**, whether or not the client could be told why - deliberately
wider than the hung-up-on case alone. A device turned away at the door
for its protocol version is refused `0x01`, and *is* told, correctly,
in its own vocabulary; but the operator's question does not change with
how well the device was answered. Somebody who has just set
`min_protocol_version: "5"` is turning a fleet away on purpose, and
that is exactly the moment they need the list: it is how they find the
devices nobody remembered to upgrade.

**A connection refused before its CONNECT named a client** - bytes that
are not MQTT, a CONNECT that cannot be read, a zero-length client id asked
to keep a session - is counted like any other, and listed under an empty
`client_id`, with the user name where the CONNECT got that far.

**Which means Sagüin has to be told, and the telling takes a hook.** A
CONNECT refused for its version is refused before any Sagüin hook runs,
so without one the broker never learns which device it was - the only
trace is a line in its listener wrapper with no client id.
`OnConnectRefused` is that hook, and it sits in the MQTT engine rather
than in Sagüin's own code because the refusal happens there: by the time
Sagüin could ask, the connection is already gone.

**A counter says how much and never which one**, and that is why this is a
route rather than a label. `saguin_connections_refused_total` shows a rate
that will not come down; the next question is which of three hundred
devices, and a metric must never answer it - a client id is a string a
client chose, so a series keyed by one is a series count chosen by whoever
connects.

**Worst first, and the cap is the same hundred**, which is what makes the
cap safe: the rows that fall off the end are the ones nobody was looking
for. `returned` is how many rows this body carries and `tracked` how many
the broker is holding, for the reason the queue route gives.

`GET /v1/operations/position-lost` answers which readers the retention
floor passed, and how much each of them lost:

```json
{"readers": [
   {"reader": "mqtt:truck-114", "channel": "events", "kind": "consumer",
    "reported": false, "count": 31, "records_missed": 18402,
    "last_position": 41288, "last_floor": 42977,
    "last_seen": "2026-09-23T08:21:45Z"}
 ],
 "returned": 1, "tracked": 1, "beyond": 0}
```

**It is the same rule as the route above, applied to the loss invariant 1
is about.** `saguin_channel_position_lost_total` says a channel passed its
readers two thousand times; the operator's next question is which devices
have a hole in their history and how big it is, and that is a route rather
than a label for the reason given there.

**Rows are keyed by reader and channel**, not by reader alone. A device
reading three channels can be passed on all three, at different positions
and for different amounts.

**The reader carries its scheme**, `mqtt:` or `bridge:`, as it does on
`/v1/operations/consumers` and in the store itself. It is not decoration: a
client may call itself `bridge:head-office`, MQTT putting almost no rule on
a client id, and without the scheme that device and the outbound rule of
that name would share a row - the counts summed, the kind whichever was
passed last, and the link's missing records handed to a device.

**`records_missed` accumulates and the positions do not.** How much a
reader has lost altogether is the question, and it is the order the rows
come back in; `last_position` and `last_floor` are the most recent passing
and explain that row rather than the total. Where `count` is 1 they agree
exactly - `records_missed` is `last_floor - last_position` - and above 1
they do not, because the floor overtakes a lagging reader repeatedly.
`count` counts those occurrences, never readers.

**`kind` and `reported` are what an operator triages on**, and the three
kinds are not equally serious:

| `kind` | `reported` | |
|---|---|---|
| `consumer` | `false` | Overtaken while connected and reading. MQTT gives no way to tell it, so the operator is the only party who will ever know - the case invariant 1 is written around |
| `session` | `true` | Its stored position had already been passed when it came back. It is told, with Session Present = 0, so it knows to start fresh rather than reading on over the hole |
| `bridge` | `false` | An outbound rule's position. The rule resumes at the floor and the peer across the link is told nothing |

A `reported` of `false` is the row to look at first: nothing downstream
knows those records are missing, so nothing downstream will ask for them
again. **It answers for every passing the row aggregates, not the most
recent one** - a reader passed while connected and passed again while away
reads `false`, because the first hole was never mentioned to anybody. A
`session` row can therefore carry `reported: false`, and that is not a
contradiction: the reader was told about the passing it came back to and
not about the one before it. `kind` is the most recent passing's.

**The route and the counter reconcile.** Summed over a channel's rows,
`count` equals `saguin_channel_position_lost_total{channel}` exactly, while
`beyond` is `0` and within one process lifetime - two instruments on the
same events, which is what makes either of them checkable. Once `beyond`
moves they part company and stay parted: a displaced row takes its `count`
with it, and `beyond` records that a reader did not fit rather than how many
passings went with it.

**Held in memory, and emptied by a restart**, like the refusal record and
unlike the stored positions themselves. The counter it stands in front of
is a process-lifetime number too, and a record that outlived it could never
be reconciled against it again. The durable record is the log, which writes
a line at every one of these sites.

**Worst first by `records_missed`, the same hundred returned and the same
thousand held**, with `beyond` counting what never fitted. When the record
is full a newcomer displaces the reader that has lost least, and only if it
has lost more; when every reader held has lost more than the newcomer, the
newcomer is what does not fit. Either way `beyond` moves, because a reader
shown everything the broker kept and not told that it kept less than it saw
believes it has seen the fleet.

**`beyond` is the one that is easy to leave out.** The record itself is
bounded - client ids arrive from strangers, and a fleet taking a fresh one
every boot would otherwise write an unbounded map into a broker that runs
on one small box (invariant 13). So is each string in it: a client id and a
user name are kept to `limits.max_topic_length`, the bound a log line keeps
them to, because a CONNECT carries up to 65,535 bytes of each and the
record outlives the connection. When it is full a newcomer displaces a
client that has been refused exactly once, never one that has been refused
twice, so a device that starts flapping later is still seen and what
is evicted is always a one-off; when every entry is a repeat offender the
newcomer is refused instead. Either way `beyond` counts what did not fit,
because a reader shown everything the broker kept and not told that it kept
less than it saw believes it has seen the fleet.

### What is deliberately not measured

**Nothing the substrate counts about a path Sagüin does not take.** Two of
its numbers are wrong here rather than merely irrelevant, and both for the
same shape of reason: Sagüin does the work itself and the substrate's
counter is only half told.

Its retained count is one - Sagüin holds its own retained store and empties
the substrate's as fast as it fills, so that number is about a store nobody
has anything in. `saguin_retained_messages` reports Sagüin's instead.

**Its in-flight count is the other, and there is no Sagüin version.** The
substrate raises that number when it sends a QoS 1 publish and lowers it on
the acknowledgement. Sagüin sends channel records itself, putting them into
the client's in-flight set without raising the count - and the
acknowledgement lowers it all the same. Measured on a broker that had
delivered three records and had them acknowledged: **-3**. A gauge that
goes negative is a wrong number, and a wrong number is worse than an absent
one. What a slow broadcast subscriber costs is answered instead by
`saguin_deliveries_refused_total` and `saguin_deliveries_dropped_total`, and
what a slow `latest` subscriber costs by `saguin_latest_superseded_total` -
counters, which only ever rise.

**No histograms, and no latency percentiles.** A timer on the publish path
is exactly the cost this document exists to refuse: Sagüin's job is MQTT,
and instrumenting the hot path to observe it is the trade the whole design
declines. Counters and gauges only. If a mean is ever wanted, a count and a
total give one for two atomic adds and no buckets.

**No payloads, ever, anywhere in this catalogue.** A metric is a number,
and reading records is MQTT's job - the last section says why.

**And nowhere else.** These numbers have one surface. Two of them telling
the same story is two things to keep in step and one of them silently
going wrong, which is the reasoning that already refused a third on-disk
format and is why there is no `$SYS` tree below.

## Logging, and the process

**Sagüin writes its log to standard output and nowhere else.** It opens no
file, rotates nothing, and deletes nothing. Whatever started it is what
decides where the output goes and how much of it is kept.

**There is no `log_file`, and that is a decision rather than a gap.**
Invariant 13: everything that accumulates is bounded, with defined behaviour
at the bound. A log file accumulates, and it would be the only such thing in
the configuration with no bound written beside it - every channel, every
provider and every session has one. Bounding it properly means a size, a
retention count, compression and deletion, which is logrotate reimplemented
inside a broker, with clock and timezone edges and a broker deleting files it
did not open. A dated filename is not an escape: it splits the growth across
many files without bounding the total.

An external `logrotate` rule covers that, so adopting the key would buy the
same gap *and* still need the external tool.

**Where the bound actually is, per supervisor:**

| Runs under | Bounded by | Default |
|---|---|---|
| systemd → journald | `SystemMaxUse` in `journald.conf` | about 10% of the filesystem, capped at 4G - bounded out of the box |
| Docker, compose | `max-size` and `max-file` on the logging driver | **unbounded** - must be set |
| runit → svlogd, daemontools → multilog | the logger's own size and count | bounded |
| `saguin >> file` | nothing | unbounded, and rotating it under the process loses lines |

That last row is the cost of declining the key, and it is written here rather
than left to be discovered. The answer is the init system, and every
Raspberry Pi OS ships systemd.

**No logging to a topic.** In a broker with channels a log line about a
delivery can cause a delivery.

### The runtime's own pauses, and the two variables that move them

**A broker at full tilt can be paused by its own runtime**, and an operator
watching tail latency should know where that comes from. Go collects
garbage while the program runs, and a goroutine holding Sagüin's broker-wide
lock can be made to help with that collection while it holds it. Everything
waiting on that lock waits for the collection, so what an operator sees is
not a slow consumer but unrelated clients - a subscribe, a disconnect, the
health probe - taking longer than usual while the broker is busy.

Measured, on the Ryzen 7 260 at about 200,000 deliveries a second to five
thousand subscribers of one channel: the longest hold of the lock was 96ms,
of which all but 30ms went with `GOGC=off`. The subscribers ran in the
broker's own process, so the collection that held the lock was of their
garbage as well as the broker's; a broker serving clients over the network
collects only its own.

**`GOGC` and `GOMEMLIMIT` are the two knobs, and they are the Go runtime's
rather than Sagüin's** - environment variables set where the process is
started, no rebuild and no key in the configuration file. A larger `GOGC`
collects less often, which makes those pauses rarer; `GOMEMLIMIT` is what
stops that costing unbounded memory, by giving the runtime a ceiling to
collect against. `GOGC=400 GOMEMLIMIT=6GiB` is the shape of it, with the
ceiling chosen from the machine and what the deployment holds - RFC 0004's
figures for the stores, plus what the sessions and in-flight windows come to.
What the collector is doing is on `/metrics`: `saguin_go_gc_cycles_total`,
its CPU and the heap it runs against ("The Go runtime" in the catalogue).
Sagüin also holds a fixed 8 MB minimum-heap allowance, so a broker with few
connections does not collect many times a second: it costs about 1 MB of
resident memory at rest and up to 10 MB under load, it counts toward
`GOMEMLIMIT` like any heap, and with `GOGC` raised it matters even less.

**It tunes a probability, not a guarantee**, and it moves memory rather than
removing a bound: with `GOGC` raised, the bound on what Sagüin's process
holds is the one the operator sets in `GOMEMLIMIT`, and a ceiling not set is
a ceiling not there. Sagüin's own bounds - a channel's `max_bytes`, a
session's queue, an in-flight window - are unaffected either way, and they
remain where the broker's own limits are written.

### `broker.pid_file`

**A pid file is offered where a log file is not, and the difference is
that it cannot grow**: it holds one number and is replaced. RFC 0002
"Where the process writes its id" has the key and its rules.

### `SIGUSR1` re-reads the log level

**And the credential files, the `ws` listener's `same_origin` and
`allowed_origins`, and every TLS listener's certificate and client CA,
which are the only other things this signal touches** - each because a
restart on a broker holding durable sessions costs every connection and
every in-flight record (RFC 0002 "How much the broker says" and
"Withdrawing a device's access").

**The signal says what it did not do.** It logs the level it applied, the
`same_origin` it applied and how many sites `allowed_origins` now lists,
the credential files it re-read and what they now hold, each certificate
it re-read and when that one expires, and states that addresses and
storage are startup-only - so an operator who edited `max_message_size`
and sent the signal sees that it was not applied rather than believing it
was.

**An invalid or unreadable file changes nothing and does not stop the
broker.** The operator may be mid-edit, and a signal that took a running
broker down over a half-saved file would be worse than no signal.

**`SIGHUP` is caught and does nothing**, at `warn`, saying there is no
configuration reload and what `SIGUSR1` re-reads instead - RFC 0002 "How
much the broker says" has why an unhandled `HUP` would be worse than a
no-op and why a silent no-op would be worse than a line.

**There is no `SAGUIN_LOG_LEVEL`.** A live signal makes an environment
variable a third way to set one thing, and a precedence table nobody needs.

## There is no `$SYS` tree

**Sagüin publishes nothing under `$SYS`.** Broker statistics come from
`/metrics` and from nowhere else. There is no configuration key for this,
because there is no feature to switch on.

Three reasons, in order of weight.

**It cannot be put behind a credential.** Everything else Sagüin tells an
operator sits on an authenticated listener. `$SYS` is read over MQTT by
any client that can connect, and MQTT's authorization is topic access - so
granting it means granting it to devices, and refusing it means a
subscription refusal a fleet client meets at runtime. There is no third
setting. `/metrics` asks who is asking; a topic cannot.

**It costs on a timer rather than on demand.** Such a tree is refreshed on
an interval from startup to shutdown, reading process memory statistics
and republishing a retained message per counter, whether or not one client
has ever subscribed. Everything else in this document is paid for when
somebody asks for it, and on a box whose job is MQTT that difference is
the point.

**Two surfaces for one set of numbers is two things to keep in step**, and
one of them going quietly wrong. The catalogue above is the interface.

**The substrate underneath Sagüin publishes such a tree of its own, and
Sagüin silences it.** Leaving it would be worse than either answer,
because two of its counters are wrong about Sagüin rather than merely
irrelevant: its `version` names the substrate, and its `retained` counts a
store Sagüin empties as fast as it fills, so it reports the tree's own
entries and never Sagüin's. A dashboard reading a wrong number is worse
off than one reading nothing, and the first thing anybody points at a new
broker is `$SYS/broker/version`.

**A subscription to any topic under `$SYS/` is refused with `0x87 Not
authorized`**, rather than granted and then never delivered to: a
subscription that will never carry anything is a promise Sagüin cannot
keep. It applies to a wildcard filter and to a literal topic name alike.

**A publish to `$SYS/` is refused with the same code and is the one
refusal `saguin_publish_refused_total` does not count.** The substrate
answers it before any hook runs, so the packet never reaches Sagüin at all -
which is right, and means there is no log line either: this refusal is
counted nowhere and written nowhere. It is the whole of the exception:
every other refusal in this document passes through the publish path and
is counted there.

Nothing under `$SYS` is part of Sagüin's interface, and nothing there is
to be believed.

## Statistics over HTTP, records over MQTT

**Nothing on this listener returns the contents of a message.** An
operator who may read a channel reads it with an ordinary MQTT client: an
`append` channel, a `latest` channel, a dead-letter channel and broadcast
traffic are ordinary MQTT topics and need nothing from this document. A
live queue is the one that answers differently, and it answers the same
way to everybody - it is consumed only through `$saguin/queue/` followed
by the queue's name, at QoS 1, and every other subscription form is
refused, so a plain `jobs/#` gets a subscription failure rather than
somebody's work.

The reason it is drawn here rather than a route being added: whether a
given person may read a given channel is an authorization question, and
the broker answers it in exactly one place for every client
(invariant 10). A second route reading records would need a second answer
to the same question, kept in step with the first for ever.
