#!/usr/bin/env python3
"""The saguin demo. Every channel type, and the delivery paths a first
reading needs, driven by a stock MQTT 5 client:

    python3 -m venv .venv
    . .venv/bin/activate
    pip install -r examples/requirements.txt
    python examples/demo.py

It stops at every step and waits for ENTER, so that it can be read at the
speed of whoever is reading it and presented to a room at the speed of
whoever is talking.

The prerequisites are Go, to build the broker, and the shared examples Python
environment. The
certificates are checked in under examples/support/certs, so openssl is needed
only to make new ones - examples/README.md has the commands.

With no arguments this is the short, newcomer-friendly tour. ``--full`` adds
the security, partitioning, refusal, retry and dead-letter demonstrations, and
``--section NAME`` runs one deep dive on its own. ``--list`` prints the names.

**The broker it configures is a secured one**, because that is what a
reference should be. examples/saguin.yaml opens all three listeners a
broker can have - TLS on TCP, wss on WebSocket, a Unix socket - behind a
password file and an acl_file. All three files are checked in under
examples/ and this script copies them into place before it starts;
examples/README.md says what each one is. There is no anonymous
half: an acl_file is a statement about an identity, and saguin refuses
one beside a listener that admits clients having none.

**Two things a client library makes visible**, because it can read what many
command-line clients print away:

  * The SUBACK reason code for every refused queue subscription. Common
    command-line MQTT clients do not expose all of these codes; they are
    printed here, per rule.
  * The queue acknowledgement, without a second program. mosquitto_sub can
    receive MQTT 5 Correlation Data and cannot print it; paho can, so a worker
    here is the same client as every other step.

The full tour deliberately leaves out:

  * Retention. It exists - records are removed on age and on size - but
    examples/saguin.yaml sets both to `none`, so nothing is removed in the
    few minutes this runs and no channel's floor ever moves. A
    demonstration that waited for a deadline would be a demonstration of
    waiting.
  * What a consumer is told when retention has passed its position. It is
    told at CONNECT, with Session Present = 0 - but reaching that needs a
    floor to move, which needs a deadline to pass.
  * job_expires_after, for the same reason.
  * Retained messages on a broadcast topic: they work on every broker,
    with or without a `broker.retained` block. What refuses one, with
    0x9A, is an acl_file denying `retained` to the client, and this
    tour's acl.yaml gives that role to nobody.
  * A Last Will, which means killing a client rather than disconnecting
    it: a poor thing to do in a script somebody is reading along with.
  * proxy_protocol on the Unix listener, which refuses every connection
    arriving without a PROXY v2 header - so demonstrating it needs a
    proxy in front of the socket, and there is not one here.
  * Bridging to an upstream broker, which needs a second broker. This
    tour is one.

It runs against examples/saguin.yaml unchanged, which is the same file
`make demo` uses. That file names absolute paths under /tmp, so this is
portable across Unix rather than across every platform; giving it its own
generated configuration would make the demo stop exercising the shipped
one, which is most of what it is for.
"""

import argparse
import os
import re
import shutil
import socket
import ssl
import signal
import sqlite3
import subprocess
import sys
import threading
import time

# **Guarded, so that `--credentials` needs no pip install.** `make demo-server`
# runs the broker on its own and the configuration will not load until the
# certificates, the password file and the acl_file are where it names them
# - so it calls this script for that and nothing else.
try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.properties import Properties
except ImportError:
    dependency_free = {"--credentials", "--list", "--help", "-h"}
    if not dependency_free.intersection(sys.argv):
        raise SystemExit(
            "paho-mqtt is not installed; run:\n"
            "  python3 -m venv .venv\n"
            "  . .venv/bin/activate\n"
            "  pip install -r examples/requirements.txt"
        )

# The broker's three doors, all open at once, because they are not
# variations on one thing: a fleet arrives over TLS, a browser can only
# arrive over WebSocket, and a tool on the same box needs no port at all.
#
# HOST is "localhost" rather than 127.0.0.1 and that is not cosmetic: the
# certificate carries subjectAltName DNS:localhost,IP:127.0.0.1, and a
# client that verifies properly checks the name it dialled against it.
HOST, PORT = "localhost", 8883
WS_PORT = 8083

# **The topic layout the tour drives**, matching examples/saguin.yaml. One
# `iot/` hierarchy divided by its third level, which is the thing a channel
# claiming a first topic level could not express: a site holds an event
# log, current state, presence and work at once.
#
#   iot/depot/events/<thing>     append    replayed from a position
#   iot/depot/state/<thing>      latest    current value per topic
#   iot/depot/presence/<thing>   latest    who is here
#   iot/depot/work/<thing>       queue     one worker at a time
#   iot/depot/health/<thing>     -         broadcast: no filter claims it
#
# None of the filters ends in `#`, so each matches four levels and no more.
# A five-level topic under the same tree belongs to nobody, which the tour
# publishes on purpose.
SITE = "iot/depot"

# The queue's name, which is all a worker needs: it subscribes to
# `$saguin/queue/` plus this, the same name the seek topic, the response
# topic and the dead-letter channel are made of.
QUEUE = "jobs"

# And the filter the operator wrote for it, which is only used here to show
# the forms a queue refuses - one of them being this very string.
QUEUE_FILTER = "iot/+/work/+"

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = os.path.join(ROOT, "examples", "saguin.yaml")
STATE = "/tmp/saguin-demo"
DB = os.path.join(STATE, "saguin.db")
LOG = "/tmp/saguin-demo.log"
SOCKET = os.path.join(STATE, "saguin.sock")
PID_FILE = os.path.join(STATE, "saguin.pid")

# Everything the broker needs before it will start. All of it is checked
# in under examples/ - read examples/README.md for what each file is - and
# copied to STATE before the broker starts, because saguin refuses a
# relative path for any of them and no absolute path into a repository is
# true on two machines.
EXAMPLES = os.path.join(ROOT, "examples")
SHIPPED_TLS = os.path.join(EXAMPLES, "support", "certs")
TLS_DIR = os.path.join(STATE, "tls")
CA = os.path.join(TLS_DIR, "ca.pem")
PASSWD = os.path.join(STATE, "clients.passwd")
ACL = os.path.join(STATE, "acl.yaml")

# The tour's own credential. Every step below authenticates, because an
# acl_file is a statement about an identity and saguin refuses one beside
# an anonymous listener - so with authorization configured at all, there
# is no anonymous half of this demo to show.
USER, PASSWORD = "demo", "hunter2"

# A device that has no password at all: it is named by its certificate's
# Common Name, and that name is what its acl_file rule is written about.
DEVICE = "device-7"
DEVICE_CERT = os.path.join(TLS_DIR, f"{DEVICE}.pem")
DEVICE_KEY = os.path.join(TLS_DIR, f"{DEVICE}-key.pem")

BOLD = DIM = OFF = ""


class DemoFailure(RuntimeError):
    """A failed demonstration claim, phrased for the person running it."""


def configure_output(no_pause=False):
    """Use terminal affordances only when there is a terminal to receive them."""
    global BOLD, DIM, OFF, PAUSE
    colour = (
        sys.stdout.isatty()
        and os.environ.get("TERM") != "dumb"
        and "NO_COLOR" not in os.environ
    )
    if colour:
        BOLD, DIM, OFF = "\033[1m", "\033[2m", "\033[0m"
    PAUSE = not no_pause and os.environ.get("SAGUIN_DEMO_PAUSE", "1") != "0" and sys.stdin.isatty()


def say(text):
    print(f"\n{BOLD}== {text}{OFF}")


def note(text=""):
    print(f"   {DIM}{text}{OFF}")


# Pausing needs somebody to press the key. Piped or redirected, there is
# nobody, and waiting for one is a crash rather than a pause - the same
# rule the saguin-python tour follows, under the same variable.
PAUSE = False


def wait_user():
    global PAUSE
    if not PAUSE:
        return
    try:
        input("Press [ENTER] to continue...")
    except EOFError:  # somebody closed the input; walk the rest of it
        PAUSE = False


def require(condition, message):
    """Stop at the first point where the observed result contradicts the tour."""
    if not condition:
        raise DemoFailure(message)


def code_value(code):
    return int(code.value) if code is not None else -1


# --- the client ---------------------------------------------------------
#
# One small wrapper, because every step needs the same four things: connect
# as MQTT 5 under a client id it was given, say what came back, collect what
# arrives, and go away cleanly.
#
# Every subscriber connects with a client id it was given rather than one
# the library invents, and says which before it connects. The client id is
# what the broker names on every log line about a client and what a durable
# session is filed under, so a tour whose subscribers are called
# "auto-6A1F..." cannot be read against the broker's log.


ACTIVE_CLIENTS = set()


class Client:
    """One MQTT 5 client, on whichever of the broker's three doors is asked
    for.

    **The defaults are the tour's ordinary client**: TLS to the TCP
    listener, authenticated out of the password file. Every step needs
    that, because the broker this demo configures has an acl_file, and
    saguin refuses an anonymous listener beside one - authorization is a
    statement about an identity, so there has to be an identity.

    The arguments exist for the four steps that are about the doors
    themselves: `transport="websockets"` for wss, `transport="unix"` for
    the socket, `cert`/`key` for a client that authenticates with a
    certificate and no password, and `password=` wrong on purpose to be
    refused.

    ``expected_connack`` makes the claim executable. A refusal demonstration
    must name the code it expects; unexpectedly accepting it is a failure too.
    """

    def __init__(self, client_id, clean_start=True, session_expiry=None,
                 transport="tcp", user=USER, password=PASSWORD,
                 cert=None, key=None, expected_connack=0):
        self.id = client_id
        self.messages = []
        self.suback = None
        self.session_present = None
        self.connack = None
        self.pubacks = {}
        self._connected = threading.Event()
        self._subscribed = threading.Event()
        self._closed = False
        self.on_message = None

        self.c = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5,
            transport=transport,
        )
        self.c.on_connect = self._on_connect
        self.c.on_subscribe = self._on_subscribe
        self.c.on_message = self._on_message
        # **The PUBACK's reason code arrives here and nowhere else.** The
        # object publish() returns carries paho's own result - whether the
        # packet was queued locally - and a demo that printed that would
        # report Success for every refusal the broker sent. It is the same
        # trap as reading a tool's exit status instead of the code on the
        # wire.
        self.c.on_publish = self._on_publish

        if transport == "websockets":
            self.c.ws_set_options(path="/mqtt")
        if user:
            self.c.username_pw_set(user, password)
        # The Unix socket is the one door with no TLS on it: the file's
        # permissions are its access control, and there is no network hop
        # for a certificate to protect.
        if transport != "unix":
            self.c.tls_set(
                ca_certs=CA, certfile=cert, keyfile=key,
                cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )

        props = Properties(PacketTypes.CONNECT)
        if session_expiry is not None:
            props.SessionExpiryInterval = session_expiry
        where = SOCKET if transport == "unix" else HOST
        # paho parses the port even where the transport ignores it, so the
        # socket is dialled with the TCP port beside it and never uses it.
        port = WS_PORT if transport == "websockets" else PORT
        self.c.connect(
            where,
            port,
            keepalive=30,
            clean_start=clean_start,
            properties=props,
        )
        self.c.loop_start()
        ACTIVE_CLIENTS.add(self)
        if not self._connected.wait(5):
            self.close()
            raise DemoFailure(f"{client_id}: no CONNACK from {where}:{port}")
        got = code_value(self.connack)
        if expected_connack is not None and got != expected_connack:
            self.close()
            raise DemoFailure(
                f"{client_id}: CONNACK {self.connack}; expected 0x{expected_connack:02x}"
            )

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        self.session_present = flags.session_present
        self.connack = reason_code
        self._connected.set()

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        self.pubacks[mid] = reason_code

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        self.suback = reason_codes[0]
        self._subscribed.set()

    def _on_message(self, client, userdata, msg):
        self.messages.append(msg)
        if self.on_message:
            self.on_message(msg)

    def subscribe_slice(self, topic, count, index, qos=1):
        """SUBSCRIBE declaring a slice of what this filter reaches.

        The property rides on the SUBSCRIBE packet rather than on the
        filter, which is where MQTT 5 puts User Properties: one packet
        carries one set of them however many filters it names. A client
        wanting different slices for different filters sends different
        packets. Repeating it is an OR, and every call must name the same
        number of partitions.
        """
        props = Properties(PacketTypes.SUBSCRIBE)
        props.UserProperty = [
            ("saguin-filter", f"topic_hash({count}, {index})"),
        ]
        self._subscribed.clear()
        result, _ = self.c.subscribe(topic, qos=qos, properties=props)
        require(result == mqtt.MQTT_ERR_SUCCESS,
                f"{self.id}: SUBSCRIBE could not be sent ({result})")
        if not self._subscribed.wait(5):
            raise DemoFailure(f"{self.id}: no SUBACK for {topic!r}")
        require(code_value(self.suback) < 0x80,
                f"{self.id}: subscription to {topic!r} was refused with {self.suback}")
        return self.suback

    def subscribe(self, topic, qos=1, expect_refusal=False):
        """Send the SUBSCRIBE and return the reason code the broker answered.

        The reason code is the point. A refusal here is a code and a live
        connection rather than a dropped one, which is what lets a client
        subscribe again correctly - and it is invisible through the
        mosquitto tools, which print nothing for it.
        """
        self._subscribed.clear()
        result, _ = self.c.subscribe(topic, qos=qos)
        require(result == mqtt.MQTT_ERR_SUCCESS,
                f"{self.id}: paho could not send SUBSCRIBE for {topic!r}: {result}")
        if not self._subscribed.wait(5):
            raise DemoFailure(f"{self.id}: no SUBACK for {topic!r}")
        refused = code_value(self.suback) >= 0x80
        require(refused == expect_refusal,
                f"{self.id}: SUBACK {self.suback} for {topic!r}; "
                f"expected {'a refusal' if expect_refusal else 'success'}")
        return self.suback

    def publish(self, topic, payload, qos=1, correlation=None, response_topic=None,
                wait=True, expected_puback=None):
        """Publish, and hand back the reason code the broker answered with.

        A QoS 1 publish is answered per packet, and the answer is where
        every refusal on this path is said out loud: 0x87 for a rule that
        does not grant it, 0x9A where the client's roles deny `retained`.
        Returned rather than printed, so a step decides whether a refusal
        is the point or a failure.

        **`wait=False` is not an optimisation, it is the only thing that
        works from inside a receive callback**, and the reason is worth
        knowing because it made this demo lie about the broker for a
        while. paho runs callbacks on its network loop thread, and
        wait_for_publish blocks until the PUBACK arrives - which only that
        same thread can read. Called from a handler it deadlocks until the
        timeout expires, five seconds later.

        The queue steps are where that bit. A worker answering a job from
        its handler took five seconds to acknowledge the delivery it was
        answering, so every redelivery looked like a visibility timeout,
        and the note beside it said returns were immediate - a claim about
        the broker that the broker had been meeting all along. The measure
        of it: with this fixed, a returned job comes back in the 2s its
        backoff configures rather than in 7.
        """
        props = Properties(PacketTypes.PUBLISH)
        if correlation is not None:
            props.CorrelationData = correlation
        if response_topic is not None:
            props.ResponseTopic = response_topic
        info = self.c.publish(topic, payload, qos=qos, properties=props)
        require(info.rc == mqtt.MQTT_ERR_SUCCESS,
                f"{self.id}: paho could not send PUBLISH for {topic!r}: {info.rc}")
        if not wait:
            return None
        info.wait_for_publish(5)
        require(info.is_published(), f"{self.id}: no PUBACK for {topic!r}")
        deadline = time.time() + 1
        while info.mid not in self.pubacks and time.time() < deadline:
            time.sleep(0.01)
        code = self.pubacks.get(info.mid)
        require(code is not None, f"{self.id}: PUBACK for {topic!r} had no reason code")
        if expected_puback is None:
            require(code_value(code) < 0x80,
                    f"{self.id}: PUBLISH to {topic!r} was refused with {code}")
        else:
            require(code_value(code) == expected_puback,
                    f"{self.id}: PUBACK for {topic!r} was {code}; "
                    f"expected 0x{expected_puback:02x}")
        return code

    def close(self):
        if self._closed:
            return
        self._closed = True
        ACTIVE_CLIENTS.discard(self)
        self.c.disconnect()
        self.c.loop_stop()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


def user_props(msg):
    """The MQTT 5 User Properties on a delivery, as a dict.

    saguin's own metadata rides here - saguin-id, saguin-offset, and the
    saguin-dlq-* set on a dead-lettered record - because they are ordinary
    User Properties rather than an extension, which is what lets any stock
    client read them.
    """
    if msg.properties is None:
        return {}
    return {k: v for k, v in getattr(msg.properties, "UserProperty", [])}


PUBLISHER = None


def pub(topic, payload=""):
    note(f"Publishing: {topic} --> {payload or '(empty payload)'}")
    code = PUBLISHER.publish(topic, payload)
    require(code_value(code) < 0x80,
            f"demo-publisher: {topic!r} was refused with PUBACK {code}")
    return code


def wait_for(done, seconds, what):
    """Poll until something has happened, rather than sleeping and hoping.

    A fixed sleep is a race with a message that has not arrived yet, and it
    fails in the direction that reads as a broken broker: the step reports
    nothing happened and the proof arrives one line later. Worse here than
    cosmetic - a queue step that gives up early leaves jobs half-delivered
    for the next step to be confused by.
    """
    deadline = time.time() + seconds
    while time.time() < deadline:
        if done():
            return True
        time.sleep(0.1)
    raise DemoFailure(f"gave up waiting for {what} after {seconds}s")


def collect(client_id, topic, seconds, qos=1, clean_start=True, session_expiry=None,
            label="got", show_headers=False, expected=None):
    """Subscribe, gather for a few seconds, print what arrived, and go.

    Deliberately one client per step and gone at the end of it, the way the
    one-shot command-line examples run one subscriber per step: one left connected
    is a subscriber the next step's counts include.
    """
    note(f"Subscriber: {client_id}")
    c = Client(client_id, clean_start=clean_start, session_expiry=session_expiry)
    c.subscribe(topic, qos=qos)
    if expected is None or expected == 0:
        time.sleep(seconds)
    else:
        wait_for(lambda: len(c.messages) >= expected, seconds,
                 f"{client_id} to receive {expected} message(s)")
    for msg in c.messages:
        line = f"   {label}: {msg.topic} --> {msg.payload.decode(errors='replace')}"
        if msg.retain:
            line += "  [retained]"
        print(line)
        if show_headers:
            props = user_props(msg)
            if props:
                for k, v in props.items():
                    print(f"      {k} = {v}")
    n = len(c.messages)
    c.close()
    if expected is not None:
        require(n == expected,
                f"{client_id}: received {n} message(s) on {topic!r}; expected {expected}")
    return n


def count(client_id, topic, qos=1, seconds=2, expected=None):
    """How many messages a subscription receives before it gives up waiting."""
    c = Client(client_id)
    c.subscribe(topic, qos=qos)
    if expected is None or expected == 0:
        time.sleep(seconds)
    else:
        wait_for(lambda: len(c.messages) >= expected, seconds,
                 f"{client_id} to receive {expected} message(s)")
    n = len(c.messages)
    c.close()
    if expected is not None:
        require(n == expected,
                f"{client_id}: received {n} message(s) on {topic!r}; expected {expected}")
    return n


# --- the worker ---------------------------------------------------------
#
# A queue worker, and the whole application protocol is in answer(): publish
# the action to the Response Topic the broker supplied, echoing its
# Correlation Data. An ordinary MQTT 5 PUBLISH.
#
# PUBACK is not the acknowledgement. It confirms MQTT moved a packet and
# says nothing about whether the work succeeded, which is why answering is
# a publish of its own.


class Worker:
    # **The pin is the queue's name.** A queue admits one subscription form
    # and MQTT makes one consumer group out of one exact string, so every
    # other spelling is refused rather than made to work. The name is what
    # the seek topic, the response topic and the dead-letter channel are all
    # made of, so a worker needs nothing out of the configuration file.
    def __init__(self, client_id, queue=QUEUE, action="ack",
                 work=0.1, quiet=False):
        self.id = client_id
        self.action = action  # "ack", "return", or "stall"
        self.work = work
        self.quiet = quiet
        self.done = 0
        # Set by a step that wants each attempt timed from a moment it
        # chose - the retry backoff and the visibility timeout are both
        # gaps, and a gap is the one thing a line of prose cannot show.
        self.clock = None
        self.c = Client(client_id)
        self.c.on_message = self._handle
        self.c.subscribe(f"$saguin/queue/{queue}", qos=1)

    def _handle(self, msg):
        props = user_props(msg)
        if not self.quiet:
            at = f"  at +{time.time() - self.clock:.1f}s" if self.clock else ""
            print(
                f"   [{self.id}] got {msg.topic}"
                f"  attempt={props.get('saguin-attempt', '?')}"
                f"  payload={msg.payload.decode(errors='replace')!r}{at}"
            )
        time.sleep(self.work)
        if self.action == "stall":
            if not self.quiet:
                print(f"   [{self.id}] stalling: not answering, the lease will expire")
            return
        rt = msg.properties.ResponseTopic
        corr = msg.properties.CorrelationData
        # wait=False because this runs on paho's loop thread: see
        # Client.publish. Waiting here blocks the thread that has to read
        # the PUBACK, and the job's own delivery goes unacknowledged for
        # five seconds while it does.
        self.c.publish(rt, self.action, qos=1, correlation=corr, wait=False)
        self.done += 1
        if not self.quiet:
            print(f"   [{self.id}] {self.action}  ({self.done} done)")

    def close(self):
        self.c.close()


# --- the broker ---------------------------------------------------------

BROKER = None


def stage_credentials():
    """Put the shipped credentials where the configuration expects them.

    **They are checked in, under examples/, rather than made here.** The
    certificates, the password file and the acl_file are all readable
    beside the configuration that names them - see examples/README.md for
    what each one is and the openssl commands that produced the
    certificates.

    **They are copied to /tmp rather than read where they live, and that
    is saguin's rule rather than a preference.** Every one of these keys is
    refused if it is relative:

        ...tls.cert_file "examples/support/certs/broker.pem" is relative: which
        certificate a listener serves must not depend on where the broker
        was started from

    The same holds for key_file, client_ca_file, password_file and
    acl_file. So a configuration that ships in a repository cannot name
    them where they sit - no absolute path is true on two machines - and
    the demo's own state has always lived under /tmp for exactly that
    reason: examples/support/storage/durable.yaml named
    /tmp/saguin-demo/saguin.db long before any of this was here.

    A real deployment has none of this problem: the files are at
    /etc/saguin, an operator put them there, and the path is written once.
    """
    os.makedirs(TLS_DIR, exist_ok=True)
    for name in ("ca.pem", "broker.pem", "broker-key.pem",
                 "device-7.pem", "device-7-key.pem", "unrelated-ca.pem"):
        shutil.copyfile(os.path.join(SHIPPED_TLS, name),
                        os.path.join(TLS_DIR, name))
    shutil.copyfile(os.path.join(EXAMPLES, "clients.passwd"), PASSWD)
    shutil.copyfile(os.path.join(EXAMPLES, "acl.yaml"), ACL)
    # A password file is a credential list, and saguin refuses one the rest
    # of the machine can read. The repository cannot carry that mode, so it
    # is set on the copy.
    os.chmod(PASSWD, 0o600)


def start_broker():
    """Start saguin and wait until it answers, rather than sleeping and hoping."""
    global BROKER
    with open(LOG, "a") as log:
        BROKER = subprocess.Popen(
            [os.path.join(ROOT, "bin", "saguin"), "-config", CONFIG],
            stdout=log,
            stderr=log,
            cwd=ROOT,
        )
    for _ in range(50):
        if BROKER.poll() is not None:
            raise DemoFailure(
                f"the broker exited with status {BROKER.returncode}:\n{log_tail()}"
            )
        time.sleep(0.2)
        try:
            probe = Client("demo-readiness-probe")
            probe.close()
            return
        except (OSError, ssl.SSLError, DemoFailure):
            continue
    raise DemoFailure(
        f"the broker did not answer on {HOST}:{PORT}; recent log output:\n{log_tail()}"
    )


def log_tail(lines=12):
    try:
        with open(LOG) as f:
            return "".join(f.readlines()[-lines:]).rstrip()
    except OSError as err:
        return f"(cannot read {LOG}: {err})"


def admitted_protocols():
    """What this broker admits, read back from its own startup line.

    Restated here, it went stale: this tour spent a day telling readers
    that "MQTT 5 only - a 3.1.1 client is rejected at CONNECT" after the
    broker had started accepting one, because the sentence was written
    when that was true and nothing made it move with the gate. Derived
    from the running broker it cannot say the wrong thing, and that is
    the same reason the startup line itself reads the server's gate
    rather than the configuration file.
    """
    with open(LOG) as f:
        for line in f:
            found = re.search(r'protocols="([^"]*)"', line)
            if found:
                return found.group(1)
    raise DemoFailure(
        f"the broker's startup line named no protocols; see {LOG}. This tour "
        "reads that line rather than restating it, so if the line changes "
        "shape the tour has to stop rather than quietly print nothing."
    )


def stop_broker(sig=signal.SIGTERM):
    global BROKER
    if BROKER is None:
        return
    if BROKER.poll() is None:
        BROKER.send_signal(sig)
        try:
            BROKER.wait(10)
        except subprocess.TimeoutExpired:
            BROKER.kill()
            BROKER.wait(5)
    BROKER = None


def close_clients():
    """Best-effort cleanup for a scenario that failed between client steps."""
    for client in list(ACTIVE_CLIENTS):
        try:
            client.close()
        except Exception as err:
            note(f"could not close {client.id}: {err}")


def preflight():
    """Refuse to trample another demo or an unrelated service."""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)
        except (ProcessLookupError, ValueError):
            pass
        except PermissionError as err:
            raise DemoFailure(f"cannot inspect the demo pid file/process: {err}") from err
        else:
            raise DemoFailure(
                f"another Saguin demo appears to be running as pid {pid}; stop it first"
            )

    for port in (PORT, WS_PORT):
        probe = socket.socket()
        try:
            probe.bind(("127.0.0.1", port))
        except OSError as err:
            raise DemoFailure(f"demo port 127.0.0.1:{port} is unavailable: {err}") from err
        finally:
            probe.close()


def peek(why, sql):
    """Read the broker's SQLite database from another process while it runs.

    Read-only, so it cannot disturb what it is looking at - and it is
    looking at the live file rather than a copy, because in WAL mode the
    records are in saguin.db-wal and a copy of saguin.db alone is a
    database with no tables in it (RFC 0004).
    """
    print(f"   {DIM}Another process, read-only, while the broker runs:{OFF} {why}")
    try:
        with sqlite3.connect(f"file:{DB}?mode=ro", uri=True) as con:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    except sqlite3.Error as err:
        raise DemoFailure(f"could not inspect the broker database: {err}") from err
    widths = [
        max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c)
        for i, c in enumerate(cols)
    ]
    print("   " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("   " + "-+-".join("-" * w for w in widths))
    for r in rows:
        print("   " + " | ".join(str(v).ljust(w) for v, w in zip(r, widths)))


# --- the tour -----------------------------------------------------------


def overview():
    say("Saguin in one screen: ordinary MQTT, four delivery semantics")
    note("Publishing never changes. The topic selects what the broker promises:")
    note("")
    print("   broadcast  live subscribers only; nothing is stored")
    print("   append     every record, replayed independently per consumer")
    print("   latest     one current value per topic")
    print("   queue      one worker at a time, until it acknowledges the job")
    note("")
    note("Storage is a separate choice: it decides what survives a restart, not")
    note("what any of those four delivery models mean.")
    wait_user()


def recap():
    say("Recap")
    print("   broadcast  who is listening now?")
    print("   append     what happened, in order?")
    print("   latest     what is true now?")
    print("   queue      what work remains unresolved?")
    note("Every operation above was a normal MQTT publish or subscribe from Paho;")
    note("the topic and the broker configuration supplied the semantics.")


def security():
    say("Security: three doors, two ways to be named, one rule set")
    note("Nothing below is a separate broker or a second configuration. It is")
    note("the one broker examples/saguin.yaml describes, which opens all three")
    note("listeners at once and authenticates every one of them.")
    wait_user()

    say("1. a password, and what a wrong one is answered with")
    note("The broker names a password_file, so only the clients in it get in.")
    note(f"It was made a moment ago with: saguin --passwd add <file> {USER}")
    note("")
    note("A refusal first, because a refusal somebody has seen is worth more")
    note("than a paragraph saying refusals happen:")
    bad = Client("wrong-password", user=USER, password="not-the-password",
                 expected_connack=0x86)
    note(f"   CONNACK: {bad.connack}")
    bad.close()
    note("")
    note("0x86 is what a WRONG password and a MISSING one both get, on purpose:")
    note("distinguishing them would tell an unauthenticated caller which user")
    note("names exist.")
    missing = Client("no-credential-at-all", user=None, expected_connack=0x86)
    note(f"   no user name at all -> CONNACK: {missing.connack}")
    missing.close()
    note("")
    good = Client("right-password")
    note(f"   the right one       -> CONNACK: {good.connack}")
    good.close()
    wait_user()

    say("2. TLS, and the check that a client actually makes")
    note(f"The TCP listener is {HOST}:{PORT} with cert_file and key_file, so every")
    note("connection above was already encrypted. What is worth seeing is the")
    note("half a client is responsible for: verifying the broker it reached.")
    note("")
    note("The certificate carries subjectAltName DNS:localhost,IP:127.0.0.1 -")
    note("a Common Name alone fails modern verification, and that is where a")
    note("first attempt at TLS usually stops.")
    note("")
    note("A client trusting a DIFFERENT authority cannot complete the handshake.")
    note("examples/support/certs/unrelated-ca.pem is exactly that: an")
    note("authority with no relationship to this broker's, shipped so this")
    note("step proves the check rather than describing it.")
    other = os.path.join(TLS_DIR, "unrelated-ca.pem")
    stray = mqtt.Client(CallbackAPIVersion.VERSION2, client_id="wrong-ca",
                        protocol=mqtt.MQTTv5)
    stray.tls_set(ca_certs=other, cert_reqs=ssl.CERT_REQUIRED,
                  tls_version=ssl.PROTOCOL_TLS_CLIENT)
    try:
        stray.connect(HOST, PORT, keepalive=5)
        raise DemoFailure("a client trusting the unrelated CA completed the TLS handshake")
    except Exception as err:
        if isinstance(err, DemoFailure):
            raise
        note(f"   {type(err).__name__}: {err}")
    finally:
        stray.disconnect()
    note("")
    note("No CONNACK and no reason code: this one is refused below MQTT, by TLS")
    note("itself, which is why it reads as an exception rather than as a code.")
    wait_user()

    say("3. mutual TLS: a device with a certificate and no password at all")
    note(f"client_ca_file names the authority that signed {DEVICE}'s certificate,")
    note("and require_certificate is false - the mixed mode a fleet migrating in")
    note("batches needs. With a certificate you are named by it; without one you")
    note("fall through to the password file. Both on the same listener.")
    note("")
    note(f"{DEVICE} is in no password file. Its certificate says CN={DEVICE},")
    note("and saguin makes that the client's user name.")
    device = Client(DEVICE, user=None, cert=DEVICE_CERT, key=DEVICE_KEY)
    note(f"   CONNACK: {device.connack}  - with no user name and no password")
    note("")
    note("That it connected at all is the proof the certificate was sent: with")
    note("no certificate and no password there is nothing to authenticate, and")
    note("the step above shows exactly what that gets - 0x86.")
    wait_user()

    say("4. the acl_file, and %u")
    note("Authorization is a statement about an identity, so it needs one: an")
    note("acl_file requires a password_file, and saguin refuses allow_anonymous")
    note("beside it - on the broker or on any single listener. That is why the")
    note("Unix socket below authenticates too rather than trusting its own")
    note("file permissions.")
    note("")
    note(f"{DEVICE}'s whole role is one rule:")
    note("")
    note("    - topic: iot/+/health/%u")
    note("      allow: [write, read]")
    note("")
    note("%u is that client's identity - here taken from the certificate alone,")
    note("with no password anywhere in it. One rule, any number of devices.")
    note("")
    note("A `topic:` rule governs broadcast, so its filter may not lie inside a")
    note("channel - startup refuses one that does. 'iot/+/health/+' is claimed")
    note("by nothing, which is exactly what makes it writable here.")
    for topic, expected in (
        (f"{SITE}/health/{DEVICE}", 0x00),
        (f"{SITE}/health/device-8", 0x87),
        (f"{SITE}/events/x", 0x87),
    ):
        rc = device.publish(topic, "x", expected_puback=expected)
        note(f"   publish {topic:28} -> PUBACK {rc}")
    note("")
    note("Its own topic is granted; another device's is 0x87, and so is the")
    note("append channel, which its role never mentions. The tour's own user")
    note("holds a role that does name those channels, which is why every step")
    note("after this one works.")
    wait_user()

    say("5. what a client is allowed, without guessing")
    note("Roles cost indirection: with a flat list 'why can this device not")
    note("publish?' is a line in a file, and with roles it is a pattern match")
    note("and two lookups done in somebody's head. So the resolved form prints:")
    note("")
    note("A fleet's worth of ids goes in the same way and comes back one line")
    note("each, five tab-separated columns, %u already substituted:")
    note("")
    note("    printf 'device-7\\ndevice-8\\n' | saguin --acl saguin.yaml")
    note("")
    note("and 'saguin --route saguin.yaml < topics.txt' does the same for")
    note("topics: four columns saying which channel holds each one, and why.")
    print()
    done = subprocess.run(
        [os.path.join(ROOT, "bin", "saguin"), "--acl", CONFIG, DEVICE],
        capture_output=True, text=True, cwd=ROOT,
    )
    require(done.returncode == 0,
            f"saguin --acl failed:\n{done.stderr.rstrip()}")
    for line in done.stdout.rstrip().splitlines():
        print(f"   {line}")
    note("")
    note("Note the rule printed resolved rather than as written: %u is gone and")
    note(f"the device's own name is in its place. A %u left in that output would")
    note("be a grant the broker will never match.")
    device.close()
    wait_user()

    say("6. the other two doors: WebSocket and a Unix socket")
    note("A broker takes one listener of each kind - tcp, ws, unix - and this")
    note("configuration opens all three. Same broker, same rules, same")
    note("credentials; only the framing and the address differ.")
    note("")
    note(f"WebSocket over TLS on {HOST}:{WS_PORT}, which is wss rather than ws. It is")
    note("the only way a browser reaches a broker at all - a page cannot open a")
    note("TCP socket, and a page served over https refuses a plain WebSocket.")
    browser = Client("a-browser", transport="websockets")
    note(f"   CONNACK: {browser.connack}")
    rc = browser.publish("iot/depot/health/device-1", "via wss")
    note(f"   and it publishes like any other client -> PUBACK {rc}")
    browser.close()
    note("")
    note(f"A Unix socket at {SOCKET}, mode 0660.")
    note("No TCP stack, no port to firewall, and the file's permissions are the")
    note("access control. The door that matters on an edge box where the broker")
    note("and its consumers share a machine.")
    tool = Client("a-local-tool", transport="unix")
    note(f"   CONNACK: {tool.connack}")
    rc = tool.publish("iot/depot/health/device-1", "via the socket")
    note(f"   same broker, same channels -> PUBACK {rc}")
    tool.close()
    note("")
    note("There is no TLS on the socket: there is no network hop for it to")
    note("protect, and the permissions already decide who may reach it.")
    wait_user()


def broadcast():
    say("Broadcast: an MQTT topic not tied to a Saguin channel")
    wait_user()

    say("1. broadcast reaches whoever is connected")
    note("Starting first subscriber (weather-1) on topic 'iot/+/health/+'")
    listener = Client("weather-1")
    listener.on_message = lambda m: print(
        f"   weather-1 got: {m.topic} --> {m.payload.decode()}"
    )
    listener.subscribe("iot/+/health/+", qos=1)
    time.sleep(0.5)
    note("Publishing to topic 'iot/depot/health/device-1', which no filter claims")
    pub("iot/depot/health/device-1", "18C and clear")
    time.sleep(1)
    listener.close()
    wait_user()

    say("2. broadcast is not stored, a later subscriber has missed the previous messages")
    note("Starting second subscriber (weather-2), post message publishing, on 'iot/+/health/+'")
    note("Expect nothing below this line...")
    n = collect("weather-2", "iot/+/health/+", 1, label="weather-2 got", expected=0)
    note(f"({n} messages - broadcast is not stored, so there was nothing to send)")
    wait_user()


def append_channel(full=False):
    say("Append channel: 'events', stored in SQLite")
    note("A durable, replayable event stream. Every consumer holds its own")
    note("position, and reading removes nothing - so the same records can be")
    note("read again, by a consumer that did not exist when they were published.")
    note("Its filter is 'iot/+/events/+': the third level is what picks it out")
    note("of the same 'iot/depot/' tree that holds state, presence and work.")
    wait_user()

    say("1. publish three events with nobody subscribed")
    note("No subscriber is running. On a broadcast topic these three would be")
    note("gone; on a channel the broker stores them.")
    for i, what in ((1, "order-1"), (2, "order-2"), (3, "order-3")):
        pub(f"iot/depot/events/order-{i}", what)
    wait_user()

    say("2. a consumer that did not exist yet reads all three, in order")
    note("Starting a subscriber (events-reader) on 'iot/+/events/+', AFTER those three publishes")
    note("Expect all three, oldest first, and three user properties on each:")
    note("  saguin-id      the Message ID, stable for the life of the message")
    note("  saguin-offset  its position in this channel")
    note("  saguin-timestamp  when the broker admitted the message")
    note("They are different things and neither substitutes for the other - the")
    note("id is what a consumer deduplicates on, and it survives redelivery,")
    note("dead-lettering and replay unchanged.")
    collect("events-reader", "iot/+/events/+", 5, label="Reader got",
            show_headers=True, expected=3)
    peek(
        "the same rows, straight out of the file the broker is still writing to",
        'SELECT "offset", topic, CAST(payload AS TEXT) AS payload '
        "FROM records WHERE channel = 'events' ORDER BY \"offset\";",
    )
    note("Not a copy and not an export: storage you can open.")
    wait_user()

    say("3. a durable consumer resumes from its own position")
    note("The same subscriber run twice, with its position stored in between:")
    note("  clean start OFF, so the broker keeps the session")
    note("  a client id - what the stored position is filed under")
    note("  session expiry 300s. Ask for the expiry you actually want; saguin")
    note("  caps what a client may ask for at limits.max_session_expiry,")
    note("  30 days by default, and says so in the CONNACK.")
    note("")
    note("Run 1: the client id 'demo-durable' has no stored position, so it gets the backlog")
    collect(
        "demo-durable", "iot/+/events/+", 3, clean_start=False, session_expiry=300,
        label="Run 1 got", expected=3,
    )
    note("")
    note("Publishing one more event while it is away")
    pub("iot/depot/events/order-4", "order-4")
    note("Run 2: the same client id, connecting again. Expect ONLY order-4 -")
    note("the three it already acknowledged are not sent twice.")
    collect(
        "demo-durable", "iot/+/events/+", 3, clean_start=False, session_expiry=300,
        label="Run 2 got", expected=1,
    )
    peek(
        "one row per client id, and one integer in it",
        'SELECT channel, reader, "offset" FROM positions ORDER BY reader;',
    )
    note("The mqtt: on the front is the scheme: a reader is named, and one day")
    note("a bridge will hold a position here with no MQTT session at all.")
    wait_user()

    if not full:
        return

    say("4. a consumer chooses where to start, then tails from there")
    note("This channel holds four records. Rather than replay all of them, this")
    note("consumer asks to start at offset 3, by publishing to a reserved topic")
    note("the broker answers on. It needs no MQTT extension: it is an ordinary")
    note("PUBLISH a stock client can send, exactly like a queue worker's ack.")
    note("")
    note("  0  the channel's floor, whatever it is    -1  the end, from now on")
    note("")
    note("so a consumer never has to ask where either is first. It can also name")
    note("a moment instead of an offset - -12h, 90m, or an RFC 3339 time - and")
    note("the broker answers with the earliest offset at or after it. A bare")
    note("integer stays an offset, because 1763000000 is a plausible offset and")
    note("a plausible Unix time, and reading one as the other is a silent skip.")
    seeker = Client("events-tail", clean_start=False, session_expiry=300)
    print(f"   {DIM}Seeking:{OFF} events-tail --> offset 3", end="")
    seeker.publish("$saguin/consumer/events/seek", "3")
    print(" (stored)")
    seeker.close()
    note("Now the same client id (events-tail) connects and reads. Expect offsets 3")
    note("and 4, not 1 and 2, which it chose to skip, and then order-5 live.")
    tail = Client("events-tail", clean_start=False, session_expiry=300)
    tail.on_message = lambda m: print(
        f"   Live got: {m.topic} --> {m.payload.decode()}"
        f"  (offset {user_props(m).get('saguin-offset', '?')})"
    )
    tail.subscribe("iot/+/events/+", qos=1)
    wait_for(lambda: len(tail.messages) >= 2, 5, "events-tail to replay offsets 3 and 4")
    note("Publishing to topic 'iot/depot/events/order-5'")
    pub("iot/depot/events/order-5", "order-5")
    wait_for(lambda: len(tail.messages) >= 3, 5, "events-tail to receive order-5")
    require([user_props(m).get("saguin-offset") for m in tail.messages] == ["3", "4", "5"],
            "events-tail did not receive exactly offsets 3, 4, and 5")
    tail.close()
    wait_user()

    say("5. a '#' subscriber sees everything except a queue's work")
    note("Starting a subscriber on '#', the widest filter MQTT has, and the")
    note("first thing anyone types at a new broker")
    note("Then three publishes:")
    note("  iot/depot/events/order-99   the 'events' channel  --> appears, stored")
    note("  iot/depot/health/device-2   no filter claims it  --> appears, not stored")
    note("  iot/depot/work/hidden-from-hash  the 'jobs' queue --> must NOT appear")
    note("A filter is served every append and latest channel it matches, so a")
    note("client subscribes the way MQTT already works and never has to know")
    note("where the channel boundaries are. What it can never reach is a queue:")
    note("that takes one exact subscription of its own, which is why a subscriber")
    note("on '#' cannot drain a work queue into a debugging session.")
    note("")
    note("Watch what arrives before the three publishes. This subscriber is")
    note("new, and an append channel replays a consumer that has no stored")
    note("position from its retention floor - so orders 1 to 5 come back first.")
    note("That is what an append channel promises, and it is the cost of a wide")
    note("filter rather than a fault: reconnect twenty times and read it twenty")
    note("times. 'saguin --route <config> \'#\'' says so before you subscribe.")
    probe = Client("wildcard-probe")
    probe.on_message = lambda m: print(
        f"   Subscriber on # got: {m.topic} --> {m.payload.decode()}"
    )
    probe.subscribe("#", qos=0)
    time.sleep(0.5)
    pub("iot/depot/events/order-99", "channel record (appears, and is stored)")
    pub("iot/depot/health/device-2", "broadcast record (appears, stored nowhere)")
    pub("iot/depot/work/hidden-from-hash", "held for a worker, never sent to #")
    note("The queue KEEPS that third publish: work waits for a worker, not")
    note("for a subscriber. A quiet cleanup worker resolves it after this proof")
    note("so this section can be run by itself or in any order.")
    time.sleep(2)
    topics_seen = [m.topic for m in probe.messages]
    require("iot/depot/work/hidden-from-hash" not in topics_seen,
            "the subscriber on # received queue work")
    require("iot/depot/events/order-99" in topics_seen and
            "iot/depot/health/device-2" in topics_seen,
            "the subscriber on # missed the append or broadcast proof message")
    probe.close()
    cleanup = Worker("wildcard-cleanup", quiet=True)
    wait_for(lambda: cleanup.done == 1, 10, "the wildcard demonstration job to be acknowledged")
    cleanup.close()
    peek(
        "the two numbers the channel stores beside those records",
        "SELECT name, next, floor FROM channels WHERE name = 'events';",
    )
    note("next is the offset the next record will take; floor is the oldest one")
    note("still readable. Both are STORED, never worked out from the records that")
    note("happen to survive. Derived instead, a channel retention had emptied")
    note("would restart next at 1 and hand out offsets a consumer's stored")
    note("position already used - pointing it at unrelated records, which it")
    note("would read in order and report as success.")
    wait_user()


def partitioning():
    say("Partitioning: splitting 'events' between readers")
    note("Three readers, one filter, and each takes a third of it. No group,")
    note("no coordination, no configuration on the broker: each reader says")
    note("which slice it wants on its own SUBSCRIBE, as one MQTT 5 User")
    note("Property.")
    note("  saguin-filter   topic_hash(3, 0)")
    note("  saguin-filter   topic_hash(3, 1)   (the second reader)")
    note("  saguin-filter   topic_hash(3, 2)   (the third)")
    note("The broker applies FNV-1a-64 and its documented final mixing step to")
    note("the TOPIC; that value modulo the count selects the index. Every record")
    note("for one topic therefore always")
    note("goes to the same reader, in order. RFC 0003 writes the hash out in")
    note("full, with worked examples, so a client can work out which of its")
    note("topics are its own without asking the broker.")
    note("")
    note("The key is the topic and not the offset, and that is the whole")
    note("design. Splitting by offset would spread more evenly and put one")
    note("device's consecutive records on different readers. Even spreading")
    note("means ignoring the key; ordering means respecting it.")
    wait_user()

    say("1. three readers, each taking one slice of 'iot/+/events/+'")
    readers = []
    for index in range(3):
        c = Client(f"slice-{index}")
        code = c.subscribe_slice("iot/+/events/+", 3, index)
        value = int(code.value) if code is not None else -1
        note(f"   slice-{index} SUBACK {REASONS.get(value, hex(value))}")
        readers.append(c)

    # Which reader each topic belongs to, computed here from the algorithm
    # in RFC 0003 rather than from whatever arrives - a demo that accepted
    # whichever reader received a record would show that something happened
    # rather than that the broker agrees with its own specification.
    def owner(topic, count=3):
        h = 14695981039346656037
        for b in topic.encode():
            h = ((h ^ b) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
        h ^= h >> 30
        h = (h * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
        h ^= h >> 27
        h = (h * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
        h ^= h >> 31
        return h % count

    got = {i: [] for i in range(3)}
    for i, c in enumerate(readers):
        c.on_message = lambda m, i=i: got[i].append(m.topic)

    say("2. publishing twelve records across twelve topics")
    topics = [f"iot/depot/events/dev-{n}" for n in range(1, 13)]
    for t in topics:
        pub(t, "r")
    time.sleep(1.5)

    print(f"   {'topic':<32} {'expected':<10} {'received by':<12}")
    wrong = 0
    for t in topics:
        want = owner(t)
        where = [i for i in range(3) if t in got[i]]
        mark = "" if where == [want] else "   <-- WRONG"
        if mark:
            wrong += 1
        print(f"   {t:<32} slice {want:<4} {str(where):<12}{mark}")

    # A partition reader may still be finishing the append backlog after its
    # SUBACK. Count only this step's topics, not those earlier records.
    delivered = sum(t in got[i] for t in topics for i in range(3))
    note("")
    note(f"{delivered} of {len(topics)} records delivered, each to exactly one reader,")
    note(f"and {len(topics) - wrong} of {len(topics)} to the slice the hash names.")
    note("A record reaching two readers would be a duplicate; one reaching")
    note("none would be a gap nobody notices, because every reader looks")
    note("healthy. Both are what the counts above are for.")
    require(delivered == len(topics) and wrong == 0,
            f"partitioning delivered {delivered}/{len(topics)} records with {wrong} wrong owner(s)")
    wait_user()

    say("3. what this deliberately is NOT: a consumer group")
    note("There is no membership, no rebalancing, no generation and no shared")
    note("cursor - each reader is an ordinary session with its own position.")
    note("Two consequences follow, and both are the price of that:")
    note("")
    note("- Coverage is yours to get right. Nothing stops one reader saying 3")
    note("  slices while another says 2. A slice nobody claims is records")
    note("  nobody reads, and every reader still looks healthy. The broker")
    note("  cannot report that, because a missing index is indistinguishable")
    note("  from a reader that has not started yet.")
    note("- A reader's position moves past records outside its slice, so")
    note("  widening a declaration later recovers none of them.")
    note("")
    note("What the broker CAN say is that two readers on one filter declared")
    note("different counts. That cannot be correct, so it is a WARN in the")
    note("broker log naming both. Subscribing one now:")

    odd = Client("slice-disagrees")
    odd.subscribe_slice("iot/+/events/+", 2, 0)
    time.sleep(0.5)
    with open(LOG) as f:
        warned = [l for l in f if "disagree about the size of a partition space" in l]
    if warned:
        note(f"   {warned[-1].strip()[:170]}")
    else:
        raise DemoFailure("the partition-count disagreement produced no broker warning")
    odd.close()
    for c in readers:
        c.close()
    wait_user()


def latest_channel(full=False):
    say("Latest channel: 'state', stored in memory plus a snapshot")
    note("Durable latest-value-per-topic state. A publish REPLACES the value for")
    note("its topic rather than adding to a history, and a subscriber is sent")
    note("whatever is current the moment it subscribes.")
    note("Its filter is 'iot/+/state/+', beside 'events' in the same tree.")
    wait_user()

    say("1. a publish replaces the value, it does not accumulate")
    note("Publishing three times over two topics:")
    pub("iot/depot/state/device-1", "20")
    pub("iot/depot/state/device-2", "21")
    pub("iot/depot/state/device-1", "22")
    note("Three publishes, two topics. An append channel would now hold three")
    note("records; this one holds two values, as for device 1: 22 has replaced 20.")
    wait_user()

    say("2. a dashboard arriving now is sent current state, and nothing older")
    note("Starting a subscriber (dashboard-1) on 'iot/+/state/+', after all three publishes")
    note("Expect exactly two lines: device/1 reading 22 - not 20, and not both -")
    note("and device/2 reading 21. State arrives carrying the RETAIN flag, which")
    note("is how a client tells state it is catching up on from a live update.")
    collect("dashboard-1", "iot/+/state/+", 5, label="Dashboard got", expected=2)
    wait_user()

    if not full:
        return

    say("3. live updates reach a subscriber that is already connected")
    note("Starting a subscriber (dashboard-live) on 'iot/+/state/+' FIRST.")
    note("It is sent current state, and then every change as it happens.")
    live = Client("dashboard-live")
    live.on_message = lambda m: print(
        f"   Dashboard got: {m.topic} --> "
        f"{m.payload.decode() or '(empty - the key is deleted)'}"
    )
    live.subscribe("iot/+/state/+", qos=1)
    time.sleep(1)
    note("Publishing an update to 'iot/depot/state/device-1'")
    pub("iot/depot/state/device-1", "23")
    time.sleep(1)
    note("Now publishing a ZERO-LENGTH payload to the same topic (tombstone)")
    note("which DELETES the key, MQTT's own retained-message convention.")
    note("It is delivered to whoever is subscribed, because that is how they")
    note("learn the key is gone.")
    pub("iot/depot/state/device-1", "")
    time.sleep(2)
    live.close()
    peek(
        "the latest channel's rows in sqlite",
        "SELECT count(*) AS rows_for_latest_channels FROM latest_values;",
    )
    note("Zero, and that is the lesson rather than a fault: 'state' is on a")
    note("memory provider, so its values live in memory and reach disk only as a")
    note("snapshot at shutdown. Storage decides where a channel is, and nothing")
    note("about what a latest channel MEANS changes with the answer.")
    wait_user()

    say("4. the deleted key is not served to the next subscriber")
    note("Starting a fresh subscriber (dashboard-2) on 'iot/+/state/+'")
    note("Expect device/2 only. The delete was delivered above but not stored,")
    note("so a later subscriber is told nothing about device/1,")
    note("exactly as it would be for a topic that never existed.")
    collect("dashboard-2", "iot/+/state/+", 5, label="Dashboard got", expected=1)
    wait_user()

    say("5. a point read: one key, without subscribing to it")
    note("A latest channel is a key-value store, and this is its GET. Publish the")
    note("key to '$saguin/kv/get' with a Response Topic, and the value comes back")
    note("there. The caller does not become a subscriber to the key.")
    note("")
    note("This uses Paho because mosquitto_sub cannot")
    note("show Correlation Data, and Correlation Data is half of the answer.")
    reader = Client("kv-reader")
    reader.on_message = lambda m: print(
        f"   Reply: {m.payload.decode() or '(empty - there is no value)'}"
        f"  [answering: {getattr(m.properties, 'CorrelationData', b'').decode()}]"
    )
    reader.subscribe("kv-reply", qos=1)
    time.sleep(1)

    note("Reading 'iot/depot/state/device-2', which has a value. No Correlation Data")
    note("is set, so the reply carries the key it is answering - every reply says")
    note("what it is about, whether or not the caller asked it to.")
    reader.publish("$saguin/kv/get", "iot/depot/state/device-2", qos=1,
                   response_topic="kv-reply")
    wait_for(lambda: len(reader.messages) >= 1, 5, "the first point-read reply")

    note("Reading 'iot/depot/state/device-1', which was deleted in step 3.")
    note("Expect an EMPTY reply. That is the whole point of the verb: subscribing")
    note("to ask answers an absent key with silence, which is indistinguishable")
    note("from a slow one. A deleted key and a key nobody ever set are the same")
    note("answer, exactly as a zero-length payload already means on this channel.")
    reader.publish("$saguin/kv/get", "iot/depot/state/device-1", qos=1,
                   response_topic="kv-reply")
    wait_for(lambda: len(reader.messages) >= 2, 5, "the absent-key point-read reply")

    note("And with Correlation Data of the caller's own, which is echoed instead -")
    note("for a client with several reads outstanding and its own bookkeeping.")
    reader.publish("$saguin/kv/get", "iot/depot/state/device-2", qos=1,
                   correlation=b"my-request-42", response_topic="kv-reply")
    wait_for(lambda: len(reader.messages) >= 3, 5, "the correlated point-read reply")
    reader.close()
    wait_user()


# The eight forms a queue refuses, and the one it admits. Each is refused
# with the code that describes the rule it broke, and the client stays
# connected - so it can subscribe again correctly rather than being
# dropped.
QUEUE_FORMS = [
    ("try-filter", "iot/+/work/+", 1, "the channel's filter, not its name"),
    ("try-narrow", "$saguin/queue/jobs/build-1", 1, "a channel name is one level"),
    ("try-wild", "$saguin/queue/#", 1, "every queue at once"),
    ("try-resp", "$saguin/queue/jobs/#", 1, "straddles work and the response topic"),
    ("try-dlqname", "$saguin/queue/jobs__dlq", 1,
     "the dead-letter channel's NAME - a name that is not a queue"),
    ("try-topic", "iot/depot/work/build-1", 1, "one topic, no wildcard, still a queue"),
    ("try-qos0", "$saguin/queue/jobs", 0, "QoS 0: no PUBACK to time a lease from"),
    ("try-append", "$saguin/queue/events", 1, "an append channel's name, not a queue's"),
    ("try-correct", "$saguin/queue/jobs", 1, "the channel's name: the only form"),
]

REASONS = {
    0x00: "granted QoS 0",
    0x01: "granted QoS 1",
    0x83: "0x83 implementation specific error",
    0x87: "0x87 not authorized",
    0x8F: "0x8F topic filter invalid",
}


def queue_channel(full=False):
    say("Queue channel: 'jobs', stored in SQLite")
    note("Work exactly only one worker should do:")
    note("- exclusive delivery;")
    if full:
        note("- visibility timeout;")
        note("- redelivery;")
        note("- attempt limit;")
        note("- dead-letter channel.")
    note("From examples/saguin.yaml:")
    note("- visibility_timeout: 5s")
    note("- max_attempts: 3.")
    note("PUBACK is NOT an acknowledgement: a worker answers by publishing 'ack'")
    note("or 'return' to the Response Topic the broker sent it, echoing its")
    note("Correlation Data.")
    wait_user()

    if full:
        say("1. every non-canonical queue subscription is refused.")
        note("A queue admits ONE form: '$saguin/queue/' + the channel's NAME -")
        note("'$saguin/queue/jobs'. One exact string is one population of workers.")
        note("The name, not the filter: a worker needs nothing out of the")
        note("configuration file, and the same name spells this channel's seek topic,")
        note("its response topic and its dead-letter channel. Two spellings that both")
        note("worked would be two populations, and both of them would get a copy of")
        note("every job.")
        note("")
        note("'$saguin/queue/' is the broker's own: it carries a queue's name and")
        note("nothing else. 'jobs__dlq' below is the trap - a dead-letter channel is")
        note("an ordinary append channel, read with an ordinary filter, and asking")
        note("for it this way names a channel that is not a queue.")
        note("'iot/depot/work/build-1' is the one that surprises: no wildcard, so it")
        note("looks like an ordinary subscription to a single topic. It would")
        note("reach the queue's records without the exclusivity that makes it a queue.")
        note("")
        note("The reason code is printed here. Each refusal names its own rule,")
        note("and the connection stays up in every case.")
        note("")
        print(f"   {'client id':<12} {'filter':<30} QoS  {'why':<44} SUBACK")
        for client_id, topic, qos, why in QUEUE_FORMS:
            c = Client(client_id)
            expect_refusal = client_id != "try-correct"
            code = c.subscribe(topic, qos=qos, expect_refusal=expect_refusal)
            value = code_value(code)
            print(
                f"   {client_id:<12} {topic:<30} @{qos}   {'(' + why + ')':<44} "
                f"{REASONS.get(value, hex(value))}"
            )
            c.close()
        wait_user()

    say(f"{'2' if full else '1'}. two workers compete: each job goes to exactly one of them.")
    note("Publishing one job before a worker exists. The queue holds it until")
    note("one of the two workers below connects.")
    pub("iot/depot/work/waiting", "job-waiting")
    note("Starting two workers, both on $saguin/queue/jobs,")
    note("each spending 100ms pretending to work before it answers 'ack'")
    a = Worker("worker-a", work=0.1)
    b = Worker("worker-b", work=0.1)
    time.sleep(1)
    note("Publishing four more jobs, 'iot/depot/work/build-1' ... '.../build-4'")
    note("Expect five 'got' lines in total across the two workers - the held")
    note("job and these four - and never a job on both workers at once")
    for i in range(1, 5):
        pub(f"iot/depot/work/build-{i}", f"job-{i}")
    # Waited for rather than slept through, and the workers are not closed
    # until it is done: a worker closed mid-job hands that job back, and the
    # next step would then meet a redelivery of this one.
    wait_for(lambda: a.done + b.done >= 5, 20, "the five jobs to be acknowledged")
    note(f"worker-a did {a.done}, worker-b did {b.done} - {a.done + b.done} of 5")
    a.close()
    b.close()
    time.sleep(0.5)
    peek(
        "what is left of the five jobs",
        "SELECT count(*) AS jobs_still_unresolved FROM queue_items "
        "WHERE channel = 'jobs';",
    )
    note("Five jobs were resolved - one waiting and four new - and none are left,")
    note("because a queue holds unresolved")
    note("work rather than a history: an acknowledged job leaves this table for")
    note("good. An append channel would still hold all five - that difference is")
    note("the whole distinction between the two types, and it is visible on disk.")
    wait_user()

    if not full:
        return

    say("3. return: a worker hands the job back before its time is up")
    note("Starting a subscriber (dlq-watch-1) on 'iot/+/work/+/__dlq', the dead-letter")
    note("channel this queue derives automatically. It cannot be configured, and")
    note("it is an ordinary append channel in every other respect.")
    note("Starting one worker that answers 'return' to every job")
    note("Publishing one job, iot/depot/work/flaky")
    note("")
    note("Expect three attempts and then the dead-letter channel, because")
    note("max_attempts is 3. The gaps between them are the retry backoff, which")
    note("this channel configures as linear with backoff_base 2s: a returned")
    note("record waits 2s after one attempt and 4s after two, so the attempts")
    note("are not back to back. Each one prints the seconds since the job was")
    note("published, so that is checked rather than believed.")
    note("")
    note("What a return saves is the wait for the visibility timeout, not the")
    note("backoff: the timeout is what ends a job a worker is HOLDING, and a")
    note("worker that hands it back has stopped holding it. Step 4 below is the")
    note("same job with no answer at all, for the comparison.")
    dlq = Client("dlq-watch-1")
    dlq.on_message = lambda m: print(
        f"   Dead-letter got: {m.topic} --> {m.payload.decode()}"
    )
    dlq.subscribe("iot/+/work/+/__dlq", qos=1)
    flaky = Worker("worker-flaky", action="return", work=0.1)
    time.sleep(1)
    started = time.time()
    flaky.clock = started
    pub("iot/depot/work/flaky", "job-returned")
    # 45s rather than 20: two backoffs and three deliveries fit inside 20
    # comfortably, but a timeout that is only just long enough reports the
    # broker did nothing when what happened is that the step stopped
    # looking. The old figure gave up one second before the record landed,
    # and the step below then printed it as though it were its own.
    wait_for(lambda: any(b"job-returned" == m.payload for m in dlq.messages),
             45, "the job to reach the dead-letter channel")
    time.sleep(0.5)
    flaky.close()
    dlq.close()
    wait_user()

    say("4. visibility timeout: a worker that never answers")
    note("Starting a subscriber (dlq-watch-2) on 'iot/+/work/+/__dlq' again. It is a")
    note("fresh consumer of an append channel, so it replays that channel from")
    note("the start - the job from the step above arrives first, and is not")
    note("this step's.")
    note("Starting one worker that receives the job and never answers, which is")
    note("what a worker that has crashed or hung looks like")
    note("Publishing one job, iot/depot/work/doomed")
    note("Expect attempt=1, a 5s wait, attempt=2, another 5s, attempt=3, and then")
    note("the dead-letter line carrying why it failed and how many attempts were")
    note("made, as ordinary user properties any stock client can read.")
    note("This step is the slow one: about 20 seconds of waiting on purpose.")

    def show_dlq(m):
        print(f"   Dead-letter got: {m.topic} --> {m.payload.decode()}")
        for k, v in user_props(m).items():
            if k.startswith("saguin-dlq-"):
                print(f"      {k} = {v}")

    dlq = Client("dlq-watch-2")
    dlq.on_message = show_dlq
    dlq.subscribe("iot/+/work/+/__dlq", qos=1)
    stall = Worker("worker-stall", action="stall", work=0.05)
    time.sleep(1)
    stall.clock = time.time()
    pub("iot/depot/work/doomed", "job-stalled")
    wait_for(lambda: any(b"job-stalled" == m.payload for m in dlq.messages),
             40, "three leases to expire and the job to be dead-lettered")
    time.sleep(0.5)
    stall.close()
    dlq.close()
    wait_user()


def storage():
    say("Storage: the same channel behavior, three durability choices")
    note("None of the channel behavior depends on which provider a channel uses.")
    note("Storage decides how much is still there after the broker stops, and")
    note("nothing else. From examples/saguin.yaml:")
    note("")
    note("  events    append   sqlite                  survives a crash")
    note("  state     latest   memory + snapshot       survives a graceful stop")
    note("  presence  latest   memory, keeps nothing   survives nothing, on purpose")
    wait_user()

    say("1. a graceful stop: what each kind of storage keeps")
    note("Publishing one uniquely named value to each storage kind")
    pub("iot/depot/events/storage-graceful", "sqlite")
    pub("iot/depot/state/storage-graceful", "snapshot")
    pub("iot/depot/presence/storage-graceful", "memory only")
    time.sleep(0.5)
    note("Stopping the broker with SIGTERM - the one stop memory storage is")
    note("allowed to survive, because the snapshot is written on the way out")
    stop_broker(signal.SIGTERM)
    note("Restarting, and counting what each channel still holds")
    note("Expect: the event and state markers survive; the presence marker is gone")
    start_broker()
    peek(
        "every channel's offsets and retention floor",
        "SELECT name, next, floor, bytes FROM channels ORDER BY name;",
    )
    note("next is the offset the next record gets; floor is the oldest offset")
    note("still held. floor = 1 everywhere because this configuration removes")
    note("nothing. When retention does remove records the floor rises, and that")
    note("is what lets the broker tell a returning consumer its position is gone")
    note("rather than silently restarting it (invariant 1).")
    note("")
    events_kept = count("tally-1", "iot/depot/events/storage-graceful", expected=1)
    state_kept = count("tally-s1", "iot/depot/state/storage-graceful", expected=1)
    presence_kept = count("tally-p1", "iot/depot/presence/storage-graceful", expected=0)
    print(f"   events    (sqlite)          {events_kept} marker")
    print(f"   state     (snapshot)        {state_kept} marker")
    print(
        f"   presence  (keeps nothing)   {presence_kept} markers"
        " - gone, which is what this provider promises"
    )
    wait_user()

    say("2. a crash: only sqlite survives it")
    note("Publishing one more record to each of the two durable channels")
    pub("iot/depot/events/storage-crash", "written just before the kill")
    pub("iot/depot/state/storage-crash", "written just before the kill")
    time.sleep(0.5)
    event_live = count("tally-live", "iot/depot/events/storage-crash", expected=1)
    state_live = count("tally-s2", "iot/depot/state/storage-crash", expected=1)
    print(f"   while it is still running:  {event_live} event marker, {state_live} state marker")
    note("Now SIGKILL, so no snapshot is written and no shutdown path is taken")
    note("Expect: events keeps the record written just before the kill; state is")
    note("back to the last snapshot and has lost 'iot/depot/state/storage-crash'")
    stop_broker(signal.SIGKILL)
    start_broker()
    event_after_crash = count("tally-2", "iot/depot/events/storage-crash", expected=1)
    state_after_crash = count("tally-s3", "iot/depot/state/storage-crash", expected=0)
    print(f"   events    (sqlite)          {event_after_crash} marker"
          " - including the one written just before the kill")
    print(f"   state     (snapshot)        {state_after_crash} markers"
          " - back to the last snapshot, losing what came after")
    note("That is invariant 14, not a defect: memory durability is exactly the")
    note("last successful snapshot, and nothing anywhere implies more.")
    note("sqlite is not absolute either - a power cut costs the last commits,")
    note("which synchronous=NORMAL had not yet synced.")


SECTION_NAMES = ("security", "broadcast", "append", "partitioning", "latest", "queue", "storage")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Saguin's guided MQTT and channel tour.",
        epilog="With no section, the concise core tour runs. Use --full for every deep dive.",
    )
    parser.add_argument("--full", action="store_true",
                        help="include security, partitioning, queue failures, and dead lettering")
    parser.add_argument("--section", action="append", choices=SECTION_NAMES,
                        help="run one section; repeat to run several in the order named")
    parser.add_argument("--list", action="store_true", help="list the available sections and exit")
    parser.add_argument("--no-pause", action="store_true", help="run without waiting for ENTER")
    parser.add_argument("--skip-build", action="store_true", help="use the existing bin/saguin")
    parser.add_argument("--credentials", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.full and args.section:
        parser.error("--full and --section are alternatives")
    return args


def run_tour(args):
    if args.section:
        runners = {
            "security": security,
            "broadcast": broadcast,
            "append": lambda: append_channel(full=True),
            "partitioning": partitioning,
            "latest": lambda: latest_channel(full=True),
            "queue": lambda: queue_channel(full=True),
            "storage": storage,
        }
        for name in args.section:
            runners[name]()
        return

    overview()
    if args.full:
        security()
    broadcast()
    append_channel(full=args.full)
    if args.full:
        partitioning()
    latest_channel(full=args.full)
    queue_channel(full=args.full)
    storage()
    recap()


def main(args):
    if not os.path.exists(CONFIG):
        raise DemoFailure(f"run this from the repository: {CONFIG} not found")

    preflight()
    if not args.skip_build:
        subprocess.run(
            ["go", "build", "-o", os.path.join("bin", "saguin"), "./cmd/saguin"],
            cwd=ROOT,
            check=True,
        )
    require(os.path.isfile(os.path.join(ROOT, "bin", "saguin")),
            "bin/saguin does not exist; omit --skip-build or run make build")

    # From nothing every time, so the tour shows what this run did rather
    # than what the last one left behind. The configuration keeps its
    # snapshots and its database under here.
    if os.path.exists(STATE):
        shutil.rmtree(STATE)
    open(LOG, "w").close()

    # Before the broker, because it will not start without them: the
    # listener reads its certificate at startup and refuses to come up in
    # plain text if it is missing, and an acl_file naming a password file
    # that does not exist is a startup error too.
    say("staging the credentials this broker needs")
    note("The certificates, the password file and the acl_file are checked in")
    note("under examples/ - examples/README.md says what each one is and how the")
    note("certificates were made. They are copied to /tmp/saguin-demo because")
    note("saguin refuses a relative path for any of them, and no absolute path")
    note("into a repository is true on two machines.")
    stage_credentials()
    note(f"   examples/support/certs -> {TLS_DIR}")
    note(f"   examples/clients.passwd -> {PASSWD}")
    note(f"   examples/acl.yaml -> {ACL}")

    global PUBLISHER
    try:
        say("starting saguin")
        note("One binary and one config file: examples/saguin.yaml - which names")
        note("examples/support/channels/presence.yaml for the 'presence' channel,")
        note("because a fleet's channels belong in files their own owners review.")
        note("The other three are written in the main file, and the two forms mix freely.")
        note("Three listeners at once, which is all a broker takes - one of each:")
        note(f"   tcp   {HOST}:{PORT}   TLS, and a client certificate accepted")
        note(f"   ws    {HOST}:{WS_PORT}   wss")
        note(f"   unix  {SOCKET}")
        note(f"Broker log: {LOG}")
        start_broker()
        note(f"Protocols admitted: {admitted_protocols()} - read back from the broker.")
        PUBLISHER = Client("demo-publisher")
        run_tour(args)
        say(f"done - broker log: {LOG}")
    finally:
        close_clients()
        PUBLISHER = None
        stop_broker()


if __name__ == "__main__":
    # Put what the broker needs where it expects it and stop, which is what
    # `make demo-server` wants: it runs the broker itself and only needs the files
    # to be there.
    arguments = parse_args()
    configure_output(arguments.no_pause)
    if arguments.list:
        print("core (default)")
        for section in SECTION_NAMES:
            print(section)
        sys.exit(0)
    if arguments.credentials:
        os.makedirs(STATE, exist_ok=True)
        stage_credentials()
        print(f"credentials for examples/saguin.yaml are staged in {STATE}")
        sys.exit(0)

    try:
        main(arguments)
    except DemoFailure as err:
        print(f"\nDEMO FAILED: {err}", file=sys.stderr)
        stop_broker()
        sys.exit(1)
    except (OSError, subprocess.CalledProcessError) as err:
        print(f"\nDEMO FAILED: {err}", file=sys.stderr)
        stop_broker()
        sys.exit(1)
    except KeyboardInterrupt:
        stop_broker()
        sys.exit(130)
