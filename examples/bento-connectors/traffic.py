#!/usr/bin/env python3
"""The parts of saguin's catalogue the Bento pipelines cannot drive.

The two pipelines beside this file are the load: two hundred simulated
devices publishing into two `append` channels and one `latest` one, bridged
to Kafka. That is a load nobody shaped to look good on a dashboard, which is
the strongest evidence the numbers mean anything - and it is the reason this
file publishes no measurements of its own.

What it drives instead is everything the pipelines are not: a queue with a
worker that succeeds and fails and sometimes dies, an upstream feeding
saguin's bridge, a second durable consumer on a channel Bento already fills,
and a few deliberate mistakes. Three of the dashboard's rows have no
other source.

It is not a benchmark and deliberately not fast. The rates are chosen so
that a fifteen-second scrape interval produces a curve rather than a
staircase, and so that the numbers on screen stay small enough to read.

Run it anywhere with paho-mqtt; docker-compose.yml runs it beside the
broker. Addresses come from the environment so that the same file works on
the compose network and from the host:

    SAGUIN_HOST=127.0.0.1 UPSTREAM_HOST=127.0.0.1 UPSTREAM_PORT=1884 \\
        python3 traffic.py
"""

import json
import os
import random
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

SAGUIN_HOST = os.environ.get("SAGUIN_HOST", "saguin")
SAGUIN_PORT = int(os.environ.get("SAGUIN_PORT", "1883"))
UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "mosquitto")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "1883"))

DEVICES = [f"device-{n:02d}" for n in range(1, 13)]


def connect(client_id, host=SAGUIN_HOST, port=SAGUIN_PORT, clean=True,
            session_expiry=None, subscribe=()):
    """One MQTT 5 client, connected and running its own network loop.

    **Subscriptions are named here and sent from on_connect, never by the
    caller afterwards.** `connect()` returns as soon as the socket is open
    and the network loop is running, which is before the CONNACK has
    arrived - and paho refuses a subscribe on a connection that is not yet
    established, by *returning* an error rather than raising one. A caller
    doing `c = connect(...)` then `c.subscribe(...)` therefore races the
    CONNACK, and loses silently: the client stays connected, the filter is
    never sent, and the strand sits there looking healthy.

    That is what happened to the queue worker. It held a connection all day
    and was delivered nothing, so every job aged out and dead-lettered
    unattempted - `saguin_queue_delivered_total` flat at 0 while
    `_expired_total` climbed past a thousand, on a broker that was working
    perfectly. A fresh worker on the same filter drained 546 jobs in six
    seconds.

    Subscribing from on_connect also survives a reconnect, which the old
    shape did not: paho does not replay subscriptions, so any strand that
    lost its link stopped receiving for good.
    """
    c = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id,
                    protocol=mqtt.MQTTv5)
    props = None
    if session_expiry is not None:
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = session_expiry

    def on_connect(client, _userdata, _flags, reason_code, _properties=None):
        if reason_code != 0:
            print(f"{client_id}: refused at CONNECT: {reason_code}", flush=True)
            return
        for filt in subscribe:
            # **The SUBACK is read.** A queue refuses anything but the one
            # shared form with 0x8F, and paho reports that through a
            # callback rather than through the call - so a filter this
            # broker will not grant would otherwise be indistinguishable
            # from one it granted and never fills.
            client.subscribe(filt, qos=1)

    def on_subscribe(_client, _userdata, _mid, reason_codes, _properties=None):
        for rc in reason_codes:
            if getattr(rc, "is_failure", False) or "Granted" not in str(rc):
                print(f"{client_id}: subscription refused: {rc}", flush=True)

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    while True:
        try:
            c.connect(host, port, keepalive=30, clean_start=clean,
                      properties=props)
            break
        except OSError as e:
            print(f"{client_id}: waiting for {host}:{port} ({e})", flush=True)
            time.sleep(2)
    c.loop_start()
    return c


def forever(name, fn, every):
    """Run fn on a thread of its own, for as long as this process lives.

    Every strand is wrapped, because the failure this shape is prone to is a
    thread that died in the night: the dashboard goes flat and looks like a
    broker that stopped, which is the one thing a monitoring demonstration
    must not fake. A strand that throws says so and carries on.
    """
    def loop():
        while True:
            try:
                fn()
            except Exception as e:            # noqa: BLE001 - reported, not hidden
                print(f"{name}: {e!r}", flush=True)
            time.sleep(every)
    t = threading.Thread(target=loop, name=name, daemon=True)
    t.start()
    return t


def durable_consumer():
    """A second consumer with a session, on a channel Bento already fills.

    **This is the strand saguin_channel_consumer_position_min depends on**,
    and that metric is half of the one alert RFC 0005 says the whole
    catalogue exists for: a channel's floor passing a consumer's position is
    data deleted before anybody read it. With no durable consumer the metric
    has no series at all and the panel is empty rather than healthy.

    It reads iot/weather/measurement/+, which the bridge is also reading. Two
    positions on one channel is what makes the minimum a minimum rather than
    a restatement of the only number there is - and if this one falls behind,
    the panel shows it falling behind the bridge.
    """
    c = connect("traffic-reader", clean=False, session_expiry=3600,
                subscribe=["iot/weather/measurement/+"])
    # Nothing to do on the messages: paho acknowledges them, and the
    # acknowledgement is what advances the stored position.
    c.on_message = lambda *_: None


def queue_worker():
    """A worker that mostly succeeds, sometimes fails, and sometimes dies.

    All three on purpose. Acknowledging moves saguin_queue_acknowledged_total;
    returning moves _returned_total and puts the job back at once; going
    silent lets the visibility timeout take it back, which is
    _redelivered_total - and a job unlucky enough to be dropped three times
    reaches max_attempts and lands in the dead-letter channel.

    **That is not the only way _dead_lettered_total moves, and saying so was
    wrong.** A job nobody ever took expires after `job_expires_after` and
    dead-letters with `saguin-dlq-reason=expired` and `attempts=0` - which is
    what every record in this stack's dead-letter channel was, for as long as
    this worker's subscription was silently dropped. The two reasons are told
    apart by that property, and the viewer shows it.
    """
    def on_message(client, _userdata, msg):
        props = msg.properties
        response = getattr(props, "ResponseTopic", None)
        ticket = getattr(props, "CorrelationData", None)
        if response is None or ticket is None:
            print(f"worker: a job arrived with no ticket: {msg.topic}",
                  flush=True)
            return

        roll = random.random()
        if roll < 0.15:
            # Dies without answering. The job comes back on the visibility
            # timeout, counted as a redelivery.
            return
        answer = "ack" if roll < 0.85 else "return"
        out = Properties(PacketTypes.PUBLISH)
        out.CorrelationData = ticket
        client.publish(response, answer, qos=1, properties=out)

    # The one subscription form a queue admits: `$saguin/queue/` and the
    # channel's name. Anything else is refused with 0x8F, which is
    # deliberate: work that goes to different pools is two channels rather
    # than two filters, and two spellings would be two populations of
    # workers each taking a copy of every job.
    c = connect("traffic-worker", subscribe=["$saguin/queue/jobs"])
    c.on_message = on_message


def job_publisher():
    """Work for the queue, at a rate one worker can nearly keep up with.

    **The payload says what the work is.** It was the string `work` - the
    same four bytes on every job and therefore on every dead-letter record,
    which made the dead-letter channel a list of identifiers with nothing
    to read. A job somebody is meant to look at after it failed has to carry
    what it was asked to do.
    """
    c = connect("traffic-jobs")

    def tick():
        job = random.choice(["resize", "reindex", "notify", "settle"])
        job_id = uuid.uuid4().hex[:8]
        body = {
            "job": job,
            "id": job_id,
            "device": random.choice(DEVICES),
            "requested_at": int(time.time() * 1000),
            "attempt_budget": 3,
        }
        c.publish(f"iot/tasks/{job}/{job_id}", json.dumps(body), qos=1)

    forever("jobs", tick, 1.1)


def upstream_publisher():
    """A producer at the OTHER broker, which is what the bridge reads.

    It publishes to mosquitto, not to saguin. Nothing here knows saguin
    exists - that is the whole point of a bridge, and it is why
    saguin_bridge_received_total counts records saguin was never published
    to directly.
    """
    c = connect("traffic-upstream", host=UPSTREAM_HOST, port=UPSTREAM_PORT)

    def tick():
        device = random.choice(DEVICES)
        c.publish(f"fleet/{device}/telemetry/psi",
                  f"{random.uniform(0, 60):.2f}", qos=1)

    forever("upstream", tick, 0.8)


def mistakes():
    """The deliberate ones, because a refusal is a metric too.

    Three shapes, and each answers a different question an operator brings:

      * A publish into a dead-letter channel is refused. A record arrives
        there by the queue's own move; a publish into it would be somebody
        inventing a failure that never happened.
      * A publish to a topic no channel claims and nobody subscribes to is
        ordinary MQTT broadcast that reached nobody. MQTT has no reason code
        for it - the PUBACK says 0x00 - so the counter is the only thing
        that will ever tell you a channel name is misspelt.
      * A CONNECT asking to keep its session for a year is given the cap
        instead, silently as far as the client is concerned.

    Rare on purpose: these are meant to be a line that ticks up now and
    then, not a wall of red.
    """
    c = connect("traffic-mistakes")

    def tick():
        c.publish("iot/tasks/settle/invented/__dlq", "not allowed", qos=1)
        c.publish("iot/nowhere/typo/nobody", "nobody is listening", qos=1)
        greedy = connect("traffic-greedy", session_expiry=365 * 24 * 3600)
        time.sleep(1)
        greedy.disconnect()
        greedy.loop_stop()

    forever("mistakes", tick, 20)


def main():
    print(f"driving saguin at {SAGUIN_HOST}:{SAGUIN_PORT}, "
          f"upstream at {UPSTREAM_HOST}:{UPSTREAM_PORT}", flush=True)
    durable_consumer()
    queue_worker()
    job_publisher()
    upstream_publisher()
    mistakes()
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
