#!/usr/bin/env bash
#
# Is the connectors demo actually doing anything?
#
# **Review found nine faults in this demo, and
# nothing in this repository caught one of them.** `make check` never
# reaches examples/. `bento lint` passed a pipeline that would not start,
# because `processors: []` is refused at init and not at lint. `docker
# compose ps` showed every container Up (healthy), because Bento answers
# /ready once its input and output have connected - which a stalled bridge
# had. The dashboard drew convincing curves over a queue that had never
# delivered a job.
#
# So every assertion here is a fault that actually shipped, and each one
# reads a number out of the running stack rather than asking a component
# whether it is happy.
#
# **Two of them are the shape that keeps recurring in this tree**, and are
# the two written most carefully: a counter that must *move* between two
# samples rather than merely exist, and a topic's configuration read back
# from Kafka rather than from the job that set it. A gauge sweep on the same
# day reported eight passes having measured nothing, because it read only
# the final value.
#
# **And it proves it looked.** A check of this shape passes trivially when
# the stack never came up, so it counts what it found and refuses an
# implausible number - the way the log-line guard and the doc-comment check
# in this tree already do.
#
# Not part of `make check`: it needs a Docker daemon, several minutes and
# the Confluent images. Run ./smoke.sh; it leaves the stack up so
# that a failure can be looked at.
set -uo pipefail
cd "$(dirname "$0")"

fails=0
checks=0
say()  { printf '  %-58s %s\n' "$1" "$2"; }
ok()   { checks=$((checks+1)); say "$1" "ok - $2"; }
bad()  { checks=$((checks+1)); fails=$((fails+1)); say "$1" "FAILED"; printf '      %s\n' "$2"; }

metrics() { curl -sf -u prometheus:prometheus http://localhost:9090/metrics; }
# **Two credentials, and the difference is the point.** The scraper's reaches
# /metrics and nothing else; the operator's reaches the inspection routes as
# well. A scraper's password lives in a monitoring configuration and is read
# by whoever runs monitoring, so it is the one to narrow.
inspect() { curl -sf -u operator:operator "http://localhost:9090$1"; }
code()    { curl -s -o /dev/null -w '%{http_code}' -u "$1" "http://localhost:9090$2"; }
# One value out of the catalogue by exact series name. Absent is not zero:
# a metric that is missing and a metric that is genuinely 0 are the same
# number to a caller, and several of the faults below are exactly that.
metric() { metrics | awk -v s="$1" '$1 == s { print $2; found=1 } END { if (!found) print "ABSENT" }'; }
# Store-derived gauges can lag a new stored position until the next refresh.
# Allow a full 60-second refresh interval plus margin, returning the last value
# on timeout so the caller makes the same pass/fail judgement as an early read.
metric_at_least() {
  local series="$1" minimum="$2" budget="${3:-90}" deadline value
  deadline=$(( $(date +%s) + budget ))
  while :; do
    value=$(metric "$series")
    if { [ "$value" != ABSENT ] && [ "${value%.*}" -ge "$minimum" ] 2>/dev/null; } \
        || [ "$(date +%s)" -ge "$deadline" ]; then
      printf '%s\n' "$value"
      return
    fi
    sleep 1
  done
}
kafka()  { docker compose exec -T kafka "$@" 2>/dev/null; }

echo "bringing the stack up"
# **It prints what compose said.** Discarding both streams here left the one
# step in this file that hides its evidence: a bring-up refused for a port
# already allocated reported only that nothing below would mean anything, and
# the cause had to be recovered by running compose again by hand.
if ! up=$(docker compose up -d --wait 2>&1); then
  echo "docker compose up --wait failed; nothing below would mean anything" >&2
  echo "$up" | tail -20 >&2
  exit 1
fi
# The generators need a moment to have produced something to measure.
sleep 20

echo
echo "the stack itself"

# 1. Every service running, and the one-shot topics job finished cleanly.
#    The bridge dead on boot from `processors: []` looked like this.
running=$(docker compose ps --format '{{.Service}} {{.State}}' | awk '$2 == "running" { print $1 }' | wc -l)
if [ "$running" -lt 10 ]; then
  bad "services running" "only $running are running; this stack has fifteen, so nothing below measured the demo"
  echo; echo "$fails failure(s) of $checks checks"; exit 1
fi
ok "services running" "$running"

notrunning=$(docker compose ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' |
             awk '$2 != "running" && !($1 == "kafka-topics" && $3 == 0) { print $1"("$2")" }')
if [ -n "$notrunning" ]; then
  bad "every service running or a job that exited 0" "$notrunning"
else
  ok "every service running or a job that exited 0" "kafka-topics exited 0"
fi

echo
echo "saguin"

# 2. The queue delivered. The worker that connected, subscribed to nothing
#    and never sent a job left 1,099 records dead-lettered unattempted, and
#    every container was healthy throughout.
delivered=$(metric 'saguin_queue_delivered_total{channel="jobs"}')
if [ "$delivered" = ABSENT ] || [ "${delivered%.*}" -le 0 ] 2>/dev/null; then
  bad "the queue delivered a job" "saguin_queue_delivered_total is $delivered - the worker took nothing"
else
  ok "the queue delivered a job" "$delivered delivered"
fi

# 2b. **The denominator the slowest-consumer alert is a reading from.**
#     `saguin_channel_position_min` on its own cannot tell one
#     straggler in a fleet from a single consumer that has stopped, and this
#     is the gauge that separates them.
#
#     **Checked against the pipelines this stack actually runs**, not
#     against a number above zero: the demo has readers on the channels, so
#     a zero here is the gauge reporting a fleet that is not there. A `>= 1`
#     that passes on any channel would pass on a broker where every consumer
#     had gone, which is the case it exists to show.
consumers=$(metric_at_least 'saguin_channel_consumers{channel="weather-measurement"}' 1)
if [ "$consumers" = ABSENT ]; then
  bad "the consumer count is published" \
    "saguin_channel_consumers is absent - the slowest-consumer alert has no denominator, so one straggler and a dead fleet read alike"
elif [ "${consumers%.*}" -lt 1 ] 2>/dev/null; then
  bad "the consumer count is published" \
    "saguin_channel_consumers is $consumers on a channel bento_mqtt_kafka_bridge reads with clean_start: false - the gauge is counting something other than durable consumers"
else
  ok "the consumer count is published" "$consumers on weather-measurement"
fi

# 2c. **The routes the catalogue cannot answer**, and the scopes that
#     decide who reaches them.
#
#     `saguin_channel_position_min` is one number per channel, so
#     one straggler in a fleet reads exactly like a fleet that has stopped;
#     `saguin_queue_jobs` says how many jobs are stuck and nothing says
#     which. Both answers are in the store, and neither can be a metric: a
#     client id is a string a client chose, so a series keyed by one is a
#     series count chosen by whoever connects.
#
#     **The 403 is asserted as well as the 200.** A route that answered
#     everybody would pass a check that only looked for its own output, and
#     the scraper credential reaching an inspection route is exactly the
#     widening the third field exists to prevent.
if ! consumers=$(inspect /v1/operations/consumers); then
  bad "the consumers route answers" "GET /v1/operations/consumers failed for the operator credential"
elif ! printf '%s' "$consumers" | grep -q '"channels"'; then
  bad "the consumers route answers" "the body carries no channels: $consumers"
else
  ok "the consumers route answers" "$(printf '%s' "$consumers" | grep -o '"channel":"[^"]*"' | wc -l) channels"
fi

if ! queue=$(inspect /v1/operations/queues/jobs); then
  bad "the queue view answers" "GET /v1/operations/queues/jobs failed for the operator credential"
elif printf '%s' "$queue" | grep -q '"payload"'; then
  bad "the queue view answers" "the body carries a payload, and reading records is MQTT's job"
else
  ok "the queue view answers" "$(printf '%s' "$queue" | grep -o '"total":[0-9]*' | head -1) unfinished jobs"
fi

got=$(code prometheus:prometheus /v1/operations/consumers)
if [ "$got" != 403 ]; then
  bad "the scraper credential is narrowed" \
    "the scraper reached /v1/operations/consumers with $got, want 403 - its password lives in a monitoring configuration and must not reach the inspection routes"
else
  ok "the scraper credential is narrowed" "403 on /v1/operations, 200 on /metrics"
fi

# Deliveries and visibility-timeout failures must increase between samples.
# A failed attempt is counted by its cause; the retry delay follows the failure.
# Dead-lettering is rarer, so it is checked separately for an exhausted job.
# Keep the five-minute budget and report elapsed time, so a slow lifecycle
# remains visible. The counters are live; a minute between polls is generous.
advances() {
  local series="$1" label="$2" budget="${3:-300}" was now deadline began waited
  was=$(metric "$series")
  if [ "$was" = ABSENT ]; then
    bad "$label" "$series is absent - an alert on it reads absent and 0 alike"
    return
  fi
  began=$(date +%s)
  deadline=$(( began + budget ))
  now=$was
  while [ "$(date +%s)" -lt "$deadline" ]; do
    sleep 60
    now=$(metric "$series")
    if [ "$now" != ABSENT ] && [ "${now%.*}" -gt "${was%.*}" ] 2>/dev/null; then
      waited=$(( $(date +%s) - began ))
      ok "$label" "$was -> $now, after ${waited}s"
      return
    fi
  done
  bad "$label" "$was then $now across ${budget}s - the total stands where it stopped"
}

echo "  ...waiting for the queue lifecycle to move"
advances 'saguin_queue_delivered_total{channel="jobs"}' "queue deliveries advance"
advances 'saguin_queue_attempts_failed_total{channel="jobs",cause="timeout"}' "queue visibility timeouts advance"

# Wait for a job to exhaust its attempts. The worker makes random choices,
# so one instantaneous sample cannot establish that the whole path works.
# The same reason is checked against Kafka below, together with provenance.
dl=$(metric 'saguin_queue_dead_lettered_total{channel="jobs",reason="attempts_exhausted"}')
dl_deadline=$(( $(date +%s) + 300 ))
while { [ "$dl" = ABSENT ] || [ "${dl%.*}" -le 0 ] 2>/dev/null; } \
      && [ "$(date +%s)" -lt "$dl_deadline" ]; do
  sleep 15
  dl=$(metric 'saguin_queue_dead_lettered_total{channel="jobs",reason="attempts_exhausted"}')
done
if [ "$dl" = ABSENT ] || [ "${dl%.*}" -le 0 ] 2>/dev/null; then
  bad "work has been dead-lettered" "saguin_queue_dead_lettered_total is $dl after waiting 300s - no job ran out of attempts in that window, so the half of the lifecycle this demo is most about has not happened"
else
  ok "work has been dead-lettered" "$dl"
fi

# Every channel and session shares one SQLite database in full mode.
store=$(metric 'saguin_store_info{mode="full"}')
bytes=$(metric_at_least saguin_store_bytes 1)
if [ "$store" != 1 ]; then
  bad "the shared SQLite store holds data" "saguin_store_info{mode=full} is $store, want 1"
elif [ "$bytes" = ABSENT ] || [ "${bytes%.*}" -le 0 ] 2>/dev/null; then
  bad "the shared SQLite store holds data" "saguin_store_bytes is $bytes, want a nonempty database"
else
  ok "the shared SQLite store holds data" "full mode, $bytes database bytes for every channel"
fi

echo
echo "kafka"

# 6. Each topic exists with the cleanup.policy it should have, read back
#    from Kafka. `water-location` was auto-created `delete`, mirroring a
#    `latest` channel as a log.
topics=0
for pair in "weather-measurement delete" "water-measurement delete" \
            "water-location compact" "jobs-dlq delete"; do
  set -- $pair
  topics=$((topics+1))
  # **Not a greedy `.*cleanup.policy=`**, which is what this first had.
  # Kafka prints the setting and then its synonyms on one line:
  #
  #   cleanup.policy=compact sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:
  #   cleanup.policy=compact, DEFAULT_CONFIG:log.cleanup.policy=delete}
  #
  # so a greedy match runs to the last one and reads the *default*,
  # `log.cleanup.policy=delete`. It reported the compacted topic as `delete`
  # - the exact fault this row exists to catch, invented by the instrument.
  got=$(kafka kafka-configs --bootstrap-server kafka:29092 --entity-type topics \
          --entity-name "$1" --describe |
        grep -oE '(^|[^.a-z])cleanup\.policy=[a-z]+' | head -1 |
        sed 's/.*cleanup\.policy=//')
  if [ "$got" != "$2" ]; then
    bad "$1 cleanup.policy" "Kafka says '${got:-absent}', want '$2'"
  else
    ok "$1 cleanup.policy" "$got"
  fi
done
[ "$topics" -eq 4 ] || bad "topics checked" "checked $topics, want 4"

# 7. **Existing is not the same as being fed**: the dead-letter channel
#    reached no topic at all and the topic still existed. So the offsets are
#    read, and read twice where the topic is a stream.
#
#    **Not every topic is a stream, and asserting that they all advance
#    fails on a healthy stack.** This first did, on water-location. That
#    topic mirrors a `latest` channel fed by an input whose own template
#    says it "fires exactly 100 times... then this input closes and never
#    fires again": a station's location does not change. Measured here at a
#    flat 100 over ninety seconds, with saguin_records_stored_total agreeing at
#    exactly 100, every one of them at start-up. A `latest` channel is the
#    thing that does not stream, which is most of why it is in this demo.
offsets() {
  kafka kafka-get-offsets --bootstrap-server kafka:29092 --topic "$1" |
    awk -F: '{ s += $3 } END { print s+0 }'
}

declare -A before
for t in weather-measurement water-measurement; do before[$t]=$(offsets "$t"); done
echo "  ...waiting 25s to see whether the streaming topics move"
sleep 25
for t in weather-measurement water-measurement; do
  now=$(offsets "$t")
  if [ "$now" -le "${before[$t]}" ]; then
    bad "$t is being fed" "offset ${before[$t]} then $now - the topic exists and nothing is arriving"
  else
    ok "$t is being fed" "${before[$t]} -> $now"
  fi
done

# The two that are not streams: they must have been reached, which is what
# the dead-letter fault was, and they are not asked to keep moving.
loc=$(offsets water-location)
if [ "$loc" -le 0 ]; then
  bad "water-location was reached" "offset $loc - the startup batch never arrived"
else
  ok "water-location was reached" "$loc (a start-up batch, not a stream)"
fi

dlq=$(offsets jobs-dlq)
dead=$(metric 'saguin_queue_dead_lettered_total{channel="jobs",reason="attempts_exhausted"}')
if [ "$dlq" -le 0 ]; then
  bad "jobs-dlq was reached" "offset $dlq - dead-lettered work reached no topic at all"
elif [ "$dead" = ABSENT ] || [ "${dead%.*}" -le 0 ] 2>/dev/null; then
  bad "jobs-dlq was reached" "the topic holds $dlq and saguin says it dead-lettered $dead"
else
  # Two readings of one event, from the two ends: saguin says it
  # dead-lettered, and Kafka says it received. Either alone can be true
  # while the path between them is broken.
  ok "jobs-dlq was reached" "$dlq in Kafka, $dead dead-lettered by saguin"
fi

# **The schema registry crosses too.** The Kafka side holds the payloads;
# without this it does not hold the thing that says what they mean, and a
# consumer outside the broker has to be told out of band - which is the
# second service a registry in a `latest` channel exists to avoid.
sch=$(offsets schemas)
if [ "$sch" -le 0 ]; then
  bad "schemas was reached" "offset $sch - the registry stayed inside the broker, so the Kafka topics carry payloads nothing defines"
else
  ok "schemas was reached" "$sch definitions"
fi

# **The provenance crosses with the record, and this is the check that says
# so.** A dead letter whose `saguin-dlq-*` headers were dropped is a payload
# nobody can trace: the topic would say a job failed and nothing about which
# queue it came from, how many attempts it had, or why it was given up on.
# The Kafka output filters metadata by prefix, so losing them is one edit
# away and silent.
hdrs=$(kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic jobs-dlq \
        --from-beginning --max-messages 1 --timeout-ms 30000 \
        --formatter-property print.headers=true --formatter-property print.value=false)
missing=""
for want in saguin-dlq-reason saguin-dlq-channel saguin-dlq-attempts saguin-id; do
  printf '%s' "$hdrs" | grep -q "$want" || missing="$missing $want"
done
if [ -n "$missing" ]; then
  bad "the dead letter carries its provenance" "no Kafka header for:$missing - the record crossed and the reason it failed did not"
else
  ok "the dead letter carries its provenance" "reason, channel, attempts and id are Kafka headers"
fi

echo
echo "bento"

# 8. Both pipelines are sending. A pipeline can report healthy and produce
#    nothing, which is exactly what a stalled bridge did.
for svc in iot-datagen mqtt2kafka; do
  a=$(docker compose exec -T "$svc" wget -qO- http://localhost:4195/metrics 2>/dev/null |
      awk '/^output_sent/ { s += $2 } END { print s+0 }')
  sleep 6
  b=$(docker compose exec -T "$svc" wget -qO- http://localhost:4195/metrics 2>/dev/null |
      awk '/^output_sent/ { s += $2 } END { print s+0 }')
  if [ "${b:-0}" -le "${a:-0}" ]; then
    bad "$svc output_sent advances" "$a then $b - healthy and producing nothing"
  else
    ok "$svc output_sent advances" "$a -> $b"
  fi
done

echo
if [ "$checks" -lt 20 ]; then
  echo "only $checks checks ran, which is fewer than this script contains:" >&2
  echo "  something exited early and the result above is about work it did not do" >&2
  exit 1
fi
if [ "$fails" -gt 0 ]; then
  echo "$fails of $checks checks FAILED - the stack is left up to look at"
  exit 1
fi
echo "$checks checks passed; the stack is left up ('docker compose down -v' in examples/bento-connectors)"
