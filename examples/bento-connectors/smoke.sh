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
# the Confluent images. `make smoke` runs it, and it leaves the stack up so
# that a failure can be looked at.
set -uo pipefail
cd "$(dirname "$0")"

# **The viewer is built from its own repository, so say so before docker does.**
# It moved to ifnesi/saguin-viewer, and this stack builds it from a checkout: the
# default is a sibling of saguin, and SAGUIN_VIEWER_DIR points anywhere else.
# Without this, a missing or mistyped checkout surfaces as a docker build error
# about a context path, which reads as a broken demo rather than as a clone that
# is not where this expected.
#
# The resolved path is echoed rather than the default, so a wrong
# SAGUIN_VIEWER_DIR fails naming the path it actually tried.
viewer_dir="${SAGUIN_VIEWER_DIR:-../../../saguin-viewer}"
if [ ! -f "$viewer_dir/web/Dockerfile" ]; then
  printf 'the viewer is built from its own repository and this is not it:\n' >&2
  printf '  %s\n' "$(cd "$(dirname "$viewer_dir")" 2>/dev/null && pwd || echo "$viewer_dir")/$(basename "$viewer_dir")" >&2
  printf 'clone it beside saguin:\n' >&2
  printf '  git clone https://github.com/ifnesi/saguin-viewer.git %s\n' "$viewer_dir" >&2
  printf 'or point SAGUIN_VIEWER_DIR at a clone elsewhere.\n' >&2
  exit 1
fi

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
  bad "services running" "only $running are running; this stack has fourteen, so nothing below measured the demo"
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
#     `saguin_channel_consumer_position_min` on its own cannot tell one
#     straggler in a fleet from a single consumer that has stopped, and this
#     is the gauge that separates them.
#
#     **Checked against the pipelines this stack actually runs**, not
#     against a number above zero: the demo has readers on the channels, so
#     a zero here is the gauge reporting a fleet that is not there. A `>= 1`
#     that passes on any channel would pass on a broker where every consumer
#     had gone, which is the case it exists to show.
consumers=$(metric 'saguin_channel_consumers{channel="weather-measurement"}')
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
#     `saguin_channel_consumer_position_min` is one number per channel, so
#     one straggler in a fleet reads exactly like a fleet that has stopped;
#     `saguin_queue_depth` says how many jobs are stuck and nothing says
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
  ok "the queue view answers" "$(printf '%s' "$queue" | grep -o '"unresolved":[0-9]*' | head -1) unresolved"
fi

got=$(code prometheus:prometheus /v1/operations/consumers)
if [ "$got" != 403 ]; then
  bad "the scraper credential is narrowed" \
    "the scraper reached /v1/operations/consumers with $got, want 403 - its password lives in a monitoring configuration and must not reach the inspection routes"
else
  ok "the scraper credential is narrowed" "403 on /v1/operations, 200 on /metrics"
fi

# 3. **The retry and dead-letter counters move, not merely exist.** A job
#    being consumed, retried and dead-lettered is the lifecycle this demo is
#    here to show, and every one of those numbers reads plausibly on a queue
#    that has stopped: they are totals, so they keep whatever they reached.
#    What says the lifecycle is still running is that they move.
#    **Two of the three are asked to move and the third is not**, and the
#    difference is not a softening. Delivering and taking back happen every
#    few seconds; dead-lettering takes a job failing its every attempt, so
#    it is rare by design - measured at 10 then 10 across a thirty-second
#    window on a stack that was working perfectly, and four in ninety
#    seconds when it did move. That it has happened at all is asserted here,
#    and twice more below from the other end: the Kafka topic holding
#    records, and the viewer showing them with the reason each failed.
#
#    **Waited for rather than timed, and the paragraph above is why.** This
#    used to sample, sleep thirty seconds, and sample again - the fixed
#    window it had just called a coin flip, applied to `redelivered`.
#    It came up tails once: `2 then 2` on a queue that was
#    working, with the counter at 22 two minutes later. Reproducing it took
#    some doing and is the reason this is a wait and not a wider window:
#    over 457 seconds idle the longest gap between redeliveries was 14.2s
#    and no thirty-second window was empty, and saturating all sixteen
#    cores changed the rate not at all (21.6/min against 23.2/min). But six
#    consecutive runs of the old pattern gave deltas of 8, 5, 4, 6, 8 and
#    **2**, so the margin reaches two on an idle machine and a busy one
#    reached zero. A weighted coin is still a coin.
#
#    Polling keeps the assertion - the total must *move*, which is what
#    separates a running lifecycle from one that stopped - and removes the
#    flip, because it only fails if nothing happens for the whole budget.
#    It is usually faster than the sleep it replaced. The interval is not
#    below the operations listener's `min_scrape_interval`, or most polls
#    would re-read a catalogue the broker has not rebuilt.
#
#    **The budget is four minutes because the longest quiet stretch anybody
#    has measured is 93 seconds**, seen on a stack
#    that shared the machine with a ten-minute soak and five ten-minute
#    fuzzers across a 51-minute round. It was two minutes until that
#    measurement existed, which is 27 seconds of headroom over the worst
#    case - the kind of margin this check already failed on once. Four
#    attempts to reproduce those stalls all came back under 15 seconds
#    (idle, sixteen cores saturated, the soak alone, and the soak with five
#    fuzzers), so the cause is still unknown and the budget is sized to the
#    observation rather than to an explanation. It costs nothing when the
#    stack is healthy: the check returns on the first poll.
#
#    **The poll is a minute because the catalogue is rebuilt at most that
#    often**, which is `min_scrape_interval`'s floor. Polling faster re-reads
#    a catalogue the broker has not rebuilt, so every extra poll is a
#    guaranteed miss.
#
#    **And the budget went from four minutes to five for the same reason.**
#    A stall is now only *observable* once a minute, so the 93-second worst
#    case anybody has measured can take up to 153 seconds to show - four
#    minutes left 87 seconds of headroom over that, which is thinner than
#    the margin this check already failed on once.

# advances waits for a counter to rise, and says so when it does not.
#
# **It reports how long it waited, and that is not decoration.** A wait
# tolerates a lifecycle that is merely slow, where the fixed window it
# replaced would have failed - so if it does not print the elapsed time, it
# hides exactly the case nobody has explained. One run saw two
# redeliveries in the first minute where this machine does about twenty,
# and three attempts to reproduce that failed: idle, sixteen cores
# saturated, and the sqlite soak running beside it all gave 21–27 a minute
# with the longest gap under 15s. Something made that run different and
# nobody knows what. Passing on the first poll and passing on the seventh
# are different facts, and the number is how a later round sees which
# happened rather than reading `ok` and moving on.
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
    sleep 60                     # >= min_scrape_interval; see above
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
for c in delivered redelivered; do
  advances "saguin_queue_${c}_total{channel=\"jobs\"}" "queue $c advances"
done

# **Waited for, not sampled.** A job dead-letters only once its three
# attempts are exhausted - about three and a half minutes from a cold
# start - and the catalogue recomputes at most once a minute, so on a cold
# stack the first dead-letter arrives after this line does. A single read
# here failed 4 of 28 checks on a cold run while the broker's log showed
# the dead-letter landing seconds later, correct in every respect. A warm
# stack answers on the first read and waits nothing. The three checks
# below that open the __dlq channel inherit this ordering: the metric
# trails the event, so by the time it moves, the record has long since
# reached the viewer's tree and the Kafka mirror.
dl=$(metric 'saguin_queue_dead_lettered_total{channel="jobs"}')
dl_deadline=$(( $(date +%s) + 300 ))
while { [ "$dl" = ABSENT ] || [ "${dl%.*}" -le 0 ] 2>/dev/null; } \
      && [ "$(date +%s)" -lt "$dl_deadline" ]; do
  sleep 15
  dl=$(metric 'saguin_queue_dead_lettered_total{channel="jobs"}')
done
if [ "$dl" = ABSENT ] || [ "${dl%.*}" -le 0 ] 2>/dev/null; then
  bad "work has been dead-lettered" "saguin_queue_dead_lettered_total is $dl after waiting 300s - no job ran out of attempts in that window, so the half of the lifecycle this demo is most about has not happened"
else
  ok "work has been dead-lettered" "$dl"
fi

# 4. Every configured provider holds something. `volatile` was a flat zero
#    because no channel used it, which is a configuration nobody meant.
providers=0; emptyprov=""
while read -r p; do
  [ -z "$p" ] && continue
  providers=$((providers+1))
  b=$(metric "saguin_provider_bytes{provider=\"$p\"}")
  if [ "$b" = ABSENT ] || [ "${b%.*}" -le 0 ] 2>/dev/null; then
    emptyprov="$emptyprov $p($b)"
  fi
done < <(metrics | sed -n 's/^saguin_provider_info{provider="\([^"]*\)".*/\1/p')
if [ "$providers" -lt 2 ]; then
  bad "every provider holds something" "read $providers providers, and this demo configures two"
elif [ -n "$emptyprov" ]; then
  bad "every provider holds something" "empty:$emptyprov - a provider no channel uses"
else
  ok "every provider holds something" "$providers providers"
fi

echo
echo "the viewer"

# 4. Every channel a viewer can see is in its tree. `jobs` and `jobs__dlq`
#    were missing from it entirely.
#
#    **A queue is excluded, and that is not a loophole.** The viewer never
#    subscribes to one: a wildcard never reaches a queue, and a viewer that
#    could would be taking jobs from the worker. The `__dlq` it derives is
#    an append channel and must be there.
tree=$(curl -sf http://localhost:4000/api/tree)
if [ -z "$tree" ]; then
  bad "the viewer answers" "no /api/tree at all"
else
  seen=$(printf '%s' "$tree" | python3 -c 'import json,sys; print(" ".join(sorted({r["channel"] for r in json.load(sys.stdin) if r.get("channel")})))')
  want=$(metrics | sed -n 's/^saguin_channel_info{channel="\([^"]*\)".*type="\([^"]*\)".*/\2 \1/p' | awk '$1 != "queue" { print $2 }' | sort | tr '\n' ' ')
  missing=""
  for c in $want; do
    case " $seen " in *" $c "*) ;; *) missing="$missing $c" ;; esac
  done
  nwant=$(printf '%s' "$want" | wc -w)
  if [ "$nwant" -lt 4 ]; then
    bad "every non-queue channel is in the viewer's tree" "read $nwant channels from the catalogue, which cannot be this demo"
  elif [ -n "$missing" ]; then
    bad "every non-queue channel is in the viewer's tree" "missing:$missing"
  else
    ok "every non-queue channel is in the viewer's tree" "$nwant channels"
  fi
fi

# 5. **Every payload is shown as something**, whatever it turned out to be.
#
#    This once grepped for `no schema for msg_type`, which the page written
#    for this stack printed in place of a record that had arrived perfectly.
#    No viewer prints that string any more, so the check passed by comparing
#    nothing - the exact failure this suite exists to catch.
#
#    What it asserts instead is the promise the viewer actually makes: the
#    bytes are shown in every case, whether or not anything decoded them.
#    This stack sends protobuf, so most arrive as `bytes` with a hex
#    rendering, and the page draws the decoded record beside them rather
#    than in their place. **The check stays on the bytes** - that is the
#    promise that holds however the image is built, and a check on the
#    decoded card would pass or fail on which decoders happened to be
#    installed rather than on anything the viewer did.
shown=0; blank=0; sampled=0
while read -r topic; do
  [ -z "$topic" ] && continue
  sampled=$((sampled+1))
  counts=$(curl -sf --get --data-urlencode "topic=$topic" http://localhost:4000/api/messages |
    python3 -c '
import json,sys
good = bad = 0
for m in json.load(sys.stdin).get("messages", []):
    p = m.get("payload") or {}
    # Something renderable, and a byte count beside it. An empty payload is
    # a delete on a latest channel and is named rather than blank.
    if p.get("kind") == "empty" or p.get("text") is not None \
       or p.get("hex") is not None or p.get("value") is not None:
        good += 1
    else:
        bad += 1
print(good, bad)
' 2>/dev/null)
  set -- ${counts:-0 0}
  shown=$((shown + $1)); blank=$((blank + $2))
done < <(printf '%s' "$tree" | python3 -c 'import json,sys; [print(r["topic"]) for r in json.load(sys.stdin)[:12]]')
if [ "$sampled" -lt 4 ]; then
  bad "every payload is shown as something" "sampled $sampled topics, which is too few to have looked"
elif [ "$shown" -eq 0 ]; then
  bad "every payload is shown as something" "sampled $sampled topics and read no payloads at all"
elif [ "$blank" -gt 0 ]; then
  bad "every payload is shown as something" "$blank of $((shown+blank)) payloads render as nothing"
else
  ok "every payload is shown as something" "$shown payloads across $sampled topics"
fi

# 5. **The queue is in the viewer's channel list, with its depth.** It once
#    vanished from that list entirely. It is not in the topic tree and must
#    not be - the viewer never subscribes to a queue, because a wildcard
#    never reaches one and a viewer that could would be taking jobs from the
#    worker - so the channel list is the only place a person sees it.
state=$(curl -sf http://localhost:4000/api/state)
qline=$(printf '%s' "$state" | python3 -c '
import json,sys
d=json.load(sys.stdin)
for c in d.get("channels", []):
    if c.get("type") == "queue":
        print(c["name"], c.get("held"), c.get("read"))
' 2>/dev/null)
if [ -z "$qline" ]; then
  bad "the queue is in the viewer's channel list" "no queue channel in /api/state at all"
else
  set -- $qline
  if [ "$2" = None ] || [ -z "${2:-}" ]; then
    bad "the queue is in the viewer's channel list" "$1 is listed with no depth beside it"
  else
    ok "the queue is in the viewer's channel list" "$1, depth $2, read=$3"
  fi
fi

# The viewer's answers travel over its own `viewer-reply/<id>` subscription,
# and an acl_file that forgets the prefix loses every point read and seek
# reply silently - the page degrades to working without them. This demo has
# no acl_file, so today everything is granted and this check is the standing
# guard for the day one appears. The count is printed so a viewer that
# answered nothing cannot pass as one with nothing refused.
subline=$(printf '%s' "$state" | python3 -c '
import json,sys
d=json.load(sys.stdin)
subs=d.get("subscriptions") or []
refused=[s["filter"] for s in subs if not s.get("granted")]
print(str(len(subs)) + "|" + " ".join(refused))
' 2>/dev/null)
subcount=${subline%%|*}
refused=${subline#*|}
if [ -z "$subline" ] || [ "${subcount:-0}" -eq 0 ]; then
  bad "every viewer subscription is granted" "no subscriptions readable in /api/state, so nothing was examined"
elif [ -n "$refused" ]; then
  bad "every viewer subscription is granted" "refused: $refused - an acl_file must grant viewer-reply/+ read"
else
  ok "every viewer subscription is granted" "$subcount subscriptions, none refused"
fi

# 6. **The dead-lettered work is there to open**, with the reason it failed.
#    A channel that exists and holds nothing a person can read is the same
#    to them as one that was never reached.
dlqtopic=$(printf '%s' "$tree" | python3 -c '
import json,sys
# **A broadcast row carries a null channel.** The viewer subscribes to `#`
# and shows topics no channel claims, which the page written for this stack
# never did - so this once read `r["channel"].endswith(...)`, threw on the
# first broadcast topic, and the 2>/dev/null turned that into "no __dlq
# topic in the viewer'"'"'s tree". A check that fails for the wrong reason is
# worse than one that does not run.
for r in json.load(sys.stdin):
    if (r.get("channel") or "").endswith("__dlq"):
        print(r["topic"]); break
' 2>/dev/null)
if [ -z "$dlqtopic" ]; then
  bad "dead-lettered work is there to open" "no __dlq topic in the viewer's tree"
else
  msgs=$(curl -sf --get --data-urlencode "topic=$dlqtopic" http://localhost:4000/api/messages)
  n=$(printf '%s' "$msgs" | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("messages", [])))' 2>/dev/null)
  reasons=$(printf '%s' "$msgs" | grep -c "saguin-dlq-reason")
  if [ "${n:-0}" -eq 0 ]; then
    bad "dead-lettered work is there to open" "$dlqtopic holds no messages a person can read"
  elif [ "$reasons" -eq 0 ]; then
    bad "dead-lettered work is there to open" "$n message(s) on $dlqtopic and none says why it failed"
  else
    ok "dead-lettered work is there to open" "$n on $dlqtopic, carrying saguin-dlq-reason"
  fi
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
#    flat 100 over ninety seconds, with saguin_published_total agreeing at
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
dead=$(metric 'saguin_queue_dead_lettered_total{channel="jobs"}')
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
