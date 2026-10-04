#!/usr/bin/env bash
# Does this stack actually work?
#
# Every check prints pass or fail and the script counts them, because a sweep
# that silently matched nothing is a sweep reporting on work it did not do.
# The count at the end is compared against the number of checks defined, so
# losing one to a typo fails the run rather than shortening it.
set -uo pipefail

COMPOSE=${COMPOSE:-docker compose}
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-1883}
OPS=${OPS:-http://127.0.0.1:9091}
VIEWER=${VIEWER:-http://127.0.0.1:4001}
HUB_USER=zigbee2mqtt-hub-1
HUB_PASS=hub-password-change-me
# Its own credential, NOT the viewer container's. acl.yaml binds a viewer
# credential to the client id that is its name, so two clients under one name
# would evict each other - and the symptom is a check that intermittently
# sees nothing, which reads as a broken broker. `viewer-*` matches the same
# role, so this reads exactly what the viewer reads.
VIEW_USER=viewer-smoke
VIEW_PASS=smoke-password-change-me
# The radio gateway's credential, used only to prove what it may NOT do.
RF_USER=rtl433-hub-1
RF_PASS=rf-password-change-me
EXPECTED_CHECKS=32
passed=0
failed=0
ran=0

ok()   { ran=$((ran+1)); passed=$((passed+1)); printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
bad()  { ran=$((ran+1)); failed=$((failed+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$1"; [ -n "${2:-}" ] && printf '        %s\n' "$2"; }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3" "wanted '$2', got '$1'"; fi; }

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "$1 is not on PATH, and this script cannot check anything without it" >&2
    exit 2
  }
}
need mosquitto_pub
need mosquitto_sub

newest() { # topic -> the LAST record, which on an append channel is the
           # current one. Taking the first would take the oldest, which is
           # how a plug that was on read as off for an hour.
  timeout 14 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 \
    -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
    -t "$1" -v -W 15 2>/dev/null | tail -1
}

rfsub() { # the same, with a window the radio half needs
  # **Four seconds is a coin flip on this side and not on the other.** The
  # Zigbee devices report every ten seconds into channels that hold state, so
  # a short window either finds a live message or is served the stored one.
  # Nothing on the radio side is like that: `rtl433/events` is broadcast and
  # stored nowhere, and `rf_readings` carries `start: tail`, so a fresh
  # subscriber is served nothing until the next transmission. The transmitter
  # sends a round of three every thirty seconds, so one arrives every ten,
  # and this window is four times that.
  timeout 60 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 \
    -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
    -t "$1" -v -W 40 -C "${2:-1}" 2>/dev/null
}

sub() { # topic [count] -> "<topic> <payload>" lines, or empty
  # -v, so each line carries its topic. Without it the payload arrives alone
  # and any check counting topics matches nothing while every check that only
  # asks "was there output" still passes - which is a check reporting on work
  # it did not do.
  timeout 24 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 \
    -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
    -t "$1" -v -W 4 -C "${2:-1}" 2>/dev/null
}

echo "waiting for the stack"
for _ in $(seq 1 60); do
  curl -fsS -u operator:operator-password-change-me "$OPS/health" >/dev/null 2>&1 && break
  sleep 2
done

echo
echo "the broker"
curl -fsS -u operator:operator-password-change-me "$OPS/health" >/dev/null 2>&1 && ok "operations listener answers /health" || bad "operations listener answers /health"
curl -fsS -u operator:operator-password-change-me "$OPS/metrics" 2>/dev/null | grep -q "^saguin_" && ok "/metrics serves saguin's own series" || bad "/metrics serves saguin's own series"
curl -fsS "$OPS/metrics" >/dev/null 2>&1 && bad "/metrics without credentials is refused" || ok "/metrics without credentials is refused"

echo
echo "authentication and identity"
mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "$VIEW_USER" -u "$VIEW_USER" -P "wrong-password" \
  -t 'zigbee2mqtt/living-room-sensor/set' -m 'x' >/dev/null 2>&1 \
  && bad "a wrong password is refused" \
  || ok "a wrong password is refused"

# The credential is bound to its client id, so the same password from another
# client id must not work. This is the check that would catch `client_ids`
# being dropped from acl.yaml, which nothing else here would notice.
mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "borrowed-id" -u "$VIEW_USER" -P "$VIEW_PASS" \
  -t 'zigbee2mqtt/living-room-sensor/set' -m 'x' >/dev/null 2>&1 \
  && bad "the credential is bound to its client id" \
  || ok "the credential is bound to its client id"

echo
echo "what each principal may do"
out=$(mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
  -t 'zigbee2mqtt/living-room-sensor' -m '{"temperature":99}' -d 2>&1)
echo "$out" | grep -q "RC:135" && ok "a viewer may NOT write a device topic (0x87)" || bad "a viewer may NOT write a device topic (0x87)" "$out"

out=$(mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
  -t 'zigbee2mqtt/living-room-sensor/set' -m '{"state":"ON"}' -d 2>&1)
echo "$out" | grep -q "RC:0" && ok "a viewer MAY write a command topic" || bad "a viewer MAY write a command topic" "$out"

echo
echo "zigbee2mqtt is really running"
[ -n "$(sub 'zigbee2mqtt/bridge/state')" ] && ok "the bridge publishes its state" || bad "the bridge publishes its state"
[ -n "$(sub 'zigbee2mqtt/bridge/info')" ] && ok "the bridge publishes its info" || bad "the bridge publishes its info"
sub 'zigbee2mqtt/bridge/info' | grep -q '"type":"ZStack3x0"' \
  && ok "it is talking to the emulated coordinator" || bad "it is talking to the emulated coordinator"

echo
echo "the channels"
[ -n "$(newest 'zigbee2mqtt/living-room-sensor')" ] && ok "readings are arriving" || bad "readings are arriving"

# `devices` carries `start: tail`, so a reader that has never been here is
# served live readings rather than a week of history. Its own credential, so
# this cannot disturb the position the resume check below depends on.
# One reporting round is three publishes, because the sensor reports
# temperature, humidity and battery as three cluster frames and zigbee2mqtt
# republishes the merged state after each. So a few seconds of live traffic is
# a handful; a `floor` replay on a channel that has been filling is dozens.
cold=$(timeout 14 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 \
  -i viewer-cold -u viewer-cold -P cold-password-change-me \
  -t 'zigbee2mqtt/living-room-sensor' -v -W 8 2>/dev/null | grep -c '^zigbee2mqtt/')
[ "$cold" -le 12 ] && ok "a first-time reader is served live, not the backlog ($cold in 8s)" \
  || bad "a first-time reader is served live, not the backlog" "saw $cold, which looks like a replay"

# And the claim that matters: a client that kept its session resumes at its own
# position and is served what it missed. This is what `start: tail` does NOT
# affect, and saying so is the point of checking both.
#
# **Its own credential.** A durable session is state this script leaves
# behind, and under the identity every other check reads with it changed what
# the next run saw - a suite that passes once and fails the second time is
# worse than no suite. A dedicated credential keeps that state where it
# belongs.
#
# No clean-start wipe first, deliberately: discarding a session and creating
# one a moment later races, and it made the FIRST run for a fresh identity
# fail while the second passed. It is also unnecessary - the read below
# advances the position wherever it starts, so a leftover one changes nothing.
RESUME_USER=viewer-resume
RESUME_PASS=resume-password-change-me

# A durable session, held long enough to take a position.
timeout 16 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 -c -x 300 \
  -i "$RESUME_USER" -u "$RESUME_USER" -P "$RESUME_PASS" \
  -t 'zigbee2mqtt/living-room-sensor' -W 12 >/dev/null 2>&1
sleep 25
missed=$(timeout 14 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 -c -x 300 \
  -i "$RESUME_USER" -u "$RESUME_USER" -P "$RESUME_PASS" \
  -t 'zigbee2mqtt/living-room-sensor' -v -W 6 2>/dev/null | grep -c '^zigbee2mqtt/')
[ "$missed" -ge 2 ] && ok "a returning session is served what it missed ($missed)" \
  || bad "a returning session is served what it missed" "saw $missed"
# Long enough for every device to report at least once, since a cold reader
# is served live traffic rather than what the channel already holds.
n=$(timeout 26 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 \
  -i viewer-cold -u viewer-cold -P cold-password-change-me \
  -t 'zigbee2mqtt/+' -v -W 20 2>/dev/null | grep -oE '^zigbee2mqtt/[a-z-]+' | sort -u | wc -l)
[ "$n" -ge 3 ] && ok "every device reports into the tree ($n)" || bad "every device reports into the tree" "saw $n"

# The readings must be zigbee2mqtt's own decoding, not something shaped like
# it. linkquality is added by zigbee2mqtt from the radio frame and could not
# be there if anything had published this topic directly.
# Read once and test the reading twice. Two subscriptions in a row under one
# client id is two clients with one identity, and MQTT resolves that by
# evicting one - so the second check would fail for a reason that has nothing
# to do with what it is asking.
reading=$(newest 'zigbee2mqtt/living-room-sensor')

echo "$reading" | grep -q '"linkquality"' \
  && ok "readings carry linkquality, so zigbee2mqtt decoded them" \
  || bad "readings carry linkquality, so zigbee2mqtt decoded them" "got: $reading"

echo "$reading" | grep -qE '"temperature":[0-9.-]+' \
  && ok "the sensor reports a temperature" \
  || bad "the sensor reports a temperature" "got: $reading"

# The append channel, replaying. A latest channel would serve one.
events=$(sub 'zigbee2mqtt/bridge/event' 3 | grep -c device_announce)
[ "$events" -ge 2 ] && ok "the append channel replays bridge events ($events)" \
  || bad "the append channel replays bridge events" "saw $events"


# The command topic must NOT be claimed by a channel: it is broadcast, and a
# fresh subscriber must not be handed a stale command as though it were
# current state.
[ -z "$(sub 'zigbee2mqtt/living-room-sensor/set')" ] \
  && ok "the command topic is broadcast, not stored" \
  || bad "the command topic is broadcast, not stored"

echo
echo "durability, which is the point"
before=$(newest 'zigbee2mqtt/living-room-sensor')
# Who the broker is holding a session for, asked before the restart so the
# check afterwards is about the same session rather than about whatever
# reconnected in the meantime. The resume check above left this one.
held_before=$(curl -fsS -u operator:operator-password-change-me "$OPS/v1/operations/sessions" 2>/dev/null \
  | grep -c '"client_id":"viewer-resume"')
$COMPOSE restart saguin >/dev/null 2>&1
for _ in $(seq 1 30); do curl -fsS -u operator:operator-password-change-me "$OPS/health" >/dev/null 2>&1 && break; sleep 2; done
after=$(newest 'zigbee2mqtt/living-room-sensor')
[ -n "$after" ] && ok "device readings survive a broker restart" || bad "device readings survive a broker restart" "before='$before' after='$after'"

# **And the sessions survive it too**, which is the half a restart used to
# take: every client was answered "I have no state for you", re-subscribed,
# and started again. The broker now puts back what its session store kept,
# and says how many it put back.
restored=$(curl -fsS -u operator:operator-password-change-me "$OPS/metrics" 2>/dev/null \
  | awk '/^saguin_sessions_restored_total /{print $2}')
[ -n "$restored" ] && [ "${restored%.*}" -ge 1 ] \
  && ok "the restarted broker put its sessions back ($restored)" \
  || bad "the restarted broker put its sessions back" "saguin_sessions_restored_total='$restored'"

# The same session, by name, on both sides of the restart - which is what
# "put back" has to mean for the device that owns it.
held_after=$(curl -fsS -u operator:operator-password-change-me "$OPS/v1/operations/sessions" 2>/dev/null \
  | grep -c '"client_id":"viewer-resume"')
[ "$held_before" -ge 1 ] && [ "$held_after" -ge 1 ] \
  && ok "a held session is still the broker's after the restart" \
  || bad "a held session is still the broker's after the restart" \
       "before=$held_before after=$held_after"

echo
echo "the command path, end to end"
# Last, deliberately. It restarts nothing, but the durability check above
# does, and a publish that lands while the broker is restarting is refused -
# which looks exactly like a command that did not reach the device.
sleep 5
before=$(newest 'zigbee2mqtt/kitchen-plug' | grep -oE '"state":"[A-Z]+"')
want=ON; [ "$before" = '"state":"ON"' ] && want=OFF
mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
  -t 'zigbee2mqtt/kitchen-plug/set' -m "{\"state\":\"$want\"}" >/dev/null 2>&1
# Poll rather than sleep a fixed time. The device reports on its own timer and
# the viewer is watching the same topic, so a single read a few seconds later
# is a race - and a race in a check is worse than no check, because it fails
# on somebody else's machine and gets ignored.
after=""
for _ in $(seq 1 8); do
  after=$(newest 'zigbee2mqtt/kitchen-plug' | grep -oE '"state":"[A-Z]+"')
  [ "$after" = "\"state\":\"$want\"" ] && break
done
[ "$after" = "\"state\":\"$want\"" ] \
  && ok "a command reaches the device and its new state comes back ($before -> $after)" \
  || bad "a command reaches the device and its new state comes back" "wanted $want, before=$before after=$after"


echo
echo "the radio half: rtl_433, a second gateway that shares nothing with the first"
# The readings themselves. rtl_433 publishes one topic per field, so a
# temperature is a topic of its own rather than a key in a document - which
# is why `rf_readings` claims four levels of wildcard.
[ -n "$(rfsub 'rtl433/devices/+/+/+/temperature_C')" ] \
  && ok "433MHz readings arrive, decoded by rtl_433" \
  || bad "433MHz readings arrive, decoded by rtl_433"

# Three sensors, not one. A single emulated device would pass every other
# check here while proving nothing about a gateway carrying a household.
n=$(timeout 45 mosquitto_sub -h "$HOST" -p "$PORT" -V 5 -i "$VIEW_USER" -u "$VIEW_USER" -P "$VIEW_PASS" \
      -t 'rtl433/devices/+/+/+/id' -v -W 45 2>/dev/null | awk '{print $2}' | sort -u | wc -l)
[ "$n" -eq 3 ] && ok "all three radio sensors are heard ($n)" \
               || bad "all three radio sensors are heard" "distinct ids seen: $n, wanted 3"

# The gateway's own state, held as a current value rather than a history.
# It is a retained Will, which is why saguin needs a `retained:` store to
# accept rtl_433's connection at all.
[ "$(sub 'rtl433/availability' | awk '{print $2}')" = "online" ] \
  && ok "the radio gateway's availability is held as current state" \
  || bad "the radio gateway's availability is held as current state"

# **The separation, which is the reason there are two gateways here.**
# A stolen radio credential must not be able to invent a Zigbee reading.
out=$(mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i rtl_433-smoke -u "$RF_USER" -P "$RF_PASS" \
  -t 'zigbee2mqtt/living-room-sensor' -m '{"temperature":99}' -d 2>&1)
echo "$out" | grep -q "RC:135" \
  && ok "the radio gateway may NOT write a Zigbee topic (0x87)" \
  || bad "the radio gateway may NOT write a Zigbee topic (0x87)" "$out"

# And the other way round, so neither is trusted with the other's data.
out=$(mosquitto_pub -h "$HOST" -p "$PORT" -V 5 -q 1 -i "$HUB_USER" -u "$HUB_USER" -P "$HUB_PASS" \
  -t 'rtl433/devices/Nexus-TH/1/90/temperature_C' -m '99' -d 2>&1)
echo "$out" | grep -q "RC:135" \
  && ok "the Zigbee bridge may NOT write a radio topic (0x87)" \
  || bad "the Zigbee bridge may NOT write a radio topic (0x87)" "$out"

# rtl_433 also publishes each reading again as one JSON document on
# `rtl433/events`. No channel claims it, so it is ordinary broadcast: live
# to whoever is listening and stored nowhere. Storing the same readings
# twice would be the demo teaching a habit worth avoiding.
route=$($COMPOSE exec -T saguin saguin --route /etc/saguin/saguin.yaml 'rtl433/events' 2>/dev/null)
echo "$route" | grep -qi 'no channel\|broadcast' \
  && ok "the events topic is broadcast, not stored a second time" \
  || bad "the events topic is broadcast, not stored a second time" "$route"

# **And that the gateway is allowed to publish there**, which routing does
# not say. A `channel:` rule does not reach a broadcast topic, so the role
# needs a `topic:` rule as well - and without it rtl_433 is refused 0x87 once
# per reading while every other check here still passes. That is what
# happened: the only place that said so was the broker's log, which is why
# this check asks for the message rather than for the route.
[ -n "$(rfsub 'rtl433/events')" ] \
  && ok "the gateway may actually publish the events topic" \
  || bad "the gateway may actually publish the events topic" \
        "routing says broadcast, but nothing arrives: check acl.yaml for a topic rule"

# **The other half of the Will, which the availability check above cannot
# reach.** That check reads `online`, and `online` is published by rtl_433
# itself on connecting: it passes whether or not a Will was ever registered.
# Only a gateway dying without saying goodbye proves there was one, and that
# is the case the Will exists for and the reason this demo needs a
# `retained:` store at all.
#
# `docker compose kill` sends SIGKILL, which cannot be caught, so the client
# publishes nothing on its way out. An `offline` after it can only be the
# broker delivering the Will.
#
# Last, because it takes the radio gateway down and back up.
$COMPOSE kill rtl433 >/dev/null 2>&1
state=""
# A pause between reads: each one returns the stored value at once, so
# without it every attempt can land before the change it is waiting for.
for _ in $(seq 1 8); do
  state=$(sub 'rtl433/availability' | awk '{print $2}')
  [ "$state" = "offline" ] && break
  sleep 1
done
[ "$state" = "offline" ] \
  && ok "a gateway killed without a DISCONNECT is marked offline by its Will" \
  || bad "a gateway killed without a DISCONNECT is marked offline by its Will" \
        "availability read '$state', wanted 'offline'"

# And back, which both restores the stack for a second run and shows the
# latest channel replacing the Will rather than keeping it beside the live
# value.
$COMPOSE start rtl433 >/dev/null 2>&1
state=""
for _ in $(seq 1 10); do
  state=$(sub 'rtl433/availability' | awk '{print $2}')
  [ "$state" = "online" ] && break
  sleep 1
done
[ "$state" = "online" ] \
  && ok "and online again when the gateway comes back" \
  || bad "and online again when the gateway comes back" \
        "availability read '$state', wanted 'online'"

# The viewer's answers travel over its own `viewer-reply/<id>` subscription,
# and an acl_file that forgets the prefix loses every point read and seek
# reply silently: the page degrades to working without them, and the 31
# checks above once passed over exactly that. /api/state has carried
# granted:false since the day it was written; this is the reader it lacked.
# The count is printed so a viewer that answered nothing cannot pass as one
# with nothing refused.
subs=$(curl -fsS "$VIEWER/api/state" 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
subs = d.get("subscriptions") or []
refused = [s["filter"] for s in subs if not s.get("granted")]
print(str(len(subs)) + "|" + " ".join(refused))
' 2>/dev/null)
count=${subs%%|*}
refused=${subs#*|}
if [ -z "$subs" ] || [ "${count:-0}" -eq 0 ]; then
  bad "every viewer subscription is granted, the reply topic included" \
      "could not read subscriptions from $VIEWER/api/state, so nothing was examined"
elif [ -n "$refused" ]; then
  bad "every viewer subscription is granted, the reply topic included" \
      "refused: $refused - an acl_file must grant viewer-reply/+ read"
else
  ok "every viewer subscription is granted, the reply topic included ($count examined)"
fi

echo
echo "-------------------------------------------------"
printf '%d checks ran, %d passed, %d failed\n' "$ran" "$passed" "$failed"
if [ "$ran" -ne "$EXPECTED_CHECKS" ]; then
  echo "expected $EXPECTED_CHECKS checks and ran $ran: a check was lost, which is a failure" >&2
  exit 1
fi
[ "$failed" -eq 0 ] || exit 1
echo "all good"
