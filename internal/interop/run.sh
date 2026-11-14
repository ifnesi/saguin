#!/bin/sh
# Outside suites run serially; every attempt owns a fresh disk database.
set -eu
cd "$SAGUIN_ROOT"
[ -n "$INTEROP_SUITES" ] || { echo "interop: no suites selected" >&2; exit 1; }
for dependency in python3 git timeout; do
    command -v "$dependency" >/dev/null || { echo "interop: missing $dependency" >&2; exit 1; }
done
mkdir -p "$INTEROP_DIR"
run=$(mktemp -d "$INTEROP_DIR/run.XXXXXX")
work=$(mktemp -d "$TMPDIR/interop.XXXXXX")
cp "$SAGUIN_ROOT/bin/saguin" "$work/saguin"
sha256sum "$work/saguin" > "$run/broker.sha256"
bpid=
cleanup() {
    if [ -n "$bpid" ]; then kill "$bpid" 2>/dev/null || :; wait "$bpid" 2>/dev/null || :; fi
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' HUP TERM
export PYTHONDONTWRITEBYTECODE=1 NO_COLOR=1
printf 'interop logs: %s\n' "$run"

clone() {
    repo=$1 pin=$2 dest=$3
    if [ ! -d "$dest/.git" ]; then git clone --quiet --no-hardlinks "$repo" "$dest"; fi
    origin=$(git -C "$dest" remote get-url origin)
    if [ -d "$repo" ]; then repo=$(cd "$repo" && pwd -P); fi
    if [ -d "$origin" ]; then origin=$(cd "$origin" && pwd -P); fi
    [ "$origin" = "$repo" ] || {
        echo "interop: $dest has another origin; use a new INTEROP_DIR" >&2; exit 1;
    }
    if ! git -C "$dest" cat-file -e "$pin^{commit}" 2>/dev/null; then
        git -C "$dest" fetch --quiet origin "$pin"
    fi
    git -C "$dest" diff --quiet && git -C "$dest" diff --cached --quiet || {
        echo "interop: tracked changes in $dest; refusing an altered suite" >&2; exit 1;
    }
    git -C "$dest" checkout --quiet --detach "$pin"
    [ "$(git -C "$dest" rev-parse HEAD)" = "$pin" ] || exit 1
}

attempt() {
    log="$run/$suite.$name.$1"
    state=$(mktemp -d "$work/broker.XXXXXX")
    # Hold all three sockets together while choosing distinct free ports.
    ports=$(python3 - <<'PYPORTS'
import socket
sockets = [socket.socket() for _ in range(3)]
for sock in sockets:
    sock.bind(("127.0.0.1", 0))
print(*(sock.getsockname()[1] for sock in sockets))
PYPORTS
    )
    set -- $ports
    port=$1 tls_port=$2 ws_port=$3
    cat > "$state/config.yaml" <<EOF
broker:
  id: interop
  log_level: debug
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:$port
  limits:
    max_connections: 100
    max_connect_rate: none
    max_keepalive: 60s
  storage:
    file_path: $state/saguin.db
    default_retention_period: 1d
    default_retention_bytes: 1GiB
EOF
    if [ "$suite" = mqtt5test ]; then
        python3 - "$state/config.yaml" "$port" "$tls_port" "$ws_port" "$work" <<'PYTLS'
import pathlib, sys
path, port, tls_port, ws_port, work = sys.argv[1:]
p = pathlib.Path(path)
p.write_text(p.read_text().replace(
    "        address: 127.0.0.1:" + port,
    f"        - name: plain\n          address: 127.0.0.1:{port}\n"
    f"        - name: tls\n          address: 127.0.0.1:{tls_port}\n"
    f"          tls:\n            cert_file: {work}/server.pem\n"
    f"            key_file: {work}/server.key\n"
    f"      ws:\n        address: 127.0.0.1:{ws_port}"))
PYTLS
    fi
    cp "$state/config.yaml" "$log.yaml"
    "$work/saguin" --config "$state/config.yaml" > "$log.broker.log" 2>&1 &
    bpid=$!
    python3 - "$port" "$bpid" <<'PY'
import os, socket, sys, time
for _ in range(100):
    os.kill(int(sys.argv[2]), 0)
    try:
        with socket.create_connection(("127.0.0.1", int(sys.argv[1])), .1):
            break
    except OSError:
        time.sleep(.05)
else:
    sys.exit("interop: broker did not start")
PY
    status=0
    case "$suite" in
        paho*) timeout -k 2 "$INTEROP_TIMEOUT" python3 internal/interop/paho.py "$source" "$name" "$port" > "$log.suite.log" 2>&1 || status=$? ;;
        mqtt5test)
            description=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]]["description"])' "$INTEROP_DIR/mqtt_test-runner/names.json" "$name")
            # The configured WS listener is not served yet. Still attempt it:
            # no endpoint probing or transport skip can hide its three tests.
            timeout -k 2 "$INTEROP_TIMEOUT" "$mqtt5test" "${name%%.*}" "$description" "$port" "$tls_port" "$work/ca.pem" "$ws_port" > "$log.suite.log" 2>&1 || status=$?
            ;;
        mqttconf*) timeout -k 2 "$INTEROP_TIMEOUT" "$mqttconf" run --host 127.0.0.1 --port "$port" --version "$version" --section "${name%%.*}" --normative "${name#*.}" --verbose > "$log.suite.log" 2>&1 || status=$? ;;
    esac
    alive=0
    kill -0 "$bpid" 2>/dev/null || alive=1
    kill "$bpid" 2>/dev/null || :
    # Bound shutdown too: a stuck broker must not hang the test target.
    (sleep 5; kill -KILL "$bpid" 2>/dev/null || :) & watchdog=$!
    broker_status=0
    wait "$bpid" || broker_status=$?
    bpid=
    kill "$watchdog" 2>/dev/null || :
    wait "$watchdog" 2>/dev/null || :
    rm -rf "$state"
    [ "$alive" = 0 ] && [ "$broker_status" = 0 ] || {
        echo "interop: broker failed for $suite/$name; $log.broker.log" >&2; exit 1;
    }
    got=FAIL
    parsed=0
    python3 internal/interop/report.py "$suite" "$name" "$log.suite.log" "$INTEROP_DIR/mqtt_test-runner/names.json" >> "$log.suite.log" 2>&1 || parsed=$?
    if [ "$parsed" = 2 ] && [ "$status" != 124 ]; then
        echo "interop: invalid report for $suite/$name ($log.suite.log)" >&2; exit 1
    fi
    if [ "$parsed" = 0 ] && [ "$status" = 0 ]; then got=PASS; fi
    printf 'exit=%s result=%s\n' "$status" "$got" > "$log.status"
}

rc=0
for suite in $INTEROP_SUITES; do
    case "$suite" in
        paho5|paho311)
            clone "$PAHO_REPO" "$PAHO_SHA" "$INTEROP_DIR/paho.mqtt.testing"
            source="$INTEROP_DIR/paho.mqtt.testing/interoperability/client_test5.py"
            [ "$suite" = paho5 ] || source="$INTEROP_DIR/paho.mqtt.testing/interoperability/client_test.py"
            python3 internal/interop/paho.py "$source" list 0 > "$run/$suite.names"
            ;;
        mqtt5test)
            command -v openssl >/dev/null || { echo 'interop: missing openssl' >&2; exit 1; }
            clone "$MQTT5TEST_REPO" "$MQTT5TEST_SHA" "$INTEROP_DIR/mqtt_test"
            python3 internal/interop/prepare.py "$INTEROP_DIR/mqtt_test" "$INTEROP_DIR/mqtt_test-runner" > "$run/$suite.names"
            mqtt5test=$MQTT5TEST_BIN
            if [ -z "$mqtt5test" ]; then
                command -v cargo >/dev/null || { echo 'interop: missing cargo' >&2; exit 1; }
                cargo build --locked --release --manifest-path "$INTEROP_DIR/mqtt_test-runner/Cargo.toml"
                mqtt5test="$INTEROP_DIR/mqtt_test-runner/target/release/mqtt_test"
            fi
            # A leaf signed by a CA: rustls rejects a CA used as a leaf.
            openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=interop-ca -keyout "$work/ca.key" -out "$work/ca.pem" > "$run/tls.log" 2>&1
            openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout "$work/server.key" -out "$work/server.csr" >> "$run/tls.log" 2>&1
            printf 'subjectAltName=IP:127.0.0.1\nbasicConstraints=CA:FALSE\n' > "$work/server.ext"
            openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" -CAcreateserial -days 1 -extfile "$work/server.ext" -out "$work/server.pem" >> "$run/tls.log" 2>&1
            ;;
        mqttconf5|mqttconf311)
            clone "$MQTTCONF_REPO" "$MQTTCONF_SHA" "$INTEROP_DIR/mqttconformance"
            mqttconf=$MQTTCONF_BIN
            if [ -z "$mqttconf" ]; then
                command -v cargo >/dev/null || { echo 'interop: missing cargo' >&2; exit 1; }
                cargo build --locked --release --manifest-path "$INTEROP_DIR/mqttconformance/Cargo.toml"
                mqttconf="$INTEROP_DIR/mqttconformance/target/release/mqtt-conformance"
            fi
            version=5; [ "$suite" = mqttconf5 ] || version=3.1.1
            "$mqttconf" list --version "$version" > "$run/$suite.list"
            awk '/Section:/ { section=$2; sub(/:$/, "", section) } /^ *MQTT-/ { print section "." $1 }' "$run/$suite.list" > "$run/$suite.names"
            ;;
        *) echo "interop: unknown suite $suite" >&2; exit 1 ;;
    esac
    [ -s "$run/$suite.names" ] || { echo "interop: no tests in $suite" >&2; exit 1; }
    [ -z "$(sort "$run/$suite.names" | uniq -d)" ] || {
        echo "interop: duplicate test names in $suite" >&2; exit 1;
    }
    expected="$INTEROP_EXPECTED/$suite.txt"
    sed '/^#/d; /^[[:space:]]*$/d' "$expected" > "$work/expected"
    while IFS= read -r name; do
        grep -Fxq "$name" "$run/$suite.names" || { echo "interop: unknown expected test $suite/$name" >&2; exit 1; }
    done < "$work/expected"
    passed=0 failed=0 unexpected=0
    while IFS= read -r name; do
        [ -z "$INTEROP_TEST" ] || [ "$INTEROP_TEST" = "$name" ] || continue
        want=PASS
        if grep -Fxq "$name" "$work/expected"; then want=FAIL; fi
        attempt 1
        if [ "$got" != "$want" ] && [ "$want" = PASS ]; then
            echo "RETRY $suite/$name: $got, expected $want"
            attempt 2
        fi
        if [ "$got" = PASS ]; then passed=$((passed+1)); else failed=$((failed+1)); fi
        if [ "$got" != "$want" ]; then
            unexpected=$((unexpected+1)); rc=1
            echo "UNEXPECTED $suite/$name: $got, expected $want ($log.suite.log)"
        else
            echo "$suite/$name: $got (expected $want)"
        fi
        printf '%s\t%s\t%s\n' "$name" "$got" "$want" >> "$run/$suite.results"
    done < "$run/$suite.names"
    [ $((passed+failed)) -gt 0 ] || { echo "interop: no tests selected in $suite" >&2; exit 1; }
    echo "$suite: $passed passed, $failed failed, $unexpected unexpected"
done
exit "$rc"
