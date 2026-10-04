.PHONY: help build test race vet fmt quick ci docs bench check stress demo demo-full demo-server soak scale retention mosquitto nginx conformance fuzz smoke notices clean

help:
	@echo "build   compile saguin into ./bin"
	@echo "quick   gofmt + vet + build + every package but the two end-to-end ones"
	@echo "docs    the tests that read the README or an RFC - seconds, for a"
	@echo "        change to a document; check is minutes on code it cannot reach"
	@echo "test    run the end-to-end suite"
	@echo "race    run it under the race detector"
	@echo "bench   run every benchmark once, to prove they still run"
	@echo "check   gofmt + vet + race + timing + bench + fuzz - the Go side;"
	@echo "        run it before you push; GitHub does not"
	@echo "ci      gofmt + vet + build + the suite without the race detector - what"
	@echo "        GitHub runs on the Go side, with conformance"
	@echo "stress  the whole suite with and without the race detector at"
	@echo "        GOMAXPROCS 1, 2 and 4, shuffled, the end-to-end and engine"
	@echo "        packages STRESS_COUNT (default 3) times each - overnight,"
	@echo "        before a release"
	@echo "demo    run the concise guided tour (needs the repository .venv)"
	@echo "demo-full  run every guided-tour deep dive"
	@echo "demo-server  run the example broker for manual experimentation"
	@echo "notices regenerate cmd/saguin/THIRD-PARTY-NOTICES.md, which the binary embeds"
	@echo "fuzz    fuzz every property target for FUZZTIME (default 300000x) each"
	@echo "retention  drive the retention floor under concurrent load against a"
	@echo "        broker you started; run it after any change to delivery,"
	@echo "        retention or locking. Needs BROKER and OPS"
	@echo "soak    run the link-churn soak for SOAK (default 2m) instead of seconds;"
	@echo "        the test timeout follows SOAK, so an hour's dial is an hour"
	@echo "mosquitto  drive the inbound bridge against a real mosquitto; needs the"
	@echo "        binary on PATH and fails without it. Not part of check"
	@echo "conformance  run the Eclipse Paho MQTT 5 interoperability suite against"
	@echo "        saguin; needs python3 and the network. Not part of check"
	@echo "mqtt5test  run ifnesi/mqtt_test, an MQTT 5 broker compliance tool with"
	@echo "        its own packet codec; needs cargo and the network, or a binary"
	@echo "        named in MQTT5TEST_BIN. Run locally by the maintainer, not part of check or ci"
	@echo "mqttconf  run vibesrc/mqttconformance over both protocols; needs cargo"
	@echo "        and the network, or a binary named in MQTTCONF_BIN. Run locally by"
	@echo "        the maintainer, not part of check or ci"
	@echo "nginx   drive RFC 0005's nginx block against a real nginx; needs docker"
	@echo "smoke   bring examples/bento-connectors up and assert it is doing"
	@echo "        something; needs Docker. Not part of check"

build:
	go build -o bin/saguin ./cmd/saguin

# The budget is written down rather than inherited, and on both targets: go
# test allows each package ten minutes by default, and the end-to-end tests
# under the race detector have sat at about that - 597.8s when this was
# written, 600.1-600.8s on CI, where the same suite both passed and timed
# out. Sixty minutes is what the slowest package needs with headroom:
# internal/broker under -race took about 27 minutes on 4 cores and about 34
# on 2, which is what a private GitHub runner has, so thirty minutes was a
# red main waiting for a slow runner.
#
# **The plain run needs it too**, which is not obvious and cost a round to
# find: the end-to-end package took about 670s without the race detector, so
# `make test` inherited the ten minute default and died at 600.012s with a
# goroutine dump - and a dump reads as a hang rather than as a budget, with
# the test it named 10 seconds old. One number on both lines rather than two
# to keep in step.
#
# **The budget is per package, which is why the suite is being split.** The
# end-to-end tests were one binary that ran its tests one at a time; go test
# runs packages in parallel, so the sessions have their own package now
# (internal/sessiontest) and the harness they share is internal/brokertest.
# Every package the split produces sits far from this budget rather than one
# package sitting near it.
test:
	go test ./... -count=1 -timeout 60m

race:
	go test ./... -count=1 -race -timeout 60m

# The two checks on how long the broker holds a lock, run without the race
# detector, which `race` above runs everything else with. They measure time,
# and the detector does not slow a healthy broker and a regressed one by the
# same factor - fixed values under it reach two thirds of what the defect
# they guard against produces - so a bound that survives the detector cannot
# fail on the defect. They skip themselves under it and are run here instead;
# a few seconds.
timing:
	go test ./internal/broker/ -count=1 -run HoldsTheLockBriefly -timeout 10m

vet:
	go vet ./...

# CI refuses an unformatted tree, so `make check` and `make ci` have to as
# well. `check` did not for one commit, and that commit is why this exists:
# the help text said "everything CI runs", so a green `make check` was a
# claim about a build that had not been made. A target that reports on
# work it did not do is the failure this repository writes checks to
# prevent, and it is worse in the target whose whole job is to be the
# build.
fmt:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi

# Proves the benchmarks still compile and still run. It measures nothing:
# the published figures are taken by hand with -benchtime and -count, on
# a machine that is named.
#
# It is here because nothing else runs a benchmark. `go test ./...` does
# not, so one that stops working says nothing at all -
# BenchmarkTrimOnALongChannel failed on every commit from the one that
# introduced it until somebody happened to run it by hand, and it is the
# measurement behind a decision about how often the broker may sweep, so
# losing it silently loses the evidence with it.
#
# **-benchtime 100ms rather than 1x, and that is the whole of it.** At 1x
# this target passes against the exact failure it exists for: that
# benchmark drained a channel it never refilled, so it only breaks once
# the iteration count passes the channel's length. One iteration proves
# the code compiles and nothing else. The setting has to produce more
# iterations than the longest channel any benchmark here builds - 800,000
# records - and it costs about fifty seconds for all of them, nearly all
# of which is setup rather than measurement.
#
# 10ms also catches it on the machine this was written on, and is not
# taken: it is 50,000 iterations here and 10,000 on a machine five times
# slower, which is the boundary at which the failure stops being visible.
# Eight seconds is not worth a check whose answer depends on the hardware.
bench:
	go test ./... -run XXX -bench . -benchtime 100ms

# The link-churn soak, for as long as you like rather than the few seconds
# `make check` gives it. Same test, same assertions, longer dial: nothing it
# checks depends on how many times the link drops, which is what makes one
# duration as valid as the other.
#
# It is an ordinary test that reads SAGUIN_SOAK, not a test behind a build
# tag. A soak excluded at compile time is one that stops building and says
# nothing, which is what `bench` above exists for.
#
# **The pattern is checked against the tests that exist**, because the
# failure mode here is silence rather than noise: `-run` matching nothing
# exits 0 and prints a warning most people scroll past. This target once
# named two tests that were later folded into one, and it would have soaked
# nothing while reporting success - the same shape as the benchmark that ran
# on no commit for a fortnight. `-list` is the counter, and no match is an
# error.
#
# **The test timeout is derived from SOAK rather than fixed.** A constant is
# a ceiling on SOAK that nobody sees until a long run dies at it: `-timeout
# 30m` meant any SOAK above about twenty-five minutes was killed mid-run,
# and the panic arrived at minute thirty of the hour the operator asked for,
# with nothing to show for the wait.
#
# SOAK plus ten minutes: room for the run, its settle - which polls for up
# to thirty seconds - its teardown, and the race detector's overhead, while
# still short enough that a genuinely hung run dumps its goroutines instead
# of sitting there until somebody notices. The figure is printed, so what it
# was given is never a guess.
#
# SOAK is one number and one unit - `90s`, `2m`, `1h`. A compound duration
# is refused rather than parsed, because `1h30m` reads as 1 to anything
# doing arithmetic on the front of it, and a soak that quietly ran for a
# minute when an hour was asked for is worse than one that refused.
SOAK ?= 2m
SOAK_RUN ?= LinkChurn
soak:
	@t=$$(echo "$(SOAK)" | awk '/^[0-9]+[smh]$$/ { \
	        n = $$0 + 0; u = substr($$0, length($$0)); \
	        if (u == "h") n *= 3600; else if (u == "m") n *= 60; \
	        printf "%d", n + 600; exit } \
	      { print "bad"; exit }'); \
	if [ "$$t" = "bad" ]; then \
	  echo "SOAK=$(SOAK) is not one number and one unit, such as 90s, 2m or 1h" >&2; \
	  exit 1; \
	fi; \
	n=$$(go test ./internal/broker/ -list "$(SOAK_RUN)" | grep -c '^Test'); \
	if [ "$$n" -eq 0 ]; then \
	  echo "no test matches $(SOAK_RUN): this target would soak nothing and say ok" >&2; \
	  exit 1; \
	fi; \
	echo "soaking $$n test(s) matching $(SOAK_RUN) for $(SOAK) - two strands, half the dial each, one per write path - with -timeout $${t}s"; \
	SAGUIN_SOAK=$(SOAK) go test ./internal/broker/ -run "$(SOAK_RUN)" \
		-count=1 -race -timeout $${t}s -v

# Everything except the end-to-end packages - `internal/broker` and
# `internal/sessiontest`, which drive a real broker over a real socket.
# Almost all of their time is waiting: sleeps, and assertions that nothing
# arrives within a window, which cannot be made faster without weakening
# them. This runs in 23 seconds and answers the question an ordinary change
# asks: did I break the thing I touched.
#
# **It is not `make check` and does not claim to be**, which matters here
# more than it looks: the target above this one exists because a green tick
# over work that was not done is the failure this repository writes checks
# against. So, plainly - a change on a path everything goes through, such
# as the authorization hook or the publish path, reaches every one of those
# end-to-end tests and none of them run here. Judge which kind of change it
# is, and run `make check` when it is that kind. It is also run before a release
# either way.
#
# The package list is computed rather than written out, so a package added
# tomorrow is included without anybody remembering to add it. Written out,
# this target would go on reporting success over a package it had never
# heard of.
quick: fmt vet
	go build ./...
	go test -count=1 -race $$(go list ./... | grep -vE '/internal/(broker|sessiontest)$$')

# The tests that read a document, and nothing else - seconds, for a change
# to the README or an RFC, where `check` is five minutes on code such a
# change cannot reach. One session ran `check` eight times in an evening
# for one-file changes, most of them documents, so a document change
# belongs here.
#
# **Derived from the names rather than listed**, because a list is one test
# short the day somebody adds the next document check, and short silently.
# The rule it derives from is one the tests already follow: a test that
# reads a document names the document - RFC, README, the documents, an
# example, the specification, the help text, the nginx block. The count is
# held to a floor the way `soak` holds its pattern to the tests that exist,
# so a rename that empties the pattern fails rather than running nothing
# and saying ok.
#
# `quick` is not this: it skips the end-to-end package, which holds two of
# these - the RFC 0005 catalogue and the /v1 shape.
DOCS_RUN ?= 'RFC|README|Document|Doc|Example|Specification|HelpText|Nginx'
docs:
	@set -eu; n=0; \
	for pkg in $$(go list ./...); do \
	  m=$$(go test $$pkg -list $(DOCS_RUN) 2>/dev/null | grep -c '^Test' || true); \
	  n=$$((n + m)); \
	done; \
	if [ "$$n" -lt 10 ]; then \
	  echo "only $$n tests match $(DOCS_RUN), which cannot be all of the document checks:" >&2; \
	  echo "  this target would run almost nothing and say ok" >&2; \
	  exit 1; \
	fi; \
	echo "running the $$n tests that read a document"; \
	go test ./... -count=1 -run $(DOCS_RUN)

# What every contributor runs locally before pushing, together with
# `make conformance`, `make mqtt5test` and `make mqttconf`. GitHub runs
# `ci` below and `conformance`, not this: the race detector, the timing
# checks, the benchmarks and the fuzzers take about 35 minutes, so the
# local run of them is the gate.
check: fmt vet race timing bench fuzz

# The suite run the ways a test's timing assumptions break: with and
# without the race detector, at GOMAXPROCS 1, 2 and 4, in shuffled order -
# overnight, before a release. A test that waits for the clock instead of
# for the broker passes on the machine it was written on and fails on a
# runner with fewer cores, under the detector, or after a test that left
# something running; each of the six runs is one of those machines. The
# packages whose tests drive a broker over sockets - the end-to-end ones,
# the engine, its listeners, the sqlite provider and the bridge - run
# STRESS_COUNT times in each; one run of a scheduling-dependent test is not
# evidence. The rest run once.
#
# **It runs everything and fails at the end**, naming each run that failed:
# a failure at the first of six runs would leave the other five unanswered
# until the next night. The shuffle seed is in each run's output
# (`-test.shuffle`), so a failing order can be run again.
#
# About nine hours at the default count on an 8-core machine, worked out
# from `test` and `race` above rather than measured whole; each run prints
# how long it took. Fewer cores take longer, and GOMAXPROCS 1 also runs the
# packages one at a time.
#
# **On Linux a long run can fill nf_conntrack** with the TIME_WAIT entries
# of the connection-flood tests, and the kernel then drops new connections
# ("nf_conntrack: table full" in `journalctl -k`): a dial that times out
# while that is logged is the host, not the broker, and the run is evidence
# of neither.
STRESS_COUNT ?= 3
STRESS_PKGS = ./internal/broker ./internal/sessiontest ./internal/mqtt/... ./internal/store/sqlite ./internal/bridge
stress:
	@set -u; failed=""; \
	sensitive=$$(go list $(STRESS_PKGS)); \
	rest=$$(go list ./... | grep -vxF "$$sensitive"); \
	for race in "" "-race"; do \
	  for procs in 1 2 4; do \
	    run="GOMAXPROCS=$$procs $${race:-without -race}"; \
	    echo "== $$run: $(STRESS_COUNT) runs of the scheduling-sensitive packages, one of the rest"; \
	    start=$$(date +%s); \
	    GOMAXPROCS=$$procs go test $$race -shuffle=on -count=$(STRESS_COUNT) \
	      -timeout $$((60 * $(STRESS_COUNT)))m $$sensitive || failed="$$failed\n  $$run, sensitive packages"; \
	    GOMAXPROCS=$$procs go test $$race -shuffle=on -count=1 -timeout 60m $$rest \
	      || failed="$$failed\n  $$run, the rest"; \
	    echo "== $$run took $$(( ($$(date +%s) - start) / 60 )) minutes"; \
	  done; \
	done; \
	if [ -n "$$failed" ]; then printf "stress failed:$$failed\n" >&2; exit 1; fi; \
	echo "stress passed: six runs, each package at least once and the sensitive ones $(STRESS_COUNT) times"

# What GitHub runs on the Go side: gofmt, vet, the build, and the whole
# suite without the race detector, so cmd/saguin's heap-floor test runs on
# GitHub's kernel. **It is not `make check` and does not claim to be**: no
# race detector, no timing, no bench, no fuzz. It is `test` plus the cheap
# checks that fail in seconds.
ci: fmt vet
	go build ./...
	go test ./... -count=1 -timeout 60m

# The attribution that travels inside the binary. saguin ships as one file,
# so a recipient has no go.mod to read and `saguin --licenses` is the only
# place they can learn whose code is in there.
#
# Generated from `go list -deps ./cmd/saguin` - the packages that reach the
# binary - rather than from go.mod, which names two modules that never do,
# or `go list -m all`, which drags in dependencies' test dependencies. The
# licence files are taken whole from the module cache: a summary of a
# licence is not the licence, and Apache-2.0 asks for a dependency's NOTICE
# to travel unaltered.
#
# It is generated rather than written because a hand-kept list is wrong the
# first time somebody adds a dependency and says nothing about it.
# TestTheNoticesCoverEveryModuleInTheBinary is what makes that a failure
# rather than a silence.
#
# **The MQTT engine is the one entry `go list` cannot find.** It used to be a
# module and is now a stripped copy under `internal/mqtt`, so the sweep
# above reports it as saguin's own and the filter drops it - while its code
# is still every packet saguin reads and writes. Its licence is therefore
# appended from the copy kept beside it, and that copy is the reason it can
# be: a file in the tree rather than a module version somebody has to
# remember. MIT asks for the notice to travel with the software, and the
# software is this binary.
ENGINE_ORIGIN  := github.com/mochi-mqtt/server/v2
ENGINE_VERSION := v2.7.9
ENGINE_LICENCE := internal/mqtt/LICENSE.md

notices:
	@set -eu; \
	out=cmd/saguin/THIRD-PARTY-NOTICES.md; \
	mods=$$(for p in $$(go tool dist list); do \
		GOOS=$${p%/*} GOARCH=$${p#*/} go list -deps -f '{{if .Module}}{{.Module.Path}}|{{.Module.Version}}|{{.Module.Dir}}|{{with .Module.Replace}}{{.Path}}@{{.Version}}{{end}}{{end}}' ./cmd/saguin 2>/dev/null; \
	  done | sort -u | grep -v '^github.com/ifnesi/saguin' | grep -v '^$$'); \
	{ \
	  printf '# Third-party notices\n\n'; \
	  printf 'saguin ships as one binary, so this travels inside it: `saguin --licenses`\n'; \
	  printf 'prints this file. Every licence here is permissive and none constrains\n'; \
	  printf "saguin's own; what they ask for is attribution, and this is it.\n\n"; \
	  printf 'Generated by `make notices` from `go list -deps ./cmd/saguin`, the set that\n'; \
	  printf 'reaches the binary - asked of every platform the toolchain builds for and\n'; \
	  printf 'merged, because the set is not the same on all of them - plus the MQTT\n'; \
	  printf 'engine, which is a copy in the tree rather than a module and so is named\n'; \
	  printf 'below by hand. Do not edit by hand.\n\n'; \
	  printf '## Modules\n\n'; \
	  echo "$$mods" | while IFS='|' read -r path version dir replaced; do \
	    if [ -n "$$replaced" ]; then \
	      printf -- '- %s %s (%s)\n' "$$path" "$$version" "$$replaced"; \
	    else \
	      printf -- '- %s %s\n' "$$path" "$$version"; \
	    fi; \
	  done; \
	  printf -- '- %s %s (%s)\n' "$(ENGINE_ORIGIN)" "$(ENGINE_VERSION)" "stripped into internal/mqtt"; \
	  printf '\n<!-- end of module list -->\n'; \
	  echo "$$mods" | while IFS='|' read -r path version dir replaced; do \
	    printf '\n## %s %s\n\n' "$$path" "$$version"; \
	    if [ -n "$$replaced" ]; then printf 'The binary carries %s, a fork of it.\n\n' "$$replaced"; fi; \
	    if [ "$$path" = "github.com/eclipse/paho.golang" ]; then \
	      printf 'Dual-licensed EPL-2.0 or EDL-1.0, and it ships both texts.\n'; \
	      printf 'saguin elects **EDL-1.0**, which is BSD-3-Clause under another name,\n'; \
	      printf 'so the EPL text is not what applies here and is not reproduced.\n'; \
	      files=edl-v10; \
	    else \
	      files=$$(ls "$$dir" | grep -iE '^(licen[cs]e|notice|copying)' | sort); \
	    fi; \
	    for f in $$files; do \
	      printf '\n### %s\n\n```\n' "$$f"; \
	      cat "$$dir/$$f"; \
	      printf '```\n'; \
	    done; \
	  done; \
	  printf '\n## %s %s\n\n' "$(ENGINE_ORIGIN)" "$(ENGINE_VERSION)"; \
	  printf 'Not a module. saguin carries a stripped copy of this broker engine in\n'; \
	  printf '`internal/mqtt`, so `go list` reports it as saguin and the sweep above\n'; \
	  printf 'cannot see it. Its code is in the binary, so its licence is here.\n'; \
	  printf '\n### LICENSE.md\n\n```\n'; \
	  cat $(ENGINE_LICENCE); \
	  printf '```\n'; \
	} > $$out
	@echo "wrote cmd/saguin/THIRD-PARTY-NOTICES.md"

# The configuration this runs against is a secured one - TLS, a client
# certificate authority, a password file and an acl_file - and it names
# them by absolute path, which saguin requires and which therefore cannot
# point into a repository. The files are checked in under examples/;
# `--credentials` copies them where the configuration expects, and needs
# no pip install and runs no tour.
demo: build
	@test -x .venv/bin/python || { echo "make demo needs .venv; run: python3 -m venv .venv && . .venv/bin/activate && pip install -r examples/requirements.txt"; exit 1; }
	./.venv/bin/python examples/demo.py --skip-build

demo-full: build
	@test -x .venv/bin/python || { echo "make demo-full needs .venv; run: python3 -m venv .venv && . .venv/bin/activate && pip install -r examples/requirements.txt"; exit 1; }
	./.venv/bin/python examples/demo.py --skip-build --full

demo-server: build
	python3 examples/demo.py --credentials
	./bin/saguin -config examples/saguin.yaml

clean:
	rm -rf bin

# The inbound bridge against a real mosquitto, which `make check` does not
# run and deliberately does not claim to.
#
# Everything else in the suite bridges saguin to mochi - the same server
# saguin is built on - so it proves the bridge talks to itself. The numbers
# the bridge's defaults are argued from were measured against mosquitto by
# hand and nothing re-runs them.
#
# **It is not folded into check**, because the CI runner has no
# mosquitto and a test that skipped there would be a green tick over work
# nobody did. Here the binary's absence is a failure instead: a target
# invoked by name has already said what was wanted.
#
# The pattern is checked against the tests that exist, for the same reason
# `soak` checks its own: `-run` matching nothing exits 0 and prints a
# warning most people scroll past, so a renamed test would leave this
# running nothing and reporting success.
#
# **No install command is printed and none should be**, here or in the test.
# What this needs is the mosquitto broker on PATH, able to read a
# configuration the test writes under its own temporary directory; how a
# machine provides that is the machine's business. A message naming the two
# package managers somebody has met is a list, and it is wrong for everybody
# else the day it is written. When mosquitto is present and still will not
# start, the test prints what mosquitto itself said, which names causes
# nobody here has thought of.
MOSQUITTO_RUN ?= TestAnInboundBridgeCarriesRecordsThroughRealMosquitto

# The README's nginx block, extracted and driven against a real nginx.
#
# **Because the block that was driven and the block that was published have
# differed three times**, each time by the thing the driving had found: a
# missing `ssl_verify_client` that served `/v1` to anybody, a `proxy_pass`
# whose trailing `/` made it answer 404 to everybody, and a missing
# `password_file` that let a nameless caller through. Every one loaded
# cleanly under `nginx -t`, which checks syntax and not doors.
#
# The half that needs no container - that what is driven differs from what
# is published only in paths and ports - is an ordinary test and runs in
# `make check`. This is the other half.
nginx:
	@command -v docker >/dev/null 2>&1 || { \
	  echo "docker is not on PATH, and this drives nginx in a container" >&2; \
	  exit 1; \
	}
	@n=$$(go test ./cmd/saguin/ -list "TestTheNginxBlock" | grep -c '^Test'); \
	if [ "$$n" -lt 2 ]; then \
	  echo "expected both halves of the nginx check and found $$n" >&2; exit 1; \
	fi; \
	echo "driving RFC 0005's nginx block against $$(docker run --rm nginx:alpine nginx -v 2>&1 | tail -1)"; \
	SAGUIN_NGINX=1 go test ./cmd/saguin/ -run TestTheNginxBlock -count=1 -timeout 150s -v

mosquitto:
	@command -v mosquitto >/dev/null 2>&1 || { \
	  echo "mosquitto is not on PATH, and it is the whole point of this target" >&2; \
	  echo "  install the broker itself: several distributions package it" >&2; \
	  echo "  apart from mosquitto_pub and mosquitto_sub" >&2; \
	  exit 1; \
	}; \
	n=$$(go test ./internal/broker/ -list "$(MOSQUITTO_RUN)" | grep -c '^Test'); \
	if [ "$$n" -eq 0 ]; then \
	  echo "no test matches $(MOSQUITTO_RUN): this target would run nothing and say ok" >&2; \
	  exit 1; \
	fi; \
	echo "driving $$n test(s) matching $(MOSQUITTO_RUN) against \
$$(mosquitto -h 2>&1 | grep -m1 '^mosquitto version')"; \
	SAGUIN_MOSQUITTO=1 go test ./internal/broker/ -run "$(MOSQUITTO_RUN)" \
		-count=1 -race -v

# The Eclipse Paho MQTT 5 interoperability suite, run against saguin.
#
# **The README's first claim is "any stock MQTT 5 client works", and until
# this existed the only evidence was saguin's own tests** - which drive what
# somebody thought of, and so cannot find a rule nobody remembered. An
# outside suite can. It already has: the QoS 2 mis-acknowledgement fixed since
# accounted for most of the failures below, and one of the suite's
# own tests had been passing on that defect.
#
# **Not folded into check**, for the reason `mosquitto` above is not: this
# needs python3 and a clone from the network, and a check that skipped when
# either was missing would be a green tick over work nobody did. It runs in
# CI as a job of its own, which is the target nobody skips, and by name
# here. A target invoked by name has already said what was wanted, so a
# missing dependency is a failure rather than a skip.
#
# **One test at a time, each with a fresh broker and a timeout.** The suite
# does not terminate against a broker without QoS 2 - two of its tests loop
# until a third message arrives that never will - so running it whole hangs
# rather than reporting. Per-test also stops a wedged connection in one test
# poisoning the next.
CONFORMANCE_REPO ?= https://github.com/eclipse-paho/paho.mqtt.testing.git
# Pinned, because "whatever master says today" is not a result anybody can
# reproduce. Moving it is a commit, and the README line moves with it.
CONFORMANCE_SHA  ?= 9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103
CONFORMANCE_DIR  ?= $(CURDIR)/bin/conformance
CONFORMANCE_PORT ?= 11883

# **The tests that cannot pass, named individually rather than summarised.**
# "Passes except where documented" is a claim nobody can check; this is a
# list somebody can. A test outside it that fails is run once more before
# it counts, because the suite waits a fixed three seconds for a message and
# a machine under load misses that deadline on work the broker did
# correctly - one run saw `test_request_response` fail at load average 27
# and pass five times out of five on the same commit once the machine was
# quiet. A retry that passes is printed by name rather than swallowed, so a
# broker that has genuinely become slow still shows up; a test that fails
# twice fails.
#
# Any difference from it in either direction is a
# failure - a test that starts passing is news too, and means either the
# broker gained something or the pin moved.
#
# **A `test_will_delay` failure reporting 1.9 seconds was a broker defect,
# and is how it was found.** Its third block arms a five second delay
# against a two second session expiry and accepts 2 to 4 seconds, a window
# that opens at the instant the Will is due, because the upstream sweep
# this broker replaced published delayed Wills once a second. saguin fires
# punctually and therefore sits on that opening edge, and for a while it
# fell below: the session expiry was judged on whole seconds, so a session
# could end after 1.001 of its two seconds and take its Will with it. One
# failing run published at 1.892. Fixed by making the moment and
# the clock instants.
#
# **Not listed below, deliberately.** The names below always fail for a
# stated reason. This one now passes for a reason: nothing can fire the
# Will before the delay is up, so a 1.9 here is news rather than noise.
# Read it in $(CONFORMANCE_DIR)/test_will_delay.broker.log, which CI keeps
# when this job fails: the broker logs holding a Will with the delay it is
# waiting and logs delivering it, and the gap between those two timestamps
# is when it actually fired. A gap shorter than `delay_s` is the defect
# that fix closed, returned. A gap of the full delay, under a suite that
# reported 1.9, is the suite's own clock: it starts after its
# `terminate()` returns and polls every 100ms, so it can read a punctual
# fire as one tick early.
#
# **This list held fourteen names until exactly-once landed, and now holds
# none.** All fourteen needed QoS 2: eleven published at it outright and
# three connected with a Will at it, which is the suite client's default
# rather than anything those tests ask for. Ten passed the moment the
# ceiling went up. The other four were failing for reasons that had been
# sitting behind it, unreachable, because each publishes at QoS 2 early and
# was disconnected before it reached the assertion that failed: a retained
# message delivered at the subscription's QoS rather than the lower of that
# and the publisher's ([MQTT-3.8.4-8], two tests), and an acknowledgement
# carrying the publisher's own User Properties back at it (one). Both are
# fixed in the broker rather than listed here.
#
# Kept as an empty variable rather than deleted, because the next QoS the
# broker does not offer goes here and the reasoning above is what somebody
# will want.
CONFORMANCE_QOS2 :=
# And the ones where the suite is wrong rather than saguin.
#
# **test_subscribe_failure** asserts the SUBACK reason code is one of 0, 1,
# 2 or 0x80 - MQTT 3.1.1's set. saguin answers a subscription its rules
# refuse with 0x87 Not authorized, which MQTT 5 section 3.9.3 lists and
# which says which of the two things went wrong. Passing it would mean
# returning a vaguer code than the broker knows.
#
# **test_flow_control2** asks the broker to disconnect when a client exceeds
# its Receive Maximum, and saguin does: watched at debug, it answers
# `147 receive maximum exceeded` and hangs up, which is what the test
# asserts. The suite's own client then keeps writing into the closed socket
# and raises BrokenPipeError from its publish loop before it reads the
# DISCONNECT it asked for. **Mosquitto 2.0.22 fails it identically**, which
# is what makes this the suite rather than a reading of a traceback - run
# the same test against mosquitto on any port to see it.
CONFORMANCE_SUITE_BUG := test_subscribe_failure test_flow_control2
# And one that asks for something MQTT 5 allows and saguin does not do.
#
# **test_server_topic_alias** connects allowing one topic alias and asserts
# the broker's deliveries use it: the topic once with alias 1, then alias 1
# alone. MQTT 5 section 3.3.2.3.4 lets a server send aliases and does not
# require it. saguin sends none (RFC 0001), because an alias belongs to one
# connection and a delivery kept for a client outlives it: re-sent to a
# resumed session, or held back while a later message went first, it went
# out carrying an alias that connection had never been told, measured on
# bin/saguin before it stopped sending them. **Mosquitto 2.0.22 fails it
# identically** - its first delivery carries no alias - which is the broker
# this follows.
CONFORMANCE_NO_OUTBOUND_ALIAS := test_server_topic_alias

# **The 3.1.1 suite, run beside the MQTT 5 one**, because saguin admits both
# protocols and a claim about the older one measured only by saguin's own
# tests is a claim measured by the person who made it.
#
# **It cannot be told a port on its command line**, which is the suite's
# own inconsistency rather than a choice here: `client_test5.py` removes
# `-p` and its value from `sys.argv` before calling `unittest.main()`, and
# `client_test.py` does not - so passing a port and a test name together
# makes unittest reject the port as an unrecognised argument, and the
# default is the only port it can be given.
#
# **So the runner below changes the default instead**, executing their file
# with the one line that sets it substituted and `sys.argv` carrying only
# the test name. The file on disk is untouched: the suite is the thing
# measuring saguin, and a conformance run that edits its own instrument is
# measuring something else. The substitution is counted rather than
# attempted, so it fails loudly if upstream moves that line rather than
# quietly running against 1883.
#
# It was 1883 until a developer machine had something else on it, which is
# the argument for not depending on a well-known port at all.
#
# **One test at a time with a fresh broker, as above**, and for a reason
# measured rather than inherited: run in one process the suite leaves state
# behind, and `test_unsubscribe` fails in the full run and passes on its own.
CONFORMANCE_311_PORT ?= 11884

# **Six of the ten were here until exactly-once landed, and all six now
# pass.** Five published at QoS 2, and `test_keepalive` connected with a
# Will at QoS 2, which is the suite client's default rather than anything
# the test asks for. Before the 3.1.1 CONNACK mapping existed that last one
# was answered `0x9B` - a byte 3.1.1 does not define, which Eclipse Paho's
# own client reported as no error at all.
#
# Empty rather than deleted, for the reason its MQTT 5 counterpart is.
CONFORMANCE_311_QOS2 :=
# And one that asks for something saguin's authorization cannot express.
# The test subscribes to `test/nosubscribe` and requires `0x80`, which means
# a broker configured to refuse that one topic while granting the rest -
# including the `#` every other test subscribes to. saguin's authorization
# is grant-only (RFC 0001 "Non-goals"), and granting `#` grants everything
# beneath it: there is no deny rule to carve one topic back out. Passing it
# would mean a policy model this broker deliberately does not have.
CONFORMANCE_311_GRANT_ONLY := test_subscribe_failure

conformance: build
	@command -v python3 >/dev/null 2>&1 || { \
	  echo "python3 is not on PATH, and the suite is written in it" >&2; exit 1; }
	@command -v git >/dev/null 2>&1 || { \
	  echo "git is not on PATH, and the suite is cloned" >&2; exit 1; }
	@if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_PORT)\b'; then \
	  stale="$(CONFORMANCE_DIR)/saguin.pid"; \
	  pid=""; \
	  if [ -r "$$stale" ]; then pid=$$(cat "$$stale" 2>/dev/null || true); fi; \
	  if [ -n "$$pid" ] && [ -r "/proc/$$pid/cmdline" ] && \
	     tr '\0' ' ' < "/proc/$$pid/cmdline" | grep -q "$(CONFORMANCE_DIR)/saguin.yaml"; then \
	    echo "a conformance broker from an earlier run is still holding port $(CONFORMANCE_PORT); stopping it" >&2; \
	    kill "$$pid" 2>/dev/null || true; \
	    i=0; while [ $$i -lt 50 ] && ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_PORT)\b'; do \
	      i=$$((i+1)); sleep 0.1; \
	    done; \
	  fi; \
	  if ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_PORT)\b'; then \
	    echo "port $(CONFORMANCE_PORT) is already bound: something else would answer these tests" >&2; \
	    echo "  a stale broker on this port once answered every test and refused them all," >&2; \
	    echo "  and the run reported 27 of 27 failing as though it had measured saguin" >&2; \
	    exit 1; \
	  fi; \
	fi
	@set -eu; \
	suite="$(CONFORMANCE_DIR)/paho.mqtt.testing"; \
	if [ ! -d "$$suite/.git" ]; then \
	  mkdir -p "$(CONFORMANCE_DIR)"; \
	  git clone -q "$(CONFORMANCE_REPO)" "$$suite"; \
	fi; \
	git -C "$$suite" fetch -q origin "$(CONFORMANCE_SHA)" 2>/dev/null || git -C "$$suite" fetch -q origin; \
	git -C "$$suite" checkout -q "$(CONFORMANCE_SHA)"; \
	cfg="$(CONFORMANCE_DIR)/saguin.yaml"; \
	{ \
	  echo "# Written by \`make conformance\`. A plain MQTT 5 broker with no"; \
	  echo "# channels at all, because what this measures is the claim on the"; \
	  echo "# README's first page: that saguin is an ordinary MQTT 5 broker."; \
	  echo "broker:"; \
	  echo "  id: conformance"; \
	  echo "  # So that a run killed mid-loop can be recovered from rather"; \
	  echo "  # than blocking every later one. The preflight reads this."; \
	  echo "  pid_file: $(CONFORMANCE_DIR)/saguin.pid"; \
	  echo "  # info rather than error, so that a failure is decidable from"; \
	  echo "  # the log this run keeps. At error the broker logs nothing at"; \
	  echo "  # all and a timing failure leaves no evidence behind it; the"; \
	  echo "  # Will lines below are the case that proved it."; \
	  echo "  log_level: info"; \
	  echo "  mqtt:"; \
	  echo "    listen:"; \
	  echo "      tcp:"; \
	  echo "        address: 127.0.0.1:$(CONFORMANCE_PORT)"; \
	  echo "  limits:"; \
	  echo "    # test_server_keep_alive asks for the Server Keep Alive"; \
	  echo "    # property and the value 60. Absent, saguin sends no such"; \
	  echo "    # property, which MQTT 5 permits - so this is the suite"; \
	  echo "    # expecting a configured broker, not a defect."; \
	  echo "    max_keepalive: 60s"; \
	  echo "  storage:"; \
	  echo "    default: mem"; \
	  echo "    default_retention_period: none"; \
	  echo "    default_retention_bytes: none"; \
	  echo "    providers:"; \
	  echo "      - mem:"; \
	  echo "          type: memory"; \
	  echo "          snapshot_dir: none"; \
	  echo "channels: {}"; \
	} > "$$cfg"; \
	./bin/saguin --check-config "$$cfg" >/dev/null; \
	names=$$(sed -n 's/^[[:space:]]*def \(test_[a-zA-Z0-9_]*\)(.*/\1/p' \
	           "$$suite/interoperability/client_test5.py"); \
	total=$$(printf '%s\n' $$names | grep -c .); \
	if [ "$$total" -lt 20 ]; then \
	  echo "read only $$total test names from the suite, which cannot be all of them" >&2; \
	  echo "  the pin may have moved the file, or its shape changed" >&2; \
	  exit 1; \
	fi; \
	echo "$(CONFORMANCE_SHA)" | cut -c1-12 | xargs -I{} \
	  echo "Eclipse Paho MQTT 5 interoperability suite at {}, $$total tests, one at a time"; \
	expected_fail="$(CONFORMANCE_QOS2) $(CONFORMANCE_SUITE_BUG) $(CONFORMANCE_NO_OUTBOUND_ALIAS)"; \
	rc=0; passed=0; failed=0; retried=""; \
	bpid=""; \
	trap '[ -n "$$bpid" ] && kill $$bpid 2>/dev/null; exit 130' HUP INT TERM; \
	trap '[ -n "$$bpid" ] && kill $$bpid 2>/dev/null' EXIT; \
	for n in $$names; do \
	  ./bin/saguin -config "$$cfg" > "$(CONFORMANCE_DIR)/$$n.broker.log" 2>&1 & \
	  bpid=$$!; \
	  i=0; while [ $$i -lt 50 ]; do \
	    if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_PORT)\b'; then break; fi; \
	    i=$$((i+1)); sleep 0.1; \
	  done; \
	  if grep -q "address already in use" "$(CONFORMANCE_DIR)/$$n.broker.log" 2>/dev/null; then \
	    echo "the broker for $$n could not bind the port" >&2; kill $$bpid 2>/dev/null || true; exit 1; \
	  fi; \
	  ( cd "$$suite/interoperability" && timeout 30 python3 ./client_test5.py \
	      -p $(CONFORMANCE_PORT) "Test.$$n" ) > "$(CONFORMANCE_DIR)/$$n.log" 2>&1 \
	    && got=PASS || got=FAIL; \
	  kill $$bpid 2>/dev/null || true; wait $$bpid 2>/dev/null || true; \
	  bpid=""; \
	  want=PASS; \
	  for e in $$expected_fail; do [ "$$e" = "$$n" ] && want=FAIL; done; \
	  if [ "$$got" != "$$want" ] && [ "$$want" = PASS ]; then \
	    ./bin/saguin -config "$$cfg" > "$(CONFORMANCE_DIR)/$$n.broker.log" 2>&1 & \
	    bpid=$$!; \
	    i=0; while [ $$i -lt 50 ]; do \
	      if ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_PORT)\b'; then break; fi; \
	      i=$$((i+1)); sleep 0.1; \
	    done; \
	    ( cd "$$suite/interoperability" && timeout 30 python3 ./client_test5.py \
	        -p $(CONFORMANCE_PORT) "Test.$$n" ) > "$(CONFORMANCE_DIR)/$$n.retry.log" 2>&1 \
	      && got=PASS || got=FAIL; \
	    kill $$bpid 2>/dev/null || true; wait $$bpid 2>/dev/null || true; \
	    bpid=""; \
	    if [ "$$got" = PASS ]; then \
	      retried="$$retried $$n"; \
	    fi; \
	  fi; \
	  if [ "$$got" = "$$want" ]; then \
	    [ "$$got" = PASS ] && passed=$$((passed+1)) || failed=$$((failed+1)); \
	  else \
	    rc=1; \
	    echo "  $$n: $$got, expected $$want - $(CONFORMANCE_DIR)/$$n.log"; \
	  fi; \
	done; \
	if [ -n "$$retried" ]; then \
	  echo "passed only on a second run:$$retried" >&2; \
	  echo "  the suite waits a fixed three seconds for a message, so a loaded" >&2; \
	  echo "  machine fails a test the broker answered correctly. Reported rather" >&2; \
	  echo "  than hidden: if a name appears here on a quiet machine, it is the" >&2; \
	  echo "  broker being slow and not the runner" >&2; \
	fi; \
	echo "$$passed passed, $$failed refused as listed in the Makefile"; \
	if [ "$$rc" -ne 0 ]; then \
	  echo "a test did not do what the list above says it does" >&2; \
	  echo "  a new failure is a defect or a documented refusal nobody wrote down;" >&2; \
	  echo "  a new pass means the list is stale and the README line with it" >&2; \
	fi; \
	if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_311_PORT)\b'; then \
	  echo "port $(CONFORMANCE_311_PORT) is already bound, so the 3.1.1 suite cannot run" >&2; \
	  echo "  every test would answer for whatever is on that port rather than" >&2; \
	  echo "  for saguin. Set CONFORMANCE_311_PORT to a free one." >&2; \
	  exit 1; \
	fi; \
	cfg311="$(CONFORMANCE_DIR)/saguin311.yaml"; \
	sed 's|127.0.0.1:$(CONFORMANCE_PORT)|127.0.0.1:$(CONFORMANCE_311_PORT)|; \
	     s|$(CONFORMANCE_DIR)/saguin.pid|$(CONFORMANCE_DIR)/saguin311.pid|' \
	  "$$cfg" > "$$cfg311"; \
	./bin/saguin --check-config "$$cfg311" >/dev/null; \
	runner="$(CONFORMANCE_DIR)/run311.py"; \
	{ \
	  echo "# Written by 'make conformance'. Runs Eclipse Paho's 3.1.1"; \
	  echo "# interoperability suite against a port of our choosing."; \
	  echo "#"; \
	  echo "# Their v5 suite removes -p from sys.argv before calling"; \
	  echo "# unittest.main(); the 3.1.1 one does not, so a port and a test"; \
	  echo "# name together make unittest reject the port. This execs their"; \
	  echo "# file with the line that sets the default substituted, which"; \
	  echo "# leaves the file on disk untouched -- the suite is the thing"; \
	  echo "# measuring saguin, and an instrument this run had edited would"; \
	  echo "# be measuring something else."; \
	  echo "import os, sys"; \
	  echo ""; \
	  echo "# Python puts this script's own directory on sys.path, not the"; \
	  echo "# working directory, so the suite's 'mqtt' package is invisible"; \
	  echo "# without this. Running their file directly hid the difference."; \
	  echo "sys.path.insert(0, os.getcwd())"; \
	  echo ""; \
	  echo "port, name = sys.argv[1], sys.argv[2]"; \
	  echo "src = open('client_test.py').read()"; \
	  echo "old = '  port = 1883'"; \
	  echo "if src.count(old) != 1:"; \
	  echo "    sys.exit('client_test.py no longer sets its port at exactly '"; \
	  echo "             'one place, so this substitution would be silent and '"; \
	  echo "             'the suite would answer for whatever holds 1883')"; \
	  echo "sys.argv = ['client_test.py', name]"; \
	  echo "exec(compile(src.replace(old, '  port = ' + port),"; \
	  echo "             'client_test.py', 'exec'))"; \
	} > "$$runner"; \
	names311=$$(sed -n 's/^[[:space:]]*def \(test[a-zA-Z0-9_]*\)(.*/\1/p' \
	              "$$suite/interoperability/client_test.py"); \
	total311=$$(printf '%s\n' $$names311 | grep -c .); \
	if [ "$$total311" -lt 8 ]; then \
	  echo "read only $$total311 test names from the 3.1.1 suite, which cannot be all" >&2; \
	  exit 1; \
	fi; \
	echo "Eclipse Paho MQTT 3.1.1 interoperability suite, $$total311 tests, one at a time"; \
	expected311="$(CONFORMANCE_311_QOS2) $(CONFORMANCE_311_GRANT_ONLY)"; \
	passed311=0; failed311=0; \
	for n in $$names311; do \
	  ./bin/saguin -config "$$cfg311" > "$(CONFORMANCE_DIR)/311-$$n.broker.log" 2>&1 & \
	  bpid=$$!; \
	  i=0; while [ $$i -lt 50 ]; do \
	    if ss -ltn 2>/dev/null | grep -q ':$(CONFORMANCE_311_PORT)\b'; then break; fi; \
	    i=$$((i+1)); sleep 0.1; \
	  done; \
	  ( cd "$$suite/interoperability" && timeout 45 \
	      python3 "$$runner" "$(CONFORMANCE_311_PORT)" "Test.$$n" ) \
	    > "$(CONFORMANCE_DIR)/311-$$n.log" 2>&1 && got=PASS || got=FAIL; \
	  kill $$bpid 2>/dev/null || true; wait $$bpid 2>/dev/null || true; \
	  bpid=""; \
	  want=PASS; \
	  for e in $$expected311; do [ "$$e" = "$$n" ] && want=FAIL; done; \
	  if [ "$$got" = "$$want" ]; then \
	    [ "$$got" = PASS ] && passed311=$$((passed311+1)) || failed311=$$((failed311+1)); \
	  else \
	    rc=1; \
	    echo "  3.1.1 $$n: $$got, expected $$want - $(CONFORMANCE_DIR)/311-$$n.log"; \
	  fi; \
	done; \
	echo "3.1.1: $$passed311 passed, $$failed311 refused as listed in the Makefile"; \
	exit $$rc

# Two more broker-compliance suites, and the reason there are three.
#
# The Paho suite above drives saguin from a stock MQTT client and asks
# whether the answers are right. These two ask a different question with a
# codec of their own: **what does saguin do with a packet a client library
# would never send** - a reserved flag set, a QoS 0 PUBLISH carrying a packet
# identifier, a truncated payload. A library cannot express those, so a suite
# built on one cannot test them.
#
# Both were run by hand for months and by nothing else, which is how the two
# defects they find stayed invisible: they were found the first time anybody
# ran them in a session that then wrote this target.
#
# **Both are Rust**, so a toolchain is needed. Where there is none, point the
# target at a binary somebody else built:
#
#   make mqtt5test MQTT5TEST_BIN=/path/to/mqtt_test
#   make mqttconf MQTTCONF_BIN=/path/to/mqtt-conformance
#
# Pinned like the Paho suite, and for the same reason: "whatever main says
# today" is not a result anybody can reproduce, and the expected-failure
# lists below are only meaningful against a known revision.
MQTT5TEST_REPO ?= https://github.com/ifnesi/mqtt_test.git
MQTT5TEST_SHA  ?= 26a97ae4e731371c80f0d87993edd2b1fe0b3f52
MQTT5TEST_DIR  ?= $(CURDIR)/bin/conformance/mqtt5test
MQTT5TEST_PORT ?= 11884
# **Three ports, because saguin has one TCP listener and it is either plain
# or TLS.** The suite asks a broker about each transport independently - no
# test publishes on one and reads on another - so the plain and TLS doors
# are two processes rather than one, and the WebSocket door rides with the
# plain one.
MQTT5TEST_TLS_PORT ?= 11886
MQTT5TEST_WS_PORT  ?= 11887
MQTT5TEST_WS_PATH  ?= /mqtt
MQTT5TEST_BIN  ?=

MQTTCONF_REPO ?= https://github.com/vibesrc/mqttconformance.git
MQTTCONF_SHA  ?= e15945187a48796e8243d4be673f6095d0d5b211
MQTTCONF_DIR  ?= $(CURDIR)/bin/conformance/mqttconf
MQTTCONF_PORT ?= 11885
MQTTCONF_BIN  ?=

# **The three results that are not passes, named individually and each with
# somewhere to read why.** The target fails on any deviation in either
# direction: a new failure is a regression, and a failure that has been fixed
# without shrinking this list is a stale baseline saying the broker is worse
# than it is.
#
# **MQTT-3.3.4-3** - not a defect, and it was recorded as one here until the
# wire was read. saguin sends every matching identifier in one PUBLISH; the
# suite cannot see the second. Against the very broker this target starts,
# the delivery for two overlapping subscriptions carries:
#
#     properties: 04 0b 0a 0b 14
#                 ^^ length 4
#                    ^^^^^ 0x0B Subscription Identifier = 10
#                          ^^^^^ 0x0B Subscription Identifier = 20
#
# The suite holds that property in a scalar - `subscription_identifier:
# Option<u32>` (codec.rs:168) - and assigns it on every occurrence
# (codec.rs:265), so the second overwrites the first and its own report reads
# `got [20]`. A decoder shaped that way cannot observe compliance with the
# requirement it is testing, because the requirement exists so the property
# may repeat.
#
# **It stays on this list.** The suite will go on failing it against any
# compliant broker, so removing it turns this target red for a broker that is
# right. What changed is the reason, which is a claim about saguin's
# behaviour and was the wrong one.
#
# **MQTT-2.2.1-2** - not a defect, and no broker can make it one. The suite
# sends a QoS 0 PUBLISH with two bytes inserted where a Packet Identifier
# would go, and expects it refused as malformed. Nothing is refusable: for
# QoS 0 a parser never reads a Packet Identifier, so it takes the next byte
# as the property length and the rest as payload, and the remaining length
# agrees with that reading.
#
#     30 0e                  PUBLISH, QoS 0, remaining length 14
#     00 05 6d 71 74 74 2f   topic "mqtt/"
#     00 01                  the "illegal Packet Identifier"
#     00                     ... read as property length 0
#     74 65 73 74            ... and payload \x01\x00test
#
# Driven at the wire, saguin delivers that payload and keeps the connection.
# So does mosquitto 2.0.22, byte for byte, on the same packet. The
# requirement constrains what a *client* may send; at QoS 0 the violation
# leaves nothing a receiver can see.
#
# **It stays on this list** for the reason MQTT-3.3.4-3 does: the suite fails
# it against any broker, so removing it turns this target red for one that is
# right.
#
# **MQTT-3.1.2-2** - not a defect, and the reason here used to misdescribe
# what saguin does. It said saguin answers `0x01`. Read at the wire, a v4
# CONNECT is answered `20 02 00 00` - CONNACK, Session Present 0, reason
# `0x00` - and the connection stays open: saguin **accepts** the 3.1.1
# client, which is what RFC 0001 says it does. The suite is asking whether
# this is a v5-only broker and reports `unsupported` rather than a failure
# when it is not.
#
# Removing it from this list means saguin stopped admitting 3.1.1, which is a
# decision rather than a fix.
MQTT5TEST_EXPECTED ?= MQTT-2.2.1-2 MQTT-3.1.2-2 MQTT-3.3.4-3

# mqttconformance passes whole, both protocols, so the baseline is empty and
# any failure at all is red.
MQTTCONF_EXPECTED ?=

# **ifnesi/mqtt_test**, a fork of sammiq/mqtt_test (GPLv3), whose original
# repository is no longer available: an MQTT 5 broker compliance tool with a
# packet codec of its own, so it sends what no client library will - a reserved flag set,
# a QoS 0 PUBLISH carrying a packet identifier, a truncated payload. That is
# the half the Paho suite above cannot reach, because it is built on a client.
#
# **Its exit code is not the result**, measured rather than assumed: it exits
# 0 with three MUST failures in its report. A job checking the status would
# be green having found three, which is the shape of instrument this
# repository refuses - so the report is parsed and compared name by name.
#
# **All three transports, because a skip is what this target exists to
# stop.** The suite carries a TLS half of Transport and three WebSocket
# MUSTs - subprotocol negotiation, closing on a non-binary frame, and not
# assuming MQTT packets align on frame boundaries, which is a genuine
# implementation trap nothing else here tests. saguin opens all three doors
# in its own example, so measuring one of them was measuring a third of what
# it ships.
#
# **The certificate is generated per run into the run directory**, as a CA
# and a leaf it signs rather than one self-signed file: a self-signed
# certificate carries `CA:TRUE`, and rustls refuses a CA presented as the
# end entity - `CaUsedAsEndEntity`, which reads as a saguin TLS failure and
# is a fixture defect. saguin is given the leaf; the suite is given the CA.
mqtt5test: build
	@command -v git >/dev/null 2>&1 || { \
	  echo "git is not on PATH, and the suite is cloned" >&2; exit 1; }
	@command -v openssl >/dev/null 2>&1 || { \
	  echo "openssl is not on PATH, and the TLS door needs a certificate" >&2; \
	  exit 1; }
	@if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':$(MQTT5TEST_PORT)\b'; then \
	  stale="$(MQTT5TEST_DIR)/saguin.pid"; \
	  pid=""; \
	  if [ -r "$$stale" ]; then pid=$$(cat "$$stale" 2>/dev/null || true); fi; \
	  if [ -n "$$pid" ] && [ -r "/proc/$$pid/cmdline" ] && \
	     tr '\0' ' ' < "/proc/$$pid/cmdline" | grep -q "$(MQTT5TEST_DIR)/saguin.yaml"; then \
	    echo "a mqtt5test broker from an earlier run is still holding port $(MQTT5TEST_PORT); stopping it" >&2; \
	    kill "$$pid" 2>/dev/null || true; \
	    i=0; while [ $$i -lt 50 ] && ss -ltn 2>/dev/null | grep -q ':$(MQTT5TEST_PORT)\b'; do \
	      i=$$((i+1)); sleep 0.1; \
	    done; \
	  fi; \
	  if ss -ltn 2>/dev/null | grep -q ':$(MQTT5TEST_PORT)\b'; then \
	    echo "port $(MQTT5TEST_PORT) is already bound: something else would answer these tests" >&2; \
	    echo "  a stale broker on a conformance port once answered every test and refused" >&2; \
	    echo "  them all, and the run reported that as a verdict about saguin" >&2; \
	    exit 1; \
	  fi; \
	fi
	@set -eu; \
	mkdir -p "$(MQTT5TEST_DIR)"; \
	bin="$(MQTT5TEST_BIN)"; \
	if [ -z "$$bin" ]; then \
	  command -v cargo >/dev/null 2>&1 || { \
	    echo "cargo is not on PATH and this suite is Rust" >&2; \
	    echo "  install a toolchain, or point the target at a binary already built:" >&2; \
	    echo "  make mqtt5test MQTT5TEST_BIN=/path/to/mqtt_test" >&2; \
	    exit 1; }; \
	  suite="$(MQTT5TEST_DIR)/mqtt_test"; \
	  if [ -d "$$suite/.git" ] \
	     && [ "$$(git -C "$$suite" remote get-url origin 2>/dev/null)" != "$(MQTT5TEST_REPO)" ]; then \
	    echo "mqtt5test: clone's origin is not $(MQTT5TEST_REPO); cloning again" >&2; \
	    rm -rf "$$suite"; fi; \
	  if [ ! -d "$$suite/.git" ]; then git clone -q "$(MQTT5TEST_REPO)" "$$suite"; fi; \
	  git -C "$$suite" fetch -q origin "$(MQTT5TEST_SHA)" 2>/dev/null \
	    || git -C "$$suite" fetch -q origin; \
	  git -C "$$suite" checkout -q "$(MQTT5TEST_SHA)"; \
	  ( cd "$$suite" && cargo build --release -q ); \
	  bin="$$suite/target/release/mqtt_test"; \
	fi; \
	[ -x "$$bin" ] || { echo "$$bin is not an executable" >&2; exit 1; }; \
	{ \
	  echo "# Written by the Makefile. A plain MQTT 5 broker with no channels,"; \
	  echo "# because what these suites measure is the claim on the README's first"; \
	  echo "# page: that saguin is an ordinary MQTT 5 broker."; \
	  echo "broker:"; \
	  echo "  id: conformance"; \
	  echo "  # Read by the preflight above, so a run killed mid-suite can be"; \
	  echo "  # recovered from rather than blocking every later one."; \
	  echo "  pid_file: $(MQTT5TEST_DIR)/saguin.pid"; \
	  echo "  log_level: info"; \
	  echo "  mqtt:"; \
	  echo "    listen:"; \
	  echo "      tcp:"; \
	  echo "        address: 127.0.0.1:$(MQTT5TEST_PORT)"; \
	  echo "      ws:"; \
	  echo "        address: 127.0.0.1:$(MQTT5TEST_WS_PORT)"; \
	  echo "  limits:"; \
	  echo "    max_keepalive: 60s"; \
	  echo "  storage:"; \
	  echo "    default: mem"; \
	  echo "    default_retention_period: none"; \
	  echo "    default_retention_bytes: none"; \
	  echo "    providers:"; \
	  echo "      - mem:"; \
	  echo "          type: memory"; \
	  echo "          snapshot_dir: none"; \
	  echo "channels: {}"; \
	} > "$(MQTT5TEST_DIR)/saguin.yaml"; \
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
	  -subj "/CN=saguin-conformance-ca" \
	  -keyout "$(MQTT5TEST_DIR)/ca.key" -out "$(MQTT5TEST_DIR)/ca.pem" 2>/dev/null; \
	openssl req -newkey rsa:2048 -nodes -subj "/CN=127.0.0.1" \
	  -keyout "$(MQTT5TEST_DIR)/tls.key" -out "$(MQTT5TEST_DIR)/tls.csr" 2>/dev/null; \
	printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' \
	  > "$(MQTT5TEST_DIR)/tls.ext"; \
	openssl x509 -req -in "$(MQTT5TEST_DIR)/tls.csr" -days 1 \
	  -CA "$(MQTT5TEST_DIR)/ca.pem" -CAkey "$(MQTT5TEST_DIR)/ca.key" -CAcreateserial \
	  -extfile "$(MQTT5TEST_DIR)/tls.ext" -out "$(MQTT5TEST_DIR)/tls.pem" 2>/dev/null; \
	[ -s "$(MQTT5TEST_DIR)/tls.pem" ] || { \
	  echo "openssl produced no leaf certificate, so the TLS door cannot open" >&2; \
	  exit 1; }; \
	sed -e 's|address: 127.0.0.1:$(MQTT5TEST_PORT)|address: 127.0.0.1:$(MQTT5TEST_TLS_PORT)\
        tls:\
          cert_file: $(MQTT5TEST_DIR)/tls.pem\
          key_file: $(MQTT5TEST_DIR)/tls.key|' \
	    -e '/^      ws:/,+1d' \
	    -e 's|pid_file: .*|pid_file: $(MQTT5TEST_DIR)/saguin-tls.pid|' \
	    "$(MQTT5TEST_DIR)/saguin.yaml" > "$(MQTT5TEST_DIR)/saguin-tls.yaml"; \
	./bin/saguin --check-config "$(MQTT5TEST_DIR)/saguin.yaml" >/dev/null; \
	./bin/saguin --check-config "$(MQTT5TEST_DIR)/saguin-tls.yaml" >/dev/null; \
	./bin/saguin -config "$(MQTT5TEST_DIR)/saguin.yaml" > "$(MQTT5TEST_DIR)/broker.log" 2>&1 & \
	bpid=$$!; \
	./bin/saguin -config "$(MQTT5TEST_DIR)/saguin-tls.yaml" > "$(MQTT5TEST_DIR)/broker-tls.log" 2>&1 & \
	tpid=$$!; \
	trap 'kill $$bpid $$tpid 2>/dev/null || true' EXIT; \
	for port in $(MQTT5TEST_PORT) $(MQTT5TEST_TLS_PORT) $(MQTT5TEST_WS_PORT); do \
	  i=0; while [ $$i -lt 50 ]; do \
	    if ss -ltn 2>/dev/null | grep -q ":$$port\b"; then break; fi; \
	    i=$$((i+1)); sleep 0.1; \
	  done; \
	  ss -ltn 2>/dev/null | grep -q ":$$port\b" || { \
	    echo "no door opened on port $$port:" >&2; \
	    tail -5 "$(MQTT5TEST_DIR)/broker.log" "$(MQTT5TEST_DIR)/broker-tls.log" >&2; \
	    exit 1; }; \
	done; \
	echo "ifnesi/mqtt_test (a fork of sammiq/mqtt_test) at $$(echo $(MQTT5TEST_SHA) | cut -c1-12), MQTT 5 over TCP, TLS and WebSocket"; \
	"$$bin" 127.0.0.1 --tcp-port $(MQTT5TEST_PORT) \
	  --tls-port $(MQTT5TEST_TLS_PORT) --ca-cert "$(MQTT5TEST_DIR)/ca.pem" \
	  --ws-port $(MQTT5TEST_WS_PORT) --ws-path $(MQTT5TEST_WS_PATH) \
	  > "$(MQTT5TEST_DIR)/report.txt" 2>&1 || true; \
	kill $$bpid $$tpid 2>/dev/null || true; \
	wait $$bpid 2>/dev/null || true; wait $$tpid 2>/dev/null || true; \
	seen=$$(grep -E '^[[:space:]]*\[' "$(MQTT5TEST_DIR)/report.txt" \
	        | grep -c 'MUST' || true); \
	if [ "$$seen" -lt 100 ]; then \
	  echo "read $$seen MUST result lines from the report, which cannot be all of them" >&2; \
	  echo "  the pin may have moved or the report's shape changed:" >&2; \
	  echo "  $(MQTT5TEST_DIR)/report.txt" >&2; \
	  exit 1; \
	fi; \
	skipped=$$(grep -c 'not configured' "$(MQTT5TEST_DIR)/report.txt" || true); \
	if [ "$$skipped" -ne 0 ]; then \
	  echo "$$skipped requirement(s) were skipped for want of a transport:" >&2; \
	  grep 'not configured' "$(MQTT5TEST_DIR)/report.txt" >&2; \
	  exit 1; \
	fi; \
	for want in 'MQTT-4.2-1' 'MQTT-6.0.0-1' 'MQTT-6.0.0-2' 'MQTT-6.0.0-4'; do \
	  grep -q "\[PASS\].*$$want" "$(MQTT5TEST_DIR)/report.txt" || { \
	    echo "$$want is not reported as passing, so the transport it measures" >&2; \
	    echo "  was not exercised - which is the silence this target exists" >&2; \
	    echo "  to stop, and it survives an unchanged baseline" >&2; \
	    exit 1; }; \
	done; \
	got=$$(grep -E '^[[:space:]]*\[' "$(MQTT5TEST_DIR)/report.txt" \
	       | grep 'MUST' | grep -v '\[PASS\]' | grep -v '\[SKIP\]' \
	       | sed -e 's/.*MUST[[:space:]]*\[\([^]]*\)\].*/\1/' -e 's/[[:space:]]//g' \
	       | sort -u | tr '\n' ' ' | sed 's/ $$//'); \
	want=$$(printf '%s\n' $(MQTT5TEST_EXPECTED) | sort -u | tr '\n' ' ' | sed 's/ $$//'); \
	echo "$$seen MUST requirements examined"; \
	if [ "$$got" != "$$want" ]; then \
	  echo "the expected-failure baseline no longer describes this broker:" >&2; \
	  echo "  got:  $$got" >&2; \
	  echo "  want: $$want" >&2; \
	  echo "  a name in got and not in want is a regression; a name in want and" >&2; \
	  echo "  not in got was fixed, and the list has to shrink with it." >&2; \
	  echo "  The report is $(MQTT5TEST_DIR)/report.txt" >&2; \
	  exit 1; \
	fi; \
	echo "as expected, and nothing else: $$want"

# **vibesrc/mqttconformance**: normative-statement coverage for both
# protocols, mapping each test to the statement it comes from. Run twice,
# because 3.1.1 and 5 are different sets of statements and a pass in one says
# nothing about the other.
#
# Its summary is parsed rather than its exit status, for the reason written
# above mqtt5test: a suite that reports failures and still exits 0 turns a
# status check into a green light that measured nothing.
#
# **TCP only, and that is the suite rather than this target.** `mqtt5test`
# above now drives TLS and WebSocket as well; this one takes `--host` and
# `--port` and offers nothing else, so there is no transport here left
# unmeasured by choice. Written down so the next
# reader does not go looking for the half that does not exist.
mqttconf: build
	@command -v git >/dev/null 2>&1 || { \
	  echo "git is not on PATH, and the suite is cloned" >&2; exit 1; }
	@if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':$(MQTTCONF_PORT)\b'; then \
	  stale="$(MQTTCONF_DIR)/saguin.pid"; \
	  pid=""; \
	  if [ -r "$$stale" ]; then pid=$$(cat "$$stale" 2>/dev/null || true); fi; \
	  if [ -n "$$pid" ] && [ -r "/proc/$$pid/cmdline" ] && \
	     tr '\0' ' ' < "/proc/$$pid/cmdline" | grep -q "$(MQTTCONF_DIR)/saguin.yaml"; then \
	    echo "a mqttconf broker from an earlier run is still holding port $(MQTTCONF_PORT); stopping it" >&2; \
	    kill "$$pid" 2>/dev/null || true; \
	    i=0; while [ $$i -lt 50 ] && ss -ltn 2>/dev/null | grep -q ':$(MQTTCONF_PORT)\b'; do \
	      i=$$((i+1)); sleep 0.1; \
	    done; \
	  fi; \
	  if ss -ltn 2>/dev/null | grep -q ':$(MQTTCONF_PORT)\b'; then \
	    echo "port $(MQTTCONF_PORT) is already bound: something else would answer these tests" >&2; \
	    echo "  a stale broker on a conformance port once answered every test and refused" >&2; \
	    echo "  them all, and the run reported that as a verdict about saguin" >&2; \
	    exit 1; \
	  fi; \
	fi
	@set -eu; \
	mkdir -p "$(MQTTCONF_DIR)"; \
	bin="$(MQTTCONF_BIN)"; \
	if [ -z "$$bin" ]; then \
	  command -v cargo >/dev/null 2>&1 || { \
	    echo "cargo is not on PATH and this suite is Rust" >&2; \
	    echo "  install a toolchain, or point the target at a binary already built:" >&2; \
	    echo "  make mqttconf MQTTCONF_BIN=/path/to/mqtt-conformance" >&2; \
	    exit 1; }; \
	  suite="$(MQTTCONF_DIR)/mqttconformance"; \
	  if [ ! -d "$$suite/.git" ]; then git clone -q "$(MQTTCONF_REPO)" "$$suite"; fi; \
	  git -C "$$suite" fetch -q origin "$(MQTTCONF_SHA)" 2>/dev/null \
	    || git -C "$$suite" fetch -q origin; \
	  git -C "$$suite" checkout -q "$(MQTTCONF_SHA)"; \
	  ( cd "$$suite" && cargo build --release -q ); \
	  bin="$$suite/target/release/mqtt-conformance"; \
	fi; \
	[ -x "$$bin" ] || { echo "$$bin is not an executable" >&2; exit 1; }; \
	{ \
	  echo "# Written by the Makefile. A plain MQTT 5 broker with no channels,"; \
	  echo "# because what these suites measure is the claim on the README's first"; \
	  echo "# page: that saguin is an ordinary MQTT 5 broker."; \
	  echo "broker:"; \
	  echo "  id: conformance"; \
	  echo "  # Read by the preflight above, so a run killed mid-suite can be"; \
	  echo "  # recovered from rather than blocking every later one."; \
	  echo "  pid_file: $(MQTTCONF_DIR)/saguin.pid"; \
	  echo "  log_level: info"; \
	  echo "  mqtt:"; \
	  echo "    listen:"; \
	  echo "      tcp:"; \
	  echo "        address: 127.0.0.1:$(MQTTCONF_PORT)"; \
	  echo "  limits:"; \
	  echo "    max_keepalive: 60s"; \
	  echo "  storage:"; \
	  echo "    default: mem"; \
	  echo "    default_retention_period: none"; \
	  echo "    default_retention_bytes: none"; \
	  echo "    providers:"; \
	  echo "      - mem:"; \
	  echo "          type: memory"; \
	  echo "          snapshot_dir: none"; \
	  echo "channels: {}"; \
	} > "$(MQTTCONF_DIR)/saguin.yaml"; \
	./bin/saguin --check-config "$(MQTTCONF_DIR)/saguin.yaml" >/dev/null; \
	echo "vibesrc/mqttconformance at $$(echo $(MQTTCONF_SHA) | cut -c1-12), MQTT 5 and 3.1.1"; \
	rc=0; \
	for v in 5 3; do \
	  ./bin/saguin -config "$(MQTTCONF_DIR)/saguin.yaml" \
	    > "$(MQTTCONF_DIR)/broker-v$$v.log" 2>&1 & \
	  bpid=$$!; \
	  i=0; while [ $$i -lt 50 ]; do \
	    if ss -ltn 2>/dev/null | grep -q ':$(MQTTCONF_PORT)\b'; then break; fi; \
	    i=$$((i+1)); sleep 0.1; \
	  done; \
	  if ! ss -ltn 2>/dev/null | grep -q ':$(MQTTCONF_PORT)\b'; then \
	    echo "the broker never bound port $(MQTTCONF_PORT):" >&2; \
	    tail -5 "$(MQTTCONF_DIR)/broker-v$$v.log" >&2; \
	    kill $$bpid 2>/dev/null || true; exit 1; \
	  fi; \
	  "$$bin" run -H 127.0.0.1 -p $(MQTTCONF_PORT) -v $$v \
	    > "$(MQTTCONF_DIR)/report-v$$v.txt" 2>&1 || true; \
	  kill $$bpid 2>/dev/null || true; wait $$bpid 2>/dev/null || true; \
	  total=$$(sed -n 's/.*[0-9][0-9]* total.*/&/p' "$(MQTTCONF_DIR)/report-v$$v.txt" \
	           | tail -1 | tr -cd '0-9'); \
	  failed=$$(grep 'failed' "$(MQTTCONF_DIR)/report-v$$v.txt" | tail -1 | tr -cd '0-9'); \
	  if [ -z "$$total" ] || [ -z "$$failed" ]; then \
	    echo "no summary could be read out of the v$$v report, so nothing was measured" >&2; \
	    echo "  $(MQTTCONF_DIR)/report-v$$v.txt" >&2; \
	    rc=1; continue; \
	  fi; \
	  if [ "$$total" -lt 90 ]; then \
	    echo "v$$v ran $$total tests, which cannot be all of them" >&2; \
	    rc=1; continue; \
	  fi; \
	  echo "  MQTT $$v: $$total tests, $$failed failed"; \
	  if [ "$$failed" -ne 0 ]; then \
	    grep -E "FAIL|✗" "$(MQTTCONF_DIR)/report-v$$v.txt" | head -20 >&2; \
	    echo "  the report is $(MQTTCONF_DIR)/report-v$$v.txt" >&2; \
	    rc=1; \
	  fi; \
	done; \
	exit $$rc

# The property targets, actually fuzzed.
#
# **`go test` runs a fuzz target's seed corpus and stops.** The fuzzing
# itself needs `-fuzz`, one target at a time - so a target added and left
# alone becomes exactly the shape `make bench` exists to prevent: it
# compiles, it is green, and it has not fuzzed anything since the day it was
# written. This runs each of them.
#
# **The list is derived from the toolchain, not written here.** `go test
# -list` asks the packages themselves, so a target added tomorrow is fuzzed
# tomorrow - a list kept in this file is one target short the day somebody
# adds the next one, and short silently.
#
# It is in `check`, which is what makes it run.
#
# **A count of executions rather than a wall-clock dial**, for two reasons.
# A duration does different work on a fast machine and a slow one, and what
# `check` asks is whether the properties still hold rather than how much
# hardware somebody has. And `-fuzztime 5s` fails outright with `context
# deadline exceeded` when the clock runs out mid-iteration - measured, on
# this target, in `make check` - which is a coin flip in CI and is what this
# repository refuses. A count cannot expire.
#
# FUZZTIME turns it into a hunt, and takes either form:
# `make fuzz FUZZTIME=2m` or `make fuzz FUZZTIME=20000000x`.
#
# A counterexample is written to the package's testdata/fuzz and becomes a
# seed, which is the mechanism that turns one random find into a permanent
# regression test. That directory is committed.
FUZZTIME ?= 300000x

fuzz:
	@set -eu; \
	found=0; failed=0; \
	for pkg in $$(go list ./...); do \
	  for t in $$(go test -list 'Fuzz.*' $$pkg 2>/dev/null | grep '^Fuzz' || true); do \
	    found=$$((found+1)); \
	    printf '  %-52s ' "$$t"; \
	    if out=$$(go test $$pkg -run '^$$' -fuzz "^$$t$$" -fuzztime $(FUZZTIME) 2>&1); then \
	      echo "$$out" | grep -oE 'execs: [0-9]+' | tail -1 || echo ok; \
	    else \
	      echo FAIL; echo "$$out" >&2; failed=$$((failed+1)); \
	    fi; \
	  done; \
	done; \
	if [ "$$found" -eq 0 ]; then \
	  echo "no fuzz target was found in any package: this target fuzzed nothing" >&2; \
	  echo "  and would have said ok, which is the failure it exists to prevent" >&2; \
	  exit 1; \
	fi; \
	echo "fuzzed $$found target(s) for $(FUZZTIME) each"; \
	[ "$$failed" -eq 0 ]

# Is the connectors demo actually doing anything?
#
# **Review found nine faults in it, and
# nothing in this repository caught one of them.** `make check` never
# reaches examples/. `bento lint` passed a pipeline that would not start.
# `docker compose ps` showed every container healthy over a bridge that had
# stalled. The dashboard drew curves over a queue that had never delivered a
# job. Every assertion in the script is one of those faults.
#
# **Not part of `check`**, for the reason `mosquitto` and `conformance` are
# not: it needs a Docker daemon, several minutes and the Confluent images,
# and a check that skipped when one was missing would be a green tick over
# work nobody did. Run it before a release and after any change to the
# example, which is the standing `soak` has.
#
# It leaves the stack up, because a failure is a thing to look at.
smoke:
	@examples/bento-connectors/smoke.sh

# Retention under concurrent load: the floor moving while many readers read.
#
# **Run it after any change to delivery, retention or locking.** That is the
# cadence, and it is written here because here is where somebody looking for
# it will be. The five orphaned benchmarks in this repository are what
# happens to an instrument with no standing occasion to run: one of them
# failed on every commit from the day it was written until somebody happened
# to try it by hand.
#
# It is not `make check`'s job and does not pretend to be. What this proves -
# one reader passed correctly by the floor while its neighbour on the same
# channel loses nothing - needs real sockets, real pacing and minutes of
# floor movement. `make check` carries a seconds-scale smoke of the same role
# (TestTheRetentionRoleStillRuns), which asserts that the role still runs and
# nothing about the broker's behaviour under load.
#
# The broker is yours to start, on the same machine, with append channels
# whose `retention_bytes` is small enough that the sweep trims repeatedly
# while the run lasts - 512KiB against a 400-byte payload is the size the
# 2026-09-22 run used. CHANNELS names them as `channel:topic-prefix` pairs.
# No CA means plaintext, which is what a broker on localhost normally is;
# set CA to drive one over TLS.
#
# **A skipped role fails this target, and so does a failed one.**
# TestScaleRole skips itself when SAGUIN_SCALE_ROLE is unset, so a typo in
# the plumbing below would leave `go test` exiting 0 over a run that never
# happened. The verdict is read out of the output rather than from `$$?`,
# because the run is piped through `tee` so that a six-minute run prints as
# it goes - and a pipeline's status is its last command's, which is tee's,
# which is always 0. Taken from `$$?`, this target reported a FAILED run as
# success: the same green tick for work not done, one case further along.
#
# **The credentials are named for this target.** `USER` is in the
# environment of every Unix shell and make imports it, so a variable of
# that name is never empty - a run against the anonymous localhost broker
# this role documents would send the operator's login name as its MQTT
# username. `make scale` has the same shape and has not been bitten,
# because its runbook always passes credentials explicitly; it is worth
# renaming there too, when its runbook is next touched.
RETENTION ?= 6m
RETENTION_CHANNELS ?= trim-db:trim/db,trim-mem:trim/mem
retention:
	@t=$$(echo "$(RETENTION)" | awk '/^[0-9]+[smh]$$/ { \
	        n = $$0 + 0; u = substr($$0, length($$0)); \
	        if (u == "h") n *= 3600; else if (u == "m") n *= 60; \
	        printf "%d", n + 600; exit } \
	      { print "bad"; exit }'); \
	if [ "$$t" = "bad" ]; then \
	  echo "RETENTION=$(RETENTION) is not one number and one unit, such as 90s, 6m or 1h" >&2; \
	  exit 1; \
	fi; \
	if [ -z "$(BROKER)" ] || [ -z "$(OPS)" ]; then \
	  echo "BROKER=host:1883 and OPS=host:9090 are both required: this role reads the" >&2; \
	  echo "  broker's own counters and /v1/operations/position-lost, and will not" >&2; \
	  echo "  infer either" >&2; \
	  exit 1; \
	fi; \
	n=$$(go test ./internal/scaletest/ -list TestScaleRole | grep -c '^Test'); \
	if [ "$$n" -eq 0 ]; then \
	  echo "no test matches TestScaleRole: this target would run nothing and say ok" >&2; \
	  exit 1; \
	fi; \
	echo "retention under load for $(RETENTION) on $(RETENTION_CHANNELS), -timeout $${t}s"; \
	out=$$(mktemp); \
	SAGUIN_SCALE_ROLE=retention SAGUIN_SCALE_BROKER="$(BROKER)" \
	SAGUIN_SCALE_OPS="$(OPS)" SAGUIN_SCALE_CA="$(CA)" \
	SAGUIN_SCALE_USER="$(RETENTION_USER)" SAGUIN_SCALE_PASS="$(RETENTION_PASS)" \
	SAGUIN_SCALE_ADMIN_USER="$(ADMIN_USER)" SAGUIN_SCALE_ADMIN_PASS="$(ADMIN_PASS)" \
	SAGUIN_SCALE_DURATION="$(RETENTION)" SAGUIN_SCALE_SETTLE="$(SETTLE)" \
	SAGUIN_SCALE_REPORT="$(REPORT)" SAGUIN_SCALE_CHANNELS="$(RETENTION_CHANNELS)" \
	SAGUIN_SCALE_FAST="$(FAST)" SAGUIN_SCALE_SLOW="$(SLOW)" \
	SAGUIN_SCALE_PUBLISHERS="$(PUBLISHERS)" SAGUIN_SCALE_PAUSE="$(PAUSE)" \
	SAGUIN_SCALE_PAD="$(PAD)" \
	go test ./internal/scaletest/ -run TestScaleRole -v -count=1 -timeout $${t}s \
	  2>&1 | tee $$out; \
	if grep -q '^--- SKIP: TestScaleRole' $$out; then \
	  echo "TestScaleRole skipped itself, so this target measured nothing and would" >&2; \
	  echo "  otherwise have reported success" >&2; \
	  rm -f $$out; exit 1; \
	fi; \
	if ! grep -qE '^--- (PASS|FAIL): TestScaleRole' $$out; then \
	  echo "TestScaleRole neither passed nor failed, so it did not run" >&2; \
	  rm -f $$out; exit 1; \
	fi; \
	if grep -q '^--- FAIL: TestScaleRole' $$out; then \
	  rm -f $$out; exit 1; \
	fi; \
	rm -f $$out; exit 0

# The scale-run harness (internal/scaletest), one role per machine, driven
# by hand or by a runbook. Never part of `make check`: without
# a ROLE the one test skips. SCALE is one number and one unit, as SOAK is;
# the go test timeout is derived from it, with room for the ramp, the
# settle window and the waits between roles. SCENARIO=gateway runs the fat
# publisher shape with SIZE, COHORT, SHAPE, READY, DONE and TARGET; the
# internal/scaletest/scale_test.go says what each is.
SCALE ?= 30m
scale:
	@t=$$(echo "$(SCALE)" | awk '/^[0-9]+[smh]$$/ { \
	        n = $$0 + 0; u = substr($$0, length($$0)); \
	        if (u == "h") n *= 3600; else if (u == "m") n *= 60; \
	        printf "%d", n + 3600; exit } \
	      { print "bad"; exit }'); \
	if [ "$$t" = "bad" ]; then \
	  echo "SCALE=$(SCALE) is not one number and one unit, such as 90s, 2m or 1h" >&2; \
	  exit 1; \
	fi; \
	if [ -z "$(ROLE)" ]; then \
	  echo "ROLE is required: consumers, publishers, say, watch or kvget" >&2; \
	  exit 1; \
	fi; \
	SAGUIN_SCALE_ROLE="$(ROLE)" SAGUIN_SCALE_BROKER="$(BROKER)" \
	SAGUIN_SCALE_OPS="$(OPS)" SAGUIN_SCALE_CA="$(CA)" \
	SAGUIN_SCALE_USER="$(USER)" SAGUIN_SCALE_PASS="$(PASS)" \
	SAGUIN_SCALE_ADMIN_USER="$(ADMIN_USER)" SAGUIN_SCALE_ADMIN_PASS="$(ADMIN_PASS)" \
	SAGUIN_SCALE_DURATION="$(SCALE)" SAGUIN_SCALE_REPORT="$(REPORT)" \
	SAGUIN_SCALE_CLIENTS="$(CLIENTS)" SAGUIN_SCALE_PEERS="$(PEERS)" \
	SAGUIN_SCALE_RATE="$(RATE)" SAGUIN_SCALE_RAMP="$(RAMP)" \
	SAGUIN_SCALE_SETTLE="$(SETTLE)" SAGUIN_SCALE_TOPIC="$(TOPIC)" \
	SAGUIN_SCALE_MSG="$(MSG)" \
	SAGUIN_SCALE_SCENARIO="$(SCENARIO)" SAGUIN_SCALE_SIZE="$(SIZE)" \
	SAGUIN_SCALE_COHORT="$(COHORT)" SAGUIN_SCALE_SHAPE="$(SHAPE)" \
	SAGUIN_SCALE_READY="$(READY)" SAGUIN_SCALE_DONE="$(DONE)" \
	SAGUIN_SCALE_TARGET="$(TARGET)" \
	go test ./internal/scaletest/ -run TestScaleRole -v -count=1 -timeout $${t}s
