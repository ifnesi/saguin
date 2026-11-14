# Contributing to Sagüin

Contributions are welcome, and the barrier is deliberately low. This
document is short because it should be.

## The most useful thing you can do today

Sagüin is early: it runs, and the design is ahead of it. So the
highest-value contribution available right now is:

**Read the design and tell us where it is wrong, ambiguous, or
self-contradictory.**

The design is four documents: [design.md](docs/design.md),
[config.md](docs/config.md), [metrics.md](docs/metrics.md) and
[storage.md](docs/storage.md). No Go, no running server, no setup - just a
careful reading. If two people could read a sentence and build different
things, that is a defect even when neither reading is wrong, and it is
worth an issue.

Also welcome: documentation fixes, and tests that check a behaviour the
design states but nothing currently checks.

## The one rule that is unusual

**Behaviour is designed before it is built.** The design is the single
source of truth. If a change needs behaviour the design does not state,
the design changes first - in the same pull request or an earlier one.
Inventing it in code is how a design and its implementation quietly
diverge.

Most changes need no design edit:

| Change | Design change? |
|---|---|
| Bug fix, so the code matches what the design already says | No |
| Tests, comments, refactors, performance | No |
| Changing what an operation accepts, returns, or refuses | **Yes** |
| A new configuration key, metric, or reason code | **Yes** |
| Anything touching a stored table or the MQTT mapping | **Yes** |
| Resolving an ambiguity you found | **Yes** - that *is* the fix |

Unsure? Open an issue and ask before writing code.

## Two things that will get a change sent back

**An MQTT extension.** Sagüin defines no packets or flags of its own. Its
additions use MQTT's own mechanisms: User Properties and topics
([design.md, R7](docs/design.md#rules)). If a change cannot cite the
standard mechanism it uses, it is an extension, and the answer is no.

**A rule enforced anywhere but the broker.** Validation that lives in an
SDK, a convention, or a well-behaved client is not validation.
A stock MQTT client reaches the same broker.

## Conventions in the code

- **The broker runs exactly the goroutines in the design's R1 table**, and
  one goroutine, the core, owns all shared state. No mutexes, no new
  goroutine without the table changing first.
- **Every rule the broker enforces has a test, and the test cites the
  design or MQTT sentence it checks** in a comment on the test function.
  Prose cannot fail; a test can.
- **Tests drive the broker from outside**, as a client or an operator sees
  it: the wire, the command line, the files and `/metrics`. A test inside
  the code checks only what neither can see
  ([design.md, Testing](docs/design.md#testing)).
- Test oracles come from the design and the MQTT standards in
  [docs/mqtt](docs/mqtt/), never from the implementation.
- **Problems are fixed at their cause.** No retry, fallback, special case
  or wider timeout that makes a symptom go away while the cause stays.
- Anything that accumulates has a stated limit and defined behaviour at
  that limit ([design.md, R6](docs/design.md#rules)).
- Reuse, never duplicate: code and tests call the helpers the repository
  already has.

## Conventions in the documents

If you are editing the design:

- **Describe what is, not what was.** No rejected alternatives, no
  "previously", no "no longer". Why a choice was made belongs in its
  commit message. History is in `git log`.
- **One fact, one place.** A rule has one normative home; other places
  link to it.
- **Nothing is referenced that is not defined.** A thing named in the
  design and defined nowhere guarantees the implementer invents one.
- **Name the MQTT mechanism.** Every behaviour that touches the wire cites
  the standard mechanism it uses. One that cannot is an extension.

## Issue or pull request?

A design defect, an ambiguity, or a bug you are not fixing yourself starts
as an issue. A fix that makes the code match what the design already says
can arrive directly as a pull request. Anything that changes behaviour
starts as an issue, because the design changes first, and agreeing on a
sentence is cheaper than reviewing code twice.

## Pull requests

Fork, work on a short-lived branch, and open the pull request against
`main`. There is no `develop` branch and no branch naming convention.

**Run the suite locally before you push**, and say in the pull request
that you ran it:

```sh
make ci          # lint, tests with the race detector, conformance, fuzz,
                 # a short soak and the end-to-end fuzzer; about 6 minutes
make soak-full   # the long soak; about 10 minutes
make fuzz-full   # every fuzz target for a minute, then 10 minutes end to end
```

One logical change each. Name the design sections it implements. In the
commit message, say *why* - the diff already says what.

Changes land by rebase or squash, never a merge commit, so the history
stays linear and each change keeps its reasoning in one commit. `main`
is never force-pushed: a commit id, once it exists, stays valid, and
reviews and records refer to commits by id.

## Releases

Releases are tags cut from `main`, so `main` is usually ahead of the
latest release. Each release has its section in
[CHANGELOG.md](CHANGELOG.md), which is its release notes.

To cut a release:

1. Give the release its dated section in CHANGELOG.md,
   `## <version> - YYYY-MM-DD`, and commit it to `main` with everything
   the release holds.
2. Tag that commit `v<version>` and push the tag:
   `git tag -a v0.2.0 -m "Sagüin 0.2.0"`, then `git push origin v0.2.0`.
3. The release workflow (`.github/workflows/release.yml`) does the rest. It
   refuses a tag with no dated section, or one the built binary does not
   report as its version. It builds the Linux amd64 and arm64 binaries,
   each on its own machine, tests the container image for both, pushes it
   to `ghcr.io/ifnesi/saguin`, and publishes the GitHub release with the
   archives, `SHA256SUMS` and the section as its notes. A tag with a hyphen
   (`v0.3.0-rc.1`) is a pre-release: it gets no `latest` image.

## Sign-off

Sign your commits with `git commit -s`, which adds a
[DCO](https://developercertificate.org/) line certifying you have the
right to submit the contribution. That is the whole process: no CLA, no
account to create, no company to involve. Apache-2.0 section 5 already
places your contribution under the project's licence.

## Conduct

Be decent to each other. Assume the person on the other end is trying to
help. Disagreement about a design is normal and welcome; contempt is not.
