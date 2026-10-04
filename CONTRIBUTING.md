# Contributing to Sagüin

Contributions are welcome, and the barrier is deliberately low. This
document is short because it should be.

## The most useful thing you can do today

Sagüin is early: it runs, and the specification is ahead of it. So the
highest-value contribution available right now is:

**Read an RFC and tell us where it is wrong, ambiguous, or
self-contradictory.**

No Go, no running server, no setup - just a careful reading. If two people
could read a sentence and build different things, that is a defect even
when neither reading is wrong, and it is worth an issue. Several sections
exist because someone asked "which of these two did you mean?"

Also welcome: documentation fixes, examples, and tests that assert a
behaviour an RFC states but nothing currently checks.

## The one rule that is unusual

**Behaviour is specified before it is implemented.** Every behaviour
implements a named section of a numbered RFC. If a change needs behaviour
no RFC states, the RFC changes first - in the same pull request or an
earlier one. Inventing it in code is how a specification and its
implementation quietly diverge.

Most changes need no RFC edit:

| Change | RFC amendment? |
|---|---|
| Bug fix, so the code matches what an RFC already says | No |
| Tests, docs, comments, examples, refactors, performance | No |
| Changing what an operation accepts, returns, or rejects | **Yes** |
| A new configuration key, error, metric, or reason code | **Yes** |
| Anything touching a durable format or the MQTT mapping | **Yes** |
| Resolving an ambiguity you found | **Yes** - that *is* the fix |

Unsure? Open an issue and ask before writing code.

## Two things that will get a change sent back

**An MQTT extension.** Sagüin defines no packets, properties, or flags of
its own. If a change cannot cite the standard MQTT 5 mechanism it uses, it
is an extension, and the answer is no. See
[non-goals](docs/rfcs/0001-overview-and-scope.md#non-goals).

**A rule enforced anywhere but the broker.** Validation that lives in an
SDK, a convention, or a well-behaved client is not validation.
A stock MQTT client reaches the same broker.

## Conventions in the code

- **Every rule the broker enforces has a test, and the test names the RFC
  section it asserts** in a comment on the test function. This is how the
  code and the specification are kept from drifting apart: a rule with no
  test is a rule nothing is holding down, and
  `grep -rn "// RFC 000" --include="*_test.go"` is the list of the ones
  that are. Prose cannot fail; a test can.
- Tests assert the **failure**, not only the happy path. Most failures in
  [invariants.md](docs/invariants.md) report success when the code is
  broken, so a green happy-path test proves nothing about them.
- Anything that accumulates has a stated limit and defined behaviour at
  that limit.
- No panics on a request path. A malformed packet is an error to return.
- Zero values of enumerated types are invalid, and every switch over one
  has a `default` that returns an error.

## Conventions in the documents

If you are editing an RFC or the invariants:

- **State the failure, not just the rule.** Nearly every requirement
  exists because a plausible implementation would otherwise report success
  while losing or duplicating data. A rule without its failure gets
  "simplified" by the next reader.
- **Describe what is, not what was.** No rejected alternatives, no
  "previously", no "no longer". A decided trade-off is written as the
  decision and its cost. History is in `git log`.
- **Nothing is referenced that is not defined.** A type or operation named
  in an RFC and defined nowhere guarantees the implementer invents one.
- **Name the MQTT 5 mechanism.** Every behaviour that touches the wire
  cites the standard mechanism it uses - a property, a reason code, a
  subscription form. One that cannot is an extension.
- **Nothing new gets a new file.** New information goes into a document
  that already exists, or it does not get written.

## Issue or pull request?

An RFC defect, an ambiguity, or a bug you are not fixing yourself starts
as an issue. A fix that makes the code match what an RFC already says can
arrive directly as a pull request. Anything that changes behaviour starts
as an issue, because the RFC changes first, and agreeing on a sentence is
cheaper than reviewing code twice.

## Pull requests

Fork, work on a short-lived branch, and open the pull request against
`main`. There is no `develop` branch and no branch naming convention.
The maintainer's changes arrive the same way.

**Run the full suite locally before you push.** GitHub runs only the
quick set (`make ci`) and the Paho conformance suite on a pull request;
the rest is yours to run, and a pull request should say that you ran it:

```sh
make check        # race detector, timing, benchmarks, fuzzing; about 35 minutes
make conformance  # Eclipse Paho MQTT 5 and 3.1.1 suites; about 6 minutes
make mqtt5test    # needs cargo; about 2 minutes
make mqttconf     # needs cargo; about 2 minutes
```

On macOS, the scale tests open hundreds of connections at once, and the
default listen queue (`kern.ipc.somaxconn=128`) makes macOS reset them; run
`sudo sysctl kern.ipc.somaxconn=1024` first (it lasts until reboot).

One logical change each. Name the RFC sections it implements. In the
commit message, say *why* - the diff already says what.

Changes land by rebase or squash, never a merge commit, so the history
stays linear and each change keeps its reasoning in one commit. `main`
is never force-pushed: a commit id, once it exists, stays valid, and
reviews and records refer to commits by id. Delete the branch after the
merge, or let GitHub do it.

## Releases

Releases are tags cut from `main`, so `main` is usually ahead of the
latest release. The maintainer tags one only after an overnight
`make stress` passes on it - what that runs is in the Makefile. Install
a release; building `main` gets you work in progress. A fix to a
released version, when `main` already carries newer work, is made on a
branch cut from that release's tag and tagged from there.

### Cutting a release

For the maintainer. A release is a `v*` tag pushed to GitHub; the release
workflow (`.github/workflows/release.yml`) does the rest and runs on
nothing else.

1. `main` is the commit to release, its CI is green and an overnight
   `make stress` has passed on it.
2. `CHANGELOG.md`: the version's section is dated,
   `## X.Y.Z - YYYY-MM-DD`.
   That section is the release notes.
3. `const Version` in `cmd/saguin/main.go` names the same version. It is a
   constant, so it cannot be set at build time, and the workflow refuses a
   tag that disagrees with it. The README's container image tag names it
   too.
4. `make docs`, then commit and push `main`.
5. Tag and push: `git tag -a vX.Y.Z -m "Sagüin X.Y.Z"` and
   `git push origin vX.Y.Z`.

The workflow refuses the release if the tag, `Version` and a dated
CHANGELOG section disagree. Otherwise it builds Linux (amd64, arm64,
armv7) and macOS (amd64, arm64) archives with no cgo, each holding the
binary, `LICENSE`, the third-party notices and `README.md`; publishes a
GitHub release with those archives, `checksums.txt` and the CHANGELOG
section as its notes (a version with a hyphen is a pre-release); and
pushes the multi-arch image (linux/amd64, linux/arm64) to
`ghcr.io/ifnesi/saguin`, where `latest` moves only for a version without a
hyphen. There is no Windows build.

Check it with `gh release view vX.Y.Z`, the checksums of a downloaded
archive, and `docker run --rm ghcr.io/ifnesi/saguin:X.Y.Z --version` from
a machine that is not logged in. A new ghcr.io package starts private:
link it to this repository and make it public in its settings.

If the run fails before anything was published, delete the tag
(`git push --delete origin vX.Y.Z`, then `git tag -d vX.Y.Z`), fix, and
tag again. Once a release or image carries a tag, never move it: fix
forward with the next version.

## Sign-off

Sign your commits with `git commit -s`, which adds a
[DCO](https://developercertificate.org/) line certifying you have the
right to submit the contribution. That is the whole process: no CLA, no
account to create, no company to involve. Apache-2.0 section 5 already
places your contribution under the project's licence.

## Conduct

Be decent to each other. Assume the person on the other end is trying to
help. Disagreement about a design is normal and welcome; contempt is not.
