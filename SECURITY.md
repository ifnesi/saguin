# Security policy

## Reporting a vulnerability

**Do not open a public issue.** Use GitHub's private vulnerability
reporting - the "Report a vulnerability" button under the repository's
Security tab - or email
[italonesi@gmail.com](mailto:italonesi@gmail.com).

Include what you can: the affected commit, what an attacker gains, and the
smallest reproduction you have. A proof of concept is welcome but not
required; a clear description of the flaw is enough to start.

You will get an acknowledgement within a week. This is a
single-maintainer project, so please read that as a genuine commitment
rather than a service level - there is no on-call rotation behind it.

Disclose publicly once a fix is released, or after 90 days, whichever
comes first. If a fix is taking longer, we will say so rather than let the
clock run out quietly.

## Supported versions

None yet. Sagüin is pre-release and has never been tagged. Only `main`
receives fixes, and there is nothing deployed to protect. This section
becomes meaningful at v0.1.

## Scope

**In scope:** authentication or authorization bypass; reading a channel or
its dead-letter companion without the corresponding grant; credential
material reaching a log, metric, or error message; forging or replaying a
Delivery ID to resolve work you were not given; and any violation of an
invariant in [invariants.md](docs/invariants.md) that an untrusted
MQTT client can trigger.

That last category is the interesting one. Sagüin's invariants are mostly
failures that **report success** - a consumer silently skipping records
past a retention floor, two workers both acknowledging the same job - and
that is exactly where security defects hide.

**Out of scope:** anything requiring an attacker who can already read or
write the SQLite database or the configuration file, which is game-over by
design. Also out of scope: resource exhaustion above the stated throughput
target, and the absence of clustering, high availability, or encryption at
rest, all of which are documented
[non-goals](docs/rfcs/0001-overview-and-scope.md#non-goals).

## Instead of a bug bounty

There is no bounty and no swag. There is
[invariants.md](docs/invariants.md) - a list of failures the system must
never exhibit - which is where a finding of the interesting kind lands as
a rule, with the failure it prevents written beside it.

If you find something, credit in the fix commit and the release notes is
yours unless you would rather not be named.
