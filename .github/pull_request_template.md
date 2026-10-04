<!--
Thanks for contributing. This is a checklist, not a gate - delete any line
that does not apply rather than padding it.
-->

## What and why

<!-- The diff says what changed. Say why it should change. -->

## Specification

<!--
Which RFC sections does this implement or rely on? Name them, e.g.
RFC 0002 "Which channel a topic belongs to".

Does it need an RFC amendment? Short version: yes for anything that
changes what an operation accepts, returns, or rejects, adds a config key,
error, metric, or reason code, or touches a durable format or the MQTT
mapping. No for bug fixes toward already-stated behaviour, tests, docs,
refactors, and performance work. Full table in CONTRIBUTING.md.
-->

- RFC sections:
- Needs an RFC amendment: no / yes (linked)

## Checks

- [ ] No MQTT extension - anything touching the wire cites the standard
      MQTT 5 mechanism it uses
- [ ] Any new rule is enforced by the broker, not by an SDK or convention
- [ ] Where an invariant applies, a test asserts the *failure*, not only
      the happy path
- [ ] Commits signed off (`git commit -s`) - [DCO](https://developercertificate.org/)

<!-- No CLA. Apache-2.0 section 5 already covers it; the sign-off is all. -->
