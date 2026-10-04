---
name: saguin-ask
description: "Answer operator and integrator questions about saguin from this repository's own documentation, including behavior, configuration, capabilities, and troubleshooting. Use when someone asks how saguin works, what a setting does, whether it supports something, why the broker behaved a certain way, or what a configuration should contain. Read-only: do not change repository files, code, tests, or documentation."
---

# Answer questions about saguin

Answer from the files in this checkout, not from memory or generic MQTT knowledge. Give the shortest complete answer that is true for the user's deployment, cite the document and section supporting each claim, and say "The documentation does not say" when the corpus does not establish an answer.

The user is an operator or integrator running saguin, or preparing to run it. Focus on behavior and configuration rather than contributor history. Follow-up questions retain their conversational context. If no question was supplied, ask for one before retrieving documents.

The user's instructions take precedence over this skill. If they ask for work beyond answering from the docs, clearly separate that work from this read-only documentation workflow.

## Boundaries

- Do not change any repository document, code, test, or configuration.
- Do not use `git` or `gh`. Answer from files as they currently exist.
- Do not run commands that change tracked files. Put any approved probe or draft configuration in a temporary directory.
- Source claims from the documented corpus. You may inspect implementation or tests only to check whether documentation agrees with the broker. Go source alone does not support an operator-facing answer; if code and docs disagree, state the discrepancy and stop rather than fixing it.
- Do not substitute MQTT specifications, other brokers, or general expectations for saguin documentation.

## Documentation corpus

- `README.md`: broker overview; RFCs contain the details.
- `docs/invariants.md`: numbered behavioral rules.
- `docs/rfcs/0001-*.md` through `docs/rfcs/0005-*.md`: scope, configuration, delivery, storage, and operations.
- `examples/`: annotated `saguin.yaml`, ACL examples, demonstrations, and channel/storage fragments.
- `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, and `LICENSE`: reporting, contributing, releases, and licensing.

Do not treat assistant working files, project-management systems, or repository history as product documentation. `docs/saguin-agent/` implements the same grounded-or-refused documentation job for users without a checkout; keep local answers consistent with it.

## Retrieve evidence

For every material claim, read supporting text during the current session. Do not rely on remembered saguin behavior.

Start by counting matches across the corpus, then locate relevant sections and read each section in context. Prefer `rg`; for example:

```sh
rg -n -i 'retained' README.md docs/invariants.md docs/rfcs examples | wc -l
rg -n '^#' docs/rfcs/0003-delivery-semantics.md
sed -n '436,520p' docs/rfcs/0003-delivery-semantics.md
```

Do not read an entire large RFC when targeted retrieval will answer the question. Do not conclude from truncated output such as `head`. When checking whether something is mentioned everywhere, use complete counts and results.

The invariant numbers are embedded in bold rule headings rather than phrases such as "invariant 4". Search for words from the rule or read the relevant heading section when a number-based search is empty.

Read whole relevant sections, including adjacent constraints and failure cases. If an answer depends on channel type, storage provider, clean versus unclean shutdown, worker count, or another deployment fact, ask for that fact when necessary or present the genuine cases briefly.

## Cite answers

Name the document and human-readable section alongside each claim, such as *RFC 0003, "Retained messages"*, *the invariants, rule 14*, or *the comments in `examples/saguin.yaml`*.

Use one pointer per claim, or two when two documents genuinely support it. Prefer document names and section titles over line-number citations. Do not overwhelm a short answer with a longer citation list.

## When documentation is silent

Say exactly: **"The documentation does not say."** Then state the terms and corpus locations searched and identify the nearest documented behavior, if useful. Do not fill the gap using another broker, the MQTT specification, or an inferred sensible default.

Distinguish absence from an explicit non-goal. If an RFC says saguin deliberately does not support something, answer that it does not and cite the relevant section.

## Draft configurations

Start from `examples/saguin.yaml` and the schema in RFC 0002. Verify every proposed key against the configuration fields loaded by the code:

```sh
rg -o 'yaml:"[a-z0-9_]+' internal/config/*.go | rg -o '"[a-z0-9_]+' | tr -d '"' | sort -u
```

Write drafts only in a temporary directory. Validate a draft before returning it:

```sh
./bin/saguin --check-config /tmp/path/to/draft.yaml
```

If referenced certificates, password files, or similar inputs are intentionally unavailable, clearly say validation could not establish those external files. Never invent a configuration key. When a behavior is fixed rather than configurable, cite the fixed behavior instead.

## Runtime probes

When the question is about observed behavior rather than only written behavior, offer to run the broker, name the command, and wait for approval. Building into `/bin/` is acceptable because it is ignored; keep other probe artifacts in a temporary directory.

For approved probes:

- Prefer Eclipse Paho (`examples/demo.py`) or an in-process Go client when client behavior could mask the broker. Command-line MQTT tools may normalize packets before sending them; for example, they can clear a retain flag after CONNACK says retained messages are unavailable.
- Separate sending from collection so a helper does not consume messages while waiting for an acknowledgement.
- Report the broker's actual MQTT reason code rather than only a client library's label.
- Read RFC 0005 before testing `/metrics`; requests inside `min_scrape_interval` may return the previous catalogue.
- `make demo` is the quickest general demonstration using `examples/saguin.yaml`.

Report only the behavior relevant to the question and the evidence needed to establish it.

## Response shape

Lead with the answer in one or two sentences. Add only deployment-dependent cases or settings that could change it. End with compact document-and-section citations. If a probe was run, briefly state what it observed.

Do not include commit IDs, repository history, irrelevant reason codes, test coverage, or an explanation of the documentation process. If the documentation disagrees with runtime behavior, state both plainly and suggest reporting the mismatch to the maintainers; do not edit anything.

## Regression questions

When this skill or the documentation changes substantially, use `docs/saguin-agent/questions.yaml` as a manual behavioral check. Answer each question before reading its expected citation, then compare. Do not edit that file. Classify disagreements as ambiguous documentation, broken retrieval, or an answer drawn from memory.
