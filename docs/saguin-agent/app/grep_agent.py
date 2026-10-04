"""The loop: search the files, read a section, then answer with references.

saguin's documentation is a corpus of small files under half a megabyte, so
this searches them the way a person at a terminal would rather than through a
vector database. Each turn the model chooses one tool:

- `grep` - exact-term search. Finds a rule whose words the question shares,
  which is where dense embedding alone kept failing ("can two workers get the
  same job" is invariant 4, whose words the question does not use).
- `semantic_search` - a section that *means* what the question means when they
  share no words ("turn verification off on a bridge" and "there is no way to
  turn verification off"). It carries a score, which is the refusal signal grep
  lacks: nothing scoring near the question means the documents do not cover it.
- `read` - open the section a hit points to and answer from it.

The model is handed a compact table of contents - every document's major
headings, built from the text, not summarised - while exact and semantic search
surface the narrower subsections relevant to the question.

**What it refuses to do is the feature.** Grounded in the documents it read or
it is not given: a claim about saguin borrowed from another broker is the
exact failure this tool exists to prevent.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from . import tools
from .config import Settings
from .conversation import (
    contextual_query,
    needs_context,
    normalize_context,
    question_prompt,
)
from .llm import ask
from .semantic import SemanticIndex


@dataclass
class Answer:
    markdown: str
    references: list[dict] = field(default_factory=list)
    refused: bool = False
    turns: int = 0
    trace: list[str] = field(default_factory=list)
    """What the loop did, so a reader can see why an answer looks as it does."""


# How much of a hit or a read is shown back to the model, and how many
# references an answer carries. The read cap keeps a turn cheap on a slow
# machine; the reference cap keeps the list under an answer to the handful a
# reader would open, with sections read in full ahead of ones a search merely
# turned up.
READ_CHARS = 1400
MAX_REFERENCES = 8

# References, ordered by how much of the section stands behind them: a section
# read in full first, then one a semantic hit surfaced whole, then a line grep
# turned up. Lower sorts earlier.
_SOURCE_ORDER = {"read": 0, "semantic": 1, "grep": 2}


def _parse(reply: str) -> dict:
    """The model's JSON, however it wrapped it.

    Small models fence their JSON, prefix it with a sentence, or answer in
    prose. None of that is worth failing over: prose with no action is an
    answer, which is what the caller treats it as.

    **Decoded rather than sliced-and-`json.loads`ed.** A model that tacks one
    stray `}` onto an otherwise well-formed object used to fail the whole parse
    - `json.loads` demands the entire span be valid, and a regex slice from the
    first `{` to the last `}` an extra brace makes one character too long. The
    result was not an empty dict but the raw wrapper shown to the reader as the
    answer. `raw_decode` reads one complete value from where it starts and
    stops there, so anything trailing it is ignored rather than poisoning the
    parse.
    """
    text = reply.strip()
    fence = re.search(r"```(?:json)?\s*(\{.*)\s*```", text, re.S)
    if fence:
        text = fence.group(1)
    start = text.find("{")
    if start == -1:
        return {}
    try:
        parsed, _ = json.JSONDecoder().raw_decode(text, start)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


_RETRY = ("That was not one valid JSON object. Reply with exactly one: a search "
          "or read action, or {\"action\": \"answer\"} when you have read enough.")

# How much of each gathered section is handed to the synthesis step, and how
# many sections. Enough to answer from, bounded so a slow model is not made to
# read the whole corpus back.
EVIDENCE_CHARS = 1600
MAX_EVIDENCE = 8

# **The answer is written in its own call, not in the loop.** The loop's job is
# to gather - search, read, decide it has enough. The final answer is then
# generated from the question, bounded conversational context, and gathered
# excerpts, with none of the loop's tool-result turns in front of it. That is
# what stops the model
# narrating the machinery ("thank you for the section", "I'll read…", the
# read's own header copied back): in this call there is no loop to narrate,
# only questions and excerpts, so an opener referring to retrieval has nothing to
# refer to. It is a class fixed by structure rather than a list of phrases to
# strip, which the last three leaks each slipped past.
_ANSWER_SYSTEM = """You are given a question about saguin, an MQTT 5 broker, \
and excerpts from its own documentation. Write the answer in Markdown, using \
only the excerpts.

- Begin with the answer itself. Do not open with a greeting, a thank-you, a \
mention of the excerpts, sections, or a search, or a description of what you \
are about to do - the reader saw none of that, so a sentence about it is noise.
- Use only the excerpts. Never use general knowledge about MQTT or other \
brokers: they differ from saguin in ways that matter.
- Assemble the answer across excerpts where the question needs it: if the \
pieces are documented - a socket listener here, a proxy that forwards the \
certificate name there - show how they fit rather than refusing because no \
single excerpt spells out the whole scenario word for word.
- Keep the distinction the documents draw between what is built and what is \
only specified.
- Do not quote an excerpt's citation header or line numbers; write the answer \
in your own words with fenced examples where they help.
- Do not write a references section; it is added for you.
- Only when the excerpts genuinely do not cover the topic, reply with exactly: \
The documentation does not say. - and nothing else."""


def _looks_like_action(reply: str) -> bool:
    """Whether a reply is a JSON tool call rather than a decision to answer.

    A search or read - valid, or, as happens, missing its closing brace so it
    does not parse - is a command the loop should run or ask to be re-sent, not
    a signal that the model is done gathering.
    """
    text = reply.strip()
    if text.startswith("```"):
        text = text.lstrip("`")
        if text[:4].lower() == "json":
            text = text[4:]
        text = text.strip()
    return text.startswith("{") and '"action"' in text


def _evidence(seen: dict[str, dict]) -> str:
    """The gathered sections, best-first, as the material the answer is written from."""
    ordered = sorted(seen.values(), key=lambda r: _SOURCE_ORDER.get(r["source"], 9))
    parts = []
    for r in ordered[:MAX_EVIDENCE]:
        text = r["text"]
        if len(text) > EVIDENCE_CHARS:
            text = text[:EVIDENCE_CHARS] + " […]"
        parts.append(f"[{r['citation']}]\n{text}")
    return "\n\n---\n\n".join(parts)


def _synthesize(
    cfg: Settings,
    question: str,
    seen: dict[str, dict],
    context: dict[str, list[str]] | None = None,
) -> str:
    """Write the final answer from the question, its context, and gathered excerpts."""
    messages = [
        {"role": "system", "content": _ANSWER_SYSTEM},
        {
            "role": "user",
            "content": f"{question_prompt(question, context or {})}\n\nExcerpts:\n\n{_evidence(seen)}",
        },
    ]
    return ask(cfg, messages).strip()


def _system(min_score: float) -> str:
    """The instructions for the loop that gathers the excerpts.

    This model does not write the answer - it decides what to search and read,
    and says when it has enough. The answer is written afterwards from what it
    gathered, by `_synthesize`, which is why nothing here is about phrasing or
    formatting the reply.
    """
    return (
        "You gather the documentation needed to answer a question about saguin, "
        "an MQTT 5 broker. You do not write the answer - that is done for you "
        "from what you gather. Your job is to find and read the right sections.\n\n"
        "The current question is at the top, optionally preceded by earlier user "
        "questions that only resolve follow-up wording. Every message after it "
        "is search results and document text returned by your own previous tool calls, "
        "not a person speaking.\n\n"
        "Each turn you reply with exactly one JSON object and nothing else:\n\n"
        '  {"action": "grep", "pattern": "..."}                  '
        "search the documents (a regular expression, case-insensitive)\n"
        '  {"action": "semantic_search", "query": "..."}          '
        "find sections that mean the same thing, with a score\n"
        '  {"action": "read", "path": "...", "start": N, "end": M}  '
        "read lines N..M of a document (omit start/end to read all of it)\n"
        '  {"action": "answer"}                                  '
        "you have read enough; the answer is written from what you gathered\n\n"
        "How to work:\n"
        "- You are given the list of documents with their sections, and the "
        "results of a first search already. Read them.\n"
        "- A result names a document, a line number and the section it falls "
        "under. Open the section it points to: {\"action\": \"read\", "
        "\"path\": \"docs/invariants.md\", \"start\": 54, \"end\": 62}. Read "
        "around the line a hit sits on, not only the matching line.\n"
        "- If the first search did not find it, search again with the words the "
        "documents themselves use rather than the words the question used. "
        "saguin's documents say \"in flight\", \"floor\", \"record\", "
        "\"offset\", \"channel\", \"invariant\", \"consumer\" - a question may "
        "say \"unacknowledged\", \"oldest\", \"message\", \"worker\", \"job\".\n"
        "- semantic_search returns a score from 0 to 1: a top score below about "
        f"{min_score} means nothing in the documents is closely related.\n"
        "- When the sections you have read contain what the question asks, reply "
        "{\"action\": \"answer\"}. If after searching nothing in the documents "
        "answers it, reply {\"action\": \"answer\"} anyway - the answer step "
        "will say the documentation does not say. Never keep searching for "
        "something that is not there."
    )


def run(
    cfg: Settings,
    question: str,
    semantic: SemanticIndex,
    context: dict[str, list[str]] | None = None,
):
    """The loop as a stream of events: a step each time it acts, then the answer.

    Yields `{"type": "step", "text": ...}` as it searches and reads - the
    feedback a reader watches while the model works, because a loop that shows
    nothing until it finishes reads as a hang on slow hardware - and finally
    `{"type": "answer", "answer": Answer}`. `answer()` below drains this for the
    callers that only want the end: the question set and the non-streaming
    endpoint.
    """
    root = Path(cfg.docs)
    context = (
        normalize_context(context) if needs_context(question) else normalize_context(None)
    )
    messages = [
        {"role": "system", "content": _system(cfg.min_score)},
        {"role": "user", "content": question_prompt(question, context)},
        {"role": "user", "content": _map(root)},
    ]

    # A citation -> reference. A read adds the section in full; a semantic hit
    # adds the section it surfaced; a grep hit adds the line it found. Fuller
    # sources win when several touch the same section - `_SOURCE_ORDER` decides
    # both that and the order under the answer.
    seen: dict[str, dict] = {}
    reads: set[tuple] = set()  # exact read requests already served, to stop re-reads
    trace: list[str] = []

    def acted(text: str) -> dict:
        """Record a step and hand it to the stream in one move."""
        trace.append(text)
        return {"type": "step", "text": text}

    # **The first search is free**: asking the model to decide it should search
    # for the question it was just handed costs a whole round trip and arrives
    # at the obvious query. So the loop opens with the question's own words
    # already grepped and already embedded.
    yield acted(f"searched the documentation for “{question}”")
    pattern = tools.keywords(question, root)
    if pattern:
        grep_result = tools.grep(root, pattern)
        _remember_grep(seen, grep_result)
        messages.append({"role": "user", "content": _grep_text(grep_result)})
    hits = semantic.search(question, cfg.excerpts)
    _remember_semantic(seen, hits)
    messages.append({"role": "user", "content": _semantic_text(hits, cfg.min_score)})
    if context["questions"] or context["citations"]:
        yield acted("searched using the preceding exchange as context")
        context_hits = semantic.search(contextual_query(question, context), cfg.excerpts)
        _remember_semantic(seen, context_hits)
        messages.append({
            "role": "user",
            "content": "Sections by meaning with the preceding exchange as context:\n\n"
            + _semantic_text(context_hits, cfg.min_score),
        })

    turn = 0
    gathered = False  # did the model signal it has gathered enough?
    for turn in range(1, cfg.max_turns + 1):
        reply = ask(cfg, messages)
        step = _parse(reply)
        messages.append({"role": "assistant", "content": reply})

        action = step.get("action")

        if action == "grep":
            pat = step.get("pattern") or pattern
            yield acted(f"grepped the documentation for “{pat}”")
            result = tools.grep(root, pat)
            _remember_grep(seen, result)
            messages.append({"role": "user", "content": _grep_text(result)})
            continue

        if action == "semantic_search":
            query = step.get("query") or question
            yield acted(f"searched by meaning for “{query}”")
            hits = semantic.search(query, cfg.excerpts)
            _remember_semantic(seen, hits)
            messages.append({"role": "user", "content": _semantic_text(hits, cfg.min_score)})
            continue

        if action == "read":
            path = step.get("path") or ""
            # **Do not serve the same read twice.** Left to itself the model
            # re-reads a section it already has, turn after turn, until it hits
            # the turn cap - five identical reads of one section, measured. A
            # repeat is answered with a nudge instead of the text again, which
            # breaks the loop and spends the turns on ground not yet covered.
            sig = (path, step.get("start"), step.get("end"))
            if sig in reads:
                yield acted("asked to move on from an already-read section")
                messages.append({"role": "user", "content":
                    f"You already read that. Read a different section, or reply "
                    f"{{\"action\": \"answer\"}} if you have enough."})
                continue

            result = tools.read(root, path, step.get("start"), step.get("end"))
            if "error" in result:
                yield acted("tried to read a document outside the corpus")
                messages.append({"role": "user", "content": result["error"]})
                continue
            reads.add(sig)
            yield acted(f"read {result['citation']}")
            seen[result["citation"]] = {
                "citation": result["citation"],
                "document": result["document"],
                "text": result["text"],
                "source": "read",
            }
            messages.append({"role": "user", "content": _read_text(result)})
            continue

        # Anything else is the model saying it has gathered enough - an
        # {"action": "answer"}, or plain prose. Stop the loop and write the
        # answer separately. The one exception is a mangled tool call - JSON
        # that was trying to be a command but did not parse - which gets
        # another turn rather than being mistaken for "done".
        if not step and _looks_like_action(reply):
            if turn < cfg.max_turns:
                yield acted("the reply was not usable; asked it to try again")
                messages.append({"role": "user", "content": _RETRY})
                continue
        gathered = True
        break

    if not gathered:
        yield acted(f"stopped after {cfg.max_turns} turns")

    # **An answer with no evidence is a refusal wearing an answer's clothes.**
    # If the loop never found or read anything, there is nothing to write from.
    if not seen:
        yield {"type": "answer", "answer": Answer(
            markdown=(
                "**The documentation does not say.**\n\n"
                "Nothing in saguin's README, invariants, RFCs or the other "
                "documents is close enough to this question to answer it."
            ),
            refused=True, turns=turn, trace=trace,
        )}
        return

    # The answer, written in its own call from the excerpts alone - see
    # `_synthesize`. This is the step that costs the extra round trip; it is
    # also the one that keeps the loop's narration out of what the reader sees.
    yield acted("writing the answer")
    markdown = _synthesize(cfg, question, seen, context)
    refused = "the documentation does not say" in markdown.lower()
    yield {"type": "answer", "answer": Answer(
        markdown=markdown, references=_references(seen), refused=refused,
        turns=turn, trace=trace,
    )}


def answer(
    cfg: Settings,
    question: str,
    semantic: SemanticIndex,
    context: dict[str, list[str]] | None = None,
) -> Answer:
    """One question, drained to its final answer - for callers that only want the end."""
    result = Answer(markdown="**The documentation does not say.**", refused=True)
    for event in run(cfg, question, semantic, context):
        if event["type"] == "answer":
            result = event["answer"]
    return result


def _remember_grep(seen: dict[str, dict], result: dict) -> None:
    """Add a grep's hits as references, without displacing a fuller source.

    A search touches many lines in a few sections; deduplicating by citation
    collapses six `allow_anonymous` hits in one section to one reference.
    `setdefault` never overwrites a read or a semantic hit with a one-line
    match.
    """
    for hit in result.get("matches", []):
        seen.setdefault(hit["citation"], {
            "citation": hit["citation"],
            "document": hit["document"],
            "text": hit["text"],
            "source": "grep",
        })


def _remember_semantic(seen: dict[str, dict], hits: list) -> None:
    """Add semantic hits as references, carrying their score, never over a read."""
    for hit in hits:
        cite = hit.payload["citation"]
        previous = seen.get(cite, {})
        if previous.get("source") == "read":
            continue
        if previous.get("source") == "semantic" and previous.get("score", -1) >= hit.score:
            continue
        seen[cite] = {
            "citation": cite,
            "document": hit.payload["document"],
            "text": hit.payload["text"],
            "score": round(hit.score, 3),
            "source": "semantic",
        }


def _references(seen: dict[str, dict]) -> list[dict]:
    """The references under the answer: read first, then semantic, then grep."""
    ordered = sorted(seen.values(), key=lambda r: _SOURCE_ORDER.get(r["source"], 9))
    return [{k: v for k, v in r.items() if k != "source"} for r in ordered[:MAX_REFERENCES]]


def _map(root: Path) -> str:
    """The corpus as a compact table of contents: documents and major sections.

    A source-derived project index: the documents' own H1/H2 headings and
    their line numbers let the model navigate without
    repeating the roughly 10 KB full heading tree on every model call. Search
    results retain the exact deeper section and line. This map is deterministic,
    so it never drifts from the text the way an AI-written summary would.
    """
    parts = []
    for doc in tools.headings(root):
        lines = [f"{doc['document']} ({doc['lines']} lines)"]
        for h in doc["headings"]:
            if h["depth"] > 2:
                continue
            indent = "  " * h["depth"]
            lines.append(f"{indent}{h['line']:>4}  {h['title']}")
        parts.append("\n".join(lines))
    return ("The documents you can search and read, with their major sections and the "
            "line each begins at:\n\n" + "\n\n".join(parts))


def _grep_text(result: dict) -> str:
    if "error" in result:
        return result["error"]
    matches = result.get("matches", [])
    if not matches:
        return ("Nothing matched that pattern. Try different words - the ones the "
                "documents use - or answer that the documentation does not say.")
    parts = [f"{m['document']}:{m['line']}  [{m['citation']}]\n    {m['text']}" for m in matches]
    tail = "\n\n(truncated; narrow the pattern)" if result.get("truncated") else ""
    return "Search results:\n\n" + "\n".join(parts) + tail


def _semantic_text(hits: list, min_score: float) -> str:
    if not hits:
        return "Nothing came back. Answer that the documentation does not say."
    parts = []
    for h in hits:
        text = h.payload["text"]
        if len(text) > READ_CHARS:
            text = text[:READ_CHARS] + "\n\n[…read the section for the rest]"
        parts.append(f"[{h.score:.3f}] {h.payload['citation']}\n\n{text}")
    top = hits[0].score
    note = ""
    if top < min_score:
        note = (f"\n\nThe best score is {top:.3f}, below {min_score}: nothing here is "
                "closely related. Unless a grep found the exact statement, the answer "
                "is that the documentation does not say.")
    return "Sections by meaning (score 0..1):\n\n" + "\n\n---\n\n".join(parts) + note


def _read_text(result: dict) -> str:
    text = result["text"]
    if len(text) > READ_CHARS:
        text = text[:READ_CHARS] + "\n\n[…this section continues; read a later line range for the rest]"
    span = f"{result['document']} lines {result['start']}–{result['end']}  [{result['citation']}]"
    return f"{span}\n\n{text}"
