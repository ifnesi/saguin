"""The documents saguin ships, searched the way a person at a terminal would.

This is the grep-mode retrieval: no vectors, no chunking, no store to build.
The corpus is a set of small files totalling under half a megabyte, and
its vocabulary is precise - a numbered rule, a heading, a configuration key.
Exact-term search over the files answers the questions dense embedding kept
missing ("can two workers get the same job" is invariant 4, whose words the
question does not share) and reads whole sections rather than fragments, so a
rule and the failure it prevents are never handed over in halves.

**Every result carries a heading-path citation**, in the same shape the
vector path emits - `docs/invariants.md › Queue correctness › invariant 4` -
so an answer's references mean the same thing however it was retrieved, and
the question set scores both paths against one another fairly.

**A path from the model is checked against the corpus list, never joined to
the docs root.** The checkout is mounted read-only, but that mount is the
whole repository, and this agent's own `.env` with its cloud credentials
lives inside it. `read` resolves a relative path only if it is one the corpus
already contains; anything else comes back as an error naming the valid ones,
never as a file.
"""

from __future__ import annotations

import re
from pathlib import Path

from .corpus import NUMBERED, documents, _plain

# A grep returns at most this many lines, and a read at most this many
# characters. Not a statement about meaning - a bound on what one tool call
# can cost the model's context, so a pattern matching half the corpus cannot
# crowd out the turn that reads the answer. Both name the truncation when they
# hit it, so the model knows to narrow the pattern or ask for a line range
# rather than believing it saw everything.
MAX_MATCHES = 90
MAX_READ_CHARS = 20000

# **The budget is spread across the files, not spent on the first one.** A
# common word - "queue", "default", "two" - matches the README and RFC 0002
# scores of times before the walk reaches the invariants, and a single global
# cap fills entirely from those two and answers "nothing in the invariants",
# which is where the answer to "can two workers get the same job" actually is.
# Capping each file's share keeps the smaller documents visible however loud
# the large ones are, which is the whole point of searching all of them.
MAX_PER_FILE = 12

_HEADING = re.compile(r"^(#{1,6})\s+(.*\S)\s*$")

# A YAML document's only stable structure: its top-level keys.
_TOPKEY = re.compile(r"^([A-Za-z_][\w.-]*):")

# Words worth searching for, when the loop seeds itself from the question. The
# stop list is small on purpose: it removes the words that match everything
# ("what", "does", "the") and keeps everything that carries the question, so
# the free first grep lands on the same terms a person would have typed.
_STOP = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "do", "does", "did", "how", "what", "when", "where", "which", "who",
    "why", "can", "could", "should", "would", "will", "to", "of", "in",
    "on", "for", "and", "or", "if", "it", "its", "with", "that", "this",
    "i", "my", "get", "got", "so", "only", "own",
}


def _outline(rel: str, text: str) -> list[list[str]]:
    """The heading path in force at every line of a document.

    Walks the file once, tracking fenced blocks so a `#` inside a YAML or
    shell example is not mistaken for a heading - the same care the chunker
    takes, for the same reason: these documents are full of configuration,
    and a comment line is not a section.

    For the invariants file a numbered rule adds its own step to the path, so
    a line inside rule 4 cites `… › Queue correctness › invariant 4`, which
    is the unit a reader means by "why is that refused". A new heading resets
    the rule, because the next section's text belongs to no rule at all.

    A YAML document has no headings at all - every `#` in it is a comment,
    and treating one as a heading cited a hit by whatever wrapped comment
    fragment came last ("examples/saguin.yaml › one says so."). Its outline
    is the top-level key in force, the same cut the chunker makes.
    """
    lines = text.split("\n")
    if rel.endswith((".yaml", ".yml")):
        key: list[str] = []
        out_yaml: list[list[str]] = []
        for line in lines:
            m = _TOPKEY.match(line)
            if m:
                key = [m.group(1)]
            out_yaml.append(list(key))
        return out_yaml
    numbered = NUMBERED.get(rel)
    stack: list[tuple[int, str]] = []
    inv: str | None = None
    fenced = False
    out: list[list[str]] = []

    for line in lines:
        if line.lstrip().startswith("```"):
            fenced = not fenced
            out.append(_compose(stack, inv))
            continue

        if not fenced:
            heading = _HEADING.match(line)
            if heading:
                level, title = len(heading.group(1)), _plain(heading.group(2))
                while stack and stack[-1][0] >= level:
                    stack.pop()
                stack.append((level, title))
                inv = None
                out.append(_compose(stack, inv))
                continue
            if numbered is not None:
                rule = numbered.match(line)
                if rule:
                    inv = rule.group(1)

        out.append(_compose(stack, inv))

    return out


def _compose(stack: list[tuple[int, str]], inv: str | None) -> list[str]:
    path = [title for _, title in stack]
    return path + [f"invariant {inv}"] if inv is not None else path


def _citation(rel: str, heading_path: list[str]) -> str:
    where = " › ".join(heading_path)
    return f"{rel} › {where}" if where else rel


def headings(root: Path) -> list[dict]:
    """Every document's section headings, with the line each begins at.

    The honest table of contents - built from the documents' own `#` headings,
    not summarised by a model, so it cannot drift from the text or invent a
    section that is not there. Given to the loop up front, it is the map a
    reader skims before searching: the model sees `On a bridge, dialling out`
    and `Who may connect` by name and reads straight to them, and sees that no
    section promises a thing the corpus does not cover. Fence-aware, so a `#`
    inside a YAML example is not listed as a section - and a YAML document
    lists its top-level keys, because every `#` in it is a comment.
    """
    out: list[dict] = []
    for path in documents(root):
        rel = str(path.relative_to(root))
        text = path.read_text(encoding="utf-8")
        lines = text.split("\n")
        rows: list[dict] = []
        if rel.endswith((".yaml", ".yml")):
            for i, line in enumerate(lines):
                m = _TOPKEY.match(line)
                if m:
                    rows.append({"line": i + 1, "depth": 1, "title": m.group(1)})
            out.append({"document": rel, "lines": len(lines), "headings": rows})
            continue
        fenced = False
        for i, line in enumerate(lines):
            if line.lstrip().startswith("```"):
                fenced = not fenced
                continue
            if fenced:
                continue
            heading = _HEADING.match(line)
            if heading:
                rows.append({"line": i + 1, "depth": len(heading.group(1)),
                             "title": _plain(heading.group(2))})
        out.append({"document": rel, "lines": len(lines), "headings": rows})
    return out


def grep(root: Path, pattern: str, max_matches: int = MAX_MATCHES) -> dict:
    """Every line matching a regular expression, with where it sits.

    Line-oriented and case-insensitive: the documents are written in lines,
    and a reader searching them does not care about case. Each match names its
    document, line number and the heading path it falls under, so the model
    can decide what to open next without reading anything yet.
    """
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        return {"error": f"that is not a valid regular expression: {exc}"}

    matches: list[dict] = []
    truncated = False
    for path in documents(root):
        rel = str(path.relative_to(root))
        text = path.read_text(encoding="utf-8")
        lines = text.split("\n")
        heads = _outline(rel, text)
        in_file = 0
        for i, line in enumerate(lines):
            if rx.search(line):
                if in_file >= MAX_PER_FILE:
                    truncated = True
                    break
                heading_path = heads[i]
                matches.append(
                    {
                        "document": rel,
                        "line": i + 1,
                        "citation": _citation(rel, heading_path),
                        "heading_path": heading_path,
                        "text": line.strip(),
                    }
                )
                in_file += 1
                if len(matches) >= max_matches:
                    truncated = True
                    break
        if len(matches) >= max_matches:
            break

    return {"matches": matches, "truncated": truncated}


def _known(root: Path) -> dict[str, Path]:
    return {str(p.relative_to(root)): p for p in documents(root)}


def read(
    root: Path,
    path: str,
    start: int | None = None,
    end: int | None = None,
    max_chars: int = MAX_READ_CHARS,
) -> dict:
    """A span of one document - or the whole of it - with a citation.

    `path` is resolved only against the corpus list. A path that is not one
    of the indexed documents returns an error naming the valid ones, never a
    file: the read-only mount is the whole repository, `.env` included, and a
    read that trusted the path it was handed would be a way out of the corpus.

    Line numbers are 1-based and inclusive, the way `grep` reports them, so
    the model can read straight around a match it just found. The citation is
    the heading path at the first line read, which for a rule in the
    invariants file is that rule's number.
    """
    known = _known(root)
    if path not in known:
        return {
            "error": (
                f"{path!r} is not part of the corpus. Read one of: "
                + ", ".join(sorted(known))
            )
        }

    text = known[path].read_text(encoding="utf-8")
    lines = text.split("\n")
    heads = _outline(path, text)

    if start is None:
        s, e = 1, len(lines)
    else:
        s = max(1, int(start))
        e = min(len(lines), int(end)) if end is not None else len(lines)
        if e < s:
            e = s

    body = "\n".join(lines[s - 1 : e])
    truncated = False
    if len(body) > max_chars:
        body = body[:max_chars]
        truncated = True

    heading_path = heads[s - 1] if heads else []
    return {
        "document": path,
        "start": s,
        "end": e,
        "citation": _citation(path, heading_path),
        "heading_path": heading_path,
        "text": body,
        "truncated": truncated,
    }


def keywords(question: str, root: Path | None = None, max_terms: int = 2) -> str:
    """A regular expression over the question's own salient words.

    Used to seed the loop with a free first grep, so the model opens with the
    same hits a person typing the question's nouns into a search box would get
    - the grep-mode counterpart of the vector path's free first search. Words
    shorter than three characters and the ones that match everything are
    dropped. When the corpus is available, only its least frequent matching
    terms are kept: broad words such as "MQTT" and "client" used to spend a
    file's result budget before its useful section was reached. Semantic
    search still carries the full question's meaning.

    `_` and `-` count as boundaries, so "connection rate" finds
    `max_connect_rate` and "origins" finds `allowed_origins`.
    """
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", question.lower())
    terms = [w for w in dict.fromkeys(words) if w not in _STOP]
    if not terms:
        return ""

    def term_pattern(term: str) -> str:
        return r"(?<![A-Za-z0-9])" + re.escape(term) + r"(?![A-Za-z0-9])"

    if root is not None and max_terms > 0:
        corpus = "\n".join(path.read_text(encoding="utf-8") for path in documents(root))
        ranked = []
        for position, term in enumerate(terms):
            count = len(re.findall(term_pattern(term), corpus, re.IGNORECASE))
            if count:
                ranked.append((count, position, term))
        terms = [term for _, _, term in sorted(ranked)[:max_terms]]
        if not terms:
            return ""

    return "(" + "|".join(term_pattern(t) for t in terms) + ")"
