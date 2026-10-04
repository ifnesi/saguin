"""The documents saguin ships, cut into the pieces an answer can cite.

**A chunk is a heading's worth of text, not a fixed number of characters.**
These documents state a rule and then the failure it prevents, and the
second half is what makes the first comprehensible - an invariant severed
from its failure reads as an arbitrary restriction, and a configuration key
severed from its reasoning reads as a suggestion. Fixed-size chunking cuts
exactly there, and it cuts YAML blocks in half, which produces configuration
answers that do not load.

So the unit is the heading, the heading path travels with the text, and a
fenced block is never split.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# The corpus, in the order a reader would meet it. Every one of these is a
# document saguin ships; nothing here is generated, and nothing outside this
# list is indexed - an answer's authority comes from the reader being able
# to open the file it cites.
CORPUS = [
    "README.md",
    "docs/invariants.md",
    "docs/rfcs/*.md",
    "examples/saguin.yaml",
    "SECURITY.md",
    "CONTRIBUTING.md",
    "CHANGELOG.md",
    "LICENSE",
]

# Documents that are one chunk whatever their size.
#
# LICENSE is 201 lines of Apache 2.0 boilerplate. Split by heading it would
# answer half the questions asked of it with "Definitions" or
# "Redistribution" while crowding out better matches; whole, it answers the
# only question anybody actually asks of it. The test for this list: would a
# reader be helped by landing in the middle of this document? For an RFC
# section, yes. For a licence, never.
WHOLE = {"LICENSE"}

# What a whole document is embedded *as*.
#
# LICENSE is one chunk of 11,310 characters, and one vector over that much
# boilerplate is diluted to the point of matching nothing: "what licence is
# saguin under?" returned CONTRIBUTING, SECURITY and an invariant, with the
# licence itself nowhere in the top three. So the text stored and shown is
# the whole document, and the text *embedded* is a card that says what the
# document is - which is the question anybody actually asks of it.
CARDS = {
    "LICENSE": (
        "LICENSE - the licence saguin is released under. Apache License, "
        "Version 2.0: what you may do with saguin, redistribution, "
        "contribution terms, patent grant, trademarks, and the warranty "
        "disclaimer. Open source, permissive."
    )
}

# Documents whose sections are cut again into numbered rules.
#
# The invariants are fifteen numbered rules, each written as a statement and
# then the failure it prevents, and they are the unit a reader means: "why
# is that refused" is answered by invariant 4, not by the section it sits
# in. Chunked by heading alone, four rules share a chunk, one of them gets
# split at a paragraph boundary, and the citation reads "Queue correctness
# (1/2)" where it could read "invariant 4".
#
# **A numbered rule is never split**, whatever its length: an invariant
# severed from the failure it prevents is the one cut these documents cannot
# survive, and two of the fifteen are longer than the bound below.
NUMBERED = {"docs/invariants.md": re.compile(r"^\*\*(\d+)\.\s")}

# A chunk longer than this is split on paragraph boundaries, keeping its
# heading path. Not a hard rule about meaning - a bound on what one
# retrieval hit can cost, so that a long section cannot crowd out three
# short ones that between them answer the question better.
MAX_CHARS = 2400


@dataclass
class Chunk:
    """One retrievable piece, and everything needed to cite it."""

    document: str
    """Repository-relative path, as a reader would open it."""

    heading_path: list[str] = field(default_factory=list)
    """Headings from the document title down to this section."""

    text: str = ""
    part: int = 1
    parts: int = 1

    card: str = ""
    """Embedded in place of the text, for a document too long to embed whole."""

    @property
    def embedded(self) -> str:
        """What is embedded, which is not the same as what is shown.

        **The heading path goes in front of the text.** Without it a chunk
        about restarts and deliveries is only its own prose, and a question
        naming "invariant 15" or "TLS on a listener" has nothing to match
        against - the words a reader uses to ask are frequently the words in
        the heading rather than in the body.

        Measured on six trap questions: with
        bare text, "what happens to unacknowledged queue work when the
        broker restarts" did not return invariant 15 at all.
        """
        where = " › ".join(self.heading_path)
        lead = f"{self.document} › {where}" if where else self.document
        return f"{lead}\n\n{self.card or self.text}"

    @property
    def citation(self) -> str:
        """What goes under an answer.

        The path rather than the heading alone: `RFC 0002 › TLS on a
        listener › Client certificates` tells a reader where to look, where
        "Client certificates" alone tells them nothing about which document.
        """
        where = " › ".join(self.heading_path) if self.heading_path else ""
        cite = f"{self.document}" + (f" › {where}" if where else "")
        return cite + (f" ({self.part}/{self.parts})" if self.parts > 1 else "")


def documents(root: Path) -> list[Path]:
    """Every file in the corpus, resolved against a checkout."""
    found: list[Path] = []
    for pattern in CORPUS:
        if "*" in pattern:
            found.extend(sorted(root.glob(pattern)))
        else:
            path = root / pattern
            if path.exists():
                found.append(path)
    return found


def chunk_document(path: Path, root: Path) -> list[Chunk]:
    """Cut one document into chunks."""
    rel = str(path.relative_to(root))
    text = path.read_text(encoding="utf-8")

    if rel in WHOLE:
        return [Chunk(document=rel, heading_path=[], text=text.strip(), card=CARDS.get(rel, ""))]

    if rel.endswith((".yaml", ".yml")):
        return _split_yaml(rel, text)

    # A numbered document is not bounded during the section pass: the bound
    # would cut a section between two rules - or through one - before the
    # numbering has a chance to cut it in the right place. Invariant 7 was
    # severed exactly there, 418 characters of rule and 1,077 orphaned in a
    # chunk carrying no number at all.
    numbered = NUMBERED.get(rel)
    chunks = _split_sections(rel, text, bound=numbered is None)
    if numbered is not None:
        chunks = _by_number(rel, chunks, numbered)
    return chunks


def _split_yaml(rel: str, text: str) -> list[Chunk]:
    """Cut a YAML example at its top-level keys.

    A `#` in YAML is a comment rather than a heading, and the comments are
    most of the document - cut at them, as the markdown walk would, and
    every wrapped comment block keeps its last line as a heading and loses
    the rest of itself entirely. The stable structure is the top-level
    keys, of which a configuration has a handful; the bound then cuts the
    long ones at blank lines exactly as it does a long section of prose,
    which keeps a comment block glued to the lines it explains.
    """
    lines = text.split("\n")
    starts = [i for i, line in enumerate(lines) if re.match(r"^[A-Za-z_][\w.-]*:", line)]
    if not starts:
        return _bounded(rel, [], text.strip())

    chunks: list[Chunk] = []
    if starts[0] > 0:
        head = "\n".join(lines[: starts[0]]).strip()
        if head:
            chunks.extend(_bounded(rel, [], head))
    bounds = starts + [len(lines)]
    for i, start in enumerate(starts):
        body = "\n".join(lines[start : bounds[i + 1]]).strip()
        key = lines[start].split(":", 1)[0]
        if body:
            chunks.extend(_bounded(rel, [key], body))
    return chunks


def _split_sections(rel: str, text: str, bound: bool = True) -> list[Chunk]:
    """Walk the document, cutting at headings that are outside code fences.

    **Fences are tracked rather than assumed**, because these documents are
    full of YAML and shell blocks, and a `#` inside one is a comment rather
    than a heading. Cutting there would split a configuration example and
    hand somebody half of one.
    """
    lines = text.split("\n")
    fenced = False
    path_stack: list[tuple[int, str]] = []
    current: list[str] = []
    chunks: list[Chunk] = []

    def flush() -> None:
        body = "\n".join(current).strip()
        if body:
            heads = [h for _, h in path_stack]
            if bound:
                chunks.extend(_bounded(rel, heads, body))
            else:
                chunks.append(Chunk(document=rel, heading_path=list(heads), text=body))
        current.clear()

    for line in lines:
        if line.lstrip().startswith("```"):
            fenced = not fenced
            current.append(line)
            continue

        heading = None if fenced else re.match(r"^(#{1,6})\s+(.*\S)\s*$", line)
        if heading:
            flush()
            level, title = len(heading.group(1)), heading.group(2)
            while path_stack and path_stack[-1][0] >= level:
                path_stack.pop()
            path_stack.append((level, _plain(title)))
            continue

        current.append(line)

    flush()
    return chunks


def _by_number(rel: str, chunks: list[Chunk], marker: re.Pattern[str]) -> list[Chunk]:
    """Cut sections again at numbered rules, one chunk per rule.

    A rule keeps the section it came from in its heading path and gains its
    own number, so a citation reads `docs/invariants.md › Queue correctness
    › invariant 4` - which is what somebody would write by hand.

    Text before the first number stays as the section's own chunk: the
    invariants file opens with why the list exists and what happens if it
    reaches twenty-five, which is worth retrieving and belongs to no rule.
    """
    out: list[Chunk] = []
    for chunk in chunks:
        lines = chunk.text.split("\n")
        starts = [i for i, line in enumerate(lines) if marker.match(line)]
        if not starts:
            out.append(chunk)
            continue

        if starts[0] > 0:
            preamble = "\n".join(lines[: starts[0]]).strip()
            if preamble:
                out.append(Chunk(document=rel, heading_path=list(chunk.heading_path), text=preamble))

        bounds = starts + [len(lines)]
        for i, start in enumerate(starts):
            body = "\n".join(lines[start : bounds[i + 1]]).strip()
            number = marker.match(lines[start]).group(1)
            out.append(
                Chunk(
                    document=rel,
                    heading_path=chunk.heading_path + [f"invariant {number}"],
                    text=body,
                )
            )
    return out


def _bounded(rel: str, heading_path: list[str], body: str) -> list[Chunk]:
    """One section, split on paragraphs if it is very long.

    Splitting never happens inside a fenced block: a configuration example
    handed over in halves is worse than one that made a chunk too big.
    """
    if len(body) <= MAX_CHARS:
        return [Chunk(document=rel, heading_path=list(heading_path), text=body)]

    parts: list[str] = []
    buf: list[str] = []
    size = 0
    for block in _paragraphs(body):
        if size and size + len(block) > MAX_CHARS:
            parts.append("\n\n".join(buf))
            buf, size = [], 0
        buf.append(block)
        size += len(block) + 2
    if buf:
        parts.append("\n\n".join(buf))

    return [
        Chunk(document=rel, heading_path=list(heading_path), text=p, part=i + 1, parts=len(parts))
        for i, p in enumerate(parts)
    ]


def _paragraphs(body: str) -> list[str]:
    """Blank-line-separated blocks, with fenced blocks kept whole."""
    out: list[str] = []
    buf: list[str] = []
    fenced = False
    for line in body.split("\n"):
        if line.lstrip().startswith("```"):
            fenced = not fenced
        if not fenced and line.strip() == "":
            if buf:
                out.append("\n".join(buf))
                buf = []
            continue
        buf.append(line)
    if buf:
        out.append("\n".join(buf))
    return out


def _plain(title: str) -> str:
    """A heading as a reader says it, without its markup.

    The documents lean on bold and code spans in headings; a citation
    reading "**`--licenses`**" is a citation somebody has to decode.
    """
    title = re.sub(r"`([^`]*)`", r"\1", title)
    title = re.sub(r"\*\*([^*]*)\*\*", r"\1", title)
    title = re.sub(r"\*([^*]*)\*", r"\1", title)
    title = re.sub(r"<[^>]+>", "", title)
    return title.strip()


def chunk_all(root: Path) -> list[Chunk]:
    """The whole corpus, chunked, in reading order."""
    chunks: list[Chunk] = []
    for path in documents(root):
        chunks.extend(chunk_document(path, root))
    return chunks


def corpus_version(root: Path) -> str:
    """The commit the documents came from, or "unknown".

    **An answer without it is a rumour.** saguin gained authentication, TLS
    and mutual TLS in a single day, so every "no, saguin does not do that"
    from the day before became false - and an index built last month answers
    this month's questions with last month's documents, fluently and wrongly.
    The version travels with every answer for the same reason a citation
    does: so a reader can tell what they are being told about.

    Read out of `.git` rather than by running git, because the container
    that indexes has the checkout mounted and no reason to carry a git
    binary. `SAGUIN_COMMIT` overrides it, for an image built from an export
    with no `.git` at all.
    """
    import os

    if forced := os.environ.get("SAGUIN_COMMIT"):
        return forced.strip()

    head = root / ".git" / "HEAD"
    if not head.exists():
        return "unknown"
    ref = head.read_text(encoding="utf-8").strip()
    if ref.startswith("ref: "):
        target = root / ".git" / ref[5:]
        if target.exists():
            return target.read_text(encoding="utf-8").strip()[:12]
        packed = root / ".git" / "packed-refs"
        if packed.exists():
            for line in packed.read_text(encoding="utf-8").split("\n"):
                if line.endswith(" " + ref[5:]):
                    return line.split(" ", 1)[0][:12]
        return "unknown"
    return ref[:12]
