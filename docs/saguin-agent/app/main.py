"""The Flask application: three endpoints and a page.

Indexing runs in a thread at startup so the page can load and show what is
happening. **A page that will not load until an index is built looks
identical to a page that is broken**, and the first run of this embeds a few
hundred chunks on whatever laptop it is on.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory, stream_with_context

from . import grep_agent, llm
from . import semantic as semantic_index
from .config import load
from .conversation import build_context, normalize_context, normalize_question
from .corpus import corpus_version, documents
from .semantic import Progress, SemanticIndex

cfg = load()
progress = Progress()
app = Flask(__name__, static_folder="../static", static_url_path="")

_semantic: SemanticIndex | None = None


def _index() -> None:
    """Chunk and embed the corpus into memory. No vector store, no network."""
    global _semantic
    root = Path(cfg.docs)
    progress.set(stage="reading the documents", commit=corpus_version(root))
    try:
        _semantic = semantic_index.build(cfg.docs, progress)
    except Exception as exc:  # surfaced to the reader, not only to the log
        progress.set(stage="failed", error=f"{type(exc).__name__}: {exc}")


@app.get("/")
def page():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/saguin.png")
def icon():
    """saguin's own icon, from the checkout rather than a copy.

    The repository already has it at docs/img/saguin_64.png; a second copy
    under static/ would be 119KB committed twice and one of them eventually
    stale. This also fails loudly if the mount is wrong, which is worth
    knowing on the page rather than only in the indexer.
    """
    return send_from_directory(str(Path(cfg.docs) / "docs" / "img"), "saguin_64.png")


@app.get("/api/status")
def status():
    """Everything the interface needs to describe itself honestly.

    The model's health is asked on every poll rather than cached: ollama is
    usually a process on the operator's own machine, and it can go away
    between one question and the next. The reader is told, rather than
    finding out when an answer fails.
    """
    health = llm.health(cfg)
    return jsonify(
        {
            **progress.snapshot(),
            "llm": {
                "provider": cfg.llm,
                "model": cfg.model_name,
                "local": cfg.local,
                "ok": health.ok,
                "detail": health.detail,
                "context": cfg.llm_context,
                "excerpts": cfg.excerpts,
            },
        }
    )


@app.get("/api/docs")
def docs_list():
    """Every document in the corpus, as the navbar's dropdown lists them."""
    root = Path(cfg.docs)
    return jsonify({"documents": [str(p.relative_to(root)) for p in documents(root)]})


@app.get("/api/docs/<path:relpath>")
def doc_content(relpath: str):
    """One document's full text, whole rather than in the chunks it was split into.

    **`relpath` is checked against the corpus list, never joined to `cfg.docs`
    directly.** The checkout is mounted read-only at `/docs`, but that mount
    is the whole repository two directories up - this agent's own `.env`,
    with whatever cloud credentials an operator configured, lives inside it.
    A route that trusted a client-supplied path would be a way to read it.
    """
    root = Path(cfg.docs)
    known = {str(p.relative_to(root)): p for p in documents(root)}
    if relpath not in known:
        return jsonify({"error": "not part of the indexed corpus"}), 404
    return jsonify({"document": relpath, "text": known[relpath].read_text(encoding="utf-8")})


def _blocked(question: str):
    """Why an ask cannot run yet, as a (body, status) pair - or None if it can.

    Checked before either endpoint touches the model: an empty question, an
    index still building, or a model that cannot be reached. The last is the
    ordinary first-run mistake, and the reader gets the operator's fix in the
    operator's words rather than a spinner and then a timeout.
    """
    if not question:
        return jsonify({"error": "ask something"}), 400
    if not progress.snapshot()["ready"]:
        return jsonify({"error": "the documentation is still being indexed"}), 409
    health = llm.health(cfg)
    if not health.ok:
        return jsonify({"error": health.detail}), 503
    return None


@app.post("/api/ask")
def ask_question():
    payload = request.get_json(silent=True) or {}
    question = normalize_question(payload.get("question"))
    context = normalize_context(payload.get("context"))
    if (blocked := _blocked(question)) is not None:
        return blocked

    try:
        result = grep_agent.answer(cfg, question, _semantic, context)
    except Exception as exc:
        return jsonify({"error": f"{type(exc).__name__}: {exc}"}), 500

    return jsonify(
        {
            "markdown": result.markdown,
            "references": result.references,
            "refused": result.refused,
            "trace": result.trace,
            "commit": progress.snapshot()["commit"],
            "context": build_context(question, context, result.references),
        }
    )


@app.post("/api/ask/stream")
def ask_stream():
    """The same answer, as Server-Sent Events, so the page can show the loop work.

    A step event per search or read as it happens, then one answer event with
    the markdown, references and trace. **A loop that shows nothing until it
    finishes reads as a hang** on the slow hardware saguin runs beside, so the
    reader watches it search and read rather than a spinner. The readiness and
    health checks run before the stream opens - once the events are flowing the
    status code is already sent, so a failure there could not be reported.
    """
    payload = request.get_json(silent=True) or {}
    question = normalize_question(payload.get("question"))
    context = normalize_context(payload.get("context"))
    if (blocked := _blocked(question)) is not None:
        return blocked

    commit = progress.snapshot()["commit"]

    def events():
        try:
            for event in grep_agent.run(cfg, question, _semantic, context):
                if event["type"] == "answer":
                    a = event["answer"]
                    payload = {
                        "type": "answer",
                        "markdown": a.markdown,
                        "references": a.references,
                        "refused": a.refused,
                        "trace": a.trace,
                        "commit": commit,
                        "context": build_context(question, context, a.references),
                    }
                else:
                    payload = event
                yield f"data: {json.dumps(payload)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': f'{type(exc).__name__}: {exc}'})}\n\n"

    return Response(
        stream_with_context(events()),
        mimetype="text/event-stream",
        # No proxy buffering: an event held back until the response closes is an
        # event that arrives with the answer it was meant to precede.
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def main() -> None:
    threading.Thread(target=_index, daemon=True).start()
    app.run(host="0.0.0.0", port=cfg.port)


if __name__ == "__main__":
    main()
