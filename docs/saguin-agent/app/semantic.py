"""Semantic search over the chunks, in this process, with no vector store.

The recall half of grep-mode retrieval. `grep` finds a term the documents and
the question happen to share; this finds a section that *means* what the
question means when they share no words - "turn verification off on a bridge"
and the sentence "there is no way to turn verification off" are the same
question, and an embedding knows it where an exact match may not.

**No VectorDB.** The corpus is a few hundred chunks of 384 numbers each - a
matrix under half a megabyte - so the whole thing lives in a numpy array and
a search is one matrix multiply. A vector database is machinery for a scale
this corpus is three orders of magnitude below; the store it replaced was a
container to run, a collection to keep in step with the commit, and a network
hop per query, for a dot product.

**The chunks are the same ones the vector path indexes** - heading-aware,
fence-aware, a numbered rule to a chunk - so a hit cites `docs/invariants.md ›
Queue correctness › invariant 4` exactly as it always did, and the score is
plain cosine similarity, which is what `SAGUIN_MIN_SCORE` was always measured
against. Retrieval changed shape; a reference did not.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
from fastembed import TextEmbedding

from .corpus import Chunk, chunk_all

# The embedding model: ONNX, no PyTorch, no GPU, downloaded into the image at
# build time so this works on a machine with no route to the internet.
MODEL = "BAAI/bge-small-en-v1.5"


@dataclass
class Progress:
    """What the indexer is doing, for the bar the reader watches.

    **The progress says what it is doing, not only how far along it is**: the
    first run of anything that touches a model looks identical to a hang, and
    "embedding 40/283" is the difference between waiting and restarting.
    """

    stage: str = "starting"
    done: int = 0
    total: int = 0
    ready: bool = False
    error: str = ""
    commit: str = ""
    chunks: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def set(self, **kw) -> None:
        with self._lock:
            for k, v in kw.items():
                setattr(self, k, v)

    def snapshot(self) -> dict:
        with self._lock:
            pct = 100 if self.ready else (int(self.done * 100 / self.total) if self.total else 0)
            return {
                "stage": self.stage,
                "done": self.done,
                "total": self.total,
                "percent": pct,
                "ready": self.ready,
                "error": self.error,
                "commit": self.commit,
                "chunks": self.chunks,
            }


@dataclass
class Hit:
    """One retrieved chunk and its score, the shape the loop already reads."""

    score: float
    payload: dict


class SemanticIndex:
    """The corpus embedded once, searched by matrix multiply."""

    def __init__(self, chunks: list[Chunk], matrix: np.ndarray, embedder: TextEmbedding):
        self._chunks = chunks
        self._matrix = matrix  # (N, D), each row L2-normalized
        self._embedder = embedder

    def search(self, query: str, limit: int) -> list[Hit]:
        """The chunks nearest the query, best first, with cosine scores.

        The query is normalized the same way the rows were, so the dot
        product is cosine similarity directly - the number `SAGUIN_MIN_SCORE`
        is set against and the one shown under a citation.
        """
        raw = next(iter(self._embedder.embed([query])))
        vec = np.asarray(raw, dtype=np.float32)
        norm = float(np.linalg.norm(vec))
        if norm:
            vec = vec / norm
        scores = self._matrix @ vec
        order = np.argsort(scores)[::-1][:limit]
        hits: list[Hit] = []
        for i in order:
            chunk = self._chunks[int(i)]
            hits.append(
                Hit(
                    score=float(scores[int(i)]),
                    payload={
                        "citation": chunk.citation,
                        "document": chunk.document,
                        "text": chunk.text,
                    },
                )
            )
        return hits


def build(docs_root: str, progress: Progress) -> SemanticIndex:
    """Chunk the corpus and embed it into memory, reporting as it goes.

    The same reporting the vector path does, for the same reason: the first
    run embeds a few hundred chunks on whatever laptop this is, and a bar that
    only shows a percentage looks exactly like a hang. What it does *not* do is
    stand up a database or upsert to one - the embeddings stay in this
    process, which is the whole point of this mode.
    """
    root = Path(docs_root)
    chunks = chunk_all(root)
    progress.set(stage="chunking", total=len(chunks), chunks=len(chunks))
    if not chunks:
        raise RuntimeError(f"no documents found under {docs_root}. Is the checkout mounted?")

    embedder = TextEmbedding(MODEL)
    progress.set(stage="Indexing...", done=0)

    vectors: list[np.ndarray] = []
    batch: list[str] = []
    done = 0
    for i, chunk in enumerate(chunks, start=1):
        batch.append(chunk.embedded)
        if len(batch) == 32 or i == len(chunks):
            vectors.extend(np.asarray(v, dtype=np.float32) for v in embedder.embed(batch))
            done = i
            progress.set(stage="Indexing...", done=done)
            batch = []

    matrix = np.vstack(vectors)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    matrix = matrix / norms

    progress.set(stage="ready", ready=True, done=len(chunks))
    return SemanticIndex(chunks, matrix, embedder)
