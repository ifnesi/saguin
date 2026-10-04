"""Run the question set against a running agent, and report.

**This is the instrument that says whether a change made answers worse.**
Chunking, the embedding model, the prompt, the retrieval depth: each is a
knob, and a change that reads better and answers worse is otherwise
indistinguishable from an improvement.

It drives the HTTP endpoint rather than the functions underneath, because
what it is checking is what a reader gets - an answer with its references,
or a refusal.
"""

from __future__ import annotations

import json
import sys
import unicodedata
import urllib.request

import yaml


def folded(s: str) -> str:
    """s as a citation is compared: case, accents and backticks dropped.

    **The documents respell their headings; a question should not fail
    for it.** "The delay is saguin's" stopped matching when the heading
    became "Sagüin's", and "WAL, and synchronous=NORMAL" when it gained
    backticks - two questions that failed whatever the agent answered.
    cmd/saguin's TestEveryCitationTheAskToolsMakeIsInTheDocuments folds the
    same way, so a spelling this cannot match fails there first.
    """
    decomposed = unicodedata.normalize("NFKD", s)
    plain = "".join(c for c in decomposed if not unicodedata.combining(c))
    return plain.replace("`", "").casefold()


def run(base: str, path: str) -> int:
    questions = yaml.safe_load(open(path))
    passed = failed = 0

    for q in questions:
        body = json.dumps({
            "question": q["question"],
            "context": q.get("context", {}),
        }).encode()
        req = urllib.request.Request(
            f"{base}/api/ask", data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                data = json.load(r)
        except Exception as exc:
            print(f"ERROR  {q['question']}\n       {type(exc).__name__}: {exc}")
            failed += 1
            continue

        cites = " | ".join(ref["citation"] for ref in data.get("references", []))
        if q.get("expect_refusal"):
            ok = bool(data.get("refused"))
            detail = "refused" if ok else "answered anyway"
        else:
            # A fact the corpus states in more than one place has more than one
            # right citation: the restart rule is invariant 15 and also RFC
            # 0003's section literally named "Restart". expect_citation may be a
            # single string or a list, and any one matching is a pass. The
            # response must also be an answer rather than a refusal.
            want = q["expect_citation"]
            wants = want if isinstance(want, list) else [want]
            citation_ok = any(folded(w) in folded(cites) for w in wants)
            # A refusal can still carry the section retrieval happened to
            # find. That is not a correct answer, and allowing it to pass made
            # the suite score retrieval presence rather than what a reader got.
            ok = citation_ok and not bool(data.get("refused"))
            detail = f"want one of {wants}" if len(wants) > 1 else f"want {wants[0]!r}"

        print(f"{'PASS' if ok else 'FAIL'}  {q['question']}")
        if not ok:
            print(f"      {detail}")
            print(f"      got: {cites[:200] or '(no references)'}")
            print(f"      answer: {(data.get('markdown') or '')[:160]}")
        passed, failed = (passed + 1, failed) if ok else (passed, failed + 1)

    print(f"\n{passed} passed, {failed} failed, of {len(questions)}")
    return 1 if failed else 0


if __name__ == "__main__":
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5005"
    path = sys.argv[2] if len(sys.argv) > 2 else "/app/questions.yaml"
    sys.exit(run(base, path))
