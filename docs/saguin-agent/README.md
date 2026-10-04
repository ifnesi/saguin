# Sagüin Q&A Agent

Ask questions about Sagüin and get answers **from Sagüin's own
documentation**, with a reference under every one.

```sh
cp .env_example .env      # optional; edit if you are not using ollama
docker compose up --build
```

The copy is optional. Every default lives in `app/config.py` and an unset
variable arrives empty, which that file treats as unset - so with no `.env`
at all you get ollama on its own default port, which is what most people
want. Make one when you want something else.

Then open <http://localhost:5005>.

The first run indexes the documentation: the corpus, cut into a few hundred
chunks and embedded on the machine, no network. **It takes a few minutes** -
around eleven on a 2014 four-core desktop, less on anything modern - and the
progress bar says which stage it is on and how far through, because a first
run that only shows a percentage looks exactly like a hang.

## What it is

A retrieval agent in a loop: it searches the documentation, reads what comes
back, searches again with better words if that did not answer the question,
and then answers. Nothing is fine-tuned and nothing is memorised - the model
sees the same excerpts you can open yourself, which is why every answer ends
with what it read.

The corpus is `README.md`, `docs/invariants.md`, every RFC, the annotated
`examples/saguin.yaml`, `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG.md` and
`LICENSE`, read from the checkout this runs in. **Every answer states the commit** those documents came from, and
the navbar states it too - Sagüin gained authentication, TLS and mutual TLS
in a single day, so an index a month old answers this month's questions with
last month's documents, fluently and wrongly.

## What it refuses to do

Ask any language model an MQTT question and it will answer - from other
brokers, from the specification. Almost all of that is *nearly* right for
Sagüin, which is worse than wrong: somebody writes another broker's
`max_queued_messages` into a configuration and the broker refuses to start,
or believes a queue behaves like a shared subscription, which fails
silently.

So when nothing in the documentation is close enough to the question, the
answer is **"The documentation does not say."** rather than a paragraph with
no references under it. There is no wider internet to fall back on: every
answer is grounded in Sagüin's own documents or it is not given.

Where your question goes is still worth knowing, even with nowhere else to
search: with a cloud model configured, the question and excerpts retrieved for
it are sent to that provider. After each answer the stateless server gives the
open page a small context capsule: at most two short user questions and three
citation labels for documentation sections it retrieved. A follow-up returns
that capsule so retrieval can resolve words such as “that”; a self-contained
new topic replaces it. Generated answers are never in the capsule, so an
earlier mistake cannot become evidence, and no extra model call is spent
summarising the conversation. The navbar says which model is answering
and whether it is local, so you never have to remember which you configured -
only ollama keeps everything on your machine.

## Configuration

Everything is environment variables; `.env_example` is the list, `.env` is
yours and is gitignored.

| | |
|---|---|
| `SAGUIN_LLM` | `ollama` (default), `bedrock`, `azure`, `gcp` |
| `OLLAMA_ENDPOINT` | default `http://host.docker.internal:11434` |
| `OLLAMA_MODEL` | default `qwen2.5:3b` - about 2GB, answers on a laptop |
| `SAGUIN_MIN_SCORE` | below this, the agent refuses rather than guesses |
| `SAGUIN_MAX_TURNS` | a bound on the loop, so a metered provider cannot be billed without one |

If the model is not reachable, the interface says so and names the fix
rather than spinning: *"ollama is running but does not have qwen2.5:3b. Run
`ollama pull qwen2.5:3b`"*.

## The question set

`questions.yaml` holds questions with known answers, including the ones that
trap a fluent model: how to disable bridge certificate verification (there
is deliberately no way), whether a memory provider survives a power cut (it
does not), what `allow_anonymous` defaults to (it follows the password
file). A question the documents genuinely do not answer expects the
refusal itself.

```sh
docker compose exec agent python -m app.questions
```

It exists because chunking, the embedding model, the prompt and the
retrieval depth are all knobs somebody will turn, and a change that reads
better and answers worse is otherwise indistinguishable from an improvement.

## What it is made of

`BAAI/bge-small-en-v1.5` for the embeddings (ONNX, no PyTorch, no GPU), held
in memory and searched with a numpy dot product - the corpus is a few hundred
chunks, three orders of magnitude below the scale a vector database is for, so
there is no database to run. Alongside it, exact-term search over the files
themselves: the documents' vocabulary is precise, and a rule about "one
consumer per record" is the answer to "can two workers get the same job" that
an embedding trained on prose does not reward. Flask for the backend, React
from vendored UMD files for the interface - no npm, no build step, no CDN at
runtime. The embedding model and the JavaScript are both downloaded when the
image is built, so this works on a machine with no route to the internet,
which is where Sagüin usually lives.
