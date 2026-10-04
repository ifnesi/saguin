"""The model, whichever one the operator chose, and whether it is there.

**A model that cannot be reached is an error the reader sees.** The
alternative is a question that spins and then fails with nothing useful, on
a box where ollama is simply not running - which is the ordinary first-run
mistake, not an exotic one.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

import requests

from .config import Settings


@dataclass
class Health:
    ok: bool
    detail: str
    """Empty when ok; otherwise what an operator has to fix, in their words."""


def health(cfg: Settings) -> Health:
    """Can the configured model be reached, right now.

    Asked when the interface loads and again before an answer is attempted:
    ollama is usually a process on the operator's own machine, and a process
    can go away between one question and the next.
    """
    if cfg.llm == "ollama":
        try:
            r = requests.get(f"{cfg.ollama_endpoint}/api/tags", timeout=3)
            r.raise_for_status()
            models = [m.get("name", "") for m in r.json().get("models", [])]
        except Exception as exc:
            return Health(
                False,
                f"ollama is not answering at {cfg.ollama_endpoint} ({type(exc).__name__}). "
                f"Start it, or set OLLAMA_ENDPOINT to where it runs.",
            )
        if cfg.ollama_model not in models:
            return Health(
                False,
                f"ollama is running but does not have {cfg.ollama_model}. "
                f"Run `ollama pull {cfg.ollama_model}`, or set OLLAMA_MODEL to one it has"
                + (f": {', '.join(sorted(models)[:6])}" if models else " (it has none)"),
            )
        return Health(True, "")

    # The cloud providers are not reachability-tested with a request, because
    # the cheapest request to most of them is still a billable one. What is
    # checked is that the operator gave the settings it cannot work without -
    # which is the failure that actually happens, and it happens at startup.
    required = {
        "bedrock": [("BEDROCK_MODEL", cfg.bedrock_model), ("AWS_REGION", cfg.aws_region)],
        "azure": [
            ("AZURE_OPENAI_ENDPOINT", cfg.azure_endpoint),
            ("AZURE_OPENAI_DEPLOYMENT", cfg.azure_deployment),
        ],
        "gcp": [("GCP_PROJECT", cfg.gcp_project), ("GOOGLE_APPLICATION_CREDENTIALS", cfg.gcp_credentials)],
    }.get(cfg.llm)

    if required is None:
        return Health(
            False,
            f"SAGUIN_LLM is {cfg.llm!r}, which is not one of ollama, bedrock, azure, gcp.",
        )
    missing = [name for name, value in required if not value]
    if missing:
        return Health(False, f"{cfg.llm} is selected and {', '.join(missing)} is not set.")
    return Health(True, "")


def ask(cfg: Settings, messages: list[dict]) -> str:
    """One turn with the model. Only ollama is wired today."""
    if cfg.llm == "ollama":
        try:
            r = requests.post(
                f"{cfg.ollama_endpoint}/api/chat",
                json={
                "model": cfg.ollama_model,
                "messages": messages,
                "stream": False,
                # **The context window has to be asked for.** A model that
                # supports 32k is served by ollama at its own default -
                # historically 2048 - unless the request says otherwise, and
                # when the prompt is longer the oldest tokens are dropped
                # silently. The oldest tokens here are the system prompt and
                # the start of the excerpts, so the model answers without
                # having seen what it is citing, while the references stay
                # attached because they come from retrieval rather than from
                # the model. An answer with a citation it never read is the
                # exact failure this tool exists to prevent.
                "options": {"num_ctx": cfg.llm_context},
            },
                timeout=cfg.llm_timeout,
            )
        except requests.Timeout:
            # **Measured, not hypothetical.** On a 2014 four-core desktop
            # qwen2.5:3b takes over three minutes for one turn of this loop,
            # and the first version of this had 120 seconds hardcoded: every
            # question failed with "ReadTimeout" while the model was still
            # working, and nothing said which knob to turn.
            raise TimeoutError(
                f"{cfg.ollama_model} did not answer within {cfg.llm_timeout}s. On slow "
                f"hardware that is normal for a large model - raise SAGUIN_LLM_TIMEOUT, "
                f"or use a smaller model."
            ) from None
        r.raise_for_status()
        return r.json().get("message", {}).get("content", "")

    if cfg.llm == "bedrock":
        return _ask_bedrock(cfg, messages)

    if cfg.llm == "azure":
        return _ask_azure(cfg, messages)

    if cfg.llm == "gcp":
        return _ask_gcp(cfg, messages)

    raise NotImplementedError(
        f"{cfg.llm} is configured but not wired yet; ollama is. "
        f"This is a gap in the agent, not in your configuration."
    )


def _ask_bedrock(cfg: Settings, messages: list[dict]) -> str:
    """One turn through Bedrock's Converse API.

    Converse rather than InvokeModel: it is the one shape that works the
    same across every model family Bedrock hosts, so this does not have to
    know whether `BEDROCK_MODEL` names an Anthropic, Amazon or Meta model.

    A system message is a separate argument there, not a turn - the loop
    here carries it as `messages[0]`, the same shape ollama's `/api/chat`
    takes, so it is split out rather than duplicated at the call site in
    `agent.py`.
    """
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    client = boto3.client(
        "bedrock-runtime",
        region_name=cfg.aws_region,
        config=Config(read_timeout=cfg.llm_timeout, connect_timeout=10),
    )

    system = [{"text": m["content"]} for m in messages if m["role"] == "system"]
    turns = [
        {"role": m["role"], "content": [{"text": m["content"]}]}
        for m in messages
        if m["role"] != "system"
    ]

    try:
        resp = client.converse(
            modelId=cfg.bedrock_model,
            system=system,
            messages=turns,
            # No inferenceConfig.maxTokens override: the model's own default
            # is what every other provider here gets too, and a hard cap
            # tuned for one model family is a silent truncation for another.
        )
    except (BotoCoreError, ClientError) as exc:
        raise TimeoutError(f"Bedrock did not answer ({type(exc).__name__}): {exc}") from None

    content = resp.get("output", {}).get("message", {}).get("content", [])
    return "".join(block.get("text", "") for block in content)


def _ask_azure(cfg: Settings, messages: list[dict]) -> str:
    """One turn through an Azure OpenAI chat-completions deployment.

    Its request body already takes `{"role", "content"}` turns, system
    message included - the same shape the loop builds for ollama - so
    nothing here is reshaped, unlike Bedrock's Converse API.
    """
    url = (
        f"{cfg.azure_endpoint}/openai/deployments/{cfg.azure_deployment}"
        "/chat/completions?api-version=2024-06-01"
    )
    try:
        r = requests.post(
            url,
            headers={"api-key": os.environ.get("AZURE_OPENAI_API_KEY", "")},
            json={"messages": messages},
            timeout=cfg.llm_timeout,
        )
    except requests.Timeout:
        raise TimeoutError(
            f"{cfg.azure_deployment} did not answer within {cfg.llm_timeout}s. "
            f"Raise SAGUIN_LLM_TIMEOUT, or use a smaller deployment."
        ) from None
    r.raise_for_status()
    choices = r.json().get("choices", [])
    return choices[0]["message"]["content"] if choices else ""


def _ask_gcp(cfg: Settings, messages: list[dict]) -> str:
    """One turn through Vertex AI's `generateContent` REST endpoint.

    Vertex's SDK pulls in a heavy dependency tree for one call; the REST
    shape needs only a bearer token, which `google-auth` gets from the
    credentials file `GOOGLE_APPLICATION_CREDENTIALS` names - the same
    Application Default Credentials path every other Google client library
    reads, so an operator who has already set this up for anything else
    Google does not have to learn a second way here.
    """
    import google.auth
    import google.auth.transport.requests as gareq

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(gareq.Request())

    system = [{"text": m["content"]} for m in messages if m["role"] == "system"]
    turns = [
        {"role": "model" if m["role"] == "assistant" else "user", "parts": [{"text": m["content"]}]}
        for m in messages
        if m["role"] != "system"
    ]

    url = (
        f"https://{cfg.gcp_region}-aiplatform.googleapis.com/v1/projects/{cfg.gcp_project}"
        f"/locations/{cfg.gcp_region}/publishers/google/models/{cfg.gcp_model}:generateContent"
    )
    try:
        r = requests.post(
            url,
            headers={"Authorization": f"Bearer {creds.token}"},
            json={"contents": turns, "systemInstruction": {"parts": [s for s in system]}},
            timeout=cfg.llm_timeout,
        )
    except requests.Timeout:
        raise TimeoutError(
            f"{cfg.gcp_model} did not answer within {cfg.llm_timeout}s. "
            f"Raise SAGUIN_LLM_TIMEOUT, or use a smaller model."
        ) from None
    r.raise_for_status()
    candidates = r.json().get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in parts)
