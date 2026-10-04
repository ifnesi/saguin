"""Everything the operator set, read once, in one place."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    llm: str
    ollama_endpoint: str
    ollama_model: str
    bedrock_model: str
    azure_endpoint: str
    azure_deployment: str
    gcp_project: str
    gcp_region: str
    gcp_model: str
    gcp_credentials: str
    aws_region: str
    docs: str
    commit: str
    max_turns: int
    llm_timeout: int
    excerpts: int
    llm_context: int
    min_score: float
    port: int

    @property
    def local(self) -> bool:
        """Whether the question stays on this machine.

        The interface says this beside the model's name. "Docs only" is
        about which documents are searched; it is not about where the
        question goes, and only ollama answers that second question the way
        somebody typing a client id into the box would hope.
        """
        return self.llm == "ollama"

    @property
    def model_name(self) -> str:
        return {
            "ollama": self.ollama_model,
            "bedrock": self.bedrock_model,
            "azure": self.azure_deployment,
            "gcp": f"vertex/{self.gcp_model}",
        }.get(self.llm, "unconfigured")


def _env(name: str, default: str = "") -> str:
    """An environment variable, treating empty as unset.

    **compose passes every variable through whether the operator set it or
    not**, so an unset one arrives as the empty string rather than as
    absent. Without this, `os.environ.get(name, default)` returns "" and the
    default below never applies - which is why the default has to live in
    exactly one place and empty has to mean the same as missing.

    That is not hypothetical: the model default was written here and again
    in docker-compose.yml, the two drifted within a day, and the compose
    copy was the one that won.
    """
    return os.environ.get(name, "").strip() or default


def _int(name: str, default: int) -> int:
    """A whole number from the environment, or the default.

    **Nothing here may crash on a value somebody typed.** An empty variable
    is what compose sends for anything unset, and `int("")` raises - which
    took down the whole application at import time on a machine where the
    image was older than the compose file, with a stack trace and no hint
    that the number was the problem. A number this cannot parse is worth a
    line on the way past, not a broker of an agent that will not start.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"{name}={raw!r} is not a whole number; using {default}", flush=True)
        return default


def _float(name: str, default: float) -> float:
    """The same, for a fraction."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        print(f"{name}={raw!r} is not a number; using {default}", flush=True)
        return default


def load() -> Settings:
    return Settings(
        llm=_env("SAGUIN_LLM", "ollama").lower(),
        ollama_endpoint=_env("OLLAMA_ENDPOINT", "http://host.docker.internal:11434"),
        ollama_model=_env("OLLAMA_MODEL", "qwen2.5:3b"),
        bedrock_model=_env("BEDROCK_MODEL"),
        azure_endpoint=_env("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=_env("AZURE_OPENAI_DEPLOYMENT"),
        gcp_project=_env("GCP_PROJECT"),
        gcp_region=_env("GCP_REGION", "us-central1"),
        gcp_model=_env("GCP_MODEL", "gemini-1.5-flash-002"),
        gcp_credentials=_env("GOOGLE_APPLICATION_CREDENTIALS"),
        aws_region=_env("AWS_REGION"),
        docs=_env("SAGUIN_DOCS", "/docs"),
        commit=_env("SAGUIN_COMMIT"),
        max_turns=_int("SAGUIN_MAX_TURNS", 6),
        llm_timeout=_int("SAGUIN_LLM_TIMEOUT", 300),
        excerpts=_int("SAGUIN_EXCERPTS", 5),
        llm_context=_int("SAGUIN_LLM_CONTEXT", 8192),
        min_score=_float("SAGUIN_MIN_SCORE", 0.35),
        port=_int("SAGUIN_AGENT_PORT", 5005),
    )
