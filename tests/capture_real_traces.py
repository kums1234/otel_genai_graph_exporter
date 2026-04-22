#!/usr/bin/env python3
"""Capture real OTLP GenAI traces from the Anthropic / OpenAI SDKs.

This script does *not* depend on OpenLLMetry / Traceloop — it wraps the
SDK calls in hand-rolled OTel-compatible spans so you get v1.37
semconv-conformant output with no extra dependencies. Drop in
`opentelemetry-instrumentation-anthropic` later if you want richer
instrumentation; the output format is the same.

Features
--------
  * Reads prompts from a plain-text file (one per line) or from --prompt
    flags.
  * Hard budget cap: stops before exceeding --budget-usd. Estimates cost
    from usage tokens via `otel_genai_graph.cost`.
  * Emits one OTLP/JSON file per conversation under --output-dir,
    following the fixture schema so the same tests can run against real
    traces.
  * Supports --shape chat | agent_tool — `agent_tool` wraps the LLM call
    in a synthetic invoke_agent span so multi-agent graph features get
    exercised.

Usage
-----
  # Anthropic
  export ANTHROPIC_API_KEY=...
  pip install anthropic
  python tests/capture_real_traces.py \\
      --provider anthropic --model claude-haiku-4-5 \\
      --prompts prompts.txt --budget-usd 5.00 \\
      --output-dir tests/fixtures/real

  # OpenAI
  export OPENAI_API_KEY=...
  pip install openai
  python tests/capture_real_traces.py \\
      --provider openai --model gpt-4o-mini \\
      --prompt "What is 2+2?" --budget-usd 0.10 \\
      --output-dir tests/fixtures/real

  # Google AI Studio (Gemini) — free tier, no credit card
  export GEMINI_API_KEY=...     # from aistudio.google.com
  pip install google-genai
  python tests/capture_real_traces.py \\
      --provider google --model gemini-2.5-flash \\
      --prompt "Hello" --budget-usd 0.10 \\
      --output-dir tests/fixtures/real

  # Ollama (local, unlimited, $0) — uses the openai adapter + base_url env
  brew install ollama && ollama serve &
  ollama pull qwen2.5:7b            # chat / tool_call (good tool use)
  ollama pull nomic-embed-text      # embeddings
  export OPENAI_BASE_URL=http://localhost:11434/v1/
  export OPENAI_API_KEY=ollama      # any non-empty value; ignored
  # Local models can be slow — bump the SDK read timeout if needed:
  export OPENAI_TIMEOUT_SECONDS=600  # default 300; raise for bigger models
  # And cap output length — the single biggest latency win for local
  # models. Without it, a 7B model will cheerfully generate 4 k tokens
  # for a "hello" prompt.
  export OPENAI_MAX_OUTPUT_TOKENS=256
  python tests/capture_real_traces.py \\
      --provider openai --model qwen2.5:7b \\
      --shape tool_call --ignore-unknown-pricing \\
      --prompt "What time is it?" --budget-usd 0 \\
      --output-dir tests/fixtures/real/ollama

Shapes
------
  chat         single chat span
  agent_tool   invoke_agent span wrapping one chat span
  embeddings   single /v1/embeddings span (OpenAI-SDK-compatible only)
  multi_turn   N chat spans (one per --prompt), all sharing one
               conversation.id across distinct trace_ids — mirrors
               fixtures/multi_turn_conversation.json
  tool_call    invoke_agent → chat (model requests a tool) →
               execute_tool (locally evaluated) → chat (follow-up).
               OpenAI-SDK-compatible providers only.

  # Azure OpenAI Service (classic Azure OpenAI endpoint)
  export AZURE_OPENAI_ENDPOINT=https://myres.openai.azure.com
  export AZURE_OPENAI_API_KEY=...
  # optional: AZURE_OPENAI_API_VERSION (default 2024-10-21)
  pip install openai
  python tests/capture_real_traces.py \\
      --provider azure_openai \\
      --model my-prod-gpt4-deploy --cost-model gpt-4o \\
      --prompt "Hello" --budget-usd 0.10 \\
      --output-dir tests/fixtures/real

  # Microsoft Foundry unified inference endpoint
  export AZURE_AI_INFERENCE_ENDPOINT=https://project.services.ai.azure.com/models
  export AZURE_AI_INFERENCE_KEY=...
  pip install azure-ai-inference
  python tests/capture_real_traces.py \\
      --provider azure_inference \\
      --model gpt-4o-mini --cost-model gpt-4o-mini \\
      --prompt "Hello" --budget-usd 0.10 \\
      --output-dir tests/fixtures/real

Azure note
----------
`--model` is the *deployment name* in Azure, which is an arbitrary string
you chose in the portal. The cost.PRICING table is keyed on canonical
model names (gpt-4o, gpt-4o-mini, ...). Pass `--cost-model` to point the
budget guard at the matching entry. If you leave it unset and the
deployment name doesn't match any row, the guard refuses to run —
pass `--ignore-unknown-pricing` to bypass.

Auth
----
  - API key: set the `*_API_KEY` env var for your provider.
  - Entra ID (recommended for Azure in prod): omit the `*_API_KEY` env
    var and `pip install azure-identity`. The script uses
    DefaultAzureCredential, so `az login` / managed identity / workload
    identity all work.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

# We import cost lazily from the installed package path so this script also
# runs when the package isn't pip-installed yet.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from otel_genai_graph._env import load_env  # noqa: E402
from otel_genai_graph.cost import compute_cost  # noqa: E402


# ---------------------------------------------------------------------------
# OTLP/JSON span builders (same shape as generate_traces.py / fixtures)
# ---------------------------------------------------------------------------

def _s(k: str, v: Any) -> dict:
    return {"key": k, "value": {"stringValue": str(v)}}


def _i(k: str, v: int) -> dict:
    return {"key": k, "value": {"intValue": str(int(v))}}


def _new_trace_id() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex[:16]  # 32 hex chars / 16 bytes


def _new_span_id() -> str:
    return uuid.uuid4().hex[:16]


# ---------------------------------------------------------------------------
# Budget guard
# ---------------------------------------------------------------------------

class BudgetExceeded(RuntimeError):
    pass


class BudgetGuard:
    def __init__(self, cap_usd: float, allow_unknown: bool = False) -> None:
        self.cap = cap_usd
        self.spent = 0.0
        self.allow_unknown = allow_unknown

    def charge(
        self,
        cost_provider: str,
        cost_model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> float:
        cost = compute_cost(cost_provider, cost_model, input_tokens, output_tokens)
        if cost is None:
            if not self.allow_unknown:
                raise BudgetExceeded(
                    f"No pricing for {cost_provider}/{cost_model} — add it to "
                    "cost.PRICING, pass --cost-model to alias a known model, "
                    "or pass --ignore-unknown-pricing to bypass the budget."
                )
            return 0.0
        if self.spent + cost > self.cap:
            raise BudgetExceeded(
                f"Budget exceeded: spent=${self.spent:.4f} + call=${cost:.4f} "
                f"> cap=${self.cap:.2f}"
            )
        self.spent += cost
        return cost


# ---------------------------------------------------------------------------
# Provider adapters — call SDK, return token usage + response text
# ---------------------------------------------------------------------------

def _call_anthropic(model: str, prompt: str) -> tuple[str, int, int]:
    try:
        from anthropic import Anthropic
    except ImportError as e:
        raise RuntimeError("pip install anthropic") from e
    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    resp = client.messages.create(
        model=model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(block.text for block in resp.content if getattr(block, "type", "") == "text")
    return text, resp.usage.input_tokens, resp.usage.output_tokens


def _call_openai(model: str, prompt: str) -> tuple[str, int, int]:
    # Delegates to the shared factory so the timeout override also flows
    # here. Works with plain OpenAI + any OpenAI-compatible backend
    # (Ollama, Groq, etc.) via `OPENAI_BASE_URL`.
    client = _openai_client_for("openai")
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        **_chat_create_kwargs(),
    )
    text = resp.choices[0].message.content or ""
    return text, resp.usage.prompt_tokens, resp.usage.completion_tokens


def _call_google(model: str, prompt: str) -> tuple[str, int, int]:
    """Google AI Studio (Gemini API) via the unified `google-genai` SDK.

    Env: `GEMINI_API_KEY` (preferred) or `GOOGLE_API_KEY`. The Client
    constructor picks either up automatically, but we read and validate
    so the error message is actionable.

    Notes on token accounting: Gemini 2.5 "thinking" models bill
    `thoughts_token_count` as output. We fold it into output_tokens so
    the budget guard and graph match the invoice.
    """
    try:
        from google import genai  # `pip install google-genai`
    except ImportError as e:
        raise RuntimeError("pip install google-genai") from e

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("set GEMINI_API_KEY (or GOOGLE_API_KEY) from aistudio.google.com")

    client = genai.Client(api_key=api_key)
    resp = client.models.generate_content(model=model, contents=prompt)
    text = resp.text or ""

    usage = resp.usage_metadata
    input_tokens = int(getattr(usage, "prompt_token_count", 0) or 0)
    output_tokens = int(getattr(usage, "candidates_token_count", 0) or 0)
    # 2.5 "thinking" models emit `thoughts_token_count` — billed as output.
    output_tokens += int(getattr(usage, "thoughts_token_count", 0) or 0)
    return text, input_tokens, output_tokens


def _call_azure_openai(deployment: str, prompt: str) -> tuple[str, int, int]:
    """Azure OpenAI Service — same SDK as `_call_openai`, different client.

    `deployment` is the deployment name you configured in Azure — NOT the
    canonical model name. Pass `--cost-model gpt-4o-mini` (or similar) so
    the budget guard can price it against OpenAI's table.
    """
    try:
        from openai import AzureOpenAI
    except ImportError as e:
        raise RuntimeError("pip install openai") from e
    try:
        endpoint = os.environ["AZURE_OPENAI_ENDPOINT"]
    except KeyError as e:
        raise RuntimeError("set AZURE_OPENAI_ENDPOINT (e.g. https://myres.openai.azure.com)") from e

    api_key = os.environ.get("AZURE_OPENAI_API_KEY")
    api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-10-21")
    if api_key:
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            api_key=api_key,
            api_version=api_version,
        )
    else:
        # Entra ID — preferred for prod. Requires `pip install azure-identity`.
        try:
            from azure.identity import DefaultAzureCredential, get_bearer_token_provider
        except ImportError as e:
            raise RuntimeError(
                "set AZURE_OPENAI_API_KEY, or `pip install azure-identity` for Entra ID auth"
            ) from e
        token_provider = get_bearer_token_provider(
            DefaultAzureCredential(),
            "https://cognitiveservices.azure.com/.default",
        )
        client = AzureOpenAI(
            azure_endpoint=endpoint,
            azure_ad_token_provider=token_provider,
            api_version=api_version,
        )

    resp = client.chat.completions.create(
        model=deployment,  # for Azure, the "model" argument is the deployment name
        messages=[{"role": "user", "content": prompt}],
        **_chat_create_kwargs(),
    )
    text = resp.choices[0].message.content or ""
    return text, resp.usage.prompt_tokens, resp.usage.completion_tokens


def _call_azure_openai_v1(deployment: str, prompt: str) -> tuple[str, int, int]:
    """Azure OpenAI v1 / OpenAI-compatible data plane.

    Uses the vanilla `OpenAI` client pointed at
    `https://<resource>.openai.azure.com/openai/v1/` — this is the newer
    path you see in the Foundry portal's *Consume* tab. It coexists on
    the same resource as the classic AOAI routes (`/openai/deployments/...`)
    but uses OpenAI's request/response shape without deployment-in-path
    routing, so the stock `openai` SDK works unmodified.

    Env:
      AZURE_OPENAI_ENDPOINT  — e.g. https://myres.openai.azure.com
                               (the script appends /openai/v1/ if missing)
      AZURE_OPENAI_API_KEY   — key, or omit for Entra ID (requires
                               `pip install azure-identity`)
    """
    try:
        from openai import OpenAI
    except ImportError as e:
        raise RuntimeError("pip install openai") from e

    try:
        endpoint = os.environ["AZURE_OPENAI_ENDPOINT"].rstrip("/")
    except KeyError as e:
        raise RuntimeError("set AZURE_OPENAI_ENDPOINT") from e
    if not endpoint.endswith("/openai/v1"):
        endpoint = f"{endpoint}/openai/v1"
    base_url = endpoint + "/"

    api_key = os.environ.get("AZURE_OPENAI_API_KEY")
    if not api_key:
        # Entra ID — mint a bearer token once per call.
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as e:
            raise RuntimeError(
                "set AZURE_OPENAI_API_KEY, or `pip install azure-identity` for Entra ID auth"
            ) from e
        token = DefaultAzureCredential().get_token("https://cognitiveservices.azure.com/.default").token
        client = OpenAI(api_key=token, base_url=base_url)
    else:
        client = OpenAI(api_key=api_key, base_url=base_url)

    resp = client.chat.completions.create(
        model=deployment,
        messages=[{"role": "user", "content": prompt}],
        **_chat_create_kwargs(),
    )
    text = resp.choices[0].message.content or ""
    return text, resp.usage.prompt_tokens, resp.usage.completion_tokens


def _call_azure_inference(model: str, prompt: str) -> tuple[str, int, int]:
    """Microsoft Foundry unified inference endpoint (azure-ai-inference SDK).

    Use this for Foundry deployments of non-OpenAI models (Mistral, Llama,
    Phi, etc.) or when you prefer the unified endpoint over the classic
    Azure OpenAI one. Requires `pip install azure-ai-inference`.
    """
    try:
        from azure.ai.inference import ChatCompletionsClient
        from azure.ai.inference.models import UserMessage
        from azure.core.credentials import AzureKeyCredential
    except ImportError as e:
        raise RuntimeError("pip install azure-ai-inference") from e

    try:
        endpoint = os.environ["AZURE_AI_INFERENCE_ENDPOINT"]
    except KeyError as e:
        raise RuntimeError(
            "set AZURE_AI_INFERENCE_ENDPOINT (Foundry project or deployment URL)"
        ) from e

    api_version = os.environ.get("AZURE_AI_INFERENCE_API_VERSION", "2024-10-21")
    api_key = os.environ.get("AZURE_AI_INFERENCE_KEY")
    if api_key:
        client = ChatCompletionsClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(api_key),
            api_version=api_version,
        )
    else:
        try:
            from azure.identity import DefaultAzureCredential
        except ImportError as e:
            raise RuntimeError(
                "set AZURE_AI_INFERENCE_KEY, or `pip install azure-identity` for Entra ID auth"
            ) from e
        client = ChatCompletionsClient(
            endpoint=endpoint,
            credential=DefaultAzureCredential(),
            api_version=api_version,
        )

    resp = client.complete(
        model=model,
        messages=[UserMessage(content=prompt)],
    )
    text = resp.choices[0].message.content or ""
    return text, resp.usage.prompt_tokens, resp.usage.completion_tokens


# ---------------------------------------------------------------------------
# OpenAI-SDK-compatible extensions (embeddings + tool calling)
#
# These work with any backend the `openai` SDK can talk to — OpenAI proper,
# Azure (classic + v1), Ollama (`OPENAI_BASE_URL=http://localhost:11434/v1/`),
# Groq, Together, Fireworks, etc. Anthropic / Google have different APIs and
# aren't wired here.
# ---------------------------------------------------------------------------

def _openai_timeout_seconds() -> float:
    """Read OPENAI_TIMEOUT_SECONDS; default is 300 s (5 min).

    The openai SDK defaults to 600 s but the underlying httpx.ReadTimeout
    can kick in sooner when a local model (Ollama) takes a long time to
    stream a response. We surface the knob explicitly so you can bump it
    without code changes.
    """
    raw = os.environ.get("OPENAI_TIMEOUT_SECONDS", "300")
    try:
        return float(raw)
    except ValueError:
        return 300.0


def _openai_max_output_tokens() -> Optional[int]:
    """Read OPENAI_MAX_OUTPUT_TOKENS; default None (unbounded).

    Local models (Ollama) will happily generate thousands of tokens for a
    "Hello" prompt — setting this to 128 / 256 is the difference between
    a 2-second request and a 10-minute one.
    """
    raw = os.environ.get("OPENAI_MAX_OUTPUT_TOKENS", "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
        return n if n > 0 else None
    except ValueError:
        return None


def _chat_create_kwargs() -> dict[str, Any]:
    """Kwargs for client.chat.completions.create() — currently just max_tokens."""
    kw: dict[str, Any] = {}
    cap = _openai_max_output_tokens()
    if cap is not None:
        kw["max_tokens"] = cap
    return kw


def _openai_client_for(provider_id: str):
    """Construct an openai.OpenAI (or AzureOpenAI) client for the provider.

    Unifies the adapter choice so the embedding / tools helpers don't
    duplicate each provider's auth dance. Respects OPENAI_TIMEOUT_SECONDS
    so slow local models (Ollama) don't get cut off at httpx's 60 s
    default read timeout.
    """
    from openai import OpenAI

    timeout = _openai_timeout_seconds()

    if provider_id == "openai":
        return OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY", ""),
            base_url=os.environ.get("OPENAI_BASE_URL") or None,
            timeout=timeout,
        )
    if provider_id == "azure_openai_v1":
        endpoint = os.environ["AZURE_OPENAI_ENDPOINT"].rstrip("/")
        if not endpoint.endswith("/openai/v1"):
            endpoint += "/openai/v1"
        api_key = os.environ.get("AZURE_OPENAI_API_KEY")
        if api_key:
            return OpenAI(api_key=api_key, base_url=endpoint + "/", timeout=timeout)
        from azure.identity import DefaultAzureCredential
        token = DefaultAzureCredential().get_token(
            "https://cognitiveservices.azure.com/.default"
        ).token
        return OpenAI(api_key=token, base_url=endpoint + "/", timeout=timeout)
    raise RuntimeError(
        f"provider {provider_id!r} doesn't expose an OpenAI-SDK-compatible "
        "client (embeddings/tool_call shapes are OpenAI-SDK-only for now)"
    )


def _embed_openai(provider_id: str, model: str, text: str) -> tuple[int, int]:
    """Call /v1/embeddings. Returns (vector_length, input_tokens)."""
    client = _openai_client_for(provider_id)
    resp = client.embeddings.create(model=model, input=text)
    vec_len = len(resp.data[0].embedding) if resp.data else 0
    input_tokens = getattr(resp.usage, "prompt_tokens", 0) or 0
    return vec_len, int(input_tokens)


# --- tool calling ---------------------------------------------------------

def _tool_get_current_time(_args: dict) -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


TOOL_REGISTRY: dict[str, Callable[[dict], str]] = {
    "get_current_time": _tool_get_current_time,
}

TOOL_DEFINITIONS: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_current_time",
            "description": "Return the current UTC time as an ISO-8601 string.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    }
]


def _call_openai_with_tools(
    provider_id: str,
    model: str,
    messages: list[dict],
) -> tuple[Any, int, int]:
    """One round-trip of chat.completions.create with tools enabled."""
    client = _openai_client_for(provider_id)
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        tools=TOOL_DEFINITIONS,
        tool_choice="auto",
        **_chat_create_kwargs(),
    )
    usage = resp.usage
    return resp, int(usage.prompt_tokens or 0), int(usage.completion_tokens or 0)


@dataclass(frozen=True)
class ProviderInfo:
    """One registry row per API backend."""
    call: Callable[[str, str], tuple[str, int, int]]
    semconv_name: str  # value used for `gen_ai.provider.name`
    cost_provider: str  # key used for cost.PRICING lookup


PROVIDERS: dict[str, ProviderInfo] = {
    "anthropic":       ProviderInfo(_call_anthropic,       "anthropic",           "anthropic"),
    "openai":          ProviderInfo(_call_openai,          "openai",              "openai"),
    "azure_openai":    ProviderInfo(_call_azure_openai,    "azure.ai.openai",     "openai"),
    "azure_openai_v1": ProviderInfo(_call_azure_openai_v1, "azure.ai.openai",     "openai"),
    "azure_inference": ProviderInfo(_call_azure_inference, "azure.ai.inference",  "openai"),
    "google":          ProviderInfo(_call_google,          "gcp.gen_ai",          "google"),
}


# ---------------------------------------------------------------------------
# Trace assembly
# ---------------------------------------------------------------------------

def record_chat_trace(
    *,
    provider: str,
    model: str,
    cost_model: str,
    prompt: str,
    conv_id: str,
    budget: BudgetGuard,
    service_name: str,
) -> dict:
    """Single chat span, like fixtures/simple_llm_call.json."""
    info = PROVIDERS[provider]
    trace_id = _new_trace_id()
    span_id = _new_span_id()

    start = time.time_ns()
    text, inp, out = info.call(model, prompt)
    cost = budget.charge(info.cost_provider, cost_model, inp, out)
    end = time.time_ns()

    span = {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": "",
        "name": f"chat {model}",
        "kind": 3,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(end),
        "attributes": [
            _s("gen_ai.operation.name", "chat"),
            _s("gen_ai.provider.name", info.semconv_name),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _s("gen_ai.conversation.id", conv_id),
            _i("gen_ai.usage.input_tokens", inp),
            _i("gen_ai.usage.output_tokens", out),
        ],
        "status": {"code": 1},
    }
    return {
        "name": f"real_chat_{conv_id}",
        "description": f"{info.semconv_name}/{model} real chat — ${cost:.5f}, tokens {inp}/{out}",
        "otlp": {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_s("service.name", service_name)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "capture_real_traces", "version": "0.1.0"},
                            "spans": [span],
                        }
                    ],
                }
            ]
        },
        "captured": {
            "cost_usd": cost,
            "input_tokens": inp,
            "output_tokens": out,
            "response_preview": text[:200],
        },
    }


def record_agent_tool_trace(
    *,
    provider: str,
    model: str,
    cost_model: str,
    prompt: str,
    conv_id: str,
    agent_id: str,
    budget: BudgetGuard,
    service_name: str,
) -> dict:
    """Synthetic invoke_agent span wrapping a real chat span.

    Real SDKs don't emit `invoke_agent` spans yet — agent orchestration
    usually sits above the SDK (LangGraph, etc.). We add one so the
    multi-agent graph features get exercised against real LLM output.
    """
    info = PROVIDERS[provider]
    trace_id = _new_trace_id()
    agent_span_id = _new_span_id()
    chat_span_id = _new_span_id()

    agent_start = time.time_ns()

    text, inp, out = info.call(model, prompt)
    cost = budget.charge(info.cost_provider, cost_model, inp, out)
    chat_end = time.time_ns()

    agent_span = {
        "traceId": trace_id,
        "spanId": agent_span_id,
        "parentSpanId": "",
        "name": f"invoke_agent {agent_id}",
        "kind": 1,
        "startTimeUnixNano": str(agent_start),
        "endTimeUnixNano": str(chat_end),
        "attributes": [
            _s("gen_ai.operation.name", "invoke_agent"),
            _s("gen_ai.agent.id", agent_id),
            _s("gen_ai.agent.name", agent_id),
            _s("gen_ai.conversation.id", conv_id),
        ],
        "status": {"code": 1},
    }
    chat_span = {
        "traceId": trace_id,
        "spanId": chat_span_id,
        "parentSpanId": agent_span_id,
        "name": f"chat {model}",
        "kind": 3,
        "startTimeUnixNano": str(agent_start + 1_000_000),
        "endTimeUnixNano": str(chat_end - 1_000_000),
        "attributes": [
            _s("gen_ai.operation.name", "chat"),
            _s("gen_ai.provider.name", info.semconv_name),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _s("gen_ai.conversation.id", conv_id),
            _s("gen_ai.agent.id", agent_id),
            _i("gen_ai.usage.input_tokens", inp),
            _i("gen_ai.usage.output_tokens", out),
        ],
        "status": {"code": 1},
    }

    return {
        "name": f"real_agent_tool_{conv_id}",
        "description": f"{info.semconv_name}/{model} under {agent_id} — ${cost:.5f}, tokens {inp}/{out}",
        "otlp": {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_s("service.name", service_name)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "capture_real_traces", "version": "0.1.0"},
                            "spans": [agent_span, chat_span],
                        }
                    ],
                }
            ]
        },
        "captured": {
            "cost_usd": cost,
            "input_tokens": inp,
            "output_tokens": out,
            "response_preview": text[:200],
        },
    }


def record_embeddings_trace(
    *,
    provider: str,
    model: str,
    cost_model: str,
    prompt: str,
    conv_id: str,
    budget: BudgetGuard,
    service_name: str,
) -> dict:
    """Single `embeddings` span — mirrors fixtures/rag_flow.json's first span."""
    info = PROVIDERS[provider]
    trace_id = _new_trace_id()
    span_id = _new_span_id()

    start = time.time_ns()
    vec_len, inp = _embed_openai(provider, model, prompt)
    out = 0  # embeddings have no output tokens
    cost = budget.charge(info.cost_provider, cost_model, inp, out)
    end = time.time_ns()

    span = {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": "",
        "name": f"embeddings {model}",
        "kind": 3,
        "startTimeUnixNano": str(start),
        "endTimeUnixNano": str(end),
        "attributes": [
            _s("gen_ai.operation.name", "embeddings"),
            _s("gen_ai.provider.name", info.semconv_name),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _s("gen_ai.conversation.id", conv_id),
            _i("gen_ai.usage.input_tokens", inp),
            _i("gen_ai.usage.output_tokens", out),
        ],
        "status": {"code": 1},
    }
    return {
        "name": f"real_embeddings_{conv_id}",
        "description": f"{info.semconv_name}/{model} embeddings — dim={vec_len}, {inp} tok, ${cost:.5f}",
        "otlp": {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_s("service.name", service_name)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "capture_real_traces", "version": "0.1.0"},
                            "spans": [span],
                        }
                    ],
                }
            ]
        },
        "captured": {
            "cost_usd": cost,
            "input_tokens": inp,
            "output_tokens": out,
            "vector_length": vec_len,
            "response_preview": f"<vector of dim {vec_len}>",
        },
    }


def record_multi_turn_trace(
    *,
    provider: str,
    model: str,
    cost_model: str,
    prompts: list[str],
    conv_id: str,
    budget: BudgetGuard,
    service_name: str,
) -> list[dict]:
    """N chat spans — each in its own trace — that share one conversation.id.

    Exercises the Session-merge invariant the way fixtures/multi_turn_conversation
    does: different `trace_id`s collapse onto one Session node in the graph.

    Returns ONE document per turn, each already shaped for the fixtures dir.
    """
    docs: list[dict] = []
    info = PROVIDERS[provider]
    rolling: list[dict] = []
    for i, prompt in enumerate(prompts):
        rolling.append({"role": "user", "content": prompt})

        trace_id = _new_trace_id()
        span_id = _new_span_id()
        start = time.time_ns()

        text, inp, out = info.call(model, prompt)  # stateless per-turn; good enough
        cost = budget.charge(info.cost_provider, cost_model, inp, out)
        end = time.time_ns()
        rolling.append({"role": "assistant", "content": text})

        span = {
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": "",
            "name": f"chat {model}",
            "kind": 3,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(end),
            "attributes": [
                _s("gen_ai.operation.name", "chat"),
                _s("gen_ai.provider.name", info.semconv_name),
                _s("gen_ai.request.model", model),
                _s("gen_ai.response.model", model),
                _s("gen_ai.conversation.id", conv_id),
                _i("gen_ai.usage.input_tokens", inp),
                _i("gen_ai.usage.output_tokens", out),
            ],
            "status": {"code": 1},
        }
        docs.append({
            "name": f"real_multi_turn_{conv_id}_turn{i}",
            "description": f"{info.semconv_name}/{model} turn {i+1}/{len(prompts)} — ${cost:.5f}",
            "otlp": {
                "resourceSpans": [
                    {
                        "resource": {"attributes": [_s("service.name", service_name)]},
                        "scopeSpans": [
                            {
                                "scope": {"name": "capture_real_traces", "version": "0.1.0"},
                                "spans": [span],
                            }
                        ],
                    }
                ]
            },
            "captured": {
                "cost_usd": cost,
                "input_tokens": inp,
                "output_tokens": out,
                "turn": i + 1,
                "response_preview": text[:200],
            },
        })
    return docs


def record_tool_call_trace(
    *,
    provider: str,
    model: str,
    cost_model: str,
    prompt: str,
    conv_id: str,
    agent_id: str,
    budget: BudgetGuard,
    service_name: str,
) -> dict:
    """invoke_agent → chat (tool_use) → execute_tool → chat (final).

    Mirrors fixtures/agent_with_tool.json. Exercises CALLED + INVOKED +
    PARENT_OF edges with real LLM output. OpenAI-SDK-compatible providers
    only (openai, azure_openai_v1, or any OpenAI-compatible endpoint like
    Ollama / Groq).
    """
    info = PROVIDERS[provider]
    trace_id = _new_trace_id()
    agent_span_id = _new_span_id()
    chat1_span_id = _new_span_id()
    tool_span_id = _new_span_id()
    chat2_span_id: Optional[str] = None

    t_start = time.time_ns()

    # Turn 1: ask the model with tools available.
    messages: list[dict] = [{"role": "user", "content": prompt}]
    resp1, inp1, out1 = _call_openai_with_tools(provider, model, messages)
    cost1 = budget.charge(info.cost_provider, cost_model, inp1, out1)
    t_chat1_end = time.time_ns()
    msg1 = resp1.choices[0].message
    tool_calls = msg1.tool_calls or []

    # Execute tool(s) locally, if any.
    tool_status = 1  # OTel OK
    tool_err: Optional[str] = None
    invoked_tool_name = "(none)"
    tool_result = ""
    if tool_calls:
        tc = tool_calls[0]
        invoked_tool_name = tc.function.name
        try:
            args = json.loads(tc.function.arguments or "{}")
            fn = TOOL_REGISTRY.get(invoked_tool_name)
            if fn is None:
                raise KeyError(f"unknown tool {invoked_tool_name!r}")
            tool_result = fn(args)
        except Exception as e:
            tool_status = 2  # ERROR
            tool_err = str(e)
            tool_result = f"error: {e}"

    t_tool_end = time.time_ns()

    # Turn 2: feed the tool result back so the model writes a final answer.
    final_text = msg1.content or ""
    inp2 = out2 = 0
    cost2 = 0.0
    chat2_span: Optional[dict] = None
    if tool_calls:
        messages.append({
            "role": "assistant",
            "content": msg1.content,
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in tool_calls
            ],
        })
        messages.append({
            "role": "tool",
            "tool_call_id": tool_calls[0].id,
            "content": tool_result,
        })
        t_chat2_start = time.time_ns()
        resp2, inp2, out2 = _call_openai_with_tools(provider, model, messages)
        cost2 = budget.charge(info.cost_provider, cost_model, inp2, out2)
        t_chat2_end = time.time_ns()
        final_text = resp2.choices[0].message.content or ""
        chat2_span_id = _new_span_id()
        chat2_span = {
            "traceId": trace_id,
            "spanId": chat2_span_id,
            "parentSpanId": agent_span_id,
            "name": f"chat {model}",
            "kind": 3,
            "startTimeUnixNano": str(t_chat2_start),
            "endTimeUnixNano": str(t_chat2_end),
            "attributes": [
                _s("gen_ai.operation.name", "chat"),
                _s("gen_ai.provider.name", info.semconv_name),
                _s("gen_ai.request.model", model),
                _s("gen_ai.response.model", model),
                _s("gen_ai.conversation.id", conv_id),
                _s("gen_ai.agent.id", agent_id),
                _i("gen_ai.usage.input_tokens", inp2),
                _i("gen_ai.usage.output_tokens", out2),
            ],
            "status": {"code": 1},
        }

    t_agent_end = time.time_ns()

    agent_span = {
        "traceId": trace_id,
        "spanId": agent_span_id,
        "parentSpanId": "",
        "name": f"invoke_agent {agent_id}",
        "kind": 1,
        "startTimeUnixNano": str(t_start),
        "endTimeUnixNano": str(t_agent_end),
        "attributes": [
            _s("gen_ai.operation.name", "invoke_agent"),
            _s("gen_ai.agent.id", agent_id),
            _s("gen_ai.agent.name", agent_id),
            _s("gen_ai.conversation.id", conv_id),
        ],
        "status": {"code": 1},
    }
    chat1_span = {
        "traceId": trace_id,
        "spanId": chat1_span_id,
        "parentSpanId": agent_span_id,
        "name": f"chat {model}",
        "kind": 3,
        "startTimeUnixNano": str(t_start + 1_000_000),
        "endTimeUnixNano": str(t_chat1_end),
        "attributes": [
            _s("gen_ai.operation.name", "chat"),
            _s("gen_ai.provider.name", info.semconv_name),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _s("gen_ai.conversation.id", conv_id),
            _s("gen_ai.agent.id", agent_id),
            _i("gen_ai.usage.input_tokens", inp1),
            _i("gen_ai.usage.output_tokens", out1),
        ],
        "status": {"code": 1},
    }
    tool_span = {
        "traceId": trace_id,
        "spanId": tool_span_id,
        "parentSpanId": agent_span_id,
        "name": f"execute_tool {invoked_tool_name}",
        "kind": 1,
        "startTimeUnixNano": str(t_chat1_end),
        "endTimeUnixNano": str(t_tool_end),
        "attributes": [
            _s("gen_ai.operation.name", "execute_tool"),
            _s("gen_ai.tool.name", invoked_tool_name),
            _s("gen_ai.conversation.id", conv_id),
            _s("gen_ai.agent.id", agent_id),
        ],
        "status": ({"code": tool_status, "message": tool_err} if tool_err else {"code": tool_status}),
    }

    spans = [agent_span, chat1_span, tool_span]
    if chat2_span is not None:
        spans.append(chat2_span)

    total_inp = inp1 + inp2
    total_out = out1 + out2
    total_cost = cost1 + cost2

    return {
        "name": f"real_tool_call_{conv_id}",
        "description": (
            f"{info.semconv_name}/{model} via {agent_id} "
            f"calling {invoked_tool_name} — ${total_cost:.5f}, "
            f"tokens {total_inp}/{total_out}"
        ),
        "otlp": {
            "resourceSpans": [
                {
                    "resource": {"attributes": [_s("service.name", service_name)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "capture_real_traces", "version": "0.1.0"},
                            "spans": spans,
                        }
                    ],
                }
            ]
        },
        "captured": {
            "cost_usd": total_cost,
            "input_tokens": total_inp,
            "output_tokens": total_out,
            "tool_called": invoked_tool_name,
            "tool_result_preview": tool_result[:200],
            "response_preview": final_text[:200],
        },
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def load_prompts(args: argparse.Namespace) -> list[str]:
    prompts: list[str] = list(args.prompt or [])
    if args.prompts:
        with open(args.prompts) as f:
            prompts.extend(line.strip() for line in f if line.strip() and not line.startswith("#"))
    return prompts


def main(argv: Optional[list[str]] = None) -> int:
    load_env()  # fills env from ./.env if present; shell wins
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--provider", choices=sorted(PROVIDERS), default="anthropic",
                   help="anthropic, openai, azure_openai, azure_inference")
    p.add_argument("--model", required=True,
                   help="model name (OpenAI/Anthropic) or deployment name (Azure)")
    p.add_argument("--cost-model", default=None,
                   help="model name used for the cost.PRICING lookup; "
                        "defaults to --model. Set this for Azure when the "
                        "deployment name doesn't match an entry in cost.PRICING "
                        "(e.g. --model my-prod-gpt4 --cost-model gpt-4o).")
    p.add_argument("--ignore-unknown-pricing", action="store_true",
                   help="don't abort when pricing is unknown; cost recorded as 0.0")
    p.add_argument(
        "--shape",
        choices=["chat", "agent_tool", "embeddings", "multi_turn", "tool_call"],
        default="chat",
        help="chat: single chat span. agent_tool: invoke_agent → chat. "
             "embeddings: /v1/embeddings. multi_turn: N chats sharing one "
             "conversation.id (different trace_ids). tool_call: agent + chat "
             "+ real tool execution + follow-up chat.",
    )
    p.add_argument("--prompt", action="append", help="inline prompt; may repeat")
    p.add_argument("--prompts", type=Path, help="path to newline-delimited prompt file")
    p.add_argument("--budget-usd", type=float, required=True, help="hard cap; stops before exceeding")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--service-name", default="real-capture")
    p.add_argument("--agent-id", default="research-agent",
                   help="used when --shape is agent_tool or tool_call")
    p.add_argument("--shared-conv-id", default=None,
                   help="used by --shape multi_turn: all turns share this id")
    args = p.parse_args(argv)

    prompts = load_prompts(args)
    if not prompts:
        print("no prompts provided; pass --prompt or --prompts", file=sys.stderr)
        return 2

    cost_model = args.cost_model or args.model
    args.output_dir.mkdir(parents=True, exist_ok=True)
    budget = BudgetGuard(cap_usd=args.budget_usd, allow_unknown=args.ignore_unknown_pricing)
    info = PROVIDERS[args.provider]

    print(f"Provider: {args.provider} (semconv={info.semconv_name}) | "
          f"Model: {args.model} | Cost-model: {cost_model} | Shape: {args.shape}",
          file=sys.stderr)
    print(f"Prompts: {len(prompts)} | Budget cap: ${args.budget_usd:.2f}", file=sys.stderr)
    print(f"Output:  {args.output_dir}", file=sys.stderr)
    print("-" * 60, file=sys.stderr)

    captured = 0

    # multi_turn consumes the whole prompt list in one call; every other
    # shape runs once per prompt.
    if args.shape == "multi_turn":
        shared = args.shared_conv_id or f"real-multi-{int(time.time())}"
        try:
            docs = record_multi_turn_trace(
                provider=args.provider, model=args.model, cost_model=cost_model,
                prompts=prompts, conv_id=shared,
                budget=budget, service_name=args.service_name,
            )
        except BudgetExceeded as e:
            print(f"\n[stopped] {e}", file=sys.stderr)
            docs = []
        for doc in docs:
            path = args.output_dir / f"{doc['name']}.json"
            path.write_text(json.dumps(doc, indent=2))
            captured += 1
            c = doc["captured"]
            print(f"  {path.name} — ${c['cost_usd']:.5f} (tokens "
                  f"{c['input_tokens']}/{c['output_tokens']}) | "
                  f"cumulative ${budget.spent:.4f}", file=sys.stderr)
    else:
        for i, prompt in enumerate(prompts):
            conv_id = f"real-{int(time.time())}-{i}"
            try:
                if args.shape == "chat":
                    doc = record_chat_trace(
                        provider=args.provider, model=args.model, cost_model=cost_model,
                        prompt=prompt, conv_id=conv_id,
                        budget=budget, service_name=args.service_name,
                    )
                elif args.shape == "agent_tool":
                    doc = record_agent_tool_trace(
                        provider=args.provider, model=args.model, cost_model=cost_model,
                        prompt=prompt, conv_id=conv_id, agent_id=args.agent_id,
                        budget=budget, service_name=args.service_name,
                    )
                elif args.shape == "embeddings":
                    doc = record_embeddings_trace(
                        provider=args.provider, model=args.model, cost_model=cost_model,
                        prompt=prompt, conv_id=conv_id,
                        budget=budget, service_name=args.service_name,
                    )
                elif args.shape == "tool_call":
                    doc = record_tool_call_trace(
                        provider=args.provider, model=args.model, cost_model=cost_model,
                        prompt=prompt, conv_id=conv_id, agent_id=args.agent_id,
                        budget=budget, service_name=args.service_name,
                    )
                else:
                    raise AssertionError(f"unreachable shape: {args.shape}")
            except BudgetExceeded as e:
                print(f"\n[stopped] {e}", file=sys.stderr)
                break

            path = args.output_dir / f"{doc['name']}.json"
            path.write_text(json.dumps(doc, indent=2))
            captured += 1
            c = doc["captured"]
            print(f"  [{i+1}/{len(prompts)}] {path.name} — ${c['cost_usd']:.5f} "
                  f"(tokens {c['input_tokens']}/{c['output_tokens']}) | "
                  f"cumulative ${budget.spent:.4f}",
                  file=sys.stderr)

    print("-" * 60, file=sys.stderr)
    print(f"Captured: {captured} | Total spend: ${budget.spent:.4f}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
