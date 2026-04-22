#!/usr/bin/env python3
"""Capture OTel spans emitted by an *upstream* instrumentor.

Complements `capture_real_traces.py`:
  * `capture_real_traces.py` builds spans we author ourselves — useful for
    testing the mapper against our own interpretation of v1.37.
  * `capture_with_instrumentor.py` lets a third-party instrumentor emit
    the spans, then captures them with an in-memory exporter. That's how
    we get *peer-implementation* OTLP/JSON into `tests/fixtures/public/`
    for cross-implementation conformance testing.

Supported instrumentors
-----------------------
  openai-v2     — `opentelemetry-instrumentation-openai-v2`   (Python
                  Contrib; emits v1.37 `gen_ai.*` for the OpenAI SDK
                  family, incl. AzureOpenAI / OpenAI-compatible base_urls).
  google-genai  — `opentelemetry-instrumentation-google-genai` (Python
                  Contrib; emits v1.37 `gen_ai.*` for the `google-genai`
                  SDK, so AI Studio and Vertex AI paths both work).

Add more by extending `INSTRUMENTORS` below.

Usage
-----
  # via Azure OpenAI v1 (Foundry) — requires AZURE_OPENAI_* env vars
  python tests/capture_with_instrumentor.py \\
      --instrumentor openai-v2 \\
      --provider azure_openai_v1 \\
      --model gpt-oss-120b --cost-model gpt-oss-120b \\
      --ignore-unknown-pricing \\
      --prompt "Explain quicksort" \\
      --budget-usd 0.10 \\
      --output-dir tests/fixtures/public/openai-v2

  # plain OpenAI
  python tests/capture_with_instrumentor.py \\
      --instrumentor openai-v2 --provider openai \\
      --model gpt-4o-mini \\
      --prompt "Hello" --budget-usd 0.05 \\
      --output-dir tests/fixtures/public/openai-v2

  # Google AI Studio (free tier) — matching instrumentor + matching provider
  export GEMINI_API_KEY=...
  pip install google-genai opentelemetry-instrumentation-google-genai
  python tests/capture_with_instrumentor.py \\
      --instrumentor google-genai --provider google \\
      --model gemini-2.5-flash \\
      --prompt "Hello" --budget-usd 0.05 \\
      --output-dir tests/fixtures/public/google-genai

Output is one JSON per call under --output-dir, shaped like every other
fixture in the repo (`{"otlp": {"resourceSpans": [...]}, "captured": ...,
"provenance": ...}`), so the mapper and invariant tests pick them up
automatically.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

# Make the capture_real_traces module importable for provider reuse.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from capture_real_traces import (  # type: ignore[import-not-found]
    PROVIDERS,
    BudgetGuard,
    BudgetExceeded,
)

from otel_genai_graph._env import load_env
from otel_genai_graph.exporter import group_spans_to_resource_spans

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


# ---------------------------------------------------------------------------
# Instrumentor registry
# ---------------------------------------------------------------------------

def _enable_openai_v2() -> dict:
    """Enable the upstream OpenAI v2 instrumentor and return provenance info."""
    from opentelemetry.instrumentation.openai_v2 import OpenAIInstrumentor
    import importlib.metadata as md

    OpenAIInstrumentor().instrument()
    try:
        version = md.version("opentelemetry-instrumentation-openai-v2")
    except md.PackageNotFoundError:
        version = "unknown"
    return {
        "name": "opentelemetry-instrumentation-openai-v2",
        "version": version,
        "source_repo": "open-telemetry/opentelemetry-python-contrib",
        "license": "Apache-2.0",
    }


def _disable_openai_v2() -> None:
    from opentelemetry.instrumentation.openai_v2 import OpenAIInstrumentor
    OpenAIInstrumentor().uninstrument()


def _enable_google_genai() -> dict:
    """Enable the upstream google-genai instrumentor and return provenance info."""
    from opentelemetry.instrumentation.google_genai import GoogleGenAiSdkInstrumentor
    import importlib.metadata as md

    GoogleGenAiSdkInstrumentor().instrument()
    try:
        version = md.version("opentelemetry-instrumentation-google-genai")
    except md.PackageNotFoundError:
        version = "unknown"
    return {
        "name": "opentelemetry-instrumentation-google-genai",
        "version": version,
        "source_repo": "open-telemetry/opentelemetry-python-contrib",
        "license": "Apache-2.0",
    }


def _disable_google_genai() -> None:
    from opentelemetry.instrumentation.google_genai import GoogleGenAiSdkInstrumentor
    GoogleGenAiSdkInstrumentor().uninstrument()


INSTRUMENTORS: dict[str, tuple[Callable[[], dict], Callable[[], None]]] = {
    "openai-v2":    (_enable_openai_v2,    _disable_openai_v2),
    "google-genai": (_enable_google_genai, _disable_google_genai),
}


# ---------------------------------------------------------------------------
# Span capture
# ---------------------------------------------------------------------------

def _setup_provider(service_name: str) -> tuple[TracerProvider, InMemorySpanExporter]:
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    mem = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(mem))
    trace.set_tracer_provider(provider)
    return provider, mem


def _resource_spans_from_memory(exporter: InMemorySpanExporter) -> list[dict]:
    spans = exporter.get_finished_spans()
    return group_spans_to_resource_spans(spans)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def load_prompts(args: argparse.Namespace) -> list[str]:
    prompts: list[str] = list(args.prompt or [])
    if args.prompts:
        with open(args.prompts) as f:
            prompts.extend(
                line.strip() for line in f if line.strip() and not line.startswith("#")
            )
    return prompts


def main(argv: Optional[list[str]] = None) -> int:
    load_env()  # fills env from ./.env if present; shell wins
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--instrumentor", choices=sorted(INSTRUMENTORS), required=True)
    p.add_argument("--provider", choices=sorted(PROVIDERS), required=True,
                   help="anthropic, openai, azure_openai, azure_openai_v1, azure_inference")
    p.add_argument("--model", required=True,
                   help="model name (OpenAI/Anthropic) or deployment name (Azure)")
    p.add_argument("--cost-model", default=None,
                   help="override model name for cost lookup (Azure deployments)")
    p.add_argument("--ignore-unknown-pricing", action="store_true")
    p.add_argument("--prompt", action="append", help="inline prompt; may repeat")
    p.add_argument("--prompts", type=Path, help="path to newline-delimited prompts")
    p.add_argument("--budget-usd", type=float, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--service-name", default="instrumentor-capture")
    args = p.parse_args(argv)

    prompts = load_prompts(args)
    if not prompts:
        print("no prompts — pass --prompt or --prompts", file=sys.stderr)
        return 2

    cost_model = args.cost_model or args.model
    args.output_dir.mkdir(parents=True, exist_ok=True)
    budget = BudgetGuard(cap_usd=args.budget_usd, allow_unknown=args.ignore_unknown_pricing)
    provider_info = PROVIDERS[args.provider]

    enable_fn, disable_fn = INSTRUMENTORS[args.instrumentor]

    # Set up one TracerProvider for the whole run; each prompt gets its own
    # in-memory exporter so their spans don't cross-contaminate.
    print(
        f"Instrumentor: {args.instrumentor} | Provider: {args.provider} "
        f"(semconv={provider_info.semconv_name}) | Model: {args.model}",
        file=sys.stderr,
    )
    print(f"Prompts: {len(prompts)} | Budget cap: ${args.budget_usd:.2f}", file=sys.stderr)
    print(f"Output:  {args.output_dir}", file=sys.stderr)
    print("-" * 64, file=sys.stderr)

    provenance_instrumentor = enable_fn()
    captured = 0
    try:
        for i, prompt in enumerate(prompts):
            provider, mem = _setup_provider(args.service_name)
            conv_id = f"instr-{int(time.time())}-{uuid.uuid4().hex[:6]}-{i}"

            try:
                text, inp, out = provider_info.call(args.model, prompt)
                cost = budget.charge(provider_info.cost_provider, cost_model, inp, out)
            except BudgetExceeded as e:
                print(f"\n[stopped] {e}", file=sys.stderr)
                break

            provider.force_flush()
            provider.shutdown()
            resource_spans = _resource_spans_from_memory(mem)

            span_count = sum(
                len(ss.get("spans", []))
                for rs in resource_spans
                for ss in rs.get("scopeSpans", [])
            )

            doc = {
                "name": f"instrumentor_{args.instrumentor}_{conv_id}",
                "description": (
                    f"captured by {provenance_instrumentor['name']}=="
                    f"{provenance_instrumentor['version']} calling "
                    f"{provider_info.semconv_name}/{args.model}"
                ),
                "otlp": {"resourceSpans": resource_spans},
                "captured": {
                    "cost_usd": cost,
                    "input_tokens": inp,
                    "output_tokens": out,
                    "response_preview": text[:200],
                    "span_count": span_count,
                },
                "provenance": {
                    "instrumentor": provenance_instrumentor,
                    "api_provider": args.provider,
                    "semconv_provider": provider_info.semconv_name,
                    "api_model_or_deployment": args.model,
                    "cost_model": cost_model,
                    "captured_at_unix_ns": time.time_ns(),
                },
            }
            path = args.output_dir / f"{doc['name']}.json"
            path.write_text(json.dumps(doc, indent=2))
            captured += 1
            print(
                f"  [{i+1}/{len(prompts)}] {path.name} "
                f"— {span_count} span(s) captured, ${cost:.5f} "
                f"(tokens {inp}/{out}) | cumulative ${budget.spent:.4f}",
                file=sys.stderr,
            )
    finally:
        # Best-effort uninstrument. Some Python Contrib instrumentors
        # (e.g. google-genai 1.0a0) crash on uninstrument() with an
        # internal assertion — not fatal; captured spans are already on
        # disk before we reach here.
        try:
            disable_fn()
        except Exception as e:  # pragma: no cover
            print(f"[warn] uninstrument failed (harmless): {e}", file=sys.stderr)

    print("-" * 64, file=sys.stderr)
    print(
        f"Captured: {captured}/{len(prompts)} | Total spend: ${budget.spent:.4f}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
