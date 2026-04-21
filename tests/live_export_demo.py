#!/usr/bin/env python3
"""Minimal live-export demo.

Instruments a tiny multi-agent flow using the OTel SDK directly (no LLM
calls, no framework), streams it through `Neo4jGenAIExporter`, and prints
Cypher you can paste into the Neo4j browser to see the result.

Usage
-----
    export NEO4J_URI=bolt://localhost:17687
    export NEO4J_PASSWORD=testtest
    python tests/live_export_demo.py

This is the scaffold you'd plug a real LangGraph / CrewAI / LlamaIndex
call-site into — swap the manual spans for framework-emitted ones.
"""
from __future__ import annotations

import os
import sys
import uuid

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from otel_genai_graph.exporter import Neo4jGenAIExporter  # noqa: E402
from otel_genai_graph.neo4j_sink import Neo4jSink  # noqa: E402


def main() -> int:
    uri = os.environ.get("NEO4J_URI", "bolt://localhost:17687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "testtest")
    database = os.environ.get("NEO4J_DATABASE", "neo4j")

    sink = Neo4jSink(uri, user, password, database)
    sink.connect()
    sink.ensure_schema()

    provider = TracerProvider(
        resource=Resource.create({"service.name": "live-export-demo"})
    )
    provider.add_span_processor(BatchSpanProcessor(Neo4jGenAIExporter(sink)))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("live.demo", "0.1.0")

    conv_id = f"demo-{uuid.uuid4().hex[:8]}"
    print(f"emitting spans for conversation {conv_id!r}", file=sys.stderr)

    with tracer.start_as_current_span(
        "invoke_agent orchestrator",
        kind=SpanKind.INTERNAL,
        attributes={
            "gen_ai.operation.name":  "invoke_agent",
            "gen_ai.agent.id":        "orchestrator",
            "gen_ai.agent.name":      "orchestrator",
            "gen_ai.conversation.id": conv_id,
        },
    ):
        with tracer.start_as_current_span(
            "invoke_agent researcher",
            kind=SpanKind.INTERNAL,
            attributes={
                "gen_ai.operation.name":  "invoke_agent",
                "gen_ai.agent.id":        "researcher",
                "gen_ai.agent.name":      "researcher",
                "gen_ai.conversation.id": conv_id,
            },
        ):
            with tracer.start_as_current_span(
                "chat claude-haiku-4-5",
                kind=SpanKind.CLIENT,
                attributes={
                    "gen_ai.operation.name":      "chat",
                    "gen_ai.provider.name":       "anthropic",
                    "gen_ai.request.model":       "claude-haiku-4-5",
                    "gen_ai.response.model":      "claude-haiku-4-5",
                    "gen_ai.conversation.id":     conv_id,
                    "gen_ai.agent.id":            "researcher",
                    "gen_ai.usage.input_tokens":  250,
                    "gen_ai.usage.output_tokens": 180,
                },
            ):
                pass

            with tracer.start_as_current_span(
                "execute_tool web_search",
                kind=SpanKind.INTERNAL,
                attributes={
                    "gen_ai.operation.name":  "execute_tool",
                    "gen_ai.tool.name":       "web_search",
                    "gen_ai.conversation.id": conv_id,
                    "gen_ai.agent.id":        "researcher",
                },
            ):
                pass

    # Flush and shut down — BatchSpanProcessor defers writes until here.
    provider.shutdown()

    print(f"✓ written. Try in Neo4j browser ({uri.replace('bolt://', 'http://').replace(':17687', ':17474').replace(':7687', ':7474')}):",
          file=sys.stderr)
    print(file=sys.stderr)
    print(f"  MATCH p = (s:Session {{id:'{conv_id}'}})-[:CONTAINS]->(o:Operation) RETURN p",
          file=sys.stderr)
    print(f"  MATCH p = (a:Agent)-[:DELEGATED_TO]->(b:Agent) RETURN p",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
