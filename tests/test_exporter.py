"""Tests for the live OTel SpanExporter.

Unit tests: OTLP/JSON conversion of in-memory SDK spans.
Integration: real TracerProvider → exporter → live Neo4j → query back.
Gated on `NEO4J_URI` (same convention as test_neo4j_sink.py).
"""
from __future__ import annotations

import os

import pytest
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode

from otel_genai_graph.exporter import (
    Neo4jGenAIExporter,
    _attr_value_to_otlp,
    group_spans_to_resource_spans,
    sdk_span_to_otlp,
)
from otel_genai_graph.mapper import map_spans
from otel_genai_graph.neo4j_sink import Neo4jSink


# ---------------------------------------------------------------------------
# Attribute value encoding
# ---------------------------------------------------------------------------

def test_attr_encoding_primitives() -> None:
    assert _attr_value_to_otlp("hi") == {"stringValue": "hi"}
    assert _attr_value_to_otlp(42) == {"intValue": "42"}
    assert _attr_value_to_otlp(3.14) == {"doubleValue": 3.14}
    assert _attr_value_to_otlp(True) == {"boolValue": True}
    # bool must not be treated as int despite being a subclass
    assert _attr_value_to_otlp(False) == {"boolValue": False}


def test_attr_encoding_array() -> None:
    assert _attr_value_to_otlp(["a", "b"]) == {
        "arrayValue": {"values": [{"stringValue": "a"}, {"stringValue": "b"}]}
    }


# ---------------------------------------------------------------------------
# End-to-end: SDK span → OTLP dict → mapper → Graph
# ---------------------------------------------------------------------------

def _build_provider_and_exporter() -> tuple[TracerProvider, InMemorySpanExporter]:
    resource = Resource.create({"service.name": "exporter-test"})
    provider = TracerProvider(resource=resource)
    mem = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(mem))
    return provider, mem


def test_sdk_span_converts_to_otlp_shape_the_mapper_understands() -> None:
    provider, mem = _build_provider_and_exporter()
    tracer = provider.get_tracer("exporter.test", "0.1.0")

    with tracer.start_as_current_span(
        "chat claude-sonnet-4-5",
        kind=SpanKind.CLIENT,
        attributes={
            "gen_ai.operation.name":     "chat",
            "gen_ai.provider.name":      "anthropic",
            "gen_ai.request.model":      "claude-sonnet-4-5",
            "gen_ai.response.model":     "claude-sonnet-4-5",
            "gen_ai.conversation.id":    "exporter-conv-1",
            "gen_ai.usage.input_tokens":  120,
            "gen_ai.usage.output_tokens":  80,
        },
    ) as span:
        span.set_status(Status(StatusCode.OK))

    provider.force_flush()
    spans = mem.get_finished_spans()
    assert len(spans) == 1

    otlp_span = sdk_span_to_otlp(spans[0])
    assert otlp_span["name"] == "chat claude-sonnet-4-5"
    assert otlp_span["kind"] == SpanKind.CLIENT.value
    assert otlp_span["status"]["code"] == StatusCode.OK.value
    assert len(otlp_span["traceId"]) == 32 and len(otlp_span["spanId"]) == 16
    assert otlp_span["parentSpanId"] == ""
    # int tokens became strings per protobuf-JSON mapping
    int_attrs = {a["key"]: a["value"] for a in otlp_span["attributes"]}
    assert int_attrs["gen_ai.usage.input_tokens"] == {"intValue": "120"}

    # Round-trip through the mapper — this is the canonical integration point.
    resource_spans = group_spans_to_resource_spans(spans)
    graph = map_spans(resource_spans)
    assert graph.node_count("Session") == 1
    assert graph.node_count("Model") == 1
    assert graph.node_count("Operation") == 1
    assert graph.edge_count("CONTAINS") == 1
    assert graph.edge_count("EXECUTED") == 1


def test_parent_child_produces_parent_of_edge() -> None:
    provider, mem = _build_provider_and_exporter()
    tracer = provider.get_tracer("exporter.test", "0.1.0")

    with tracer.start_as_current_span(
        "invoke_agent researcher",
        kind=SpanKind.INTERNAL,
        attributes={
            "gen_ai.operation.name":  "invoke_agent",
            "gen_ai.agent.id":        "researcher",
            "gen_ai.conversation.id": "parent-test",
        },
    ):
        with tracer.start_as_current_span(
            "chat claude",
            kind=SpanKind.CLIENT,
            attributes={
                "gen_ai.operation.name":  "chat",
                "gen_ai.provider.name":   "anthropic",
                "gen_ai.request.model":   "claude-haiku-4-5",
                "gen_ai.response.model":  "claude-haiku-4-5",
                "gen_ai.conversation.id": "parent-test",
                "gen_ai.agent.id":        "researcher",
            },
        ):
            pass

    provider.force_flush()
    spans = mem.get_finished_spans()
    resource_spans = group_spans_to_resource_spans(spans)
    graph = map_spans(resource_spans)
    assert graph.edge_count("PARENT_OF") == 1
    assert graph.edge_count("INVOKED") == 1
    assert graph.edge_count("EXECUTED") == 1


def test_error_status_propagates() -> None:
    provider, mem = _build_provider_and_exporter()
    tracer = provider.get_tracer("exporter.test", "0.1.0")

    with tracer.start_as_current_span(
        "invoke_agent ops",
        attributes={
            "gen_ai.operation.name":  "invoke_agent",
            "gen_ai.agent.id":        "ops-agent",
            "gen_ai.conversation.id": "err-test",
        },
    ) as agent_span:
        with tracer.start_as_current_span(
            "execute_tool db_query",
            attributes={
                "gen_ai.operation.name":  "execute_tool",
                "gen_ai.tool.name":       "db_query",
                "gen_ai.conversation.id": "err-test",
            },
        ) as tool_span:
            tool_span.set_status(Status(StatusCode.ERROR, "timeout"))
        # agent span stays OK — mapper should propagate ERROR up
        _ = agent_span

    provider.force_flush()
    spans = mem.get_finished_spans()
    resource_spans = group_spans_to_resource_spans(spans)
    graph = map_spans(resource_spans)

    err_ops = [op for op in graph.nodes_of("Operation") if op.status == "ERROR"]  # type: ignore[attr-defined]
    assert len(err_ops) == 2, f"expected 2 ERROR ops (tool + ancestor agent), got {len(err_ops)}"


# ---------------------------------------------------------------------------
# Live Neo4j integration — gated on NEO4J_URI
# ---------------------------------------------------------------------------

_NEO4J_URI = os.environ.get("NEO4J_URI")
skip_if_no_neo4j = pytest.mark.skipif(
    _NEO4J_URI is None,
    reason="set NEO4J_URI to run the live-exporter test",
)


@skip_if_no_neo4j
def test_live_exporter_end_to_end() -> None:
    """Wire a real TracerProvider → Neo4jGenAIExporter → live Neo4j, then query."""
    sink = Neo4jSink(
        _NEO4J_URI,  # type: ignore[arg-type]
        os.environ.get("NEO4J_USER", "neo4j"),
        os.environ.get("NEO4J_PASSWORD", "testtest"),
        os.environ.get("NEO4J_DATABASE", "neo4j"),
    )
    sink.connect()
    sink.ensure_schema()

    # Clean slate for a clean assertion.
    with sink._driver.session(database=sink.database) as sess:  # type: ignore[union-attr]
        sess.run("MATCH (n) DETACH DELETE n")

    provider = TracerProvider(resource=Resource.create({"service.name": "live-exporter-test"}))
    exporter = Neo4jGenAIExporter(sink)
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("live.test", "0.1.0")

    try:
        with tracer.start_as_current_span(
            "invoke_agent researcher",
            kind=SpanKind.INTERNAL,
            attributes={
                "gen_ai.operation.name":  "invoke_agent",
                "gen_ai.agent.id":        "researcher",
                "gen_ai.conversation.id": "live-conv-1",
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
                    "gen_ai.conversation.id":     "live-conv-1",
                    "gen_ai.agent.id":            "researcher",
                    "gen_ai.usage.input_tokens":  200,
                    "gen_ai.usage.output_tokens": 100,
                },
            ):
                pass

        provider.force_flush()

        with sink._driver.session(database=sink.database) as sess:  # type: ignore[union-attr]
            nodes = sess.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            edges = sess.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
            session_row = sess.run(
                "MATCH (s:Session {id:'live-conv-1'})-[:CONTAINS]->(o:Operation) "
                "RETURN count(o) AS c"
            ).single()
            executed_row = sess.run(
                "MATCH (:Operation)-[:EXECUTED]->(m:Model) "
                "WHERE m.provider='anthropic' AND m.name='claude-haiku-4-5' "
                "RETURN count(m) AS c"
            ).single()
            resource_row = sess.run(
                "MATCH (r:Resource {service_name:'live-exporter-test'}) RETURN count(r) AS c"
            ).single()
            ops_with_service = sess.run(
                "MATCH (o:Operation) WHERE o.service_name = 'live-exporter-test' "
                "RETURN count(o) AS c"
            ).single()

        # expected graph: 1 Session + 1 Agent + 1 Model + 2 Operations + 1 Resource = 6 nodes
        # edges: CONTAINS×2 + INVOKED + EXECUTED + PARENT_OF = 5
        assert nodes == 6
        assert edges == 5
        assert session_row["c"] == 2
        assert executed_row["c"] == 1
        # Resource node is emitted from the TracerProvider's resource attribute,
        # and every Operation carries service_name as a denormalised FK.
        assert resource_row["c"] == 1
        assert ops_with_service["c"] == 2
    finally:
        provider.shutdown()  # closes the sink as a side-effect via exporter.shutdown()
