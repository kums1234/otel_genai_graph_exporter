"""Legacy-attribute compatibility tests.

Peer implementations (e.g. `opentelemetry-instrumentation-google-genai` as of
0.7b0) emit spans with pre-v1.37 attribute shapes:

  * `gen_ai.system` instead of `gen_ai.provider.name`
  * `gen_ai.operation.name = "generate_content"` instead of `"chat"`
  * Older Anthropic wrappers use `operation.name = "message"`

The mapper canonicalises these at read time so the resulting graph looks the
same regardless of which SDK produced the span. These tests pin that
behaviour.
"""
from __future__ import annotations

import json
from pathlib import Path

from otel_genai_graph.mapper import map_spans


def _build(
    *,
    op_name: str,
    provider_key: str,
    provider_value: str,
    model: str = "gemini-2.5-flash",
    input_tokens: int = 8,
    output_tokens: int = 51,
    extra_attrs: list[tuple[str, str]] | None = None,
) -> dict:
    """Build a single-span OTLP resourceSpans blob with specified legacy shapes."""
    attrs = [
        {"key": "gen_ai.operation.name", "value": {"stringValue": op_name}},
        {"key": provider_key,            "value": {"stringValue": provider_value}},
        {"key": "gen_ai.request.model",  "value": {"stringValue": model}},
        {"key": "gen_ai.usage.input_tokens",  "value": {"intValue": str(input_tokens)}},
        {"key": "gen_ai.usage.output_tokens", "value": {"intValue": str(output_tokens)}},
    ]
    if extra_attrs:
        for k, v in extra_attrs:
            attrs.append({"key": k, "value": {"stringValue": v}})
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "legacy-test"}}]},
                "scopeSpans": [
                    {
                        "scope": {"name": "legacy-test", "version": "0"},
                        "spans": [
                            {
                                "traceId": "a" * 32,
                                "spanId":  "b" * 16,
                                "parentSpanId": "",
                                "name":  f"{op_name} {model}",
                                "kind":  0,
                                "startTimeUnixNano": "1737000000000000000",
                                "endTimeUnixNano":   "1737000000500000000",
                                "attributes": attrs,
                                "status": {"code": 0},
                            }
                        ],
                    }
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# Google GenAI instrumentor shape
# ---------------------------------------------------------------------------

def test_gemini_legacy_shape_canonicalises() -> None:
    """`generate_content` + `gen_ai.system=gemini` → `chat` + `gcp.gen_ai`."""
    graph = map_spans(
        _build(
            op_name="generate_content",
            provider_key="gen_ai.system",
            provider_value="gemini",
        )["resourceSpans"]
    )
    assert graph.node_count("Operation") == 1
    assert graph.node_count("Model") == 1
    assert graph.edge_count("EXECUTED") == 1

    op = graph.nodes_of("Operation")[0]
    assert op.type == "chat", f"expected canonical 'chat', got {op.type!r}"

    model = graph.nodes_of("Model")[0]
    assert model.provider == "gcp.gen_ai"
    assert model.name == "gemini-2.5-flash"


def test_anthropic_legacy_message_alias() -> None:
    graph = map_spans(
        _build(
            op_name="message",
            provider_key="gen_ai.provider.name",
            provider_value="anthropic",
            model="claude-sonnet-4-5",
        )["resourceSpans"]
    )
    op = graph.nodes_of("Operation")[0]
    assert op.type == "chat"
    assert graph.edge_count("EXECUTED") == 1


def test_text_generation_alias() -> None:
    graph = map_spans(
        _build(
            op_name="text_generation",
            provider_key="gen_ai.provider.name",
            provider_value="openai",
            model="gpt-3.5-turbo-instruct",
        )["resourceSpans"]
    )
    op = graph.nodes_of("Operation")[0]
    assert op.type == "text_completion"
    assert graph.edge_count("EXECUTED") == 1


# ---------------------------------------------------------------------------
# Provider-key fallback
# ---------------------------------------------------------------------------

def test_provider_name_beats_system_when_both_present() -> None:
    """If a span has both keys, v1.37 `gen_ai.provider.name` wins."""
    spans = _build(
        op_name="chat",
        provider_key="gen_ai.provider.name",
        provider_value="anthropic",
    )
    # also inject a conflicting legacy key
    spans["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"].append(
        {"key": "gen_ai.system", "value": {"stringValue": "gemini"}}
    )
    graph = map_spans(spans["resourceSpans"])
    model = graph.nodes_of("Model")[0]
    assert model.provider == "anthropic"


def test_azure_legacy_system_alias() -> None:
    """`gen_ai.system=az.ai.openai` → `azure.ai.openai`."""
    graph = map_spans(
        _build(
            op_name="chat",
            provider_key="gen_ai.system",
            provider_value="az.ai.openai",
            model="gpt-4o",
        )["resourceSpans"]
    )
    model = graph.nodes_of("Model")[0]
    assert model.provider == "azure.ai.openai"


def test_unknown_provider_passes_through() -> None:
    """We don't rewrite values we don't know — that would be worse than leaving them."""
    graph = map_spans(
        _build(
            op_name="chat",
            provider_key="gen_ai.system",
            provider_value="some_new_vendor",
        )["resourceSpans"]
    )
    model = graph.nodes_of("Model")[0]
    assert model.provider == "some_new_vendor"


# ---------------------------------------------------------------------------
# Conversation-id / Session resolution
# ---------------------------------------------------------------------------

def _session_id_only(attr_key: str, value: str = "sess-123") -> dict:
    """One span carrying a specific session-id-equivalent attribute."""
    return _build(
        op_name="chat",
        provider_key="gen_ai.provider.name",
        provider_value="anthropic",
        model="claude-haiku-4-5",
        extra_attrs=[(attr_key, value)],
    )


def test_canonical_conversation_id_makes_session() -> None:
    graph = map_spans(_session_id_only("gen_ai.conversation.id", "canon-1")["resourceSpans"])
    sessions = graph.nodes_of("Session")
    assert len(sessions) == 1 and sessions[0].id == "canon-1"


def test_openinference_session_id_fallback() -> None:
    graph = map_spans(_session_id_only("session.id", "oi-1")["resourceSpans"])
    sessions = graph.nodes_of("Session")
    assert len(sessions) == 1 and sessions[0].id == "oi-1"


def test_langsmith_session_id_fallback() -> None:
    graph = map_spans(
        _session_id_only("langsmith.trace.session_id", "ls-1")["resourceSpans"]
    )
    assert graph.node_count("Session") == 1
    assert graph.nodes_of("Session")[0].id == "ls-1"


def test_traceloop_association_session_id_fallback() -> None:
    graph = map_spans(
        _session_id_only(
            "traceloop.association.properties.session_id", "tl-sess-1"
        )["resourceSpans"]
    )
    assert graph.node_count("Session") == 1
    assert graph.nodes_of("Session")[0].id == "tl-sess-1"


def test_traceloop_thread_id_fallback() -> None:
    graph = map_spans(
        _session_id_only(
            "traceloop.association.properties.thread_id", "tl-thread-42"
        )["resourceSpans"]
    )
    assert graph.nodes_of("Session")[0].id == "tl-thread-42"


def test_canonical_wins_when_multiple_keys_present() -> None:
    """OTel compatibility: `gen_ai.conversation.id` MUST beat every fallback.

    This is the spec-compatibility anchor — a canonical emitter's session
    identity never gets overridden by anything else on the span.
    """
    spans = _build(
        op_name="chat",
        provider_key="gen_ai.provider.name",
        provider_value="anthropic",
        extra_attrs=[
            ("gen_ai.conversation.id",                         "canonical-winner"),
            ("session.id",                                     "openinference-loser"),
            ("langsmith.trace.session_id",                     "langsmith-loser"),
            ("traceloop.association.properties.session_id",    "traceloop-loser"),
            ("traceloop.association.properties.thread_id",     "thread-loser"),
        ],
    )
    graph = map_spans(spans["resourceSpans"])
    sessions = graph.nodes_of("Session")
    assert len(sessions) == 1
    assert sessions[0].id == "canonical-winner"


def test_no_session_emitted_when_all_keys_absent() -> None:
    """No fallback to trace_id — matches research-agent's explicit guidance."""
    graph = map_spans(
        _build(
            op_name="chat",
            provider_key="gen_ai.provider.name",
            provider_value="anthropic",
        )["resourceSpans"]
    )
    assert graph.node_count("Session") == 0
    assert graph.edge_count("CONTAINS") == 0


def test_fallback_priority_order() -> None:
    """If canonical absent, first-defined fallback wins over later ones."""
    spans = _build(
        op_name="chat",
        provider_key="gen_ai.provider.name",
        provider_value="anthropic",
        extra_attrs=[
            ("session.id",                                  "openinference-wins"),
            ("langsmith.trace.session_id",                  "langsmith-loses"),
            ("traceloop.association.properties.session_id", "traceloop-loses"),
        ],
    )
    graph = map_spans(spans["resourceSpans"])
    assert graph.nodes_of("Session")[0].id == "openinference-wins"


# ---------------------------------------------------------------------------
# End-to-end: the instrumentor fixture must now yield a Model + EXECUTED edge
# ---------------------------------------------------------------------------

def test_public_gemini_instrumentor_fixture_produces_executed_edge() -> None:
    public_dir = Path(__file__).parent / "fixtures" / "public" / "google-genai"
    files = sorted(public_dir.glob("*.json"))
    if not files:
        import pytest
        pytest.skip("no google-genai instrumentor fixture captured yet")

    data = json.loads(files[0].read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])

    # Before Option A: 1 node, 0 edges (mapper couldn't match generate_content).
    # After:           2 nodes (Operation + Model), 1 edge (EXECUTED).
    # No Session because this SDK doesn't emit gen_ai.conversation.id.
    assert graph.node_count("Operation") == 1
    assert graph.node_count("Model") == 1, (
        "Model node missing — legacy compat didn't activate for the real fixture"
    )
    assert graph.edge_count("EXECUTED") == 1
