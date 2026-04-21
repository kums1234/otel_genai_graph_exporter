"""OTLP GenAI spans → Graph.

The contract lives in `docs/mapping.md`; this module is its implementation.
Tests in `tests/test_mapper.py` are the enforcement.

The algorithm is a two-sweep projection:

  1. Flatten every span from every resource into a single list; parse its
     attributes and remember `(span_id → span_info)`.
  2. Compute the ERROR closure by walking each ERROR span upward through
     its parent chain — ancestors of a failed span are also failed.
  3. Emit nodes and edges per the rules in docs/mapping.md, using natural
     keys for dedup. Re-running the mapper on the same input is a no-op.

Everything is read directly from the OTLP/JSON dicts. We don't round-trip
through the protobuf classes — keeps this lib SDK-optional.
"""
from __future__ import annotations

from typing import Any, Optional

from .schema import (
    Agent,
    DataSource,
    Edge,
    EdgeType,
    Graph,
    Model,
    Operation,
    Session,
    Status,
    Tool,
)

# ---------------------------------------------------------------------------
# OTLP/JSON helpers
# ---------------------------------------------------------------------------

_STATUS_CODE_TO_STR: dict[int, str] = {
    0: Status.UNSET.value,
    1: Status.OK.value,
    2: Status.ERROR.value,
}

# Legacy → v1.37 canonical op name. Normalising here means downstream Cypher
# can `MATCH (o:Operation {type:"chat"})` regardless of which SDK/instrumentor
# produced the span. Observed sources:
#   * opentelemetry-instrumentation-google-genai (≤0.7b0) emits "generate_content"
#   * older Anthropic SDK wrappers emit "message"
#   * some HF / custom emitters use "text_generation"
# Keep this list small and source-justified — silently rewriting unfamiliar
# values is worse than letting them through.
_OP_NAME_ALIAS: dict[str, str] = {
    "generate_content": "chat",
    "text_generation":  "text_completion",
    "message":          "chat",
}

_MODEL_OPS = {"chat", "text_completion", "embeddings"}
_AGENT_OPS = {"invoke_agent", "create_agent"}

# Legacy `gen_ai.system` → v1.37 `gen_ai.provider.name` canonical values.
# `gen_ai.system` was the pre-v1.37 convention; many instrumentors still emit
# it. We prefer `gen_ai.provider.name` when both are present.
_PROVIDER_ALIAS: dict[str, str] = {
    "gemini":          "gcp.gen_ai",
    "vertex_ai":       "gcp.vertex_ai",
    "az.ai.openai":    "azure.ai.openai",
    "az.ai.inference": "azure.ai.inference",
}


def _canonical_provider(attrs: dict[str, Any]) -> Optional[str]:
    """Resolve the Model provider, preferring v1.37 over legacy."""
    raw = attrs.get("gen_ai.provider.name") or attrs.get("gen_ai.system")
    if not raw:
        return None
    return _PROVIDER_ALIAS.get(raw, raw)


def _canonical_op_name(raw: Optional[str]) -> str:
    if not raw:
        return "unknown"
    return _OP_NAME_ALIAS.get(raw, raw)


# Priority-ordered conversation-id resolution.
#
# `gen_ai.conversation.id` is the OTel GenAI v1.37 canonical attribute and
# MUST win when present — that keeps us compatible with any spec-conformant
# emitter (Traceloop's SDK is currently the only one that sets it).
# The fallbacks cover observed peer conventions so downstream graphs still
# get Session grouping when the emitter predates or ignores the canonical key.
#
# Per the spec: `gen_ai.conversation.id` is Conditionally Required — "apps MAY
# add it using custom span or log record processors." This list is the
# pragmatic read of what apps and SDKs actually emit today.
_CONVERSATION_KEYS: tuple[str, ...] = (
    "gen_ai.conversation.id",                       # OTel GenAI v1.37 canonical
    "session.id",                                   # Arize OpenInference
    "langsmith.trace.session_id",                   # LangSmith OTel bridge
    "traceloop.association.properties.session_id",  # Traceloop association bag
    "traceloop.association.properties.thread_id",   # Traceloop variant
)


def _conversation_id(attrs: dict[str, Any]) -> Optional[str]:
    """Resolve the Session id. Canonical-first; returns None if no key present."""
    for key in _CONVERSATION_KEYS:
        val = attrs.get(key)
        if val:
            return str(val)
    return None


def _value(av: dict[str, Any]) -> Any:
    """Unwrap one OTLP/JSON AnyValue object → Python scalar."""
    if "stringValue" in av:
        return av["stringValue"]
    if "intValue" in av:
        iv = av["intValue"]
        # OTLP/JSON encodes int64 as string per the protobuf-JSON mapping,
        # but some SDKs emit raw ints — tolerate both.
        return int(iv) if isinstance(iv, str) else iv
    if "doubleValue" in av:
        return float(av["doubleValue"])
    if "boolValue" in av:
        return bool(av["boolValue"])
    if "arrayValue" in av:
        return [_value(v) for v in av["arrayValue"].get("values", [])]
    return None


def _attrs_to_dict(attrs: list[dict[str, Any]]) -> dict[str, Any]:
    return {a["key"]: _value(a.get("value", {})) for a in attrs or []}


def _parent(span: dict[str, Any]) -> Optional[str]:
    p = span.get("parentSpanId")
    return p if p else None  # treat "" as None


def _unix_ns(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Mapper
# ---------------------------------------------------------------------------

def map_spans(resource_spans: list[dict[str, Any]]) -> Graph:
    graph = Graph()

    # Sweep 1: flatten spans
    parsed: list[dict[str, Any]] = []
    for rs in resource_spans or []:
        for ss in rs.get("scopeSpans", []) or []:
            for span in ss.get("spans", []) or []:
                attrs = _attrs_to_dict(span.get("attributes", []))
                status_code = (span.get("status") or {}).get("code", 0)
                parsed.append(
                    {
                        "span_id": span["spanId"],
                        "trace_id": span["traceId"],
                        "parent_span_id": _parent(span),
                        "attrs": attrs,
                        "status": _STATUS_CODE_TO_STR.get(status_code, Status.UNSET.value),
                        "status_message": (span.get("status") or {}).get("message"),
                        "start_ns": _unix_ns(span.get("startTimeUnixNano")),
                        "end_ns": _unix_ns(span.get("endTimeUnixNano")),
                    }
                )

    by_id: dict[str, dict[str, Any]] = {s["span_id"]: s for s in parsed}

    # Sweep 2: error propagation — an ERROR span marks every ancestor ERROR.
    error_set: set[str] = set()
    for s in parsed:
        if s["status"] != Status.ERROR.value:
            continue
        cur: Optional[str] = s["span_id"]
        # guard against pathological cycles (shouldn't happen, but cheap)
        seen: set[str] = set()
        while cur and cur in by_id and cur not in seen:
            seen.add(cur)
            error_set.add(cur)
            cur = by_id[cur]["parent_span_id"]

    # Sweep 3: emit nodes + edges
    for s in parsed:
        _emit(graph, s, by_id, error_set)

    return graph


def _emit(
    graph: Graph,
    s: dict[str, Any],
    by_id: dict[str, dict[str, Any]],
    error_set: set[str],
) -> None:
    attrs = s["attrs"]
    op_type = _canonical_op_name(attrs.get("gen_ai.operation.name"))
    span_id = s["span_id"]
    final_status = Status.ERROR.value if span_id in error_set else s["status"]

    # ---- Operation node -----------------------------------------------------
    op = Operation(
        span_id=span_id,
        trace_id=s["trace_id"],
        type=op_type,
        start_ns=s["start_ns"],
        end_ns=s["end_ns"],
        status=final_status,
        input_tokens=attrs.get("gen_ai.usage.input_tokens"),
        output_tokens=attrs.get("gen_ai.usage.output_tokens"),
        error_message=s["status_message"] if final_status == Status.ERROR.value else None,
    )
    graph.add_node(op)

    # ---- Session / CONTAINS -------------------------------------------------
    conv_id = _conversation_id(attrs)
    if conv_id:
        session = Session(id=conv_id)
        graph.add_node(session)
        graph.add_edge(Edge(EdgeType.CONTAINS.value, session.key, op.key))

    # ---- Agent / INVOKED ----------------------------------------------------
    agent_id = attrs.get("gen_ai.agent.id")
    if agent_id:
        agent = Agent(id=agent_id, name=attrs.get("gen_ai.agent.name"))
        graph.add_node(agent)
        if op_type in _AGENT_OPS:
            graph.add_edge(Edge(EdgeType.INVOKED.value, agent.key, op.key))

    # ---- Model / EXECUTED ---------------------------------------------------
    if op_type in _MODEL_OPS:
        provider = _canonical_provider(attrs)
        model_name = attrs.get("gen_ai.response.model") or attrs.get("gen_ai.request.model")
        if provider and model_name:
            model = Model(provider=provider, name=model_name)
            graph.add_node(model)
            graph.add_edge(Edge(EdgeType.EXECUTED.value, op.key, model.key))

    # ---- Tool / CALLED ------------------------------------------------------
    if op_type == "execute_tool":
        tool_name = attrs.get("gen_ai.tool.name")
        if tool_name:
            tool = Tool(name=tool_name)
            graph.add_node(tool)
            graph.add_edge(Edge(EdgeType.CALLED.value, op.key, tool.key))

    # ---- DataSource / RETRIEVED_FROM (+ ACCESSED convenience edge) ----------
    ds_id = attrs.get("gen_ai.data_source.id")
    if ds_id:
        ds = DataSource(id=ds_id, kind=attrs.get("gen_ai.data_source.kind"))
        graph.add_node(ds)
        graph.add_edge(Edge(EdgeType.RETRIEVED_FROM.value, op.key, ds.key))
        if agent_id:
            graph.add_edge(
                Edge(EdgeType.ACCESSED.value, ("Agent", agent_id), ds.key)
            )

    # ---- PARENT_OF ----------------------------------------------------------
    # Always emit on parent_span_id presence, even if the parent isn't in this
    # batch. Under live streaming (SimpleSpanProcessor) child and parent spans
    # arrive in separate export() calls; the sink stubs the absent endpoint
    # and the parent's own export later fills in its fields.
    parent_id = s["parent_span_id"]
    if parent_id:
        graph.add_edge(
            Edge(EdgeType.PARENT_OF.value, ("Operation", parent_id), op.key)
        )

        # ---- DELEGATED_TO (Agent → Agent) -----------------------------------
        # This one still requires the parent's attributes to decide whether
        # to emit, so it's in-batch only. Live streaming under
        # SimpleSpanProcessor will miss DELEGATED_TO when spans split across
        # batches — use BatchSpanProcessor if you rely on it.
        if op_type in _AGENT_OPS and agent_id and parent_id in by_id:
            parent = by_id[parent_id]
            p_attrs = parent["attrs"]
            p_op = p_attrs.get("gen_ai.operation.name")
            p_agent = p_attrs.get("gen_ai.agent.id")
            if p_op in _AGENT_OPS and p_agent and p_agent != agent_id:
                graph.add_edge(
                    Edge(
                        EdgeType.DELEGATED_TO.value,
                        ("Agent", p_agent),
                        ("Agent", agent_id),
                    )
                )
