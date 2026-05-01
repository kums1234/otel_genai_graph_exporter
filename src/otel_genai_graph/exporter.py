"""OTel SpanExporter that streams GenAI spans into a chosen backend.

Plug this into a ``TracerProvider`` and any span an app emits with GenAI
semconv attributes lands in your backend immediately — no file round-trip.
The exporter is backend-agnostic; it accepts any object that satisfies
the :class:`otel_genai_graph.sink.Sink` protocol (``Neo4jSink``,
``DuckDBSink``, …).

Example — Neo4j
---------------
::

    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from otel_genai_graph.exporter import GenAIExporter
    from otel_genai_graph.neo4j_sink import Neo4jSink

    sink = Neo4jSink("bolt://localhost:17687", "neo4j", "testtest")
    sink.connect()
    sink.ensure_schema()

    provider = TracerProvider()
    provider.add_span_processor(BatchSpanProcessor(GenAIExporter(sink)))
    trace.set_tracer_provider(provider)

Example — DuckDB
----------------
::

    from otel_genai_graph.duckdb_sink import DuckDBSink

    sink = DuckDBSink("./trace.duckdb")
    sink.connect()
    sink.ensure_schema()
    provider.add_span_processor(BatchSpanProcessor(GenAIExporter(sink)))

Picking from config / env: see :func:`otel_genai_graph.sink.config_from_env`.

Note on processor choice
------------------------
Pick ``BatchSpanProcessor`` in production — it amortizes backend
round-trips across a span batch. ``SimpleSpanProcessor`` is fine for
tests and demos (one write per span).
"""
from __future__ import annotations

import logging
from typing import Any, Sequence

from .mapper import map_spans
from .sink import Sink

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SDK span → OTLP/JSON dict conversion
# ---------------------------------------------------------------------------

def _attr_value_to_otlp(v: Any) -> dict:
    # bool must be tested before int (bool is a subclass of int in Python).
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        return {"intValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, str):
        return {"stringValue": v}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [_attr_value_to_otlp(x) for x in v]}}
    return {"stringValue": str(v)}


def _attrs_to_otlp_list(attrs: dict | None) -> list[dict]:
    return [{"key": k, "value": _attr_value_to_otlp(v)} for k, v in (attrs or {}).items()]


def _hex_trace_id(int_id: int) -> str:
    return format(int_id, "032x")


def _hex_span_id(int_id: int) -> str:
    return format(int_id, "016x")


def sdk_span_to_otlp(span: Any) -> dict:
    """Serialise one `ReadableSpan` to the OTLP/JSON span shape the mapper expects."""
    ctx = span.get_span_context()
    parent = span.parent

    # SpanKind is an IntEnum in the SDK — .value gives the protobuf int.
    # Don't use getattr(..., int(...)) — the default is evaluated eagerly.
    if span.kind is None:
        kind = 0
    elif hasattr(span.kind, "value"):
        kind = span.kind.value
    else:
        kind = int(span.kind)

    # Status: status_code is an enum too.
    status_code_obj = getattr(span.status, "status_code", None)
    if status_code_obj is None:
        status_code = 0
    elif hasattr(status_code_obj, "value"):
        status_code = status_code_obj.value
    else:
        status_code = int(status_code_obj)
    status: dict[str, Any] = {"code": status_code}
    desc = getattr(span.status, "description", None)
    if desc:
        status["message"] = desc

    return {
        "traceId":           _hex_trace_id(ctx.trace_id),
        "spanId":            _hex_span_id(ctx.span_id),
        "parentSpanId":      _hex_span_id(parent.span_id) if parent else "",
        "name":              span.name,
        "kind":              kind,
        "startTimeUnixNano": str(span.start_time) if span.start_time else "0",
        "endTimeUnixNano":   str(span.end_time) if span.end_time else "0",
        "attributes":        _attrs_to_otlp_list(dict(span.attributes or {})),
        "status":            status,
    }


def group_spans_to_resource_spans(spans: Sequence[Any]) -> list[dict]:
    """Bucket `ReadableSpan`s by (resource, instrumentation scope) → OTLP resourceSpans.

    The mapper ignores the grouping for node/edge counts, but we still build
    it faithfully so anything that later depends on resource/scope metadata
    (e.g. a future Resource node) has what it needs.
    """
    by_resource: dict[tuple, list[Any]] = {}
    for span in spans:
        res_attrs = dict((span.resource.attributes or {}) if span.resource else {})
        res_key = tuple(sorted(res_attrs.items()))
        by_resource.setdefault(res_key, []).append(span)

    out: list[dict] = []
    for res_key, res_spans in by_resource.items():
        by_scope: dict[tuple, list[dict]] = {}
        for s in res_spans:
            scope = s.instrumentation_scope
            scope_key = (
                getattr(scope, "name", "") or "",
                getattr(scope, "version", "") or "",
            )
            by_scope.setdefault(scope_key, []).append(sdk_span_to_otlp(s))

        out.append(
            {
                "resource": {"attributes": _attrs_to_otlp_list(dict(res_key))},
                "scopeSpans": [
                    {
                        "scope": {"name": name, "version": version},
                        "spans": sp,
                    }
                    for (name, version), sp in by_scope.items()
                ],
            }
        )
    return out


# ---------------------------------------------------------------------------
# SpanExporter
# ---------------------------------------------------------------------------

try:
    from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
except ImportError:  # pragma: no cover - SDK is a runtime dep
    SpanExporter = object  # type: ignore[misc,assignment]

    class SpanExportResult:  # type: ignore[no-redef]
        SUCCESS = 0
        FAILURE = 1


class GenAIExporter(SpanExporter):  # type: ignore[misc,valid-type]
    """Stream OTel GenAI spans into any backend that satisfies ``Sink``.

    Wraps a sink. Each ``export()`` call groups the incoming spans into
    OTLP/JSON ``resourceSpans``, runs them through ``map_spans``, and
    writes the resulting ``Graph`` in one transaction. Idempotency is the
    sink's responsibility — both shipped sinks (Neo4j, DuckDB) provide it.
    """

    def __init__(self, sink: Sink, *, auto_connect: bool = True) -> None:
        self.sink = sink
        if auto_connect:
            # Both shipped sinks make connect() idempotent. Safe to call
            # even if the caller already connected. Failing here surfaces
            # auth / network problems before the first batch.
            sink.connect()

    def export(self, spans: Sequence[Any]) -> int:
        try:
            resource_spans = group_spans_to_resource_spans(spans)
            graph = map_spans(resource_spans)
            self.sink.write(graph)
            return SpanExportResult.SUCCESS  # type: ignore[attr-defined]
        except Exception:
            log.exception("GenAIExporter.export failed")
            return SpanExportResult.FAILURE  # type: ignore[attr-defined]

    def shutdown(self) -> None:
        self.sink.close()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        # export() writes synchronously, so there's nothing buffered.
        return True


# ---------------------------------------------------------------------------
# Back-compat alias
# ---------------------------------------------------------------------------

class Neo4jGenAIExporter(GenAIExporter):
    """Deprecated alias for :class:`GenAIExporter`.

    Kept so existing imports (``from otel_genai_graph.exporter import
    Neo4jGenAIExporter``) keep working after the backend abstraction.
    The class is structurally identical — it accepts any ``Sink``, but
    historically callers paired it with ``Neo4jSink``.

    Prefer ``GenAIExporter`` in new code.
    """
