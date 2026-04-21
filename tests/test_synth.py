"""Synthetic-trace tests.

Uses `tests/generate_traces.py` to produce hundreds of deterministic traces
on the fly, and asserts that the mapper's output matches the generator's
declared `expected_graph` exactly.

This is the layer that catches edge cases the six hand-written fixtures
miss — fencepost errors, dedup mishaps, attribute-ordering assumptions.
"""
from __future__ import annotations

import random

import pytest

from otel_genai_graph.mapper import map_spans

from generate_traces import (  # type: ignore[import-not-found]
    TraceBuilder,
    apply_chaos,
    shape_agent_tool,
    shape_multi_agent,
    shape_multi_turn,
    shape_rag,
    shape_random,
    shape_simple,
)


def _graph_counts(graph) -> tuple[dict, dict]:  # type: ignore[no-untyped-def]
    nodes: dict[str, int] = {}
    for (label, _id) in graph.nodes:
        nodes[label] = nodes.get(label, 0) + 1
    edges: dict[str, int] = {}
    for e in graph.edges:
        edges[e.edge_type] = edges.get(e.edge_type, 0) + 1
    return nodes, edges


# ---------------------------------------------------------------------------
# Per-shape sweeps — each shape is run with several seeds and parameter sets.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(20))
def test_simple_shape(seed: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_simple(tb, provider="anthropic", model="claude-sonnet-4-5")
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"]
    assert edges == doc["expected_graph"]["edges"]


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("tools", [0, 1, 3, 7])
@pytest.mark.parametrize("err_rate", [0.0, 0.5, 1.0])
def test_agent_tool_shape(seed: int, tools: int, err_rate: float) -> None:
    tb = TraceBuilder(seed=seed)
    shape_agent_tool(
        tb, provider="anthropic", model="claude-sonnet-4-5",
        tools_per_call=tools, error_rate=err_rate,
    )
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"]
    assert edges == doc["expected_graph"]["edges"]

    # If error_rate == 1.0 and tools > 0, every tool span failed → ancestor
    # agent span is ERROR too. Double-check propagation.
    if err_rate == 1.0 and tools > 0:
        error_ops = {
            op.span_id for op in graph.nodes_of("Operation")
            if op.status == "ERROR"  # type: ignore[attr-defined]
        }
        assert set(doc["expected_graph"]["error_ops"]).issubset(error_ops)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("depth", [2, 3, 4, 5])
def test_multi_agent_shape(seed: int, depth: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_multi_agent(tb, provider="anthropic", model="claude-opus-4-7", depth=depth)
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"]
    assert edges == doc["expected_graph"]["edges"]

    # depth-1 DELEGATED_TO edges (each step)
    assert edges.get("DELEGATED_TO", 0) == depth - 1


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("retrievals", [1, 2, 4, 8])
def test_rag_shape(seed: int, retrievals: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_rag(tb, provider="anthropic", model="claude-sonnet-4-5", retrievals=retrievals)
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"]
    assert edges == doc["expected_graph"]["edges"]
    assert edges.get("RETRIEVED_FROM", 0) == retrievals


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("turns", [2, 3, 5, 10])
def test_multi_turn_shape(seed: int, turns: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_multi_turn(tb, provider="anthropic", model="claude-sonnet-4-5", turns=turns)
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"]
    assert edges == doc["expected_graph"]["edges"]
    # All turns collapse to one Session regardless of turn count
    assert nodes["Session"] == 1


@pytest.mark.parametrize("seed", range(50))
def test_random_shape_corpus(seed: int) -> None:
    """Bulk corpus: 50 random-shape traces must all map correctly."""
    tb = TraceBuilder(seed=seed)
    shape_random(tb, provider="anthropic", error_rate=0.1)
    doc = tb.finalize()

    graph = map_spans(doc["otlp"]["resourceSpans"])
    nodes, edges = _graph_counts(graph)
    assert nodes == doc["expected_graph"]["nodes"], f"seed={seed} nodes diff"
    assert edges == doc["expected_graph"]["edges"], f"seed={seed} edges diff"


# ---------------------------------------------------------------------------
# Chaos — mapper must not crash on adversarial input.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(40))
def test_chaos_does_not_crash_mapper(seed: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_random(tb, provider="anthropic", error_rate=0.2)
    doc = tb.finalize()
    apply_chaos(doc["otlp"], random.Random(seed + 9999))

    # contract under chaos: no exceptions, graph is internally consistent
    graph = map_spans(doc["otlp"]["resourceSpans"])
    assert graph.node_count() >= 0
    assert graph.edge_count() >= 0


# ---------------------------------------------------------------------------
# Idempotency holds across every synthetic corpus.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(20))
def test_synthetic_mapping_is_idempotent(seed: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_random(tb, provider="anthropic", error_rate=0.1)
    doc = tb.finalize()

    g1 = map_spans(doc["otlp"]["resourceSpans"])
    g2 = map_spans(doc["otlp"]["resourceSpans"])
    assert g1.node_count() == g2.node_count()
    assert g1.edge_count() == g2.edge_count()
    assert set(g1.nodes.keys()) == set(g2.nodes.keys())
    assert g1.edges == g2.edges
