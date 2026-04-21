"""Graph-level invariant tests.

Runs `invariants.check` against every data source we have and asserts no
violations. This is the shape-independent safety net: anything count-based
tests can't see ends up here — mislabelled edges, cycles, orphan Models,
Sessions split across Ops, etc.

Data sources covered:
  * tests/fixtures/*.json           — hand-written fixtures
  * tests/fixtures/real/*.json      — captured real traces (if any)
  * generate_traces.py              — parametric synthetic corpora
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from otel_genai_graph.invariants import Violation, check
from otel_genai_graph.mapper import map_spans

from generate_traces import (  # type: ignore[import-not-found]
    TraceBuilder,
    shape_agent_tool,
    shape_multi_agent,
    shape_multi_turn,
    shape_rag,
    shape_random,
    shape_simple,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
REAL_DIR = FIXTURES_DIR / "real"
PUBLIC_DIR = FIXTURES_DIR / "public"

HANDWRITTEN_FIXTURES = sorted(FIXTURES_DIR.glob("*.json"))
REAL_FIXTURES = sorted(REAL_DIR.glob("*.json")) if REAL_DIR.exists() else []
PUBLIC_FIXTURES = sorted(PUBLIC_DIR.rglob("*.json")) if PUBLIC_DIR.exists() else []


def _violations_for(resource_spans: list[dict]) -> list[Violation]:
    graph = map_spans(resource_spans)
    return check(graph)


def _format(vs: list[Violation]) -> str:
    return "\n  " + "\n  ".join(str(v) for v in vs) if vs else ""


# ---------------------------------------------------------------------------
# Hand-written fixtures
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "fixture_path", HANDWRITTEN_FIXTURES, ids=lambda p: p.stem
)
def test_handwritten_fixtures_are_well_formed(fixture_path: Path) -> None:
    data = json.loads(fixture_path.read_text())
    vs = _violations_for(data["otlp"]["resourceSpans"])
    assert not vs, f"violations in {fixture_path.stem}:{_format(vs)}"


# ---------------------------------------------------------------------------
# Real captures (skipped if directory is empty)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not REAL_FIXTURES, reason="no captures under tests/fixtures/real/")
@pytest.mark.parametrize(
    "fixture_path", REAL_FIXTURES or [None], ids=lambda p: p.stem if p else "none"
)
def test_real_captures_are_well_formed(fixture_path: Path) -> None:
    data = json.loads(fixture_path.read_text())
    vs = _violations_for(data["otlp"]["resourceSpans"])
    assert not vs, f"violations in {fixture_path.stem}:{_format(vs)}"


# ---------------------------------------------------------------------------
# Public captures (peer instrumentors — e.g. opentelemetry-instrumentation-openai-v2)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not PUBLIC_FIXTURES, reason="no captures under tests/fixtures/public/")
@pytest.mark.parametrize(
    "fixture_path", PUBLIC_FIXTURES or [None],
    ids=lambda p: str(p.relative_to(FIXTURES_DIR)) if p else "none",
)
def test_public_captures_are_well_formed(fixture_path: Path) -> None:
    data = json.loads(fixture_path.read_text())
    vs = _violations_for(data["otlp"]["resourceSpans"])
    assert not vs, f"violations in {fixture_path.relative_to(FIXTURES_DIR)}:{_format(vs)}"


# ---------------------------------------------------------------------------
# Synthesizer — per-shape sweeps
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(20))
def test_simple_shape_invariants(seed: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_simple(tb, provider="anthropic", model="claude-sonnet-4-5")
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("tools", [0, 1, 3, 7])
@pytest.mark.parametrize("err_rate", [0.0, 0.5, 1.0])
def test_agent_tool_shape_invariants(seed: int, tools: int, err_rate: float) -> None:
    tb = TraceBuilder(seed=seed)
    shape_agent_tool(
        tb, provider="anthropic", model="claude-sonnet-4-5",
        tools_per_call=tools, error_rate=err_rate,
    )
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("depth", [2, 3, 4, 5])
def test_multi_agent_shape_invariants(seed: int, depth: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_multi_agent(tb, provider="anthropic", model="claude-opus-4-7", depth=depth)
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("retrievals", [1, 2, 4, 8])
def test_rag_shape_invariants(seed: int, retrievals: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_rag(tb, provider="anthropic", model="claude-sonnet-4-5", retrievals=retrievals)
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("turns", [2, 5, 10])
def test_multi_turn_shape_invariants(seed: int, turns: int) -> None:
    tb = TraceBuilder(seed=seed)
    shape_multi_turn(tb, provider="anthropic", model="claude-sonnet-4-5", turns=turns)
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


@pytest.mark.parametrize("seed", range(100))
def test_random_shape_corpus_invariants(seed: int) -> None:
    """Bulk sweep — 100 random-shape traces must all satisfy every invariant."""
    tb = TraceBuilder(seed=seed)
    shape_random(tb, provider="anthropic", error_rate=0.1)
    vs = _violations_for(tb.finalize()["otlp"]["resourceSpans"])
    assert not vs, _format(vs)


# ---------------------------------------------------------------------------
# Negative tests — hand-crafted broken graphs must trip specific checks.
# ---------------------------------------------------------------------------

def test_detects_bad_edge_endpoints() -> None:
    from otel_genai_graph.schema import (
        Agent,
        Edge,
        Graph,
        Model,
        Operation,
    )

    g = Graph()
    g.add_node(Operation(span_id="s1", trace_id="t", type="chat"))
    g.add_node(Model(provider="anthropic", name="claude-sonnet-4-5"))
    g.add_node(Agent(id="a1"))
    # Nonsense: EXECUTED should go Operation→Model, not Agent→Model.
    g.add_edge(Edge("EXECUTED", ("Agent", "a1"), ("Model", "anthropic/claude-sonnet-4-5")))

    vs = check(g)
    codes = {v.invariant for v in vs}
    assert "edge_endpoint_labels" in codes, codes


def test_detects_orphan_model() -> None:
    from otel_genai_graph.schema import Graph, Model

    g = Graph()
    g.add_node(Model(provider="anthropic", name="unreferenced"))
    vs = check(g)
    assert any(v.invariant == "orphan_secondary" for v in vs)


def test_detects_cycle_in_parent_of() -> None:
    from otel_genai_graph.schema import Edge, Graph, Operation

    g = Graph()
    g.add_node(Operation(span_id="a", trace_id="t", type="chat"))
    g.add_node(Operation(span_id="b", trace_id="t", type="chat"))
    g.add_edge(Edge("PARENT_OF", ("Operation", "a"), ("Operation", "b")))
    g.add_edge(Edge("PARENT_OF", ("Operation", "b"), ("Operation", "a")))

    vs = check(g)
    assert any(v.invariant == "cycle" for v in vs)


def test_detects_negative_token_counts() -> None:
    from otel_genai_graph.schema import Graph, Operation

    g = Graph()
    g.add_node(Operation(span_id="s1", trace_id="t", type="chat", input_tokens=-5))
    vs = check(g)
    assert any(v.invariant == "negative_tokens" for v in vs)


def test_detects_time_ordering_inversion() -> None:
    from otel_genai_graph.schema import Graph, Operation

    g = Graph()
    g.add_node(Operation(span_id="s1", trace_id="t", type="chat",
                         start_ns=2_000, end_ns=1_000))
    vs = check(g)
    assert any(v.invariant == "time_ordering" for v in vs)


def test_clean_graph_produces_no_violations() -> None:
    """Sanity — the mapper output on a known-good fixture has zero violations."""
    data = json.loads((FIXTURES_DIR / "agent_with_tool.json").read_text())
    g = map_spans(data["otlp"]["resourceSpans"])
    assert check(g) == []
