"""Executable contract for `mapper.map_spans`.

Every fixture under `tests/fixtures/*.json` carries an `expected_graph`
block. This module turns that block into assertions so the mapper has a
concrete target: make these tests pass and the semantic mapping is correct.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from otel_genai_graph.mapper import map_spans

FIXTURES_DIR = Path(__file__).parent / "fixtures"
FIXTURE_FILES = sorted(FIXTURES_DIR.glob("*.json"))


def _load(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


@pytest.fixture(params=FIXTURE_FILES, ids=lambda p: p.stem)
def fixture(request: pytest.FixtureRequest) -> dict:
    return _load(request.param)


# ---------------------------------------------------------------------------
# Core contract: every fixture's expected_graph must match what the mapper
# produces, label-by-label and edge-type-by-edge-type.
# ---------------------------------------------------------------------------

def test_node_counts_match_expected(fixture: dict) -> None:
    expected = fixture["expected_graph"]
    graph = map_spans(fixture["otlp"]["resourceSpans"])

    for label, count in expected.get("nodes", {}).items():
        got = graph.node_count(label)
        assert got == count, (
            f"[{fixture['name']}] node_count({label!r}): expected {count}, got {got}"
        )

    if "total_nodes" in expected:
        got = graph.node_count()
        assert got == expected["total_nodes"], (
            f"[{fixture['name']}] total_nodes: expected {expected['total_nodes']}, got {got}"
        )


def test_edge_counts_match_expected(fixture: dict) -> None:
    expected = fixture["expected_graph"]
    graph = map_spans(fixture["otlp"]["resourceSpans"])

    for edge_type, count in expected.get("edges", {}).items():
        got = graph.edge_count(edge_type)
        assert got == count, (
            f"[{fixture['name']}] edge_count({edge_type!r}): expected {count}, got {got}"
        )

    if "total_edges" in expected:
        got = graph.edge_count()
        assert got == expected["total_edges"], (
            f"[{fixture['name']}] total_edges: expected {expected['total_edges']}, got {got}"
        )


# ---------------------------------------------------------------------------
# Idempotency: re-ingesting the same payload must not change the graph.
# Written once, deduped naturally via node keys and edge identity.
# ---------------------------------------------------------------------------

def test_mapping_is_idempotent(fixture: dict) -> None:
    otlp = fixture["otlp"]["resourceSpans"]
    g1 = map_spans(otlp)
    g2 = map_spans(otlp)

    assert g1.node_count() == g2.node_count()
    assert g1.edge_count() == g2.edge_count()
    assert set(g1.nodes.keys()) == set(g2.nodes.keys())
    assert g1.edges == g2.edges


# ---------------------------------------------------------------------------
# Scenario-specific checks: each fixture has one distinguishing behaviour
# that a count-only test won't catch.
# ---------------------------------------------------------------------------

def test_multi_turn_merges_to_one_session() -> None:
    fx = _load(FIXTURES_DIR / "multi_turn_conversation.json")
    graph = map_spans(fx["otlp"]["resourceSpans"])
    assert graph.node_count("Session") == 1, (
        "Three spans sharing conversation.id must collapse to one Session node."
    )


def test_multi_agent_emits_delegated_to() -> None:
    fx = _load(FIXTURES_DIR / "multi_agent.json")
    graph = map_spans(fx["otlp"]["resourceSpans"])
    edges = graph.edges_of("DELEGATED_TO")
    assert len(edges) == 1
    edge = edges[0]
    assert edge.src == ("Agent", "orchestrator")
    assert edge.dst == ("Agent", "specialist")


def test_rag_flow_attaches_retrieval_to_datasource() -> None:
    fx = _load(FIXTURES_DIR / "rag_flow.json")
    graph = map_spans(fx["otlp"]["resourceSpans"])
    retrieved = graph.edges_of("RETRIEVED_FROM")
    assert len(retrieved) == 1
    assert retrieved[0].dst == ("DataSource", "vector-store-kb1")


def test_error_status_propagates_to_ancestors() -> None:
    fx = _load(FIXTURES_DIR / "error_case.json")
    graph = map_spans(fx["otlp"]["resourceSpans"])

    # Both the tool span (direct error) and its agent ancestor must be ERROR.
    expected_ids = set(fx["expected_graph"]["error_ops"])
    error_ids = {
        op.span_id
        for op in graph.nodes_of("Operation")
        if op.status == "ERROR"  # type: ignore[attr-defined]
    }
    assert expected_ids.issubset(error_ids), (
        f"Expected ERROR on {expected_ids}, got {error_ids}"
    )


def test_simple_call_has_model_executed_edge() -> None:
    fx = _load(FIXTURES_DIR / "simple_llm_call.json")
    graph = map_spans(fx["otlp"]["resourceSpans"])
    executed = graph.edges_of("EXECUTED")
    assert len(executed) == 1
    assert executed[0].dst == ("Model", "anthropic/claude-sonnet-4-5")
