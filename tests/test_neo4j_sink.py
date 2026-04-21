"""Tests for the Neo4j sink.

Unit tests (always run): row builders and the batching logic.

Integration tests (run iff `NEO4J_URI` is set in the env): round-trip
every fixture through a live Neo4j and assert the DB counts match the
mapper's in-memory counts.

To run the integration tests locally:

    docker run -d -p 7474:7474 -p 7687:7687 \\
        -e NEO4J_AUTH=neo4j/test neo4j:5
    export NEO4J_URI=bolt://localhost:7687
    export NEO4J_USER=neo4j
    export NEO4J_PASSWORD=test
    pytest tests/test_neo4j_sink.py
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from otel_genai_graph.mapper import map_spans
from otel_genai_graph.neo4j_sink import (
    Neo4jSink,
    _EDGE_CYPHER,
    _NODE_CYPHER,
    edge_row,
    node_row,
)
from otel_genai_graph.schema import (
    Agent,
    DataSource,
    Edge,
    Model,
    Operation,
    Session,
    Tool,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Unit — row builders
# ---------------------------------------------------------------------------

def test_edge_row_splits_model_key() -> None:
    e = Edge("EXECUTED", ("Operation", "abc"), ("Model", "anthropic/claude-sonnet-4-5"))
    row = edge_row(e)
    assert row == {
        "src_id": "abc",
        "dst_provider": "anthropic",
        "dst_name": "claude-sonnet-4-5",
    }


def test_edge_row_splits_model_key_with_slashes_in_name() -> None:
    # Provider names don't contain "/", so the split is always first-slash-wins.
    e = Edge("EXECUTED", ("Operation", "abc"), ("Model", "azure.ai.openai/gpt-oss-120b"))
    row = edge_row(e)
    assert row == {
        "src_id": "abc",
        "dst_provider": "azure.ai.openai",
        "dst_name": "gpt-oss-120b",
    }


def test_edge_row_tool_uses_dst_name() -> None:
    e = Edge("CALLED", ("Operation", "abc"), ("Tool", "web_search"))
    assert edge_row(e) == {"src_id": "abc", "dst_name": "web_search"}


def test_edge_row_plain_id_endpoints() -> None:
    e = Edge("CONTAINS", ("Session", "conv-1"), ("Operation", "span-1"))
    assert edge_row(e) == {"src_id": "conv-1", "dst_id": "span-1"}

    e2 = Edge("DELEGATED_TO", ("Agent", "orchestrator"), ("Agent", "specialist"))
    assert edge_row(e2) == {"src_id": "orchestrator", "dst_id": "specialist"}


def test_node_row_round_trips_dataclass_fields() -> None:
    s = Session(id="conv-1")
    assert node_row(s) == {"id": "conv-1"}

    a = Agent(id="agent-A", name="researcher")
    assert node_row(a) == {"id": "agent-A", "name": "researcher"}

    m = Model(provider="anthropic", name="claude-sonnet-4-5")
    assert node_row(m) == {"provider": "anthropic", "name": "claude-sonnet-4-5"}

    t = Tool(name="web_search")
    assert node_row(t) == {"name": "web_search"}

    ds = DataSource(id="kb-1", kind="vector_store")
    assert node_row(ds) == {"id": "kb-1", "kind": "vector_store"}

    op = Operation(span_id="s1", trace_id="t1", type="chat", status="OK",
                   input_tokens=100, output_tokens=50)
    row = node_row(op)
    assert row["span_id"] == "s1"
    assert row["status"] == "OK"
    assert row["input_tokens"] == 100


def test_every_edge_and_node_type_has_cypher() -> None:
    """Sanity: enums and Cypher maps stay in sync."""
    from otel_genai_graph.schema import EdgeType, NodeLabel

    # Every emitted edge type has a Cypher template.
    for et in EdgeType:
        assert et.value in _EDGE_CYPHER, f"missing Cypher for edge {et.value}"
    # Every node label has a Cypher template.
    for lbl in NodeLabel:
        assert lbl.value in _NODE_CYPHER, f"missing Cypher for label {lbl.value}"


# ---------------------------------------------------------------------------
# Integration — live Neo4j (gated on NEO4J_URI)
# ---------------------------------------------------------------------------

_NEO4J_URI = os.environ.get("NEO4J_URI")
_NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
_NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "test")
_NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

skip_if_no_neo4j = pytest.mark.skipif(
    _NEO4J_URI is None,
    reason="set NEO4J_URI to run live-Neo4j round-trip tests",
)


@pytest.fixture(scope="module")
def sink():  # type: ignore[no-untyped-def]
    assert _NEO4J_URI is not None
    with Neo4jSink(_NEO4J_URI, _NEO4J_USER, _NEO4J_PASSWORD, _NEO4J_DATABASE) as s:
        s.ensure_schema()
        # Clean slate per test module so counts are exact.
        with s._driver.session(database=s.database) as sess:  # type: ignore[union-attr]
            sess.run("MATCH (n) DETACH DELETE n")
        yield s


@skip_if_no_neo4j
@pytest.mark.parametrize(
    "fixture_name",
    [p.stem for p in sorted(FIXTURES_DIR.glob("*.json"))],
)
def test_fixture_round_trips_to_neo4j(sink, fixture_name: str) -> None:  # type: ignore[no-untyped-def]
    """Every fixture: mapper → sink → query back, counts must agree."""
    # Clean slate per fixture — counts are per-file assertions.
    with sink._driver.session(database=sink.database) as sess:
        sess.run("MATCH (n) DETACH DELETE n")

    path = FIXTURES_DIR / f"{fixture_name}.json"
    data = json.loads(path.read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])
    sink.write(graph)

    with sink._driver.session(database=sink.database) as sess:
        db_nodes = sess.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        db_edges = sess.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

    assert db_nodes == graph.node_count(), (
        f"[{fixture_name}] Neo4j node count {db_nodes} != mapper {graph.node_count()}"
    )
    assert db_edges == graph.edge_count(), (
        f"[{fixture_name}] Neo4j edge count {db_edges} != mapper {graph.edge_count()}"
    )


@skip_if_no_neo4j
def test_double_write_is_idempotent(sink) -> None:  # type: ignore[no-untyped-def]
    with sink._driver.session(database=sink.database) as sess:
        sess.run("MATCH (n) DETACH DELETE n")

    data = json.loads((FIXTURES_DIR / "agent_with_tool.json").read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])

    sink.write(graph)
    sink.write(graph)  # second write must add nothing

    with sink._driver.session(database=sink.database) as sess:
        n = sess.run("MATCH (n) RETURN count(n) AS c").single()["c"]
        e = sess.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    assert n == graph.node_count()
    assert e == graph.edge_count()
