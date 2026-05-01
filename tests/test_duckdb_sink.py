"""Tests for the DuckDB sink.

Two layers:

  * **Unit** — row-builders (``ops_rows``, ``dim_rows``, ``agent_delegation_rows``).
    No DB needed, exercise the flattening logic on small synthetic graphs.
  * **Integration** — round-trip every fixture through an in-memory DuckDB.
    DuckDB is in-process so these always run; no env-gating like Neo4j.

The integration tests are the contract-equivalent of ``test_neo4j_sink``:
fixture → mapper → sink → ``SELECT count(*)`` → must equal mapper counts,
and a second write must be idempotent.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from otel_genai_graph.duckdb_sink import (  # noqa: E402
    DuckDBSink,
    _OPS_COLUMNS,
    agent_delegation_rows,
    dim_rows,
    ops_rows,
)
from otel_genai_graph.mapper import map_spans  # noqa: E402
from otel_genai_graph.schema import (  # noqa: E402
    Agent,
    DataSource,
    Edge,
    EdgeType,
    Graph,
    Model,
    NodeLabel,
    Operation,
    Session,
    Tool,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Unit — row builders
# ---------------------------------------------------------------------------

def _mini_graph_one_op() -> Graph:
    """A representative single-op graph: Session → Op → Model + Tool + DataSource."""
    g = Graph()
    g.add_node(Session(id="conv-1"))
    g.add_node(Agent(id="agent-A", name="researcher"))
    g.add_node(Model(provider="anthropic", name="claude-sonnet-4-5"))
    g.add_node(Tool(name="web_search"))
    g.add_node(DataSource(id="kb-1", kind="vector_store"))
    g.add_node(Operation(
        span_id="s1", trace_id="t1", type="chat", status="OK",
        input_tokens=100, output_tokens=50,
    ))
    op_key = (NodeLabel.OPERATION.value, "s1")
    g.add_edge(Edge(EdgeType.CONTAINS.value,       (NodeLabel.SESSION.value,    "conv-1"),                    op_key))
    g.add_edge(Edge(EdgeType.INVOKED.value,        (NodeLabel.AGENT.value,      "agent-A"),                   op_key))
    g.add_edge(Edge(EdgeType.EXECUTED.value,       op_key, (NodeLabel.MODEL.value,      "anthropic/claude-sonnet-4-5")))
    g.add_edge(Edge(EdgeType.CALLED.value,         op_key, (NodeLabel.TOOL.value,       "web_search")))
    g.add_edge(Edge(EdgeType.RETRIEVED_FROM.value, op_key, (NodeLabel.DATA_SOURCE.value, "kb-1")))
    return g


def test_ops_rows_flattens_all_structural_edges() -> None:
    rows = ops_rows(_mini_graph_one_op())
    assert len(rows) == 1
    r = rows[0]
    assert r["span_id"]        == "s1"
    assert r["trace_id"]       == "t1"
    assert r["type"]           == "chat"
    assert r["status"]         == "OK"
    assert r["input_tokens"]   == 100
    assert r["output_tokens"]  == 50
    assert r["session_id"]     == "conv-1"
    assert r["agent_id"]       == "agent-A"
    assert r["model_provider"] == "anthropic"
    assert r["model_name"]     == "claude-sonnet-4-5"
    assert r["tool_name"]      == "web_search"
    assert r["data_source_id"] == "kb-1"
    assert r["parent_span_id"] is None  # no PARENT_OF in this fixture


def test_ops_rows_propagates_parent_of() -> None:
    g = Graph()
    g.add_node(Operation(span_id="parent", trace_id="t", type="invoke_agent"))
    g.add_node(Operation(span_id="child",  trace_id="t", type="chat"))
    g.add_edge(Edge(
        EdgeType.PARENT_OF.value,
        (NodeLabel.OPERATION.value, "parent"),
        (NodeLabel.OPERATION.value, "child"),
    ))
    rows = {r["span_id"]: r for r in ops_rows(g)}
    assert rows["parent"]["parent_span_id"] is None
    assert rows["child"]["parent_span_id"]  == "parent"


def test_ops_rows_handles_provider_with_dots() -> None:
    """Model key splits on first slash — providers with dots survive."""
    g = Graph()
    g.add_node(Operation(span_id="s1", trace_id="t1", type="chat"))
    g.add_node(Model(provider="azure.ai.openai", name="gpt-oss-120b"))
    g.add_edge(Edge(
        EdgeType.EXECUTED.value,
        (NodeLabel.OPERATION.value, "s1"),
        (NodeLabel.MODEL.value, "azure.ai.openai/gpt-oss-120b"),
    ))
    r = ops_rows(g)[0]
    assert r["model_provider"] == "azure.ai.openai"
    assert r["model_name"]     == "gpt-oss-120b"


def test_dim_rows_buckets_by_label() -> None:
    g = _mini_graph_one_op()
    d = dim_rows(g)
    assert d["sessions"]     == [{"id": "conv-1"}]
    assert d["agents"]       == [{"id": "agent-A", "name": "researcher"}]
    assert d["models"]       == [{"provider": "anthropic", "name": "claude-sonnet-4-5"}]
    assert d["tools"]        == [{"name": "web_search"}]
    assert d["data_sources"] == [{"id": "kb-1", "kind": "vector_store"}]
    assert d["resources"]    == []


def test_agent_delegation_rows_only_picks_delegated_to() -> None:
    g = Graph()
    g.add_node(Agent(id="orchestrator"))
    g.add_node(Agent(id="specialist"))
    g.add_edge(Edge(
        EdgeType.DELEGATED_TO.value,
        (NodeLabel.AGENT.value, "orchestrator"),
        (NodeLabel.AGENT.value, "specialist"),
    ))
    # Add an unrelated edge — must not appear.
    g.add_node(Operation(span_id="s1", trace_id="t1", type="chat"))
    g.add_edge(Edge(
        EdgeType.INVOKED.value,
        (NodeLabel.AGENT.value, "orchestrator"),
        (NodeLabel.OPERATION.value, "s1"),
    ))
    rows = agent_delegation_rows(g)
    assert rows == [{"parent_agent_id": "orchestrator", "child_agent_id": "specialist"}]


def test_ops_columns_constant_matches_schema_fields() -> None:
    """Sanity: every column we promise to upsert is in _OPS_COLUMNS exactly once."""
    assert len(_OPS_COLUMNS) == len(set(_OPS_COLUMNS))
    assert "span_id" in _OPS_COLUMNS
    # Must include all flattened structural columns.
    for col in (
        "session_id", "agent_id", "model_provider", "model_name",
        "tool_name", "data_source_id", "parent_span_id",
    ):
        assert col in _OPS_COLUMNS


# ---------------------------------------------------------------------------
# Integration — in-memory DuckDB round-trips
# ---------------------------------------------------------------------------

@pytest.fixture()
def sink():  # type: ignore[no-untyped-def]
    """A fresh in-memory DuckDB sink per test."""
    with DuckDBSink(":memory:") as s:
        s.ensure_schema()
        yield s


def _ops_count(sink: DuckDBSink) -> int:
    assert sink._con is not None
    return int(sink._con.execute("SELECT count(*) FROM ops").fetchone()[0])


def _all_dim_counts(sink: DuckDBSink) -> dict[str, int]:
    assert sink._con is not None
    return {
        t: int(sink._con.execute(f"SELECT count(*) FROM {t}").fetchone()[0])
        for t in (
            "sessions", "agents", "models", "tools", "data_sources",
            "resources", "agent_delegations", "ops",
        )
    }


@pytest.mark.parametrize(
    "fixture_name",
    [p.stem for p in sorted(FIXTURES_DIR.glob("*.json"))],
)
def test_fixture_round_trips_to_duckdb(sink: DuckDBSink, fixture_name: str) -> None:
    """Every fixture: mapper → sink → counts must match the graph's counts.

    Per-table assertions (instead of a single total) because the schema
    is denormalised — total rows in DuckDB is intentionally NOT equal to
    Neo4j node count.
    """
    path = FIXTURES_DIR / f"{fixture_name}.json"
    data = json.loads(path.read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])

    sink.write(graph)
    counts = _all_dim_counts(sink)

    # ops table = one row per Operation node.
    assert counts["ops"] == graph.node_count(NodeLabel.OPERATION.value), (
        f"[{fixture_name}] ops count {counts['ops']} != "
        f"Operation nodes {graph.node_count(NodeLabel.OPERATION.value)}"
    )
    # Dimension rows = node count for each label.
    assert counts["sessions"]     == graph.node_count(NodeLabel.SESSION.value)
    assert counts["agents"]       == graph.node_count(NodeLabel.AGENT.value)
    assert counts["models"]       == graph.node_count(NodeLabel.MODEL.value)
    assert counts["tools"]        == graph.node_count(NodeLabel.TOOL.value)
    assert counts["data_sources"] == graph.node_count(NodeLabel.DATA_SOURCE.value)
    assert counts["resources"]    == graph.node_count(NodeLabel.RESOURCE.value)
    # Delegation rows = DELEGATED_TO edge count.
    assert counts["agent_delegations"] == graph.edge_count(EdgeType.DELEGATED_TO.value)


def test_double_write_is_idempotent(sink: DuckDBSink) -> None:
    """Writing the same graph twice must not double row counts."""
    data = json.loads((FIXTURES_DIR / "agent_with_tool.json").read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])

    sink.write(graph)
    first = _all_dim_counts(sink)
    sink.write(graph)
    second = _all_dim_counts(sink)

    assert first == second


def test_select_cost_per_agent_one_liner(sink: DuckDBSink) -> None:
    """The README pitch: cost-per-agent in one SQL line, no joins required.

    This is the ergonomic claim that justifies the wide-ops shape over a
    graph mirror — locks it in as a contract.
    """
    data = json.loads((FIXTURES_DIR / "multi_agent.json").read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])
    sink.write(graph)

    assert sink._con is not None
    rows = sink._con.execute(
        "SELECT agent_id, "
        "       sum(coalesce(input_tokens, 0)  + coalesce(output_tokens, 0)) AS tokens "
        "FROM ops "
        "WHERE agent_id IS NOT NULL "
        "GROUP BY agent_id "
        "ORDER BY tokens DESC"
    ).fetchall()

    # At least one agent should show up (multi_agent.json has multiple).
    assert len(rows) >= 1
    # Every result row must have non-null agent_id and non-negative tokens.
    for agent_id, tokens in rows:
        assert isinstance(agent_id, str) and agent_id
        assert tokens is None or tokens >= 0


def test_persists_to_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A sink pointed at a file must survive close + reopen."""
    db = tmp_path / "trace.duckdb"
    data = json.loads((FIXTURES_DIR / "simple_llm_call.json").read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])

    with DuckDBSink(str(db)) as s:
        s.ensure_schema()
        s.write(graph)
        first = _all_dim_counts(s)

    assert db.exists()

    with DuckDBSink(str(db)) as s:
        # No ensure_schema needed — file already has the tables.
        second = _all_dim_counts(s)

    assert first == second
    assert second["ops"] == graph.node_count(NodeLabel.OPERATION.value)


def test_service_name_flows_through_mapper_and_sink(sink: DuckDBSink) -> None:
    """Operation rows must carry service_name from the OTLP resource attribute.

    Locks in the Resource→ops denormalisation: ops.service_name is the FK
    to the resources dim table, populated by the mapper from
    ``resourceSpans[*].resource.service.name``. Without this column,
    questions like "cost per service" would require joining through a
    side table that doesn't exist in DuckDB.
    """
    data = json.loads((FIXTURES_DIR / "agent_with_tool.json").read_text())
    graph = map_spans(data["otlp"]["resourceSpans"])
    sink.write(graph)

    assert sink._con is not None

    # Every Operation row should have a service_name set (the fixture has
    # a service.name resource attribute).
    null_svc = sink._con.execute(
        "SELECT count(*) FROM ops WHERE service_name IS NULL"
    ).fetchone()[0]
    assert null_svc == 0, "every op should carry service_name from its resource"

    # That service_name should match a row in the resources dim table.
    orphans = sink._con.execute(
        "SELECT count(*) FROM ops o "
        "LEFT JOIN resources r ON r.service_name = o.service_name "
        "WHERE r.service_name IS NULL"
    ).fetchone()[0]
    assert orphans == 0, "ops.service_name must be present in the resources dim table"


def test_satisfies_sink_protocol() -> None:
    """The Sink protocol from sink.py must be structurally satisfied."""
    from otel_genai_graph.sink import Sink
    s = DuckDBSink(":memory:")
    assert isinstance(s, Sink)
