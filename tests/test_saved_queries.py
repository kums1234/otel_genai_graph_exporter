"""Consistency tests for the saved-query library + live smoke-run."""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

from otel_genai_graph.saved_queries import (
    GRAPH,
    TABLE,
    QUERIES,
    SavedQuery,
    get_query,
    list_queries,
    validate_params,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Library-level invariants (run without Neo4j)
# ---------------------------------------------------------------------------

def test_query_names_are_snake_case_and_unique() -> None:
    seen: set[str] = set()
    for q in QUERIES.values():
        assert re.fullmatch(r"[a-z][a-z0-9_]*", q.name), f"bad name: {q.name!r}"
        assert q.name not in seen
        seen.add(q.name)


def test_every_query_has_valid_result_type() -> None:
    for q in QUERIES.values():
        assert q.result_type in (GRAPH, TABLE), q.name


def test_every_query_has_description_and_tags() -> None:
    for q in QUERIES.values():
        assert q.description.strip()
        # tags are optional but tuple-typed
        assert isinstance(q.tags, tuple)


def test_declared_parameters_match_cypher_tokens() -> None:
    """Any $param in the Cypher must have a matching Parameter (and vice versa)."""
    for q in QUERIES.values():
        cypher_params = set(re.findall(r"\$([a-zA-Z_][a-zA-Z0-9_]*)", q.cypher))
        declared = q.param_names()
        assert declared == cypher_params, (
            f"query {q.name!r}: declared params {declared} "
            f"!= cypher tokens {cypher_params}"
        )


def test_required_parameter_enforcement() -> None:
    for q in QUERIES.values():
        missing = [p.name for p in q.parameters if p.required and p.default is None]
        if not missing:
            continue
        with pytest.raises(ValueError, match="missing required"):
            validate_params(q, {})


def test_default_parameters_fill_in() -> None:
    from otel_genai_graph.saved_queries import Parameter
    q = SavedQuery(
        name="_probe", description="…", result_type=GRAPH, cypher="RETURN $x",
        parameters=(Parameter(name="x", description="", required=False, default="hi"),),
    )
    resolved = validate_params(q, {})
    assert resolved == {"x": "hi"}


def test_unknown_param_is_rejected() -> None:
    q = get_query("session_tree")
    with pytest.raises(ValueError, match="unknown parameter"):
        validate_params(q, {"session_id": "x", "bogus": "y"})


def test_list_queries_tag_filter() -> None:
    all_queries = list_queries()
    cost_queries = list_queries(tag="cost")
    assert len(cost_queries) < len(all_queries)
    assert all("cost" in q.tags for q in cost_queries)


def test_get_query_unknown_name_raises() -> None:
    with pytest.raises(KeyError):
        get_query("no_such_query")


# ---------------------------------------------------------------------------
# Integration smoke test — each query actually runs against live Neo4j
# ---------------------------------------------------------------------------

_NEO4J_URI = os.environ.get("NEO4J_URI")
skip_if_no_neo4j = pytest.mark.skipif(
    _NEO4J_URI is None,
    reason="set NEO4J_URI to run the saved-query smoke tests",
)


@pytest.fixture(scope="module")
def loaded_sink():  # type: ignore[no-untyped-def]
    """Load every hand-written fixture so queries have something to hit."""
    assert _NEO4J_URI is not None
    from otel_genai_graph.mapper import map_spans
    from otel_genai_graph.neo4j_sink import Neo4jSink

    sink = Neo4jSink(
        _NEO4J_URI,
        os.environ.get("NEO4J_USER", "neo4j"),
        os.environ.get("NEO4J_PASSWORD", "testtest"),
        os.environ.get("NEO4J_DATABASE", "neo4j"),
    )
    sink.connect()
    sink.ensure_schema()

    with sink._driver.session(database=sink.database) as sess:  # type: ignore[union-attr]
        sess.run("MATCH (n) DETACH DELETE n")

    for path in sorted(FIXTURES_DIR.glob("*.json")):
        data = json.loads(path.read_text())
        sink.write(map_spans(data["otlp"]["resourceSpans"]))

    yield sink
    sink.close()


@skip_if_no_neo4j
@pytest.mark.parametrize("name", sorted(QUERIES))
def test_saved_query_runs_against_live_neo4j(loaded_sink, name: str) -> None:
    """Every saved query executes without error and returns a sane shape."""
    from otel_genai_graph.export import neo4j_result_to_graph, neo4j_result_to_table

    q = get_query(name)

    # Supply dummy values for any required parameter. `session_tree` wants
    # a real session — pick one we know the fixtures created.
    params: dict[str, str] = {}
    for p in q.parameters:
        if not p.required:
            continue
        if p.name == "session_id":
            params[p.name] = "conv-3"  # from multi_agent.json
        else:
            params[p.name] = p.example or "x"

    with loaded_sink._driver.session(database=loaded_sink.database) as sess:  # type: ignore[union-attr]
        records = list(sess.run(q.cypher, **params))

    if q.result_type == GRAPH:
        g = neo4j_result_to_graph(records)
        # Most graph queries should produce something against the fixture
        # set. session_tree for conv-3 definitely does; agent_delegation
        # does because multi_agent has a delegation edge.
        if name in {"agent_delegation", "overview", "session_tree"}:
            assert g.node_count() >= 1, f"{name} produced an empty graph"
    else:  # table
        cols, rows = neo4j_result_to_table(records)
        assert cols, f"{name} returned no columns"
