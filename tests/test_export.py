"""Tests for the export module (node-link JSON, HTML, GraphML, table)."""
from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from otel_genai_graph.export import (
    table_to_ascii,
    table_to_csv,
    table_to_jsonl,
    to_graphml,
    to_html,
    to_node_link_json,
)
from otel_genai_graph.mapper import map_spans

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def multi_agent_graph():  # type: ignore[no-untyped-def]
    data = json.loads((FIXTURES_DIR / "multi_agent.json").read_text())
    return map_spans(data["otlp"]["resourceSpans"])


# ---------------------------------------------------------------------------
# node-link JSON
# ---------------------------------------------------------------------------

def test_node_link_json_has_all_nodes_and_edges(multi_agent_graph) -> None:
    data = to_node_link_json(multi_agent_graph)
    assert len(data["nodes"]) == multi_agent_graph.node_count()
    assert len(data["edges"]) == multi_agent_graph.edge_count()


def test_node_link_json_edges_reference_real_node_ids(multi_agent_graph) -> None:
    data = to_node_link_json(multi_agent_graph)
    node_ids = {n["id"] for n in data["nodes"]}
    for e in data["edges"]:
        assert e["source"] in node_ids, f"dangling edge.source: {e}"
        assert e["target"] in node_ids, f"dangling edge.target: {e}"
        assert "type" in e
        assert e["type"] in {
            "CONTAINS", "EXECUTED", "INVOKED", "CALLED",
            "RETRIEVED_FROM", "PARENT_OF", "DELEGATED_TO", "ACCESSED",
        }


def test_node_link_json_is_valid_json(multi_agent_graph) -> None:
    serialised = json.dumps(to_node_link_json(multi_agent_graph))
    reloaded = json.loads(serialised)
    assert "nodes" in reloaded and "edges" in reloaded


def test_node_properties_exposed(multi_agent_graph) -> None:
    data = to_node_link_json(multi_agent_graph)
    models = [n for n in data["nodes"] if n["label"] == "Model"]
    assert models
    for m in models:
        assert m["properties"].get("provider")
        assert m["properties"].get("name")


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

def test_html_is_self_contained_and_embeds_data(multi_agent_graph) -> None:
    html_doc = to_html(multi_agent_graph, title="multi_agent")
    assert html_doc.startswith("<!DOCTYPE html>")
    # single-file: all our own content is inline (we only load cytoscape
    # from CDN — count should be exactly one script src).
    assert html_doc.count("<script src=") == 1
    # payload is embedded as a JS literal, not fetched
    assert "const data =" in html_doc
    # sanity: the embedded data parses back to the same shape
    m = re.search(r"const data = (\{.*?\});", html_doc, re.DOTALL)
    assert m, "embedded data block not found"
    reloaded = json.loads(m.group(1))
    assert len(reloaded["nodes"]) == multi_agent_graph.node_count()


def test_html_title_shown(multi_agent_graph) -> None:
    html_doc = to_html(multi_agent_graph, title="my-special-title")
    assert "my-special-title" in html_doc


def test_html_legend_lists_labels_present_in_graph(multi_agent_graph) -> None:
    html_doc = to_html(multi_agent_graph)
    for expected in ("Session", "Agent", "Model", "Operation"):
        assert f">{expected}<" in html_doc


# ---------------------------------------------------------------------------
# GraphML
# ---------------------------------------------------------------------------

def test_graphml_parses_as_xml_and_has_right_counts(multi_agent_graph) -> None:
    xml = to_graphml(multi_agent_graph)
    root = ET.fromstring(xml)  # must parse
    ns = "{http://graphml.graphdrawing.org/xmlns}"
    graph = root.find(f"{ns}graph")
    assert graph is not None
    node_elems = graph.findall(f"{ns}node")
    edge_elems = graph.findall(f"{ns}edge")
    assert len(node_elems) == multi_agent_graph.node_count()
    assert len(edge_elems) == multi_agent_graph.edge_count()


def test_graphml_escapes_special_characters() -> None:
    from otel_genai_graph.schema import Edge, Graph, Session
    g = Graph()
    # An id containing characters that would corrupt XML if not escaped.
    weird_id = 'conv<&>"demo\''
    g.add_node(Session(id=weird_id))
    xml = to_graphml(g)
    # Must still parse
    ET.fromstring(xml)
    # Literal '<' and raw '&' MUST NOT appear in an attribute value;
    # they should be entity-encoded
    assert "&lt;" in xml
    assert "&amp;" in xml


# ---------------------------------------------------------------------------
# Table exporters
# ---------------------------------------------------------------------------

_TABLE_COLS = ["provider", "model", "calls", "input_tokens"]
_TABLE_ROWS = [
    {"provider": "anthropic", "model": "claude-sonnet-4-5", "calls": 3, "input_tokens": 1200},
    {"provider": "openai",    "model": "gpt-4o-mini",        "calls": 1, "input_tokens": 50},
]


def test_csv_has_header_and_rows() -> None:
    out = table_to_csv(_TABLE_COLS, _TABLE_ROWS)
    lines = out.strip().splitlines()
    assert lines[0] == ",".join(_TABLE_COLS)
    assert "anthropic" in lines[1]
    assert len(lines) == 1 + len(_TABLE_ROWS)


def test_jsonl_round_trips_row_count() -> None:
    out = table_to_jsonl(_TABLE_ROWS)
    lines = [l for l in out.splitlines() if l]
    assert len(lines) == len(_TABLE_ROWS)
    for line, expected in zip(lines, _TABLE_ROWS):
        assert json.loads(line) == {**expected}


def test_ascii_table_has_separator_and_contains_values() -> None:
    out = table_to_ascii(_TABLE_COLS, _TABLE_ROWS)
    assert "---" in out  # divider row
    assert "anthropic" in out
    assert "gpt-4o-mini" in out


def test_ascii_table_empty_rows() -> None:
    assert "no rows" in table_to_ascii(_TABLE_COLS, [])
