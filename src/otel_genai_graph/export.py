"""Export a `Graph` (or a live Neo4j query result) as JSON / HTML / GraphML.

Three output formats, one source of truth (node-link JSON):

  * `to_node_link_json(graph) -> dict` — cytoscape / D3 / observable-friendly
  * `to_html(graph, …) -> str`         — self-contained interactive viewer
  * `to_graphml(graph) -> str`         — Gephi / yEd / NetworkX loadable

Plus:
  * `neo4j_result_to_graph(records) -> Graph` — consumes a neo4j driver
    `Result` iterable, unpacks Nodes / Relationships / Paths into a Graph
    you can feed to any of the exporters above.
  * `table_to_csv(rows) -> str`   and `table_to_ascii(rows) -> str` for
    `result_type == "table"` queries that don't have a graph shape.
"""
from __future__ import annotations

import csv
import html
import io
import json
from typing import Any, Iterable

from .schema import (
    Agent,
    DataSource,
    Edge,
    Graph,
    Model,
    NodeLabel,
    Operation,
    Session,
    Tool,
)


# ---------------------------------------------------------------------------
# Node-link JSON (source of truth for HTML + downstream tooling)
# ---------------------------------------------------------------------------

def _node_id(key: tuple[str, str]) -> str:
    return f"{key[0]}:{key[1]}"


def _node_props(node: Any) -> dict[str, Any]:
    """Extract the instance fields of a schema dataclass, filtering out None."""
    d: dict[str, Any] = {}
    for k, v in node.__dict__.items():
        if v is not None:
            d[k] = v
    return d


def to_node_link_json(graph: Graph) -> dict:
    """Emit `{"nodes": [...], "edges": [...]}` in a tool-agnostic shape."""
    nodes = []
    for key, node in graph.nodes.items():
        nodes.append(
            {
                "id": _node_id(key),
                "label": key[0],
                "properties": _node_props(node),
            }
        )
    edges = []
    for i, e in enumerate(graph.edges):
        edges.append(
            {
                "id": f"e{i}",
                "source": _node_id(e.src),
                "target": _node_id(e.dst),
                "type": e.edge_type,
                "properties": dict(e.properties) if e.properties else {},
            }
        )
    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# Self-contained HTML viewer (cytoscape.js from CDN, everything else inline)
# ---------------------------------------------------------------------------

# Palette matches docs/images/multi-agent-graph.svg.
_LABEL_COLORS: dict[str, tuple[str, str]] = {
    "Session":    ("#eef2ff", "#6366f1"),   # indigo
    "Agent":      ("#ecfdf5", "#10b981"),   # emerald
    "Operation":  ("#f0f9ff", "#0ea5e9"),   # sky
    "Model":      ("#fffbeb", "#f59e0b"),   # amber
    "Tool":       ("#fdf2f8", "#db2777"),   # pink
    "DataSource": ("#f5f3ff", "#7c3aed"),   # violet
    "Resource":   ("#f8fafc", "#64748b"),   # slate
}

_EDGE_COLOR_DEFAULT = "#64748b"
_EDGE_COLOR_DELEGATED = "#9333ea"  # same violet as the SVG


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>__TITLE__</title>
<style>
  html, body { margin: 0; padding: 0; height: 100%; font-family: -apple-system, system-ui, "Segoe UI", Roboto, sans-serif; background:#f8fafc; color:#0f172a; }
  header { padding: 12px 20px; border-bottom: 1px solid #e2e8f0; background: #fff; display:flex; align-items:center; gap:16px; }
  header h1 { margin: 0; font-size: 15px; font-weight: 600; }
  header .meta { font-size: 12px; color:#475569; }
  main { display: grid; grid-template-columns: 1fr 280px; height: calc(100% - 49px); }
  #cy { width: 100%; height: 100%; background: #fff; }
  aside { border-left: 1px solid #e2e8f0; background: #fff; padding: 16px; overflow:auto; font-size: 12px; }
  aside h2 { margin: 0 0 8px 0; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; color:#475569; }
  aside .legend { display: grid; grid-template-columns: 16px 1fr; row-gap: 6px; column-gap: 8px; align-items:center; margin-bottom: 20px; }
  aside .legend .swatch { width: 14px; height: 14px; border-radius: 3px; }
  aside pre { background:#f1f5f9; padding: 10px; border-radius: 6px; font-size: 11px; white-space: pre-wrap; word-break: break-all; max-height: 60vh; overflow:auto; }
  aside .empty { color:#94a3b8; font-style: italic; }
</style>
</head>
<body>
<header>
  <h1>__TITLE__</h1>
  <span class="meta">__SUMMARY__</span>
</header>
<main>
  <div id="cy"></div>
  <aside>
    <h2>Legend</h2>
    <div class="legend">__LEGEND__</div>
    <h2>Selection</h2>
    <div id="sel" class="empty">Click a node or edge to inspect.</div>
  </aside>
</main>
<script src="https://cdn.jsdelivr.net/npm/cytoscape@3.28.1/dist/cytoscape.min.js"></script>
<script>
const data = __DATA__;
const labelColors = __LABEL_COLORS__;

const elements = [];
for (const n of data.nodes) {
  elements.push({ data: { id: n.id, label: n.label, props: n.properties, display: shortLabel(n) } });
}
for (const e of data.edges) {
  elements.push({ data: { id: e.id, source: e.source, target: e.target, type: e.type, props: e.properties } });
}

function shortLabel(n) {
  const p = n.properties || {};
  // Pick the most human field per label.
  if (n.label === "Session")    return "Session\n" + (p.id ?? "");
  if (n.label === "Agent")      return "Agent\n" + (p.name ?? p.id ?? "");
  if (n.label === "Model")      return "Model\n" + (p.provider ?? "") + "/" + (p.name ?? "");
  if (n.label === "Tool")       return "Tool\n" + (p.name ?? "");
  if (n.label === "DataSource") return "DataSource\n" + (p.id ?? "");
  if (n.label === "Operation") {
    const oid = p.span_id ?? "";
    return "Operation\n" + (p.type ?? "") + "\n" + (oid.length > 12 ? oid.slice(0,8) + "…" : oid);
  }
  return n.label;
}

const styles = [
  { selector: "node", style: {
      "shape": "round-rectangle",
      "label": "data(display)",
      "text-wrap": "wrap",
      "text-max-width": 160,
      "font-size": 10,
      "padding": 8,
      "background-color": "#f8fafc",
      "border-width": 2,
      "border-color": "#64748b",
      "color": "#0f172a",
      "text-valign": "center",
      "width": "label", "height": "label",
  }},
  { selector: "edge", style: {
      "label": "data(type)",
      "font-size": 8,
      "color": "#475569",
      "curve-style": "bezier",
      "target-arrow-shape": "triangle",
      "target-arrow-color": "#64748b",
      "line-color": "#64748b",
      "width": 1.2,
      "text-background-color": "#ffffff",
      "text-background-opacity": 1,
      "text-background-padding": 2,
  }},
  { selector: "edge[type = 'PARENT_OF']", style: { "line-style": "dashed" } },
  { selector: "edge[type = 'DELEGATED_TO']", style: {
      "line-color": "__DELEGATED_COLOR__",
      "target-arrow-color": "__DELEGATED_COLOR__",
      "width": 2,
  }},
];
for (const [label, [fill, border]] of Object.entries(labelColors)) {
  styles.push({
    selector: `node[label = '${label}']`,
    style: { "background-color": fill, "border-color": border }
  });
}

const cy = cytoscape({
  container: document.getElementById("cy"),
  elements,
  style: styles,
  layout: { name: "cose", animate: false, padding: 30, nodeRepulsion: 9000 },
  wheelSensitivity: 0.2,
});

const sel = document.getElementById("sel");
cy.on("tap", "node, edge", (evt) => {
  const d = evt.target.data();
  const lines = [];
  for (const [k, v] of Object.entries(d)) {
    if (k === "display") continue;
    lines.push(k + ": " + (typeof v === "object" ? JSON.stringify(v, null, 2) : v));
  }
  sel.innerHTML = "<pre>" + lines.join("\n") + "</pre>";
  sel.classList.remove("empty");
});
cy.on("tap", (evt) => {
  if (evt.target === cy) {
    sel.innerHTML = "Click a node or edge to inspect.";
    sel.classList.add("empty");
  }
});
</script>
</body>
</html>
"""


def _legend_html(graph: Graph) -> str:
    present = sorted({key[0] for key in graph.nodes})
    lines = []
    for label in present:
        _fill, border = _LABEL_COLORS.get(label, ("#f8fafc", "#64748b"))
        lines.append(
            f'<span class="swatch" style="background:{border}"></span>'
            f'<span>{html.escape(label)}</span>'
        )
    return "".join(lines)


def to_html(
    graph: Graph,
    *,
    title: str = "otel-genai-graph",
) -> str:
    data = to_node_link_json(graph)
    summary = f"{graph.node_count()} nodes · {graph.edge_count()} edges"
    return (
        _HTML_TEMPLATE
        .replace("__TITLE__",       html.escape(title))
        .replace("__SUMMARY__",     html.escape(summary))
        .replace("__DATA__",        json.dumps(data))
        .replace("__LABEL_COLORS__", json.dumps(_LABEL_COLORS))
        .replace("__DELEGATED_COLOR__", _EDGE_COLOR_DELEGATED)
        .replace("__LEGEND__",      _legend_html(graph))
    )


# ---------------------------------------------------------------------------
# GraphML — NetworkX / Gephi / yEd friendly
# ---------------------------------------------------------------------------

def _xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&apos;")
    )


def to_graphml(graph: Graph) -> str:
    """Emit GraphML 1.0 with simple string-typed node/edge properties.

    Complex values are JSON-stringified into a single `properties_json`
    attribute — enough to round-trip and view in yEd / Gephi / NetworkX
    without depending on their more opinionated type systems.
    """
    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<graphml xmlns="http://graphml.graphdrawing.org/xmlns" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns '
        'http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">\n'
        '  <key id="label" for="node" attr.name="label" attr.type="string"/>\n'
        '  <key id="properties_json" for="node" attr.name="properties_json" attr.type="string"/>\n'
        '  <key id="etype" for="edge" attr.name="type" attr.type="string"/>\n'
        '  <graph id="G" edgedefault="directed">\n'
    )
    body = io.StringIO()
    body.write(header)
    for key, node in graph.nodes.items():
        nid = _xml_escape(_node_id(key))
        label = _xml_escape(key[0])
        props = _xml_escape(json.dumps(_node_props(node), sort_keys=True))
        body.write(
            f'    <node id="{nid}">\n'
            f'      <data key="label">{label}</data>\n'
            f'      <data key="properties_json">{props}</data>\n'
            f'    </node>\n'
        )
    for i, e in enumerate(graph.edges):
        body.write(
            f'    <edge id="e{i}" source="{_xml_escape(_node_id(e.src))}" '
            f'target="{_xml_escape(_node_id(e.dst))}">\n'
            f'      <data key="etype">{_xml_escape(e.edge_type)}</data>\n'
            f'    </edge>\n'
        )
    body.write('  </graph>\n</graphml>\n')
    return body.getvalue()


# ---------------------------------------------------------------------------
# Live-Neo4j → Graph (for `--from-neo4j`)
# ---------------------------------------------------------------------------

# Label → constructor args from Neo4j-node properties. Kept in sync with
# schema.py. If a query returns labels we don't recognise, we still
# materialise them as generic nodes (ensures --from-neo4j doesn't crash
# on custom schemas; they just won't re-serialise via schema dataclasses).
def _node_from_neo4j(label: str, props: dict) -> Any:
    if label == NodeLabel.SESSION.value:
        return Session(id=str(props["id"]))
    if label == NodeLabel.AGENT.value:
        return Agent(id=str(props["id"]), name=props.get("name"))
    if label == NodeLabel.MODEL.value:
        return Model(provider=str(props["provider"]), name=str(props["name"]))
    if label == NodeLabel.TOOL.value:
        return Tool(name=str(props["name"]))
    if label == NodeLabel.DATA_SOURCE.value:
        return DataSource(id=str(props["id"]), kind=props.get("kind"))
    if label == NodeLabel.OPERATION.value:
        return Operation(
            span_id=str(props["span_id"]),
            trace_id=str(props.get("trace_id", "")),
            type=str(props.get("type", "unknown")),
            status=str(props.get("status", "UNSET")),
            input_tokens=props.get("input_tokens"),
            output_tokens=props.get("output_tokens"),
            start_ns=props.get("start_ns"),
            end_ns=props.get("end_ns"),
            error_message=props.get("error_message"),
            service_name=props.get("service_name"),
        )
    # generic pass-through
    return _GenericNode(label=label, props=dict(props))


class _GenericNode:
    """Fallback when Neo4j returns a label we don't have a dataclass for."""
    def __init__(self, label: str, props: dict):
        self.label = label
        self.props = props

    @property
    def key(self) -> tuple[str, str]:
        # Best-effort: first "id"-ish property, else a stringified repr.
        for k in ("id", "name", "span_id"):
            if k in self.props:
                return (self.label, str(self.props[k]))
        return (self.label, json.dumps(self.props, sort_keys=True))

    @property
    def __dict__(self):  # type: ignore[override]
        return self.props


def _neo4j_label_of(node: Any) -> str:
    """Extract the primary label from a neo4j Node object."""
    labels = list(getattr(node, "labels", []) or [])
    return labels[0] if labels else "Unknown"


def neo4j_result_to_graph(records: Iterable[Any]) -> Graph:
    """Consume a neo4j driver Result iterator → a Graph.

    Handles query values that are Nodes, Relationships, or Paths (and
    any combination). Aggregation queries that return scalars belong in
    `table_rows` territory — use `result_type="table"` in the saved
    query, not this function.
    """
    from neo4j.graph import Node as _N4jNode  # lazy import
    from neo4j.graph import Path as _N4jPath
    from neo4j.graph import Relationship as _N4jRel

    graph = Graph()
    nodes_by_element_id: dict[str, tuple] = {}  # n4j element_id → our key

    def _add_node(n: Any) -> tuple[str, str]:
        label = _neo4j_label_of(n)
        props = dict(n) if hasattr(n, "__iter__") else {}
        schema_node = _node_from_neo4j(label, props)
        key = schema_node.key
        graph.add_node(schema_node)
        nodes_by_element_id[getattr(n, "element_id", id(n))] = key
        return key

    def _add_rel(r: Any) -> None:
        src = nodes_by_element_id.get(getattr(r.start_node, "element_id", id(r.start_node)))
        dst = nodes_by_element_id.get(getattr(r.end_node, "element_id", id(r.end_node)))
        if src is None:
            src = _add_node(r.start_node)
        if dst is None:
            dst = _add_node(r.end_node)
        graph.add_edge(Edge(edge_type=r.type, src=src, dst=dst))

    for record in records:
        for value in record.values():
            if value is None:
                continue
            if isinstance(value, _N4jPath):
                for n in value.nodes:
                    _add_node(n)
                for r in value.relationships:
                    _add_rel(r)
            elif isinstance(value, _N4jNode):
                _add_node(value)
            elif isinstance(value, _N4jRel):
                _add_rel(value)
            elif isinstance(value, list):
                # Some queries return lists of nodes/rels.
                for item in value:
                    if isinstance(item, _N4jNode):
                        _add_node(item)
                    elif isinstance(item, _N4jRel):
                        _add_rel(item)
                    elif isinstance(item, _N4jPath):
                        for n in item.nodes:
                            _add_node(n)
                        for r in item.relationships:
                            _add_rel(r)
            # scalars are silently ignored — table queries should have been
            # dispatched to the table exporters, not here.

    return graph


# ---------------------------------------------------------------------------
# Table exporters (for result_type="table" queries)
# ---------------------------------------------------------------------------

def table_to_csv(columns: list[str], rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    for r in rows:
        w.writerow([r.get(c, "") for c in columns])
    return buf.getvalue()


def table_to_jsonl(rows: list[dict]) -> str:
    return "\n".join(json.dumps(r, default=str, sort_keys=True) for r in rows) + "\n"


def table_to_ascii(columns: list[str], rows: list[dict]) -> str:
    """Simple ASCII rendering. Not as pretty as `rich` would give, but dep-free."""
    if not rows:
        return " (no rows)\n"
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    sep = "  "
    def fmt_row(r: dict) -> str:
        return sep.join(str(r.get(c, "")).ljust(widths[c]) for c in columns)
    lines = [
        sep.join(c.ljust(widths[c]) for c in columns),
        sep.join("-" * widths[c] for c in columns),
    ]
    lines.extend(fmt_row(r) for r in rows)
    return "\n".join(lines) + "\n"


def neo4j_result_to_table(records: Iterable[Any]) -> tuple[list[str], list[dict]]:
    """Buffer a Result into `(columns, rows)`."""
    records = list(records)
    if not records:
        return [], []
    columns = list(records[0].keys())
    rows = [dict(rec) for rec in records]
    return columns, rows
