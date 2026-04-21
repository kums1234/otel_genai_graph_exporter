#!/usr/bin/env python3
"""Render a graph from a fixture or a live Neo4j query, as DOT / JSON / HTML / GraphML / table.

Two input modes:

  (1) --fixture path.json            → run through mapper → export
  (2) --from-neo4j {--query NAME | --cypher STRING}
                                     → run against a live Neo4j → export

Discovery:

  --list-queries               list every saved query
  --list-queries --tag cost    filter by tag
  --describe-query NAME        show description + parameters + Cypher

Examples
--------
  # a fixture, all graph-shaped outputs
  python tools/render_graph.py --fixture tests/fixtures/multi_agent.json \\
      --output docs/images/multi-agent-graph --format all

  # live query by saved name → interactive HTML
  python tools/render_graph.py --from-neo4j \\
      --query session_tree --param session_id=conv-3 \\
      --output /tmp/session --format html

  # cost roll-up as ASCII table
  python tools/render_graph.py --from-neo4j --query cost_by_model --format table

  # custom Cypher one-off
  python tools/render_graph.py --from-neo4j \\
      --cypher "MATCH (a:Agent)-[:DELEGATED_TO]->(b:Agent) RETURN a, b" \\
      --output /tmp/delegations --format html
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from otel_genai_graph.export import (  # noqa: E402
    neo4j_result_to_graph,
    neo4j_result_to_table,
    table_to_ascii,
    table_to_csv,
    table_to_jsonl,
    to_graphml,
    to_html,
    to_node_link_json,
)
from otel_genai_graph.mapper import map_spans  # noqa: E402
from otel_genai_graph.saved_queries import (  # noqa: E402
    GRAPH,
    TABLE,
    QUERIES,
    SavedQuery,
    get_query,
    list_queries,
    validate_params,
)


# ---------------------------------------------------------------------------
# DOT emitter (kept for legacy callers)
# ---------------------------------------------------------------------------

_LABEL_DOT_STYLE = {
    "Session":    ("#eef2ff", "#6366f1"),
    "Agent":      ("#ecfdf5", "#10b981"),
    "Operation":  ("#f0f9ff", "#0ea5e9"),
    "Model":      ("#fffbeb", "#f59e0b"),
    "Tool":       ("#fdf2f8", "#db2777"),
    "DataSource": ("#f5f3ff", "#7c3aed"),
    "Resource":   ("#f8fafc", "#64748b"),
}

_EDGE_DOT_STYLE = {
    "PARENT_OF":    ("#64748b", "dashed"),
    "DELEGATED_TO": ("#9333ea", "solid"),
    "ACCESSED":     ("#94a3b8", "dotted"),
}


def _dot_node_id(key: tuple[str, str]) -> str:
    return f'"{key[0]}:{key[1]}"'


def _dot_node_label(key: tuple[str, str]) -> str:
    label, nat_id = key
    short = nat_id if len(nat_id) <= 28 else nat_id[:12] + "…" + nat_id[-12:]
    return f"{label}\\n{short}"


def to_dot(graph, title: str = "graph") -> str:
    lines = [
        f'digraph "{title}" {{',
        '  rankdir=LR;',
        '  node [shape=box, style="rounded,filled", fontname="-apple-system,Helvetica,sans-serif", fontsize=11, margin="0.2,0.08"];',
        '  edge [fontname="-apple-system,Helvetica,sans-serif", fontsize=9];',
    ]
    for key in graph.nodes:
        fill, border = _LABEL_DOT_STYLE.get(key[0], ("#f8fafc", "#64748b"))
        lines.append(
            f'  {_dot_node_id(key)} [label="{_dot_node_label(key)}", '
            f'fillcolor="{fill}", color="{border}"];'
        )
    for e in graph.edges:
        color, style = _EDGE_DOT_STYLE.get(e.edge_type, ("#64748b", "solid"))
        lines.append(
            f'  {_dot_node_id(e.src)} -> {_dot_node_id(e.dst)} '
            f'[label="{e.edge_type}", color="{color}", style="{style}"];'
        )
    lines.append("}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Input: fixture → Graph
# ---------------------------------------------------------------------------

def _graph_from_fixture(path: Path):  # type: ignore[no-untyped-def]
    data = json.loads(path.read_text())
    rs = data["otlp"]["resourceSpans"] if "otlp" in data else data["resourceSpans"]
    return map_spans(rs)


# ---------------------------------------------------------------------------
# Input: live Neo4j → Graph | (columns, rows)
# ---------------------------------------------------------------------------

def _neo4j_driver():  # type: ignore[no-untyped-def]
    from neo4j import GraphDatabase

    uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD", "test")
    return GraphDatabase.driver(uri, auth=(user, password))


def _run_cypher(
    cypher: str, params: dict, result_type: str
):  # type: ignore[no-untyped-def]
    """Run a Cypher against live Neo4j; return Graph or (cols, rows)."""
    database = os.environ.get("NEO4J_DATABASE", "neo4j")
    driver = _neo4j_driver()
    try:
        with driver.session(database=database) as sess:
            result = sess.run(cypher, **params)
            records = list(result)
        if result_type == TABLE:
            return neo4j_result_to_table(records)
        return neo4j_result_to_graph(records)
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Output dispatch
# ---------------------------------------------------------------------------

_GRAPH_FORMATS = {"dot", "svg", "png", "json", "html", "graphml", "all"}
_TABLE_FORMATS = {"csv", "jsonl", "table"}


def _write_graph_formats(graph, out_stem: Path, formats: list[str], title: str) -> None:
    """Write one-or-more graph formats rooted at `out_stem`."""
    if "all" in formats:
        formats = ["dot", "json", "html", "graphml"]  # svg/png only if dot exists
    out_stem.parent.mkdir(parents=True, exist_ok=True)

    dot_source: Optional[str] = None
    if any(f in formats for f in ("dot", "svg", "png")):
        dot_source = to_dot(graph, title=title)

    if "dot" in formats and dot_source is not None:
        p = out_stem.with_suffix(".dot")
        p.write_text(dot_source)
        print(f"wrote {p}", file=sys.stderr)

    for fmt in ("svg", "png"):
        if fmt not in formats or dot_source is None:
            continue
        dot_bin = shutil.which("dot")
        if dot_bin is None:
            print(
                f"[note] skipping --format {fmt}: install graphviz (`brew install graphviz` "
                "/ `apt-get install graphviz`) for SVG/PNG rendering.",
                file=sys.stderr,
            )
            continue
        dot_path = out_stem.with_suffix(".dot")
        if not dot_path.exists():
            dot_path.write_text(dot_source)
        out_path = out_stem.with_suffix(f".{fmt}")
        subprocess.run([dot_bin, f"-T{fmt}", str(dot_path), "-o", str(out_path)], check=True)
        print(f"wrote {out_path}", file=sys.stderr)

    if "json" in formats:
        p = out_stem.with_suffix(".json")
        p.write_text(json.dumps(to_node_link_json(graph), indent=2))
        print(f"wrote {p}", file=sys.stderr)

    if "html" in formats:
        p = out_stem.with_suffix(".html")
        p.write_text(to_html(graph, title=title))
        print(f"wrote {p}", file=sys.stderr)

    if "graphml" in formats:
        p = out_stem.with_suffix(".graphml")
        p.write_text(to_graphml(graph))
        print(f"wrote {p}", file=sys.stderr)


def _write_table_format(
    columns: list[str], rows: list[dict], out_stem: Optional[Path], fmt: str
) -> None:
    if fmt == "csv":
        text = table_to_csv(columns, rows)
    elif fmt == "jsonl":
        text = table_to_jsonl(rows)
    elif fmt == "table":
        text = table_to_ascii(columns, rows)
    else:
        raise ValueError(f"unknown table format {fmt!r}")
    if out_stem is None:
        sys.stdout.write(text)
    else:
        suffix = {"csv": ".csv", "jsonl": ".jsonl", "table": ".txt"}[fmt]
        p = out_stem.with_suffix(suffix)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        print(f"wrote {p}", file=sys.stderr)


# ---------------------------------------------------------------------------
# --list-queries / --describe-query rendering
# ---------------------------------------------------------------------------

def _print_query_catalogue(tag: Optional[str]) -> int:
    rows = list_queries(tag=tag)
    if not rows:
        print(f"no saved queries (filtered by tag={tag!r})", file=sys.stderr)
        return 1
    name_w = max(len(q.name) for q in rows)
    for q in sorted(rows, key=lambda x: x.name):
        tags = ",".join(q.tags) or "-"
        print(f"  {q.name.ljust(name_w)}  [{q.result_type:5}]  {tags:20}  {q.description}")
    return 0


def _describe_query(name: str) -> int:
    q = get_query(name)
    print(f"{q.name}  ({q.result_type})")
    print(f"  {q.description}")
    if q.tags:
        print(f"  tags: {', '.join(q.tags)}")
    if q.parameters:
        print("  parameters:")
        for p in q.parameters:
            req = "required" if p.required else f"optional (default: {p.default!r})"
            eg = f" [eg {p.example!r}]" if p.example else ""
            print(f"    --param {p.name}=…    {req}{eg}  — {p.description}")
    print("  cypher:")
    for line in q.cypher.strip().splitlines():
        print(f"    {line}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_params(items: Optional[list[str]]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"--param expects key=value; got {item!r}")
        k, v = item.split("=", 1)
        params[k.strip()] = v
    return params


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )

    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--fixture", type=Path, help="OTLP/JSON fixture to render")
    mode.add_argument("--from-neo4j", action="store_true",
                      help="source the graph from a live Neo4j via --query or --cypher")
    mode.add_argument("--list-queries", action="store_true", help="list the saved-query library")
    mode.add_argument("--describe-query", metavar="NAME",
                      help="show full spec + Cypher for one saved query")

    p.add_argument("--query", help="saved query name (with --from-neo4j)")
    p.add_argument("--cypher", help="raw Cypher string (with --from-neo4j)")
    p.add_argument("--param", action="append",
                   help="saved-query parameter, repeatable. e.g. --param session_id=conv-3")
    p.add_argument("--tag", help="filter --list-queries by tag")

    p.add_argument("--output", type=Path, default=None,
                   help="output path stem (extension added by --format). "
                        "Table outputs default to stdout when omitted.")
    p.add_argument("--format", default="all",
                   help=f"graph formats: {sorted(_GRAPH_FORMATS - {'all'})} or 'all'. "
                        f"table formats: {sorted(_TABLE_FORMATS)}.")

    args = p.parse_args(argv)

    # ── discovery commands ───────────────────────────────────────────────
    if args.list_queries:
        return _print_query_catalogue(args.tag)
    if args.describe_query:
        return _describe_query(args.describe_query)

    # ── positional-less invocations ──────────────────────────────────────
    if not args.fixture and not args.from_neo4j:
        # Back-compat: one positional arg = a fixture.
        remaining = argv if argv is not None else sys.argv[1:]
        positional = [a for a in remaining if not a.startswith("-")]
        if len(positional) == 1:
            args.fixture = Path(positional[0])
        else:
            p.error("pass --fixture PATH or --from-neo4j …")

    # ── gather the Graph or table ────────────────────────────────────────
    graph = None
    table: Optional[tuple[list[str], list[dict]]] = None
    title = "graph"

    if args.fixture:
        graph = _graph_from_fixture(args.fixture)
        title = args.fixture.stem

    else:  # --from-neo4j
        if bool(args.query) == bool(args.cypher):
            p.error("--from-neo4j requires exactly one of --query NAME or --cypher STRING")
        if args.query:
            q = get_query(args.query)
            params = validate_params(q, _parse_params(args.param))
            if q.result_type == TABLE:
                table = _run_cypher(q.cypher, params, TABLE)
            else:
                graph = _run_cypher(q.cypher, params, GRAPH)
            title = f"query:{q.name}"
        else:
            # raw Cypher; we can't know result_type, try graph first and fall
            # back to table if the result had no nodes/rels.
            graph = _run_cypher(args.cypher, _parse_params(args.param), GRAPH)
            title = "cypher:adhoc"

    # ── dispatch output ──────────────────────────────────────────────────
    formats = [f.strip() for f in args.format.split(",") if f.strip()]
    is_table = table is not None

    if is_table:
        assert table is not None
        cols, rows = table
        # Default table format when caller didn't specify one
        if formats == ["all"]:
            formats = ["table"]
        for fmt in formats:
            if fmt not in _TABLE_FORMATS:
                p.error(
                    f"query returned a table; --format must be one of {sorted(_TABLE_FORMATS)} "
                    f"(got {fmt!r})"
                )
            _write_table_format(cols, rows, args.output, fmt)
        return 0

    # graph path
    assert graph is not None
    if args.output is None:
        p.error("graph outputs need --output STEM (extension added automatically)")

    for fmt in formats:
        if fmt != "all" and fmt not in _GRAPH_FORMATS:
            p.error(
                f"query returned a graph; --format must be one of {sorted(_GRAPH_FORMATS)} "
                f"(got {fmt!r})"
            )
    _write_graph_formats(graph, args.output, formats, title)
    return 0


if __name__ == "__main__":
    sys.exit(main())
