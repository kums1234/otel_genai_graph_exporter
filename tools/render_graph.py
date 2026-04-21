#!/usr/bin/env python3
"""Render a fixture's graph as DOT / SVG for inclusion in docs.

Takes any OTLP/JSON fixture, runs it through the mapper, and emits a
Graphviz DOT description. If the `dot` binary is on PATH, also renders
SVG and PNG alongside.

Usage
-----
    python tools/render_graph.py tests/fixtures/multi_agent.json \\
        --output docs/images/multi-agent-graph

Produces `multi-agent-graph.dot` and (if `dot` is available)
`multi-agent-graph.svg` / `.png`.

Install Graphviz (optional, only needed for SVG/PNG rendering):
    brew install graphviz           # macOS
    apt-get install graphviz        # Debian / Ubuntu
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from otel_genai_graph.mapper import map_spans  # noqa: E402


_LABEL_STYLE = {
    "Session":    ("#eef2ff", "#6366f1"),   # indigo
    "Agent":      ("#ecfdf5", "#10b981"),   # emerald
    "Operation":  ("#f0f9ff", "#0ea5e9"),   # sky
    "Model":      ("#fffbeb", "#f59e0b"),   # amber
    "Tool":       ("#fdf2f8", "#db2777"),   # pink
    "DataSource": ("#f5f3ff", "#7c3aed"),   # violet
    "Resource":   ("#f8fafc", "#64748b"),   # slate
}

_EDGE_STYLE = {
    "CONTAINS":       ("#475569", "solid"),
    "EXECUTED":       ("#475569", "solid"),
    "INVOKED":        ("#475569", "solid"),
    "CALLED":         ("#475569", "solid"),
    "RETRIEVED_FROM": ("#475569", "solid"),
    "PARENT_OF":      ("#64748b", "dashed"),
    "DELEGATED_TO":   ("#9333ea", "solid"),
    "ACCESSED":       ("#94a3b8", "dotted"),
}


def _node_id(key: tuple[str, str]) -> str:
    # Graphviz disallows most punctuation in unquoted ids — we always quote.
    return f'"{key[0]}:{key[1]}"'


def _node_label(key: tuple[str, str]) -> str:
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
        fill, border = _LABEL_STYLE.get(key[0], ("#f8fafc", "#64748b"))
        lines.append(
            f'  {_node_id(key)} [label="{_node_label(key)}", '
            f'fillcolor="{fill}", color="{border}"];'
        )
    for e in graph.edges:
        color, style = _EDGE_STYLE.get(e.edge_type, ("#64748b", "solid"))
        lines.append(
            f'  {_node_id(e.src)} -> {_node_id(e.dst)} '
            f'[label="{e.edge_type}", color="{color}", style="{style}"];'
        )
    lines.append("}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("fixture", type=Path, help="OTLP/JSON fixture (same shape as tests/fixtures/*.json)")
    p.add_argument("--output", type=Path, required=True,
                   help="output path stem (extension added automatically)")
    p.add_argument("--formats", default="svg,png",
                   help="comma-separated list of rendered formats (requires `dot`)")
    args = p.parse_args(argv)

    data = json.loads(args.fixture.read_text())
    rs = data["otlp"]["resourceSpans"] if "otlp" in data else data["resourceSpans"]
    graph = map_spans(rs)
    dot_source = to_dot(graph, title=args.fixture.stem)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    dot_path = args.output.with_suffix(".dot")
    dot_path.write_text(dot_source)
    print(f"wrote {dot_path}", file=sys.stderr)

    dot_bin = shutil.which("dot")
    if dot_bin is None:
        print(
            "[note] `dot` not on PATH — only the .dot file was written. "
            "Install Graphviz (brew install graphviz / apt-get install graphviz) "
            "to render SVG/PNG.",
            file=sys.stderr,
        )
        return 0

    for fmt in (f.strip() for f in args.formats.split(",") if f.strip()):
        out_path = args.output.with_suffix(f".{fmt}")
        subprocess.run(
            [dot_bin, f"-T{fmt}", str(dot_path), "-o", str(out_path)],
            check=True,
        )
        print(f"wrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
