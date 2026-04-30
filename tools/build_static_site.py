#!/usr/bin/env python3
"""Generate the static GitHub Pages site for otel-genai-graph.

Walks `tests/fixtures/*.json` and `examples/*.json`, runs each through
the mapper, and emits a self-contained website:

  docs/_site/
  ├── index.html            landing page (hero + cards + tables)
  ├── style.css             hand-rolled, no external CSS framework
  ├── fixtures/<name>.html  per-fixture interactive cytoscape.js viewer
  ├── fixtures/<name>.json  node-link JSON for each
  ├── examples/<name>.html
  ├── examples/<name>.json
  └── images/               copied from docs/images/

Local preview:
    python tools/build_static_site.py
    open docs/_site/index.html

CI runs this exact script via `.github/workflows/pages.yml` — the
artefact you preview locally is byte-identical to what ships.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from otel_genai_graph.export import to_html, to_node_link_json  # noqa: E402
from otel_genai_graph.mapper import map_spans                  # noqa: E402
from otel_genai_graph.schema import Graph                       # noqa: E402

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
EXAMPLES_DIR = REPO_ROOT / "examples"
IMAGES_DIR = REPO_ROOT / "docs" / "images"
DEFAULT_OUT = REPO_ROOT / "docs" / "_site"

REPO_URL = "https://github.com/kums1234/otel_genai_graph_exporter"

# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load_graph(path: Path):  # type: ignore[no-untyped-def]
    try:
        d = json.loads(path.read_text())
    except Exception:
        return None
    rs = d.get("otlp", {}).get("resourceSpans") or d.get("resourceSpans")
    if not rs:
        return None
    return map_spans(rs)


# Curated order — illustrative-first.
_FIXTURE_ORDER = [
    "simple_llm_call",
    "multi_agent",
    "agent_with_tool",
    "rag_flow",
    "multi_turn_conversation",
    "error_case",
]

DESCRIPTIONS: dict[str, str] = {
    "simple_llm_call":         "Smallest possible graph. One chat span. Three nodes total — Session, Model, Operation.",
    "multi_agent":             "Orchestrator delegates to a specialist. Shows the DELEGATED_TO edge that distinguishes multi-agent traces from a single timeline.",
    "agent_with_tool":         "An agent invokes an LLM, calls a tool, then invokes the LLM again. Classic ReAct shape.",
    "rag_flow":                "Embeddings → retrieve from a vector store → chat with retrieved context. Exercises DataSource and RETRIEVED_FROM.",
    "multi_turn_conversation": "Three chat turns sharing one conversation.id across distinct trace_ids — collapsed to a single Session node.",
    "error_case":              "A tool fails. The ERROR status propagates up the parent chain to every ancestor.",
    "real_gemini_chat":        "Real Gemini 2.5 Flash chat captured via the bundled capture script. Sanitised conversation id; token counts and response preview authentic.",
}


def discover_inputs() -> list[tuple[str, Path]]:
    """Return ordered (kind, path) pairs. kind is 'fixtures' or 'examples'."""
    out: list[tuple[str, Path]] = []
    for name in _FIXTURE_ORDER:
        p = FIXTURES_DIR / f"{name}.json"
        if p.exists():
            out.append(("fixtures", p))
    if EXAMPLES_DIR.exists():
        for p in sorted(EXAMPLES_DIR.glob("*.json")):
            out.append(("examples", p))
    return out


# ---------------------------------------------------------------------------
# Aggregations across the union of all loaded graphs
# ---------------------------------------------------------------------------

def _merge_graphs(graphs: list[Graph]) -> Graph:
    merged = Graph()
    for g in graphs:
        merged.nodes.update(g.nodes)
        merged.edges.update(g.edges)
    return merged


def cost_by_model(graph: Graph) -> tuple[list[str], list[list[Any]]]:
    by_model: dict[tuple[str, str], list[int]] = {}
    for e in graph.edges:
        if e.edge_type != "EXECUTED":
            continue
        op = graph.nodes.get(e.src)
        if op is None:
            continue
        provider, name = e.dst[1].split("/", 1)
        agg = by_model.setdefault((provider, name), [0, 0, 0])
        agg[0] += 1
        agg[1] += int(getattr(op, "input_tokens", 0) or 0)
        agg[2] += int(getattr(op, "output_tokens", 0) or 0)
    rows = sorted(
        [[p, m, c, i, o] for (p, m), (c, i, o) in by_model.items()],
        key=lambda r: -r[2],
    )
    return ["provider", "model", "calls", "input_tokens", "output_tokens"], rows


def tool_usage(graph: Graph) -> tuple[list[str], list[list[Any]]]:
    by_tool: dict[str, int] = {}
    for e in graph.edges:
        if e.edge_type != "CALLED":
            continue
        by_tool[e.dst[1]] = by_tool.get(e.dst[1], 0) + 1
    rows = sorted([[k, v] for k, v in by_tool.items()], key=lambda r: -r[1])
    return ["tool", "calls"], rows


def provider_distribution(graph: Graph) -> tuple[list[str], list[list[Any]]]:
    by_provider: dict[str, int] = {}
    for e in graph.edges:
        if e.edge_type != "EXECUTED":
            continue
        provider = e.dst[1].split("/", 1)[0]
        by_provider[provider] = by_provider.get(provider, 0) + 1
    rows = sorted([[k, v] for k, v in by_provider.items()], key=lambda r: -r[1])
    return ["provider", "calls"], rows


def _ascii_table(cols: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "(empty)"
    widths = [
        max(len(str(c)), max((len(str(r[i])) for r in rows), default=0))
        for i, c in enumerate(cols)
    ]
    sep = "  ".join("-" * w for w in widths)
    head = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    body = "\n".join(
        "  ".join(str(r[i]).ljust(widths[i]) for i in range(len(cols)))
        for r in rows
    )
    return f"{head}\n{sep}\n{body}"


# ---------------------------------------------------------------------------
# HTML generation — hand-rolled CSS so the build has zero front-end deps
# ---------------------------------------------------------------------------

CSS = """\
:root {
  --bg: #0f172a;
  --surface: #1e293b;
  --surface-2: #334155;
  --text: #e2e8f0;
  --muted: #94a3b8;
  --accent: #6366f1;
  --accent-hover: #4f46e5;
  --border: #334155;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 32px 20px; }
header.hero { text-align: center; padding: 48px 20px 16px; }
h1 { font-size: 2.4rem; font-weight: 700; margin-bottom: 12px; letter-spacing: -0.02em; }
h1 .accent { color: var(--accent); }
.tagline { color: var(--muted); font-size: 1.05rem; max-width: 720px; margin: 0 auto 24px; }
.cta { display: inline-flex; gap: 12px; flex-wrap: wrap; justify-content: center; }
.btn {
  display: inline-block; padding: 10px 18px;
  background: var(--accent); color: white; text-decoration: none;
  border-radius: 6px; font-weight: 500; transition: background 0.15s;
}
.btn:hover { background: var(--accent-hover); }
.btn.secondary { background: transparent; border: 1px solid var(--border); color: var(--text); }
.btn.secondary:hover { background: var(--surface); }
section { margin: 48px 0; }
h2 {
  font-size: 1.4rem; font-weight: 600; margin-bottom: 8px;
  display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap;
}
h2 .small { font-weight: 400; color: var(--muted); font-size: 0.95rem; }
.section-lead { color: var(--muted); margin-bottom: 20px; }
.grid {
  display: grid; gap: 16px; grid-template-columns: 1fr;
}
@media (min-width: 720px)  { .grid { grid-template-columns: repeat(2, 1fr); } }
@media (min-width: 1000px) { .grid { grid-template-columns: repeat(3, 1fr); } }
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 18px;
  display: flex; flex-direction: column; gap: 8px;
  transition: border-color 0.15s, transform 0.15s;
}
.card:hover { border-color: var(--accent); transform: translateY(-2px); }
.card h3 { font-size: 1rem; font-weight: 600; }
.card .desc { color: var(--muted); font-size: 0.9rem; flex: 1; }
.card .stat {
  font-size: 0.78rem; color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
.card .actions { display: flex; gap: 8px; margin-top: 8px; }
.card .actions a {
  padding: 6px 10px; font-size: 0.85rem;
  border: 1px solid var(--border); color: var(--text);
  text-decoration: none; border-radius: 4px;
}
.card .actions a:hover { background: var(--surface-2); }
.card .actions a.primary { background: var(--accent); border-color: var(--accent); }
.card .actions a.primary:hover { background: var(--accent-hover); }
pre.table {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px 20px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.85rem;
  overflow-x: auto; color: var(--text);
}
.demo-img { max-width: 100%; height: auto; border-radius: 8px; margin-top: 16px; box-shadow: 0 8px 32px rgba(0,0,0,0.2); }
footer { text-align: center; color: var(--muted); padding: 32px 20px; font-size: 0.9rem; }
footer a { color: var(--muted); }
.kbd {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.85rem; background: var(--surface-2);
  padding: 2px 6px; border-radius: 4px;
}
"""


def _humanize(stem: str) -> str:
    return stem.replace("_", " ")


def _card_html(*, title: str, desc: str, stat: str, html_link: str, json_link: str) -> str:
    return f"""\
    <div class="card">
      <h3>{title}</h3>
      <p class="desc">{desc}</p>
      <div class="stat">{stat}</div>
      <div class="actions">
        <a href="{html_link}" class="primary">Open graph →</a>
        <a href="{json_link}" download>JSON</a>
      </div>
    </div>"""


def render_index(stats: dict, cards_html: str, tables: tuple[str, str, str]) -> str:
    cost_table, tool_table, prov_table = tables
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>otel-genai-graph — interactive demo</title>
<meta name="description" content="OpenTelemetry GenAI spans → queryable Neo4j graph. Mixed-vendor cost attribution, agent delegation, tool blast radius — interactive demos.">
<meta property="og:title" content="otel-genai-graph">
<meta property="og:description" content="Turn your OpenTelemetry GenAI traces into a graph you can ask questions of. Cost per model, agent delegation, tool blast radius — one command, zero Cypher.">
<meta property="og:image" content="images/multi-agent-graph.svg">
<meta name="twitter:card" content="summary_large_image">
<link rel="stylesheet" href="style.css">
<link rel="icon" href="images/multi-agent-graph.svg" type="image/svg+xml">
</head>
<body>
<div class="container">
  <header class="hero">
    <h1>otel-genai-<span class="accent">graph</span></h1>
    <p class="tagline">
      Turn your OpenTelemetry GenAI traces into a graph you can actually ask
      questions of. Cost per model, agent delegation, tool blast radius — one
      command, zero Cypher, mixed vendors, real tokens.
    </p>
    <div class="cta">
      <a class="btn" href="{REPO_URL}">⭐ View on GitHub</a>
      <a class="btn secondary" href="{REPO_URL}#60-second-quickstart">60-second quickstart</a>
    </div>
    <img src="images/demo.svg" alt="cost_by_model running across mixed-vendor captures" class="demo-img">
  </header>

  <section id="graphs">
    <h2>Try the graphs <span class="small">— interactive, no install</span></h2>
    <p class="section-lead">
      Each card opens a self-contained cytoscape.js viewer for one captured
      trace. Pan, zoom, hover for properties, share by sending one HTML file.
      Same output as <span class="kbd">python tools/render_graph.py --fixture …</span>
      locally.
    </p>
    <div class="grid">
{cards_html}
    </div>
  </section>

  <section id="cost">
    <h2>Token spend by model <span class="small">— union of every loaded trace</span></h2>
    <p class="section-lead">
      Mixed-vendor cost attribution from OTel spans alone. Real tokens from real captures.
    </p>
    <pre class="table">{cost_table}</pre>
  </section>

  <section id="tools">
    <h2>Tool usage <span class="small">— from agent_with_tool, error_case, and real tool_call captures</span></h2>
    <pre class="table">{tool_table}</pre>
  </section>

  <section id="vendors">
    <h2>Calls by vendor</h2>
    <pre class="table">{prov_table}</pre>
  </section>

  <section id="install">
    <h2>Get it</h2>
    <p class="section-lead">All the demos above came out of the same library you can install in 10 seconds.</p>
    <pre class="table"># once published to PyPI
pip install otel-genai-graph

# or clone for the CLIs and capture scripts
git clone {REPO_URL}.git
cd otel_genai_graph_exporter
pip install -e ".[dev]"</pre>
  </section>

  <footer>
    Apache-2.0 · maintained by <a href="https://github.com/kums1234">@kums1234</a> ·
    {stats['n_graphs']} captures = {stats['n_nodes']} nodes / {stats['n_edges']} edges loaded.
    <br>
    <a href="{REPO_URL}/blob/main/CHANGELOG.md">Changelog</a> ·
    <a href="{REPO_URL}/blob/main/CONTRIBUTING.md">Contribute</a> ·
    <a href="{REPO_URL}/blob/main/CONTRIBUTORS.md">Contributors</a> ·
    <a href="{REPO_URL}/issues">Issues</a>
  </footer>
</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", type=Path, default=DEFAULT_OUT,
                   help="output directory (default: docs/_site)")
    args = p.parse_args(argv)

    out = args.output
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    # 1. Static assets
    images_out = out / "images"
    images_out.mkdir()
    if IMAGES_DIR.exists():
        for img in IMAGES_DIR.glob("*.svg"):
            shutil.copy2(img, images_out / img.name)

    (out / "style.css").write_text(CSS)
    (out / ".nojekyll").write_text("")  # GitHub Pages: don't run Jekyll

    # 2. Per-fixture pages + JSON sidecars
    inputs = discover_inputs()
    cards: list[str] = []
    graphs: list[Graph] = []
    for kind, path in inputs:
        graph = _load_graph(path)
        if graph is None:
            print(f"  skip {path}: failed to parse", file=sys.stderr)
            continue
        graphs.append(graph)

        sub = out / kind
        sub.mkdir(exist_ok=True)
        stem = path.stem

        (sub / f"{stem}.html").write_text(to_html(graph, title=stem))
        (sub / f"{stem}.json").write_text(
            json.dumps(to_node_link_json(graph), indent=2)
        )

        labels = sorted({k[0] for k in graph.nodes})
        cards.append(_card_html(
            title=_humanize(stem),
            desc=DESCRIPTIONS.get(stem, "Captured GenAI trace."),
            stat=f"{graph.node_count()} nodes · {graph.edge_count()} edges · {', '.join(labels)}",
            html_link=f"{kind}/{stem}.html",
            json_link=f"{kind}/{stem}.json",
        ))

    if not graphs:
        print("error: no fixtures found — aborting", file=sys.stderr)
        return 1

    # 3. Aggregations
    merged = _merge_graphs(graphs)
    cost_table = _ascii_table(*cost_by_model(merged))
    tool_table = _ascii_table(*tool_usage(merged))
    prov_table = _ascii_table(*provider_distribution(merged))

    stats = {
        "n_graphs": len(graphs),
        "n_nodes": merged.node_count(),
        "n_edges": merged.edge_count(),
    }

    (out / "index.html").write_text(
        render_index(stats, "\n".join(cards), (cost_table, tool_table, prov_table))
    )

    print(f"wrote site to {out}", file=sys.stderr)
    print(f"  {stats['n_graphs']} graphs, {stats['n_nodes']} nodes, {stats['n_edges']} edges",
          file=sys.stderr)
    print(f"  preview: open {out}/index.html", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
