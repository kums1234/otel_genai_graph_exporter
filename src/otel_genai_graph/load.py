"""Load OTLP/JSON fixture files into Neo4j.

Usage
-----
  python -m otel_genai_graph.load tests/fixtures/real/*.json \\
      --uri bolt://localhost:7687 --user neo4j --password test

Env overrides: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE.

Accepts any file with the shape used by the project's fixtures, i.e.
`{"otlp": {"resourceSpans": [...]}}`. The mapper is run on each file and
the resulting graph is MERGE-written through `Neo4jSink`. Re-running the
same file is a no-op.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

from ._env import load_env
from .mapper import map_spans
from .neo4j_sink import Neo4jSink


def _load_resource_spans(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if "otlp" in data and "resourceSpans" in data["otlp"]:
        return data["otlp"]["resourceSpans"]
    if "resourceSpans" in data:
        return data["resourceSpans"]
    raise ValueError(f"{path}: expected 'otlp.resourceSpans' or 'resourceSpans'")


def main(argv: Optional[list[str]] = None) -> int:
    load_env()  # fills env from ./.env if present; shell wins
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="+", type=Path, help="one or more OTLP/JSON fixture files")
    p.add_argument("--uri",      default=os.environ.get("NEO4J_URI",      "bolt://localhost:7687"))
    p.add_argument("--user",     default=os.environ.get("NEO4J_USER",     "neo4j"))
    p.add_argument("--password", default=os.environ.get("NEO4J_PASSWORD", "test"))
    p.add_argument("--database", default=os.environ.get("NEO4J_DATABASE", "neo4j"))
    p.add_argument("--no-schema", action="store_true",
                   help="skip constraint creation (already created, or read-only user)")
    args = p.parse_args(argv)

    with Neo4jSink(args.uri, args.user, args.password, args.database) as sink:
        if not args.no_schema:
            sink.ensure_schema()

        total_nodes = 0
        total_edges = 0
        for path in args.files:
            rs = _load_resource_spans(path)
            graph = map_spans(rs)
            sink.write(graph)
            total_nodes += graph.node_count()
            total_edges += graph.edge_count()
            print(
                f"  {path.name}: {graph.node_count()} nodes, {graph.edge_count()} edges",
                file=sys.stderr,
            )

        print(
            f"-- wrote {total_nodes} node-ops, {total_edges} edge-ops "
            f"across {len(args.files)} file(s)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
