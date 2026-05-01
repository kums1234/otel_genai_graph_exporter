"""Load OTLP/JSON fixture files into a chosen backend.

Usage — Neo4j (default, preserves prior behaviour)
--------------------------------------------------
    python -m otel_genai_graph.load tests/fixtures/real/*.json \\
        --uri bolt://localhost:7687 --user neo4j --password test

Usage — DuckDB
--------------
    python -m otel_genai_graph.load tests/fixtures/real/*.json \\
        --backend duckdb --duckdb-path ./trace.duckdb

The backend is picked, in order, from:
  1. ``--backend`` CLI flag
  2. ``OTGG_BACKEND`` env var
  3. ``neo4j`` (default; preserves the prior behaviour of this tool)

Per-backend connection details fall back to env vars too:
  * Neo4j  : ``NEO4J_URI``, ``NEO4J_USER``, ``NEO4J_PASSWORD``, ``NEO4J_DATABASE``
  * DuckDB : ``DUCKDB_PATH`` (default ``:memory:``)

Accepts any file with the shape used by the project's fixtures, i.e.
``{"otlp": {"resourceSpans": [...]}}``. The mapper is run on each file and
the resulting graph is written through whichever sink was selected.
Re-running the same file is a no-op on both backends.
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
from .sink import (
    BACKEND_ENV,
    DEFAULT_BACKEND,
    config_from_env,
    make_sink,
    resolve_backend,
)


def _load_resource_spans(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if "otlp" in data and "resourceSpans" in data["otlp"]:
        return data["otlp"]["resourceSpans"]
    if "resourceSpans" in data:
        return data["resourceSpans"]
    raise ValueError(f"{path}: expected 'otlp.resourceSpans' or 'resourceSpans'")


def _build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "files", nargs="+", type=Path,
        help="one or more OTLP/JSON fixture files",
    )

    # ── backend selection ──────────────────────────────────────────────────
    p.add_argument(
        "--backend",
        choices=("neo4j", "duckdb"),
        default=os.environ.get(BACKEND_ENV),
        help=(
            f"backend to write into (env: {BACKEND_ENV}, "
            f"default: {DEFAULT_BACKEND})"
        ),
    )

    # ── neo4j connection ───────────────────────────────────────────────────
    g_neo4j = p.add_argument_group("neo4j")
    g_neo4j.add_argument(
        "--uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        help="neo4j bolt URI (env: NEO4J_URI)",
    )
    g_neo4j.add_argument(
        "--user", default=os.environ.get("NEO4J_USER", "neo4j"),
        help="neo4j user (env: NEO4J_USER)",
    )
    g_neo4j.add_argument(
        "--password", default=os.environ.get("NEO4J_PASSWORD", "test"),
        help="neo4j password (env: NEO4J_PASSWORD)",
    )
    g_neo4j.add_argument(
        "--database", default=os.environ.get("NEO4J_DATABASE", "neo4j"),
        help="neo4j database (env: NEO4J_DATABASE)",
    )

    # ── duckdb connection ──────────────────────────────────────────────────
    g_duckdb = p.add_argument_group("duckdb")
    g_duckdb.add_argument(
        "--duckdb-path",
        default=os.environ.get("DUCKDB_PATH", ":memory:"),
        help="filesystem path or ':memory:' (env: DUCKDB_PATH)",
    )

    # ── shared ─────────────────────────────────────────────────────────────
    p.add_argument(
        "--no-schema", action="store_true",
        help="skip schema creation (already created, or read-only user)",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    load_env()  # fills env from ./.env if present; shell wins
    args = _build_argparser().parse_args(argv)

    backend = resolve_backend(args.backend)
    config = config_from_env(
        backend,
        neo4j_uri=args.uri,
        neo4j_user=args.user,
        neo4j_password=args.password,
        neo4j_database=args.database,
        duckdb_path=args.duckdb_path,
    )

    print(f"-- backend: {backend}", file=sys.stderr)

    with make_sink(config) as sink:
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
