"""Backend-agnostic sink protocol + factory.

Two backends today, one protocol:

  * `Neo4jSink`  (graph store)   — see ``neo4j_sink.py``
  * `DuckDBSink` (analytical SQL) — see ``duckdb_sink.py``

Both implement ``Sink`` (a structural protocol — no inheritance required).
The mapper produces a ``Graph``; either sink consumes it. Pick the backend
at startup; everything upstream of the sink is identical.

Usage
-----
::

    from otel_genai_graph.sink import Neo4jConfig, DuckDBConfig, make_sink

    sink = make_sink(DuckDBConfig(path="./trace.duckdb"))
    with sink:
        sink.ensure_schema()
        sink.write(graph)

Or resolve from CLI args / env vars via :func:`config_from_env`.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol, Union, runtime_checkable

from .schema import Graph


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class Sink(Protocol):
    """The minimal interface every backend implements.

    The protocol is structural: any object with these methods qualifies.
    No ABC, no inheritance — the existing ``Neo4jSink`` already satisfies
    it without modification.

    ``connect()`` must be idempotent — exporters and CLI paths call it
    defensively and the sink should silently no-op when already open.
    """

    def connect(self) -> None: ...
    def ensure_schema(self) -> None: ...
    def write(self, graph: Graph) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "Sink": ...
    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None: ...


# ---------------------------------------------------------------------------
# Backend configs (tagged union via isinstance dispatch in make_sink)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Neo4jConfig:
    """Connection parameters for the Neo4j backend."""
    uri: str
    user: str
    password: str
    database: str = "neo4j"

    backend: str = field(default="neo4j", init=False)


@dataclass(frozen=True)
class DuckDBConfig:
    """Connection parameters for the DuckDB backend.

    ``path`` is a filesystem path or ``":memory:"`` for an ephemeral DB.
    DuckDB is single-writer per file; concurrent writers race.
    """
    path: str = ":memory:"

    backend: str = field(default="duckdb", init=False)


SinkConfig = Union[Neo4jConfig, DuckDBConfig]


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def make_sink(config: SinkConfig) -> Sink:
    """Instantiate the right sink for ``config``. Does NOT call ``connect()``."""
    if isinstance(config, Neo4jConfig):
        from .neo4j_sink import Neo4jSink
        return Neo4jSink(
            uri=config.uri,
            user=config.user,
            password=config.password,
            database=config.database,
        )
    if isinstance(config, DuckDBConfig):
        from .duckdb_sink import DuckDBSink
        return DuckDBSink(path=config.path)
    raise TypeError(f"unknown sink config: {type(config).__name__}")


# ---------------------------------------------------------------------------
# CLI / env resolution
# ---------------------------------------------------------------------------

# Default backend when nothing is specified. Preserves pre-DuckDB behaviour.
DEFAULT_BACKEND = "neo4j"

# Env var that selects the backend ("neo4j" or "duckdb").
BACKEND_ENV = "OTGG_BACKEND"


def resolve_backend(cli_backend: Optional[str] = None) -> str:
    """Pick the backend name. CLI wins, then env, then default."""
    if cli_backend:
        return cli_backend.lower()
    env = os.environ.get(BACKEND_ENV)
    if env:
        return env.lower()
    return DEFAULT_BACKEND


def config_from_env(
    backend: str,
    *,
    # neo4j overrides (typically come from argparse defaults that consult env)
    neo4j_uri: Optional[str] = None,
    neo4j_user: Optional[str] = None,
    neo4j_password: Optional[str] = None,
    neo4j_database: Optional[str] = None,
    # duckdb overrides
    duckdb_path: Optional[str] = None,
) -> SinkConfig:
    """Build a ``SinkConfig`` for the chosen backend.

    Each parameter falls back to the matching env var, then to a sensible
    default. Mirrors the existing ``load.py`` argparse defaults.
    """
    backend = backend.lower()
    if backend == "neo4j":
        return Neo4jConfig(
            uri=neo4j_uri      or os.environ.get("NEO4J_URI",      "bolt://localhost:7687"),
            user=neo4j_user    or os.environ.get("NEO4J_USER",     "neo4j"),
            password=neo4j_password or os.environ.get("NEO4J_PASSWORD", "test"),
            database=neo4j_database or os.environ.get("NEO4J_DATABASE", "neo4j"),
        )
    if backend == "duckdb":
        return DuckDBConfig(
            path=duckdb_path or os.environ.get("DUCKDB_PATH", ":memory:"),
        )
    raise ValueError(
        f"unknown backend {backend!r}; expected 'neo4j' or 'duckdb'"
    )
