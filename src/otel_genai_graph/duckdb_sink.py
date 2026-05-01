"""Idempotent DuckDB writer — analytical-shape (Option B).

Why DuckDB and why this shape
-----------------------------
The Neo4j sink is faithful to the graph: nodes-and-edges, traversable.
DuckDB is a different audience — analysts who want SQL aggregates without
standing up a database server. So the schema is **denormalised, not a
graph mirror**:

    ops                  -- one row per Operation (span). Wide. The star.
    sessions             -- dimension table, keyed on id
    agents               -- dimension table, keyed on id
    models               -- dimension table, composite key (provider, name)
    tools                -- dimension table, keyed on name
    data_sources         -- dimension table, keyed on id
    resources            -- dimension table, keyed on service_name
    agent_delegations    -- agent → agent edges (DELEGATED_TO)

The structural relationships from the graph are flattened onto ``ops`` as
foreign-key columns:

    CONTAINS       (Session → Op)        →  ops.session_id
    INVOKED        (Agent → Op)          →  ops.agent_id
    EXECUTED       (Op → Model)          →  ops.model_provider, ops.model_name
    CALLED         (Op → Tool)           →  ops.tool_name
    RETRIEVED_FROM (Op → DataSource)     →  ops.data_source_id
    PARENT_OF      (parent Op → child)   →  ops.parent_span_id

The ``ACCESSED`` edge (Agent → DataSource, a convenience link in the
graph) is **not stored** — it's fully derivable as
``SELECT DISTINCT agent_id, data_source_id FROM ops`` whenever an analyst
wants it.

Idempotency
-----------
Every UPSERT keys on the natural primary key from ``schema.py``. Re-writing
the same ``Graph`` is a no-op on row count.

Concurrency
-----------
DuckDB is single-writer per file. This sink does not attempt to coordinate
across processes; running two instances against the same file races.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from .schema import (
    Agent,
    DataSource,
    Edge,
    EdgeType,
    Graph,
    Model,
    NodeKey,
    NodeLabel,
    Operation,
    Resource,
    Session,
    Tool,
)


# ---------------------------------------------------------------------------
# Schema — single source of truth
# ---------------------------------------------------------------------------
#
# ``ops`` columns and their SQL types are declared once. The ``CREATE TABLE``
# DDL and the ``INSERT … ON CONFLICT … DO UPDATE`` statement are both
# derived from this list, so adding a column is a one-line change here.
#
# Order matters — it's the order rows are projected into the parameterised
# UPSERT.

# (column_name, sql_type) — span_id is implicitly the PRIMARY KEY; do not
# repeat it elsewhere.
_OPS_SCHEMA: tuple[tuple[str, str], ...] = (
    ("span_id",        "VARCHAR PRIMARY KEY"),
    ("parent_span_id", "VARCHAR"),
    ("trace_id",       "VARCHAR"),
    ("type",           "VARCHAR"),
    ("name",           "VARCHAR"),
    ("status",         "VARCHAR"),
    ("start_ns",       "BIGINT"),
    ("end_ns",         "BIGINT"),
    ("input_tokens",   "INTEGER"),
    ("output_tokens",  "INTEGER"),
    ("cost_usd",       "DOUBLE"),
    ("error_message",  "VARCHAR"),
    ("service_name",   "VARCHAR"),
    ("session_id",     "VARCHAR"),
    ("agent_id",       "VARCHAR"),
    ("model_provider", "VARCHAR"),
    ("model_name",     "VARCHAR"),
    ("tool_name",      "VARCHAR"),
    ("data_source_id", "VARCHAR"),
)

_OPS_COLUMNS: list[str] = [name for name, _ in _OPS_SCHEMA]


def _ops_create_table_sql() -> str:
    cols = ",\n        ".join(f"{name:15} {sqltype}" for name, sqltype in _OPS_SCHEMA)
    return f"CREATE TABLE IF NOT EXISTS ops (\n        {cols}\n    )"


_DDL: list[str] = [
    "CREATE TABLE IF NOT EXISTS sessions (id VARCHAR PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS agents (id VARCHAR PRIMARY KEY, name VARCHAR)",
    (
        "CREATE TABLE IF NOT EXISTS models ("
        "provider VARCHAR NOT NULL, name VARCHAR NOT NULL, "
        "PRIMARY KEY (provider, name))"
    ),
    "CREATE TABLE IF NOT EXISTS tools (name VARCHAR PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS data_sources (id VARCHAR PRIMARY KEY, kind VARCHAR)",
    (
        "CREATE TABLE IF NOT EXISTS resources ("
        "service_name VARCHAR PRIMARY KEY, service_version VARCHAR)"
    ),
    (
        "CREATE TABLE IF NOT EXISTS agent_delegations ("
        "parent_agent_id VARCHAR NOT NULL, child_agent_id VARCHAR NOT NULL, "
        "PRIMARY KEY (parent_agent_id, child_agent_id))"
    ),
    _ops_create_table_sql(),
]


# ---------------------------------------------------------------------------
# Upsert SQL — keep separate from the row builders so tests can assert shape
# ---------------------------------------------------------------------------

# (sql, columns_in_param_order)
_UPSERT_SESSIONS = (
    "INSERT INTO sessions (id) VALUES (?) "
    "ON CONFLICT (id) DO NOTHING",
    ["id"],
)

_UPSERT_AGENTS = (
    "INSERT INTO agents (id, name) VALUES (?, ?) "
    "ON CONFLICT (id) DO UPDATE SET name = COALESCE(EXCLUDED.name, agents.name)",
    ["id", "name"],
)

_UPSERT_MODELS = (
    "INSERT INTO models (provider, name) VALUES (?, ?) "
    "ON CONFLICT (provider, name) DO NOTHING",
    ["provider", "name"],
)

_UPSERT_TOOLS = (
    "INSERT INTO tools (name) VALUES (?) "
    "ON CONFLICT (name) DO NOTHING",
    ["name"],
)

_UPSERT_DATA_SOURCES = (
    "INSERT INTO data_sources (id, kind) VALUES (?, ?) "
    "ON CONFLICT (id) DO UPDATE SET kind = COALESCE(EXCLUDED.kind, data_sources.kind)",
    ["id", "kind"],
)

_UPSERT_RESOURCES = (
    "INSERT INTO resources (service_name, service_version) VALUES (?, ?) "
    "ON CONFLICT (service_name) DO UPDATE SET "
    "service_version = COALESCE(EXCLUDED.service_version, resources.service_version)",
    ["service_name", "service_version"],
)

_UPSERT_AGENT_DELEGATIONS = (
    "INSERT INTO agent_delegations (parent_agent_id, child_agent_id) VALUES (?, ?) "
    "ON CONFLICT (parent_agent_id, child_agent_id) DO NOTHING",
    ["parent_agent_id", "child_agent_id"],
)

# Build the ops upsert dynamically so adding a column is a one-liner in
# ``_OPS_SCHEMA`` above and everything below stays in sync.
_OPS_PLACEHOLDERS = ", ".join(["?"] * len(_OPS_COLUMNS))
_OPS_UPDATE = ", ".join(
    f"{c} = EXCLUDED.{c}" for c in _OPS_COLUMNS if c != "span_id"
)
_UPSERT_OPS = (
    f"INSERT INTO ops ({', '.join(_OPS_COLUMNS)}) VALUES ({_OPS_PLACEHOLDERS}) "
    f"ON CONFLICT (span_id) DO UPDATE SET {_OPS_UPDATE}",
    _OPS_COLUMNS,
)


# ---------------------------------------------------------------------------
# Row builders — exposed for unit tests so we can verify flattening logic
# without booting DuckDB
# ---------------------------------------------------------------------------

def ops_rows(graph: Graph) -> list[dict[str, Any]]:
    """Flatten Operations + their incident edges into wide rows.

    Pre-indexes edges by src/dst to avoid an O(N*E) scan, then walks each
    Operation node and reads off its structural neighbours.
    """
    incoming: dict[NodeKey, list[Edge]] = defaultdict(list)
    outgoing: dict[NodeKey, list[Edge]] = defaultdict(list)
    for e in graph.edges:
        incoming[e.dst].append(e)
        outgoing[e.src].append(e)

    rows: list[dict[str, Any]] = []
    for key, node in graph.nodes.items():
        if key[0] != NodeLabel.OPERATION.value:
            continue
        assert isinstance(node, Operation)

        row: dict[str, Any] = {
            "span_id":        node.span_id,
            "parent_span_id": None,
            "trace_id":       node.trace_id,
            "type":           node.type,
            "name":           node.name,
            "status":         node.status,
            "start_ns":       node.start_ns,
            "end_ns":         node.end_ns,
            "input_tokens":   node.input_tokens,
            "output_tokens":  node.output_tokens,
            "cost_usd":       node.cost_usd,
            "error_message":  node.error_message,
            "service_name":   node.service_name,
            "session_id":     None,
            "agent_id":       None,
            "model_provider": None,
            "model_name":     None,
            "tool_name":      None,
            "data_source_id": None,
        }

        # incoming: who points AT this op?
        for e in incoming.get(key, ()):
            if e.edge_type == EdgeType.CONTAINS.value:        # Session → Op
                row["session_id"] = e.src[1]
            elif e.edge_type == EdgeType.INVOKED.value:        # Agent → Op
                row["agent_id"] = e.src[1]
            elif e.edge_type == EdgeType.PARENT_OF.value:      # parent Op → this
                row["parent_span_id"] = e.src[1]

        # outgoing: what does this op point AT?
        for e in outgoing.get(key, ()):
            if e.edge_type == EdgeType.EXECUTED.value:        # Op → Model
                provider, name = e.dst[1].split("/", 1)
                row["model_provider"] = provider
                row["model_name"]     = name
            elif e.edge_type == EdgeType.CALLED.value:         # Op → Tool
                row["tool_name"] = e.dst[1]
            elif e.edge_type == EdgeType.RETRIEVED_FROM.value: # Op → DataSource
                row["data_source_id"] = e.dst[1]

        rows.append(row)

    return rows


def agent_delegation_rows(graph: Graph) -> list[dict[str, str]]:
    """Extract DELEGATED_TO edges as (parent, child) rows."""
    out: list[dict[str, str]] = []
    for e in graph.edges:
        if e.edge_type != EdgeType.DELEGATED_TO.value:
            continue
        out.append({"parent_agent_id": e.src[1], "child_agent_id": e.dst[1]})
    return out


def dim_rows(graph: Graph) -> dict[str, list[dict[str, Any]]]:
    """Bucket dimension-node rows by table name."""
    out: dict[str, list[dict[str, Any]]] = {
        "sessions": [], "agents": [], "models": [],
        "tools": [], "data_sources": [], "resources": [],
    }
    for key, node in graph.nodes.items():
        label = key[0]
        if label == NodeLabel.SESSION.value:
            assert isinstance(node, Session)
            out["sessions"].append({"id": node.id})
        elif label == NodeLabel.AGENT.value:
            assert isinstance(node, Agent)
            out["agents"].append({"id": node.id, "name": node.name})
        elif label == NodeLabel.MODEL.value:
            assert isinstance(node, Model)
            out["models"].append({"provider": node.provider, "name": node.name})
        elif label == NodeLabel.TOOL.value:
            assert isinstance(node, Tool)
            out["tools"].append({"name": node.name})
        elif label == NodeLabel.DATA_SOURCE.value:
            assert isinstance(node, DataSource)
            out["data_sources"].append({"id": node.id, "kind": node.kind})
        elif label == NodeLabel.RESOURCE.value:
            assert isinstance(node, Resource)
            out["resources"].append({
                "service_name":    node.service_name,
                "service_version": node.service_version,
            })
        # Operation handled separately via ops_rows.
    return out


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

class DuckDBSink:
    """Minimal DuckDB sink. In-process, single-file (or :memory:), idempotent."""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._con: Optional[Any] = None

    # ---- lifecycle --------------------------------------------------------

    def connect(self) -> None:
        if self._con is not None:
            return
        try:
            import duckdb
        except ImportError as e:  # pragma: no cover
            raise RuntimeError(
                "DuckDB backend selected but `duckdb` is not installed. "
                "Install with: pip install 'otel-genai-graph[duckdb]'"
            ) from e
        self._con = duckdb.connect(self.path)

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self) -> "DuckDBSink":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    # ---- schema -----------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create tables idempotently. Safe to call repeatedly."""
        assert self._con is not None, "call connect() first"
        for stmt in _DDL:
            self._con.execute(stmt)

    # ---- write ------------------------------------------------------------

    def write(self, graph: Graph) -> None:
        """Write one graph in a single transaction.

        Order: dimension tables first (so foreign-key-ish columns on ops
        reference rows that exist), then ``ops``, then ``agent_delegations``.
        DuckDB doesn't enforce FKs by default, but the order matches the
        intent and makes the writes easier to reason about.
        """
        assert self._con is not None, "call connect() first"

        dims  = dim_rows(graph)
        ops   = ops_rows(graph)
        deleg = agent_delegation_rows(graph)

        self._con.execute("BEGIN")
        try:
            self._executemany(_UPSERT_SESSIONS,         dims["sessions"])
            self._executemany(_UPSERT_AGENTS,           dims["agents"])
            self._executemany(_UPSERT_MODELS,           dims["models"])
            self._executemany(_UPSERT_TOOLS,            dims["tools"])
            self._executemany(_UPSERT_DATA_SOURCES,     dims["data_sources"])
            self._executemany(_UPSERT_RESOURCES,        dims["resources"])
            self._executemany(_UPSERT_OPS,              ops)
            self._executemany(_UPSERT_AGENT_DELEGATIONS, deleg)
            self._con.execute("COMMIT")
        except Exception:
            self._con.execute("ROLLBACK")
            raise

    # ---- internals --------------------------------------------------------

    def _executemany(
        self,
        stmt: tuple[str, list[str]],
        rows: list[dict[str, Any]],
    ) -> None:
        """Run a parameterised statement once per row.

        DuckDB's Python ``executemany`` accepts a list of parameter tuples;
        we project each dict into the column order declared with the SQL.
        """
        if not rows:
            return
        sql, columns = stmt
        params = [tuple(r.get(c) for c in columns) for r in rows]
        assert self._con is not None
        self._con.executemany(sql, params)
