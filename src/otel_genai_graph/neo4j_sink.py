"""Idempotent Neo4j writer.

Every node is written with `MERGE` on its natural key from `schema.py`, and
every edge is written with `MERGE` keyed on `(src, dst, type)`. Re-ingesting
the same trace is a no-op on node and edge count.

Batching strategy
-----------------
Within one `write(graph)` call, nodes are bucketed by label and edges by
edge type. Each bucket goes through the server in one `UNWIND` statement,
so a graph with N nodes across 3 labels is 3 round-trips, not N.
"""
from __future__ import annotations

import dataclasses
from typing import Any, Optional

from .schema import Edge, Graph, NodeLabel


# ---------------------------------------------------------------------------
# Cypher
# ---------------------------------------------------------------------------

_CONSTRAINTS: list[str] = [
    "CREATE CONSTRAINT session_id          IF NOT EXISTS FOR (n:Session)    REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT agent_id            IF NOT EXISTS FOR (n:Agent)      REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT model_key           IF NOT EXISTS FOR (n:Model)      REQUIRE (n.provider, n.name) IS UNIQUE",
    "CREATE CONSTRAINT tool_name           IF NOT EXISTS FOR (n:Tool)       REQUIRE n.name IS UNIQUE",
    "CREATE CONSTRAINT data_source_id      IF NOT EXISTS FOR (n:DataSource) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT operation_span_id   IF NOT EXISTS FOR (n:Operation)  REQUIRE n.span_id IS UNIQUE",
    "CREATE CONSTRAINT resource_svc        IF NOT EXISTS FOR (n:Resource)   REQUIRE n.service_name IS UNIQUE",
]


_NODE_CYPHER: dict[str, str] = {
    NodeLabel.SESSION.value:
        "UNWIND $rows AS r MERGE (n:Session {id: r.id})",
    NodeLabel.AGENT.value:
        "UNWIND $rows AS r MERGE (n:Agent {id: r.id}) "
        "SET n.name = coalesce(r.name, n.name)",
    NodeLabel.MODEL.value:
        "UNWIND $rows AS r MERGE (n:Model {provider: r.provider, name: r.name})",
    NodeLabel.TOOL.value:
        "UNWIND $rows AS r MERGE (n:Tool {name: r.name})",
    NodeLabel.DATA_SOURCE.value:
        "UNWIND $rows AS r MERGE (n:DataSource {id: r.id}) "
        "SET n.kind = coalesce(r.kind, n.kind)",
    NodeLabel.OPERATION.value: (
        "UNWIND $rows AS r "
        "MERGE (n:Operation {span_id: r.span_id}) "
        "SET n.trace_id      = r.trace_id, "
        "    n.type          = r.type, "
        "    n.status        = r.status, "
        "    n.start_ns      = r.start_ns, "
        "    n.end_ns        = r.end_ns, "
        "    n.input_tokens  = r.input_tokens, "
        "    n.output_tokens = r.output_tokens, "
        "    n.error_message = r.error_message"
    ),
    NodeLabel.RESOURCE.value:
        "UNWIND $rows AS r MERGE (n:Resource {service_name: r.service_name}) "
        "SET n.service_version = coalesce(r.service_version, n.service_version)",
}


_EDGE_CYPHER: dict[str, str] = {
    "CONTAINS":
        "UNWIND $rows AS r "
        "MATCH (a:Session {id: r.src_id}) "
        "MATCH (b:Operation {span_id: r.dst_id}) "
        "MERGE (a)-[:CONTAINS]->(b)",
    "EXECUTED":
        "UNWIND $rows AS r "
        "MATCH (a:Operation {span_id: r.src_id}) "
        "MATCH (b:Model {provider: r.dst_provider, name: r.dst_name}) "
        "MERGE (a)-[:EXECUTED]->(b)",
    "INVOKED":
        "UNWIND $rows AS r "
        "MATCH (a:Agent {id: r.src_id}) "
        "MATCH (b:Operation {span_id: r.dst_id}) "
        "MERGE (a)-[:INVOKED]->(b)",
    "CALLED":
        "UNWIND $rows AS r "
        "MATCH (a:Operation {span_id: r.src_id}) "
        "MATCH (b:Tool {name: r.dst_name}) "
        "MERGE (a)-[:CALLED]->(b)",
    "RETRIEVED_FROM":
        "UNWIND $rows AS r "
        "MATCH (a:Operation {span_id: r.src_id}) "
        "MATCH (b:DataSource {id: r.dst_id}) "
        "MERGE (a)-[:RETRIEVED_FROM]->(b)",
    "PARENT_OF":
        # MERGE both endpoints — under live streaming the parent may not have
        # shown up in this batch yet. The Operation UNWIND fills in fields
        # later when the parent's own span exports.
        "UNWIND $rows AS r "
        "MERGE (a:Operation {span_id: r.src_id}) "
        "MERGE (b:Operation {span_id: r.dst_id}) "
        "MERGE (a)-[:PARENT_OF]->(b)",
    "DELEGATED_TO":
        "UNWIND $rows AS r "
        "MATCH (a:Agent {id: r.src_id}) "
        "MATCH (b:Agent {id: r.dst_id}) "
        "MERGE (a)-[:DELEGATED_TO]->(b)",
    "ACCESSED":
        "UNWIND $rows AS r "
        "MATCH (a:Agent {id: r.src_id}) "
        "MATCH (b:DataSource {id: r.dst_id}) "
        "MERGE (a)-[:ACCESSED]->(b)",
}


# ---------------------------------------------------------------------------
# Row builders — shape a Graph.edges entry into the dict the Cypher expects
# ---------------------------------------------------------------------------

def edge_row(edge: Edge) -> dict[str, Any]:
    """Flatten an `Edge` into the row shape expected by `_EDGE_CYPHER[...]`.

    Exposed for tests — exercising this separately means unit tests don't
    need a live Neo4j.
    """
    _src_label, src_id = edge.src
    dst_label, dst_id = edge.dst

    row: dict[str, Any] = {"src_id": src_id}

    if dst_label == NodeLabel.MODEL.value:
        # Model key is "provider/name" — split back out.
        provider, name = dst_id.split("/", 1)
        row["dst_provider"] = provider
        row["dst_name"] = name
    elif dst_label == NodeLabel.TOOL.value:
        row["dst_name"] = dst_id
    else:
        row["dst_id"] = dst_id

    return row


def node_row(node: Any) -> dict[str, Any]:
    """Flatten a node dataclass into a row dict for the MERGE statement."""
    return dataclasses.asdict(node)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------

class Neo4jSink:
    """Minimal Neo4j sink. Connects lazily; writes a Graph via batched MERGE."""

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
    ) -> None:
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self._driver: Optional[Any] = None

    # ---- lifecycle --------------------------------------------------------

    def connect(self) -> None:
        if self._driver is not None:
            return
        try:
            from neo4j import GraphDatabase
        except ImportError as e:  # pragma: no cover
            raise RuntimeError("pip install neo4j") from e
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        # verify_connectivity raises on auth / network errors — fail loud.
        self._driver.verify_connectivity()

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    def __enter__(self) -> "Neo4jSink":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()

    # ---- schema -----------------------------------------------------------

    def ensure_schema(self) -> None:
        """Create uniqueness constraints idempotently. Safe to call repeatedly."""
        assert self._driver is not None, "call connect() first"
        with self._driver.session(database=self.database) as sess:
            for stmt in _CONSTRAINTS:
                sess.run(stmt)

    # ---- write ------------------------------------------------------------

    def write(self, graph: Graph) -> None:
        """Write one graph in a single transaction.

        Ordering: nodes first, then edges. Within each, grouped by label or
        edge_type and batched through `UNWIND`.
        """
        assert self._driver is not None, "call connect() first"

        # Bucket nodes by label.
        nodes_by_label: dict[str, list[dict]] = {}
        for key, node in graph.nodes.items():
            label = key[0]
            nodes_by_label.setdefault(label, []).append(node_row(node))

        # Bucket edges by type.
        edges_by_type: dict[str, list[dict]] = {}
        for e in graph.edges:
            edges_by_type.setdefault(e.edge_type, []).append(edge_row(e))

        with self._driver.session(database=self.database) as sess:
            sess.execute_write(_run_batches, nodes_by_label, edges_by_type)


def _run_batches(
    tx: Any,
    nodes_by_label: dict[str, list[dict]],
    edges_by_type: dict[str, list[dict]],
) -> None:
    for label, rows in nodes_by_label.items():
        stmt = _NODE_CYPHER.get(label)
        if stmt is None:
            continue  # unknown label — skip rather than crash
        tx.run(stmt, rows=rows)

    for edge_type, rows in edges_by_type.items():
        stmt = _EDGE_CYPHER.get(edge_type)
        if stmt is None:
            continue
        tx.run(stmt, rows=rows)
