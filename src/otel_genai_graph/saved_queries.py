"""Curated Cypher queries against the otel-genai-graph Neo4j store.

See `docs/saved-queries.md` for the full contract. The short version:

  * Each entry is a `SavedQuery` with a unique snake_case `name`,
    parameter declarations, and a `result_type` of "graph" (returns
    Node / Relationship / Path) or "table" (returns aggregations).
  * Adding a query here automatically surfaces it via
    `tools/render_graph.py --list-queries` and
    `--from-neo4j --query <name>`.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Parameter:
    name: str
    description: str
    required: bool = True
    default: Optional[str] = None
    example: Optional[str] = None


@dataclass(frozen=True)
class SavedQuery:
    name: str
    description: str
    result_type: str  # "graph" | "table"
    cypher: str
    tags: tuple[str, ...] = ()
    parameters: tuple[Parameter, ...] = ()

    def param_names(self) -> set[str]:
        return {p.name for p in self.parameters}


GRAPH = "graph"
TABLE = "table"


# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

_QUERIES: tuple[SavedQuery, ...] = (
    # ── discovery / debugging ──────────────────────────────────────────────
    SavedQuery(
        name="overview",
        description="Every node in the DB (capped at 500). Good first look after loading fixtures.",
        result_type=GRAPH,
        tags=("discover",),
        cypher="MATCH (n) RETURN n LIMIT 500",
    ),
    SavedQuery(
        name="session_tree",
        description="Full hierarchy of one Session: its Operations, parent/child links, any Agents/Tools/Models touched.",
        result_type=GRAPH,
        tags=("session", "debug"),
        parameters=(
            Parameter(
                name="session_id",
                description="the `id` of the Session node",
                required=True,
                example="conv-3",
            ),
        ),
        cypher=(
            "MATCH (s:Session {id: $session_id})-[:CONTAINS]->(o:Operation) "
            "OPTIONAL MATCH (o)-[r1:PARENT_OF|EXECUTED|CALLED|RETRIEVED_FROM]->(n) "
            "OPTIONAL MATCH (a:Agent)-[r2:INVOKED]->(o) "
            "RETURN s, o, r1, n, a, r2"
        ),
    ),

    # ── structural questions ───────────────────────────────────────────────
    SavedQuery(
        name="agent_delegation",
        description="Agent → Agent DELEGATED_TO chains across every loaded session.",
        result_type=GRAPH,
        tags=("agents", "delegation"),
        cypher=(
            "MATCH p = (a:Agent)-[:DELEGATED_TO*]->(b:Agent) "
            "RETURN p"
        ),
    ),
    SavedQuery(
        name="failed_tools",
        description="Tools called from an ERROR Operation, plus the ancestor chain (blast radius).",
        result_type=GRAPH,
        tags=("errors", "reliability"),
        cypher=(
            "MATCH (t:Tool)<-[:CALLED]-(err:Operation {status: 'ERROR'}) "
            "OPTIONAL MATCH p = (err)<-[:PARENT_OF*]-(ancestor:Operation) "
            "RETURN t, err, p, ancestor"
        ),
    ),
    SavedQuery(
        name="data_source_usage",
        description="Which Agents accessed which DataSources, and the Operations that triggered it.",
        result_type=GRAPH,
        tags=("rag", "data"),
        cypher=(
            "MATCH (a:Agent)-[acc:ACCESSED]->(d:DataSource) "
            "OPTIONAL MATCH (o:Operation)-[r:RETRIEVED_FROM]->(d) "
            "RETURN a, acc, d, o, r"
        ),
    ),

    # ── aggregations / cost attribution ────────────────────────────────────
    SavedQuery(
        name="cost_by_model",
        description="Token spend aggregated per (provider, model).",
        result_type=TABLE,
        tags=("cost",),
        cypher=(
            "MATCH (o:Operation)-[:EXECUTED]->(m:Model) "
            "RETURN m.provider AS provider, "
            "       m.name     AS model, "
            "       count(*)   AS calls, "
            "       sum(coalesce(o.input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(o.output_tokens, 0)) AS output_tokens "
            "ORDER BY calls DESC"
        ),
    ),
    SavedQuery(
        name="cost_by_session",
        description="Token spend and Operation count per Session.",
        result_type=TABLE,
        tags=("cost", "session"),
        cypher=(
            "MATCH (s:Session)-[:CONTAINS]->(o:Operation) "
            "RETURN s.id       AS session_id, "
            "       count(o)   AS operations, "
            "       sum(coalesce(o.input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(o.output_tokens, 0)) AS output_tokens "
            "ORDER BY input_tokens + output_tokens DESC"
        ),
    ),
    SavedQuery(
        name="cost_by_agent",
        description="Token spend aggregated per Agent — direct ops plus delegated sub-agent ops.",
        result_type=TABLE,
        tags=("cost", "agents"),
        cypher=(
            "MATCH (a:Agent) "
            "OPTIONAL MATCH (a)-[:DELEGATED_TO*0..]->(descendant:Agent) "
            "OPTIONAL MATCH (descendant)-[:INVOKED]->(:Operation)-[:PARENT_OF*0..]->(op:Operation) "
            "WITH a, collect(DISTINCT op) AS ops "
            "RETURN a.id AS agent_id, "
            "       size(ops) AS operations, "
            "       reduce(s=0, x IN ops | s + coalesce(x.input_tokens, 0))  AS input_tokens, "
            "       reduce(s=0, x IN ops | s + coalesce(x.output_tokens, 0)) AS output_tokens "
            "ORDER BY input_tokens + output_tokens DESC"
        ),
    ),
    SavedQuery(
        name="tool_usage",
        description="Tool call counts, ranked.",
        result_type=TABLE,
        tags=("tools",),
        cypher=(
            "MATCH (t:Tool)<-[:CALLED]-(o:Operation) "
            "RETURN t.name AS tool, "
            "       count(*) AS calls, "
            "       sum(CASE WHEN o.status='ERROR' THEN 1 ELSE 0 END) AS failures "
            "ORDER BY calls DESC"
        ),
    ),
    SavedQuery(
        name="provider_distribution",
        description="Calls and token spend grouped by provider only.",
        result_type=TABLE,
        tags=("cost", "vendor"),
        cypher=(
            "MATCH (o:Operation)-[:EXECUTED]->(m:Model) "
            "RETURN m.provider AS provider, "
            "       count(*)   AS calls, "
            "       sum(coalesce(o.input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(o.output_tokens, 0)) AS output_tokens "
            "ORDER BY calls DESC"
        ),
    ),
)


# ---------------------------------------------------------------------------
# Registry + helpers
# ---------------------------------------------------------------------------

QUERIES: dict[str, SavedQuery] = {q.name: q for q in _QUERIES}


def list_queries(tag: Optional[str] = None) -> list[SavedQuery]:
    """All saved queries (optionally filtered by tag)."""
    if tag is None:
        return list(QUERIES.values())
    return [q for q in QUERIES.values() if tag in q.tags]


def get_query(name: str) -> SavedQuery:
    if name not in QUERIES:
        raise KeyError(
            f"unknown saved query {name!r}. Known: {sorted(QUERIES)}"
        )
    return QUERIES[name]


def validate_params(query: SavedQuery, provided: dict[str, str]) -> dict[str, str]:
    """Fill defaults + reject missing required / unknown keys."""
    resolved: dict[str, str] = {}
    declared = {p.name: p for p in query.parameters}

    unknown = set(provided) - set(declared)
    if unknown:
        raise ValueError(
            f"query {query.name!r}: unknown parameter(s) {sorted(unknown)}; "
            f"declared: {sorted(declared)}"
        )

    for p in query.parameters:
        if p.name in provided:
            resolved[p.name] = provided[p.name]
        elif p.default is not None:
            resolved[p.name] = p.default
        elif p.required:
            raise ValueError(
                f"query {query.name!r}: missing required --param {p.name}"
            )
    return resolved
