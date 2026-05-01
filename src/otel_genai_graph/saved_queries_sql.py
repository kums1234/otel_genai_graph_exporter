"""Curated SQL queries against the otel-genai-graph DuckDB store.

Sibling library to ``saved_queries.py`` (Cypher), kept deliberately
separate. The two backends have different idiomatic shapes — the
DuckDB schema is denormalised (wide ``ops`` + dim tables, see
``duckdb_sink.py``) and most useful questions there are direct
aggregates rather than graph traversals. Pretending the two are
1:1 translations would force awkwardness on both sides.

Naming convention: query names that exist in both registries are
**parallel-purpose** (e.g. both have ``cost_by_model``) but the SQL
result shape is what's idiomatic for SQL — column-oriented tables,
not graph ``Result`` records.

Adding a query
--------------
Append a ``SqlQuery`` to ``_QUERIES`` below. A short description, a
DuckDB-flavoured SQL string, and zero or more ``Parameter`` declarations
(``$name`` placeholders inside the SQL get resolved against the
``parameters`` dict at call time).
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
class SqlQuery:
    """A named, parameterised SQL query against the DuckDB schema."""
    name: str
    description: str
    sql: str
    tags: tuple[str, ...] = ()
    parameters: tuple[Parameter, ...] = ()

    def param_names(self) -> set[str]:
        return {p.name for p in self.parameters}


# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

_QUERIES: tuple[SqlQuery, ...] = (
    # ── discovery / debugging ──────────────────────────────────────────────
    SqlQuery(
        name="overview",
        description="Row counts per table — first sanity check after a load.",
        tags=("discover",),
        sql=(
            "SELECT 'ops'               AS table, count(*) AS rows FROM ops UNION ALL "
            "SELECT 'sessions',                    count(*) FROM sessions UNION ALL "
            "SELECT 'agents',                      count(*) FROM agents UNION ALL "
            "SELECT 'models',                      count(*) FROM models UNION ALL "
            "SELECT 'tools',                       count(*) FROM tools UNION ALL "
            "SELECT 'data_sources',                count(*) FROM data_sources UNION ALL "
            "SELECT 'resources',                   count(*) FROM resources UNION ALL "
            "SELECT 'agent_delegations',           count(*) FROM agent_delegations "
            "ORDER BY rows DESC"
        ),
    ),
    SqlQuery(
        name="session_tree",
        description="All Operations of one Session, ordered chronologically.",
        tags=("session", "debug"),
        parameters=(
            Parameter(
                name="session_id",
                description="value of ops.session_id",
                required=True,
                example="conv-3",
            ),
        ),
        sql=(
            "SELECT span_id, parent_span_id, type, status, "
            "       agent_id, model_provider, model_name, tool_name, data_source_id, "
            "       input_tokens, output_tokens, start_ns, end_ns "
            "FROM ops "
            "WHERE session_id = $session_id "
            "ORDER BY coalesce(start_ns, 0)"
        ),
    ),

    # ── structural questions ───────────────────────────────────────────────
    SqlQuery(
        name="agent_delegation",
        description="Direct Agent → Agent delegation pairs.",
        tags=("agents", "delegation"),
        sql=(
            "SELECT parent_agent_id, child_agent_id "
            "FROM agent_delegations "
            "ORDER BY parent_agent_id, child_agent_id"
        ),
    ),
    SqlQuery(
        name="agent_delegation_chains",
        description="Transitive Agent → Agent delegation chains (recursive CTE).",
        tags=("agents", "delegation"),
        sql=(
            "WITH RECURSIVE chain(root, current, depth, path) AS ( "
            "  SELECT parent_agent_id, child_agent_id, 1, "
            "         list_value(parent_agent_id, child_agent_id) "
            "  FROM agent_delegations "
            "  UNION ALL "
            "  SELECT c.root, d.child_agent_id, c.depth + 1, "
            "         list_append(c.path, d.child_agent_id) "
            "  FROM chain c "
            "  JOIN agent_delegations d ON d.parent_agent_id = c.current "
            "  WHERE NOT list_contains(c.path, d.child_agent_id) "
            ") "
            "SELECT root AS root_agent, current AS leaf_agent, depth, path "
            "FROM chain "
            "ORDER BY root, depth, leaf_agent"
        ),
    ),
    SqlQuery(
        name="failed_tools",
        description="Tools called from an ERROR Operation. Failure count per tool.",
        tags=("errors", "reliability"),
        sql=(
            "SELECT tool_name, "
            "       count(*) AS failures, "
            "       count(DISTINCT trace_id) AS traces "
            "FROM ops "
            "WHERE status = 'ERROR' AND tool_name IS NOT NULL "
            "GROUP BY tool_name "
            "ORDER BY failures DESC"
        ),
    ),
    SqlQuery(
        name="data_source_usage",
        description="Which Agents touched which DataSources, with call counts.",
        tags=("rag", "data"),
        sql=(
            "SELECT agent_id, data_source_id, count(*) AS calls "
            "FROM ops "
            "WHERE data_source_id IS NOT NULL "
            "GROUP BY agent_id, data_source_id "
            "ORDER BY calls DESC, agent_id, data_source_id"
        ),
    ),

    # ── aggregations / cost attribution ────────────────────────────────────
    SqlQuery(
        name="cost_by_model",
        description="Token spend aggregated per (provider, model).",
        tags=("cost",),
        sql=(
            "SELECT model_provider AS provider, "
            "       model_name     AS model, "
            "       count(*)       AS calls, "
            "       sum(coalesce(input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(output_tokens, 0)) AS output_tokens "
            "FROM ops "
            "WHERE model_provider IS NOT NULL "
            "GROUP BY model_provider, model_name "
            "ORDER BY calls DESC"
        ),
    ),
    SqlQuery(
        name="cost_by_session",
        description="Token spend and Operation count per Session.",
        tags=("cost", "session"),
        sql=(
            "SELECT session_id, "
            "       count(*) AS operations, "
            "       sum(coalesce(input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(output_tokens, 0)) AS output_tokens "
            "FROM ops "
            "WHERE session_id IS NOT NULL "
            "GROUP BY session_id "
            "ORDER BY input_tokens + output_tokens DESC"
        ),
    ),
    SqlQuery(
        name="cost_by_agent",
        description=(
            "Token spend per Agent, direct ops only. (For delegation-aware "
            "rollups, use cost_by_agent_with_descendants — it walks "
            "agent_delegations with a recursive CTE.)"
        ),
        tags=("cost", "agents"),
        sql=(
            "SELECT agent_id, "
            "       count(*) AS operations, "
            "       sum(coalesce(input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(output_tokens, 0)) AS output_tokens "
            "FROM ops "
            "WHERE agent_id IS NOT NULL "
            "GROUP BY agent_id "
            "ORDER BY input_tokens + output_tokens DESC"
        ),
    ),
    SqlQuery(
        name="cost_by_agent_with_descendants",
        description=(
            "Token spend per Agent, including ops attributed to delegated "
            "sub-agents. Mirrors the Cypher cost_by_agent that walked "
            "DELEGATED_TO*."
        ),
        tags=("cost", "agents"),
        sql=(
            "WITH RECURSIVE descendants(root, descendant) AS ( "
            "  SELECT id, id FROM agents "
            "  UNION ALL "
            "  SELECT d.root, ad.child_agent_id "
            "  FROM descendants d "
            "  JOIN agent_delegations ad ON ad.parent_agent_id = d.descendant "
            ") "
            "SELECT d.root AS agent_id, "
            "       count(o.span_id)              AS operations, "
            "       sum(coalesce(o.input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(o.output_tokens, 0)) AS output_tokens "
            "FROM descendants d "
            "LEFT JOIN ops o ON o.agent_id = d.descendant "
            "GROUP BY d.root "
            "ORDER BY input_tokens + output_tokens DESC"
        ),
    ),
    SqlQuery(
        name="tool_usage",
        description="Tool call counts plus failure counts, ranked.",
        tags=("tools",),
        sql=(
            "SELECT tool_name AS tool, "
            "       count(*) AS calls, "
            "       sum(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS failures "
            "FROM ops "
            "WHERE tool_name IS NOT NULL "
            "GROUP BY tool_name "
            "ORDER BY calls DESC"
        ),
    ),
    SqlQuery(
        name="provider_distribution",
        description="Calls and token spend grouped by provider only.",
        tags=("cost", "vendor"),
        sql=(
            "SELECT model_provider AS provider, "
            "       count(*)       AS calls, "
            "       sum(coalesce(input_tokens, 0))  AS input_tokens, "
            "       sum(coalesce(output_tokens, 0)) AS output_tokens "
            "FROM ops "
            "WHERE model_provider IS NOT NULL "
            "GROUP BY model_provider "
            "ORDER BY calls DESC"
        ),
    ),
)


# ---------------------------------------------------------------------------
# Registry + helpers (mirror the Cypher library shape so tooling can swap)
# ---------------------------------------------------------------------------

QUERIES: dict[str, SqlQuery] = {q.name: q for q in _QUERIES}


def list_queries(tag: Optional[str] = None) -> list[SqlQuery]:
    """All saved SQL queries (optionally filtered by tag)."""
    if tag is None:
        return list(QUERIES.values())
    return [q for q in QUERIES.values() if tag in q.tags]


def get_query(name: str) -> SqlQuery:
    if name not in QUERIES:
        raise KeyError(
            f"unknown SQL saved query {name!r}. Known: {sorted(QUERIES)}"
        )
    return QUERIES[name]


def validate_params(query: SqlQuery, provided: dict[str, str]) -> dict[str, str]:
    """Fill defaults + reject missing required / unknown keys.

    Mirrors ``saved_queries.validate_params`` so a single CLI surface can
    accept either backend.
    """
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
