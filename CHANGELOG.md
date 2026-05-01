# Changelog

All notable changes to this project will be documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.1] - 2026-05-02

### Fixed

- `otel_genai_graph.__version__` is now derived from `importlib.metadata.version("otel-genai-graph")` instead of being hard-coded in `__init__.py`. The hard-coded constant lagged a release behind `pyproject.toml`: v0.2.0 shipped with `__version__ == "0.1.0"` even though the wheel itself was correctly tagged `0.2.0`. New invariant test (`tests/test_version.py`) locks the import-side version against the distribution metadata so this can't drift again.

## [0.2.0] - 2026-05-02

### Added

- **DuckDB as a first-class alternative backend** alongside Neo4j. Pick at startup: `--backend duckdb --duckdb-path ./trace.duckdb`, or via the `OTGG_BACKEND` env var. Defaults to `neo4j` to preserve prior behaviour.
- New `Sink` protocol (`otel_genai_graph.sink.Sink`) that both `Neo4jSink` and the new `DuckDBSink` satisfy structurally — no inheritance required. `make_sink(config)` factory dispatches on `Neo4jConfig` / `DuckDBConfig` tagged-union configs. `config_from_env` and `resolve_backend` resolve CLI flags, env vars, and defaults.
- `DuckDBSink` (`otel_genai_graph.duckdb_sink.DuckDBSink`) writes a denormalised analytics-shaped schema:
  - Wide **`ops`** table — one row per Operation with structural edges flattened as FK columns (`session_id`, `agent_id`, `model_provider`, `model_name`, `tool_name`, `data_source_id`, `parent_span_id`, `service_name`).
  - Dimension tables: `sessions`, `agents`, `models`, `tools`, `data_sources`, `resources`.
  - `agent_delegations` side table for the `Agent → Agent` edge.
  - Idempotent `INSERT … ON CONFLICT … DO UPDATE` upserts in a single transaction. `_OPS_SCHEMA` drives both DDL and the upsert SQL — adding a column is a one-line edit.
- Parallel SQL saved-query library (`otel_genai_graph.saved_queries_sql`) — 12 named queries paralleling the Cypher library, including a recursive-CTE `cost_by_agent_with_descendants` that walks the delegation graph, `cost_by_model`, `cost_by_session`, `tool_usage`, `failed_tools`, `agent_delegation_chains`, …
- Generic `GenAIExporter` (`otel_genai_graph.exporter.GenAIExporter`) accepts any `Sink`. `Neo4jGenAIExporter` is retained as an alias subclass for back-compat — existing imports keep working.
- **`Resource` nodes are now emitted by the mapper** from `resourceSpans[*].resource.service.name`, closing the v0.1 known limitation. The `Resource` dimension table in DuckDB is no longer always-empty.
- **`Operation.service_name`** is denormalised onto every operation as the natural FK to `Resource`. Lights up "cost per service" as a no-join SQL query in DuckDB and as a queryable property in Neo4j. See [`docs/mapping.md`](docs/mapping.md) and [`docs/schema.md`](docs/schema.md) for the contract.
- Open Graph social-preview image (`docs/images/og.png`, 1200×630) for repo URL shares; `og.svg` is the editable source.
- README "Backends — Neo4j or DuckDB" section documenting the asymmetry (Neo4j: graph viz; DuckDB: SQL analytics) and the choice point.

### Changed

- `pyproject.toml`: `neo4j` moved from base dependencies to a new `[neo4j]` optional extra alongside the new `[duckdb]` and `[all]` extras. The `[dev]` extra installs both for local development.
- The fixture suite's `expected_graph` blocks (`tests/fixtures/*.json`) and the `TraceBuilder` accounting (`tests/generate_traces.py`) gained one `Resource` node per fixture, reflecting the mapper change.

### Breaking

- **`pip install otel-genai-graph` no longer pulls the Neo4j driver.** Pick at install time: `pip install 'otel-genai-graph[neo4j]'`, `pip install 'otel-genai-graph[duckdb]'`, or `pip install 'otel-genai-graph[all]'` for both.
- **`Resource` nodes are now emitted.** Queries that count *total* nodes (e.g., `MATCH (n) RETURN count(n)`) will see N+1 results per resource bundle. Per-label queries are unaffected; this only matters for naïve totals.

### Tests

- 489 passing, 18 skipped (live `NEO4J_URI` integration tests). `test_duckdb_sink.py` covers row-builder unit tests, per-fixture round-trips against an in-memory DuckDB, double-write idempotency, file persistence, and the `Sink` protocol satisfaction contract.

## [0.1.0] - 2026-04-24

### Added

- First public release.
- Seven-label graph schema (`Session`, `Agent`, `Model`, `Tool`, `DataSource`, `Operation`, `Resource`) with eight edge types (`CONTAINS`, `EXECUTED`, `INVOKED`, `CALLED`, `RETRIEVED_FROM`, `PARENT_OF`, `DELEGATED_TO`, `ACCESSED`). See [`docs/schema.md`](docs/schema.md).
- OTLP/JSON `resourceSpans` → `Graph` mapper (`otel_genai_graph.mapper.map_spans`) with:
  - Full OpenTelemetry GenAI semantic conventions v1.37 canonical ingest.
  - Legacy-attribute compatibility for pre-v1.37 emitters — `gen_ai.system` → `gen_ai.provider.name`, `generate_content` → `chat`, `message` → `chat`, `text_generation` → `text_completion`.
  - Priority-ordered session-id resolution: `gen_ai.conversation.id` (canonical v1.37) → `session.id` (OpenInference) → `langsmith.trace.session_id` → `traceloop.association.properties.session_id` → `traceloop.association.properties.thread_id`. Canonical always wins.
  - Error-status propagation: ERROR on any span marks every ancestor ERROR.
- Idempotent Neo4j sink (`otel_genai_graph.neo4j_sink.Neo4jSink`) using `MERGE` on natural keys; re-ingesting the same trace is a no-op on node and edge counts. Batched `UNWIND` writes per label / edge type.
- Streaming `SpanExporter` (`otel_genai_graph.exporter.Neo4jGenAIExporter`) that plugs into any `TracerProvider` / `BatchSpanProcessor`.
- File loader CLI: `python -m otel_genai_graph.load trace.json …`, also installed as `otel-genai-load` console script.
- Cost table (`otel_genai_graph.cost`) covering Anthropic, OpenAI, Google, and common Azure/Vertex equivalents, with date-suffixed model-name fallback.
- Seven shape-independent invariants (`otel_genai_graph.invariants.check`) — edge endpoint types, Session uniqueness, EXECUTED / CALLED cardinality, INVOKED target type, no orphan Model / Tool / DataSource, DAG property of `PARENT_OF` and `DELEGATED_TO`, time-ordering and token-count sanity. Every violation is reported with a context tuple.
- Soft `.env` auto-loader (`otel_genai_graph._env.load_env`) using `python-dotenv`; shell env vars always win (`override=False`). `.env.example` shipped at repo root.
- Four-layer validation pipeline: hand-written fixtures, parametric synthesizer with chaos mode, real-API capture (Anthropic · OpenAI · Google · Azure OpenAI classic / v1 / Foundry · Ollama / Groq / any OpenAI-SDK-compatible backend), upstream-instrumentor capture (`opentelemetry-instrumentation-openai-v2`, `opentelemetry-instrumentation-google-genai`).
- Saved-query library (`otel_genai_graph.saved_queries`) with 10 curated queries: `overview`, `session_tree`, `agent_delegation`, `failed_tools`, `data_source_usage`, `cost_by_model`, `cost_by_session`, `cost_by_agent`, `tool_usage`, `provider_distribution`. Library documented in [`docs/saved-queries.md`](docs/saved-queries.md).
- Export module (`otel_genai_graph.export`) for interactive HTML (cytoscape.js, single self-contained file), node-link JSON, GraphML, DOT, SVG / PNG (via graphviz), CSV, JSONL, and ASCII tables.
- Unified CLI (`tools/render_graph.py`) for fixture-mode or `--from-neo4j` mode rendering against saved or ad-hoc Cypher.
- 815 tests passing in CI (18 additional integration tests gated on a live Neo4j via `NEO4J_URI`).
- GitHub Actions CI with Python 3.10 / 3.11 / 3.12 matrix plus a Neo4j-backed integration job.

### Known limitations

- `Resource` nodes are not yet emitted by the mapper (the schema has the label; v0.1 mapper skips it to keep the graph small).
- Streaming spans are represented by the final span only — mid-stream partials are not retained.
- Cost lives as `Operation.cost_usd` property; a dedicated `Budget` node for roll-ups is deferred to a later release.
- No built-in back-pressure policy on the live `SpanExporter`; high-volume ingest may need a bulk-load mode.

[Unreleased]: https://github.com/kums1234/otel_genai_graph_exporter/compare/v0.2.1...HEAD
[0.2.1]:      https://github.com/kums1234/otel_genai_graph_exporter/releases/tag/v0.2.1
[0.2.0]:      https://github.com/kums1234/otel_genai_graph_exporter/releases/tag/v0.2.0
[0.1.0]:      https://github.com/kums1234/otel_genai_graph_exporter/releases/tag/v0.1.0
