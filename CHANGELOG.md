# Changelog

All notable changes to this project will be documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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

[Unreleased]: https://github.com/kums1234/otel_genai_graph_exporter/compare/v0.1.0...HEAD
[0.1.0]:      https://github.com/kums1234/otel_genai_graph_exporter/releases/tag/v0.1.0
