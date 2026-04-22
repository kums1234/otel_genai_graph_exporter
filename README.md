# otel-genai-graph

**Turn OpenTelemetry GenAI spans into a queryable Neo4j graph.**

Linear LLM tracers show you one agent's timeline. This exporter maps the
same spans to a graph, conversations, agents, LLM calls, tools, data
sources, with typed relationships between them, so you can ask
*structural* questions: who delegated to whom, what did this agent cost
across its sub-agents, which data sources did a session hit, what broke
when that tool failed.

Works with every major provider (OpenAI, Anthropic, Gemini, Azure OpenAI
classic / v1 / Foundry, Ollama, Groq, anything OpenAI-SDK-compatible) and
ingests both OpenTelemetry GenAI **semantic conventions v1.37** canonical
emitters and the pre-v1.37 shapes that most instrumentors still ship
today (Google `gen_ai.system`, Arize OpenInference `session.id`,
LangSmith, Traceloop's association bag, all handled with precedence
tables documented in [`docs/mapping.md`](docs/mapping.md)).

![Multi-agent delegation, rendered from the `multi_agent` fixture](docs/images/multi-agent-graph.svg)

## What it does

- **Ingest** OTLP/JSON `resourceSpans` from any GenAI-instrumented app.
- **Map** to a typed graph: `Session` → `Operation` → `Model`/`Tool`/`DataSource`, `Agent` ─`INVOKED`→ `Operation`, `Agent` ─`DELEGATED_TO`→ `Agent`.
- **Write idempotently** to Neo4j via `MERGE` on natural keys. Re-ingesting the same trace is a no-op.
- **Stream live or load files**, ships a `SpanExporter` (plug into any `TracerProvider`) and a file loader (`python -m otel_genai_graph.load trace.json ...`).
- **Explore without Cypher**, a bundled library of 10 saved queries (`cost_by_model`, `session_tree`, `agent_delegation`, `failed_tools`, …) that emit interactive HTML (cytoscape.js, single-file, no install), node-link JSON, GraphML, or CSV/ASCII tables. See the [sample](#explore-without-cypher) below.
- **Canonicalise legacy attributes**, `gen_ai.system → gen_ai.provider.name`, `generate_content → chat`, plus a priority-ordered `conversation.id` fallback list (`session.id`, `langsmith.trace.session_id`, `traceloop.association.properties.*`). Spec-conformant emitters still win; everything else gets pulled into the same graph coordinate system.
- **Enforce shape-independent invariants**, 7 structural checks that run against every fixture: edge endpoint types, Session uniqueness, DAG property of parent/delegation edges, orphan secondaries, token-count and time-ordering sanity.

## 60-second quickstart

```bash
git clone <repo-url> && cd otel-genai-graph
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                                    # 833 unit/integration tests

# optional: live Neo4j + load the six sample fixtures
docker run -d --name otel-neo4j \
    -p 17474:7474 -p 17687:7687 \
    -e NEO4J_AUTH=neo4j/testtest neo4j:5

# configure connection + API keys once — shell env vars always win
cp .env.example .env          # then uncomment / fill in what you need

python -m otel_genai_graph.load tests/fixtures/*.json
# explore — no Cypher required:
python tools/render_graph.py --from-neo4j --query overview \
    --output /tmp/overview --format html && open /tmp/overview.html
```

### Configuration

Every CLI (loader, `tools/render_graph.py`, capture scripts) reads its
settings from environment variables. Copy `.env.example` → `.env` at the
project root, uncomment the blocks you need, and they get auto-loaded.

- Works from any working directory — the loader walks up to find `.env`.
- **Shell env always wins** (`override=False`). CI / container env vars
  can't be shadowed by a committed or stale `.env`.
- Soft dep: `python-dotenv` is in `dependencies`; if it's ever missing
  (minimal container), `load_env()` silently no-ops and you fall back to
  explicit exports.
- `.env` is gitignored (`.env`, `.env.local`); only `.env.example` is
  committed.

Minimum viable `.env` for the quickstart:

```bash
NEO4J_URI=bolt://localhost:17687
NEO4J_USER=neo4j
NEO4J_PASSWORD=testtest
```

## Explore without Cypher

Most users don't want to write Cypher. `tools/render_graph.py` bundles a
curated library of the questions people actually ask, and exports the
answer as an interactive HTML viewer, a structured JSON/GraphML file, or
a plain ASCII table.

### Discover

```bash
python tools/render_graph.py --list-queries
python tools/render_graph.py --list-queries --tag cost
python tools/render_graph.py --describe-query session_tree
```

The v0.1 library (10 queries, full list in [`docs/saved-queries.md`](docs/saved-queries.md)):

| Question | Query name | Output type |
|---|---|---|
| Which agents delegated to which? | `agent_delegation` | graph |
| What did I load? | `overview` | graph |
| Full hierarchy of one conversation | `session_tree` | graph |
| Which tools failed, and what did they touch? | `failed_tools` | graph |
| Agent ↔ DataSource access map | `data_source_usage` | graph |
| Token spend per (provider, model) | `cost_by_model` | table |
| Token spend per session | `cost_by_session` | table |
| Token spend per agent (incl. delegated sub-agents) | `cost_by_agent` | table |
| Tool call counts, ranked | `tool_usage` | table |
| Calls by vendor | `provider_distribution` | table |

### Sample: "where is my token spend going?"

```bash
python tools/render_graph.py --from-neo4j --query cost_by_model --format table
```

Against the bundled fixtures this prints:

```
provider   model                   calls  input_tokens  output_tokens
---------  ----------------------  -----  ------------  -------------
anthropic  claude-sonnet-4-5       7      1760          830
anthropic  claude-opus-4-7         1      500           300
openai     text-embedding-3-small  1      20            0
```

Same query as CSV piped into a spreadsheet / BI tool:

```bash
python tools/render_graph.py --from-neo4j --query cost_by_model \
    --output /tmp/cost --format csv
```

### Sample: "show me one conversation's call tree"

```bash
python tools/render_graph.py --from-neo4j \
    --query session_tree --param session_id=conv-3 \
    --output /tmp/session-conv3 --format html
open /tmp/session-conv3.html
```

`session-conv3.html` is self-contained (cytoscape.js from CDN, everything
else inline). Pan / zoom / hover nodes for properties / click to select.
Share it with a non-technical stakeholder by emailing one file. No
server, no Neo4j required on their side.

### Sample: any fixture, offline, no Neo4j

```bash
python tools/render_graph.py --fixture tests/fixtures/multi_agent.json \
    --output /tmp/multi-agent --format all
# → /tmp/multi-agent.{dot,html,json,graphml} in one shot
```

`--format all` covers the four dep-free formats. Add `.svg` / `.png`
output with `brew install graphviz` (or `apt-get install graphviz`).

### Output formats

| Format    | Flag              | Shape  | When to use                                                 |
|-----------|-------------------|--------|-------------------------------------------------------------|
| `html`    | `--format html`   | graph  | Share with humans. Interactive, single file.                |
| `json`    | `--format json`   | graph  | Feed into D3, Observable, another graph viz, or your tests. |
| `graphml` | `--format graphml`| graph  | Gephi / yEd for force-directed layout + centrality metrics. |
| `dot`     | `--format dot`    | graph  | Pipe into your own `dot` for PDFs / papers.                 |
| `svg`/`png`| `--format svg`   | graph  | Docs / READMEs. Requires graphviz installed.                |
| `table`   | `--format table`  | table  | Terminal output for aggregations.                           |
| `csv`     | `--format csv`    | table  | Spreadsheets, BI tools.                                     |
| `jsonl`   | `--format jsonl`  | table  | Pipe into `jq`, stream-processors, dashboards.              |

The CLI refuses to render a table query as HTML or a graph query as CSV,
so you can't accidentally produce junk output.

### Custom Cypher — for when the saved library isn't enough

```bash
python tools/render_graph.py --from-neo4j \
    --cypher "MATCH (a:Agent)-[:DELEGATED_TO*]->(b:Agent) RETURN a, b" \
    --output /tmp/delegations --format html
```

## Graph schema (v0.1)

| Node          | Natural key                | Sourced from                                 |
|---------------|----------------------------|----------------------------------------------|
| `Session`     | `id`                       | `gen_ai.conversation.id` (+ legacy fallbacks)|
| `Agent`       | `id`                       | `gen_ai.agent.id`                            |
| `Model`       | `(provider, name)`         | `gen_ai.provider.name` + `gen_ai.response.model` |
| `Tool`        | `name`                     | `gen_ai.tool.name`                           |
| `DataSource`  | `id`                       | `gen_ai.data_source.id`                      |
| `Operation`   | `span_id`                  | span id                                      |

| Edge              | Direction                   | Emitted when                                         |
|-------------------|-----------------------------|------------------------------------------------------|
| `CONTAINS`        | `Session → Operation`       | Operation carries a conversation id                  |
| `EXECUTED`        | `Operation → Model`         | Op type ∈ {chat, text_completion, embeddings}        |
| `INVOKED`         | `Agent → Operation`         | Op type ∈ {invoke_agent, create_agent}               |
| `CALLED`          | `Operation → Tool`          | Op type = execute_tool                               |
| `RETRIEVED_FROM`  | `Operation → DataSource`    | Op carries `gen_ai.data_source.id`                   |
| `PARENT_OF`       | `Operation → Operation`     | span has parent_span_id                              |
| `DELEGATED_TO`    | `Agent → Agent`             | child `invoke_agent` under a different parent Agent  |
| `ACCESSED`        | `Agent → DataSource`        | agent-owned Op retrieves from a data source          |

See [`docs/schema.md`](docs/schema.md) for the full reference, extension seams, and open questions for v0.2.

## Raw Cypher (power users)

If the saved-query library doesn't cover your question, the Neo4j
browser (`http://localhost:17474`) is always available. A handful of
useful queries for reference, each also available as a saved query:

```cypher
// agent delegation graph across all loaded traces
// (saved query: agent_delegation)
MATCH p = (a:Agent)-[:DELEGATED_TO*]->(b:Agent) RETURN p

// cost / token attribution per (provider, model)
// (saved query: cost_by_model)
MATCH (o:Operation)-[:EXECUTED]->(m:Model)
RETURN m.provider, m.name,
       count(*)                  AS calls,
       sum(o.input_tokens)       AS in_tok,
       sum(o.output_tokens)      AS out_tok
ORDER BY calls DESC

// blast radius, which agents/sessions did a failing tool touch?
// (saved query: failed_tools — graph shape)
MATCH (t:Tool)<-[:CALLED]-(op:Operation {status:"ERROR"})
      <-[:PARENT_OF*]-(ancestor:Operation)
      <-[:CONTAINS]-(s:Session)
RETURN t, s, collect(DISTINCT ancestor) AS affected_ops
```

To run one-off ad-hoc Cypher through the export pipeline (HTML/JSON/etc.):

```bash
python tools/render_graph.py --from-neo4j \
    --cypher "MATCH (o:Operation {status:'ERROR'}) RETURN o" \
    --output /tmp/errors --format html
```

## Validation pipeline

Four independent data sources catch different bug classes:

1. **Hand-written fixtures**, 6 canonical cases in `tests/fixtures/*.json`. Each ships an `expected_graph` block with hand-counted node/edge totals; the mapper has to match exactly.
2. **Parametric synthesizer**, [`tests/generate_traces.py`](tests/generate_traces.py) emits thousands of deterministic OTLP traces across five shapes (`simple`, `agent_tool`, `multi_agent`, `rag`, `multi_turn`) with a **chaos mode** that introduces dropped attributes, orphaned children, reordered spans, and corrupted trace_ids. The mapper must stay correct on clean traces and must not crash on chaos.
3. **Real-SDK capture**, [`tests/capture_real_traces.py`](tests/capture_real_traces.py) calls actual LLMs (Anthropic, OpenAI, Gemini, Azure OpenAI classic / v1 / Foundry, Ollama) with a `BudgetGuard` that refuses to run past a cap. Five shapes: `chat`, `agent_tool`, `embeddings`, `multi_turn`, `tool_call` (with real tool execution).
4. **Upstream-instrumentor capture**, [`tests/capture_with_instrumentor.py`](tests/capture_with_instrumentor.py) runs spans through Python Contrib's own `OpenAIInstrumentor` / `GoogleGenAiSdkInstrumentor` and dumps what they emit. This is how we found that the Google instrumentor emits the pre-v1.37 `gen_ai.system="gemini"` + `operation.name="generate_content"` shape, now handled by the mapper's legacy-compat table.

All four feed into a **shape-independent invariant test suite** that checks 7 structural properties across every captured graph. See [`docs/mapping.md`](docs/mapping.md) for the full contract and [`tests/test_invariants.py`](tests/test_invariants.py) for the enforcement.

## Supported providers

Out of the box, with the bundled capture scripts:

| Provider           | CLI `--provider` value | Notes                                      |
|--------------------|------------------------|--------------------------------------------|
| Anthropic          | `anthropic`            | `ANTHROPIC_API_KEY`                        |
| OpenAI             | `openai`               | `OPENAI_API_KEY` (+ `OPENAI_BASE_URL` for Ollama/Groq) |
| Azure OpenAI       | `azure_openai`         | classic `/openai/deployments/…` path       |
| Azure OpenAI v1    | `azure_openai_v1`      | new `/openai/v1/` path (Foundry portal)   |
| Azure AI Inference | `azure_inference`      | Foundry unified inference endpoint         |
| Google AI Studio   | `google`               | `GEMINI_API_KEY`; free tier works          |
| Ollama             | `openai` + `OPENAI_BASE_URL=http://localhost:11434/v1/` | local, unlimited, $0 |

**Free paths**: Google AI Studio (no credit card, 1,500 requests/day on Flash), Ollama (local), Groq free tier.

## Streaming export

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from otel_genai_graph.exporter import Neo4jGenAIExporter
from otel_genai_graph.neo4j_sink import Neo4jSink

sink = Neo4jSink("bolt://localhost:17687", "neo4j", "testtest")
sink.connect()
sink.ensure_schema()

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(Neo4jGenAIExporter(sink)))
trace.set_tracer_provider(provider)
# any instrumented code below this line streams into the graph
```

See [`tests/live_export_demo.py`](tests/live_export_demo.py) for a runnable walkthrough.

## Status

**v0.1, reference implementation, 833 tests passing.**
All components live: schema, mapper (with legacy-compat canonicalisation), Neo4j sink (MERGE-based, idempotent), SpanExporter, file loader, cost table, synthesizer, real-API capture, upstream-instrumentor capture, invariants, saved-query library, HTML / JSON / GraphML / CSV / ASCII-table export.

Known open questions, tracked in [`docs/schema.md`](docs/schema.md#open-questions-v02-candidates):

- `Resource` nodes aren't currently emitted (schema has the label; mapper skips it to keep v0.1 simple).
- Streaming spans (partial output): we take the final span only.
- Cost attribution lives as `Operation` properties, not a dedicated `Budget` node, deferred until a concrete UX asks for it.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the dev loop, adding fixtures, adding provider adapters, and the invariant contract.

## License

Apache-2.0.
