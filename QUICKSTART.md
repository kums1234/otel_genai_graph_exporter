# Quickstart — 5 minutes to your first cost-by-model query

You'll go from `pip install` to a SQL aggregate over real LLM telemetry in
five commands. No Neo4j, no API keys, no Docker — just Python 3.10+ and a
shell.

> **Prefer the graph backend?** Skip to [Quickstart — Neo4j](#quickstart--neo4j) further down.

---

## Quickstart — DuckDB (recommended for first try)

### 1. Install

```bash
pip install 'otel-genai-graph[duckdb]'
```

Requires Python 3.10 or newer. Pulls `duckdb` and `opentelemetry-sdk`.

### 2. Grab a sample trace

```bash
curl -sSO https://raw.githubusercontent.com/kums1234/otel_genai_graph_exporter/main/tests/fixtures/multi_agent.json
```

`multi_agent.json` is a hand-written OTLP/JSON capture of an orchestrator
agent delegating to a specialist agent that calls Claude. Three operations,
two agents, one model — small enough to follow by eye, real enough to
exercise every load path.

### 3. Load it into a single-file DuckDB

```bash
python -m otel_genai_graph.load multi_agent.json \
    --backend duckdb \
    --duckdb-path ./trace.duckdb
```

You should see:

```text
-- backend: duckdb
  multi_agent.json: 8 nodes, 9 edges
-- wrote 8 node-ops, 9 edge-ops across 1 file(s)
```

That's it — `./trace.duckdb` is now a queryable single-file database. You
can `scp` it, email it, hand it to anyone with a SQL client.

### 4. Ask "what did this trace cost, by model?"

```bash
python -c "
import duckdb
con = duckdb.connect('./trace.duckdb', read_only=True)
rows = con.execute('''
    SELECT model_provider, model_name,
           count(*)                                            AS calls,
           sum(coalesce(input_tokens,0)+coalesce(output_tokens,0)) AS tokens
    FROM ops
    WHERE model_provider IS NOT NULL
    GROUP BY 1, 2
    ORDER BY tokens DESC
''').fetchall()
for r in rows:
    print(r)
"
```

Output:

```text
('anthropic', 'claude-opus-4-7', 1, 800)
```

Three numbers from one SQL query: vendor, model, total tokens used. No
joins, no graph traversal, no Cypher. The wide `ops` table flattens every
structural relationship into FK columns so questions like *"cost per
agent"*, *"which tools failed"*, *"which session was most expensive"* are
all one-line `GROUP BY`s.

### 5. Try one more — agent delegation chains

```bash
python -c "
import duckdb
con = duckdb.connect('./trace.duckdb', read_only=True)
print(con.execute('SELECT * FROM agent_delegations').fetchall())
"
```

Output:

```text
[('orchestrator', 'specialist')]
```

The `Agent → Agent` delegation edge survives the trace boundary —
`orchestrator` delegated to `specialist`. Useful for blast-radius
questions: *"if the specialist agent breaks, which orchestrators are
affected?"*

---

## Where to go next

### Try it on your own traces

You have three options, ranked by effort:

1. **Already have OTel GenAI traces in OTLP/JSON?** Point the loader at
   them: `python -m otel_genai_graph.load my_traces/*.json --backend duckdb --duckdb-path ./prod.duckdb`. Re-running over the same files is
   idempotent — node and edge counts stay stable.
2. **Have an LLM app instrumented with one of the OTel GenAI
   instrumentors** (`opentelemetry-instrumentation-openai-v2`,
   `opentelemetry-instrumentation-google-genai`, Traceloop, …)? Wire up
   the `GenAIExporter` directly — see the [README's *Streaming live*
   section](README.md#streaming-live-from-an-otel-tracerprovider).
3. **Don't have OTel set up yet?** The [`tests/` directory](tests/)
   contains capture scripts for OpenAI, Anthropic, Google, Azure OpenAI,
   Ollama, and Groq. Set the relevant API key, run
   `python tests/capture_real_traces.py --provider openai`, then load the
   captured JSON the same way as step 3 above.

### Browse the saved-query library

Twelve named SQL queries beyond `cost_by_model`:

```python
from otel_genai_graph.saved_queries_sql import QUERIES, get_query

for q in QUERIES.values():
    print(f"  {q.name:32} {q.description}")

# Run one
import duckdb
con = duckdb.connect('./trace.duckdb', read_only=True)
print(con.execute(get_query('cost_by_agent_with_descendants').sql).fetchall())
```

Includes a recursive-CTE `cost_by_agent_with_descendants` that walks the
delegation graph for hierarchical cost rollups.

---

## Quickstart — Neo4j

If you want graph traversals and the interactive HTML viewer
(`tools/render_graph.py`), use the Neo4j backend instead.

```bash
# 1. Install with the neo4j extra
pip install 'otel-genai-graph[neo4j]'

# 2. Run a local Neo4j (Docker)
docker run -d --name otel-neo4j \
    -p 7474:7474 -p 7687:7687 \
    -e NEO4J_AUTH=neo4j/testtest neo4j:5

# 3. Load the same fixture
curl -sSO https://raw.githubusercontent.com/kums1234/otel_genai_graph_exporter/main/tests/fixtures/multi_agent.json
NEO4J_PASSWORD=testtest python -m otel_genai_graph.load multi_agent.json

# 4. Render an interactive HTML graph (requires the dev install)
python tools/render_graph.py --from-neo4j --query overview \
    --output /tmp/multi --format html
open /tmp/multi.html
```

See the [README → Backends](README.md#backends--neo4j-or-duckdb) section
for when to pick which.

---

## Got stuck?

- Open an [issue](https://github.com/kums1234/otel_genai_graph_exporter/issues/new/choose) — the bug-report template asks for the four things we'd need anyway (Python version, backend, version of `otel-genai-graph`, repro).
- Open a [discussion](https://github.com/kums1234/otel_genai_graph_exporter/discussions) for usage questions or *"is this the right tool for X?"* prompts.
- Honest feedback is the most valuable thing right now — *"I gave up at step 2"* is more useful than *"looks cool"*. The first 5 minutes of friction is what gets fixed first.
