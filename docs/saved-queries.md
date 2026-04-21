# Saved Cypher queries

A curated library of common structural questions against the
otel-genai-graph Neo4j store, runnable without writing Cypher yourself.

The companion CLI is `tools/render_graph.py`, which can both list queries
and execute them against a live Neo4j, piping the result into one of the
export formats documented in [`docs/exports.md`](./exports.md).

## Goals

1. **Zero-Cypher path for the 10 questions most users actually ask.** If
   you want total cost per model, you type
   `render_graph.py --query cost_by_model`, not four lines of MATCH.
2. **Introspectable** — every query declares its parameters, its tags,
   and what kind of result it returns (graph vs. table).
3. **Extensible** — adding a new saved query is a single dict entry with
   a test; the CLI picks it up automatically.

## Usage

```bash
# discover
python tools/render_graph.py --list-queries
python tools/render_graph.py --list-queries --tag cost

# run (graph-returning — renders to HTML / JSON / GraphML)
python tools/render_graph.py --from-neo4j \
    --query session_tree --param session_id=conv-3 \
    --output /tmp/session --format html

# run (table-returning — renders to CSV / JSON-lines / ASCII table)
python tools/render_graph.py --from-neo4j \
    --query cost_by_model --format table
```

## Library contract

Each entry is a `SavedQuery`:

```python
@dataclass(frozen=True)
class SavedQuery:
    name: str                      # unique short id, snake_case
    description: str               # one-sentence summary
    result_type: str               # "graph" or "table"
    tags: tuple[str, ...]          # e.g. ("cost",), ("agents","delegation")
    parameters: tuple[Parameter, ...]  # see below
    cypher: str                    # may reference $param_name
    requires: tuple[str, ...] = () # optional: label names the query assumes exist

@dataclass(frozen=True)
class Parameter:
    name: str                      # cypher binding name
    description: str
    required: bool                 # if False, default must be set
    default: str | None = None
    example: str | None = None     # shown by --list-queries
```

### Result types

- **`graph`** — query returns `Node`, `Relationship`, or `Path` values.
  Output: HTML (interactive cytoscape.js), node-link JSON, or GraphML.
- **`table`** — query returns aggregations, scalar columns.
  Output: rich ASCII table, CSV, or JSON-lines.

The CLI refuses to render a `table` result to HTML (and vice versa) —
it's a hard check against silently producing a useless graph of three
rows of numbers.

### Parameter substitution

Saved queries use Neo4j's `$name` parameter syntax — no string
interpolation, no injection risk. The CLI passes `--param key=value`
pairs straight through the neo4j driver's parameter binding.

## v0.1 library

| Name | Type | Tags | Description |
|---|---|---|---|
| `overview` | graph | `discover` | All nodes, capped at 500. Useful first look. |
| `session_tree` | graph | `session`, `debug` | One session's full hierarchy. `session_id` required. |
| `agent_delegation` | graph | `agents`, `delegation` | `Agent → Agent` delegation chains. |
| `failed_tools` | graph | `errors`, `reliability` | Tools called by failed Operations + ancestor chain. |
| `data_source_usage` | graph | `rag`, `data` | `Agent → DataSource` accesses and the ops that triggered them. |
| `cost_by_model` | table | `cost` | Token spend grouped by `(provider, name)`. |
| `cost_by_session` | table | `cost`, `session` | Token spend grouped by `Session`. |
| `cost_by_agent` | table | `cost`, `agents` | Token spend aggregated per `Agent` (direct ops + delegated ops). |
| `tool_usage` | table | `tools` | Tool call counts, ranked. |
| `provider_distribution` | table | `cost`, `vendor` | Call counts grouped by `gen_ai.provider.name`. |

Every query also carries a short "why this is useful" doc-comment in
`src/otel_genai_graph/saved_queries.py`. Use `--list-queries` to see
them inline.

## Adding a query

1. Append the entry to the `QUERIES` dict in
   `src/otel_genai_graph/saved_queries.py`.
2. Add a row to the table above.
3. Add a test in `tests/test_saved_queries.py` verifying:
   - name is unique and snake_case
   - Cypher parses (we don't run it in unit tests; a smoke test against
     the integration Neo4j instance covers that)
   - parameters declared match `$name` tokens in the Cypher string
   - `result_type` is `"graph"` or `"table"`

The integration test in `tests/test_neo4j_sink.py` runs every
`result_type == "graph"` query against the standard fixture load and
asserts (a) no crash, (b) the returned value types match (Node /
Relationship / Path).

## Stability

Query names are part of the public API — renames go in a CHANGELOG.
Adding new queries or widening the graph of an existing query is
non-breaking. Removing a query or changing its parameter signature IS
breaking and requires a major-version bump.
