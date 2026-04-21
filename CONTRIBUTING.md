# Contributing

Thanks for your interest. This project is a reference implementation — it
stays valuable only if every change keeps the mapper faithful and the
validation pipeline tight. A few conventions make that possible.

## Dev loop

```bash
git clone <repo-url> && cd otel-genai-graph
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest                       # unit + mapper + synth + invariants + legacy-compat
```

Integration tests against a live Neo4j auto-skip when `NEO4J_URI` is
unset. To run them:

```bash
docker run -d --name otel-neo4j \
    -p 17474:7474 -p 17687:7687 \
    -e NEO4J_AUTH=neo4j/testtest neo4j:5
export NEO4J_URI=bolt://localhost:17687 NEO4J_USER=neo4j NEO4J_PASSWORD=testtest
pytest tests/test_neo4j_sink.py tests/test_exporter.py
```

CI runs both matrices automatically; see `.github/workflows/test.yml`.

## The four data sources

Every mapper / sink / invariant change is validated against **all four**:

1. **Hand-written fixtures** (`tests/fixtures/*.json`) — canonical cases
   with hand-counted `expected_graph` blocks. Count-based.
2. **Synthesizer** (`tests/generate_traces.py`) — parametric, thousands
   of deterministic traces + chaos mode. Count + shape.
3. **Real-API capture** (`tests/capture_real_traces.py`) — your own
   Ollama / Gemini / Anthropic / OpenAI key, budget-capped. Structural.
4. **Upstream-instrumentor capture** (`tests/capture_with_instrumentor.py`)
   — Python Contrib's own emitters. Cross-implementation.

And wrapped around all four: the **invariant suite**
(`tests/test_invariants.py`) runs 7 shape-independent checks on every
graph — edge endpoint labels, Session uniqueness, acyclicity of
`PARENT_OF` / `DELEGATED_TO`, orphan secondaries, token-count sanity,
time ordering.

## Adding a fixture

Canonical fixtures live in `tests/fixtures/` with an `expected_graph`
block of hand-counted node / edge totals:

```json
{
  "name": "…",
  "description": "…",
  "otlp": {"resourceSpans": [ … ]},
  "expected_graph": {
    "nodes": {"Session": 1, "Model": 1, "Operation": 1},
    "edges": {"CONTAINS": 1, "EXECUTED": 1},
    "total_nodes": 3,
    "total_edges": 2
  }
}
```

Once dropped in, `pytest tests/test_mapper.py` and
`pytest tests/test_invariants.py` pick it up automatically via glob.

## Adding a provider adapter

The capture CLIs share one registry. To add (say) OpenRouter:

1. Write `_call_openrouter(model, prompt) -> (text, input_tokens, output_tokens)` in `tests/capture_real_traces.py`.
2. Register it:
   ```python
   PROVIDERS["openrouter"] = ProviderInfo(
       _call_openrouter,
       "openrouter",           # value emitted as gen_ai.provider.name
       "openrouter",           # key into cost.PRICING (or reuse "openai" if compat)
   )
   ```
3. If your provider is OpenAI-SDK-compatible (`OPENAI_BASE_URL` works),
   extend `_openai_client_for` so the `embeddings` and `tool_call` shapes
   reuse your adapter for free.

If the provider ships an official OTel instrumentor, also register it in
`tests/capture_with_instrumentor.py`'s `INSTRUMENTORS` map — that's how
we surface cross-implementation divergence.

## Adding a legacy-attribute alias

When a new instrumentor emits a pre-v1.37 shape, add a row to
`mapper._PROVIDER_ALIAS` or `mapper._OP_NAME_ALIAS` (or `_CONVERSATION_KEYS`
for session-id), with a comment citing the source:

```python
_OP_NAME_ALIAS["generate_content"] = "chat"
# opentelemetry-instrumentation-google-genai ≤0.7b0 emits the SDK method
# name rather than the v1.37 canonical "chat".
```

Then pin it with a test in `tests/test_legacy_compat.py`. Rule: never
silently rewrite an unfamiliar value — surface it and let the caller
decide. Canonical v1.37 keys always win over aliases.

## Adding an invariant

Invariants live in `src/otel_genai_graph/invariants.py`. Each is a
single function `Graph → Iterator[Violation]` registered in `check()`.

Every new invariant MUST ship with:

- A positive test running it against the existing fixtures (it must not
  trip on any of them).
- A **negative test** that hand-crafts a broken graph and asserts the
  invariant fires — see `test_invariants.py::test_detects_cycle_in_parent_of`
  for the pattern. This keeps invariants from silently becoming no-ops.

## Style

- Stdlib + documented deps only.
- Type hints on public functions. `Any` is fine in the ingest path where
  OTLP/JSON is genuinely untyped.
- Comments explain *why*, not *what*. The mapper in particular has
  non-obvious choices (error-closure sweep, canonical-wins-alias rule,
  PARENT_OF under streaming) — keep those comments current.
- Ruff config is in `pyproject.toml`; CI runs it non-blocking for now.

## Commit messages

Short imperative summary (< 72 chars), then a body explaining *why* and
which data sources you ran against:

```
mapper: treat `message` as chat alias for Anthropic SDK wrappers

…because older Anthropic instrumentation emits operation.name="message"
rather than v1.37 "chat". Verified against handwritten fixtures,
100-seed synth corpus, and the anthropic capture fixture under
tests/fixtures/real/.
```

## Philosophy

This project is opinionated on two points:

1. **The OTel GenAI semconv v1.37 is canonical.** We expand the mapper's
   tolerance for legacy shapes, but we never emit non-v1.37 attribute
   names ourselves. Canonical always wins in precedence.
2. **Invariants are a ratchet.** They run in milliseconds and they
   enforce structural correctness across every data source. Adding one
   is cheap; weakening one should be rare and justified in a commit.

Please match both when contributing.
