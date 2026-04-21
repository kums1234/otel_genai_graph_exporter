# OTLP GenAI spans → Graph — mapping rules

This document is the source of truth for `mapper.map_spans`. It describes
every attribute the mapper reads and every edge it emits, with an explicit
precedence so the implementation (and tests) stay deterministic.

Semconv version: **OpenTelemetry GenAI v1.37**.

## 0. Legacy-attribute compatibility

The canonical reading is OpenTelemetry GenAI semconv **v1.37**. Real-world
instrumentors ship at various points along the spec's evolution, so the
mapper also accepts a small set of pre-v1.37 shapes and canonicalises them
at read-time. The resulting graph looks identical regardless of which SDK
produced the span.

| Legacy attribute / value                          | Canonical (v1.37)                           | Observed in                                      |
|---------------------------------------------------|----------------------------------------------|--------------------------------------------------|
| `gen_ai.system` (whole attribute)                 | `gen_ai.provider.name`                       | most pre-v1.37 instrumentors                     |
| `gen_ai.system = "gemini"`                        | `gen_ai.provider.name = "gcp.gen_ai"`        | `opentelemetry-instrumentation-google-genai` ≤0.7b0 |
| `gen_ai.system = "vertex_ai"`                     | `gen_ai.provider.name = "gcp.vertex_ai"`     | older Vertex instrumentors                       |
| `gen_ai.system = "az.ai.openai"`                  | `gen_ai.provider.name = "azure.ai.openai"`   | older Azure instrumentors                        |
| `gen_ai.system = "az.ai.inference"`               | `gen_ai.provider.name = "azure.ai.inference"`| older Azure instrumentors                        |
| `gen_ai.operation.name = "generate_content"`      | `"chat"`                                     | `opentelemetry-instrumentation-google-genai` ≤0.7b0 |
| `gen_ai.operation.name = "message"`               | `"chat"`                                     | older Anthropic SDK wrappers                     |
| `gen_ai.operation.name = "text_generation"`       | `"text_completion"`                          | HF / custom emitters                             |

Precedence rule: when both `gen_ai.provider.name` and `gen_ai.system` are
present on the same span, `gen_ai.provider.name` wins.

Unknown values pass through unchanged — silently rewriting an unfamiliar
provider or operation name is worse than surfacing it. Add a new row to
`_PROVIDER_ALIAS` / `_OP_NAME_ALIAS` in `mapper.py` when you encounter a
new legacy shape in the wild, with a comment citing the source
instrumentor.

### 0.1 Conversation / Session identity

`gen_ai.conversation.id` is the OTel GenAI v1.37 canonical attribute. It is
**Conditionally Required** by the spec, which also explicitly blesses the
pattern of apps setting it via a custom `SpanProcessor.on_start` hook when
the underlying SDK has no native session concept. As of early 2026, most
OTel Python Contrib instrumentors (openai-v2, google-genai, vertexai,
anthropic, openai-agents-v2) omit it entirely — only the Traceloop SDK
emits it today.

To keep graphs coherent across emitters without breaking OTel
compatibility, the mapper resolves the Session id by walking this priority
list, canonical first. The first key present wins:

| # | Attribute                                             | Source                     |
|---|-------------------------------------------------------|----------------------------|
| 1 | `gen_ai.conversation.id`                              | OTel GenAI v1.37 canonical |
| 2 | `session.id`                                          | Arize OpenInference        |
| 3 | `langsmith.trace.session_id`                          | LangSmith OTel bridge      |
| 4 | `traceloop.association.properties.session_id`         | Traceloop association bag  |
| 5 | `traceloop.association.properties.thread_id`          | Traceloop variant          |

**No fallback to `trace_id`.** Coalescing by trace_id breaks when a single
conversation spans multiple traces (async agents, retries, background
batches) or when multiple conversations share a trace. Apps that want
stable session identity MUST set one of the keys above.

When none of these keys are present, the mapper emits no Session node and
no `CONTAINS` edges — the Operations are written as orphans, which is
legal in the schema and faithful to the input.

## 1. Attribute ingest

The mapper reads only these attributes (plus resource attributes for
`Resource` nodes):

| Span attribute                  | Used for                                          |
|---------------------------------|---------------------------------------------------|
| `gen_ai.operation.name`         | `Operation.type` — decides which edges emit       |
| `gen_ai.provider.name`          | `Model.provider`                                  |
| `gen_ai.response.model`         | `Model.name` (preferred)                          |
| `gen_ai.request.model`          | `Model.name` (fallback when response.model absent)|
| `gen_ai.conversation.id`        | `Session.id`                                      |
| `gen_ai.agent.id`               | `Agent.id`                                        |
| `gen_ai.agent.name`             | `Agent.name`                                      |
| `gen_ai.tool.name`              | `Tool.name`                                       |
| `gen_ai.data_source.id`         | `DataSource.id` (extension — see `schema.md`)     |
| `gen_ai.data_source.kind`       | `DataSource.kind` (extension)                     |
| `gen_ai.usage.input_tokens`     | `Operation.input_tokens` → cost                   |
| `gen_ai.usage.output_tokens`    | `Operation.output_tokens` → cost                  |

Resource attributes used:

| Resource attribute     | Used for                  |
|------------------------|---------------------------|
| `service.name`         | `Resource.service_name`   |
| `service.version`      | `Resource.service_version`|

Everything else is ignored. That's intentional — the mapping is a
**projection**, not a lossless translation.

## 2. Node emission

For every span:

1. If `gen_ai.conversation.id` is set → `Session` node with that id.
2. If `gen_ai.agent.id` is set → `Agent` node.
3. If `gen_ai.operation.name` ∈ {`chat`, `text_completion`, `embeddings`}
   and a model name is resolvable → `Model(provider, name)`.
4. If `gen_ai.operation.name == "execute_tool"` and `gen_ai.tool.name` is
   set → `Tool` node.
5. If `gen_ai.data_source.id` is set → `DataSource` node.
6. Always → `Operation(span_id=…, trace_id=…, type=operation.name, …)`.

Status mapping:

| OTel `status.code` | `Operation.status` |
|--------------------|--------------------|
| 0 (UNSET)          | `"UNSET"`          |
| 1 (OK)             | `"OK"`             |
| 2 (ERROR)          | `"ERROR"`          |

**Status propagation.** After the first pass, walk each trace's span tree
and set `Operation.status = "ERROR"` on every ancestor of an ERROR span.
This is how the `error_case` fixture's test passes.

## 3. Edge emission

Rules are order-independent — an edge is emitted iff its condition
holds. Duplicate edges are deduped by `Edge.__hash__`.

| Edge              | Condition                                                                                   |
|-------------------|---------------------------------------------------------------------------------------------|
| `CONTAINS`        | `Operation` has a `Session` → `Session → Operation`                                         |
| `EXECUTED`        | Op type ∈ {chat, text_completion, embeddings} and `Model` resolved → `Operation → Model`    |
| `INVOKED`         | Op type ∈ {invoke_agent, create_agent} and `Agent` present → `Agent → Operation`            |
| `CALLED`          | Op type = execute_tool and `Tool` present → `Operation → Tool`                              |
| `RETRIEVED_FROM`  | Op has `data_source.id` → `Operation → DataSource`                                          |
| `PARENT_OF`       | span has parent_span_id present in this batch → `parent Op → child Op`                      |
| `DELEGATED_TO`    | Op is `invoke_agent` under another `invoke_agent` with a **different** `agent.id` → `parentAgent → childAgent` |
| `ACCESSED`        | Op has `data_source.id` **and** `agent.id` → `Agent → DataSource` (convenience)             |

Ambiguity rules:

* When multiple models appear on a single span (rare), prefer
  `response.model`. If neither is present, drop the `Model` node and
  skip the `EXECUTED` edge rather than inventing a synthetic id.
* A span missing `conversation.id` still becomes an `Operation` — it
  just has no `Session` edge. Orphan ops are allowed; the sink writes
  them.

## 4. Idempotency

* `Graph.nodes` is keyed by `NodeKey = (label, id)` — re-adding the same
  node overwrites its fields (which, for equal inputs, is a no-op).
* `Graph.edges` is a `set` keyed on `(edge_type, src, dst)`.
* Consequence: `map_spans(X) == map_spans(X)` in node/edge count, and
  writing the result twice through `Neo4jSink.write` is safe.

## 5. What the mapper does NOT do

* **No model inference.** If provider/model isn't on the span, no
  `Model` node.
* **No cost** at mapping time. `cost.py` is a separate utility; call it
  where you want it attached.
* **No cross-batch joining.** Each call to `map_spans` is a pure
  function over its input. Session merging across batches happens at
  the sink (`MERGE Session` unifies them).

## 6. Worked examples

See `tests/fixtures/` — each fixture is a worked example with the
expected graph counted out by hand in the `expected_graph` block.
`test_mapper.py` is the executable version of this document.
