# Graph schema — v0.1

Goal: represent OTel GenAI spans as a **graph** so you can ask structural
questions linear tracing can't answer — cost per agent, blast radius of a
failed tool, who delegated to whom, which sessions hit which data sources.

This is a **reference schema**, not the only possible one. It's small on
purpose — seven node labels, eight edge types. You can extend it cleanly
(we list the seams below).

## Nodes

| Label         | Natural key (`MERGE` key)    | Source attribute(s) in OTel v1.37   |
|---------------|------------------------------|-------------------------------------|
| `Session`     | `id`                         | `gen_ai.conversation.id`            |
| `Agent`       | `id`                         | `gen_ai.agent.id` (+ `agent.name`)  |
| `Model`       | `(provider, name)`           | `gen_ai.provider.name` + `gen_ai.response.model` (fallback `request.model`) |
| `Tool`        | `name`                       | `gen_ai.tool.name`                  |
| `DataSource`  | `id`                         | `gen_ai.data_source.id` (extension) |
| `Operation`   | `span_id`                    | span id                             |
| `Resource`    | `service.name`               | resource attribute (`service.name`) |

Notes:

* **Operation = span.** Everything interesting about an LLM call, tool
  execution, or agent invocation lives on the span — we surface it as a
  node so you can hang metrics and status on it.
* **Model key is composite** `(provider, name)`. Same model name on
  different providers (e.g. `gpt-4o` on `openai` vs `azure`) stays
  distinct.
* **DataSource is a v1.37 extension.** The spec doesn't standardise
  retrieval yet. We adopt `gen_ai.data_source.id` as the pending
  convention — swap it when the WG lands one.

## Edges

| Type              | Direction                   | Emitted when                                                 |
|-------------------|-----------------------------|--------------------------------------------------------------|
| `CONTAINS`        | `Session → Operation`       | Op has `gen_ai.conversation.id`                              |
| `EXECUTED`        | `Operation → Model`         | Op is `chat` \| `text_completion` \| `embeddings`           |
| `INVOKED`         | `Agent → Operation`         | Op is `invoke_agent` \| `create_agent`                       |
| `CALLED`          | `Operation → Tool`          | Op is `execute_tool`                                         |
| `RETRIEVED_FROM`  | `Operation → DataSource`    | Op carries `gen_ai.data_source.id`                           |
| `PARENT_OF`       | `Operation → Operation`     | span has `parent_span_id`                                    |
| `DELEGATED_TO`    | `Agent → Agent`             | child `invoke_agent` under a different parent-agent span     |
| `ACCESSED`        | `Agent → DataSource`        | agent-owned op retrieves from a data source (convenience)    |

All edges carry a `properties` bag for payload like token counts, cost,
status — consult `schema.py` for the staging container and
`docs/mapping.md` for exact attribute → property wiring.

## Write semantics (Neo4j)

* Every node is written with `MERGE` on its natural key, so re-ingesting
  the same spans is a no-op.
* Every edge is written with `MERGE` keyed on `(src, dst, type)` — same
  rule.
* The sink batches with `UNWIND` per label/edge-type (sketch in
  `neo4j_sink.py`).

## Extension seams

* **New node label** — add to `NodeLabel` enum, give it a `key` property,
  add a `MERGE` clause in the sink.
* **New edge type** — add to `EdgeType` enum; the mapper is the only
  place that decides when to emit it.
* **New attribute** — thread it as `Operation.properties[...]` or add a
  typed field on the dataclass.

## Open questions (v0.2 candidates)

* Should `Session` carry a `user.id` when `gen_ai.user.id` is present?
  Probably yes — adds a `USED` edge.
* Streaming spans (partial output) — today we take the final span.
  Tests for streaming semantics are a separate fixture category.
* Cost attribution edges — right now cost lives as a property on
  `Operation`. A `COSTS` edge into a `Budget` node might make rollups
  easier. Deferred until there's a concrete UX asking for it.
