#!/usr/bin/env python3
"""Synthetic OTLP GenAI trace generator.

Emits OTLP/JSON `resourceSpans` in the exact shape of `tests/fixtures/*.json`
and, alongside each trace, an `expected_graph` block **computed from what
the generator emitted** — not from what the mapper returns. That's the
whole point: a mapper bug shows up as a diff between generator-declared
and mapper-observed graph counts.

Shapes
------
  simple          single chat span
  agent_tool      1 agent, 1 tool, K chats
  multi_agent     orchestrator delegating, depth N
  rag             embeddings + K retrievals + chat
  multi_turn      T turns sharing one conversation.id
  random          random mix (useful for bulk corpora)

Chaos
-----
  --chaos enables adversarial mutations — dropped attributes, orphaned
  children, reordered spans, corrupted trace_ids. Chaos output carries
  `"expected_graph": null` because its graph isn't well-defined; the
  mapper is only required to not crash on it.

Examples
--------
  python tests/generate_traces.py --shape multi_agent --depth 4 \\
      --seed 7 --output /tmp/synth.json

  python tests/generate_traces.py --shape random --count 50 --seed 0 \\
      --output-dir tests/fixtures/synth

  python tests/generate_traces.py --shape random --chaos --count 100 \\
      --seed 0 --output-dir tests/fixtures/chaos
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

# ---------------------------------------------------------------------------
# OTLP/JSON primitives
# ---------------------------------------------------------------------------

_MODELS = {
    "anthropic": ["claude-opus-4-7", "claude-sonnet-4-5", "claude-haiku-4-5"],
    "openai":    ["gpt-4o", "gpt-4o-mini", "gpt-4.1"],
    "google":    ["gemini-2.5-pro", "gemini-2.5-flash"],
}
_EMBEDDING_MODELS = {
    "openai": ["text-embedding-3-small", "text-embedding-3-large"],
}
_TOOL_NAMES = [
    "web_search", "db_query", "code_exec", "file_read", "http_fetch",
    "calendar_lookup", "calculator", "image_gen",
]
_AGENT_NAMES = [
    "orchestrator", "researcher", "coder", "critic", "planner", "summarizer",
]


def _s(k: str, v: Any) -> dict:
    return {"key": k, "value": {"stringValue": str(v)}}


def _i(k: str, v: int) -> dict:
    return {"key": k, "value": {"intValue": str(int(v))}}


# ---------------------------------------------------------------------------
# Builder — tracks what it emits and what the graph should look like
# ---------------------------------------------------------------------------

@dataclass
class TraceBuilder:
    seed: int
    service_name: str = "synth-service"
    rng: random.Random = field(init=False)

    _resource_spans: list[dict] = field(init=False, default_factory=list)
    _current_spans: list[dict] = field(init=False, default_factory=list)
    _span_index: dict[str, dict] = field(init=False, default_factory=dict)

    # graph ledger — what the mapper *should* produce
    nodes: dict[tuple, str] = field(init=False, default_factory=dict)
    edges: set[tuple] = field(init=False, default_factory=set)
    _error_spans: set[str] = field(init=False, default_factory=set)
    _span_parents: dict[str, Optional[str]] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)

    # --- id helpers --------------------------------------------------------

    def _trace_id(self) -> str:
        return self.rng.getrandbits(128).to_bytes(16, "big").hex()

    def _span_id(self) -> str:
        return self.rng.getrandbits(64).to_bytes(8, "big").hex()

    # --- trace lifecycle ---------------------------------------------------

    def start_trace(self) -> str:
        """Close the current trace, start a new one. Returns the new trace_id."""
        self._flush()
        trace_id = self._trace_id()
        self._current_trace_id = trace_id  # type: ignore[attr-defined]
        self._current_spans = []
        return trace_id

    def _flush(self) -> None:
        if self._current_spans:
            self._resource_spans.append(
                {
                    "resource": {"attributes": [_s("service.name", self.service_name)]},
                    "scopeSpans": [
                        {
                            "scope": {"name": "synth.generator", "version": "0.1.0"},
                            "spans": self._current_spans,
                        }
                    ],
                }
            )
            # The mapper emits a Resource node from the resource.service.name
            # attribute. Recording it here keeps the expected_graph in sync
            # with what the mapper actually produces. Same service_name across
            # flushes naturally dedupes via the (label, id) dict key.
            self.nodes[("Resource", self.service_name)] = "Resource"
            self._current_spans = []

    # --- low-level span emit ----------------------------------------------

    def _emit_span(
        self,
        span_id: str,
        name: str,
        attrs: list[dict],
        kind: int,
        parent: Optional[str],
        error: bool,
        error_message: Optional[str] = None,
    ) -> dict:
        start = 1_737_000_000_000_000_000 + len(self._span_index) * 10_000_000
        span = {
            "traceId": self._current_trace_id,  # type: ignore[attr-defined]
            "spanId": span_id,
            "parentSpanId": parent or "",
            "name": name,
            "kind": kind,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(start + 500_000_000),
            "attributes": attrs,
            "status": {"code": 2 if error else 1, **({"message": error_message} if error and error_message else {})},
        }
        self._current_spans.append(span)
        self._span_index[span_id] = span
        self._span_parents[span_id] = parent
        if error:
            self._error_spans.add(span_id)
        return span

    # --- primitives --------------------------------------------------------

    def chat(
        self,
        *,
        parent: Optional[str] = None,
        provider: str,
        model: str,
        conv_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        input_tokens: int = 100,
        output_tokens: int = 50,
        error: bool = False,
    ) -> str:
        sid = self._span_id()
        attrs = [
            _s("gen_ai.operation.name", "chat"),
            _s("gen_ai.provider.name", provider),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _i("gen_ai.usage.input_tokens", input_tokens),
            _i("gen_ai.usage.output_tokens", output_tokens),
        ]
        if conv_id:
            attrs.append(_s("gen_ai.conversation.id", conv_id))
        if agent_id:
            attrs.append(_s("gen_ai.agent.id", agent_id))
        self._emit_span(sid, f"chat {model}", attrs, kind=3, parent=parent, error=error)

        op = ("Operation", sid)
        self.nodes[op] = "Operation"
        mk = ("Model", f"{provider}/{model}")
        self.nodes[mk] = "Model"
        self.edges.add(("EXECUTED", op, mk))
        self._attach_session_and_parent(op, conv_id, parent)
        if agent_id:
            self.nodes[("Agent", agent_id)] = "Agent"
        return sid

    def embeddings(
        self,
        *,
        parent: Optional[str] = None,
        provider: str = "openai",
        model: str = "text-embedding-3-small",
        conv_id: Optional[str] = None,
        input_tokens: int = 20,
    ) -> str:
        sid = self._span_id()
        attrs = [
            _s("gen_ai.operation.name", "embeddings"),
            _s("gen_ai.provider.name", provider),
            _s("gen_ai.request.model", model),
            _s("gen_ai.response.model", model),
            _i("gen_ai.usage.input_tokens", input_tokens),
            _i("gen_ai.usage.output_tokens", 0),
        ]
        if conv_id:
            attrs.append(_s("gen_ai.conversation.id", conv_id))
        self._emit_span(sid, f"embeddings {model}", attrs, kind=3, parent=parent, error=False)

        op = ("Operation", sid)
        self.nodes[op] = "Operation"
        mk = ("Model", f"{provider}/{model}")
        self.nodes[mk] = "Model"
        self.edges.add(("EXECUTED", op, mk))
        self._attach_session_and_parent(op, conv_id, parent)
        return sid

    def agent(
        self,
        *,
        agent_id: str,
        name: Optional[str] = None,
        parent: Optional[str] = None,
        conv_id: Optional[str] = None,
    ) -> str:
        sid = self._span_id()
        attrs = [
            _s("gen_ai.operation.name", "invoke_agent"),
            _s("gen_ai.agent.id", agent_id),
        ]
        if name:
            attrs.append(_s("gen_ai.agent.name", name))
        if conv_id:
            attrs.append(_s("gen_ai.conversation.id", conv_id))
        self._emit_span(sid, f"invoke_agent {agent_id}", attrs, kind=1, parent=parent, error=False)

        op = ("Operation", sid)
        self.nodes[op] = "Operation"
        ak = ("Agent", agent_id)
        self.nodes[ak] = "Agent"
        self.edges.add(("INVOKED", ak, op))
        self._attach_session_and_parent(op, conv_id, parent)

        # DELEGATED_TO if parent is an invoke_agent span for a different agent
        if parent and parent in self._span_index:
            p_attrs = {a["key"]: a["value"].get("stringValue") for a in self._span_index[parent]["attributes"]}
            if p_attrs.get("gen_ai.operation.name") in ("invoke_agent", "create_agent"):
                p_agent = p_attrs.get("gen_ai.agent.id")
                if p_agent and p_agent != agent_id:
                    self.edges.add(("DELEGATED_TO", ("Agent", p_agent), ak))
        return sid

    def tool(
        self,
        *,
        tool_name: str,
        parent: Optional[str] = None,
        conv_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        error: bool = False,
    ) -> str:
        sid = self._span_id()
        attrs = [
            _s("gen_ai.operation.name", "execute_tool"),
            _s("gen_ai.tool.name", tool_name),
        ]
        if conv_id:
            attrs.append(_s("gen_ai.conversation.id", conv_id))
        if agent_id:
            attrs.append(_s("gen_ai.agent.id", agent_id))
        self._emit_span(
            sid, f"execute_tool {tool_name}", attrs, kind=1,
            parent=parent, error=error,
            error_message="synthetic tool failure" if error else None,
        )

        op = ("Operation", sid)
        self.nodes[op] = "Operation"
        tk = ("Tool", tool_name)
        self.nodes[tk] = "Tool"
        self.edges.add(("CALLED", op, tk))
        self._attach_session_and_parent(op, conv_id, parent)
        if agent_id:
            self.nodes[("Agent", agent_id)] = "Agent"
        return sid

    def retrieve(
        self,
        *,
        data_source_id: str,
        parent: Optional[str] = None,
        conv_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        data_source_kind: str = "vector_store",
    ) -> str:
        sid = self._span_id()
        attrs = [
            _s("gen_ai.operation.name", "retrieve"),
            _s("gen_ai.data_source.id", data_source_id),
            _s("gen_ai.data_source.kind", data_source_kind),
        ]
        if conv_id:
            attrs.append(_s("gen_ai.conversation.id", conv_id))
        if agent_id:
            attrs.append(_s("gen_ai.agent.id", agent_id))
        self._emit_span(sid, f"retrieve {data_source_id}", attrs, kind=1, parent=parent, error=False)

        op = ("Operation", sid)
        self.nodes[op] = "Operation"
        dsk = ("DataSource", data_source_id)
        self.nodes[dsk] = "DataSource"
        self.edges.add(("RETRIEVED_FROM", op, dsk))
        self._attach_session_and_parent(op, conv_id, parent)
        if agent_id:
            self.nodes[("Agent", agent_id)] = "Agent"
            self.edges.add(("ACCESSED", ("Agent", agent_id), dsk))
        return sid

    # --- helpers -----------------------------------------------------------

    def _attach_session_and_parent(
        self,
        op_key: tuple,
        conv_id: Optional[str],
        parent: Optional[str],
    ) -> None:
        if conv_id:
            sk = ("Session", conv_id)
            self.nodes[sk] = "Session"
            self.edges.add(("CONTAINS", sk, op_key))
        if parent:
            self.edges.add(("PARENT_OF", ("Operation", parent), op_key))

    def _propagate_errors(self) -> None:
        """Match mapper rule: every ancestor of an ERROR span is ERROR."""
        closure = set()
        for sid in self._error_spans:
            cur: Optional[str] = sid
            while cur and cur not in closure:
                closure.add(cur)
                cur = self._span_parents.get(cur)
        self._error_closure = closure  # type: ignore[attr-defined]

    # --- output ------------------------------------------------------------

    def finalize(self) -> dict:
        self._flush()
        self._propagate_errors()

        nodes_by_label: dict[str, int] = {}
        for (_key, label) in self.nodes.items():
            nodes_by_label[label] = nodes_by_label.get(label, 0) + 1

        edges_by_type: dict[str, int] = {}
        for (t, _s_, _d) in self.edges:
            edges_by_type[t] = edges_by_type.get(t, 0) + 1

        return {
            "otlp": {"resourceSpans": self._resource_spans},
            "expected_graph": {
                "nodes": nodes_by_label,
                "edges": edges_by_type,
                "total_nodes": sum(nodes_by_label.values()),
                "total_edges": sum(edges_by_type.values()),
                "error_ops": sorted(self._error_closure),  # type: ignore[attr-defined]
            },
        }


# ---------------------------------------------------------------------------
# Shape generators
# ---------------------------------------------------------------------------

def shape_simple(tb: TraceBuilder, provider: str, model: str) -> None:
    tb.start_trace()
    conv = f"conv-simple-{tb.rng.randint(1000, 9999)}"
    tb.chat(provider=provider, model=model, conv_id=conv,
            input_tokens=tb.rng.randint(50, 500),
            output_tokens=tb.rng.randint(20, 300))


def shape_agent_tool(
    tb: TraceBuilder,
    provider: str,
    model: str,
    tools_per_call: int,
    error_rate: float,
) -> None:
    tb.start_trace()
    conv = f"conv-agent-{tb.rng.randint(1000, 9999)}"
    agent_id = f"agent-{tb.rng.choice(_AGENT_NAMES)}"
    root = tb.agent(agent_id=agent_id, name=agent_id, conv_id=conv)
    # initial reasoning
    tb.chat(parent=root, provider=provider, model=model, conv_id=conv,
            agent_id=agent_id, input_tokens=200, output_tokens=150)
    for _ in range(tools_per_call):
        is_error = tb.rng.random() < error_rate
        tb.tool(tool_name=tb.rng.choice(_TOOL_NAMES), parent=root,
                conv_id=conv, agent_id=agent_id, error=is_error)
    # wrap-up
    tb.chat(parent=root, provider=provider, model=model, conv_id=conv,
            agent_id=agent_id, input_tokens=400, output_tokens=200)


def shape_multi_agent(
    tb: TraceBuilder,
    provider: str,
    model: str,
    depth: int,
) -> None:
    tb.start_trace()
    conv = f"conv-multi-{tb.rng.randint(1000, 9999)}"
    agents = tb.rng.sample(_AGENT_NAMES, k=min(depth, len(_AGENT_NAMES)))
    parent: Optional[str] = None
    for agent_name in agents:
        parent = tb.agent(agent_id=agent_name, name=agent_name,
                          parent=parent, conv_id=conv)
    # innermost agent does the work
    tb.chat(parent=parent, provider=provider, model=model, conv_id=conv,
            agent_id=agents[-1], input_tokens=500, output_tokens=300)


def shape_rag(
    tb: TraceBuilder,
    provider: str,
    model: str,
    retrievals: int,
) -> None:
    tb.start_trace()
    conv = f"conv-rag-{tb.rng.randint(1000, 9999)}"
    embed_model = tb.rng.choice(_EMBEDDING_MODELS["openai"])
    emb = tb.embeddings(provider="openai", model=embed_model, conv_id=conv,
                        input_tokens=tb.rng.randint(10, 60))
    parent = emb
    for i in range(retrievals):
        parent = tb.retrieve(
            data_source_id=f"vector-store-{i}",
            parent=parent, conv_id=conv,
        )
    tb.chat(parent=parent, provider=provider, model=model, conv_id=conv,
            input_tokens=800, output_tokens=150)


def shape_multi_turn(
    tb: TraceBuilder,
    provider: str,
    model: str,
    turns: int,
) -> None:
    conv = f"conv-mt-{tb.rng.randint(1000, 9999)}"
    for _ in range(turns):
        tb.start_trace()  # new trace_id each turn, shared conversation.id
        tb.chat(provider=provider, model=model, conv_id=conv,
                input_tokens=tb.rng.randint(50, 400),
                output_tokens=tb.rng.randint(30, 300))


def shape_random(tb: TraceBuilder, provider: str, error_rate: float) -> None:
    pick = tb.rng.choice([
        lambda: shape_simple(tb, provider, tb.rng.choice(_MODELS[provider])),
        lambda: shape_agent_tool(tb, provider, tb.rng.choice(_MODELS[provider]),
                                  tools_per_call=tb.rng.randint(1, 5),
                                  error_rate=error_rate),
        lambda: shape_multi_agent(tb, provider, tb.rng.choice(_MODELS[provider]),
                                   depth=tb.rng.randint(2, 4)),
        lambda: shape_rag(tb, provider, tb.rng.choice(_MODELS[provider]),
                           retrievals=tb.rng.randint(1, 3)),
        lambda: shape_multi_turn(tb, provider, tb.rng.choice(_MODELS[provider]),
                                  turns=tb.rng.randint(2, 5)),
    ])
    pick()


# ---------------------------------------------------------------------------
# Chaos mutations — invalidates expected_graph on purpose
# ---------------------------------------------------------------------------

def _walk_spans(otlp: dict):
    for rs in otlp["resourceSpans"]:
        for ss in rs.get("scopeSpans", []):
            for span in ss.get("spans", []):
                yield span


def apply_chaos(otlp: dict, rng: random.Random) -> dict:
    mutations = [
        _chaos_drop_attr,
        _chaos_orphan_child,
        _chaos_reorder_spans,
        _chaos_corrupt_trace_id,
        _chaos_duplicate_span,
    ]
    # apply 1-3 random mutations
    for mut in rng.sample(mutations, k=rng.randint(1, min(3, len(mutations)))):
        mut(otlp, rng)
    return otlp


def _chaos_drop_attr(otlp: dict, rng: random.Random) -> None:
    for span in _walk_spans(otlp):
        if span["attributes"] and rng.random() < 0.25:
            span["attributes"].pop(rng.randrange(len(span["attributes"])))


def _chaos_orphan_child(otlp: dict, rng: random.Random) -> None:
    for span in _walk_spans(otlp):
        if span.get("parentSpanId") and rng.random() < 0.2:
            span["parentSpanId"] = rng.getrandbits(64).to_bytes(8, "big").hex()


def _chaos_reorder_spans(otlp: dict, rng: random.Random) -> None:
    for rs in otlp["resourceSpans"]:
        for ss in rs.get("scopeSpans", []):
            rng.shuffle(ss["spans"])


def _chaos_corrupt_trace_id(otlp: dict, rng: random.Random) -> None:
    for span in _walk_spans(otlp):
        if rng.random() < 0.1:
            span["traceId"] = "X" * 32  # non-hex


def _chaos_duplicate_span(otlp: dict, rng: random.Random) -> None:
    for rs in otlp["resourceSpans"]:
        for ss in rs.get("scopeSpans", []):
            if ss["spans"] and rng.random() < 0.15:
                dup = json.loads(json.dumps(ss["spans"][0]))
                ss["spans"].append(dup)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

SHAPES: dict[str, Callable[..., None]] = {
    "simple":      lambda tb, **kw: shape_simple(tb, kw["provider"], kw["model"]),
    "agent_tool":  lambda tb, **kw: shape_agent_tool(
        tb, kw["provider"], kw["model"],
        kw["tools_per_call"], kw["error_rate"],
    ),
    "multi_agent": lambda tb, **kw: shape_multi_agent(
        tb, kw["provider"], kw["model"], kw["depth"],
    ),
    "rag":         lambda tb, **kw: shape_rag(
        tb, kw["provider"], kw["model"], kw["rag_retrievals"],
    ),
    "multi_turn":  lambda tb, **kw: shape_multi_turn(
        tb, kw["provider"], kw["model"], kw["turns"],
    ),
    "random":      lambda tb, **kw: shape_random(
        tb, kw["provider"], kw["error_rate"],
    ),
}


def generate_one(args: argparse.Namespace, index: int) -> dict:
    seed = args.seed + index
    tb = TraceBuilder(seed=seed)
    provider = args.provider
    model = args.model or TraceBuilder(seed=seed + 1).rng.choice(_MODELS[provider])
    SHAPES[args.shape](
        tb,
        provider=provider,
        model=model,
        tools_per_call=args.tools_per_call,
        depth=args.depth,
        rag_retrievals=args.rag_retrievals,
        turns=args.turns,
        error_rate=args.error_rate,
    )
    result = tb.finalize()
    result["name"] = f"{args.shape}_{seed}"
    result["description"] = f"synthetic {args.shape} (seed={seed})"
    if args.chaos:
        apply_chaos(result["otlp"], random.Random(seed + 9999))
        result["expected_graph"] = None  # graph is no longer well-defined
    return result


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--shape", choices=sorted(SHAPES), default="random")
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--provider", choices=sorted(_MODELS), default="anthropic")
    p.add_argument("--model", default=None, help="specific model name; default=random pick for --provider")
    p.add_argument("--depth", type=int, default=3, help="multi_agent depth")
    p.add_argument("--tools-per-call", type=int, default=2)
    p.add_argument("--rag-retrievals", type=int, default=2)
    p.add_argument("--turns", type=int, default=3)
    p.add_argument("--error-rate", type=float, default=0.0)
    p.add_argument("--chaos", action="store_true")
    p.add_argument("--output", type=Path, default=None, help="single-file output")
    p.add_argument("--output-dir", type=Path, default=None, help="bulk output dir; --count files")
    args = p.parse_args(argv)

    if args.output is None and args.output_dir is None:
        # stdout
        for i in range(args.count):
            print(json.dumps(generate_one(args, i), indent=2))
        return 0

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        for i in range(args.count):
            doc = generate_one(args, i)
            path = args.output_dir / f"{doc['name']}.json"
            path.write_text(json.dumps(doc, indent=2))
        print(f"wrote {args.count} traces → {args.output_dir}", file=sys.stderr)
        return 0

    # single --output
    if args.count != 1:
        print("--output is single-file; use --output-dir for --count > 1", file=sys.stderr)
        return 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(generate_one(args, 0), indent=2))
    print(f"wrote 1 trace → {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
