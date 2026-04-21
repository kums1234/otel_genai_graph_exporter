"""Graph-level structural invariants.

`check(graph)` returns a list of `Violation`s; an empty list means the
graph is well-formed against the v0.1 schema.

Invariants are intentionally *shape-independent*: they hold regardless of
what the trace looks like (simple chat, agent tree, RAG, etc.), so they
catch mapper bugs that count-based fixture tests can miss.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

from .schema import Graph


# ---------------------------------------------------------------------------
# Violation record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Violation:
    invariant: str
    message: str
    context: tuple = field(default_factory=tuple)

    def __str__(self) -> str:  # pragma: no cover - dev convenience
        return f"[{self.invariant}] {self.message}"


# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------

# Every edge type must link these two labels in this order.
_EDGE_SRC_DST: dict[str, tuple[str, str]] = {
    "CONTAINS":       ("Session",    "Operation"),
    "EXECUTED":       ("Operation",  "Model"),
    "INVOKED":        ("Agent",      "Operation"),
    "CALLED":         ("Operation",  "Tool"),
    "RETRIEVED_FROM": ("Operation",  "DataSource"),
    "PARENT_OF":      ("Operation",  "Operation"),
    "DELEGATED_TO":   ("Agent",      "Agent"),
    "ACCESSED":       ("Agent",      "DataSource"),
}

_MODEL_OPS = {"chat", "text_completion", "embeddings"}
_AGENT_OPS = {"invoke_agent", "create_agent"}

# Labels that exist only to be pointed at — an instance with no incoming
# reference is dead weight in the graph and likely a mapper bug.
_SECONDARY_LABELS = {"Model", "Tool", "DataSource"}

# These edge types must be acyclic. Nested agents or spans forming a cycle
# would mean the mapper confused parent/child direction somewhere.
_ACYCLIC_EDGE_TYPES = ("PARENT_OF", "DELEGATED_TO")


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def check(graph: Graph) -> list[Violation]:
    """Run every invariant check; return an aggregated violation list."""
    violations: list[Violation] = []
    violations.extend(_edge_endpoint_labels(graph))
    violations.extend(_session_uniqueness(graph))
    violations.extend(_operation_cardinalities(graph))
    violations.extend(_invoked_targets(graph))
    violations.extend(_no_orphan_secondaries(graph))
    violations.extend(_acyclic(graph))
    violations.extend(_value_ranges(graph))
    return violations


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _edge_endpoint_labels(g: Graph) -> Iterator[Violation]:
    """Every edge links labels in the shape `schema.md` declares."""
    for e in g.edges:
        expected = _EDGE_SRC_DST.get(e.edge_type)
        if expected is None:
            yield Violation("unknown_edge_type", f"edge type {e.edge_type!r} is not in schema")
            continue
        if e.src[0] != expected[0] or e.dst[0] != expected[1]:
            yield Violation(
                "edge_endpoint_labels",
                f"{e.edge_type}: expected {expected[0]}->{expected[1]}, "
                f"got {e.src[0]}->{e.dst[0]}",
                context=(e.src, e.dst),
            )


def _session_uniqueness(g: Graph) -> Iterator[Violation]:
    """No Operation belongs to more than one Session."""
    by_op: dict[tuple, tuple] = {}
    for e in g.edges:
        if e.edge_type != "CONTAINS":
            continue
        if e.dst in by_op and by_op[e.dst] != e.src:
            yield Violation(
                "session_uniqueness",
                f"Operation {e.dst[1]} contained by multiple Sessions "
                f"({by_op[e.dst][1]}, {e.src[1]})",
                context=(e.dst,),
            )
        by_op[e.dst] = e.src


def _operation_cardinalities(g: Graph) -> Iterator[Violation]:
    """
    - chat/embeddings/text_completion → at most one EXECUTED Model.
    - execute_tool                    → at most one CALLED Tool.
    """
    executed_out: dict[tuple, int] = {}
    called_out: dict[tuple, int] = {}
    for e in g.edges:
        if e.edge_type == "EXECUTED":
            executed_out[e.src] = executed_out.get(e.src, 0) + 1
        elif e.edge_type == "CALLED":
            called_out[e.src] = called_out.get(e.src, 0) + 1

    for key, node in g.nodes.items():
        if key[0] != "Operation":
            continue
        op_type = getattr(node, "type", None)
        if op_type in _MODEL_OPS and executed_out.get(key, 0) > 1:
            yield Violation(
                "executed_cardinality",
                f"Operation {key[1]} (type={op_type}) has "
                f"{executed_out[key]} EXECUTED edges (expected ≤1)",
                context=(key,),
            )
        if op_type == "execute_tool" and called_out.get(key, 0) > 1:
            yield Violation(
                "called_cardinality",
                f"execute_tool op {key[1]} has {called_out[key]} "
                f"CALLED edges (expected ≤1)",
                context=(key,),
            )


def _invoked_targets(g: Graph) -> Iterator[Violation]:
    """INVOKED may only target an Operation whose type is invoke_agent/create_agent."""
    op_type_by_key = {
        key: getattr(node, "type", None)
        for key, node in g.nodes.items()
        if key[0] == "Operation"
    }
    for e in g.edges:
        if e.edge_type != "INVOKED":
            continue
        t = op_type_by_key.get(e.dst)
        if t is not None and t not in _AGENT_OPS:
            yield Violation(
                "invoked_target_type",
                f"INVOKED -> Operation {e.dst[1]} whose type is {t!r}, "
                f"expected one of {sorted(_AGENT_OPS)}",
                context=(e.dst,),
            )


def _no_orphan_secondaries(g: Graph) -> Iterator[Violation]:
    """Model / Tool / DataSource nodes must have at least one incoming edge."""
    incoming: dict[tuple, int] = {k: 0 for k in g.nodes}
    for e in g.edges:
        incoming[e.dst] = incoming.get(e.dst, 0) + 1
    for key in g.nodes:
        if key[0] in _SECONDARY_LABELS and incoming.get(key, 0) == 0:
            yield Violation(
                "orphan_secondary",
                f"{key[0]} {key[1]} has no incoming edges",
                context=(key,),
            )


def _acyclic(g: Graph) -> Iterator[Violation]:
    """PARENT_OF and DELEGATED_TO must be DAGs."""
    for et in _ACYCLIC_EDGE_TYPES:
        adj: dict[tuple, list[tuple]] = {}
        for e in g.edges:
            if e.edge_type == et:
                adj.setdefault(e.src, []).append(e.dst)
        # iterative DFS; WHITE=0, GRAY=1, BLACK=2
        color: dict[tuple, int] = {}
        for n in adj:
            color.setdefault(n, 0)
        for children in adj.values():
            for c in children:
                color.setdefault(c, 0)

        for root in list(color):
            if color[root] != 0:
                continue
            stack: list[tuple[tuple, int]] = [(root, 0)]
            color[root] = 1
            while stack:
                node, idx = stack[-1]
                children = adj.get(node, ())
                if idx == len(children):
                    color[node] = 2
                    stack.pop()
                    continue
                stack[-1] = (node, idx + 1)
                nxt = children[idx]
                c = color.get(nxt, 0)
                if c == 1:  # back edge → cycle
                    yield Violation(
                        "cycle",
                        f"cycle in {et} at {nxt[1]}",
                        context=(nxt,),
                    )
                    # unwind and break — one cycle report per edge type is enough
                    stack.clear()
                    break
                if c == 0:
                    color[nxt] = 1
                    stack.append((nxt, 0))


def _value_ranges(g: Graph) -> Iterator[Violation]:
    """Sanity on numeric fields of Operation nodes."""
    for key, node in g.nodes.items():
        if key[0] != "Operation":
            continue
        start_ns = getattr(node, "start_ns", None)
        end_ns = getattr(node, "end_ns", None)
        if start_ns is not None and end_ns is not None and start_ns > end_ns:
            yield Violation(
                "time_ordering",
                f"Operation {key[1]} start_ns ({start_ns}) > end_ns ({end_ns})",
                context=(key,),
            )
        for field_name in ("input_tokens", "output_tokens"):
            val = getattr(node, field_name, None)
            if val is not None and val < 0:
                yield Violation(
                    "negative_tokens",
                    f"Operation {key[1]} {field_name}={val}",
                    context=(key,),
                )
