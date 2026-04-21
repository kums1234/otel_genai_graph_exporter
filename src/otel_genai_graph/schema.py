"""Graph schema v0.1 — dataclasses and enums for GenAI spans.

Design notes:
  * Every node has a stable natural key (the `key` property). Neo4j MERGE
    uses this key so ingests are idempotent.
  * `Edge` hashes by (edge_type, src, dst); property bags are metadata and
    do not participate in identity. This keeps deduping obvious.
  * The `Graph` container is an in-memory staging buffer — the mapper
    builds it, the sink writes it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NodeLabel(str, Enum):
    SESSION = "Session"
    AGENT = "Agent"
    MODEL = "Model"
    TOOL = "Tool"
    DATA_SOURCE = "DataSource"
    OPERATION = "Operation"
    RESOURCE = "Resource"


class EdgeType(str, Enum):
    CONTAINS = "CONTAINS"           # Session → Operation
    EXECUTED = "EXECUTED"           # Operation → Model
    INVOKED = "INVOKED"             # Agent → Operation
    CALLED = "CALLED"               # Operation → Tool
    RETRIEVED_FROM = "RETRIEVED_FROM"  # Operation → DataSource
    PARENT_OF = "PARENT_OF"         # Operation → Operation
    DELEGATED_TO = "DELEGATED_TO"   # Agent → Agent
    ACCESSED = "ACCESSED"           # Agent → DataSource


class OperationType(str, Enum):
    """OTel GenAI v1.37 gen_ai.operation.name values (+ minor extensions)."""
    CHAT = "chat"
    TEXT_COMPLETION = "text_completion"
    EMBEDDINGS = "embeddings"
    EXECUTE_TOOL = "execute_tool"
    INVOKE_AGENT = "invoke_agent"
    CREATE_AGENT = "create_agent"
    # extension: v1.37 does not standardise retrieval; used only when an op
    # carries gen_ai.data_source.id.
    RETRIEVE = "retrieve"


class Status(str, Enum):
    OK = "OK"
    ERROR = "ERROR"
    UNSET = "UNSET"


# A node key is the (label, natural_id) tuple used for dedup + MERGE.
NodeKey = tuple[str, str]


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Session:
    id: str

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.SESSION.value, self.id)


@dataclass(frozen=True)
class Agent:
    id: str
    name: Optional[str] = None

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.AGENT.value, self.id)


@dataclass(frozen=True)
class Model:
    provider: str
    name: str

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.MODEL.value, f"{self.provider}/{self.name}")


@dataclass(frozen=True)
class Tool:
    name: str

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.TOOL.value, self.name)


@dataclass(frozen=True)
class DataSource:
    id: str
    kind: Optional[str] = None  # e.g. "vector_store", "sql", "http"

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.DATA_SOURCE.value, self.id)


@dataclass(frozen=True)
class Resource:
    service_name: str
    service_version: Optional[str] = None

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.RESOURCE.value, self.service_name)


@dataclass(frozen=True)
class Operation:
    """One OTel span rendered as an Operation node.

    The span_id is the natural key — distinct spans are distinct operations
    even when they share a trace_id or conversation.id.
    """
    span_id: str
    trace_id: str
    type: str  # OperationType value
    name: Optional[str] = None
    start_ns: Optional[int] = None
    end_ns: Optional[int] = None
    status: str = Status.UNSET.value
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    cost_usd: Optional[float] = None
    error_message: Optional[str] = None

    @property
    def key(self) -> NodeKey:
        return (NodeLabel.OPERATION.value, self.span_id)


Node = Session | Agent | Model | Tool | DataSource | Operation | Resource


# ---------------------------------------------------------------------------
# Edges
# ---------------------------------------------------------------------------

@dataclass
class Edge:
    """A labelled directed edge. Identity ignores `properties`."""
    edge_type: str  # EdgeType value
    src: NodeKey
    dst: NodeKey
    properties: dict[str, Any] = field(default_factory=dict)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Edge):
            return NotImplemented
        return (self.edge_type, self.src, self.dst) == (
            other.edge_type,
            other.src,
            other.dst,
        )

    def __hash__(self) -> int:
        return hash((self.edge_type, self.src, self.dst))


# ---------------------------------------------------------------------------
# Graph container
# ---------------------------------------------------------------------------

@dataclass
class Graph:
    """In-memory staging buffer for a mapping pass."""
    nodes: dict[NodeKey, Node] = field(default_factory=dict)
    edges: set[Edge] = field(default_factory=set)

    def add_node(self, node: Node) -> None:
        # last write wins — mapper should ensure equivalent nodes dedup cleanly
        self.nodes[node.key] = node

    def add_edge(self, edge: Edge) -> None:
        self.edges.add(edge)

    # --- query helpers used by tests ---------------------------------------

    def node_count(self, label: Optional[str] = None) -> int:
        if label is None:
            return len(self.nodes)
        return sum(1 for k in self.nodes if k[0] == label)

    def edge_count(self, edge_type: Optional[str] = None) -> int:
        if edge_type is None:
            return len(self.edges)
        return sum(1 for e in self.edges if e.edge_type == edge_type)

    def get(self, key: NodeKey) -> Optional[Node]:
        return self.nodes.get(key)

    def nodes_of(self, label: str) -> list[Node]:
        return [n for k, n in self.nodes.items() if k[0] == label]

    def edges_of(self, edge_type: str) -> list[Edge]:
        return [e for e in self.edges if e.edge_type == edge_type]
