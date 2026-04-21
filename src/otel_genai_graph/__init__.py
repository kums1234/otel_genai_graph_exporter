"""otel-genai-graph: OTel GenAI spans → Neo4j graph."""
from .invariants import Violation, check
from .schema import (
    NodeLabel,
    EdgeType,
    OperationType,
    Session,
    Agent,
    Model,
    Tool,
    DataSource,
    Operation,
    Resource,
    Edge,
    Graph,
)

__version__ = "0.1.0"

__all__ = [
    "NodeLabel",
    "EdgeType",
    "OperationType",
    "Session",
    "Agent",
    "Model",
    "Tool",
    "DataSource",
    "Operation",
    "Resource",
    "Edge",
    "Graph",
    "Violation",
    "check",
    "__version__",
]
