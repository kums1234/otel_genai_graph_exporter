"""otel-genai-graph: OTel GenAI spans → Neo4j or DuckDB graph."""
from importlib.metadata import PackageNotFoundError, version as _pkg_version

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

# `__version__` is derived from packaging metadata so it can never drift
# from the wheel's actual version. Hard-coding it here previously caused
# `__version__` to lag a release behind `pyproject.toml`. See
# tests/test_version.py for the lock-in invariant.
try:
    __version__ = _pkg_version("otel-genai-graph")
except PackageNotFoundError:  # editable / not installed (rare — dev only)
    __version__ = "0.0.0+unknown"

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
