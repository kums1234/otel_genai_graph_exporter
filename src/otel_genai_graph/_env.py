"""Optional `.env` auto-loader.

Reads `.env` from the current working directory or any parent (project
root, typically). Shell env vars always win — `.env` only fills blanks,
so CI / production overrides are never shadowed.

The import is soft: if `python-dotenv` isn't installed, `load_env()` is a
silent no-op. That keeps the library importable in minimal environments
(CI that doesn't need dotfile support, containerised deployments where
env comes from the orchestrator, etc.).

Call once from each CLI entry point before argparse reads `os.environ`.
"""
from __future__ import annotations

from typing import Optional


def load_env(path: Optional[str] = None) -> bool:
    """Load a `.env` file if present. Returns True iff something was loaded.

    Resolution order:
      1. explicit ``path`` argument
      2. `.env` walking up from the current working directory

    `override=False` ensures the shell environment always wins.
    """
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:  # python-dotenv not installed — no-op
        return False

    resolved = path or find_dotenv(usecwd=True)
    if not resolved:
        return False
    return load_dotenv(resolved, override=False)
