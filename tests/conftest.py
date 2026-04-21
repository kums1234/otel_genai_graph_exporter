"""Shared pytest fixtures."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def all_fixtures() -> dict[str, dict]:
    return {p.stem: _load(p) for p in sorted(FIXTURES_DIR.glob("*.json"))}
