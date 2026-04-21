"""Tests for the cost lookup table."""
from __future__ import annotations

import math

from otel_genai_graph.cost import PRICING, compute_cost, lookup


def test_lookup_known_anthropic() -> None:
    rate = lookup("anthropic", "claude-sonnet-4-5")
    assert rate is not None
    assert rate.input_per_mtok == 3.0
    assert rate.output_per_mtok == 15.0


def test_lookup_known_openai() -> None:
    rate = lookup("openai", "gpt-4o-mini")
    assert rate is not None
    assert rate.input_per_mtok == 0.15
    assert rate.output_per_mtok == 0.60


def test_lookup_embedding_has_zero_output_cost() -> None:
    rate = lookup("openai", "text-embedding-3-small")
    assert rate is not None
    assert rate.output_per_mtok == 0.0


def test_lookup_trims_date_suffix() -> None:
    rate = lookup("anthropic", "claude-sonnet-4-5-20251001")
    assert rate is not None
    # should resolve to the un-dated base rate
    assert rate == PRICING[("anthropic", "claude-sonnet-4-5")]


def test_lookup_unknown_returns_none() -> None:
    assert lookup("ufo", "whatever") is None
    assert lookup("", "gpt-4o") is None
    assert lookup("openai", "") is None


def test_compute_cost_sonnet_one_million_each() -> None:
    # 1M input @ $3 + 1M output @ $15 = $18.00
    cost = compute_cost("anthropic", "claude-sonnet-4-5", 1_000_000, 1_000_000)
    assert cost is not None
    assert math.isclose(cost, 18.0, rel_tol=1e-9)


def test_compute_cost_opus_partial_tokens() -> None:
    # 10k in @ $15/M + 5k out @ $75/M = $0.525
    cost = compute_cost("anthropic", "claude-opus-4-7", 10_000, 5_000)
    assert cost is not None
    assert math.isclose(cost, 0.525, rel_tol=1e-9)


def test_compute_cost_zero_tokens() -> None:
    assert compute_cost("anthropic", "claude-opus-4-7", 0, 0) == 0.0


def test_compute_cost_unknown_model_is_none() -> None:
    assert compute_cost("ufo", "whatever", 1000, 1000) is None
