"""Cost lookup table for common GenAI models.

Rates are USD per 1M tokens. Embedding models have zero output cost.
The table is indicative — plug in your real rates for production.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PriceRate:
    input_per_mtok: float
    output_per_mtok: float


PRICING: dict[tuple[str, str], PriceRate] = {
    # Anthropic
    ("anthropic", "claude-opus-4"):       PriceRate(15.00, 75.00),
    ("anthropic", "claude-opus-4-1"):     PriceRate(15.00, 75.00),
    ("anthropic", "claude-opus-4-7"):     PriceRate(15.00, 75.00),
    ("anthropic", "claude-sonnet-4"):     PriceRate(3.00,  15.00),
    ("anthropic", "claude-sonnet-4-5"):   PriceRate(3.00,  15.00),
    ("anthropic", "claude-sonnet-4-6"):   PriceRate(3.00,  15.00),
    ("anthropic", "claude-haiku-4"):      PriceRate(1.00,   5.00),
    ("anthropic", "claude-haiku-4-5"):    PriceRate(1.00,   5.00),

    # OpenAI
    ("openai", "gpt-4o"):                 PriceRate(2.50,  10.00),
    ("openai", "gpt-4o-mini"):            PriceRate(0.15,   0.60),
    ("openai", "gpt-4.1"):                PriceRate(2.00,   8.00),
    ("openai", "gpt-4.1-mini"):           PriceRate(0.40,   1.60),
    ("openai", "o1"):                     PriceRate(15.00, 60.00),
    ("openai", "o3-mini"):                PriceRate(1.10,   4.40),
    ("openai", "text-embedding-3-small"): PriceRate(0.02,   0.00),
    ("openai", "text-embedding-3-large"): PriceRate(0.13,   0.00),

    # Google
    ("google", "gemini-2.5-pro"):         PriceRate(1.25,  10.00),
    ("google", "gemini-2.5-flash"):       PriceRate(0.15,   0.60),
    ("google", "gemini-2.0-flash"):       PriceRate(0.10,   0.40),
}


def lookup(provider: str, model: str) -> Optional[PriceRate]:
    """Resolve a (provider, model) → rate. Tolerates date-stamped model ids."""
    if not provider or not model:
        return None
    key = (provider, model)
    if key in PRICING:
        return PRICING[key]
    # strip trailing YYYYMMDD-style suffixes, e.g. "claude-sonnet-4-5-20251001"
    if "-20" in model:
        trimmed = model.rsplit("-20", 1)[0]
        if (provider, trimmed) in PRICING:
            return PRICING[(provider, trimmed)]
    return None


def compute_cost(
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> Optional[float]:
    """Return USD cost for a single LLM call, or None if rates are unknown."""
    rate = lookup(provider, model)
    if rate is None:
        return None
    return (
        input_tokens * rate.input_per_mtok
        + output_tokens * rate.output_per_mtok
    ) / 1_000_000
