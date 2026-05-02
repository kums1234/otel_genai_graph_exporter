"""Lock-in invariant: ``otel_genai_graph.__version__`` must match the
distribution metadata installed for the package.

This test exists because v0.2.0 shipped with ``__version__ = "0.1.0"``
hard-coded in ``__init__.py`` while ``pyproject.toml`` had been bumped
to ``0.2.0``. The wheel was correct (``pip install`` reported 0.2.0)
but ``import otel_genai_graph; otel_genai_graph.__version__`` returned
the stale ``0.1.0``. v0.2.1 derives ``__version__`` from
``importlib.metadata`` so the two sources are guaranteed identical.

If this ever fails, the package and the metadata have drifted again —
look at ``src/otel_genai_graph/__init__.py`` for re-introduced
hard-coding, or at the build / publish pipeline for a wheel that was
built without the package metadata in step.
"""
from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

import otel_genai_graph


def test_version_matches_distribution_metadata() -> None:
    """``__version__`` MUST equal the installed distribution's version."""
    try:
        meta_version = version("otel-genai-graph")
    except PackageNotFoundError:
        # In an editable / un-installed checkout the package metadata
        # may be absent. In that case ``__init__.py`` falls back to
        # ``"0.0.0+unknown"``; both sides should agree on that fallback.
        assert otel_genai_graph.__version__ == "0.0.0+unknown", (
            "package metadata missing AND __version__ is not the "
            f"declared fallback (got {otel_genai_graph.__version__!r})"
        )
        return

    assert otel_genai_graph.__version__ == meta_version, (
        f"__version__ drift: import says {otel_genai_graph.__version__!r}, "
        f"importlib.metadata says {meta_version!r}. "
        "Did __init__.py re-introduce a hard-coded constant?"
    )


def test_version_is_a_string() -> None:
    """Cheap shape check — guards against ``None`` from a typo in the metadata loader."""
    assert isinstance(otel_genai_graph.__version__, str)
    assert otel_genai_graph.__version__  # non-empty
