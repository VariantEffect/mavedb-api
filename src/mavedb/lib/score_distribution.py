"""Precomputed summaries of a score set's score distribution.

The variant page draws a small histogram of each measured score set beside the variant's score. Fetching
every variant to draw it is too slow for large score sets, so the summary is computed once when scores are
written and stored on the score set (``ScoreSet.score_distribution``).
"""

import math
from typing import Any, Iterable, Optional

from mavedb.lib.variants import score_from_variant_data

SCORE_DISTRIBUTION_VERSION = 1
"""Bump when the binning changes, then re-run the backfill with ``--force``."""

SCORE_DISTRIBUTION_BIN_COUNT = 24


def summarize_scores(scores: Iterable[Optional[float]]) -> Optional[dict[str, Any]]:
    """Bin scores into equal-width bins spanning their full range.

    The full ``[min, max]`` is used rather than a clipped range: percentile clipping always hides some
    scores, and IQR fences drop a whole mode in bimodal genome-editing data.

    Args:
        scores: Scores to summarize. ``None`` and non-finite entries are counted in ``null_count`` and not binned.

    Returns:
        ``{"version", "min", "max", "counts", "null_count"}``, or ``None`` when there are no numeric scores.
        The maximum falls in the last bin. When every score is the same value, all of them fall in the
        first bin.
    """
    values: list[float] = []
    null_count = 0
    for score in scores:
        if score is None or not math.isfinite(score):
            null_count += 1
        else:
            values.append(score)

    if not values:
        return None

    low, high = min(values), max(values)
    counts = [0] * SCORE_DISTRIBUTION_BIN_COUNT
    width = (high - low) / SCORE_DISTRIBUTION_BIN_COUNT
    for value in values:
        index = 0 if width == 0 else min(int((value - low) / width), SCORE_DISTRIBUTION_BIN_COUNT - 1)
        counts[index] += 1

    return {
        "version": SCORE_DISTRIBUTION_VERSION,
        "min": low,
        "max": high,
        "counts": counts,
        "null_count": null_count,
    }


def summarize_variant_data(variant_data: Iterable[Optional[dict[str, Any]]]) -> Optional[dict[str, Any]]:
    """Summarize scores read from variants' ``data`` JSONB. See :func:`summarize_scores`."""
    return summarize_scores(score_from_variant_data(data) for data in variant_data)
