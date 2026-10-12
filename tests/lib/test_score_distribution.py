import math

import pytest

from mavedb.lib.score_distribution import (
    SCORE_DISTRIBUTION_BIN_COUNT,
    SCORE_DISTRIBUTION_VERSION,
    summarize_scores,
    summarize_variant_data,
)


@pytest.mark.parametrize("scores", [[], [None], [None, math.nan, math.inf]])
def test_summarize_scores_without_numeric_scores_returns_none(scores):
    assert summarize_scores(scores) is None


def test_summarize_scores_shape():
    summary = summarize_scores([0.0, 1.0])

    assert summary is not None
    assert summary["version"] == SCORE_DISTRIBUTION_VERSION
    assert summary["min"] == 0.0
    assert summary["max"] == 1.0
    assert len(summary["counts"]) == SCORE_DISTRIBUTION_BIN_COUNT
    assert summary["null_count"] == 0


def test_summarize_scores_puts_min_in_first_bin_and_max_in_last_bin():
    summary = summarize_scores([-2.0, 0.0, 4.0])

    assert summary is not None
    assert summary["counts"][0] == 1
    assert summary["counts"][-1] == 1
    assert sum(summary["counts"]) == 3


def test_summarize_scores_bins_by_equal_width():
    # Width is 1.0 over [0, 24]; 11.5 falls in bin 11 and 12.0 starts bin 12.
    summary = summarize_scores([0.0, 11.5, 12.0, 24.0])

    assert summary is not None
    assert summary["counts"][11] == 1
    assert summary["counts"][12] == 1


def test_summarize_scores_single_repeated_value_lands_in_first_bin():
    summary = summarize_scores([3.5, 3.5, 3.5])

    assert summary is not None
    assert summary["min"] == summary["max"] == 3.5
    assert summary["counts"][0] == 3
    assert sum(summary["counts"]) == 3


def test_summarize_scores_counts_nulls_separately():
    summary = summarize_scores([1.0, None, 2.0, math.nan, 3.0])

    assert summary is not None
    assert summary["null_count"] == 2
    assert sum(summary["counts"]) == 3
    assert (summary["min"], summary["max"]) == (1.0, 3.0)


def test_summarize_variant_data_reads_canonical_score():
    variant_data = [
        {"score_data": {"score": 1.0}},
        {"score_data": {"score": "2.0"}},
        {"score_data": {"score": None}},
        {"score_data": {}},
        None,
    ]

    summary = summarize_variant_data(variant_data)

    assert summary is not None
    assert sum(summary["counts"]) == 2
    assert summary["null_count"] == 3
