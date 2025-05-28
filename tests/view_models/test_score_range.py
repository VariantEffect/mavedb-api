import pytest
from pydantic import ValidationError

from mavedb.view_models.score_range import ScoreRangeModify, ScoreRangeCreate, ScoreSetRangesCreate

from tests.helpers.constants import (
    TEST_SCORE_SET_NORMAL_RANGE_WITH_ODDS_PATH,
    TEST_SCORE_SET_ABNORMAL_RANGE_WITH_ODDS_PATH,
    TEST_SCORE_SET_RANGE,
    TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE,
)


def test_score_range_modify_valid_range():
    valid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [0.0, 1.0],
    }
    score_range = ScoreRangeModify(**valid_data)
    assert score_range.range == [0.0, 1.0]


def test_score_range_modify_invalid_range_length():
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [0.0],
    }
    with pytest.raises(ValidationError, match="Only a lower and upper bound are allowed."):
        ScoreRangeModify(**invalid_data)


def test_score_range_modify_invalid_range_order():
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [1.0, 0.0],
    }
    with pytest.raises(
        ValidationError, match="The lower bound of the score range may not be larger than the upper bound."
    ):
        ScoreRangeModify(**invalid_data)


def test_score_range_modify_equal_bounds():
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [1.0, 1.0],
    }
    with pytest.raises(ValidationError, match="The lower and upper bound of the score range may not be the same."):
        ScoreRangeModify(**invalid_data)


@pytest.mark.parametrize(
    "valid_data", [TEST_SCORE_SET_NORMAL_RANGE_WITH_ODDS_PATH, TEST_SCORE_SET_ABNORMAL_RANGE_WITH_ODDS_PATH]
)
def test_score_range_create_with_odds_path(valid_data):
    score_range = ScoreRangeCreate(**valid_data)
    assert score_range.odds_path.ratio == valid_data["odds_path"]["ratio"]
    assert score_range.odds_path.evidence == valid_data["odds_path"]["evidence"]


def test_score_ranges_create_valid():
    score_ranges = ScoreSetRangesCreate(**TEST_SCORE_SET_RANGE)
    assert len(score_ranges.ranges) == 2
    assert score_ranges.ranges[0].label == TEST_SCORE_SET_RANGE["ranges"][0]["label"]
    assert score_ranges.ranges[1].classification == TEST_SCORE_SET_RANGE["ranges"][1]["classification"]


def test_score_ranges_create_valid_with_odds_path_source():
    score_ranges = ScoreSetRangesCreate(**TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE)
    assert len(score_ranges.ranges) == 2
    assert score_ranges.ranges[0].label == TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE["ranges"][0]["label"]
    assert (
        score_ranges.ranges[1].classification
        == TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE["ranges"][1]["classification"]
    )
    assert (
        score_ranges.odds_path_source[0].identifier
        == TEST_SCORE_SET_RANGE_WITH_ODDS_PATH_AND_SOURCE["odds_path_source"][0]["identifier"]
    )
    assert len(score_ranges.odds_path_source) == 1


def test_score_ranges_create_invalid_range():
    invalid_data = {
        "wt_score": 0.5,
        "ranges": [
            {
                "label": "Range 1",
                "classification": "normal",
                "range": [0.0, 1.0],
            },
            {
                "label": "Range 2",
                "classification": "abnormal",
                "range": [2.0, 1.0],
            },
        ],
    }
    with pytest.raises(
        ValidationError, match="The lower bound of the score range may not be larger than the upper bound."
    ):
        ScoreSetRangesCreate(**invalid_data)
