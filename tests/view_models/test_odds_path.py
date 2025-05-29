import pytest
from pydantic import ValidationError

from mavedb.view_models.odds_path import OddsPathBase, OddsPathModify, OddsPathCreate

from tests.helpers.constants import TEST_BS3_ODDS_PATH, TEST_PS3_ODDS_PATH


@pytest.mark.parametrize("valid_data", [TEST_BS3_ODDS_PATH, TEST_PS3_ODDS_PATH])
def test_odds_path_base_valid_data(valid_data):
    odds_path = OddsPathBase(**valid_data)
    assert odds_path.ratio == valid_data["ratio"]
    assert odds_path.evidence == valid_data["evidence"]


def test_odds_path_base_no_evidence():
    odds_with_no_evidence = TEST_BS3_ODDS_PATH.copy()
    odds_with_no_evidence["evidence"] = None

    odds_path = OddsPathBase(**odds_with_no_evidence)
    assert odds_path.ratio == odds_with_no_evidence["ratio"]
    assert odds_path.evidence is None


@pytest.mark.parametrize("valid_data", [TEST_BS3_ODDS_PATH, TEST_PS3_ODDS_PATH])
def test_odds_path_base_invalid_data(valid_data):
    odds_path = OddsPathModify(**valid_data)
    assert odds_path.ratio == valid_data["ratio"]
    assert odds_path.evidence == valid_data["evidence"]


def test_odds_path_modify_invalid_ratio():
    invalid_data = {
        "ratio": -1.0,
        "evidence": "BS3_STRONG",
    }
    with pytest.raises(ValidationError, match="OddsPath value must be greater than or equal to 0"):
        OddsPathModify(**invalid_data)


@pytest.mark.parametrize("valid_data", [TEST_BS3_ODDS_PATH, TEST_PS3_ODDS_PATH])
def test_odds_path_create_valid(valid_data):
    odds_path = OddsPathCreate(**valid_data)
    assert odds_path.ratio == valid_data["ratio"]
    assert odds_path.evidence == valid_data["evidence"]
