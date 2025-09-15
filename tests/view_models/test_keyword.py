import pytest

from mavedb.view_models.experiment_controlled_keyword import ExperimentControlledKeywordCreate
from tests.helpers.constants import TEST_DESCRIPTION


def test_create_keyword_with_description():
    # Test valid keyword with description
    keyword = {
        "key": "Variant Library Creation Method",
        "label": "Endogenous locus library method",
        "special": False,
        "description": TEST_DESCRIPTION,
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=TEST_DESCRIPTION)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.label == "Endogenous locus library method"


def test_create_keyword_without_description():
    # Test valid keyword without description
    keyword = {
        "key": "Variant Library Creation Method",
        "label": "Endogenous locus library method",
        "special": False,
        "description": TEST_DESCRIPTION,
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=None)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.label == "Endogenous locus library method"


def test_create_keyword_with_other_value():
    # Keyword must have description if its value is Other.
    keyword = {
        "key": "Variant Library Creation Method",
        "label": "Other",
        "special": False,
        "description": TEST_DESCRIPTION,
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=TEST_DESCRIPTION)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.label == "Other"


def test_cannot_create_keyword_without_description_if_value_is_other():
    # Keyword must have description if its value is Other.
    keyword = {
        "key": "Variant Library Creation Method",
        "label": "Other",
        "special": False,
        "description": TEST_DESCRIPTION,
    }
    with pytest.raises(ValueError) as exc_info:
        ExperimentControlledKeywordCreate(keyword=keyword, description=None)
    assert "Other option does not allow empty description." in str(exc_info.value)


# TODO#273: Add view model tests for required keyword values.
