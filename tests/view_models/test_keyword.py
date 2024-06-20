from fastapi import HTTPException
from mavedb.view_models.experiment_controlled_keyword import ExperimentControlledKeywordCreate
from tests.helpers.constants import TEST_DESCRIPTION

import pytest


def test_create_keyword_with_description():
    # Test valid keyword with description
    keyword = {
        "key": "Variant Library Creation Method",
        "value": "Endogenous locus library method",
        "special": False,
        "description": TEST_DESCRIPTION
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=TEST_DESCRIPTION)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.value == "Endogenous locus library method"


def test_create_keyword_without_description():
    # Test valid keyword without description
    keyword = {
        "key": "Variant Library Creation Method",
        "value": "Endogenous locus library method",
        "special": False,
        "description": TEST_DESCRIPTION
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=None)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.value == "Endogenous locus library method"


def test_create_keyword_value_is_other():
    # Keyword must have description if its value is Other.
    keyword = {
        "key": "Variant Library Creation Method",
        "value": "Other",
        "special": False,
        "description": TEST_DESCRIPTION
    }
    keyword_obj = ExperimentControlledKeywordCreate(keyword=keyword, description=TEST_DESCRIPTION)
    assert keyword_obj.keyword.key == "Variant Library Creation Method"
    assert keyword_obj.keyword.value == "Other"


def test_create_keyword_value_is_other_without_description():
    # Keyword must have description if its value is Other.
    keyword = {
        "key": "Variant Library Creation Method",
        "value": "Other",
        "special": False,
        "description": TEST_DESCRIPTION
    }
    with pytest.raises(HTTPException) as exc_info:
        ExperimentControlledKeywordCreate(keyword=keyword, description=None)
    assert "Other option does not allow empty description." in str(exc_info.value.detail)