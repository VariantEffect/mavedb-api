import pytest

from mavedb.view_models.experiment_controlled_keyword import ExperimentControlledKeywordCreate
from mavedb.view_models.keyword import KeywordCreate


# Test valid experiment controlled keyword
def test_create_experiment_controlled_keyword():
    new_keyword = KeywordCreate(key="test", value="keyword")

    experiment_controlled_keyword = ExperimentControlledKeywordCreate(
        keyword=new_keyword,
    )
    assert experiment_controlled_keyword.keyword == new_keyword


def test_keyword_with_other_value_and_none_description_fails():
    other_keyword = KeywordCreate(
        key="test",
        value="other",
    )

    with pytest.raises(ValueError) as exc_info:
        ExperimentControlledKeywordCreate(
            keyword=other_keyword,
        )

    assert "Other option does not allow empty description" in str(exc_info.value)


def test_keyword_with_other_value_and_description_is_created():
    other_keyword = KeywordCreate(
        key="test",
        value="other",
    )
    description = "keyword is other."

    experiment_controlled_keyword = ExperimentControlledKeywordCreate(keyword=other_keyword, description=description)
    assert experiment_controlled_keyword.keyword == other_keyword
    assert experiment_controlled_keyword.description == description
