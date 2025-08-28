from mavedb.view_models.search import ExperimentsSearch, ScoreSetsSearch, TextSearch

from tests.helpers.constants import (
    TEST_POPULATED_EXPERIMENT_SEARCH,
    TEST_POPULATED_SCORE_SET_SEARCH,
    TEST_POPULATED_TEXT_SEARCH,
)


def test_populated_experiment_search():
    experiment_search = ExperimentsSearch(**TEST_POPULATED_EXPERIMENT_SEARCH)
    assert all(experiment_search.__getattribute__(k) == v for k, v in TEST_POPULATED_EXPERIMENT_SEARCH.items())


def test_populated_score_set_search():
    score_set_search = ScoreSetsSearch(**TEST_POPULATED_SCORE_SET_SEARCH)
    assert all(score_set_search.__getattribute__(k) == v for k, v in TEST_POPULATED_SCORE_SET_SEARCH.items())


def test_populated_text_search():
    text_search = TextSearch(**TEST_POPULATED_EXPERIMENT_SEARCH)
    assert all(text_search.__getattribute__(k) == v for k, v in TEST_POPULATED_TEXT_SEARCH.items())
