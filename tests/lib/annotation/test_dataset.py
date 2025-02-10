from mavedb.lib.annotation.dataset import score_set_to_data_set
from ga4gh.core.entity_models import DataSet


def test_score_set_to_data_set(mock_score_set):
    data_set = score_set_to_data_set(mock_score_set)

    assert isinstance(data_set, DataSet)
    assert data_set.id == mock_score_set.urn
    assert data_set.label == "Variant effect data set"
    assert data_set.license == mock_score_set.license.short_name
    assert data_set.releaseDate == mock_score_set.published_date.strftime("%Y-%m-%d")
    assert len(data_set.contributions) == 2
    assert len(data_set.reportedIn) > 0


def test_score_set_to_data_set_no_published_date(mock_score_set):
    mock_score_set.published_date = None
    data_set = score_set_to_data_set(mock_score_set)

    assert data_set.releaseDate is None
