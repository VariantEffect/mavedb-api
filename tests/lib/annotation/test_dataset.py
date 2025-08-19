from mavedb.lib.annotation.dataset import score_set_to_data_set
from ga4gh.core.models import iriReference
from ga4gh.va_spec.base import DataSet


def test_score_set_to_data_set(mock_score_set):
    data_set = score_set_to_data_set(mock_score_set)

    assert isinstance(data_set, DataSet)
    assert data_set.id == mock_score_set.urn
    assert data_set.description == mock_score_set.short_description
    assert data_set.license.name == mock_score_set.license.short_name
    assert data_set.releaseDate.strftime("%Y-%m-%d") == mock_score_set.published_date.strftime("%Y-%m-%d")
    assert data_set.reportedIn is not None
    assert isinstance(data_set.reportedIn, iriReference)


def test_score_set_to_data_set_no_published_date(mock_score_set):
    mock_score_set.published_date = None
    data_set = score_set_to_data_set(mock_score_set)

    assert data_set.releaseDate is None
