import pytest

from mavedb.view_models.doi_identifier import DoiIdentifierCreate


# Test valid doi identifier
def test_create_doi_identifier():
    doi = "10.1000/182"
    doi_identifier = DoiIdentifierCreate(
        identifier=doi,
    )
    assert doi_identifier.identifier == doi


def test_invalid_doi_fails():
    invalid_doi = "10.1000182"
    with pytest.raises(ValueError) as exc_info:
        DoiIdentifierCreate(
            identifier=invalid_doi,
        )

    assert f"'{invalid_doi}' is not a valid DOI identifier" in str(exc_info.value)


@pytest.mark.parametrize(
    "doi, expected",
    [
        ("10.7554/eLife.58026", "10.7554/ELIFE.58026"),
        ("https://doi.org/10.7554/elife.58026", "10.7554/ELIFE.58026"),
    ],
    ids=["mixed-case", "url"],
)
def test_create_doi_identifier_mixed_case_is_upper_cased(doi, expected):
    doi_identifier = DoiIdentifierCreate(identifier=doi)

    assert doi_identifier.identifier == expected
