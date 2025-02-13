from unittest.mock import Mock

from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from mavedb.lib.annotation.method import (
    publication_as_iri,
    publication_identifier_to_method,
    publication_identifiers_to_method,
    mavedb_api_releases_as_iri,
    mavedb_api_as_method,
    mavedb_vrs_releases_as_iri,
    mavedb_vrs_as_method,
    pillar_project_calibrations_as_iri,
    pillar_project_calibration_method,
    variant_interpretation_functional_guideline_as_iri,
    variant_interpretation_functional_guideline_method,
    variant_interpretation_clinical_guideline_as_iri,
    variant_interpretation_clinical_guideline_method,
)

MAVEDB_API_RELEASES_URL = "https://github.com/VariantEffect/mavedb-api/releases"
MAVEDB_MAPPER_RELEASES_URL = "https://github.com/VariantEffect/dcd_mapping2/releases"
MAVEDB_CALIBRATION_URL = "https://github.com/Dzeiberg/mave_calibration"
FUNCITONAL_GUIDELINES_URL = "https://pubmed.ncbi.nlm.nih.gov/29785012/"
CLINICAL_GUIDELINES_URL = "https://pubmed.ncbi.nlm.nih.gov/29785012/"


def test_publication_as_iri(mock_publication):
    assert publication_as_iri(mock_publication) == mock_publication.url


def test_publication_as_iri_no_url(mock_publication):
    mock_publication.url = None
    assert publication_as_iri(mock_publication) is None


def test_publication_identifier_to_method(mock_publication):
    method = publication_identifier_to_method(mock_publication, subtype="Test subtype")
    assert method.label == "Test subtype"
    assert len(method.reportedIn) > 0
    assert mock_publication.url in [iri.root for iri in method.reportedIn]


def test_publication_identifier_to_method_no_url(mock_publication):
    mock_publication.url = None
    method = publication_identifier_to_method(mock_publication, subtype="Test subtype")
    assert method.label == "Test subtype"
    assert method.reportedIn is None


def test_publication_identifiers_to_method(mock_publication):
    mock_publication.primary = True

    association = Mock(spec=ScoreSetPublicationIdentifierAssociation)
    association.publication = mock_publication

    method = publication_identifiers_to_method([association])
    assert method.label == "Experimental protocol"
    assert len(method.reportedIn) > 0
    assert mock_publication.url in [iri.root for iri in method.reportedIn]


def test_empty_publication_identifiers_to_method():
    method = publication_identifiers_to_method([])
    assert method is None


def test_nonexistent_primary_publication_identifiers_to_method(mock_publication):
    association = Mock(spec=ScoreSetPublicationIdentifierAssociation)
    association.publication = mock_publication
    association.primary = False

    method = publication_identifiers_to_method([association])
    assert method is None


def test_mavedb_api_releases_as_iri():
    assert mavedb_api_releases_as_iri() == MAVEDB_API_RELEASES_URL


def test_mavedb_api_as_method():
    method = mavedb_api_as_method()
    assert method.label == "Software version"
    assert len(method.reportedIn) > 0
    assert MAVEDB_API_RELEASES_URL in [iri.root for iri in method.reportedIn]


def test_mavedb_vrs_releases_as_iri():
    assert mavedb_vrs_releases_as_iri() == MAVEDB_MAPPER_RELEASES_URL


def test_mavedb_vrs_as_method():
    method = mavedb_vrs_as_method()
    assert method.label == "Software version"
    assert len(method.reportedIn) > 0
    assert MAVEDB_MAPPER_RELEASES_URL in [iri.root for iri in method.reportedIn]


def test_pillar_project_calibrations_as_iri():
    assert pillar_project_calibrations_as_iri() == MAVEDB_CALIBRATION_URL


def test_pillar_project_calibration_method():
    method = pillar_project_calibration_method()
    assert method.label == "Software version"
    assert len(method.reportedIn) > 0
    assert MAVEDB_CALIBRATION_URL in [iri.root for iri in method.reportedIn]


def test_variant_interpretation_functional_guideline_as_iri():
    assert variant_interpretation_functional_guideline_as_iri() == FUNCITONAL_GUIDELINES_URL


def test_variant_interpretation_functional_guideline_method():
    method = variant_interpretation_functional_guideline_method()
    assert method.label == "Variant interpretation guideline"
    assert len(method.reportedIn) > 0
    assert FUNCITONAL_GUIDELINES_URL in [iri.root for iri in method.reportedIn]


def test_variant_interpretation_clinical_guideline_as_iri():
    assert variant_interpretation_clinical_guideline_as_iri() == CLINICAL_GUIDELINES_URL


def test_variant_interpretation_clinical_guideline_method():
    method = variant_interpretation_clinical_guideline_method()
    assert method.label == "Variant interpretation guideline"
    assert len(method.reportedIn) > 0
    assert CLINICAL_GUIDELINES_URL in [iri.root for iri in method.reportedIn]
