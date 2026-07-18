# ruff: noqa: E402
"""Router tests for the variant annotation surface relocated onto the Allele substrate.

The VA-Spec statement endpoints (``/variants/{urn}/va/*``) and the VRS-identifier lookup
(``/variants/vrs/{identifier}``) moved off the legacy ``/mapped-variants`` router in #743; they build
from the ``MappingRecord`` / ``Allele`` substrate (seeded by ``seed_annotation_substrate``), never the
legacy ``MappedVariant``.
"""

import json
from datetime import datetime, timezone

import pytest

from tests.helpers.util.user import change_ownership

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from urllib.parse import quote_plus

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement

from sqlalchemy import select

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from tests.helpers.constants import (
    TEST_BIORXIV_IDENTIFIER,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_GA4GH_IDENTIFIER,
    TEST_PUBMED_IDENTIFIER,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
)
from tests.helpers.util.annotation import AlleleSpec, seed_mapping_record
from tests.helpers.util.common import deepcamelize
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_calibration import create_publish_and_promote_score_calibration
from tests.helpers.util.score_set import (
    create_seq_score_set_with_mapped_variants,
    create_seq_score_set_with_variants,
    seed_annotation_substrate,
)

_PUBLICATION_FETCH = [
    {"dbName": "PubMed", "identifier": f"{TEST_PUBMED_IDENTIFIER}"},
    {"dbName": "bioRxiv", "identifier": TEST_BIORXIV_IDENTIFIER},
]


# --- StudyResult -----------------------------------------------------------------------------------


def test_variant_study_result(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    response = client.get(f"/api/v1/variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant effect study result for {score_set['urn']}#1."
    ExperimentalVariantFunctionalImpactStudyResult.model_validate_json(json.dumps(response_data))


def test_variant_study_result_404_when_variant_missing(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    urn = f"{score_set['urn']}#404"
    response = client.get(f"/api/v1/variants/{quote_plus(urn)}/va/study-result")

    assert response.status_code == 404
    assert response.json()["detail"] == f"variant with URN '{urn}' not found"


def test_variant_study_result_404_when_no_mapping_data(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    # The variant exists but has no mapping record seeded on the new substrate.
    urn = f"{score_set['urn']}#1"
    response = client.get(f"/api/v1/variants/{quote_plus(urn)}/va/study-result")

    assert response.status_code == 404
    assert f"No study result exists for variant {urn}: no mapping data exists." in response.json()["detail"]


# --- Functional-impact Statement -------------------------------------------------------------------


@pytest.mark.parametrize("mock_publication_fetch", [_PUBLICATION_FETCH], indirect=["mock_publication_fetch"])
def test_variant_functional_impact_statement(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    seed_annotation_substrate(session, score_set)

    response = client.get(f"/api/v1/variants/{quote_plus(score_set['urn'] + '#1')}/va/functional-statement")
    response_data = response.json()

    assert response.status_code == 200
    assert response_data["description"] == f"Variant functional impact statement for {score_set['urn']}#1."
    Statement.model_validate_json(json.dumps(response_data))


def test_variant_functional_impact_statement_404_when_insufficient_evidence(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    # Mapped on the new substrate, but no (primary) calibration => insufficient evidence.
    seed_annotation_substrate(session, score_set)

    urn = f"{score_set['urn']}#1"
    response = client.get(f"/api/v1/variants/{quote_plus(urn)}/va/functional-statement")

    assert response.status_code == 404
    assert (
        f"No functional impact statement exists for variant {urn}. Variant does not have sufficient evidence"
        in response.json()["detail"]
    )


# --- Pathogenicity Statement -----------------------------------------------------------------------


@pytest.mark.parametrize("mock_publication_fetch", [_PUBLICATION_FETCH], indirect=["mock_publication_fetch"])
def test_variant_pathogenicity_statement(
    client, session, data_provider, data_files, setup_router_db, mock_publication_fetch
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    create_publish_and_promote_score_calibration(
        client, score_set["urn"], deepcamelize(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    )
    seed_annotation_substrate(session, score_set)

    response = client.get(f"/api/v1/variants/{quote_plus(score_set['urn'] + '#2')}/va/pathogenicity-statement")
    response_data = response.json()

    assert response.status_code == 200
    assert f"Variant pathogenicity statement for {score_set['urn']}#2" in response_data["description"]
    VariantPathogenicityStatement.model_validate_json(json.dumps(response_data))


def test_variant_pathogenicity_statement_404_when_insufficient_evidence(
    client, session, data_provider, data_files, setup_router_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    urn = f"{score_set['urn']}#1"
    response = client.get(f"/api/v1/variants/{quote_plus(urn)}/va/pathogenicity-statement")

    assert response.status_code == 404
    assert (
        f"No pathogenicity statement exists for variant {urn}; Variant does not have sufficient evidence"
        in response.json()["detail"]
    )


# --- VRS-identifier lookup -------------------------------------------------------------------------


def test_lookup_variants_by_vrs_identifier(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == f"{score_set['urn']}#1"))
    # vrs_digest stores the full GA4GH VRS id in production (the mapper writes post_mapped["id"] into it),
    # so the lookup matches the identifier against vrs_digest. The allele's ClinGen id rides alongside the URN.
    seed_mapping_record(
        session,
        variant,
        assay_level="cdna",
        alleles=[
            AlleleSpec(
                digest=TEST_GA4GH_IDENTIFIER,
                level="cdna",
                is_authoritative=True,
                clingen_allele_id="CA1",
                post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
            )
        ],
    )

    response = client.get(f"/api/v1/variants/vrs/{quote_plus(TEST_GA4GH_IDENTIFIER)}")
    response_data = response.json()

    assert response.status_code == 200
    assert len(response_data) == 1
    assert response_data[0]["variantUrn"] == f"{score_set['urn']}#1"
    assert response_data[0]["clingenAlleleId"] == "CA1"
    assert response_data[0]["vrsId"] == TEST_GA4GH_IDENTIFIER


def test_lookup_variants_by_vrs_identifier_empty_when_none_match(
    client, session, data_provider, data_files, setup_router_db
):
    """The lookup is collection-shaped: no match is an empty list, not a 404."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    # A well-formed GA4GH id (32-char digest) that no seeded allele carries.
    response = client.get(f"/api/v1/variants/vrs/{quote_plus('ga4gh:VA.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')}")

    assert response.status_code == 200
    assert response.json() == []


def test_lookup_variants_by_vrs_identifier_empty_without_read_permission(
    client, session, data_provider, data_files, setup_router_db
):
    """Existence-hiding: an identifier that matches only a private score set is filtered to an empty list,
    indistinguishable from a genuine no-match — so the response never reveals the private allele exists."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == f"{score_set['urn']}#1"))
    # The identifier matches a real allele (by vrs_digest); the permission filter drops it to an empty list.
    seed_mapping_record(
        session,
        variant,
        assay_level="cdna",
        alleles=[AlleleSpec(digest=TEST_GA4GH_IDENTIFIER, level="cdna", is_authoritative=True)],
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    response = client.get(f"/api/v1/variants/vrs/{quote_plus(TEST_GA4GH_IDENTIFIER)}")

    assert response.status_code == 200
    assert response.json() == []


# --- as_of temporal behavior ------------------------------------------------------------------------


@pytest.mark.parametrize("va_endpoint", ["study-result", "functional-statement", "pathogenicity-statement"])
def test_va_endpoint_404_when_no_mapping_data_at_as_of(
    client, session, data_provider, data_files, setup_router_db, va_endpoint
):
    """A past instant with no live mapping yields the single-resource 404 — the statement did not exist then.

    ``as_of`` is threaded into ``variant_annotation_context``, so a far-past instant sees no live mapping
    record and the derived VA statement cannot be constructed. This is the deliberate contrast with the
    score-set *collection* endpoints: an empty collection is 200 + empty, but a single derived resource
    that does not exist at the requested instant is a 404.
    """
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    urn = f"{score_set['urn']}#1"
    past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    response = client.get(f"/api/v1/variants/{quote_plus(urn)}/va/{va_endpoint}", params={"as_of": past.isoformat()})

    assert response.status_code == 404
    assert "no mapping data exists" in response.json()["detail"]


def test_variant_study_result_honors_as_of(client, session, data_provider, data_files, setup_router_db):
    """The study-result route reconstructs its subject at ``as_of`` and echoes the instant on ``X-As-Of``.

    Study result needs only live mapping (no calibration), so it cleanly exercises the positive path: a
    far-future instant still sees the freshly-seeded substrate as live and returns 200. Omitting ``as_of``
    reports ``current``.
    """
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_annotation_substrate(session, score_set)

    url = f"/api/v1/variants/{quote_plus(score_set['urn'] + '#1')}/va/study-result"

    # No as_of: the header reports the current standing.
    current_response = client.get(url)
    assert current_response.status_code == 200
    assert current_response.headers["X-As-Of"] == "current"

    # Far future: the seeded mapping is live, so the subject reconstructs and the instant echoes back.
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    future_response = client.get(url, params={"as_of": future.isoformat()})
    assert future_response.status_code == 200
    assert datetime.fromisoformat(future_response.headers["X-As-Of"]) == future


def test_lookup_variants_by_vrs_identifier_honors_as_of(client, session, data_provider, data_files, setup_router_db):
    """The VRS lookup filters its matches by ``as_of`` (the allele link must be live at that instant)."""
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == f"{score_set['urn']}#1"))
    seed_mapping_record(
        session,
        variant,
        assay_level="cdna",
        alleles=[
            AlleleSpec(
                digest=TEST_GA4GH_IDENTIFIER,
                level="cdna",
                is_authoritative=True,
                clingen_allele_id="CA1",
                post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
            )
        ],
    )
    url = f"/api/v1/variants/vrs/{quote_plus(TEST_GA4GH_IDENTIFIER)}"

    # Far future: the allele link is live, so the identifier resolves and the instant echoes back.
    future = datetime(2999, 1, 1, tzinfo=timezone.utc)
    future_response = client.get(url, params={"as_of": future.isoformat()})
    assert future_response.status_code == 200
    assert len(future_response.json()) == 1
    assert datetime.fromisoformat(future_response.headers["X-As-Of"]) == future

    # Far past: nothing was live yet, so no allele matches — an empty collection, not a 404.
    past = datetime(2000, 1, 1, tzinfo=timezone.utc)
    past_response = client.get(url, params={"as_of": past.isoformat()})
    assert past_response.status_code == 200
    assert past_response.json() == []
