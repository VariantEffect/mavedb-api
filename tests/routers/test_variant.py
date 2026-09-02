# ruff: noqa: E402
"""Router tests for the assayed variant-detail endpoint (``GET /variants/{urn}``) and the
single-variant CSV export (``GET /variants/{urn}/csv``).

Exercise the HTTP surface end to end: the two-tier envelope serializes and validates, the ``as_of``
content-time is echoed in the ``X-As-Of`` header, a superseded variant self-describes rather than
reading as current, the READ gate holds for private score sets, and a private calibration is never
served over either surface.
"""

import csv
from datetime import datetime, timezone
from io import StringIO
from unittest.mock import patch
from urllib.parse import quote

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from sqlalchemy import select

from mavedb.models.allele import Allele
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.view_models.variant_detail import VariantDetail
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    create_seq_score_set_with_variants,
    link_clinical_controls_to_alleles,
    publish_score_set,
    seed_csv_substrate,
)
from tests.helpers.util.user import change_ownership

_VALID_DIGEST = "0123456789abcdefghijABCDEFGHIJ_-"


def _post_mapped() -> dict:
    """A spec-valid post_mapped VRS Allele — the Cat-VRS builder hydrates this on the detail path."""
    return {
        "id": f"ga4gh:VA.{_VALID_DIGEST}",
        "type": "Allele",
        "state": {"type": "LiteralSequenceExpression", "sequence": "F"},
        "digest": _VALID_DIGEST,
        "location": {
            "id": f"ga4gh:SL.{_VALID_DIGEST}",
            "end": 6,
            "type": "SequenceLocation",
            "start": 5,
            "digest": _VALID_DIGEST,
            "sequenceReference": {
                "type": "SequenceReference",
                "label": "NP_000000.0",
                "refgetAccession": "SQ.0123456789abcdefghijABCDEFGHIJ_-",
            },
        },
    }


def _seed_mapping(session, variant_urn):
    """Give a variant a live coding-measured mapping record whose authoritative allele carries a
    digest + ClinGen id + a live VEP consequence, its genomic projection sibling (shared
    projection_group), a protein apex, and a synonymous cousin (a nt encoder of the same consequence in
    a different projection group) — enough to build Cat-VRS and exercise the projection + convergent axes."""
    variant = session.scalar(select(VariantDbModel).where(VariantDbModel.urn == variant_urn))
    record = MappingRecord(
        variant_id=variant.id,
        assay_level="cdna",
        hgvs_assay_level="NM_000546.6:c.1216G>A",
        mapping_api_version="test.0.0",
    )
    session.add(record)
    session.commit()

    measured = Allele(
        vrs_digest="cdna-digest",
        level="cdna",
        post_mapped=_post_mapped(),
        clingen_allele_id="CA123",
        hgvs_c="NM_000546.6:c.1216G>A",
    )
    genomic = Allele(
        vrs_digest="gen-digest", level="genomic", post_mapped=_post_mapped(), hgvs_g="NC_000017.11:g.7676154C>T"
    )
    protein = Allele(
        vrs_digest="prot-digest", level="protein", post_mapped=_post_mapped(), hgvs_p="NP_000537.3:p.Ala406Thr"
    )
    cousin = Allele(
        vrs_digest="cousin-digest", level="cdna", post_mapped=_post_mapped(), hgvs_c="NM_000546.6:c.1218C>T"
    )
    session.add_all([measured, genomic, protein, cousin])
    session.commit()

    session.add_all(
        [
            # The measured cdna link and its genomic projection share a projection_group; the protein
            # apex is in no pair (group None); the cousin sits in its own projection group.
            MappingRecordAllele(
                mapping_record_id=record.id, allele_id=measured.id, is_authoritative=True, projection_group=0
            ),
            MappingRecordAllele(
                mapping_record_id=record.id, allele_id=genomic.id, is_authoritative=False, projection_group=0
            ),
            MappingRecordAllele(mapping_record_id=record.id, allele_id=protein.id, is_authoritative=False),
            MappingRecordAllele(
                mapping_record_id=record.id, allele_id=cousin.id, is_authoritative=False, projection_group=1
            ),
            VepAlleleConsequence(
                allele_id=measured.id,
                functional_consequence="missense_variant",
                source_version="116",
                access_date="2026-01-01",
            ),
        ]
    )
    session.commit()


def test_get_variant_detail_envelope(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    urn = f"{score_set['urn']}#1"
    _seed_mapping(session, urn)

    response = client.get(f"/api/v1/variants/{urn.replace(chr(35), '%23')}")

    assert response.status_code == 200
    body = response.json()
    VariantDetail.model_validate(body)

    assert body["urn"] == urn
    assert body["assayLevel"] == "cdna"
    assert body["referenceHgvs"] == "NM_000546.6:c.1216G>A"
    assert body["assayLevelDigest"] == "cdna-digest"
    assert body["clingenAlleleId"] == "CA123"
    assert body["mode"] == "projection"
    # Spec-pure Cat-VRS carries its own field names (no camelization of the nested GA4GH object).
    assert body["molecularRepresentation"]["type"] == "CategoricalVariant"
    # State A: the full-closure Cat-VRS member set agrees with the sidecar (every linked allele is a
    # member, the cousin included), so the envelope is internally consistent.
    assert len(body["molecularRepresentation"]["members"]) == len(body["alleles"])
    # The alleles identity sidecar serializes camelCase, keyed by digest; the protein member is a
    # translation of the defining coding allele (which itself has no relation to itself).
    assert body["alleles"]["cdna-digest"]["level"] == "cdna"
    assert body["alleles"]["cdna-digest"]["hgvs"] == "NM_000546.6:c.1216G>A"
    assert body["alleles"]["cdna-digest"]["clingenAlleleId"] == "CA123"
    assert (
        body["alleles"]["cdna-digest"]["relation"] is None
    )  # measured allele: no relation to itself (null, not dropped)
    # The measured allele is the focus (isFocus); it carries no derivation (that axis describes the
    # *other* members relative to it), and pairs with its genomic projection sibling (projectionOf).
    assert body["alleles"]["cdna-digest"]["isFocus"] is True
    assert body["alleles"]["cdna-digest"]["derivation"] is None  # null, not dropped (exclude_none=False)
    assert body["alleles"]["cdna-digest"]["projectionOf"] == "gen-digest"
    assert body["alleles"]["gen-digest"]["isFocus"] is False
    assert body["alleles"]["gen-digest"]["relation"] == "coordinate_representation_of"
    assert body["alleles"]["gen-digest"]["derivation"] == "projection"
    assert body["alleles"]["gen-digest"]["projectionOf"] == "cdna-digest"
    assert body["alleles"]["prot-digest"]["level"] == "protein"
    assert body["alleles"]["prot-digest"]["hgvs"] == "NP_000537.3:p.Ala406Thr"
    assert body["alleles"]["prot-digest"]["relation"] == "translation_of"
    # The apex is a deterministic projection here but pairs with nothing (projectionOf is null).
    assert body["alleles"]["prot-digest"]["derivation"] == "projection"
    assert body["alleles"]["prot-digest"]["projectionOf"] is None
    # The synonymous cousin (different projection group) surfaces as a member wearing co_encodes and is
    # labelled `convergent` (a distinct change sharing the consequence, not an ambiguous candidate).
    assert body["alleles"]["cousin-digest"]["relation"] == "co_encodes"
    assert body["alleles"]["cousin-digest"]["derivation"] == "convergent"
    assert body["annotations"]["cdna-digest"]["vep"]["consequence"] == "missense_variant"
    assert body["isCurrent"] is True
    assert body["supersededByScoreSet"] is None  # null when current (stable envelope, not dropped)
    assert response.headers["X-As-Of"] == "current"


def test_get_variant_detail_unknown_urn_is_404(client, setup_router_db):
    response = client.get("/api/v1/variants/urn:mavedb:00000000-a-1#1")
    assert response.status_code == 404


def test_get_variant_detail_echoes_as_of_header(client, session, data_provider, data_files, setup_router_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    urn = f"{score_set['urn']}#1"

    historical = client.get(f"/api/v1/variants/{urn.replace(chr(35), '%23')}", params={"as_of": "2020-01-01T00:00:00Z"})

    assert historical.status_code == 200
    assert historical.headers["X-As-Of"] == "2020-01-01T00:00:00+00:00"
    # Before any mapping existed, the molecular layer is empty — the stable envelope carries it as null.
    assert historical.json()["molecularRepresentation"] is None


def test_superseded_variant_self_describes(client, session, data_provider, data_files, setup_router_db):
    """A variant on a superseded score set is still served, but flags its version standing."""
    experiment = create_experiment(client)
    older = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    newer = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )

    # Link newer as the version that replaces older (replaces_id / superseding relationship).
    older_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == older["urn"]))
    newer_db = session.scalar(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == newer["urn"]))
    newer_db.superseded_score_set_id = older_db.id
    session.commit()

    response = client.get(f"/api/v1/variants/{older['urn']}%231")

    assert response.status_code == 200
    body = response.json()
    assert body["isCurrent"] is False
    assert body["supersededByScoreSet"] == newer["urn"]


def test_get_variant_detail_anonymous_cannot_read_private(
    client, session, data_provider, data_files, setup_router_db, anonymous_app_overrides
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    change_ownership(session, score_set["urn"], ScoreSetDbModel)

    with DependencyOverrider(anonymous_app_overrides):
        response = client.get(f"/api/v1/variants/{score_set['urn']}%231")

    assert response.status_code == 404


def _first_variant_urn(session, score_set_urn):
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()
    return score_set.variants[0].urn


def _csv_path(variant_urn):
    """Variant URNs contain a '#', which must be percent-encoded or it is read as a URL fragment."""
    return f"/api/v1/variants/{quote(variant_urn, safe='')}/csv"


def _published_score_set_with_mappings(client, session, data_provider, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    seed_csv_substrate(session, score_set, assay_level="genomic", hgvs_g="NC_000010.11:g.1A>G")
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])
    return published


class TestGetVariantCsv:
    def test_returns_csv_attachment(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(_csv_path(variant_urn))

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert response.headers["content-disposition"] == f'attachment; filename="{variant_urn}.csv"'

    def test_reports_the_requested_variant(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(_csv_path(variant_urn))
        rows = list(csv.DictReader(StringIO(response.text)))

        assert len(rows) == 1
        assert rows[0]["accession"] == variant_urn
        assert rows[0]["score_set.score_set_urn"] == published["urn"]
        assert rows[0]["relationship.match_type"] == "exact"

    def test_namespaces_restrict_the_columns(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(f"{_csv_path(variant_urn)}?namespaces=scores")

        assert response.status_code == 200
        header = response.text.splitlines()[0]
        assert "scores.score" in header
        assert "gnomad.gnomad_af" not in header
        assert "relationship.match_type" not in header

    def test_clinvar_namespace_is_labeled_with_its_release(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        experiment = create_experiment(client)
        score_set = create_seq_score_set_with_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        link_clinical_controls_to_alleles(session, score_set)
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, score_set["urn"])
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(_csv_path(variant_urn))

        assert response.status_code == 200
        # The seeded ClinVar control is release 11_2024.
        assert "clinvar.2024_11.clinical_significance" in response.text.splitlines()[0]

    @pytest.mark.parametrize(
        "namespace",
        ["bogus", "clinvar", "clinvar.2024_13", "calibration", "calibration.not-a-urn"],
    )
    def test_invalid_namespace_is_rejected(
        self, session, data_provider, client, setup_router_db, data_files, namespace
    ):
        """FastAPI validates the namespace vocabulary from the parameter type, before the handler runs."""
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(f"{_csv_path(variant_urn)}?namespaces={namespace}")

        assert response.status_code == 422
        errors = response.json()["detail"]
        assert any(error["input"] == namespace for error in errors)

    @pytest.mark.parametrize("namespace", ["scores", "clinvar.2024_01"])
    def test_valid_namespace_is_accepted(self, session, data_provider, client, setup_router_db, data_files, namespace):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(f"{_csv_path(variant_urn)}?namespaces={namespace}")

        assert response.status_code == 200

    def test_namespace_vocabulary_is_published_to_openapi(self, client):
        """The generated schema is what the frontend builds its namespace selector from."""
        schema = client.app.openapi()["paths"]["/api/v1/variants/{urn}/csv"]["get"]
        namespaces_param = next(param for param in schema["parameters"] if param["name"] == "namespaces")

        item_schema = next(
            option["items"] for option in namespaces_param["schema"]["anyOf"] if option.get("type") == "array"
        )
        published = {value for option in item_schema["anyOf"] if "enum" in option for value in option["enum"]}
        patterns = [option["pattern"] for option in item_schema["anyOf"] if "pattern" in option]

        assert {
            "scores",
            "scores_custom",
            "counts",
            "mavedb",
            "vep",
            "gnomad",
            "clingen",
            "score_set",
            "relationship",
        } == published
        assert any("clinvar" in pattern for pattern in patterns)
        assert any("calibration" in pattern for pattern in patterns)

    def test_unknown_variant_returns_404(self, client, setup_router_db):
        response = client.get(_csv_path("urn:mavedb:00000000-a-1#1"))

        assert response.status_code == 404
        assert "not found" in response.json()["detail"]

    def test_private_score_set_is_not_readable_by_other_users(
        self, session, data_provider, client, setup_router_db, data_files, extra_user_app_overrides
    ):
        experiment = create_experiment(client)
        score_set = create_seq_score_set_with_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        variant_urn = _first_variant_urn(session, score_set["urn"])

        with DependencyOverrider(extra_user_app_overrides):
            response = client.get(_csv_path(variant_urn))

        assert response.status_code == 404


class TestPrivateCalibrationsAreNotServedOverHttp:
    """The lib tests cover the gating logic; this covers the wiring that reaches it.

    The predicate is hand-threaded through four signatures, so the endpoint is the contract worth pinning:
    a caller who may read the score set but not the calibration must get NA, even naming the URN outright.
    """

    def _private_calibration(self, session, score_set_urn):
        from mavedb.models.score_calibration import ScoreCalibration

        score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()
        calibration = ScoreCalibration(
            score_set_id=score_set.id,
            urn="urn:mavedb:calibration-99999999-9999-9999-9999-999999999999",
            title="Unpublished Calibration",
            baseline_score=0.0,
            research_use_only=False,
            primary=False,
            private=True,
            calibration_metadata={},
            created_by_id=score_set.created_by_id,
            modified_by_id=score_set.modified_by_id,
        )
        session.add(calibration)
        session.commit()
        return calibration.urn

    def test_another_user_naming_the_urn_gets_no_interpretation(
        self, session, data_provider, client, setup_router_db, data_files, extra_user_app_overrides
    ):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        calibration_urn = self._private_calibration(session, published["urn"])
        namespace = f"calibration.{calibration_urn}"

        with DependencyOverrider(extra_user_app_overrides):
            response = client.get(
                f"/api/v1/score-sets/{published['urn']}/variants/data?namespaces=scores&namespaces={quote(namespace)}"
            )

        assert response.status_code == 200
        rows = list(csv.DictReader(StringIO(response.text)))
        assert rows, "expected variant rows"
        assert all(row[f"{namespace}.title"] == "NA" for row in rows)

    def test_another_user_is_not_offered_it_by_discovery(
        self, session, data_provider, client, setup_router_db, data_files, extra_user_app_overrides
    ):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        calibration_urn = self._private_calibration(session, published["urn"])

        with DependencyOverrider(extra_user_app_overrides):
            response = client.get(f"/api/v1/score-sets/{published['urn']}/csv-namespaces")

        assert response.status_code == 200
        assert f"calibration.{calibration_urn}" not in [entry["namespace"] for entry in response.json()]


class TestVariantCsvAsOf:
    """`as_of` time-travels the single-variant CSV, as it does the score-set CSV.

    The library call has always supported it; the route did not expose it, so the whole reconstruction
    path was unreachable over HTTP and the response did not even self-describe its content-time.
    """

    def _seeded(self, client, session, data_provider, data_files, seeded_at):
        experiment = create_experiment(client)
        score_set = create_seq_score_set_with_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        seed_csv_substrate(
            session,
            score_set,
            assay_level="genomic",
            hgvs_g="NC_000010.11:g.1A>G",
            vep_consequence="missense_variant",
            valid_from=seeded_at,
        )
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            return publish_score_set(client, score_set["urn"])

    def test_csv_echoes_and_reconstructs_at_the_requested_instant(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        published = self._seeded(client, session, data_provider, data_files, datetime(2020, 1, 1, tzinfo=timezone.utc))
        variant_urn = _first_variant_urn(session, published["urn"])

        current = client.get(_csv_path(variant_urn), params={"namespaces": "vep"})
        assert current.status_code == 200
        assert current.headers["X-As-Of"] == "current"
        assert any(
            row["vep.vep_functional_consequence"] == "missense_variant"
            for row in csv.DictReader(StringIO(current.text))
        )

        # Before the mapping was live, the same columns reconstruct as NA.
        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        before = client.get(_csv_path(variant_urn), params={"namespaces": "vep", "as_of": past.isoformat()})
        assert before.status_code == 200
        assert before.headers["X-As-Of"] == past.isoformat()
        assert all(row["vep.vep_functional_consequence"] == "NA" for row in csv.DictReader(StringIO(before.text)))

    def test_namespace_discovery_answers_for_the_requested_instant(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        """Discovery and download must agree, or the picker offers columns the download returns NA."""
        published = self._seeded(client, session, data_provider, data_files, datetime(2020, 1, 1, tzinfo=timezone.utc))
        variant_urn = _first_variant_urn(session, published["urn"])
        url = f"/api/v1/variants/{quote(variant_urn, safe='')}/csv-namespaces"

        current = client.get(url)
        assert current.status_code == 200
        assert current.headers["X-As-Of"] == "current"
        assert "vep" in {entry["namespace"] for entry in current.json()}

        past = datetime(2000, 1, 1, tzinfo=timezone.utc)
        before = client.get(url, params={"as_of": past.isoformat()})
        assert before.status_code == 200
        assert before.headers["X-As-Of"] == past.isoformat()
        assert "vep" not in {entry["namespace"] for entry in before.json()}


class TestCsvNamespaceDiscovery:
    """The discovery endpoints advertise what a namespace picker should offer."""

    def test_score_set_namespaces_are_labeled_and_grouped(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)

        response = client.get(f"/api/v1/score-sets/{published['urn']}/csv-namespaces")

        assert response.status_code == 200
        entries = response.json()
        by_namespace = {entry["namespace"]: entry for entry in entries}
        assert {"scores", "score_set", "vep", "gnomad", "clingen"} <= set(by_namespace)
        assert "relationship" not in by_namespace
        assert by_namespace["gnomad"]["label"] == "gnomAD population frequency"
        assert by_namespace["gnomad"]["group"] == "annotation"
        # Every entry is renderable without the client inventing labels.
        assert all(entry["label"] and entry["group"] for entry in entries)

    def test_score_set_detail_response_is_unchanged(self, session, data_provider, client, setup_router_db, data_files):
        """Discovery is its own request, so it must not appear on the score-set page's critical path."""
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)

        response = client.get(f"/api/v1/score-sets/{published['urn']}")

        assert response.status_code == 200
        assert "availableCsvNamespaces" not in response.json()

    def test_variant_namespaces_are_labeled_and_grouped(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        published = _published_score_set_with_mappings(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(f"/api/v1/variants/{quote(variant_urn, safe='')}/csv-namespaces")

        assert response.status_code == 200
        by_namespace = {entry["namespace"]: entry for entry in response.json()}
        # The variant CSV does emit relationship columns, unlike the score-set CSV.
        assert "relationship" in by_namespace
        assert by_namespace["relationship"]["group"] == "provenance"

    def test_advertised_namespaces_are_accepted_by_the_csv_endpoints(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        """Discovery and validation must agree, or the picker offers options that 422."""
        experiment = create_experiment(client)
        score_set = create_seq_score_set_with_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        link_clinical_controls_to_alleles(session, score_set)
        with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
            published = publish_score_set(client, score_set["urn"])

        entries = client.get(f"/api/v1/score-sets/{published['urn']}/csv-namespaces").json()
        namespaces = [entry["namespace"] for entry in entries]
        assert "clinvar.2024_11" in namespaces

        query = "&".join(f"namespaces={quote(ns, safe='')}" for ns in namespaces)
        response = client.get(f"/api/v1/score-sets/{published['urn']}/variants/data?{query}")
        assert response.status_code == 200

        variant_urn = _first_variant_urn(session, published["urn"])
        variant_entries = client.get(f"/api/v1/variants/{quote(variant_urn, safe='')}/csv-namespaces").json()
        variant_query = "&".join(f"namespaces={quote(entry['namespace'], safe='')}" for entry in variant_entries)
        response = client.get(f"{_csv_path(variant_urn)}?{variant_query}")
        assert response.status_code == 200

    def test_unknown_score_set_returns_404(self, client, setup_router_db):
        response = client.get("/api/v1/score-sets/urn:mavedb:00000000-a-1/csv-namespaces")

        assert response.status_code == 404

    def test_unknown_variant_returns_404(self, client, setup_router_db):
        response = client.get(f"/api/v1/variants/{quote('urn:mavedb:00000000-a-1#1', safe='')}/csv-namespaces")

        assert response.status_code == 404
