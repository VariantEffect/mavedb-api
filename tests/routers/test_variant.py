# ruff: noqa: E402

import csv
from io import StringIO
from unittest.mock import patch
from urllib.parse import quote

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from sqlalchemy import select

from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util.experiment import create_experiment
from tests.helpers.util.score_set import (
    create_seq_score_set_with_mapped_variants,
    link_clinvar_control_to_mapped_variant,
    publish_score_set,
)


def _first_variant_urn(session, score_set_urn):
    score_set = session.scalars(select(ScoreSetDbModel).where(ScoreSetDbModel.urn == score_set_urn)).one()
    return score_set.variants[0].urn


def _csv_path(variant_urn):
    """Variant URNs contain a '#', which must be percent-encoded or it is read as a URL fragment."""
    return f"/api/v1/variants/{quote(variant_urn, safe='')}/csv"


def _published_score_set_with_mapped_variants(client, session, data_provider, data_files):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client, session, data_provider, experiment["urn"], data_files / "scores.csv"
    )
    with patch.object(arq.ArqRedis, "enqueue_job", return_value=None):
        published = publish_score_set(client, score_set["urn"])
    return published


class TestGetVariantCsv:
    def test_returns_csv_attachment(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(_csv_path(variant_urn))

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert response.headers["content-disposition"] == f'attachment; filename="{variant_urn}.csv"'

    def test_reports_the_requested_variant(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(_csv_path(variant_urn))
        rows = list(csv.DictReader(StringIO(response.text)))

        assert len(rows) == 1
        assert rows[0]["accession"] == variant_urn
        assert rows[0]["score_set.score_set_urn"] == published["urn"]
        assert rows[0]["relationship.match_type"] == "exact"

    def test_namespaces_restrict_the_columns(self, session, data_provider, client, setup_router_db, data_files):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
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
        score_set = create_seq_score_set_with_mapped_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        link_clinvar_control_to_mapped_variant(session, score_set)
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
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
        variant_urn = _first_variant_urn(session, published["urn"])

        response = client.get(f"{_csv_path(variant_urn)}?namespaces={namespace}")

        assert response.status_code == 422
        errors = response.json()["detail"]
        assert any(error["input"] == namespace for error in errors)

    @pytest.mark.parametrize("namespace", ["scores", "clinvar.2024_01"])
    def test_valid_namespace_is_accepted(self, session, data_provider, client, setup_router_db, data_files, namespace):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
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
        score_set = create_seq_score_set_with_mapped_variants(
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
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
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
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
        calibration_urn = self._private_calibration(session, published["urn"])

        with DependencyOverrider(extra_user_app_overrides):
            response = client.get(f"/api/v1/score-sets/{published['urn']}/csv-namespaces")

        assert response.status_code == 200
        assert f"calibration.{calibration_urn}" not in [entry["namespace"] for entry in response.json()]


class TestCsvNamespaceDiscovery:
    """The discovery endpoints advertise what a namespace picker should offer."""

    def test_score_set_namespaces_are_labeled_and_grouped(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)

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
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)

        response = client.get(f"/api/v1/score-sets/{published['urn']}")

        assert response.status_code == 200
        assert "availableCsvNamespaces" not in response.json()

    def test_variant_namespaces_are_labeled_and_grouped(
        self, session, data_provider, client, setup_router_db, data_files
    ):
        published = _published_score_set_with_mapped_variants(client, session, data_provider, data_files)
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
        score_set = create_seq_score_set_with_mapped_variants(
            client, session, data_provider, experiment["urn"], data_files / "scores.csv"
        )
        link_clinvar_control_to_mapped_variant(session, score_set)
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
