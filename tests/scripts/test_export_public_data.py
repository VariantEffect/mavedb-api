# ruff: noqa: E402

"""Regression protection for the public data dump.

The archive is a published contract: `src/mavedb/scripts/resources/README.md` documents which files appear
for which score sets, what each one carries, and how they join. These tests hold the export to that
README, since a consumer reading a missing file or a missing column has no other recourse.
"""

import csv
import io
import json
from unittest.mock import Mock
from zipfile import ZipFile

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from mavedb.lib.csv.namespaces import CsvNamespace, calibration_namespace_for_urn
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.variant import Variant
from mavedb.scripts.export_public_data import (
    SCORE_EXPORT_NAMESPACES,
    annotation_export_namespaces,
    annotations_csv,
    archive_path_base,
    counts_csv,
    mapped_variants_json,
    public_dump_metadata,
    public_experiment_set,
    published_experiment_sets,
    score_set_artifacts,
    score_set_has_current_mappings,
    scores_csv,
    va_ndjson,
    write_public_dump,
)
from mavedb.view_models.experiment_set import ExperimentSetPublicDump


def _parse_csv(csv_text):
    return list(csv.DictReader(io.StringIO(csv_text)))


def _header(csv_text):
    return next(csv.reader(io.StringIO(csv_text)))


def _mapped_variants_of(session, score_set):
    return (
        session.query(MappedVariant)
        .join(Variant, Variant.id == MappedVariant.variant_id)
        .filter(Variant.score_set_id == score_set.id)
        .all()
    )


def _artifacts(session, score_set, principal):
    """`score_set_artifacts` collected into a dict, for tests that assert over the whole set.

    The production caller writes and releases each pair as it arrives; these fixtures are small enough
    that holding them is free.
    """
    return dict(score_set_artifacts(session, score_set, principal))


def _written_archive(session, principal):
    """The dump written into an in-memory archive, returned open for reading."""
    buffer = io.BytesIO()
    with ZipFile(buffer, "w") as archive:
        write_public_dump(session, principal, archive)
    return ZipFile(buffer)


####################################################################################################
# Archive naming
####################################################################################################


@pytest.mark.unit
class TestArchivePathBase:
    def test_colons_become_hyphens(self):
        """The README documents this substitution as the way back from a filename to a URN."""
        assert archive_path_base("urn:mavedb:00000001-a-1") == "urn-mavedb-00000001-a-1"

    def test_a_urn_without_colons_is_unchanged(self):
        assert archive_path_base("urn-mavedb-00000001-a-1") == "urn-mavedb-00000001-a-1"


####################################################################################################
# scores.csv
####################################################################################################


@pytest.mark.integration
class TestScoresCsv:
    def test_carries_every_investigator_score_column(self, session, make_dump_score_set):
        """The dump has always carried the investigator's own score columns, not just `score`.

        Naming `scores` alone would now yield only the required column, which is the regression this
        guards: README `csv/{urn}.scores.csv` documents `scores.*` as present.
        """
        score_set = make_dump_score_set(variant_scores=({"score": 1.0, "se": 0.25, "sd": 0.5},))

        header = _header(scores_csv(session, score_set))

        assert "scores.score" in header
        assert "scores.se" in header
        assert "scores.sd" in header

    def test_score_export_namespaces_names_both_score_groups(self):
        assert set(SCORE_EXPORT_NAMESPACES) == {CsvNamespace.SCORES, CsvNamespace.SCORES_CUSTOM}

    def test_carries_the_core_identity_columns(self, session, make_dump_score_set):
        score_set = make_dump_score_set()

        header = _header(scores_csv(session, score_set))

        assert header[0] == "accession"
        assert "hgvs_nt" in header

    def test_emits_one_row_per_variant(self, session, make_dump_score_set):
        score_set = make_dump_score_set(variant_scores=({"score": 1.0}, {"score": 2.0}, {"score": 3.0}))

        rows = _parse_csv(scores_csv(session, score_set))

        assert len(rows) == 3
        assert [row["scores.score"] for row in rows] == ["1.0", "2.0", "3.0"]

    def test_carries_no_count_columns(self, session, make_dump_score_set):
        """Counts get their own file; repeating them here would double the archive's largest artifact."""
        score_set = make_dump_score_set(count_columns=("c_0",))

        header = _header(scores_csv(session, score_set))

        assert not any(column.startswith("counts.") for column in header)


####################################################################################################
# counts.csv
####################################################################################################


@pytest.mark.integration
class TestCountsCsv:
    def test_absent_when_no_count_columns_are_defined(self, session, make_dump_score_set):
        assert counts_csv(session, make_dump_score_set(count_columns=())) is None

    def test_present_when_count_columns_are_defined(self, session, make_dump_score_set):
        score_set = make_dump_score_set(count_columns=("c_0", "c_1"))

        header = _header(counts_csv(session, score_set))

        assert "counts.c_0" in header
        assert "counts.c_1" in header

    def test_a_score_set_with_no_count_columns_key_does_not_raise(self, session, make_dump_score_set):
        """`dataset_columns` is investigator-shaped data; a missing key must not abort the whole dump."""
        score_set = make_dump_score_set()
        score_set.dataset_columns = {"score_columns": ["score"]}
        session.add(score_set)
        session.commit()

        assert counts_csv(session, score_set) is None


####################################################################################################
# Mapping gate
####################################################################################################


@pytest.mark.integration
class TestScoreSetHasCurrentMappings:
    def test_true_when_a_current_mapping_exists(self, session, make_dump_score_set):
        assert score_set_has_current_mappings(session, make_dump_score_set(mapped=True, current=True))

    def test_false_when_every_mapping_is_superseded(self, session, make_dump_score_set):
        """A fully remapped-away score set yields no annotations, so it gets no annotation files."""
        assert not score_set_has_current_mappings(session, make_dump_score_set(mapped=True, current=False))

    def test_false_when_nothing_is_mapped(self, session, make_dump_score_set):
        assert not score_set_has_current_mappings(session, make_dump_score_set(mapped=False))


####################################################################################################
# Archive composition
####################################################################################################


@pytest.mark.integration
class TestScoreSetArtifacts:
    def test_an_unmapped_score_set_contributes_scores_alone(self, session, make_dump_score_set, anonymous_principal):
        score_set = make_dump_score_set(mapped=False)

        artifacts = _artifacts(session, score_set, anonymous_principal)

        assert set(artifacts) == {f"csv/{archive_path_base(score_set.urn)}.scores.csv"}

    def test_a_mapped_score_set_contributes_every_annotation_artifact(
        self, session, make_dump_score_set, anonymous_principal
    ):
        score_set = make_dump_score_set(mapped=True, current=True)
        base = archive_path_base(score_set.urn)

        artifacts = _artifacts(session, score_set, anonymous_principal)

        assert set(artifacts) == {
            f"csv/{base}.scores.csv",
            f"csv/{base}.annotations.csv",
            f"mapped/{base}.mapped-variants.json",
            f"va/{base}.va.ndjson",
        }

    def test_a_superseded_score_set_contributes_scores_alone(self, session, make_dump_score_set, anonymous_principal):
        score_set = make_dump_score_set(mapped=True, current=False)

        artifacts = _artifacts(session, score_set, anonymous_principal)

        assert set(artifacts) == {f"csv/{archive_path_base(score_set.urn)}.scores.csv"}

    def test_count_columns_add_the_counts_file(self, session, make_dump_score_set, anonymous_principal):
        score_set = make_dump_score_set(mapped=False, count_columns=("c_0",))
        base = archive_path_base(score_set.urn)

        artifacts = _artifacts(session, score_set, anonymous_principal)

        assert set(artifacts) == {f"csv/{base}.scores.csv", f"csv/{base}.counts.csv"}

    def test_every_path_sits_in_a_documented_directory(self, session, make_dump_score_set, anonymous_principal):
        """README `Archive Structure` lists exactly these three directories."""
        score_set = make_dump_score_set(count_columns=("c_0",))

        artifacts = _artifacts(session, score_set, anonymous_principal)

        assert {path.split("/")[0] for path in artifacts} == {"csv", "mapped", "va"}

    def test_every_artifact_carries_a_row_per_variant(self, session, make_dump_score_set, anonymous_principal):
        """Truthiness alone would accept a header with no rows, which is the failure worth catching."""
        score_set = make_dump_score_set(count_columns=("c_0",), variant_scores=({"score": 1.0}, {"score": 2.0}))

        artifacts = _artifacts(session, score_set, anonymous_principal)

        for path, content in artifacts.items():
            if path.endswith(".csv"):
                assert len(_parse_csv(content)) == 2, path
            elif path.endswith(".ndjson"):
                assert len(content.splitlines()) == 2, path
            else:
                assert len(json.loads(content)) == 2, path

    def test_yields_each_artifact_without_holding_the_others(self, session, make_dump_score_set, anonymous_principal):
        """A generator, so the caller writes and releases each payload rather than accumulating four.

        One score set's `va.ndjson` runs to tens of kilobytes per variant, so returning them together
        made peak memory the sum of a score set's artifacts instead of its largest one.
        """
        score_set = make_dump_score_set(count_columns=("c_0",))

        artifacts = score_set_artifacts(session, score_set, anonymous_principal)

        first_path, first_content = next(artifacts)
        assert first_path == f"csv/{archive_path_base(score_set.urn)}.scores.csv"
        assert first_content
        assert sum(1 for _ in artifacts) == 4  # annotations, mapped, va, counts still unevaluated


####################################################################################################
# annotations.csv namespace selection
####################################################################################################


@pytest.mark.integration
class TestAnnotationExportNamespaces:
    def test_omits_the_groups_that_have_their_own_files(self, session, make_dump_score_set, anonymous_viewer):
        score_set = make_dump_score_set(variant_scores=({"score": 1.0, "se": 0.25},), count_columns=("c_0",))

        namespaces = annotation_export_namespaces(session, score_set, anonymous_viewer)

        assert not {
            CsvNamespace.SCORES,
            CsvNamespace.SCORES_CUSTOM,
            CsvNamespace.COUNTS,
            CsvNamespace.SCORE_SET,
        }.intersection(namespaces)

    def test_offers_the_mapping_derived_groups(self, session, make_dump_score_set, anonymous_viewer):
        namespaces = annotation_export_namespaces(session, make_dump_score_set(), anonymous_viewer)

        assert {
            CsvNamespace.REFERENCE_HGVS,
            CsvNamespace.VEP,
            CsvNamespace.GNOMAD,
            CsvNamespace.CLINGEN,
        }.issubset(namespaces)

    def test_includes_a_research_use_only_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        """RUO is eligibility, not audience: the group is emitted and flagged, not withheld.

        The VA-Spec NDJSON makes the opposite choice deliberately; see the README, which documents the
        two artifacts as carrying different calibration sets.
        """
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, research_use_only=True)

        namespaces = annotation_export_namespaces(session, score_set, anonymous_viewer)

        assert calibration_namespace_for_urn(calibration.urn) in namespaces

    def test_excludes_a_private_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        """The archive's audience is the public, so a private calibration is never offered a column group."""
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, private=True)

        namespaces = annotation_export_namespaces(session, score_set, anonymous_viewer)

        assert calibration_namespace_for_urn(calibration.urn) not in namespaces

    def test_carries_exactly_the_expected_groups(
        self, session, make_dump_score_set, make_dump_calibration, add_clinvar_control, anonymous_viewer
    ):
        """The only assertion here that fails on a group appearing or disappearing.

        Every other test in this class is inclusion-style, which cannot catch either direction: a new
        `CsvNamespace` member reaches the published archive automatically, because this function subtracts
        from discovery rather than opting groups in, and discovery's actual job is populating a download
        dialog. A group silently dropped is worse still — a consumer's column vanishes between releases.

        Both are legitimate changes to make. Neither should be possible without editing this list.
        """
        score_set = make_dump_score_set(variant_scores=({"score": 1.0, "se": 0.25},), count_columns=("c_0",))
        add_clinvar_control(_mapped_variants_of(session, score_set)[0], db_version="01_2024")
        calibration = make_dump_calibration(score_set)

        namespaces = annotation_export_namespaces(session, score_set, anonymous_viewer)

        assert set(namespaces) == {
            CsvNamespace.REFERENCE_HGVS,
            CsvNamespace.VEP,
            CsvNamespace.GNOMAD,
            CsvNamespace.CLINGEN,
            "clinvar.2024_01",
            calibration_namespace_for_urn(calibration.urn),
        }

    def test_carries_every_ingested_clinvar_release(
        self, session, make_dump_score_set, add_clinvar_control, anonymous_viewer
    ):
        """README: this file carries every release MaveDB holds, not just the most recent."""
        score_set = make_dump_score_set()
        mapped_variant = _mapped_variants_of(session, score_set)[0]
        add_clinvar_control(mapped_variant, db_version="01_2024")
        add_clinvar_control(mapped_variant, db_version="06_2025")

        namespaces = annotation_export_namespaces(session, score_set, anonymous_viewer)

        assert "clinvar.2024_01" in namespaces
        assert "clinvar.2025_06" in namespaces


####################################################################################################
# annotations.csv content
####################################################################################################


@pytest.mark.integration
class TestAnnotationsCsv:
    def test_joins_to_scores_on_accession(self, session, make_dump_score_set, anonymous_viewer):
        """README `Joining files for a single score set` promises this join key in every CSV."""
        score_set = make_dump_score_set(variant_scores=({"score": 1.0}, {"score": 2.0}))

        annotations = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))
        scores = _parse_csv(scores_csv(session, score_set))

        assert [row["accession"] for row in annotations] == [row["accession"] for row in scores]

    def test_a_research_use_only_group_is_flagged_as_such(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, research_use_only=True)
        namespace = calibration_namespace_for_urn(calibration.urn)

        rows = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))

        assert all(row[f"{namespace}.research_use_only"] == "True" for row in rows)

    def test_a_clinical_group_reports_research_use_only_false(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, research_use_only=False)
        namespace = calibration_namespace_for_urn(calibration.urn)

        rows = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))

        assert all(row[f"{namespace}.research_use_only"] == "False" for row in rows)

    def test_carries_no_column_from_a_private_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, private=True)
        namespace = calibration_namespace_for_urn(calibration.urn)

        header = _header(annotations_csv(session, score_set, anonymous_viewer))

        assert not any(column.startswith(f"{namespace}.") for column in header)

    def test_a_clinvar_release_carries_its_release_in_the_column_name(
        self, session, make_dump_score_set, add_clinvar_control, anonymous_viewer
    ):
        score_set = make_dump_score_set()
        add_clinvar_control(_mapped_variants_of(session, score_set)[0], db_version="01_2024")

        header = _header(annotations_csv(session, score_set, anonymous_viewer))

        assert "clinvar.2024_01.clinical_significance" in header
        assert "clinvar.2024_01.clinical_review_status" in header

    def test_reports_what_the_given_viewer_may_read_rather_than_the_public_subset(
        self, session, make_dump_score_set, make_dump_calibration
    ):
        """Both the namespace selection and the cell resolution follow the viewer passed in.

        The dump's own viewer is anonymous, so an implicitly-defaulted one would agree with it on every
        artifact and no anonymous test could tell the difference. An admin is the cheapest caller that
        can: if either half of `annotations_csv` resolved its audience on its own, the private
        calibration would go missing here.
        """
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, private=True)
        namespace = calibration_namespace_for_urn(calibration.urn)
        admin = Principal(Mock(user=Mock(id=1, username="admin"), active_roles=[UserRole.admin]))

        rows = _parse_csv(annotations_csv(session, score_set, admin.viewer_for(ScoreCalibrationViewer)))

        assert rows
        assert all(row[f"{namespace}.title"] == calibration.title for row in rows)

    def test_a_variant_in_the_abnormal_range_reports_the_met_criterion(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        """The README documents these five columns; a variant in no range exercises none of them.

        Score -2.0 falls inside the abnormal range, so this is the only shape in which
        `acmg_evidence_strength` is ever populated.
        """
        score_set = make_dump_score_set(variant_scores=({"score": -2.0},))
        namespace = calibration_namespace_for_urn(make_dump_calibration(score_set).urn)

        row = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))[0]

        assert row[f"{namespace}.functional_classification"] == "abnormal"
        assert row[f"{namespace}.acmg_criterion"] == "PS3"
        assert row[f"{namespace}.acmg_evidence_strength"] == "STRONG"
        assert row[f"{namespace}.acmg_evidence_outcome_code"] == "PS3"
        assert row[f"{namespace}.pathogenicity_classification"] == "PATHOGENIC"

    def test_a_variant_in_the_normal_range_reports_the_benign_criterion(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        score_set = make_dump_score_set(variant_scores=({"score": 3.0},))
        namespace = calibration_namespace_for_urn(make_dump_calibration(score_set).urn)

        row = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))[0]

        assert row[f"{namespace}.functional_classification"] == "normal"
        assert row[f"{namespace}.acmg_criterion"] == "BS3"
        assert row[f"{namespace}.pathogenicity_classification"] == "BENIGN"

    def test_a_variant_in_no_range_reports_the_criterion_as_not_met(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_viewer
    ):
        """README: distinct from a rangeless calibration, which has no classification to give at all."""
        score_set = make_dump_score_set(variant_scores=({"score": 0.0},))
        namespace = calibration_namespace_for_urn(make_dump_calibration(score_set).urn)

        row = _parse_csv(annotations_csv(session, score_set, anonymous_viewer))[0]

        assert row[f"{namespace}.functional_classification"] == "indeterminate"
        assert row[f"{namespace}.acmg_evidence_outcome_code"] == "PS3_not_met"
        assert row[f"{namespace}.acmg_evidence_strength"] == "NA"
        assert row[f"{namespace}.pathogenicity_classification"] == "UNCERTAIN_SIGNIFICANCE"


####################################################################################################
# mapped-variants.json
####################################################################################################


@pytest.mark.integration
class TestMappedVariantsJson:
    def test_emits_only_current_mappings(self, session, make_dump_score_set):
        """README caveat: superseded records from earlier mapping runs are not retained in the dump.

        `GET /api/v1/score-sets/{urn}/mapped-variants` does return them, which is why the README
        documents `current` as always `true` here rather than as a field to filter on.
        """
        score_set = make_dump_score_set(variant_scores=({"score": 1.0}, {"score": 2.0}))
        mapped_variants = _mapped_variants_of(session, score_set)
        mapped_variants[0].current = False
        session.add(mapped_variants[0])
        session.commit()

        records = json.loads(mapped_variants_json(session, score_set))

        assert len(records) == 1
        assert records[0]["variantUrn"] == mapped_variants[1].variant.urn

    def test_carries_the_documented_join_key(self, session, make_dump_score_set):
        score_set = make_dump_score_set()

        records = json.loads(mapped_variants_json(session, score_set))

        assert records
        assert all("variantUrn" in record for record in records)

    def test_is_a_json_array(self, session, make_dump_score_set):
        assert isinstance(json.loads(mapped_variants_json(session, make_dump_score_set())), list)


####################################################################################################
# va.ndjson
####################################################################################################


def _va_records(session, score_set, principal):
    return [json.loads(line) for line in va_ndjson(session, score_set, principal).splitlines()]


@pytest.mark.integration
class TestVaNdjson:
    def test_emits_one_line_per_current_mapped_variant(self, session, make_dump_score_set, anonymous_principal):
        """README: the line count equals the current mapped-variant count."""
        score_set = make_dump_score_set(variant_scores=({"score": 1.0}, {"score": 2.0}, {"score": 3.0}))

        content = va_ndjson(session, score_set, anonymous_principal)

        assert len(content.splitlines()) == 3

    def test_every_line_is_newline_terminated(self, session, make_dump_score_set, anonymous_principal):
        """Including the last, so a line-based consumer needs no special case."""
        score_set = make_dump_score_set(variant_scores=({"score": 1.0}, {"score": 2.0}))

        content = va_ndjson(session, score_set, anonymous_principal)

        assert content.endswith("\n")
        assert "\n\n" not in content

    def test_every_line_is_an_envelope_with_both_fields(self, session, make_dump_score_set, anonymous_principal):
        score_set = make_dump_score_set(post_mapped=True, variant_scores=({"score": 1.0}, {"score": 2.0}))

        records = _va_records(session, score_set, anonymous_principal)

        assert all(set(record) == {"variant_urn", "annotation"} for record in records)

    def test_an_unannotatable_variant_still_gets_its_urn(self, session, make_dump_score_set, anonymous_principal):
        """README: `annotation` is null for a current mapping with no post-mapped allele."""
        score_set = make_dump_score_set(post_mapped=False)

        records = _va_records(session, score_set, anonymous_principal)

        assert records
        assert all(record["variant_urn"] for record in records)
        assert all(record["annotation"] is None for record in records)

    def test_is_empty_for_a_score_set_with_no_current_mappings(self, session, make_dump_score_set, anonymous_principal):
        assert va_ndjson(session, make_dump_score_set(mapped=False), anonymous_principal) == ""

    def test_a_post_mapped_variant_carries_an_annotation(self, session, make_dump_score_set, anonymous_principal):
        """The counterpart to the null case: a post-mapped allele is what makes a variant annotatable."""
        score_set = make_dump_score_set(post_mapped=True)

        records = _va_records(session, score_set, anonymous_principal)

        assert records
        assert all(record["annotation"] is not None for record in records)

    def test_a_clinical_calibration_raises_the_record_to_the_pathogenicity_layer(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """README `va/{urn}.va.ndjson`: the highest materialized layer, which a calibration supplies."""
        score_set = make_dump_score_set(post_mapped=True, variant_scores=({"score": -2.0},))
        make_dump_calibration(score_set, research_use_only=False)

        annotation = _va_records(session, score_set, anonymous_principal)[0]["annotation"]

        assert annotation["type"] == "Statement"
        assert annotation["proposition"]["type"] == "VariantPathogenicityProposition"

    def test_a_research_use_only_calibration_does_not_raise_the_layer(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """README: RUO calibrations are excluded from this file, unlike `annotations.csv`.

        The record falls back to the functional-impact layer rather than disappearing, so the variant is
        still reported — just without a pathogenicity statement built on evidence not cleared for it.
        """
        score_set = make_dump_score_set(post_mapped=True, variant_scores=({"score": -2.0},))
        make_dump_calibration(score_set, research_use_only=True)

        annotation = _va_records(session, score_set, anonymous_principal)[0]["annotation"]

        assert annotation["type"] == "ExperimentalVariantFunctionalImpactStudyResult"

    def test_a_private_calibration_does_not_raise_the_layer(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """A private calibration is withheld from every artifact, this one included."""
        score_set = make_dump_score_set(post_mapped=True, variant_scores=({"score": -2.0},))
        make_dump_calibration(score_set, private=True)

        annotation = _va_records(session, score_set, anonymous_principal)[0]["annotation"]

        assert annotation["type"] == "ExperimentalVariantFunctionalImpactStudyResult"


####################################################################################################
# main.json narrowing
####################################################################################################


@pytest.mark.integration
class TestPublicExperimentSet:
    """The narrowing mechanics alone. Which ids are visible is `TestPublicDumpMetadata`'s question."""

    @pytest.fixture
    def experiment_set_view(self, session, make_dump_score_set):
        make_dump_score_set()
        experiment_set = session.query(ExperimentSet).one()
        session.refresh(experiment_set)
        return ExperimentSetPublicDump.model_validate(experiment_set)

    def test_keeps_a_calibration_named_visible(self, session, make_dump_score_set, make_dump_calibration):
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set)
        session.refresh(score_set)
        view = ExperimentSetPublicDump.model_validate(session.query(ExperimentSet).one())

        narrowed = public_experiment_set(view, {calibration.id})

        kept = narrowed.experiments[0].score_sets[0].score_calibrations
        assert [c.id for c in kept] == [calibration.id]

    def test_drops_a_calibration_not_named_visible(self, session, make_dump_score_set, make_dump_calibration):
        score_set = make_dump_score_set()
        make_dump_calibration(score_set)
        session.refresh(score_set)
        view = ExperimentSetPublicDump.model_validate(session.query(ExperimentSet).one())

        narrowed = public_experiment_set(view, set())

        assert narrowed.experiments[0].score_sets[0].score_calibrations == []

    def test_returns_none_when_no_experiment_has_a_score_set(self, experiment_set_view):
        """An experiment set whose score sets were all filtered out by license contributes nothing."""
        emptied = experiment_set_view.model_copy(
            update={
                "experiments": [
                    experiment.model_copy(update={"score_sets": []}) for experiment in experiment_set_view.experiments
                ]
            }
        )

        assert public_experiment_set(emptied, set()) is None

    def test_does_not_mutate_the_orm_graph(self, session, make_dump_score_set, make_dump_calibration):
        """`ScoreSet.score_calibrations` cascades delete-orphan, so narrowing the ORM collection instead
        of the validated view would mark the dropped calibration for deletion. The script commits when
        run with --commit, which would make a reporting run destroy production rows.
        """
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, private=True)
        session.refresh(score_set)
        view = ExperimentSetPublicDump.model_validate(session.query(ExperimentSet).one())

        public_experiment_set(view, set())
        session.commit()

        assert session.query(ScoreCalibration).filter(ScoreCalibration.id == calibration.id).one_or_none()
        session.refresh(score_set)
        assert [c.id for c in score_set.score_calibrations] == [calibration.id]


####################################################################################################
# Dump selection
####################################################################################################


@pytest.mark.integration
class TestPublishedExperimentSets:
    """What the dump is allowed to carry at all. A miss here leaks non-public data into a CC0 archive."""

    def _selected_urns(self, session):
        """The score-set URNs the dump would carry, as a real run would see them.

        The identity map is cleared first because the selection query narrows its relationships with
        `lazyload(...).and_(...)`, whose nested option is resolved when the collection is first
        traversed. Objects the fixtures left attached are re-resolved instead under
        `populate_existing=True`, and that path raises — an artifact of building and querying in one
        session, which a run against an existing database never encounters.
        """
        session.expunge_all()
        return {
            score_set.urn
            for experiment_set in published_experiment_sets(session)
            for experiment in experiment_set.experiments
            for score_set in experiment.score_sets
        }

    def test_carries_a_published_cc0_score_set(self, session, make_dump_score_set):
        score_set = make_dump_score_set(published=True, cc0=True)
        urn = score_set.urn

        assert urn in self._selected_urns(session)

    def test_withholds_an_unpublished_score_set(self, session, make_dump_score_set):
        score_set = make_dump_score_set(published=False, cc0=True)
        urn = score_set.urn

        assert urn not in self._selected_urns(session)

    def test_withholds_a_score_set_under_another_license(self, session, make_dump_score_set):
        """README: datasets under other licenses are excluded even when publicly visible on MaveDB."""
        score_set = make_dump_score_set(published=True, cc0=False)
        urn = score_set.urn

        assert urn not in self._selected_urns(session)

    def test_separates_the_two_within_one_experiment(self, session, make_dump_score_set):
        """The narrowing is per score set, so one excluded sibling must not take the others with it."""
        carried = make_dump_score_set(published=True, cc0=True).urn
        withheld = make_dump_score_set(published=True, cc0=False).urn

        urns = self._selected_urns(session)

        assert carried in urns
        assert withheld not in urns


####################################################################################################
# main.json composition
####################################################################################################


def _metadata_calibration_ids(metadata):
    return {
        calibration.id
        for experiment_set in metadata["experimentSets"]
        for experiment in experiment_set.experiments
        for score_set in experiment.score_sets
        for calibration in (score_set.score_calibrations or [])
    }


@pytest.mark.integration
class TestPublicDumpMetadata:
    """Which calibrations the archive names at all — asked of the principal, not of the score set.

    `TestPublicExperimentSet` covers what narrowing does with a set of visible ids. This covers how that
    set is arrived at, which is the step that decides whether a private calibration reaches the archive.
    """

    def test_carries_a_public_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        calibration_id = make_dump_calibration(make_dump_score_set()).id
        session.expunge_all()

        metadata, _ = public_dump_metadata(session, anonymous_principal)

        assert _metadata_calibration_ids(metadata) == {calibration_id}

    def test_withholds_a_private_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """Publishing a score set does not publish its calibrations; this is the gate that enforces it."""
        make_dump_calibration(make_dump_score_set(), private=True)
        session.expunge_all()

        metadata, _ = public_dump_metadata(session, anonymous_principal)

        assert _metadata_calibration_ids(metadata) == set()

    def test_withholds_only_the_private_one_of_a_pair(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """A score set can carry both, so the gate has to be per calibration rather than per score set."""
        score_set = make_dump_score_set()
        public_id = make_dump_calibration(score_set).id
        make_dump_calibration(score_set, private=True)
        session.expunge_all()

        metadata, _ = public_dump_metadata(session, anonymous_principal)

        assert _metadata_calibration_ids(metadata) == {public_id}

    def test_carries_a_research_use_only_calibration(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """RUO bears on eligibility, not on audience, so it does not withhold the calibration."""
        calibration_id = make_dump_calibration(make_dump_score_set(), research_use_only=True).id
        session.expunge_all()

        metadata, _ = public_dump_metadata(session, anonymous_principal)

        assert _metadata_calibration_ids(metadata) == {calibration_id}

    def test_reports_the_urns_whose_artifacts_the_archive_carries(
        self, session, make_dump_score_set, anonymous_principal
    ):
        carried = make_dump_score_set(published=True, cc0=True).urn
        withheld = make_dump_score_set(published=True, cc0=False).urn
        session.expunge_all()

        _, score_set_urns = public_dump_metadata(session, anonymous_principal)

        assert carried in score_set_urns
        assert withheld not in score_set_urns

    def test_carries_the_documented_top_level_fields(self, session, make_dump_score_set, anonymous_principal):
        """README `main.json`: a JSON object with exactly these three fields."""
        make_dump_score_set()
        session.expunge_all()

        metadata, _ = public_dump_metadata(session, anonymous_principal)

        assert set(metadata) == {"title", "asOf", "experimentSets"}
        assert metadata["title"] == "MaveDB public data"


####################################################################################################
# The whole archive
####################################################################################################


@pytest.mark.integration
class TestWritePublicDump:
    def test_carries_the_documented_fixed_members(self, session, make_dump_score_set, anonymous_principal):
        """README `Archive Structure`: every archive opens with these three, whatever it holds."""
        make_dump_score_set()
        session.expunge_all()

        names = _written_archive(session, anonymous_principal).namelist()

        assert {"main.json", "LICENSE.txt", "README.md"}.issubset(names)

    def test_carries_every_artifact_of_a_carried_score_set(self, session, make_dump_score_set, anonymous_principal):
        base = archive_path_base(make_dump_score_set(count_columns=("c_0",)).urn)
        session.expunge_all()

        names = _written_archive(session, anonymous_principal).namelist()

        assert {
            f"csv/{base}.scores.csv",
            f"csv/{base}.counts.csv",
            f"csv/{base}.annotations.csv",
            f"mapped/{base}.mapped-variants.json",
            f"va/{base}.va.ndjson",
        }.issubset(names)

    def test_carries_no_artifact_of_a_withheld_score_set(self, session, make_dump_score_set, anonymous_principal):
        """The end-to-end license gate: a non-CC0 score set contributes no member under any prefix."""
        base = archive_path_base(make_dump_score_set(published=True, cc0=False).urn)
        session.expunge_all()

        names = _written_archive(session, anonymous_principal).namelist()

        assert not [name for name in names if base in name]

    def test_main_json_names_the_score_sets_whose_files_are_present(
        self, session, make_dump_score_set, anonymous_principal
    ):
        """README `Joining files for a single score set` starts from a URN read out of `main.json`."""
        make_dump_score_set()
        session.expunge_all()

        archive = _written_archive(session, anonymous_principal)
        metadata = json.loads(archive.read("main.json"))
        names = set(archive.namelist())

        urns = [
            score_set["urn"]
            for experiment_set in metadata["experimentSets"]
            for experiment in experiment_set["experiments"]
            for score_set in experiment["scoreSets"]
        ]
        assert urns
        for urn in urns:
            assert f"csv/{archive_path_base(urn)}.scores.csv" in names

    def test_a_private_calibration_reaches_neither_main_json_nor_the_annotations_csv(
        self, session, make_dump_score_set, make_dump_calibration, anonymous_principal
    ):
        """One assertion per artifact would let the two paths drift; the archive is where they must agree."""
        score_set = make_dump_score_set()
        calibration = make_dump_calibration(score_set, private=True)
        base, urn = archive_path_base(score_set.urn), calibration.urn
        session.expunge_all()

        archive = _written_archive(session, anonymous_principal)

        assert urn not in archive.read("main.json").decode()
        assert urn not in archive.read(f"csv/{base}.annotations.csv").decode()
