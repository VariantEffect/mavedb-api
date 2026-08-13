# ruff: noqa: E402

import csv
import io
from datetime import date
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import event

from mavedb.lib.csv.namespaces import calibration_namespace_for_urn, is_valid_csv_namespace
from mavedb.lib.csv.score_set import available_score_set_csv_namespaces, get_score_set_variants_as_csv
from mavedb.lib.csv.variant import (
    BASE_VARIANT_CSV_NAMESPACES,
    available_variant_csv_namespaces,
    get_variant_csv,
)
from mavedb.lib.permissions.principal import Principal
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.acmg_classification import ACMGClassification
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.enums.user_role import UserRole
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant
from tests.helpers.constants import (
    TEST_GA4GH_DIGEST,
    TEST_GA4GH_IDENTIFIER,
    TEST_GNOMAD_DATA_VERSION,
    TEST_GNOMAD_VARIANT,
    TEST_MINIMAL_MAPPED_VARIANT,
    TEST_MINIMAL_VARIANT,
    TEST_SEQ_SCORESET,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _add_pathogenicity_calibration(db, score_set, variants_in_abnormal_range, urn, title, research_use_only=False):
    """Attach a calibration with a normal (BS3) and abnormal (PS3) range to *score_set*.

    Only *variants_in_abnormal_range* are associated with the abnormal range, which is what
    ``functional_classification_of_variant`` consults to classify a variant.
    """
    calibration = ScoreCalibration(
        score_set_id=score_set.id,
        urn=urn,
        title=title,
        baseline_score=0.0,
        research_use_only=research_use_only,
        primary=True,
        private=False,
        calibration_metadata={},
        created_by_id=score_set.created_by_id,
        modified_by_id=score_set.modified_by_id,
    )
    db.add(calibration)
    db.commit()
    db.refresh(calibration)

    abnormal_acmg = db.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.PS3).first()
    normal_acmg = db.query(ACMGClassification).filter(ACMGClassification.criterion == ACMGCriterion.BS3).first()

    db.add(
        ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="test abnormal functional range",
            description="An abnormal functional range",
            functional_classification=FunctionalClassificationOptions.abnormal,
            range=[-5.0, -1.0],
            inclusive_lower_bound=True,
            inclusive_upper_bound=False,
            acmg_classification_id=abnormal_acmg.id,
            variants=list(variants_in_abnormal_range),
        )
    )
    db.add(
        ScoreCalibrationFunctionalClassification(
            calibration_id=calibration.id,
            label="test normal functional range",
            description="A normal functional range",
            functional_classification=FunctionalClassificationOptions.normal,
            range=[1.0, 5.0],
            inclusive_lower_bound=True,
            inclusive_upper_bound=False,
            acmg_classification_id=normal_acmg.id,
            variants=[],
        )
    )
    db.commit()
    db.refresh(calibration)

    return calibration


def _add_rangeless_calibration(db, score_set, urn, title):
    """Attach a calibration carrying only a baseline score, with no ranges to classify against.

    It can support neither a functional nor a pathogenicity annotation, so every column of its namespace
    would be NA.
    """
    calibration = ScoreCalibration(
        score_set_id=score_set.id,
        urn=urn,
        title=title,
        baseline_score=0.0,
        research_use_only=False,
        primary=True,
        private=False,
        calibration_metadata={},
        created_by_id=score_set.created_by_id,
        modified_by_id=score_set.modified_by_id,
    )
    db.add(calibration)
    db.commit()
    db.refresh(calibration)

    return calibration


def _add_second_score_set_with_equivalent_variant(db, first_score_set, clingen_allele_id):
    """Create a second score set measuring the same ClinGen allele as *first_score_set*'s variant."""
    score_set_scaffold = TEST_SEQ_SCORESET.copy()
    score_set_scaffold.pop("target_genes")
    score_set = ScoreSet(
        **score_set_scaffold,
        urn="urn:mavedb:00000001-a-2",
        experiment_id=first_score_set.experiment_id,
        licence_id=first_score_set.licence_id,
        created_by_id=first_score_set.created_by_id,
        modified_by_id=first_score_set.modified_by_id,
    )
    db.add(score_set)
    db.commit()
    db.refresh(score_set)

    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#1", score_set_id=score_set.id)
    db.add(variant)
    db.commit()
    db.refresh(variant)

    mapped_variant = MappedVariant(
        **TEST_MINIMAL_MAPPED_VARIANT,
        variant_id=variant.id,
        clingen_allele_id=clingen_allele_id,
    )
    db.add(mapped_variant)
    db.commit()
    db.refresh(mapped_variant)

    return score_set, variant, mapped_variant


def _add_clinvar_control(db, mapped_variant, significance, review_status, db_version):
    mapped_variant.clinical_controls.append(
        ClinicalControl(
            db_identifier="183058",
            gene_symbol="PTEN",
            clinical_significance=significance,
            clinical_review_status=review_status,
            db_name="ClinVar",
            db_version=db_version,
        )
    )
    db.add(mapped_variant)
    db.commit()


def _parse_csv(csv_text):
    return list(csv.DictReader(io.StringIO(csv_text)))


# ---------------------------------------------------------------------------
# TestGetVariantCsv
# ---------------------------------------------------------------------------

CALIBRATION_URN_1 = "urn:mavedb:calibration-11111111-1111-1111-1111-111111111111"
CALIBRATION_URN_2 = "urn:mavedb:calibration-22222222-2222-2222-2222-222222222222"
CALIBRATION_URN_OTHER_SCORE_SET = "urn:mavedb:calibration-33333333-3333-3333-3333-333333333333"

CALIBRATION_NS_1 = calibration_namespace_for_urn(CALIBRATION_URN_1)
CALIBRATION_NS_2 = calibration_namespace_for_urn(CALIBRATION_URN_2)
CALIBRATION_NS_OTHER = calibration_namespace_for_urn(CALIBRATION_URN_OTHER_SCORE_SET)


class TestGetVariantCsv:
    """Integration tests for the DB-bound clinical CSV composer."""

    def test_single_variant_yields_one_row_of_base_columns(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 1
        assert rows[0]["accession"] == variant.urn
        assert rows[0]["relationship.match_type"] == "exact"
        assert rows[0]["score_set.score_set_urn"] == variant.score_set.urn
        assert rows[0]["scores.score"] == str(TEST_MINIMAL_VARIANT["data"]["score_data"]["score"])
        assert rows[0]["hgvs_nt"] == TEST_MINIMAL_VARIANT["hgvs_nt"]

    def test_unknown_urn_raises(self, session, setup_lib_db_with_mapped_variant):
        with pytest.raises(ValueError, match="not found"):
            get_variant_csv(session, "urn:mavedb:00000001-a-1#999")

    def test_no_calibration_yields_no_calibration_columns(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant

        csv_text = get_variant_csv(session, variant.urn)

        assert "calibration." not in csv_text

    def test_calibration_namespace_is_included_by_default(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Test Clinical Calibration"
        )

        csv_text = get_variant_csv(session, variant.urn)
        rows = _parse_csv(csv_text)

        assert len(rows) == 1
        assert rows[0][f"{CALIBRATION_NS_1}.title"] == "Test Clinical Calibration"
        assert rows[0][f"{CALIBRATION_NS_1}.functional_classification"] == "abnormal"
        assert rows[0][f"{CALIBRATION_NS_1}.acmg_criterion"] == "PS3"
        assert rows[0][f"{CALIBRATION_NS_1}.acmg_evidence_strength"] == "STRONG"
        assert rows[0][f"{CALIBRATION_NS_1}.acmg_evidence_outcome_code"] == "PS3"
        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "PATHOGENIC"

    def test_variant_outside_calibration_ranges_is_uncertain(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [], urn=CALIBRATION_URN_1, title="Test Clinical Calibration"
        )

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert rows[0][f"{CALIBRATION_NS_1}.functional_classification"] == "indeterminate"
        assert rows[0][f"{CALIBRATION_NS_1}.acmg_evidence_strength"] == "NA"
        assert rows[0][f"{CALIBRATION_NS_1}.acmg_evidence_outcome_code"] == "PS3_not_met"
        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "UNCERTAIN_SIGNIFICANCE"

    def test_multiple_calibrations_appear_side_by_side(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Contains The Variant"
        )
        _add_pathogenicity_calibration(
            session, variant.score_set, [], urn=CALIBRATION_URN_2, title="Does Not Contain The Variant"
        )

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 1
        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "PATHOGENIC"
        assert rows[0][f"{CALIBRATION_NS_2}.pathogenicity_classification"] == "UNCERTAIN_SIGNIFICANCE"

    def test_research_use_only_calibration_is_offered_but_labelled(self, session, setup_lib_db_with_mapped_variant):
        """The score set page already shows these, so the export offers them — flagged, not hidden."""
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session,
            variant.score_set,
            [variant],
            urn=CALIBRATION_URN_1,
            title="Provisional Calibration",
            research_use_only=True,
        )

        entry = next(
            entry
            for entry in available_variant_csv_namespaces(session, variant.urn)
            if entry.namespace == CALIBRATION_NS_1
        )

        assert entry.label == "Research Use Only: Provisional Calibration"
        assert entry.selected_by_default is False

    def test_research_use_only_calibration_is_not_in_the_default_download(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """A clinically-framed default must not silently carry unvalidated thresholds."""
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session,
            variant.score_set,
            [variant],
            urn=CALIBRATION_URN_1,
            title="Provisional Calibration",
            research_use_only=True,
        )

        assert CALIBRATION_NS_1 not in get_variant_csv(session, variant.urn)

    def test_research_use_only_calibration_is_served_when_named(self, session, setup_lib_db_with_mapped_variant):
        """Naming the namespace is the opt-in, and the exported row declares its own standing."""
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session,
            variant.score_set,
            [variant],
            urn=CALIBRATION_URN_1,
            title="Provisional Calibration",
            research_use_only=True,
        )

        rows = _parse_csv(get_variant_csv(session, variant.urn, namespaces=["scores", CALIBRATION_NS_1]))

        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "PATHOGENIC"
        assert rows[0][f"{CALIBRATION_NS_1}.research_use_only"] == "True"

    def test_clinical_calibration_declares_it_is_not_research_use_only(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert rows[0][f"{CALIBRATION_NS_1}.research_use_only"] == "False"

    def test_explicit_namespaces_restrict_columns(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Requested")
        _add_pathogenicity_calibration(session, variant.score_set, [variant], urn=CALIBRATION_URN_2, title="Omitted")

        csv_text = get_variant_csv(session, variant.urn, namespaces=["scores", CALIBRATION_NS_1])
        rows = _parse_csv(csv_text)

        assert rows[0][f"{CALIBRATION_NS_1}.title"] == "Requested"
        assert CALIBRATION_NS_2 not in csv_text
        # Namespaces the caller did not ask for contribute no columns.
        assert "gnomad.gnomad_af" not in csv_text
        assert "relationship.match_type" not in csv_text

    def test_equivalent_measurements_share_clingen_allele_id(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clingen_allele_id = "CA123456"
        session.add(mapped_variant)
        session.commit()

        variant = mapped_variant.variant
        _add_second_score_set_with_equivalent_variant(session, variant.score_set, "CA123456")

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 2
        # The requested measurement comes first.
        assert rows[0]["accession"] == variant.urn
        assert rows[1]["score_set.score_set_urn"] == "urn:mavedb:00000001-a-2"
        assert all(row["relationship.match_type"] == "exact" for row in rows)
        assert all(row["clingen.clingen_allele_id"] == "CA123456" for row in rows)

    def test_each_measurement_is_interpreted_only_under_its_own_calibrations(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """A score from one assay carries no meaning under another assay's thresholds."""
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clingen_allele_id = "CA123456"
        session.add(mapped_variant)
        session.commit()

        variant = mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="First Score Set Calibration"
        )
        other_score_set, other_variant, _ = _add_second_score_set_with_equivalent_variant(
            session, variant.score_set, "CA123456"
        )
        _add_pathogenicity_calibration(
            session,
            other_score_set,
            [other_variant],
            urn=CALIBRATION_URN_OTHER_SCORE_SET,
            title="Second Score Set Calibration",
        )

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 2
        # Each row is classified under its own score set's calibration...
        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "PATHOGENIC"
        assert rows[1][f"{CALIBRATION_NS_OTHER}.pathogenicity_classification"] == "PATHOGENIC"
        # ...and left empty under the other score set's.
        assert rows[0][f"{CALIBRATION_NS_OTHER}.pathogenicity_classification"] == "NA"
        assert rows[1][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "NA"

    def test_calibration_entries_report_their_score_set(self, session, setup_lib_db_with_mapped_variant):
        """A calibration means nothing against another score set's scores, so say which one owns it."""
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clingen_allele_id = "CA123456"
        session.add(mapped_variant)
        session.commit()

        variant = mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="First Assay Calibration"
        )
        other_score_set, other_variant, _ = _add_second_score_set_with_equivalent_variant(
            session, variant.score_set, "CA123456"
        )
        _add_pathogenicity_calibration(
            session,
            other_score_set,
            [other_variant],
            urn=CALIBRATION_URN_OTHER_SCORE_SET,
            title="Second Assay Calibration",
        )

        by_namespace = {entry.namespace: entry for entry in available_variant_csv_namespaces(session, variant.urn)}

        first, second = by_namespace[CALIBRATION_NS_1], by_namespace[CALIBRATION_NS_OTHER]
        assert first.score_set.urn == variant.score_set.urn
        assert second.score_set.urn == other_score_set.urn
        # The title is carried too, so a picker can name the score set rather than show its URN.
        assert first.score_set.title == variant.score_set.title
        # The two are distinguishable, which is the whole point.
        assert first.score_set.urn != second.score_set.urn

    def test_non_calibration_entries_have_no_owning_score_set(self, session, setup_lib_db_with_mapped_variant):
        """gnomAD and friends apply to any measurement, so there is no score set to attribute them to."""
        variant = setup_lib_db_with_mapped_variant.variant

        entries = available_variant_csv_namespaces(session, variant.urn)

        assert all(entry.score_set is None for entry in entries if not entry.namespace.startswith("calibration."))

    def test_variant_without_clingen_allele_id_stands_alone(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        # A second measurement with a null allele ID must not be pulled in on a null match.
        _add_second_score_set_with_equivalent_variant(session, variant.score_set, None)

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 1
        assert rows[0]["accession"] == variant.urn
        assert rows[0]["clingen.clingen_allele_id"] == "NA"

    def test_mapped_coordinates_and_external_annotations(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.hgvs_g = "NC_000010.11:g.87933147C>T"
        mapped_variant.hgvs_c = "NM_000314.8:c.100A>G"
        mapped_variant.hgvs_p = "NP_000305.3:p.Lys34Glu"
        mapped_variant.post_mapped = TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X
        mapped_variant.vep_functional_consequence = "missense_variant"
        mapped_variant.clingen_allele_id = "CA123456"
        mapped_variant.gnomad_variants.append(GnomADVariant(**TEST_GNOMAD_VARIANT))
        session.add(mapped_variant)
        session.commit()

        # Patched where it is used, not where it is defined: `fetch` binds the value with a `from`
        # import, so patching `mavedb.lib.gnomad` would leave the query filtering on the real version.
        with patch("mavedb.lib.csv.fetch.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
            rows = _parse_csv(get_variant_csv(session, mapped_variant.variant.urn))

        assert rows[0]["mavedb.post_mapped_hgvs_g"] == "NC_000010.11:g.87933147C>T"
        assert rows[0]["mavedb.post_mapped_hgvs_c"] == "NM_000314.8:c.100A>G"
        assert rows[0]["mavedb.post_mapped_hgvs_p"] == "NP_000305.3:p.Lys34Glu"
        assert rows[0]["mavedb.post_mapped_vrs_id"] == TEST_GA4GH_IDENTIFIER
        assert rows[0]["mavedb.post_mapped_vrs_id"] != TEST_GA4GH_DIGEST
        assert rows[0]["vep.vep_functional_consequence"] == "missense_variant"
        assert rows[0]["gnomad.gnomad_af"] == str(TEST_GNOMAD_VARIANT["allele_frequency"])
        assert rows[0]["clingen.clingen_allele_id"] == "CA123456"

    def test_post_mapped_vrs_id_is_never_synthesized_from_digest(self, session, setup_lib_db_with_mapped_variant):
        """A post-mapped object carrying only a ``digest`` reports NA rather than a built-up CURIE.

        The two stored fields are known to disagree on some rows, and only ``id`` is indexed and matched
        by the VRS lookup, so a digest-derived identifier would resolve to nothing.
        """
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.post_mapped = {
            key: value for key, value in TEST_VALID_POST_MAPPED_VRS_ALLELE_VRS2_X.items() if key != "id"
        }
        session.add(mapped_variant)
        session.commit()

        rows = _parse_csv(get_variant_csv(session, mapped_variant.variant.urn))

        assert rows[0]["mavedb.post_mapped_vrs_id"] == "NA"

    def test_gnomad_namespace_reports_the_whole_frequency_record(self, session, setup_lib_db_with_mapped_variant):
        """AF alone cannot be linked out from or judged for sampling depth; the namespace carries the record."""
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.gnomad_variants.append(GnomADVariant(**TEST_GNOMAD_VARIANT))
        session.add(mapped_variant)
        session.commit()

        with patch("mavedb.lib.csv.fetch.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
            csv_text = get_variant_csv(session, mapped_variant.variant.urn)
        rows = _parse_csv(csv_text)

        assert [column for column in rows[0].keys() if column.startswith("gnomad.")] == [
            "gnomad.gnomad_af",
            "gnomad.gnomad_ac",
            "gnomad.gnomad_an",
            "gnomad.gnomad_faf95_max",
            "gnomad.gnomad_faf95_max_ancestry",
            "gnomad.gnomad_id",
            "gnomad.gnomad_version",
        ]
        assert rows[0]["gnomad.gnomad_af"] == str(TEST_GNOMAD_VARIANT["allele_frequency"])
        assert rows[0]["gnomad.gnomad_ac"] == str(TEST_GNOMAD_VARIANT["allele_count"])
        assert rows[0]["gnomad.gnomad_an"] == str(TEST_GNOMAD_VARIANT["allele_number"])
        assert rows[0]["gnomad.gnomad_faf95_max"] == str(TEST_GNOMAD_VARIANT["faf95_max"])
        assert rows[0]["gnomad.gnomad_faf95_max_ancestry"] == str(TEST_GNOMAD_VARIANT["faf95_max_ancestry"])
        assert rows[0]["gnomad.gnomad_id"] == str(TEST_GNOMAD_VARIANT["db_identifier"])
        assert rows[0]["gnomad.gnomad_version"] == TEST_GNOMAD_DATA_VERSION

    def test_gnomad_record_absent_leaves_every_column_na(self, session, setup_lib_db_with_mapped_variant):
        """A variant with no gnomAD record reports NA across the namespace, never a zero frequency."""
        mapped_variant = setup_lib_db_with_mapped_variant

        with patch("mavedb.lib.csv.fetch.GNOMAD_DATA_VERSION", TEST_GNOMAD_DATA_VERSION):
            rows = _parse_csv(get_variant_csv(session, mapped_variant.variant.urn))

        gnomad_values = {key: value for key, value in rows[0].items() if key.startswith("gnomad.")}
        assert len(gnomad_values) == 7
        assert set(gnomad_values.values()) == {"NA"}

    def test_gnomad_variant_from_another_version_is_not_reported(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.gnomad_variants.append(GnomADVariant(**TEST_GNOMAD_VARIANT))
        session.add(mapped_variant)
        session.commit()

        with patch("mavedb.lib.csv.fetch.GNOMAD_DATA_VERSION", "v9.9"):
            rows = _parse_csv(get_variant_csv(session, mapped_variant.variant.urn))

        # The variant still gets a row: the version predicate is in the join's ON clause, so a gnomAD
        # record from another version leaves the frequency NA rather than dropping the variant.
        assert len(rows) == 1
        assert rows[0]["gnomad.gnomad_af"] == "NA"

    def test_latest_clinvar_release_is_reported_and_labeled(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        _add_clinvar_control(session, mapped_variant, "Likely benign", "single submitter", "11_2024")
        _add_clinvar_control(session, mapped_variant, "Pathogenic", "reviewed by expert panel", "02_2025")

        csv_text = get_variant_csv(session, mapped_variant.variant.urn)
        rows = _parse_csv(csv_text)

        # The release is carried in the column name so the call stays citable.
        assert "clinvar.2025_02.clinical_significance" in csv_text.splitlines()[0]
        assert rows[0]["clinvar.2025_02.clinical_significance"] == "Pathogenic"
        assert rows[0]["clinvar.2025_02.clinical_review_status"] == "reviewed by expert panel"
        assert "clinvar.2024_11" not in csv_text

    def test_non_clinvar_control_is_not_reported(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clinical_controls.append(
            ClinicalControl(
                db_identifier="ABC123",
                gene_symbol="BRCA1",
                clinical_significance="benign",
                clinical_review_status="lots of convincing evidence",
                db_name="GenDB",
                db_version="2024",
            )
        )
        session.add(mapped_variant)
        session.commit()

        csv_text = get_variant_csv(session, mapped_variant.variant.urn)

        assert "clinvar" not in csv_text
        assert "benign" not in csv_text

    def test_provenance_columns(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        session.add(TargetGene(score_set_id=variant.score_set.id, name="PTEN", category="protein_coding"))
        session.commit()

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert rows[0]["score_set.score_set_urn"] == variant.score_set.urn
        assert rows[0]["score_set.target_gene"] == "PTEN"

    def test_unmapped_variant_in_an_unmapped_score_set_omits_mapping_columns(self, session, setup_lib_db_with_variant):
        """Nothing in this score set has been mapped, so those columns would say nothing at all."""
        variant = setup_lib_db_with_variant

        csv_text = get_variant_csv(session, variant.urn)
        rows = _parse_csv(csv_text)

        assert len(rows) == 1
        assert rows[0]["accession"] == variant.urn
        assert rows[0]["scores.score"] == str(TEST_MINIMAL_VARIANT["data"]["score_data"]["score"])
        for omitted in (
            "mavedb.post_mapped_hgvs_g",
            "clingen.clingen_allele_id",
            "gnomad.gnomad_af",
            "vep.vep_functional_consequence",
        ):
            assert omitted not in csv_text

    def test_unmapped_variant_in_a_mapped_score_set_keeps_mapping_columns_as_na(
        self, session, setup_lib_db_with_variant
    ):
        """The score set is mapped, so the columns exist for it; NA is the honest value for this variant."""
        variant = setup_lib_db_with_variant
        mapped_sibling = Variant(
            **{**TEST_MINIMAL_VARIANT, "urn": f"{variant.score_set.urn}#2"}, score_set_id=variant.score_set_id
        )
        session.add(mapped_sibling)
        session.commit()
        session.add(MappedVariant(**TEST_MINIMAL_MAPPED_VARIANT, variant_id=mapped_sibling.id))
        session.commit()

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert len(rows) == 1
        assert rows[0]["accession"] == variant.urn
        assert rows[0]["mavedb.post_mapped_hgvs_g"] == "NA"
        assert rows[0]["clingen.clingen_allele_id"] == "NA"
        assert rows[0]["gnomad.gnomad_af"] == "NA"

    def test_unmapped_variant_respects_requested_namespaces(self, session, setup_lib_db_with_variant):
        variant = setup_lib_db_with_variant

        csv_text = get_variant_csv(session, variant.urn, namespaces=["scores"])

        assert "gnomad.gnomad_af" not in csv_text
        assert "scores.score" in csv_text.splitlines()[0]

    def test_superseded_mapping_is_ignored(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.current = False
        session.add(mapped_variant)
        session.add(
            MappedVariant(
                **{**TEST_MINIMAL_MAPPED_VARIANT, "current": True},
                variant_id=mapped_variant.variant_id,
                clingen_allele_id="CA999999",
            )
        )
        session.commit()

        rows = _parse_csv(get_variant_csv(session, mapped_variant.variant.urn))

        assert len(rows) == 1
        assert rows[0]["clingen.clingen_allele_id"] == "CA999999"

    def test_does_not_load_whole_score_range_variant_collections(self, session, setup_lib_db_with_mapped_variant):
        """Range membership must come from the association table, not by loading every variant of a range.

        The ORM check in ``annotation.classification`` loads each range's entire variant collection — with
        every variant's score data — once per range per row. On a large score set that dominates the
        export's runtime, so this pins the cheap path rather than trusting it to stay.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="First")
        _add_pathogenicity_calibration(session, variant.score_set, [], urn=CALIBRATION_URN_2, title="Second")

        statements: list[str] = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        bind = session.get_bind()
        event.listen(bind, "before_cursor_execute", record)
        try:
            get_variant_csv(session, variant.urn)
        finally:
            event.remove(bind, "before_cursor_execute", record)

        collection_loads = [
            statement
            for statement in statements
            if "score_calibration_functional_classification_variants" in statement and "variants.data" in statement
        ]
        assert collection_loads == [], (
            f"{len(collection_loads)} range-collection load(s) during one export; "
            "membership should come from the association table"
        )

    def test_namespace_discovery_does_not_scan_score_set_variants(self, session, setup_lib_db_with_mapped_variant):
        """Discovery must key on score sets, not join through their variants.

        `ScoreSet.variants` multiplies the calibration join by every variant in the score set before
        DISTINCT collapses it again, which made this route far slower than the score-set equivalent that
        filters on score_set_id directly.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="First")

        statements: list[str] = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        bind = session.get_bind()
        event.listen(bind, "before_cursor_execute", record)
        try:
            available_variant_csv_namespaces(session, variant.urn)
        finally:
            event.remove(bind, "before_cursor_execute", record)

        calibration_scans = [
            statement
            for statement in statements
            if "score_calibrations" in statement and " variants" in statement.replace("\n", " ")
        ]
        assert (
            calibration_scans == []
        ), "calibration discovery joined the variants table; it should filter on score_set_id"

    def test_base_namespaces_are_all_present_by_default(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant

        header = _parse_csv(get_variant_csv(session, variant.urn))[0].keys()

        # One representative column per base namespace.
        for column in (
            "scores.score",
            "vep.vep_functional_consequence",
            "gnomad.gnomad_af",
            "clingen.clingen_allele_id",
            "score_set.score_set_urn",
            "relationship.match_type",
        ):
            assert column in header, f"{column} missing; BASE_VARIANT_CSV_NAMESPACES={BASE_VARIANT_CSV_NAMESPACES}"


# ---------------------------------------------------------------------------
# TestComputeAvailableCsvNamespaces
# ---------------------------------------------------------------------------


class TestComputeAvailableCsvNamespaces:
    """What a namespace selector is offered for a score set."""

    def test_mapped_score_set_offers_mapping_backed_namespaces(self, session, setup_lib_db_with_mapped_variant):
        score_set = setup_lib_db_with_mapped_variant.variant.score_set

        namespaces = [entry.namespace for entry in available_score_set_csv_namespaces(session, score_set)]

        assert "score_set" in namespaces
        assert {"vep", "gnomad", "clingen"} <= set(namespaces)

    def test_unmapped_score_set_omits_mapping_backed_namespaces(self, session, setup_lib_db_with_variant):
        score_set = setup_lib_db_with_variant.score_set

        namespaces = [entry.namespace for entry in available_score_set_csv_namespaces(session, score_set)]

        assert "score_set" in namespaces
        assert not {"vep", "gnomad", "clingen"} & set(namespaces)

    def test_relationship_is_never_offered(self, session, setup_lib_db_with_mapped_variant):
        """match_type describes a row's relation to a requested record, which a score set has no notion of."""
        score_set = setup_lib_db_with_mapped_variant.variant.score_set

        assert "relationship" not in [
            entry.namespace for entry in available_score_set_csv_namespaces(session, score_set)
        ]

    def test_score_and_count_namespaces_follow_dataset_columns(self, session, setup_lib_db_with_mapped_variant):
        score_set = setup_lib_db_with_mapped_variant.variant.score_set
        score_set.dataset_columns = {"score_columns": ["scores.score"], "count_columns": []}
        session.add(score_set)
        session.commit()

        namespaces = [entry.namespace for entry in available_score_set_csv_namespaces(session, score_set)]

        assert "scores" in namespaces
        assert "counts" not in namespaces

    def test_calibration_namespaces_are_offered(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )

        namespaces = [entry.namespace for entry in available_score_set_csv_namespaces(session, variant.score_set)]

        assert CALIBRATION_NS_1 in namespaces

    def test_research_use_only_calibration_is_offered_unchecked(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session,
            variant.score_set,
            [variant],
            urn=CALIBRATION_URN_1,
            title="Provisional Calibration",
            research_use_only=True,
        )

        entry = next(
            entry
            for entry in available_score_set_csv_namespaces(session, variant.score_set)
            if entry.namespace == CALIBRATION_NS_1
        )

        assert entry.label == "Research Use Only: Provisional Calibration"
        assert entry.selected_by_default is False
        # Reported in its own right, not left to be inferred from the label or from the unchecked box:
        # this is the one reason for unchecking that decides whether the data may be published.
        assert entry.research_use_only is True

    def test_clinical_calibration_is_offered_checked(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )

        entry = next(
            entry
            for entry in available_score_set_csv_namespaces(session, variant.score_set)
            if entry.namespace == CALIBRATION_NS_1
        )

        assert entry.label == "Clinical Calibration"
        assert entry.selected_by_default is True
        assert entry.research_use_only is False
        assert entry.score_set.urn == variant.score_set.urn

    def test_rangeless_calibration_is_offered_unchecked(self, session, setup_lib_db_with_mapped_variant):
        """The score-set export covers the score set's own calibrations, so a rangeless one is still
        requestable — but it would contribute nothing except NA, so it must not open checked and must not
        reach the public dump, which takes only what discovery selects by default.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        _add_rangeless_calibration(session, variant.score_set, urn=CALIBRATION_URN_1, title="Baseline Only")

        entry = next(
            entry
            for entry in available_score_set_csv_namespaces(session, variant.score_set)
            if entry.namespace == CALIBRATION_NS_1
        )

        assert entry.label == "Baseline Only"
        assert entry.selected_by_default is False

    def test_variant_discovery_omits_a_rangeless_calibration_entirely(self, session, setup_lib_db_with_mapped_variant):
        """A variant's calibrations are scoped to what interprets this allele; one that interprets
        nothing is not a choice worth offering.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        _add_rangeless_calibration(session, variant.score_set, urn=CALIBRATION_URN_1, title="Baseline Only")

        namespaces = [entry.namespace for entry in available_variant_csv_namespaces(session, variant.urn)]

        assert CALIBRATION_NS_1 not in namespaces

    def test_clinvar_namespaces_are_offered_per_release(self, session, setup_lib_db_with_mapped_variant):
        mapped_variant = setup_lib_db_with_mapped_variant
        _add_clinvar_control(session, mapped_variant, "Likely benign", "single submitter", "11_2024")
        _add_clinvar_control(session, mapped_variant, "Pathogenic", "expert panel", "02_2025")

        namespaces = [
            entry.namespace for entry in available_score_set_csv_namespaces(session, mapped_variant.variant.score_set)
        ]

        assert "clinvar.2024_11" in namespaces
        assert "clinvar.2025_02" in namespaces

    def test_only_the_newest_clinvar_release_is_selected_by_default(self, session, setup_lib_db_with_mapped_variant):
        """MaveDB carries around ten releases; a picker opening with all of them checked is unusable."""
        mapped_variant = setup_lib_db_with_mapped_variant
        for db_version in ("11_2024", "02_2025", "06_2024"):
            _add_clinvar_control(session, mapped_variant, "Pathogenic", "expert panel", db_version)

        by_namespace = {
            entry.namespace: entry
            for entry in available_score_set_csv_namespaces(session, mapped_variant.variant.score_set)
        }

        assert by_namespace["clinvar.2025_02"].selected_by_default is True
        assert by_namespace["clinvar.2024_11"].selected_by_default is False
        assert by_namespace["clinvar.2024_06"].selected_by_default is False
        # The older releases are still on offer — comparing a call across releases is a real thing to want.
        assert len([ns for ns in by_namespace if ns.startswith("clinvar.")]) == 3

    def test_every_offered_namespace_is_valid(self, session, setup_lib_db_with_mapped_variant):
        """Discovery must only advertise namespaces the endpoints will actually accept."""
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )
        _add_clinvar_control(session, setup_lib_db_with_mapped_variant, "Pathogenic", "expert panel", "02_2025")

        entries = available_score_set_csv_namespaces(session, variant.score_set)

        assert entries
        assert all(is_valid_csv_namespace(entry.namespace) for entry in entries)
        # Every entry must also be presentable, or a picker has nothing to render.
        assert all(entry.label for entry in entries)
        assert all(entry.group for entry in entries)

    def test_entries_are_labeled_for_a_picker(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Brnich et al. 2019"
        )
        _add_clinvar_control(session, setup_lib_db_with_mapped_variant, "Pathogenic", "expert panel", "11_2024")

        by_namespace = {
            entry.namespace: entry for entry in available_score_set_csv_namespaces(session, variant.score_set)
        }

        # A calibration is named by its title, not its URN.
        assert by_namespace[CALIBRATION_NS_1].label == "Brnich et al. 2019"
        assert by_namespace[CALIBRATION_NS_1].group == "calibration"
        # A ClinVar release is named by its date.
        assert by_namespace["clinvar.2024_11"].label == "ClinVar significance (November 2024)"
        assert by_namespace["clinvar.2024_11"].group == "annotation"
        assert by_namespace["gnomad"].label == "gnomAD population frequency"
        assert by_namespace["score_set"].group == "provenance"


# ---------------------------------------------------------------------------
# TestAnchorMappingIsDeterministic
# ---------------------------------------------------------------------------


class TestAnchorMappingIsDeterministic:
    """Everything in the CSV follows from which current mapping anchors the request."""

    def test_repeat_downloads_agree_when_several_mappings_claim_to_be_current(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """Nothing in the schema stops two rows from being current, so the pick must not be arbitrary."""
        variant = setup_lib_db_with_mapped_variant.variant

        newer = MappedVariant(
            **{**TEST_MINIMAL_MAPPED_VARIANT, "mapped_date": date(2030, 1, 1), "clingen_allele_id": "CA_NEWER"},
            variant_id=variant.id,
        )
        session.add(newer)
        session.commit()

        first = _parse_csv(get_variant_csv(session, variant.urn, ["clingen"]))
        second = _parse_csv(get_variant_csv(session, variant.urn, ["clingen"]))

        assert first == second
        # The requested variant anchors the export and comes first, so this pins which mapping was
        # picked rather than merely whether the newer one appears anywhere in the output.
        assert first[0]["clingen.clingen_allele_id"] == "CA_NEWER"

    def test_a_variant_with_two_current_mappings_is_reported_once(self, session, setup_lib_db_with_mapped_variant):
        """Two current mappings on one variant are the same measurement twice, not two equivalents.

        Emitting both would also break the row ordering downstream, which restores the caller's order from
        the variant ids alone.
        """
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clingen_allele_id = "CA123456"
        session.add(mapped_variant)
        session.commit()

        variant = mapped_variant.variant
        session.add(
            MappedVariant(
                **{**TEST_MINIMAL_MAPPED_VARIANT, "mapped_date": date(2020, 1, 1), "clingen_allele_id": "CA123456"},
                variant_id=variant.id,
            )
        )
        session.commit()

        # A genuine equivalent in another score set, so the widening is doing something to dedupe within.
        _add_second_score_set_with_equivalent_variant(session, variant.score_set, "CA123456")

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert [row["accession"] for row in rows] == [variant.urn, "urn:mavedb:00000001-a-2#1"]

    def test_an_equivalent_variants_extra_current_mapping_is_reported_once(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """The same rule applies to the widened rows, not just to the anchor."""
        mapped_variant = setup_lib_db_with_mapped_variant
        mapped_variant.clingen_allele_id = "CA123456"
        session.add(mapped_variant)
        session.commit()

        variant = mapped_variant.variant
        _, other_variant, _ = _add_second_score_set_with_equivalent_variant(session, variant.score_set, "CA123456")
        session.add(
            MappedVariant(
                **{**TEST_MINIMAL_MAPPED_VARIANT, "mapped_date": date(2020, 1, 1), "clingen_allele_id": "CA123456"},
                variant_id=other_variant.id,
            )
        )
        session.commit()

        rows = _parse_csv(get_variant_csv(session, variant.urn))

        assert [row["accession"] for row in rows] == [variant.urn, other_variant.urn]


# ---------------------------------------------------------------------------
# TestScoreSetCsvCalibrationColumns
# ---------------------------------------------------------------------------


class TestScoreSetCsvCalibrationColumns:
    """The score-set CSV must fill the calibration columns its discovery advertises."""

    def test_calibration_columns_are_populated(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )

        rows = _parse_csv(
            get_score_set_variants_as_csv(session, variant.score_set, ["scores", CALIBRATION_NS_1], namespaced=True)
        )

        row = next(r for r in rows if r["accession"] == variant.urn)
        assert row[f"{CALIBRATION_NS_1}.title"] == "Clinical Calibration"
        assert row[f"{CALIBRATION_NS_1}.acmg_criterion"] == "PS3"
        assert row[f"{CALIBRATION_NS_1}.acmg_evidence_outcome_code"] == "PS3"
        assert row[f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "PATHOGENIC"
        assert row[f"{CALIBRATION_NS_1}.research_use_only"] == "False"

    def test_variant_outside_the_range_is_uncertain_not_blank(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(session, variant.score_set, [], urn=CALIBRATION_URN_1, title="Clinical")

        rows = _parse_csv(
            get_score_set_variants_as_csv(session, variant.score_set, ["scores", CALIBRATION_NS_1], namespaced=True)
        )

        row = next(r for r in rows if r["accession"] == variant.urn)
        assert row[f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "UNCERTAIN_SIGNIFICANCE"
        assert row[f"{CALIBRATION_NS_1}.acmg_evidence_outcome_code"] == "PS3_not_met"

    def test_rangeless_calibration_reports_its_identity_with_no_interpretation(
        self, session, setup_lib_db_with_mapped_variant
    ):
        """It cannot classify anything, but which calibration was consulted is still on the record.

        The public dump carries these namespaces, so a wholly-NA block would be the archive claiming to
        know less than the database does.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        _add_rangeless_calibration(session, variant.score_set, urn=CALIBRATION_URN_1, title="Baseline Only")

        rows = _parse_csv(
            get_score_set_variants_as_csv(session, variant.score_set, ["scores", CALIBRATION_NS_1], namespaced=True)
        )

        row = next(r for r in rows if r["accession"] == variant.urn)
        assert row[f"{CALIBRATION_NS_1}.title"] == "Baseline Only"
        assert row[f"{CALIBRATION_NS_1}.research_use_only"] == "False"
        assert row[f"{CALIBRATION_NS_1}.functional_classification"] == "NA"
        assert row[f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "NA"

    def test_everything_discovery_advertises_is_actually_populated(self, session, setup_lib_db_with_mapped_variant):
        """Discovery and the export must agree, or a dump ships documented but empty columns."""
        variant = setup_lib_db_with_mapped_variant.variant
        _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Clinical Calibration"
        )

        advertised = [
            entry.namespace
            for entry in available_score_set_csv_namespaces(session, variant.score_set)
            if entry.namespace.startswith("calibration.")
        ]
        assert advertised, "no calibration namespace advertised; the rest of this test proves nothing"

        rows = _parse_csv(
            get_score_set_variants_as_csv(session, variant.score_set, ["scores"] + advertised, namespaced=True)
        )
        row = next(r for r in rows if r["accession"] == variant.urn)

        for namespace in advertised:
            assert row[f"{namespace}.title"] != "NA", f"{namespace} advertised but its columns are empty"

    def test_counts_are_always_taken_in_full(self, session, setup_lib_db_with_mapped_variant):
        """Counts have no required column, so nothing narrows them the way ``scores`` is narrowed."""
        score_set = setup_lib_db_with_mapped_variant.variant.score_set
        score_set.dataset_columns = {"score_columns": ["score"], "count_columns": ["c_0"]}
        session.add(score_set)
        session.commit()

        csv_text = get_score_set_variants_as_csv(session, score_set, ["counts"], namespaced=True)

        assert "counts.c_0" in csv_text.splitlines()[0]


class TestPrivateCalibrationsAreNotDisclosed:
    """A calibration's READ permission is stricter than its score set's.

    Private ones are readable only by their owner, by contributors when investigator-provided, or by an
    admin, so reading the measurement does not entitle a caller to the interpretation.
    """

    @pytest.fixture
    def private_calibration(self, session, setup_lib_db_with_mapped_variant):
        variant = setup_lib_db_with_mapped_variant.variant
        calibration = _add_pathogenicity_calibration(
            session, variant.score_set, [variant], urn=CALIBRATION_URN_1, title="Unpublished Calibration"
        )
        calibration.private = True
        session.add(calibration)
        session.commit()
        return calibration

    def test_score_set_discovery_omits_it_by_default(self, session, private_calibration):
        namespaces = [
            entry.namespace for entry in available_score_set_csv_namespaces(session, private_calibration.score_set)
        ]

        assert CALIBRATION_NS_1 not in namespaces

    def test_variant_discovery_omits_it_by_default(
        self, session, setup_lib_db_with_mapped_variant, private_calibration
    ):
        variant = setup_lib_db_with_mapped_variant.variant

        namespaces = [entry.namespace for entry in available_variant_csv_namespaces(session, variant.urn)]

        assert CALIBRATION_NS_1 not in namespaces

    def test_naming_the_urn_directly_yields_no_interpretation(
        self, session, setup_lib_db_with_mapped_variant, private_calibration
    ):
        """Discovery is not the gate: a caller who knows the URN must still be refused the data."""
        variant = setup_lib_db_with_mapped_variant.variant

        rows = _parse_csv(get_variant_csv(session, variant.urn, ["scores", CALIBRATION_NS_1]))

        assert rows[0][f"{CALIBRATION_NS_1}.title"] == "NA"
        assert rows[0][f"{CALIBRATION_NS_1}.pathogenicity_classification"] == "NA"

    def test_score_set_csv_withholds_it_too(self, session, private_calibration):
        rows = _parse_csv(
            get_score_set_variants_as_csv(
                session, private_calibration.score_set, ["scores", CALIBRATION_NS_1], namespaced=True
            )
        )

        assert all(row[f"{CALIBRATION_NS_1}.title"] == "NA" for row in rows)

    def test_a_permitted_caller_still_receives_it(self, session, setup_lib_db_with_mapped_variant, private_calibration):
        """Viewer-scoped emission: the viewer widens access, it is not a blanket ban on private calibrations.

        Uses a real entitled viewer rather than an always-true stand-in, so this exercises the same
        ``ScoreCalibrationViewer`` rule the routers use.
        """
        variant = setup_lib_db_with_mapped_variant.variant
        admin = Principal(Mock(user=Mock(id=1, username="admin"), active_roles=[UserRole.admin]))

        rows = _parse_csv(
            get_variant_csv(
                session,
                variant.urn,
                ["scores", CALIBRATION_NS_1],
                viewer=admin.viewer_for(ScoreCalibrationViewer),
            )
        )

        assert rows[0][f"{CALIBRATION_NS_1}.title"] == "Unpublished Calibration"

    def test_the_public_export_never_carries_it(self, session, private_calibration):
        """The dump is built for an anonymous principal, so it never reaches a private calibration."""
        from mavedb.scripts.export_public_data import annotation_export_namespaces

        anonymous = Principal().viewer_for(ScoreCalibrationViewer)

        assert CALIBRATION_NS_1 not in annotation_export_namespaces(session, private_calibration.score_set, anonymous)


class TestScoreColumnNamespaces:
    """`scores` is the required column; `scores_custom` is the rest, emitted under the same prefix."""

    @pytest.fixture
    def score_set_with_custom_columns(self, session, setup_lib_db_with_mapped_variant):
        score_set = setup_lib_db_with_mapped_variant.variant.score_set
        score_set.dataset_columns = {"score_columns": ["score", "se"], "count_columns": []}
        session.add(score_set)
        session.commit()
        return score_set

    def test_scores_alone_emits_only_the_required_column(self, session, score_set_with_custom_columns):
        header = _parse_csv(
            get_score_set_variants_as_csv(session, score_set_with_custom_columns, ["scores"], namespaced=True)
        )[0]

        assert "scores.score" in header
        assert "scores.se" not in header

    def test_custom_columns_are_emitted_under_the_scores_prefix(self, session, score_set_with_custom_columns):
        """The published header must not change: `scores_custom` is a request token, not a column prefix."""
        header = _parse_csv(
            get_score_set_variants_as_csv(session, score_set_with_custom_columns, ["scores_custom"], namespaced=True)
        )[0]

        assert "scores.se" in header
        assert not any(column.startswith("scores_custom.") for column in header)

    def test_both_namespaces_reproduce_the_whole_score_group_in_order(self, session, score_set_with_custom_columns):
        header = list(
            _parse_csv(
                get_score_set_variants_as_csv(
                    session, score_set_with_custom_columns, ["scores", "scores_custom"], namespaced=True
                )
            )[0]
        )

        assert [column for column in header if column.startswith("scores.")] == ["scores.score", "scores.se"]

    def test_discovery_offers_custom_columns_only_when_there_are_any(
        self, session, score_set_with_custom_columns, setup_lib_db_with_mapped_variant
    ):
        offered = [
            entry.namespace for entry in available_score_set_csv_namespaces(session, score_set_with_custom_columns)
        ]
        assert "scores" in offered and "scores_custom" in offered

        score_set_with_custom_columns.dataset_columns = {"score_columns": ["score"], "count_columns": []}
        session.add(score_set_with_custom_columns)
        session.commit()

        offered = [
            entry.namespace for entry in available_score_set_csv_namespaces(session, score_set_with_custom_columns)
        ]
        assert "scores" in offered and "scores_custom" not in offered
