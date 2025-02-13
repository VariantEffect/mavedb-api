from unittest.mock import patch
from ga4gh.va_spec.profiles import (
    AssayVariantEffectMeasurementStudyResult,
    AssayVariantEffectFunctionalClassificationStatement,
    AssayVariantEffectClinicalClassificationStatement,
)
from ga4gh.vrs.models import MolecularVariation
from ga4gh.va_spec.profiles.assay_var_effect import AveFunctionalClassification, AveClinicalClassification

from mavedb.lib.annotation.dataset import score_set_to_data_set
from mavedb.lib.annotation.document import variant_to_document, experiment_as_iri, score_set_to_document
from mavedb.lib.annotation.contribution import (
    mavedb_api_contribution,
    mavedb_vrs_contribution,
    mavedb_creator_contribution,
    mavedb_modifier_contribution,
)
from mavedb.lib.annotation.variant import (
    mapped_variant_to_variant_effect_measurement_study_result,
    mapped_variant_to_functional_classification_statement,
    mapped_variant_to_clinical_classification_statement,
    annotation_for_variant,
)

from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE


def test_mapped_variant_to_variant_effect_measurement_study_result(mock_mapped_variant):
    result = mapped_variant_to_variant_effect_measurement_study_result(mock_mapped_variant)

    assert isinstance(result, AssayVariantEffectMeasurementStudyResult)
    assert result.score == 1.0
    assert result.focusVariant == MolecularVariation(**TEST_VALID_POST_MAPPED_VRS_ALLELE)
    assert len(result.sourceDataSet) > 0
    assert score_set_to_data_set(mock_mapped_variant.variant.score_set) in result.sourceDataSet
    assert len(result.reportedIn) > 0
    assert variant_to_document(mock_mapped_variant.variant) in result.reportedIn
    assert len(result.contributions) > 0
    assert mavedb_api_contribution() in result.contributions
    assert mavedb_vrs_contribution(mock_mapped_variant) in result.contributions


def test_mapped_variant_to_functional_classification_statement(mock_mapped_variant):
    classification = AveFunctionalClassification.NORMAL
    with patch("mavedb.lib.annotation.variant.functional_classification_of_variant", return_value=classification):
        result = mapped_variant_to_functional_classification_statement(mock_mapped_variant)

    assert isinstance(result, AssayVariantEffectFunctionalClassificationStatement)
    assert result.classification == classification
    assert result.subjectVariant == MolecularVariation(**TEST_VALID_POST_MAPPED_VRS_ALLELE)
    assert experiment_as_iri(mock_mapped_variant.variant.score_set.experiment) == result.objectAssay
    assert len(result.reportedIn) > 0
    assert variant_to_document(mock_mapped_variant.variant) in result.reportedIn
    assert score_set_to_document(mock_mapped_variant.variant.score_set) in result.reportedIn
    assert len(result.contributions) > 0
    assert mavedb_api_contribution() in result.contributions
    assert mavedb_vrs_contribution(mock_mapped_variant) in result.contributions
    assert (
        mavedb_creator_contribution(mock_mapped_variant.variant, mock_mapped_variant.variant.score_set.created_by)
        in result.contributions
    )
    assert (
        mavedb_creator_contribution(
            mock_mapped_variant.variant.score_set, mock_mapped_variant.variant.score_set.created_by
        )
        in result.contributions
    )
    assert (
        mavedb_modifier_contribution(
            mock_mapped_variant.variant.score_set, mock_mapped_variant.variant.score_set.modified_by
        )
        in result.contributions
    )


def test_mapped_variant_to_functional_classification_statement_classification_is_none(mock_mapped_variant):
    with patch("mavedb.lib.annotation.variant.functional_classification_of_variant", return_value=None):
        result = mapped_variant_to_functional_classification_statement(mock_mapped_variant)
        assert result is None


def test_mapped_variant_to_clinical_classification_statement(mock_mapped_variant):
    classification = AveClinicalClassification.BS3_MODERATE
    with patch(
        "mavedb.lib.annotation.variant.pillar_project_clinical_classification_of_variant",
        return_value=classification,
    ):
        result = mapped_variant_to_clinical_classification_statement(mock_mapped_variant)

    assert isinstance(result, AssayVariantEffectClinicalClassificationStatement)
    assert result.classification == classification
    assert result.subjectVariant == MolecularVariation(**TEST_VALID_POST_MAPPED_VRS_ALLELE)
    assert experiment_as_iri(mock_mapped_variant.variant.score_set.experiment) == result.objectAssay
    assert len(result.reportedIn) > 0
    assert variant_to_document(mock_mapped_variant.variant) in result.reportedIn
    assert score_set_to_document(mock_mapped_variant.variant.score_set) in result.reportedIn
    assert len(result.contributions) > 0
    assert mavedb_api_contribution() in result.contributions
    assert mavedb_vrs_contribution(mock_mapped_variant) in result.contributions
    assert (
        mavedb_creator_contribution(mock_mapped_variant.variant, mock_mapped_variant.variant.score_set.created_by)
        in result.contributions
    )
    assert (
        mavedb_creator_contribution(
            mock_mapped_variant.variant.score_set, mock_mapped_variant.variant.score_set.created_by
        )
        in result.contributions
    )
    assert (
        mavedb_modifier_contribution(
            mock_mapped_variant.variant.score_set, mock_mapped_variant.variant.score_set.modified_by
        )
        in result.contributions
    )


def test_mapped_variant_to_clinical_classification_statement_classification_is_none(mock_mapped_variant):
    with patch("mavedb.lib.annotation.variant.pillar_project_clinical_classification_of_variant", return_value=None):
        result = mapped_variant_to_clinical_classification_statement(mock_mapped_variant)
        assert result is None


def test_annotation_for_variant(mock_mapped_variant):
    functional_classification = AveFunctionalClassification.ABNORMAL
    clinical_classification = AveClinicalClassification.PS3_MODERATE
    with (
        patch(
            "mavedb.lib.annotation.variant.functional_classification_of_variant",
            return_value=functional_classification,
        ),
        patch(
            "mavedb.lib.annotation.variant.pillar_project_clinical_classification_of_variant",
            return_value=clinical_classification,
        ),
    ):
        result = annotation_for_variant(mock_mapped_variant)
        assert isinstance(result, dict)
        assert result[mock_mapped_variant.variant.urn].AssayVariantEffectMeasurementStudyResult.score == 1.0
        assert (
            result[mock_mapped_variant.variant.urn].AssayVariantEffectFunctionalClassificationStatement.classification
            == functional_classification
        )
        assert (
            result[mock_mapped_variant.variant.urn].AssayVariantEffectClinicalClassificationStatement.classification
            == clinical_classification
        )


def test_annotation_for_variant_with_no_classifications(mock_mapped_variant):
    with (
        patch(
            "mavedb.lib.annotation.variant.functional_classification_of_variant",
            return_value=None,
        ),
        patch(
            "mavedb.lib.annotation.variant.pillar_project_clinical_classification_of_variant",
            return_value=None,
        ),
    ):
        result = annotation_for_variant(mock_mapped_variant)
        assert isinstance(result, dict)
        assert result[mock_mapped_variant.variant.urn].AssayVariantEffectMeasurementStudyResult.score == 1.0
        assert result[mock_mapped_variant.variant.urn].AssayVariantEffectFunctionalClassificationStatement is None
        assert result[mock_mapped_variant.variant.urn].AssayVariantEffectClinicalClassificationStatement is None
