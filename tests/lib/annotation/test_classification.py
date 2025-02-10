import pytest

from humps import camelize
from sqlalchemy import select
from mavedb.lib.annotation.classification import functional_classification_of_variant
from mavedb.models.variant import Variant as VariantDbModel
from mavedb.models.mapped_variant import MappedVariant as MappedVariantDbModel
from mavedb.models.score_set import ScoreSet as ScoreSetDbModel
from ga4gh.va_spec.profiles.assay_var_effect import AveFunctionalClassification, AveClinicalClassification
from mavedb.lib.annotation.classification import pillar_project_clinical_classification_of_variant

from tests.helpers.constants import (
    TEST_SCORE_CALIBRATION,
    TEST_SCORE_SET_RANGE,
)
from tests.helpers.dependency_overrider import DependencyOverrider
from tests.helpers.util import add_thresholds_to_score_set, create_experiment, create_seq_score_set_with_mapped_variants


@pytest.mark.parametrize(
    "file_path,expected_classification",
    [
        ("scores_indeterminate.csv", AveFunctionalClassification.INDETERMINATE),
        ("scores_normal.csv", AveFunctionalClassification.NORMAL),
        ("scores_abnormal.csv", AveFunctionalClassification.ABNORMAL),
    ],
)
def test_functional_classification_of_variant_with_ranges(
    client, session, data_provider, data_files, setup_lib_db, file_path, expected_classification
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / file_path,
        update={"scoreRanges": camelize(TEST_SCORE_SET_RANGE)},
    )

    mapped_variant = session.scalar(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set["urn"])
    )
    assert mapped_variant

    result = functional_classification_of_variant(mapped_variant)
    assert result == expected_classification


def test_functional_classification_of_variant_without_ranges(client, session, data_provider, data_files, setup_lib_db):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores_normal.csv",
    )

    mapped_variant = session.scalar(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set["urn"])
    )
    assert mapped_variant

    result = functional_classification_of_variant(mapped_variant)
    assert result is None


@pytest.mark.parametrize(
    "file_path,expected_classification",
    [
        ("scores_no_evidence.csv", None),
        ("scores_b53_supporting.csv", AveClinicalClassification.BS3_SUPPORTING),
        ("scores_p53_supporting.csv", AveClinicalClassification.PS3_SUPPORTING),
        ("scores_b53_moderate.csv", AveClinicalClassification.BS3_MODERATE),
        ("scores_p53_moderate.csv", AveClinicalClassification.PS3_MODERATE),
        ("scores_b53_strong.csv", AveClinicalClassification.BS3_STRONG),
        ("scores_p53_strong.csv", AveClinicalClassification.PS3_STRONG),
        ("scores_b53_very_strong.csv", AveClinicalClassification.BS3_STRONG),
        ("scores_p53_very_strong.csv", AveClinicalClassification.PS3_STRONG),
    ],
)
def test_clinical_classification_of_variant_with_thresholds(
    client, session, data_provider, data_files, setup_lib_db, admin_app_overrides, file_path, expected_classification
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / file_path,
    )

    with DependencyOverrider(admin_app_overrides):
        add_thresholds_to_score_set(client, score_set["urn"], TEST_SCORE_CALIBRATION)

    mapped_variant = session.scalar(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set["urn"])
    )
    assert mapped_variant

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result == expected_classification


def test_clinical_classification_of_variant_without_thresholds(
    client, session, data_provider, data_files, setup_lib_db
):
    experiment = create_experiment(client)
    score_set = create_seq_score_set_with_mapped_variants(
        client,
        session,
        data_provider,
        experiment["urn"],
        data_files / "scores_p53_supporting.csv",
    )

    mapped_variant = session.scalar(
        select(MappedVariantDbModel)
        .join(VariantDbModel)
        .join(ScoreSetDbModel)
        .filter(ScoreSetDbModel.urn == score_set["urn"])
    )
    assert mapped_variant

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result is None
