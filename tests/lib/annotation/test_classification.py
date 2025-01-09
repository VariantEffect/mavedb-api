from mavedb.lib.annotation.classification import functional_classification_of_variant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.view_models.score_set import ScoreSet, ScoreRanges, ScoreRange
from ga4gh.va_spec.profiles.assay_var_effect import AveFunctionalClassification, AveClinicalClassification
from mavedb.lib.annotation.classification import pillar_project_clinical_classification_of_variant


def test_functional_classification_of_variant_normal():
    score_ranges = ScoreRanges(ranges=[ScoreRange(range=[0, 1], classification="normal")])
    score_set = ScoreSet(score_ranges=score_ranges)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 0.5}}})

    result = functional_classification_of_variant(mapped_variant)
    assert result == AveFunctionalClassification.NORMAL


def test_functional_classification_of_variant_abnormal():
    score_ranges = ScoreRanges(ranges=[ScoreRange(range=[0, 1], classification="abnormal")])
    score_set = ScoreSet(score_ranges=score_ranges)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 0.5}}})

    result = functional_classification_of_variant(mapped_variant)
    assert result == AveFunctionalClassification.ABNORMAL


def test_functional_classification_of_variant_indeterminate():
    score_ranges = ScoreRanges(ranges=[ScoreRange(range=[0, 1], classification="normal")])
    score_set = ScoreSet(score_ranges=score_ranges)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 1.5}}})

    result = functional_classification_of_variant(mapped_variant)
    assert result == AveFunctionalClassification.INDETERMINATE


def test_functional_classification_of_variant_no_score_ranges():
    score_set = ScoreSet(score_ranges=None)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 0.5}}})

    result = functional_classification_of_variant(mapped_variant)
    assert result is None


def test_pillar_project_clinical_classification_of_variant_supporting():
    score_calibrations = {"pillar_project": {"thresholds": [0.5, 1.5], "evidence_strengths": [1, 2]}}
    score_set = ScoreSet(score_calibrations=score_calibrations)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 0.75}}})

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result == AveClinicalClassification.PS3_SUPPORTING


def test_pillar_project_clinical_classification_of_variant_moderate():
    score_calibrations = {"pillar_project": {"thresholds": [0.5, 1.5], "evidence_strengths": [2, 3]}}
    score_set = ScoreSet(score_calibrations=score_calibrations)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 1.75}}})

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result == AveClinicalClassification.PS3_MODERATE


def test_pillar_project_clinical_classification_of_variant_strong():
    score_calibrations = {"pillar_project": {"thresholds": [0.5, 1.5], "evidence_strengths": [3, 4]}}
    score_set = ScoreSet(score_calibrations=score_calibrations)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 2.0}}})

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result == AveClinicalClassification.PS3_STRONG


def test_pillar_project_clinical_classification_of_variant_no_calibrations():
    score_set = ScoreSet(score_calibrations=None)
    mapped_variant = MappedVariant(variant={"score_set": score_set, "data": {"score_data": {"score": 0.75}}})

    result = pillar_project_clinical_classification_of_variant(mapped_variant)
    assert result is None
