from copy import deepcopy

import pytest
from pydantic import ValidationError

from mavedb.lib.acmg import ACMGCriterion
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.view_models.score_calibration import (
    FunctionalRangeCreate,
    ScoreCalibration,
    ScoreCalibrationCreate,
    ScoreCalibrationWithScoreSetUrn,
)

from tests.helpers.constants import (
    TEST_FUNCTIONAL_RANGE_NORMAL,
    TEST_FUNCTIONAL_RANGE_ABNORMAL,
    TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
    TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY,
    TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY,
    TEST_BRNICH_SCORE_CALIBRATION,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_SAVED_BRNICH_SCORE_CALIBRATION,
    TEST_SAVED_PATHOGENICITY_SCORE_CALIBRATION,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict


##############################################################################
# Tests for FunctionalRange view models
##############################################################################


## Tests on models generated from dicts (e.g. request bodies)


@pytest.mark.parametrize(
    "functional_range",
    [
        TEST_FUNCTIONAL_RANGE_NORMAL,
        TEST_FUNCTIONAL_RANGE_ABNORMAL,
        TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
        TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY,
        TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY,
    ],
)
def test_can_create_valid_functional_range(functional_range):
    fr = FunctionalRangeCreate.model_validate(functional_range)

    assert fr.label == functional_range["label"]
    assert fr.description == functional_range.get("description")
    assert fr.classification == functional_range["classification"]
    assert fr.range == tuple(functional_range["range"])
    assert fr.inclusive_lower_bound == functional_range.get("inclusive_lower_bound", True)
    assert fr.inclusive_upper_bound == functional_range.get("inclusive_upper_bound", False)


def test_cannot_create_functional_range_with_reversed_range():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["range"] = (2, 1)
    with pytest.raises(ValidationError, match="The lower bound cannot exceed the upper bound."):
        FunctionalRangeCreate.model_validate(invalid_data)


def test_cannot_create_functional_range_with_equal_bounds():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["range"] = (1, 1)
    with pytest.raises(ValidationError, match="The lower and upper bounds cannot be identical."):
        FunctionalRangeCreate.model_validate(invalid_data)


def test_can_create_range_with_infinity_bounds():
    valid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    valid_data["inclusive_lower_bound"] = False
    valid_data["inclusive_upper_bound"] = False
    valid_data["range"] = (None, None)

    fr = FunctionalRangeCreate.model_validate(valid_data)
    assert fr.range == (None, None)


@pytest.mark.parametrize("ratio_property", ["oddspaths_ratio", "positive_likelihood_ratio"])
def test_cannot_create_functional_range_with_negative_ratios(ratio_property):
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data[ratio_property] = -1.0
    with pytest.raises(ValidationError, match="The ratio must be greater than or equal to 0."):
        FunctionalRangeCreate.model_validate(invalid_data)


def test_cannot_create_functional_range_with_inclusive_bounds_at_infinity():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY)
    invalid_data["inclusive_upper_bound"] = True
    with pytest.raises(ValidationError, match="An inclusive upper bound may not include positive infinity."):
        FunctionalRangeCreate.model_validate(invalid_data)

    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY)
    invalid_data["inclusive_lower_bound"] = True
    with pytest.raises(ValidationError, match="An inclusive lower bound may not include negative infinity."):
        FunctionalRangeCreate.model_validate(invalid_data)


@pytest.mark.parametrize(
    "functional_range, opposite_criterion",
    [(TEST_FUNCTIONAL_RANGE_NORMAL, ACMGCriterion.PS3), (TEST_FUNCTIONAL_RANGE_ABNORMAL, ACMGCriterion.BS3)],
)
def test_cannot_create_functional_range_when_classification_disagrees_with_acmg_criterion(
    functional_range, opposite_criterion
):
    invalid_data = deepcopy(functional_range)
    invalid_data["acmg_classification"]["criterion"] = opposite_criterion.value
    with pytest.raises(ValidationError, match="must agree with the functional range classification"):
        FunctionalRangeCreate.model_validate(invalid_data)


def test_none_type_classification_and_evidence_strength_count_as_agreement():
    valid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    valid_data["acmg_classification"] = {"criterion": None, "evidence_strength": None}

    fr = FunctionalRangeCreate.model_validate(valid_data)
    assert fr.acmg_classification.criterion is None
    assert fr.acmg_classification.evidence_strength is None


def test_cannot_create_functional_range_when_oddspaths_evidence_disagrees_with_classification():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    # Abnormal evidence strength for a normal range
    invalid_data["oddspaths_ratio"] = 350
    with pytest.raises(ValidationError, match="implies criterion"):
        FunctionalRangeCreate.model_validate(invalid_data)

    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_ABNORMAL)
    # Normal evidence strength for an abnormal range
    invalid_data["oddspaths_ratio"] = 0.1
    with pytest.raises(ValidationError, match="implies criterion"):
        FunctionalRangeCreate.model_validate(invalid_data)


def test_is_contained_by_range():
    fr = FunctionalRangeCreate.model_validate(
        {
            "label": "test range",
            "classification": "abnormal",
            "range": (0.0, 1.0),
            "inclusive_lower_bound": True,
            "inclusive_upper_bound": True,
        }
    )

    assert fr.is_contained_by_range(1.0), "1.0 (inclusive upper bound) should be contained in the range"
    assert fr.is_contained_by_range(0.0), "0.0 (inclusive lower bound) should be contained in the range"
    assert not fr.is_contained_by_range(-0.1), "values below lower bound should not be contained in the range"
    assert not fr.is_contained_by_range(5.0), "values above upper bound should not be contained in the range"

    fr.inclusive_lower_bound = False
    fr.inclusive_upper_bound = False

    assert not fr.is_contained_by_range(1.0), "1.0 (exclusive upper bound) should not be contained in the range"
    assert not fr.is_contained_by_range(0.0), "0.0 (exclusive lower bound) should not be contained in the range"


##############################################################################
# Tests for ScoreCalibration view models
##############################################################################

# Tests on models generated from dicts (e.g. request bodies)


@pytest.mark.parametrize(
    "valid_calibration",
    [TEST_BRNICH_SCORE_CALIBRATION, TEST_PATHOGENICITY_SCORE_CALIBRATION],
)
def test_can_create_valid_score_calibration(valid_calibration):
    sc = ScoreCalibrationCreate.model_validate(valid_calibration)

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("research_use_only", False)
    assert sc.baseline_score == valid_calibration.get("baseline_score")
    assert sc.baseline_score_description == valid_calibration.get("baseline_score_description")

    if valid_calibration.get("functional_ranges") is not None:
        assert len(sc.functional_ranges) == len(valid_calibration["functional_ranges"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_ranges is None

    if valid_calibration.get("threshold_sources") is not None:
        assert len(sc.threshold_sources) == len(valid_calibration["threshold_sources"])
        for pub in valid_calibration["threshold_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.threshold_sources]
    else:
        assert sc.threshold_sources is None

    if valid_calibration.get("classification_sources") is not None:
        assert len(sc.classification_sources) == len(valid_calibration["classification_sources"])
        for pub in valid_calibration["classification_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.classification_sources]
    else:
        assert sc.classification_sources is None

    if valid_calibration.get("method_sources") is not None:
        assert len(sc.method_sources) == len(valid_calibration["method_sources"])
        for pub in valid_calibration["method_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.method_sources]
    else:
        assert sc.method_sources is None

    if valid_calibration.get("calibration_metadata") is not None:
        assert sc.calibration_metadata == valid_calibration["calibration_metadata"]
    else:
        assert sc.calibration_metadata is None


# Making an exception to usually not testing the ability to create models without optional fields,
# because of the large number of model validators that need to play nice with this case.
@pytest.mark.parametrize(
    "valid_calibration",
    [TEST_BRNICH_SCORE_CALIBRATION, TEST_PATHOGENICITY_SCORE_CALIBRATION],
)
def test_can_create_valid_score_calibration_without_functional_ranges(valid_calibration):
    valid_calibration = deepcopy(valid_calibration)
    valid_calibration["functional_ranges"] = None

    sc = ScoreCalibrationCreate.model_validate(valid_calibration)

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("research_use_only", False)
    assert sc.baseline_score == valid_calibration.get("baseline_score")
    assert sc.baseline_score_description == valid_calibration.get("baseline_score_description")

    if valid_calibration.get("functional_ranges") is not None:
        assert len(sc.functional_ranges) == len(valid_calibration["functional_ranges"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_ranges is None

    if valid_calibration.get("threshold_sources") is not None:
        assert len(sc.threshold_sources) == len(valid_calibration["threshold_sources"])
        for pub in valid_calibration["threshold_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.threshold_sources]
    else:
        assert sc.threshold_sources is None

    if valid_calibration.get("classification_sources") is not None:
        assert len(sc.classification_sources) == len(valid_calibration["classification_sources"])
        for pub in valid_calibration["classification_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.classification_sources]
    else:
        assert sc.classification_sources is None

    if valid_calibration.get("method_sources") is not None:
        assert len(sc.method_sources) == len(valid_calibration["method_sources"])
        for pub in valid_calibration["method_sources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.method_sources]
    else:
        assert sc.method_sources is None

    if valid_calibration.get("calibration_metadata") is not None:
        assert sc.calibration_metadata == valid_calibration["calibration_metadata"]
    else:
        assert sc.calibration_metadata is None


def test_cannot_create_score_calibration_when_ranges_overlap():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    # Make the first two ranges overlap
    invalid_data["functional_ranges"][0]["range"] = [1.0, 3.0]
    invalid_data["functional_ranges"][1]["range"] = [2.0, 4.0]
    with pytest.raises(ValidationError, match="Score ranges may not overlap; `"):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_cannot_create_score_calibration_when_ranges_touch_with_inclusive_ranges():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    # Make the first two ranges touch
    invalid_data["functional_ranges"][0]["range"] = [1.0, 2.0]
    invalid_data["functional_ranges"][1]["range"] = [2.0, 4.0]
    invalid_data["functional_ranges"][0]["inclusive_upper_bound"] = True
    with pytest.raises(ValidationError, match="Score ranges may not overlap; `"):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_cannot_create_score_calibration_with_duplicate_range_labels():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    # Make the first two ranges have the same label
    invalid_data["functional_ranges"][0]["label"] = "duplicate label"
    invalid_data["functional_ranges"][1]["label"] = "duplicate label"
    with pytest.raises(ValidationError, match="Functional range labels must be unique"):
        ScoreCalibrationCreate.model_validate(invalid_data)


# Making an exception to usually not testing the ability to create models without optional fields,
# since model validators sometimes rely on their absence.
def test_can_create_score_calibration_without_baseline_score():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    valid_data["baseline_score"] = None

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score is None


def test_can_create_score_calibration_with_baseline_score_when_outside_all_ranges():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    valid_data["baseline_score"] = 10.0

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score == 10.0


def test_can_create_score_calibration_with_baseline_score_when_inside_normal_range():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    valid_data["baseline_score"] = 3.0

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score == 3.0


def test_cannot_create_score_calibration_with_baseline_score_when_inside_non_normal_range():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION)
    invalid_data["baseline_score"] = -3.0
    with pytest.raises(ValueError, match="Baseline scores may not fall within non-normal ranges"):
        ScoreCalibrationCreate.model_validate(invalid_data)


# Tests on models generated from attributed objects (e.g. ORM models)


@pytest.mark.parametrize(
    "valid_calibration",
    [TEST_SAVED_BRNICH_SCORE_CALIBRATION, TEST_SAVED_PATHOGENICITY_SCORE_CALIBRATION],
)
def test_can_create_valid_score_calibration_from_attributed_object(valid_calibration):
    sc = ScoreCalibration.model_validate(dummy_attributed_object_from_dict(valid_calibration))

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("researchUseOnly", False)
    assert sc.primary == valid_calibration.get("primary", True)
    assert sc.investigator_provided == valid_calibration.get("investigatorProvided", False)
    assert sc.baseline_score == valid_calibration.get("baselineScore")
    assert sc.baseline_score_description == valid_calibration.get("baselineScoreDescription")

    if valid_calibration.get("functionalRanges") is not None:
        assert len(sc.functional_ranges) == len(valid_calibration["functionalRanges"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_ranges is None

    if valid_calibration.get("thresholdSources") is not None:
        assert len(sc.threshold_sources) == len(valid_calibration["thresholdSources"])
        for pub in valid_calibration["thresholdSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.threshold_sources]
    else:
        assert sc.threshold_sources is None

    if valid_calibration.get("classificationSources") is not None:
        assert len(sc.classification_sources) == len(valid_calibration["classificationSources"])
        for pub in valid_calibration["classificationSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.classification_sources]
    else:
        assert sc.classification_sources is None

    if valid_calibration.get("methodSources") is not None:
        assert len(sc.method_sources) == len(valid_calibration["methodSources"])
        for pub in valid_calibration["methodSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.method_sources]
    else:
        assert sc.method_sources is None

    if valid_calibration.get("calibrationMetadata") is not None:
        assert sc.calibration_metadata == valid_calibration["calibrationMetadata"]
    else:
        assert sc.calibration_metadata is None


def test_cannot_create_score_calibration_when_publication_information_is_missing():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION)
    # Add publication identifiers with missing information
    invalid_data.pop("thresholdSources", None)
    invalid_data.pop("classificationSources", None)
    invalid_data.pop("methodSources", None)
    with pytest.raises(ValidationError, match="Unable to create ScoreCalibration without attribute"):
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_can_create_score_calibration_from_association_style_publication_identifiers_against_attributed_object():
    orig_data = TEST_SAVED_BRNICH_SCORE_CALIBRATION
    data = deepcopy(orig_data)

    threshold_sources = [
        dummy_attributed_object_from_dict({"publication": pub, "relation": ScoreCalibrationRelation.threshold})
        for pub in data.pop("thresholdSources", [])
    ]
    classification_sources = [
        dummy_attributed_object_from_dict({"publication": pub, "relation": ScoreCalibrationRelation.classification})
        for pub in data.pop("classificationSources", [])
    ]
    method_sources = [
        dummy_attributed_object_from_dict({"publication": pub, "relation": ScoreCalibrationRelation.method})
        for pub in data.pop("methodSources", [])
    ]

    # Simulate ORM model by adding required fields that would originate from the DB
    data["publication_identifier_associations"] = threshold_sources + classification_sources + method_sources
    data["id"] = 1
    data["score_set_id"] = 1

    sc = ScoreCalibration.model_validate(dummy_attributed_object_from_dict(data))

    assert sc.title == orig_data["title"]
    assert sc.research_use_only == orig_data.get("researchUseOnly", False)
    assert sc.primary == orig_data.get("primary", False)
    assert sc.investigator_provided == orig_data.get("investigatorProvided", False)
    assert sc.baseline_score == orig_data.get("baselineScore")
    assert sc.baseline_score_description == orig_data.get("baselineScoreDescription")

    if orig_data.get("functionalRanges") is not None:
        assert len(sc.functional_ranges) == len(orig_data["functionalRanges"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_ranges is None

    if orig_data.get("thresholdSources") is not None:
        assert len(sc.threshold_sources) == len(orig_data["thresholdSources"])
        for pub in orig_data["thresholdSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.threshold_sources]
    else:
        assert sc.threshold_sources is None

    if orig_data.get("classificationSources") is not None:
        assert len(sc.classification_sources) == len(orig_data["classificationSources"])
        for pub in orig_data["classificationSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.classification_sources]
    else:
        assert sc.classification_sources is None

    if orig_data.get("methodSources") is not None:
        assert len(sc.method_sources) == len(orig_data["methodSources"])
        for pub in orig_data["methodSources"]:
            assert pub["identifier"] in [rs.identifier for rs in sc.method_sources]
    else:
        assert sc.method_sources is None

    if orig_data.get("calibrationMetadata") is not None:
        assert sc.calibration_metadata == orig_data["calibrationMetadata"]
    else:
        assert sc.calibration_metadata is None


def test_primary_score_calibration_cannot_be_research_use_only():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION)
    invalid_data["primary"] = True
    invalid_data["researchUseOnly"] = True
    with pytest.raises(ValidationError, match="Primary score calibrations may not be marked as research use only"):
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_primary_score_calibration_cannot_be_private():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION)
    invalid_data["primary"] = True
    invalid_data["private"] = True
    with pytest.raises(ValidationError, match="Primary score calibrations may not be marked as private"):
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_score_calibration_with_score_set_urn_can_be_created_from_attributed_object():
    data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION)
    data["score_set"] = dummy_attributed_object_from_dict({"urn": "urn:mavedb:00000000-0000-0000-0000-000000000001"})

    sc = ScoreCalibrationWithScoreSetUrn.model_validate(dummy_attributed_object_from_dict(data))

    assert sc.title == data["title"]
    assert sc.score_set_urn == data["score_set"].urn


def test_score_calibration_with_score_set_urn_cannot_be_created_without_score_set_urn():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION)
    invalid_data["score_set"] = dummy_attributed_object_from_dict({})
    with pytest.raises(ValidationError, match="Unable to create ScoreCalibrationWithScoreSetUrn without attribute"):
        ScoreCalibrationWithScoreSetUrn.model_validate(dummy_attributed_object_from_dict(invalid_data))
