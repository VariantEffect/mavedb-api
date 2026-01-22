from copy import deepcopy

import pytest
from pydantic import ValidationError

from mavedb.lib.acmg import ACMGCriterion
from mavedb.models.enums.functional_classification import FunctionalClassification as FunctionalClassificationOptions
from mavedb.models.enums.score_calibration_relation import ScoreCalibrationRelation
from mavedb.view_models.score_calibration import (
    FunctionalClassificationCreate,
    ScoreCalibration,
    ScoreCalibrationCreate,
    ScoreCalibrationWithScoreSetUrn,
)
from tests.helpers.constants import (
    TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
    TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_FUNCTIONAL_CLASSIFICATION_ABNORMAL,
    TEST_FUNCTIONAL_CLASSIFICATION_NORMAL,
    TEST_FUNCTIONAL_CLASSIFICATION_NOT_SPECIFIED,
    TEST_FUNCTIONAL_RANGE_ABNORMAL,
    TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY,
    TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY,
    TEST_FUNCTIONAL_RANGE_NORMAL,
    TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
    TEST_PATHOGENICITY_SCORE_CALIBRATION,
    TEST_SAVED_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
    TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
    TEST_SAVED_PATHOGENICITY_SCORE_CALIBRATION,
)
from tests.helpers.util.common import dummy_attributed_object_from_dict

##############################################################################
# Tests for FunctionalClassification view models
##############################################################################


## Tests on models generated from dicts (e.g. request bodies)


@pytest.mark.parametrize(
    "functional_classification",
    [
        TEST_FUNCTIONAL_RANGE_NORMAL,
        TEST_FUNCTIONAL_RANGE_ABNORMAL,
        TEST_FUNCTIONAL_RANGE_NOT_SPECIFIED,
        TEST_FUNCTIONAL_CLASSIFICATION_NORMAL,
        TEST_FUNCTIONAL_CLASSIFICATION_ABNORMAL,
        TEST_FUNCTIONAL_CLASSIFICATION_NOT_SPECIFIED,
        TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY,
        TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY,
    ],
)
def test_can_create_valid_functional_classification(functional_classification):
    fr = FunctionalClassificationCreate.model_validate(functional_classification)

    assert fr.label == functional_classification["label"]
    assert fr.description == functional_classification.get("description")
    assert fr.functional_classification.value == functional_classification["functional_classification"]
    assert fr.inclusive_lower_bound == functional_classification.get("inclusive_lower_bound")
    assert fr.inclusive_upper_bound == functional_classification.get("inclusive_upper_bound")

    if "range" in functional_classification:
        assert fr.range == tuple(functional_classification["range"])
        assert fr.range_based is True
        assert fr.class_based is False
    elif "class" in functional_classification:
        assert fr.class_ == functional_classification["class"]
        assert fr.range_based is False
        assert fr.class_based is True


@pytest.mark.parametrize(
    "property_name",
    [
        "label",
        "class",
    ],
)
def test_cannot_create_functional_classification_when_string_fields_empty(property_name):
    invalid_data = deepcopy(TEST_FUNCTIONAL_CLASSIFICATION_NORMAL)
    invalid_data[property_name] = "   "
    with pytest.raises(ValidationError, match="This field may not be empty or contain only whitespace."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_cannot_create_functional_classification_without_range_or_class():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["range"] = None
    invalid_data["class"] = None
    with pytest.raises(ValidationError, match="A functional range must specify either a numeric range or a class."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_cannot_create_functional_classification_with_both_range_and_class():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["class"] = "some_class"
    with pytest.raises(ValidationError, match="A functional range may not specify both a numeric range and a class."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_cannot_create_functional_classification_with_reversed_range():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["range"] = (2, 1)
    with pytest.raises(ValidationError, match="The lower bound cannot exceed the upper bound."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_cannot_create_functional_classification_with_equal_bounds():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data["range"] = (1, 1)
    with pytest.raises(ValidationError, match="The lower and upper bounds cannot be identical."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_can_create_range_with_infinity_bounds():
    valid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    valid_data["inclusive_lower_bound"] = False
    valid_data["inclusive_upper_bound"] = False
    valid_data["range"] = (None, None)

    fr = FunctionalClassificationCreate.model_validate(valid_data)
    assert fr.range == (None, None)


@pytest.mark.parametrize("ratio_property", ["oddspaths_ratio", "positive_likelihood_ratio"])
def test_cannot_create_functional_classification_with_negative_ratios(ratio_property):
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    invalid_data[ratio_property] = -1.0
    with pytest.raises(ValidationError, match="The ratio must be greater than or equal to 0."):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_cannot_create_functional_classification_with_inclusive_bounds_at_infinity():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_INCLUDING_POSITIVE_INFINITY)
    invalid_data["inclusive_upper_bound"] = True
    with pytest.raises(ValidationError, match="An inclusive upper bound may not include positive infinity."):
        FunctionalClassificationCreate.model_validate(invalid_data)

    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_INCLUDING_NEGATIVE_INFINITY)
    invalid_data["inclusive_lower_bound"] = True
    with pytest.raises(ValidationError, match="An inclusive lower bound may not include negative infinity."):
        FunctionalClassificationCreate.model_validate(invalid_data)


@pytest.mark.parametrize(
    "functional_classification, opposite_criterion",
    [(TEST_FUNCTIONAL_RANGE_NORMAL, ACMGCriterion.PS3), (TEST_FUNCTIONAL_RANGE_ABNORMAL, ACMGCriterion.BS3)],
)
def test_cannot_create_functional_classification_when_classification_disagrees_with_acmg_criterion(
    functional_classification, opposite_criterion
):
    invalid_data = deepcopy(functional_classification)
    invalid_data["acmg_classification"]["criterion"] = opposite_criterion.value
    with pytest.raises(ValidationError, match="must agree with the functional range classification"):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_none_type_classification_and_evidence_strength_count_as_agreement():
    valid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    valid_data["acmg_classification"] = {"criterion": None, "evidence_strength": None}

    fr = FunctionalClassificationCreate.model_validate(valid_data)
    assert fr.acmg_classification.criterion is None
    assert fr.acmg_classification.evidence_strength is None


def test_cannot_create_functional_classification_when_oddspaths_evidence_disagrees_with_classification():
    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_NORMAL)
    # Abnormal evidence strength for a normal range
    invalid_data["oddspaths_ratio"] = 350
    with pytest.raises(ValidationError, match="implies criterion"):
        FunctionalClassificationCreate.model_validate(invalid_data)

    invalid_data = deepcopy(TEST_FUNCTIONAL_RANGE_ABNORMAL)
    # Normal evidence strength for an abnormal range
    invalid_data["oddspaths_ratio"] = 0.1
    with pytest.raises(ValidationError, match="implies criterion"):
        FunctionalClassificationCreate.model_validate(invalid_data)


def test_is_contained_by_range():
    fr = FunctionalClassificationCreate.model_validate(
        {
            "label": "test range",
            "functional_classification": FunctionalClassificationOptions.abnormal,
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


def test_inclusive_bounds_get_default_when_unset_and_range_exists():
    fr = FunctionalClassificationCreate.model_validate(
        {
            "label": "test range",
            "functional_classification": FunctionalClassificationOptions.abnormal,
            "range": (0.0, 1.0),
        }
    )

    assert fr.inclusive_lower_bound is True, "inclusive_lower_bound should default to True"
    assert fr.inclusive_upper_bound is False, "inclusive_upper_bound should default to False"


def test_inclusive_bounds_remain_none_when_range_is_none():
    fr = FunctionalClassificationCreate.model_validate(
        {
            "label": "test range",
            "functional_classification": FunctionalClassificationOptions.abnormal,
            "class": "some_class",
        }
    )

    assert fr.inclusive_lower_bound is None, "inclusive_lower_bound should remain None"
    assert fr.inclusive_upper_bound is None, "inclusive_upper_bound should remain None"


@pytest.mark.parametrize(
    "bound_property, bound_value, match_text",
    [
        (
            "inclusive_lower_bound",
            True,
            "An inclusive lower bound may not be set on a class based functional classification.",
        ),
        (
            "inclusive_upper_bound",
            True,
            "An inclusive upper bound may not be set on a class based functional classification.",
        ),
    ],
)
def test_cant_set_inclusive_bounds_when_range_is_none(bound_property, bound_value, match_text):
    invalid_data = {
        "label": "test range",
        "functional_classification": FunctionalClassificationOptions.abnormal,
        "class": "some_class",
        bound_property: bound_value,
    }
    with pytest.raises(ValidationError, match=match_text):
        FunctionalClassificationCreate.model_validate(invalid_data)


##############################################################################
# Tests for ScoreCalibration view models
##############################################################################

# Tests on models generated from dicts (e.g. request bodies)


@pytest.mark.parametrize(
    "valid_calibration",
    [
        TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
        TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
        TEST_PATHOGENICITY_SCORE_CALIBRATION,
    ],
)
def test_can_create_valid_score_calibration(valid_calibration):
    sc = ScoreCalibrationCreate.model_validate(valid_calibration)

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("research_use_only", False)
    assert sc.baseline_score == valid_calibration.get("baseline_score")
    assert sc.baseline_score_description == valid_calibration.get("baseline_score_description")

    if valid_calibration.get("functional_classifications") is not None:
        assert len(sc.functional_classifications) == len(valid_calibration["functional_classifications"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_classifications is None

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
    [TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED, TEST_PATHOGENICITY_SCORE_CALIBRATION],
)
def test_can_create_valid_score_calibration_without_functional_classifications(valid_calibration):
    valid_calibration = deepcopy(valid_calibration)
    valid_calibration["functional_classifications"] = None

    sc = ScoreCalibrationCreate.model_validate(valid_calibration)

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("research_use_only", False)
    assert sc.baseline_score == valid_calibration.get("baseline_score")
    assert sc.baseline_score_description == valid_calibration.get("baseline_score_description")

    if valid_calibration.get("functional_classifications") is not None:
        assert len(sc.functional_classifications) == len(valid_calibration["functional_classifications"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_classifications is None

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


def test_cannot_create_score_calibration_when_classification_ranges_overlap():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Make the first two ranges overlap
    invalid_data["functional_classifications"][0]["range"] = [1.0, 3.0]
    invalid_data["functional_classifications"][1]["range"] = [2.0, 4.0]
    with pytest.raises(ValidationError, match="Classified score ranges may not overlap; `"):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_can_create_score_calibration_when_unclassified_ranges_overlap_with_classified_ranges():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Make the first two ranges overlap, one being 'not_specified'
    valid_data["functional_classifications"][0]["range"] = [1.5, 3.0]
    valid_data["functional_classifications"][1]["range"] = [2.0, 4.0]
    valid_data["functional_classifications"][0]["functional_classification"] = (
        FunctionalClassificationOptions.not_specified
    )
    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert len(sc.functional_classifications) == len(valid_data["functional_classifications"])


def test_can_create_score_calibration_when_unclassified_ranges_overlap_with_each_other():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Make the first two ranges overlap, both being 'not_specified'
    valid_data["functional_classifications"][0]["range"] = [1.5, 3.0]
    valid_data["functional_classifications"][1]["range"] = [2.0, 4.0]
    valid_data["functional_classifications"][0]["functional_classification"] = (
        FunctionalClassificationOptions.not_specified
    )
    valid_data["functional_classifications"][1]["functional_classification"] = (
        FunctionalClassificationOptions.not_specified
    )
    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert len(sc.functional_classifications) == len(valid_data["functional_classifications"])


def test_cannot_create_score_calibration_when_ranges_touch_with_inclusive_ranges():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Make the first two ranges touch
    invalid_data["functional_classifications"][0]["range"] = [1.0, 2.0]
    invalid_data["functional_classifications"][1]["range"] = [2.0, 4.0]
    invalid_data["functional_classifications"][0]["inclusive_upper_bound"] = True
    with pytest.raises(ValidationError, match="Classified score ranges may not overlap; `"):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_cannot_create_score_calibration_with_duplicate_range_labels():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Make the first two ranges have the same label
    invalid_data["functional_classifications"][0]["label"] = "duplicate label"
    invalid_data["functional_classifications"][1]["label"] = "duplicate label"
    with pytest.raises(ValidationError, match="Functional range labels must be unique"):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_cannot_create_score_calibration_with_duplicate_range_classes():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED)
    # Make the first two ranges have the same label
    invalid_data["functional_classifications"][0]["label"] = "duplicate label"
    invalid_data["functional_classifications"][1]["label"] = "duplicate label"
    with pytest.raises(ValidationError, match="Functional range labels must be unique"):
        ScoreCalibrationCreate.model_validate(invalid_data)


# Making an exception to usually not testing the ability to create models without optional fields,
# since model validators sometimes rely on their absence.
def test_can_create_score_calibration_without_baseline_score():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    valid_data["baseline_score"] = None

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score is None


def test_can_create_score_calibration_with_baseline_score_when_outside_all_ranges():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    valid_data["baseline_score"] = 10.0

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score == 10.0


def test_can_create_score_calibration_with_baseline_score_when_inside_normal_range():
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    valid_data["baseline_score"] = 3.0

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.baseline_score == 3.0


def test_cannot_create_score_calibration_with_baseline_score_when_inside_non_normal_range():
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    invalid_data["baseline_score"] = -3.0
    with pytest.raises(ValueError, match="Baseline scores may not fall within non-normal ranges"):
        ScoreCalibrationCreate.model_validate(invalid_data)


# Tests on models generated from attributed objects (e.g. ORM models)


@pytest.mark.parametrize(
    "valid_calibration",
    [
        TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED,
        TEST_SAVED_BRNICH_SCORE_CALIBRATION_CLASS_BASED,
        TEST_SAVED_PATHOGENICITY_SCORE_CALIBRATION,
    ],
)
def test_can_create_valid_score_calibration_from_attributed_object(valid_calibration):
    sc = ScoreCalibration.model_validate(dummy_attributed_object_from_dict(valid_calibration))

    assert sc.title == valid_calibration["title"]
    assert sc.research_use_only == valid_calibration.get("researchUseOnly", False)
    assert sc.primary == valid_calibration.get("primary", True)
    assert sc.investigator_provided == valid_calibration.get("investigatorProvided", False)
    assert sc.baseline_score == valid_calibration.get("baselineScore")
    assert sc.baseline_score_description == valid_calibration.get("baselineScoreDescription")

    if valid_calibration.get("functionalClassifications") is not None:
        assert len(sc.functional_classifications) == len(valid_calibration["functionalClassifications"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_classifications is None

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
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)

    # Add publication identifiers with missing information
    invalid_data.pop("thresholdSources", None)
    invalid_data.pop("classificationSources", None)
    invalid_data.pop("methodSources", None)

    with pytest.raises(ValidationError) as exc_info:
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))

    assert "Field required" in str(exc_info.value)
    assert "thresholdSources" in str(exc_info.value)
    assert "classificationSources" in str(exc_info.value)
    assert "methodSources" in str(exc_info.value)


def test_can_create_score_calibration_from_association_style_publication_identifiers_against_attributed_object():
    orig_data = TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED
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

    if orig_data.get("functionalClassifications") is not None:
        assert len(sc.functional_classifications) == len(orig_data["functionalClassifications"])
        # functional range validation is presumed to be well tested separately.
    else:
        assert sc.functional_classifications is None

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
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    invalid_data["primary"] = True
    invalid_data["researchUseOnly"] = True
    with pytest.raises(ValidationError, match="Primary score calibrations may not be marked as research use only"):
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_primary_score_calibration_cannot_be_private():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    invalid_data["primary"] = True
    invalid_data["private"] = True
    with pytest.raises(ValidationError, match="Primary score calibrations may not be marked as private"):
        ScoreCalibration.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_can_create_score_calibration_from_non_orm_context():
    data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)

    sc = ScoreCalibration.model_validate(data)

    assert sc.title == data["title"]
    assert sc.research_use_only == data.get("researchUseOnly", False)
    assert sc.primary == data.get("primary", False)
    assert sc.investigator_provided == data.get("investigatorProvided", False)
    assert sc.baseline_score == data.get("baselineScore")
    assert sc.baseline_score_description == data.get("baselineScoreDescription")
    assert len(sc.functional_ranges) == len(data["functionalRanges"])
    assert len(sc.threshold_sources) == len(data["thresholdSources"])
    assert len(sc.classification_sources) == len(data["classificationSources"])
    assert len(sc.method_sources) == len(data["methodSources"])
    assert sc.calibration_metadata == data.get("calibrationMetadata")


def test_score_calibration_with_score_set_urn_can_be_created_from_attributed_object():
    data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    data["score_set"] = dummy_attributed_object_from_dict({"urn": "urn:mavedb:00000000-0000-0000-0000-000000000001"})

    sc = ScoreCalibrationWithScoreSetUrn.model_validate(dummy_attributed_object_from_dict(data))

    assert sc.title == data["title"]
    assert sc.score_set_urn == data["score_set"].urn


def test_score_calibration_with_score_set_urn_cannot_be_created_without_score_set_urn():
    invalid_data = deepcopy(TEST_SAVED_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    invalid_data["score_set"] = dummy_attributed_object_from_dict({})
    with pytest.raises(ValidationError, match="Unable to coerce score set urn for ScoreCalibrationWithScoreSetUrn"):
        ScoreCalibrationWithScoreSetUrn.model_validate(dummy_attributed_object_from_dict(invalid_data))


def test_cannot_create_score_calibration_with_mixed_range_and_class_based_functional_classifications():
    """Test that score calibrations cannot have both range-based and class-based functional classifications."""
    invalid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    # Add a class-based functional classification to a range-based calibration
    invalid_data["functional_classifications"].append(
        {
            "label": "class based classification",
            "functional_classification": FunctionalClassificationOptions.abnormal,
            "class": "some_class",
        }
    )

    with pytest.raises(
        ValidationError, match="All functional classifications within a score calibration must be of the same type"
    ):
        ScoreCalibrationCreate.model_validate(invalid_data)


def test_score_calibration_range_based_property():
    """Test the range_based property works correctly."""
    range_based_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    sc = ScoreCalibrationCreate.model_validate(range_based_data)
    assert sc.range_based is True
    assert sc.class_based is False


def test_score_calibration_class_based_property():
    """Test the class_based property works correctly."""
    class_based_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_CLASS_BASED)
    sc = ScoreCalibrationCreate.model_validate(class_based_data)
    assert sc.class_based is True
    assert sc.range_based is False


def test_score_calibration_properties_when_no_functional_classifications():
    """Test that properties return False when no functional classifications exist."""
    valid_data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    valid_data["functional_classifications"] = None

    sc = ScoreCalibrationCreate.model_validate(valid_data)
    assert sc.range_based is False
    assert sc.class_based is False


def test_score_calibration_with_score_set_urn_can_be_created_from_non_orm_context():
    data = deepcopy(TEST_BRNICH_SCORE_CALIBRATION_RANGE_BASED)
    data["score_set_urn"] = "urn:mavedb:00000000-0000-0000-0000-000000000001"

    sc = ScoreCalibrationWithScoreSetUrn.model_validate(data)

    assert sc.title == data["title"]
    assert sc.score_set_urn == data["score_set_urn"]
