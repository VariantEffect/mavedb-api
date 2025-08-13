from copy import deepcopy
import pytest
from pydantic import ValidationError

from mavedb.view_models.score_range import (
    ScoreRangeModify,
    ScoreRangeCreate,
    ScoreRange,
    ScoreRangesCreate,
    ScoreRangesModify,
    ScoreRanges,
    InvestigatorScoreRangeCreate,
    InvestigatorScoreRangeModify,
    InvestigatorScoreRange,
    InvestigatorScoreRangesCreate,
    InvestigatorScoreRangesModify,
    InvestigatorScoreRanges,
    PillarProjectScoreRangeCreate,
    PillarProjectScoreRangeModify,
    PillarProjectScoreRange,
    PillarProjectScoreRangesCreate,
    PillarProjectScoreRangesModify,
    PillarProjectScoreRanges,
    ScoreSetRangesModify,
    ScoreSetRangesCreate,
    ScoreSetRanges,
)

from tests.helpers.constants import (
    TEST_SCORE_SET_NORMAL_RANGE,
    TEST_SCORE_SET_ABNORMAL_RANGE,
    TEST_SCORE_SET_NOT_SPECIFIED_RANGE,
    TEST_INVESTIGATOR_PROVIDED_SCORE_SET_NORMAL_RANGE,
    TEST_INVESTIGATOR_PROVIDED_SCORE_SET_ABNORMAL_RANGE,
    TEST_PILLAR_PROJECT_SCORE_SET_NORMAL_RANGE,
    TEST_PILLAR_PROJECT_SCORE_SET_ABNORMAL_RANGE,
    TEST_SCORE_SET_RANGE,
    TEST_SCORE_SET_RANGE_WITH_SOURCE,
    TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE,
    TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE_WITH_SOURCE,
    TEST_PILLAR_PROJECT_SCORE_SET_RANGE,
    TEST_PILLAR_PROJECT_SCORE_SET_RANGE_WITH_SOURCE,
    TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED,
    TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
    TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT,
    TEST_SCORE_SET_NEGATIVE_INFINITY_RANGE,
    TEST_SCORE_SET_POSITIVE_INFINITY_RANGE,
    TEST_BASELINE_SCORE,
)


### ScoreRange Tests ###


@pytest.mark.parametrize(
    "score_range_data",
    [
        TEST_SCORE_SET_NORMAL_RANGE,
        TEST_SCORE_SET_ABNORMAL_RANGE,
        TEST_SCORE_SET_NOT_SPECIFIED_RANGE,
        TEST_SCORE_SET_POSITIVE_INFINITY_RANGE,
        TEST_SCORE_SET_NEGATIVE_INFINITY_RANGE,
    ],
)
@pytest.mark.parametrize("ScoreRangeModel", [ScoreRange, ScoreRangeModify, ScoreRangeCreate])
def test_score_range_base_valid_range(ScoreRangeModel, score_range_data):
    score_range = ScoreRangeModel(**score_range_data)
    assert score_range.label == score_range_data["label"], "Label should match"
    assert score_range.classification == score_range_data["classification"], "Classification should match"
    assert score_range.range[0] == score_range_data["range"][0], "Range should match"
    assert score_range.range[1] == score_range_data["range"][1], "Range should match"
    assert score_range.description == score_range_data.get("description", None), "Description should match"
    assert score_range.inclusive_lower_bound == score_range_data.get(
        "inclusive_lower_bound"
    ), "Inclusive lower bound should match"
    assert score_range.inclusive_upper_bound == score_range_data.get(
        "inclusive_upper_bound"
    ), "Inclusive upper bound should match"


@pytest.mark.parametrize(
    "score_range_data",
    [TEST_INVESTIGATOR_PROVIDED_SCORE_SET_NORMAL_RANGE, TEST_INVESTIGATOR_PROVIDED_SCORE_SET_ABNORMAL_RANGE],
)
@pytest.mark.parametrize(
    "ScoreRangeModel", [InvestigatorScoreRange, InvestigatorScoreRangeCreate, InvestigatorScoreRangeModify]
)
def test_score_range_investigator_valid_range(ScoreRangeModel, score_range_data):
    score_range = ScoreRangeModel(**score_range_data)
    assert score_range.label == score_range_data["label"], "Label should match"
    assert score_range.classification == score_range_data["classification"], "Classification should match"
    assert score_range.range[0] == score_range_data["range"][0], "Range should match"
    assert score_range.range[1] == score_range_data["range"][1], "Range should match"
    assert score_range.description == score_range_data.get("description", None), "Description should match"
    assert score_range.odds_path.ratio == score_range_data.get("odds_path", {}).get(
        "ratio", None
    ), "Odds path should match"
    assert score_range.odds_path.evidence == score_range_data.get("odds_path", {}).get(
        "evidence", None
    ), "Odds path should match"


@pytest.mark.parametrize(
    "score_range_data",
    [TEST_PILLAR_PROJECT_SCORE_SET_NORMAL_RANGE, TEST_PILLAR_PROJECT_SCORE_SET_ABNORMAL_RANGE],
)
@pytest.mark.parametrize(
    "ScoreRangeModel", [PillarProjectScoreRange, PillarProjectScoreRangeCreate, PillarProjectScoreRangeModify]
)
def test_score_range_pillar_project_valid_range(ScoreRangeModel, score_range_data):
    score_range = ScoreRangeModel(**score_range_data)
    assert score_range.label == score_range_data["label"], "Label should match"
    assert score_range.classification == score_range_data["classification"], "Classification should match"
    assert score_range.range[0] == score_range_data["range"][0], "Range should match"
    assert score_range.range[1] == score_range_data["range"][1], "Range should match"
    assert score_range.description == score_range_data.get("description", None), "Description should match"
    assert score_range.positive_likelihood_ratio == score_range_data.get(
        "positive_likelihood_ratio", None
    ), "Odds path should match"


@pytest.mark.parametrize(
    "ScoreRangeModel",
    [
        ScoreRange,
        ScoreRangeModify,
        ScoreRangeCreate,
        InvestigatorScoreRange,
        InvestigatorScoreRangeCreate,
        InvestigatorScoreRangeModify,
        PillarProjectScoreRange,
        PillarProjectScoreRangeCreate,
        PillarProjectScoreRangeModify,
    ],
)
def test_score_range_invalid_range_length(ScoreRangeModel):
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [0.0],
    }
    with pytest.raises(ValidationError, match=r".*Only a lower and upper bound are allowed\..*"):
        ScoreRangeModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangeModel",
    [
        ScoreRange,
        ScoreRangeModify,
        ScoreRangeCreate,
        InvestigatorScoreRange,
        InvestigatorScoreRangeCreate,
        InvestigatorScoreRangeModify,
        PillarProjectScoreRange,
        PillarProjectScoreRangeCreate,
        PillarProjectScoreRangeModify,
    ],
)
def test_score_range_base_invalid_range_order(ScoreRangeModel):
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [1.0, 0.0],
    }
    with pytest.raises(
        ValidationError,
        match=r".*The lower bound of the score range may not be larger than the upper bound\..*",
    ):
        ScoreRangeModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangeModel",
    [
        ScoreRange,
        ScoreRangeModify,
        ScoreRangeCreate,
        InvestigatorScoreRange,
        InvestigatorScoreRangeCreate,
        InvestigatorScoreRangeModify,
        PillarProjectScoreRange,
        PillarProjectScoreRangeCreate,
        PillarProjectScoreRangeModify,
    ],
)
def test_score_range_base_equal_bounds(ScoreRangeModel):
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": [1.0, 1.0],
    }
    with pytest.raises(
        ValidationError,
        match=r".*The lower and upper bound of the score range may not be the same\..*",
    ):
        ScoreRangeModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangeModel",
    [
        ScoreRange,
        ScoreRangeModify,
        ScoreRangeCreate,
        InvestigatorScoreRange,
        InvestigatorScoreRangeCreate,
        InvestigatorScoreRangeModify,
        PillarProjectScoreRange,
        PillarProjectScoreRangeCreate,
        PillarProjectScoreRangeModify,
    ],
)
@pytest.mark.parametrize(
    "range_value",
    [
        [None, 1.0],
        [1.0, None],
    ],
)
def test_score_range_may_not_include_infinity(ScoreRangeModel, range_value):
    invalid_data = {
        "label": "Test Range",
        "classification": "normal",
        "range": range_value,
        "inclusive_lower_bound": True,
        "inclusive_upper_bound": True,
    }
    with pytest.raises(
        ValidationError,
        match=r".*An inclusive lower bound may not include negative infinity\..*|An inclusive upper bound may not include positive infinity\..*",
    ):
        ScoreRangeModel(**invalid_data)


@pytest.mark.parametrize(
    "classification,evidence_strength,should_raise",
    [
        ("normal", 1, True),  # Should raise: normal with positive evidence_strength
        ("normal", 0, True),  # Should not raise: normal with zero evidence_strength
        ("normal", -1, False),  # Should not raise: normal with negative evidence_strength
        ("abnormal", -1, True),  # Should raise: abnormal with negative evidence_strength
        ("abnormal", 0, True),  # Should not raise: abnormal with zero evidence_strength
        ("abnormal", 1, False),  # Should not raise: abnormal with positive evidence_strength
        ("not_specified", 1, False),  # Should not raise: not_specified with positive evidence_strength
        ("not_specified", -1, False),  # Should not raise: not_specified with negative evidence_strength
    ],
)
@pytest.mark.parametrize(
    "ScoreRangeModel", [PillarProjectScoreRange, PillarProjectScoreRangeCreate, PillarProjectScoreRangeModify]
)
def test_pillar_project_evidence_strength_cardinality_must_agree_with_classification(
    classification, evidence_strength, should_raise, ScoreRangeModel
):
    invalid_data = deepcopy(TEST_PILLAR_PROJECT_SCORE_SET_NORMAL_RANGE)
    invalid_data["classification"] = classification
    invalid_data["evidence_strength"] = evidence_strength
    if should_raise:
        with pytest.raises(ValidationError) as excinfo:
            ScoreRangeModel(**invalid_data)
        if classification == "normal":
            assert "The evidence strength for a normal range must be negative." in str(excinfo.value)
        elif classification == "abnormal":
            assert "The evidence strength for an abnormal range must be positive." in str(excinfo.value)
    else:
        obj = ScoreRangeModel(**invalid_data)
        assert obj.evidence_strength == evidence_strength


### ScoreRanges Tests ###


@pytest.mark.parametrize(
    "score_ranges_data",
    [TEST_SCORE_SET_RANGE, TEST_SCORE_SET_RANGE_WITH_SOURCE],
)
@pytest.mark.parametrize("ScoreRangesModel", [ScoreRanges, ScoreRangesCreate, ScoreRangesModify])
def test_score_ranges_base_valid_range(ScoreRangesModel, score_ranges_data):
    score_ranges = ScoreRangesModel(**score_ranges_data)
    assert score_ranges.ranges is not None, "Ranges should not be None"
    assert score_ranges.source == score_ranges_data.get("source", None), "Source should match"


@pytest.mark.parametrize(
    "score_ranges_data",
    [TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE, TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE_WITH_SOURCE],
)
@pytest.mark.parametrize(
    "ScoreRangesModel", [InvestigatorScoreRanges, InvestigatorScoreRangesCreate, InvestigatorScoreRangesModify]
)
def test_score_ranges_investigator_valid_range(ScoreRangesModel, score_ranges_data):
    score_ranges = ScoreRangesModel(**score_ranges_data)
    assert score_ranges.ranges is not None, "Ranges should not be None"
    assert score_ranges.baseline_score == TEST_BASELINE_SCORE, "Baseline score should match"
    assert score_ranges.odds_path_source == score_ranges_data.get(
        "odds_path_source", None
    ), "Odds path source should match"
    assert score_ranges.source == score_ranges_data.get("source", None), "Source should match"


@pytest.mark.parametrize(
    "score_ranges_data",
    [TEST_PILLAR_PROJECT_SCORE_SET_RANGE, TEST_PILLAR_PROJECT_SCORE_SET_RANGE_WITH_SOURCE],
)
@pytest.mark.parametrize(
    "ScoreRangesModel", [PillarProjectScoreRanges, PillarProjectScoreRangesCreate, PillarProjectScoreRangesModify]
)
def test_score_ranges_pillar_project_valid_range(ScoreRangesModel, score_ranges_data):
    score_ranges = ScoreRangesModel(**score_ranges_data)
    assert score_ranges.ranges is not None, "Ranges should not be None"
    assert score_ranges.prior_probability_pathogenicity == score_ranges_data.get(
        "prior_probability_pathogenicity", None
    ), "Prior probability pathogenicity should match"
    assert score_ranges.parameter_sets is not None, "Parameter sets should not be None"
    assert score_ranges.source == score_ranges_data.get("source", None), "Source should match"


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        ScoreRanges,
        ScoreRangesCreate,
        ScoreRangesModify,
        InvestigatorScoreRanges,
        InvestigatorScoreRangesCreate,
        InvestigatorScoreRangesModify,
    ],
)
def test_score_ranges_ranges_may_not_overlap(ScoreRangesModel):
    range_test = ScoreRange(label="Range 1", classification="abnormal", range=[0.0, 2.0])
    range_check = ScoreRange(label="Range 2", classification="abnormal", range=[1.0, 3.0])
    invalid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }
    with pytest.raises(
        ValidationError,
        match=rf".*Score ranges may not overlap; `{range_test.label}` \(\[{range_test.range[0]}, {range_test.range[1]}\]\) overlaps with `{range_check.label}` \(\[{range_check.range[0]}, {range_check.range[1]}\]\).*",
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        ScoreRanges,
        ScoreRangesCreate,
        ScoreRangesModify,
        InvestigatorScoreRanges,
        InvestigatorScoreRangesCreate,
        InvestigatorScoreRangesModify,
    ],
)
def test_score_ranges_ranges_may_not_overlap_via_inclusive_bounds(ScoreRangesModel):
    range_test = ScoreRange(
        label="Range 1",
        classification="abnormal",
        range=[0.0, 2.0],
        inclusive_lower_bound=True,
        inclusive_upper_bound=True,
    )
    range_check = ScoreRange(
        label="Range 2",
        classification="abnormal",
        range=[2.0, 3.0],
        inclusive_lower_bound=True,
        inclusive_upper_bound=True,
    )
    invalid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }
    with pytest.raises(
        ValidationError,
        match=rf".*Score ranges may not overlap; `{range_test.label}` \(\[{range_test.range[0]}, {range_test.range[1]}\]\) overlaps with `{range_check.label}` \(\[{range_check.range[0]}, {range_check.range[1]}\]\).*",
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        ScoreRanges,
        ScoreRangesCreate,
        ScoreRangesModify,
        InvestigatorScoreRanges,
        InvestigatorScoreRangesCreate,
        InvestigatorScoreRangesModify,
    ],
)
@pytest.mark.parametrize(
    "range_value1, range_value2, orientation",
    [
        ([0.0, 2.0], [2.0, 3.0], True),
        ([0.0, 2.0], [2.0, 3.0], False),
    ],
)
def test_score_ranges_ranges_boundaries_may_be_adjacent(ScoreRangesModel, range_value1, range_value2, orientation):
    range_test = ScoreRange(
        label="Range 1",
        classification="abnormal",
        range=range_value1,
        inclusive_lower_bound=orientation,
        inclusive_upper_bound=not orientation,
    )
    range_check = ScoreRange(
        label="Range 2",
        classification="abnormal",
        range=range_value2,
        inclusive_lower_bound=orientation,
        inclusive_upper_bound=not orientation,
    )
    valid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }

    ScoreRangesModel(**valid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        PillarProjectScoreRanges,
        PillarProjectScoreRangesCreate,
        PillarProjectScoreRangesModify,
    ],
)
def test_score_ranges_pillar_project_ranges_may_not_overlap(ScoreRangesModel):
    range_test = PillarProjectScoreRange(
        label="Range 1", classification="abnormal", range=[0.0, 2.0], evidence_strength=2
    )
    range_check = PillarProjectScoreRange(
        label="Range 2", classification="abnormal", range=[1.0, 3.0], evidence_strength=3
    )
    invalid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }
    with pytest.raises(
        ValidationError,
        match=rf".*Score ranges may not overlap; `{range_test.label}` \(\[{range_test.range[0]}, {range_test.range[1]}\]\) overlaps with `{range_check.label}` \(\[{range_check.range[0]}, {range_check.range[1]}\]\).*",
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        PillarProjectScoreRanges,
        PillarProjectScoreRangesCreate,
        PillarProjectScoreRangesModify,
    ],
)
def test_score_ranges_pillar_project_ranges_may_not_overlap_via_inclusive_bounds(ScoreRangesModel):
    range_test = PillarProjectScoreRange(
        label="Range 1",
        classification="abnormal",
        range=[0.0, 2.0],
        evidence_strength=2,
        inclusive_lower_bound=True,
        inclusive_upper_bound=True,
    )
    range_check = PillarProjectScoreRange(
        label="Range 2",
        classification="abnormal",
        range=[2.0, 3.0],
        evidence_strength=3,
        inclusive_lower_bound=True,
        inclusive_upper_bound=True,
    )
    invalid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }
    with pytest.raises(
        ValidationError,
        match=rf".*Score ranges may not overlap; `{range_test.label}` \(\[{range_test.range[0]}, {range_test.range[1]}\]\) overlaps with `{range_check.label}` \(\[{range_check.range[0]}, {range_check.range[1]}\]\).*",
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [
        PillarProjectScoreRanges,
        PillarProjectScoreRangesCreate,
        PillarProjectScoreRangesModify,
    ],
)
@pytest.mark.parametrize(
    "range_value1, range_value2, orientation",
    [
        ([0.0, 2.0], [2.0, 3.0], True),
        ([0.0, 2.0], [2.0, 3.0], False),
    ],
)
def test_score_ranges_pillar_project_ranges_boundaries_may_be_adjacent(
    ScoreRangesModel, range_value1, range_value2, orientation
):
    range_test = PillarProjectScoreRange(
        label="Range 1",
        classification="abnormal",
        range=range_value1,
        evidence_strength=2,
        inclusive_lower_bound=orientation,
        inclusive_upper_bound=not orientation,
    )
    range_check = PillarProjectScoreRange(
        label="Range 2",
        classification="abnormal",
        range=range_value2,
        evidence_strength=3,
        inclusive_lower_bound=orientation,
        inclusive_upper_bound=not orientation,
    )
    valid_data = {
        "ranges": [
            range_test,
            range_check,
        ]
    }

    ScoreRangesModel(**valid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [InvestigatorScoreRanges, InvestigatorScoreRangesCreate, InvestigatorScoreRangesModify],
)
def test_score_ranges_investigator_normal_classification_exists_if_baseline_score_provided(ScoreRangesModel):
    invalid_data = deepcopy(TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE)
    invalid_data["ranges"].remove(TEST_INVESTIGATOR_PROVIDED_SCORE_SET_NORMAL_RANGE)
    with pytest.raises(
        ValidationError,
        match=r".*A baseline score has been provided, but no normal classification range exists.*",
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.parametrize(
    "ScoreRangesModel",
    [InvestigatorScoreRanges, InvestigatorScoreRangesCreate, InvestigatorScoreRangesModify],
)
def test_score_ranges_investigator_baseline_score_within_normal_range(ScoreRangesModel):
    baseline_score = 50.0
    invalid_data = deepcopy(TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE)
    invalid_data["baselineScore"] = baseline_score
    with pytest.raises(
        ValidationError,
        match=r".*The provided baseline score of {} is not within any of the provided normal ranges\. This score should be within a normal range\..*".format(
            baseline_score
        ),
    ):
        ScoreRangesModel(**invalid_data)


@pytest.mark.skip("Not applicable currently. Baseline score is not required if a normal range exists.")
@pytest.mark.parametrize(
    "ScoreRangesModel",
    [InvestigatorScoreRanges, InvestigatorScoreRangesCreate, InvestigatorScoreRangesModify],
)
def test_score_ranges_investigator_baseline_type_score_provided_if_normal_range_exists(ScoreRangesModel):
    invalid_data = deepcopy(TEST_INVESTIGATOR_PROVIDED_SCORE_SET_RANGE)
    invalid_data["baselineScore"] = None
    with pytest.raises(
        ValidationError,
        match=r".*A normal range has been provided, but no baseline type score has been provided.*",
    ):
        ScoreRangesModel(**invalid_data)


### ScoreSetRanges Tests ###


@pytest.mark.parametrize(
    "score_set_ranges_data",
    [
        TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED,
        TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
        TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT,
    ],
)
@pytest.mark.parametrize("ScoreSetRangesModel", [ScoreSetRanges, ScoreSetRangesCreate, ScoreSetRangesModify])
def test_score_set_ranges_valid_range(ScoreSetRangesModel, score_set_ranges_data):
    score_set_ranges = ScoreSetRangesModel(**score_set_ranges_data)
    assert isinstance(score_set_ranges, ScoreSetRangesModel), "ScoreSetRangesModel instantiation failed"
    # Ensure a ranges property exists. Data values are checked elsewhere in more detail.
    for range_name in score_set_ranges.__fields_set__:
        range_definition = getattr(score_set_ranges, range_name)
        assert range_definition.ranges


@pytest.mark.parametrize(
    "ScoreSetRangesModel",
    [
        ScoreSetRanges,
        ScoreSetRangesCreate,
        ScoreSetRangesModify,
    ],
)
@pytest.mark.parametrize(
    "score_set_ranges_data",
    [
        TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED,
        TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
    ],
)
def test_score_set_ranges_may_not_include_duplicate_labels(ScoreSetRangesModel, score_set_ranges_data):
    # Add a duplicate label to the ranges
    score_set_ranges_data = deepcopy(score_set_ranges_data)
    range_values = score_set_ranges_data[list(score_set_ranges_data.keys())[0]]["ranges"]
    for range_value in range_values:
        range_value["label"] = "duplicated_label"

    with pytest.raises(
        ValidationError,
        match=r".*Detected repeated label\(s\): duplicated_label\. Range labels must be unique\..*",
    ):
        ScoreSetRangesModel(**score_set_ranges_data)


@pytest.mark.parametrize(
    "ScoreSetRangesModel",
    [
        ScoreSetRanges,
        ScoreSetRangesCreate,
        ScoreSetRangesModify,
    ],
)
def test_score_set_ranges_may_include_duplicate_labels_in_different_range_definitions(ScoreSetRangesModel):
    # Add a duplicate label across all schemas
    score_set_ranges_data = deepcopy(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT)
    for key in score_set_ranges_data:
        range_schema = score_set_ranges_data[key]
        range_schema["ranges"][0]["label"] = "duplicated_label"

    ScoreSetRangesModel(**score_set_ranges_data)
