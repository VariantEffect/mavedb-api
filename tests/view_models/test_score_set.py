import pytest
from fastapi.encoders import jsonable_encoder

from mavedb.lib.validation.constants.score_set import default_ranges
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_set import ScoreSetCreate, ScoreSetModify
from mavedb.view_models.target_gene import TargetGeneCreate
from tests.helpers.constants import TEST_MINIMAL_SEQ_SCORESET


def test_cannot_create_score_set_without_a_target():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test, exclude={"targetGenes"}), target_genes=[])

    assert "Score sets should define at least one target." in str(exc_info.value)


def test_cannot_create_score_set_with_multiple_primary_publications():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    identifier_one = PublicationIdentifierCreate(identifier="2019.12.12.207222")
    identifier_two = PublicationIdentifierCreate(identifier="2019.12.12.20733333")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **jsonable_encoder(score_set_test),
            exclude={"targetGenes"},
            target_genes=[TargetGeneCreate(**jsonable_encoder(target)) for target in score_set_test["targetGenes"]],
            primary_publication_identifiers=[identifier_one, identifier_two],
        )

    assert "multiple primary publication identifiers are not allowed" in str(exc_info.value)


def test_cannot_create_score_set_without_target_gene_labels_when_multiple_targets_exist():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))
    target_gene_two = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **jsonable_encoder(score_set_test, exclude={"targetGenes"}),
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be empty when multiple targets are defined." in str(exc_info.value)


def test_cannot_create_score_set_with_non_unique_target_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))
    target_gene_two = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))

    non_unique = "BRCA1"
    target_gene_one.target_sequence.label = non_unique
    target_gene_two.target_sequence.label = non_unique

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **jsonable_encoder(score_set_test, exclude={"targetGenes"}),
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be duplicated." in str(exc_info.value)


def test_cannot_create_score_set_without_a_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"title"})
    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"title"})
    invalid_score_set["title"] = " "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"title"})
    invalid_score_set["title"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_without_a_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"shortDescription"})

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"shortDescription"})
    invalid_score_set["shortDescription"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"shortDescription"})
    invalid_score_set["shortDescription"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_without_an_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"abstractText"})

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"abstractText"})
    invalid_score_set["abstractText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"abstractText"})
    invalid_score_set["abstractText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_without_a_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"methodText"})

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"methodText"})
    invalid_score_set["methodText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    invalid_score_set = jsonable_encoder(score_set, exclude={"methodText"})
    invalid_score_set["methodText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**invalid_score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_too_many_boundaries():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1, 2.0)},
            {"label": "range_2", "classification": "abnormal", "range": (2.0, 2.1, 2.3)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Only a lower and upper bound are allowed." in str(exc_info.value)


def test_cannot_create_score_set_with_overlapping_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1.1)},
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_can_create_score_set_with_mixed_range_types():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
            {"label": "range_2", "classification": "abnormal", "range": ("1.1", 2.1)},
            {"label": "range_3", "classification": "abnormal", "range": (2.2, "3.2")},
        ],
    }

    ScoreSetModify(**jsonable_encoder(score_set_test))


def test_can_create_score_set_with_adjacent_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
        ],
    }

    ScoreSetModify(**jsonable_encoder(score_set_test))


def test_can_create_score_set_with_flipped_adjacent_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_2", "classification": "abnormal", "range": (1, 2.1)},
            {"label": "range_1", "classification": "normal", "range": (0, 1)},
        ],
    }

    ScoreSetModify(**jsonable_encoder(score_set_test))


def test_can_create_score_set_with_adjacent_negative_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    ScoreSetModify(**jsonable_encoder(score_set_test))


def test_can_create_score_set_with_flipped_adjacent_negative_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
        ],
    }

    ScoreSetModify(**jsonable_encoder(score_set_test))


def test_cannot_create_score_set_with_overlapping_upper_unbounded_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (0, None)},
            {"label": "range_2", "classification": "abnormal", "range": (1, None)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_cannot_create_score_set_with_overlapping_lower_unbounded_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (None, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (None, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Score ranges may not overlap; `range_1` overlaps with `range_2`" in str(exc_info.value)


def test_cannot_create_score_set_with_backwards_bounds():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (2, 1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "The lower bound of the score range may not be larger than the upper bound." in str(exc_info.value)


def test_cannot_create_score_set_with_equal_bounds():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": 1,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "The lower and upper bound of the score range may not be the same." in str(exc_info.value)


def test_cannot_create_score_set_with_duplicate_range_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_1", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Detected repeated label: `range_1`. Range labels must be unique." in str(exc_info.value)


def test_cannot_create_score_set_with_duplicate_range_labels_whitespace():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "     range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_1       ", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Detected repeated label: `range_1`. Range labels must be unique." in str(exc_info.value)


def test_cannot_create_score_set_with_wild_type_outside_ranges():
    wt_score = 0.5
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert (
        f"The provided wild type score of {wt_score} is not within any of the provided normal ranges. This score should be within a normal range."
        in str(exc_info.value)
    )


def test_cannot_create_score_set_with_wild_type_outside_normal_range():
    wt_score = -1.5
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": wt_score,
        "ranges": [
            {"label": "range_1", "classification": "normal", "range": (-1, 0)},
            {"label": "range_2", "classification": "abnormal", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert (
        f"The provided wild type score of {wt_score} is not within any of the provided normal ranges. This score should be within a normal range."
        in str(exc_info.value)
    )


@pytest.mark.parametrize("present_name", default_ranges)
def test_cannot_create_score_set_without_default_range(present_name):
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -1.5,
        "ranges": [
            {"label": "range_2", "classification": f"{present_name}", "range": (-3, -1)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Both `normal` and `abnormal` ranges must be provided." in str(exc_info.value)


def test_cannot_create_score_set_without_default_ranges():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = {
        "wt_score": -0.5,
        "ranges": [
            {"label": "range_1", "classification": "other", "range": (-1, 0)},
        ],
    }

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test))

    assert "Unexpected classification value(s): other. Permitted values: ['normal', 'abnormal']" in str(exc_info.value)
