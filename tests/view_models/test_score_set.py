import pytest
from copy import deepcopy

from mavedb.view_models.publication_identifier import PublicationIdentifierCreate
from mavedb.view_models.score_set import ScoreSetCreate, ScoreSetModify
from mavedb.view_models.target_gene import TargetGeneCreate
from tests.helpers.constants import (
    TEST_PUBMED_IDENTIFIER,
    TEST_MINIMAL_ACC_SCORESET,
    TEST_MINIMAL_SEQ_SCORESET,
    TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED,
    TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT,
    TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT,
)


def test_cannot_create_score_set_without_a_target():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test.pop("targetGenes")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**score_set_test, target_genes=[])

    assert "Score sets should define at least one target." in str(exc_info.value)


def test_cannot_create_score_set_with_multiple_primary_publications():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    target_genes = score_set_test.pop("targetGenes")

    identifier_one = PublicationIdentifierCreate(identifier="2019.12.12.207222")
    identifier_two = PublicationIdentifierCreate(identifier="2019.12.12.20733333")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[TargetGeneCreate(**target) for target in target_genes],
            primary_publication_identifiers=[identifier_one, identifier_two],
        )

    assert "multiple primary publication identifiers are not allowed" in str(exc_info.value)


def test_cannot_create_score_set_without_target_gene_labels_when_multiple_targets_exist():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    score_set_test.pop("targetGenes")
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be empty when multiple targets are defined." in str(exc_info.value)


def test_cannot_create_score_set_with_non_unique_target_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    non_unique = "BRCA1"
    target_gene_one.target_sequence.label = non_unique
    target_gene_two.target_sequence.label = non_unique

    score_set_test.pop("targetGenes")
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "Target sequence labels cannot be duplicated." in str(exc_info.value)


def test_cannot_create_score_set_without_a_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("title")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "field required" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["title"] = " "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_title():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["title"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "title" in str(exc_info.value)


def test_cannot_create_score_set_without_a_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("shortDescription")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "field required" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["shortDescription"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_short_description():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["shortDescription"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "shortDescription" in str(exc_info.value)


def test_cannot_create_score_set_without_an_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("abstractText")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "field required" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["abstractText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_abstract():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["abstractText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "abstractText" in str(exc_info.value)


def test_cannot_create_score_set_without_a_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set.pop("methodText")

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "field required" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_a_space_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["methodText"] = "  "

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "This field is required and cannot be empty." in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_cannot_create_score_set_with_an_empty_method():
    score_set = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set["methodText"] = ""

    with pytest.raises(ValueError) as exc_info:
        ScoreSetCreate(**score_set)

    assert "none is not an allowed value" in str(exc_info.value)
    assert "methodText" in str(exc_info.value)


def test_can_create_score_set_with_investigator_provided_score_range():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = deepcopy(TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED)
    score_set_test["secondary_publication_identifiers"] = [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}]

    ScoreSetModify(**score_set_test)


def test_cannot_create_score_set_with_investigator_provided_score_range_if_source_not_in_score_set_publications():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = deepcopy(TEST_SCORE_SET_RANGES_ONLY_INVESTIGATOR_PROVIDED)

    with pytest.raises(
        ValueError,
        match=r".*Score range source publication at index {} is not defined in score set publications.*".format(0),
    ):
        ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_pillar_project_score_range():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = deepcopy(TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT)
    score_set_test["secondary_publication_identifiers"] = [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}]

    ScoreSetModify(**score_set_test)


def test_cannot_create_score_set_with_pillar_project_score_range_if_source_not_in_score_set_publications():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = deepcopy(TEST_SCORE_SET_RANGES_ONLY_PILLAR_PROJECT)

    with pytest.raises(
        ValueError,
        match=r".*Score range source publication at index {} is not defined in score set publications.*".format(0),
    ):
        ScoreSetModify(**score_set_test)


def test_can_create_score_set_with_ranges_and_calibrations():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()
    score_set_test["score_ranges"] = deepcopy(TEST_SCORE_SET_RANGES_ALL_SCHEMAS_PRESENT)
    score_set_test["secondary_publication_identifiers"] = [{"identifier": TEST_PUBMED_IDENTIFIER, "db_name": "PubMed"}]

    ScoreSetModify(**score_set_test)


def test_cannot_create_score_set_with_inconsistent_base_editor_flags():
    score_set_test = TEST_MINIMAL_ACC_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**score_set_test["targetGenes"][0])
    target_gene_two = TargetGeneCreate(**score_set_test["targetGenes"][0])

    target_gene_one.target_accession.is_base_editor = True
    target_gene_two.target_accession.is_base_editor = False

    score_set_test.pop("targetGenes")
    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **score_set_test,
            target_genes=[target_gene_one, target_gene_two],
        )

    assert "All target accessions must be of the same base editor type." in str(exc_info.value)
