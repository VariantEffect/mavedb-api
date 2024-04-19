import pytest

from fastapi.encoders import jsonable_encoder

from mavedb.view_models.score_set import ScoreSetModify
from mavedb.view_models.target_gene import TargetGeneCreate
from mavedb.view_models.publication_identifier import PublicationIdentifierCreate

from tests.helpers.constants import TEST_MINIMAL_SEQ_SCORESET

import datetime


def test_cannot_create_score_set_without_a_target():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test, exclude={"targetGenes"}), target_genes=[])

    assert "Score sets should define at least one target gene." in str(exc_info.value)


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
            **jsonable_encoder(score_set_test, exclude={"targetGenes"}), target_genes=[target_gene_one, target_gene_two]
        )

    assert "Target sequence labels cannot be empty when multiple targets are defined." in str(exc_info.value)


def test_cannot_create_score_set_with_non_unique_target_labels():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    target_gene_one = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))
    target_gene_two = TargetGeneCreate(**jsonable_encoder(score_set_test["targetGenes"][0]))

    target_gene_one.target_sequence.label = "non_unique"
    target_gene_two.target_sequence.label = "non_unique"

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(
            **jsonable_encoder(score_set_test, exclude={"targetGenes"}), target_genes=[target_gene_one, target_gene_two]
        )

    assert "Target sequence labels cannot be duplicated." in str(exc_info.value)
