import pytest

from fastapi.encoders import jsonable_encoder

from mavedb.view_models.score_set import ScoreSetModify
from mavedb.view_models.target_gene import TargetGene

from tests.helpers.constants import TEST_MINIMAL_SEQ_SCORESET

import datetime


def test_cannot_create_score_set_without_a_target():
    score_set_test = TEST_MINIMAL_SEQ_SCORESET.copy()

    with pytest.raises(ValueError) as exc_info:
        ScoreSetModify(**jsonable_encoder(score_set_test, exclude={"targetGenes"}), target_genes=[])

    assert "Score sets should define at least one target gene." in str(exc_info.value)
