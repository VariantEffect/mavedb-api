import logging
from datetime import datetime

from ga4gh.core.entity_models import DataSet

from mavedb.models.score_set import ScoreSet

from mavedb.lib.annotation.contribution import mavedb_creator_contribution, mavedb_modifier_contribution
from mavedb.lib.annotation.document import score_set_to_document

logger = logging.getLogger(__name__)


def score_set_to_data_set(score_set: ScoreSet) -> DataSet:
    """
    Create a [VA Data Set](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/dataset.html#data-set)
    object from the provided MaveDB score set.
    """
    return DataSet(
        id=score_set.urn,
        label="Variant effect data set",
        license=score_set.license.short_name,
        reportedIn=[score_set_to_document(score_set)],
        contributions=[
            mavedb_creator_contribution(score_set, score_set.created_by),
            mavedb_modifier_contribution(score_set, score_set.modified_by),
        ],
        releaseDate=datetime.strftime(score_set.published_date, "%Y-%m-%d") if score_set.published_date else None,
    )
