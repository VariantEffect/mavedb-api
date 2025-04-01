import logging
from datetime import datetime

from ga4gh.core.models import MappableConcept
from ga4gh.va_spec.base.core import DataSet

from mavedb.lib.annotation.document import score_set_as_iri
from mavedb.models.score_set import ScoreSet

logger = logging.getLogger(__name__)


def score_set_to_data_set(score_set: ScoreSet) -> DataSet:
    """
    Create a [VA Data Set](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/dataset.html#data-set)
    object from the provided MaveDB score set.
    """
    return DataSet(
        id=score_set.urn,
        name=score_set.title,
        description=score_set.short_description,
        # XXX - Create a mappable concept for licenses. We may want a simple helper function.
        license=MappableConcept(
            name=score_set.license.short_name,
        ),
        reportedIn=score_set_as_iri(score_set),
        releaseDate=datetime.strftime(score_set.published_date, "%Y-%m-%d") if score_set.published_date else None,
    )
