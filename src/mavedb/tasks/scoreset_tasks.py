from celery.utils.log import get_task_logger

from mavedb.deps import get_db
from mavedb.lib.scoresets import create_variants_data
from mavedb.lib.worker import celery_app
from mavedb.models.scoreset import Scoreset
from mavedb.models.variant import Variant

logger = get_task_logger(__name__)


# TODO Currently unused, but kept here for future reference when we need background processing.
# In moving from Django to FastAPI, we eliminated the need to use this for short tasks like scoreset ingestion, thanks
# to FastAPI's support for asynchronous operations. But if any operations are really long-running or computationally
# intensive, we should move them to a separate process and have the option of moving them to a separate server.


@celery_app.task(
    bind=True,
    ignore_result=True,
    # base=BaseCreateVariantsTask,
    serializer="pickle",
)
def create_variants_task(self, scoreset_urn, scores, counts, index_col, dataset_columns, user_id=None):
    """
    Celery task to that creates and associates `variant.model.Variant` instances
    parsed/validated during upload to a `models.scoreset.ScoreSet` instance.

    Parameters
    ----------
    self : `BaseCreateVariantsTask`
        Bound when celery calls this task.
    user_id : int
        Primary key (id) of the submitting user.
    scoreset_urn : str
        The urn of the instance to associate variants to.
    scores : str
        JSON formatted dataframe (NaN replaced with None).
    counts :
        JSON formatted dataframe (NaN replaced with None).
    index_col : str
        HGVS column to use as the index when matching up variant data between
        scores and counts.
    dataset_columns : dict
        Contains keys `scores` and `counts`. The values are lists of strings
        indicating the columns to be expected in the variants for this dataset.

    Returns
    -------
    `models.scoreset.ScoreSet`
    """
    self.urn = scoreset_urn
    # Look for instances. This might throw an ObjectDoesNotExist exception.
    # Bind ORM objects if they were found.
    self.user = None  # User.objects.get(pk=user_id)
    db = next(get_db())
    self.instance = db.query(Scoreset).get(Scoreset.urn == scoreset_urn).one_or_none()

    logger.info(f"Sending scores dataframe with {len(scores)} rows.")
    logger.info(f"Sending counts dataframe with {len(counts)} rows.")
    logger.info(f"Formatting variants for {self.urn}")
    variants = create_variants_data(scores, counts, index_col)

    if variants:
        logger.info(f"{self.urn}:{variants[-1]}")

    # with transaction.atomic():
    logger.info(f"Deleting existing variants for {self.urn}")
    self.instance.delete_variants()
    logger.info(f"Creating variants for {self.urn}")
    Variant.bulk_create(self.instance, variants)
    logger.info(f"Saving {self.urn}")
    self.instance.dataset_columns = dataset_columns
    self.instance.save()

    return self.instance
