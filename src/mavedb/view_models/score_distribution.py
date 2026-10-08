from mavedb.view_models.base.base import BaseModel


class ScoreDistribution(BaseModel):
    """Binned summary of a score set's scores, built by ``mavedb.lib.score_distribution.summarize_scores``.

    ``counts`` holds equal-width bins spanning ``[min, max]``; scores that are null or not finite are
    counted in ``null_count`` instead.
    """

    version: int
    min: float
    max: float
    counts: list[int]
    null_count: int
