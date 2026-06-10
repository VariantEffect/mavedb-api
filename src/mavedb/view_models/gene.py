from typing import Optional

from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_set import ShortScoreSet


class GeneResponse(BaseModel):
    symbol: str
    name: str
    hgnc_id: Optional[str] = None
    locus_group: Optional[str] = None
    location: Optional[str] = None
    omim_id: Optional[str] = None
    score_sets: list[ShortScoreSet]
    limit: int
    offset: int
    total: int
    total_scored_variants: int

    class Config:
        from_attributes = True
