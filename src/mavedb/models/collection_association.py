from sqlalchemy import Column, ForeignKey
from sqlalchemy.schema import Table

from mavedb.db.base import Base

collection_experiments_association_table = Table(
    "collection_experiments",
    Base.metadata,
    Column("collection_id", ForeignKey("collections.id"), primary_key=True),
    Column("experiment_id", ForeignKey("experiments.id"), primary_key=True),
)

collection_score_sets_association_table = Table(
    "collection_score_sets",
    Base.metadata,
    Column("collection_id", ForeignKey("collections.id"), primary_key=True),
    Column("score_set_id", ForeignKey("scoresets.id"), primary_key=True),
)
