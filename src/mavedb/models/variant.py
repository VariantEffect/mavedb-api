from datetime import date

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

from .score_set import ScoreSet


class Variant(Base):
    __tablename__ = "variants"

    id = Column(Integer, primary_key=True)

    urn = Column(String(64), index=True, nullable=True, unique=True)
    data = Column(JSONB, nullable=False)

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    # TODO examine if delete-orphan is necessary, explore cascade
    score_set: Mapped[ScoreSet] = relationship(back_populates="variants")

    hgvs_nt = Column(String, nullable=True)
    hgvs_pro = Column(String, nullable=True)
    hgvs_splice = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
