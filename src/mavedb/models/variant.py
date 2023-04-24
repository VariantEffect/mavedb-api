from datetime import date

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base
from mavedb.deps import JSONB


class Variant(Base):
    __tablename__ = "variants"

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True)  # index=True, nullable=True
    data = Column(JSONB, nullable=False)

    scoreset_id = Column(Integer, ForeignKey("scoresets.id"), nullable=False)
    # TODO examine if delete-orphan is necessary, explore cascade
    scoreset = relationship("Scoreset", backref=backref("variants", cascade="all,delete-orphan"))

    hgvs_nt = Column(String, nullable=True)
    hgvs_pro = Column(String, nullable=True)
    hgvs_splice = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
