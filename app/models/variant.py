from datetime import date
from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from app.db.base import Base
from app.deps import JSONB


class Variant(Base):
    __tablename__ = "variant_variant"

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True)  # index=True, nullable=True
    data = Column(JSONB, nullable=False)

    scoreset_id = Column(Integer, ForeignKey("dataset_scoreset.id"), nullable=False)
    scoreset = relationship("Scoreset", back_populates="variants")

    hgvs_nt = Column(String, nullable=True)
    hgvs_pro = Column(String, nullable=True)
    hgvs_splice = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
