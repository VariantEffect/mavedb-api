from datetime import date

from sqlalchemy import Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref, Mapped
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.db.base import Base
from .variant import Variant


class MappedVariant(Base):
    __tablename__ = "mapped_variants"

    id = Column(Integer, primary_key=True)

    pre_mapped = Column(JSONB, nullable=True)
    post_mapped = Column(JSONB, nullable=True)
    vrs_version = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    mapped_date = Column(Date, nullable=False)
    mapping_api_version = Column(String, nullable=False)

    variant_id = Column(Integer, ForeignKey("variants.id"), index=True, nullable=False)
    variant: Mapped[Variant] = relationship("Variant", backref=backref("mapped_variants", cascade="all,delete-orphan"))
