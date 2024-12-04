from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base

from .variant import Variant


class MappedVariant(Base):
    __tablename__ = "mapped_variants"

    id = Column(Integer, primary_key=True)

    pre_mapped = Column(JSONB(none_as_null=True), nullable=True)
    post_mapped = Column(JSONB(none_as_null=True), nullable=True)
    vrs_version = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    mapped_date = Column(Date, nullable=False)
    mapping_api_version = Column(String, nullable=False)
    current = Column(Boolean, nullable=False)

    variant_id = Column(Integer, ForeignKey("variants.id"), index=True, nullable=False)
    variant: Mapped[Variant] = relationship("Variant", backref=backref("mapped_variants", cascade="all,delete-orphan"))
