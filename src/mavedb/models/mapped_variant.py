from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship, backref, Mapped

from mavedb.db.base import Base
from mavedb.deps import JSONB
from .variant import Variant


class MappedVariant(Base):
    __tablename__ = "mapped_variants"

    id = Column(Integer, primary_key=True, index=True)

    pre_mapped = Column(JSONB, nullable=False)
    post_mapped = Column(JSONB, nullable=False)

    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    variant : Mapped[Variant] = relationship("Variant", backref=backref("mapped_variants", cascade="all,delete-orphan"))
