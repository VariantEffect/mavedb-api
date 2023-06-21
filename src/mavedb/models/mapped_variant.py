from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship, backref

from mavedb.db.base import Base
from mavedb.deps import JSONB

class MappedVariant(Base):
    __tablename__ = "mapped_variants"

    id = Column(Integer, primary_key=True, index=True)

    pre_mapped = Column(JSONB, nullable=False)
    post_mapped = Column(JSONB, nullable=False)

    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    variant = relationship("Variant", backref=backref("mapped_variants", cascade="all,delete-orphan"))