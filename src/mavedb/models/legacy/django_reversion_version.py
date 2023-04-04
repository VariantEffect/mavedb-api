from sqlalchemy import Column, Integer, String

from mavedb.db.base import Base


class DjangoReversionVersion(Base):
    __tablename__ = "reversion_version"

    id = Column(Integer, primary_key=True, index=True)
    object_id = Column(String(191), nullable=False)
    format = Column(String(255), nullable=False)
    serialized_data = Column(String, nullable=False)
    object_repr = Column(String, nullable=False)
    content_type_id = Column(Integer, nullable=False)
    revision_id = Column(Integer, nullable=False)
    db = Column(String(191), nullable=True)
