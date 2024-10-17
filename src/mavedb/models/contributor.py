from sqlalchemy import Column, Integer, String

from mavedb.db.base import Base


class Contributor(Base):
    __tablename__ = "contributors"

    id = Column(Integer, primary_key=True)
    orcid_id = Column(String, index=True, nullable=False, unique=True)
    given_name = Column(String, nullable=True)
    family_name = Column(String, nullable=True)
