from sqlalchemy import Column, ForeignKey, Integer, String

from mavedb.db.base import Base
from mavedb.lib.external_identifier_urls import raw_read_url
from mavedb.models.mixins.external_identifier import ExternalIdentifier


class RawReadIdentifierLink(ExternalIdentifier):
    """A raw read accession (SRA, BioProject, GEO or ArrayExpress) cited by one record."""

    db_name = "SRA"

    @property
    def url(self) -> str:
        return raw_read_url(self.identifier)


class ScoreSetRawReadIdentifier(RawReadIdentifierLink, Base):
    __tablename__ = "scoreset_sra_identifiers"

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), primary_key=True)
    identifier = Column(String, primary_key=True)


class ExperimentRawReadIdentifier(RawReadIdentifierLink, Base):
    __tablename__ = "experiment_sra_identifiers"

    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    identifier = Column(String, primary_key=True)


class ExperimentSetRawReadIdentifier(RawReadIdentifierLink, Base):
    __tablename__ = "experiment_set_sra_identifiers"

    experiment_set_id = Column(Integer, ForeignKey("experiment_sets.id"), primary_key=True)
    identifier = Column(String, primary_key=True)
