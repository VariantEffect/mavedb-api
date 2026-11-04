from sqlalchemy import Column, ForeignKey, Integer, String

from mavedb.db.base import Base
from mavedb.lib.external_identifier_urls import doi_url
from mavedb.models.mixins.external_identifier import ExternalIdentifier


class DoiIdentifierLink(ExternalIdentifier):
    """A DOI cited by one record. DOIs are case-insensitive, so the identifier is stored upper-cased, as the DOI
    registry stores it."""

    db_name = "DOI"

    @property
    def url(self) -> str:
        return doi_url(self.identifier)


class ScoreSetDoiIdentifier(DoiIdentifierLink, Base):
    __tablename__ = "scoreset_doi_identifiers"

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), primary_key=True)
    identifier = Column(String, primary_key=True)


class ExperimentDoiIdentifier(DoiIdentifierLink, Base):
    __tablename__ = "experiment_doi_identifiers"

    experiment_id = Column(Integer, ForeignKey("experiments.id"), primary_key=True)
    identifier = Column(String, primary_key=True)


class ExperimentSetDoiIdentifier(DoiIdentifierLink, Base):
    __tablename__ = "experiment_set_doi_identifiers"

    experiment_set_id = Column(Integer, ForeignKey("experiment_sets.id"), primary_key=True)
    identifier = Column(String, primary_key=True)
