from mavedb.lib.validation.publication import validate_publication, validate_db_name
from mavedb.view_models.base.base import BaseModel, validator
from datetime import date
from typing import Optional

import logging

logger = logging.getLogger(__name__)


class PublicationIdentifierBase(BaseModel):
    identifier: str
    db_name: Optional[str]


class PublicationIdentifierCreate(PublicationIdentifierBase):
    @validator("identifier")
    def validate_publication(cls, v):
        validate_publication(identifier=v)
        return v

    @validator("db_name")
    def validate_publication_db(cls, v):
        validate_db_name(db_name=v)
        return v


# Properties of external publication identifiers
class ExternalPublicationIdentifier(PublicationIdentifierBase):
    url: str
    reference_html: str
    title: str
    abstract: str
    authors: list[dict[str, str]]
    publication_doi: Optional[str]
    preprint_doi: Optional[str]
    publication_year: Optional[int]
    preprint_date: Optional[date]
    publication_journal: Optional[str]

    class Config:
        orm_mode = True


# Properties shared by models stored in DB
class SavedPublicationIdentifier(ExternalPublicationIdentifier):
    id: int


class ValidatedPublicationIdentifier(PublicationIdentifierBase):
    valid_for: dict[str, bool]


# Properties to return to non-admin clients
class PublicationIdentifier(SavedPublicationIdentifier):
    pass
