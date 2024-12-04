import logging
from typing import Optional

from mavedb.lib.identifiers import PublicationAuthors
from mavedb.lib.validation.publication import validate_db_name, validate_publication
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator

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
    record_type: str = None  # type: ignore
    title: str
    authors: list[PublicationAuthors]

    abstract: Optional[str]
    doi: Optional[str]
    publication_year: Optional[int]
    publication_journal: Optional[str]
    url: Optional[str]
    reference_html: Optional[str]

    _record_type_factory = record_type_validator()(set_record_type)

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
