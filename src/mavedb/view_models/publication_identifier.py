import logging
from typing import Optional

from pydantic import field_validator

from mavedb.lib.identifiers import PublicationAuthors
from mavedb.lib.validation.publication import validate_db_name, validate_publication
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel

logger = logging.getLogger(__name__)


class PublicationIdentifierBase(BaseModel):
    identifier: str
    db_name: Optional[str] = None


class PublicationIdentifierCreate(PublicationIdentifierBase):
    @field_validator("identifier")
    def validate_publication(cls, v: str) -> str:
        validate_publication(identifier=v)
        return v

    @field_validator("db_name")
    def validate_publication_db(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None

        validate_db_name(db_name=v)
        return v


# Properties of external publication identifiers
class ExternalPublicationIdentifier(PublicationIdentifierBase):
    record_type: str = None  # type: ignore
    title: str
    authors: list[PublicationAuthors]

    abstract: Optional[str] = None
    doi: Optional[str] = None
    publication_year: Optional[int] = None
    publication_journal: Optional[str] = None
    url: Optional[str] = None
    reference_html: Optional[str] = None

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties shared by models stored in DB
class SavedPublicationIdentifier(ExternalPublicationIdentifier):
    id: int


class ValidatedPublicationIdentifier(PublicationIdentifierBase):
    valid_for: dict[str, bool]


# Properties to return to non-admin clients
class PublicationIdentifier(SavedPublicationIdentifier):
    pass
