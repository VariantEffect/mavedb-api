from typing import Optional
from typing_extensions import Self

from pydantic import field_validator, model_validator

from mavedb.lib.validation import identifier as identifier_validator
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ExternalGeneIdentifierBase(BaseModel):
    db_name: str
    identifier: str


class ExternalGeneIdentifierCreate(ExternalGeneIdentifierBase):
    @field_validator("db_name")
    def validate_db_name(cls, value: str) -> str:
        identifier_validator.validate_db_name(value)
        return value

    @model_validator(mode="after")
    def validate_identifier(self) -> Self:
        identifier_validator.validate_identifier(self.db_name, self.identifier)
        return self


# Properties shared by models stored in DB
class SavedExternalGeneIdentifier(ExternalGeneIdentifierBase):
    record_type: str = None  # type: ignore
    db_version: Optional[str] = None
    url: Optional[str] = None
    reference_html: Optional[str] = None

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to non-admin clients
class ExternalGeneIdentifier(SavedExternalGeneIdentifier):
    pass
