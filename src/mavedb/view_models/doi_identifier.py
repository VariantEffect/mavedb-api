import idutils

from pydantic import field_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class DoiIdentifierBase(BaseModel):
    identifier: str


class DoiIdentifierCreate(DoiIdentifierBase):
    @field_validator("identifier")
    def must_be_valid_doi(cls, v: str):
        if not idutils.is_doi(v):
            raise ValidationError("'{}' is not a valid DOI identifier.".format(v))
        return v


# Properties shared by models stored in DB
class SavedDoiIdentifier(DoiIdentifierBase):
    id: int
    record_type: str = None  # type: ignore
    url: str

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to non-admin clients
class DoiIdentifier(SavedDoiIdentifier):
    pass
