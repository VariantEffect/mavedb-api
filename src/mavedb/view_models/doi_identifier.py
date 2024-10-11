import idutils

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models.base.base import BaseModel, validator


class DoiIdentifierBase(BaseModel):
    identifier: str


class DoiIdentifierCreate(DoiIdentifierBase):
    @validator("identifier")
    def must_be_valid_doi(cls, v):
        if not idutils.is_doi(v):
            raise ValidationError("'{}' is not a valid DOI identifier.".format(v))
        return v


# Properties shared by models stored in DB
class SavedDoiIdentifier(DoiIdentifierBase):
    id: int
    url: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class DoiIdentifier(SavedDoiIdentifier):
    pass
