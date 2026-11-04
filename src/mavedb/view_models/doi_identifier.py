from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.components.doi import Doi


class DoiIdentifierBase(BaseModel):
    identifier: str


class DoiIdentifierCreate(DoiIdentifierBase):
    identifier: Doi


# Properties shared by models stored in DB
class SavedDoiIdentifier(DoiIdentifierBase):
    record_type: str = None  # type: ignore
    url: str

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to non-admin clients
class DoiIdentifier(SavedDoiIdentifier):
    pass
