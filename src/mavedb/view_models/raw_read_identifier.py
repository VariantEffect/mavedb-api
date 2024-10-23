from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class RawReadIdentifierBase(BaseModel):
    identifier: str


class RawReadIdentifierCreate(RawReadIdentifierBase):
    pass


# Properties shared by models stored in DB
class SavedRawReadIdentifier(RawReadIdentifierBase):
    id: int
    record_type: str = None  # type: ignore
    url: str

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class RawReadIdentifier(SavedRawReadIdentifier):
    pass
