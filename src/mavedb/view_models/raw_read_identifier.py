from mavedb.view_models.base.base import BaseModel


class RawReadIdentifierBase(BaseModel):
    identifier: str


class RawReadIdentifierCreate(RawReadIdentifierBase):
    pass


# Properties shared by models stored in DB
class SavedRawReadIdentifier(RawReadIdentifierBase):
    id: int
    url: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class RawReadIdentifier(SavedRawReadIdentifier):
    pass
