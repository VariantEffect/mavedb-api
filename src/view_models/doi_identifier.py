from src.view_models.base.base import BaseModel


class DoiIdentifierBase(BaseModel):
    identifier: str


class DoiIdentifierCreate(DoiIdentifierBase):
    pass


# Properties shared by models stored in DB
class SavedDoiIdentifier(DoiIdentifierBase):
    id: int
    url: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class DoiIdentifier(SavedDoiIdentifier):
    pass
