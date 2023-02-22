from src.view_models.base.base import BaseModel


class PubmedIdentifierBase(BaseModel):
    identifier: str


class PubmedIdentifierCreate(PubmedIdentifierBase):
    pass


# Properties shared by models stored in DB
class SavedPubmedIdentifier(PubmedIdentifierBase):
    id: int
    url: str
    reference_html: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class PubmedIdentifier(SavedPubmedIdentifier):
    pass
