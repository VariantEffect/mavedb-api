from mavedb.view_models.base.base import BaseModel


class AuthorBase(BaseModel):
    publication_identifier_id: int


class AuthorCreate(AuthorBase):
    name: str
    primary_author: bool


# Properties shared by models stored in DB
class SavedAuthor(AuthorCreate):
    pass

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class PublicationIdentifier(SavedAuthor):
    pass
