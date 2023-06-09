import idutils

from mavedb.view_models.base.base import BaseModel, validator


class PublicationIdentifierBase(BaseModel):
    identifier: str


class PublicationIdentifierCreate(PublicationIdentifierBase):
    @validator("identifier")
    def must_be_valid_pubmed(cls, v):
        if not idutils.is_pmid(v):
            # ValidationError shows weird error and test can't catch it.
            raise ValueError("{} is not a valid PubMed identifier.".format(v))
        return v


# Properties shared by models stored in DB
class SavedPublicationIdentifier(PublicationIdentifierBase):
    id: int
    url: str
    reference_html: str

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class PublicationIdentifier(SavedPublicationIdentifier):
    pass
