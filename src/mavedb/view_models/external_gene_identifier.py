from typing import Optional

from mavedb.view_models.base.base import BaseModel


class ExternalGeneIdentifierBase(BaseModel):
    db_name: str
    identifier: str


class ExternalGeneIdentifierCreate(ExternalGeneIdentifierBase):
    pass


# Properties shared by models stored in DB
class SavedExternalGeneIdentifier(ExternalGeneIdentifierBase):
    db_version: Optional[str]
    url: Optional[str]
    reference_html: Optional[str]

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ExternalGeneIdentifier(SavedExternalGeneIdentifier):
    pass
