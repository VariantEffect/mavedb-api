from datetime import date

from mavedb.view_models.base.base import BaseModel


class TargetAccessionBase(BaseModel):
    accession: str
    assembly: str


class TargetAccessionModify(TargetAccessionBase):
    # Consider some validation, ie: Accession is in our SeqRepo instance
    pass


class TargetAccessionCreate(TargetAccessionModify):
    pass


class TargetAccessionUpdate(TargetAccessionModify):
    pass


# Properties shared by models stored in DB
class SavedTargetAccession(TargetAccessionBase):
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetAccession(SavedTargetAccession):
    pass


# Properties to return to admin clients
class AdminTargetAccession(SavedTargetAccession):
    creation_date: date
    modification_date: date
