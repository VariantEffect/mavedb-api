from datetime import date

from mavedb.view_models.base.base import BaseModel, validator
from mavedb.lib.validation import target


class WildTypeSequenceBase(BaseModel):
    sequence_type: str
    sequence: str


class WildTypeSequenceModify(WildTypeSequenceBase):

    @validator('sequence_type')
    def validate_category(cls, v):
        target.validate_sequence_category(v)
        return v

    @validator('sequence')
    def validate_sequence(cls, v):
        target.validate_target_sequence(v)
        return v


class WildTypeSequenceCreate(WildTypeSequenceModify):
    pass


class WildTypeSequenceUpdate(WildTypeSequenceModify):
    pass


# Properties shared by models stored in DB
class SavedWildTypeSequence(WildTypeSequenceBase):
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class WildTypeSequence(SavedWildTypeSequence):
    pass


# Properties to return to admin clients
class AdminWildTypeSequence(SavedWildTypeSequence):
    creation_date: date
    modification_date: date
