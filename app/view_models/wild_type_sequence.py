from datetime import date

from app.view_models.base.base import BaseModel


class WildTypeSequenceBase(BaseModel):
    sequence_type: str
    sequence: str


class WildTypeSequenceCreate(WildTypeSequenceBase):
    pass


class WildTypeSequenceUpdate(WildTypeSequenceBase):
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
