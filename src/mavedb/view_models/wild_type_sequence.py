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
    def validate_identifier(cls, field_value, values, field, config):
        # If sequence_type is invalid, values["sequence_type"] doesn't exist.
        if "sequence_type" in values.keys():
            sequence_type = values["sequence_type"]
            # field_value is sequence
            target.validate_target_sequence(sequence_type, field_value)
        else:
            raise ValueError("sequence_type is invalid.")
        return field_value

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
