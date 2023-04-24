from datetime import date

from mavedb.view_models.base.base import BaseModel, validator
from mavedb.lib.validation import target

from fqfa import infer_sequence_type


class WildTypeSequenceBase(BaseModel):
    sequence_type: str
    sequence: str


class WildTypeSequenceModify(WildTypeSequenceBase):
    @validator("sequence_type")
    def validate_category(cls, field_value, values, field, config):
        field_value = field_value.lower()
        target.validate_sequence_category(field_value)
        if field_value == "infer":
            if "sequence" in values.keys():
                field_value = infer_sequence_type(values["sequence"])
            else:
                raise ValueError("sequence is invalid")
        return field_value

    @validator("sequence")
    def validate_identifier(cls, field_value, values, field, config):
        # If sequence_type is invalid, values["sequence_type"] doesn't exist.
        field_value = field_value.upper()
        if "sequence_type" in values.keys():
            sequence_type = values["sequence_type"]
            # field_value is sequence
            target.validate_target_sequence(field_value, sequence_type)
        else:
            raise ValueError("sequence_type is invalid")
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
