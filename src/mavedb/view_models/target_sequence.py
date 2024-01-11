from datetime import date
from typing import Optional

from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.taxonomy import Taxonomy
from mavedb.lib.validation import target
from mavedb.lib.validation.exceptions import ValidationError

from fqfa import infer_sequence_type


class TargetSequenceBase(BaseModel):
    sequence_type: str
    sequence: str
    label: Optional[str]
    taxonomy: Taxonomy


class TargetSequenceModify(TargetSequenceBase):
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

    @validator("label")
    def check_alphanumeric(cls, field_value, values, field, config) -> str:
        if isinstance(field_value, str):
            is_alphanumeric = field_value.replace("_", "").isalnum()
            if not is_alphanumeric:
                raise ValidationError(
                    f"Target sequence label `{field_value}` can contain only letters, numbers, and underscores."
                )
        return field_value


class TargetSequenceCreate(TargetSequenceModify):
    pass


class TargetSequenceUpdate(TargetSequenceModify):
    pass


# Properties shared by models stored in DB
class SavedTargetSequence(TargetSequenceBase):
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetSequence(SavedTargetSequence):
    pass


# Properties to return to admin clients
class AdminTargetSequence(SavedTargetSequence):
    creation_date: date
    modification_date: date
