from datetime import date
from typing import Optional

from fqfa import infer_sequence_type

from mavedb.lib.validation import target
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.taxonomy import AdminTaxonomy, SavedTaxonomy, Taxonomy, TaxonomyCreate


def sanitize_target_sequence_label(label: str):
    return label.strip().replace(" ", "_")


class TargetSequenceBase(BaseModel):
    sequence_type: str
    sequence: str
    label: Optional[str]


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
    def label_does_not_include_colon(cls, field_value, values, field, config) -> str:
        if isinstance(field_value, str):
            if ":" in field_value:
                raise ValidationError(f"Target sequence label `{field_value}` may not contain a colon.")

            # Sanitize the label by stripping leading/trailing whitespace and replacing any internal whitespace with
            # underscores. Fully qualified variants should never contain whitespace.
            return sanitize_target_sequence_label(field_value)

        return field_value


class TargetSequenceCreate(TargetSequenceModify):
    taxonomy: TaxonomyCreate


class TargetSequenceUpdate(TargetSequenceModify):
    pass


# Properties shared by models stored in DB
class SavedTargetSequence(TargetSequenceBase):
    record_type: str = None  # type: ignore
    taxonomy: SavedTaxonomy

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetSequence(SavedTargetSequence):
    taxonomy: Taxonomy


# Properties to return to admin clients
class AdminTargetSequence(SavedTargetSequence):
    creation_date: date
    modification_date: date
    taxonomy: AdminTaxonomy
