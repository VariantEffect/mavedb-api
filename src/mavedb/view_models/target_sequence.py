from datetime import date
from typing import Optional
from typing_extensions import Self

from fqfa import infer_sequence_type
from pydantic import field_validator, model_validator

from mavedb.lib.identifiers import sanitize_target_sequence_label
from mavedb.lib.validation import target
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.taxonomy import AdminTaxonomy, SavedTaxonomy, Taxonomy, TaxonomyCreate


class TargetSequenceBase(BaseModel):
    sequence_type: str
    sequence: str
    label: Optional[str] = None


class TargetSequenceModify(TargetSequenceBase):
    @field_validator("sequence_type")
    def validate_sequence_category(cls, v: str) -> str:
        v = v.lower()
        target.validate_sequence_category(v)
        return v

    @field_validator("label")
    def label_does_not_include_colon(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None

        if ":" in v:
            raise ValidationError(f"Target sequence label `{v}` may not contain a colon.")

        # Fully qualified variants should never contain whitespace.
        return sanitize_target_sequence_label(v)

    # Validate the target sequence, inferring its type if necessary.
    @model_validator(mode="after")
    def validate_target_sequence(self) -> Self:
        self.sequence = self.sequence.upper()

        if self.sequence_type == "infer":
            inferred_sequence_type = infer_sequence_type(self.sequence)
            if inferred_sequence_type is not None:
                target.validate_sequence_category(inferred_sequence_type)
                self.sequence_type = inferred_sequence_type
            else:
                raise ValueError("Invalid sequence.")

        target.validate_target_sequence(self.sequence, self.sequence_type)
        return self


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
        from_attributes = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetSequence(SavedTargetSequence):
    taxonomy: Taxonomy


# Properties to return to admin clients
class AdminTargetSequence(SavedTargetSequence):
    creation_date: date
    modification_date: date
    taxonomy: AdminTaxonomy
