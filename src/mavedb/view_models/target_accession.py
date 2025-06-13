from datetime import date
from typing import Optional
from typing_extensions import Self

from pydantic import model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class TargetAccessionBase(BaseModel):
    accession: str
    is_base_editor: bool
    assembly: Optional[str] = None
    gene: Optional[str] = None

    @model_validator(mode="after")
    def check_gene_or_assembly(self) -> Self:
        if self.assembly is None and self.gene is None:
            raise ValidationError(
                f"Could not create {self.__class__.__name__} object: Either a `gene` or `assembly` is required."
            )
        return self


class TargetAccessionModify(TargetAccessionBase):
    # Consider some validation, ie: Accession is in our CDOT data files
    pass


class TargetAccessionCreate(TargetAccessionModify):
    pass


class TargetAccessionUpdate(TargetAccessionModify):
    pass


# Properties shared by models stored in DB
class SavedTargetAccession(TargetAccessionBase):
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetAccession(SavedTargetAccession):
    pass


# Properties to return to admin clients
class AdminTargetAccession(SavedTargetAccession):
    creation_date: date
    modification_date: date
