from datetime import date
from typing import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator


class TargetAccessionBase(BaseModel):
    accession: str
    assembly: Optional[str]
    gene: Optional[str]

    @validator("gene", always=True)
    def check_gene_or_assembly(cls, gene, values):
        if "assembly" not in values and not gene:
            raise ValueError("either a `gene` or `assembly` is required")
        return gene


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
        orm_mode = True
        arbitrary_types_allowed = True


# Properties to return to non-admin clients
class TargetAccession(SavedTargetAccession):
    pass


# Properties to return to admin clients
class AdminTargetAccession(SavedTargetAccession):
    creation_date: date
    modification_date: date
