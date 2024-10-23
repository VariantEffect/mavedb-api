from mavedb.view_models import external_gene_identifier, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator


class ExternalGeneIdentifierOffsetBase(BaseModel):
    identifier: external_gene_identifier.ExternalGeneIdentifierBase
    offset: int


class ExternalGeneIdentifierOffsetCreate(ExternalGeneIdentifierOffsetBase):
    identifier: external_gene_identifier.ExternalGeneIdentifierCreate

    @validator("offset")
    def validate_offset(cls, v):
        if v < 0:
            raise ValueError("Offset should not be a negative number")
        return v


# Properties shared by models stored in DB
class SavedExternalGeneIdentifierOffset(ExternalGeneIdentifierOffsetBase):
    record_type: str = None  # type: ignore
    identifier: external_gene_identifier.SavedExternalGeneIdentifier

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ExternalGeneIdentifierOffset(SavedExternalGeneIdentifierOffset):
    identifier: external_gene_identifier.ExternalGeneIdentifier
