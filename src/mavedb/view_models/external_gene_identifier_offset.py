from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models import external_gene_identifier
from mavedb.lib.validation import identifier


class ExternalGeneIdentifierOffsetBase(BaseModel):
    identifier: external_gene_identifier.ExternalGeneIdentifierBase
    offset: int


class ExternalGeneIdentifierOffsetCreate(ExternalGeneIdentifierOffsetBase):
    identifier: external_gene_identifier.ExternalGeneIdentifierCreate

    @validator('identifier')
    def validate_identifier(cls, v):
        identifier.validate_external_identifier(v)
        return v


# Properties shared by models stored in DB
class SavedExternalGeneIdentifierOffset(ExternalGeneIdentifierOffsetBase):
    identifier: external_gene_identifier.SavedExternalGeneIdentifier

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ExternalGeneIdentifierOffset(SavedExternalGeneIdentifierOffset):
    identifier: external_gene_identifier.ExternalGeneIdentifier
