from typing import Any

from pydantic import field_validator, model_validator

from mavedb.models.target_gene_external_identifier import TargetGeneExternalIdentifier
from mavedb.view_models import external_gene_identifier, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class ExternalGeneIdentifierOffsetBase(BaseModel):
    identifier: external_gene_identifier.ExternalGeneIdentifierBase
    offset: int


class ExternalGeneIdentifierOffsetCreate(ExternalGeneIdentifierOffsetBase):
    identifier: external_gene_identifier.ExternalGeneIdentifierCreate

    @field_validator("offset")
    def validate_offset(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Offset should not be a negative number")
        return v


# Properties shared by models stored in DB
class SavedExternalGeneIdentifierOffset(ExternalGeneIdentifierOffsetBase):
    record_type: str = None  # type: ignore
    identifier: external_gene_identifier.SavedExternalGeneIdentifier

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True

    # A stored row holds the identifier beside its offset. The api nests it, as it did when each database had
    # its own identifier table.
    @model_validator(mode="before")
    @classmethod
    def nest_stored_identifier(cls, data: Any) -> Any:
        if isinstance(data, TargetGeneExternalIdentifier):
            return {
                "identifier": {"db_name": data.db_name, "identifier": data.identifier, "url": data.url},
                "offset": data.offset,
            }
        return data


# Properties to return to non-admin clients
class ExternalGeneIdentifierOffset(SavedExternalGeneIdentifierOffset):
    identifier: external_gene_identifier.ExternalGeneIdentifier
