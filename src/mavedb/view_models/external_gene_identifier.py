from typing import Optional

from mavedb.view_models.base.base import BaseModel, validator
from mavedb.lib.validation import identifier as identifier_validator


class ExternalGeneIdentifierBase(BaseModel):
    db_name: str
    identifier: str


class ExternalGeneIdentifierCreate(ExternalGeneIdentifierBase):
    @validator("db_name")
    def validate_db_name(cls, value):
        identifier_validator.validate_db_name(value)
        return value

    @validator("identifier")
    def validate_identifier(cls, field_value, values, field, config):
        # if db_name is none, values["db_name"] will raise an error.
        if "db_name" in values.keys():
            db_name = values["db_name"]
            # field_value is identifier
            identifier_validator.validate_identifier(db_name, field_value)
        return field_value


# Properties shared by models stored in DB
class SavedExternalGeneIdentifier(ExternalGeneIdentifierBase):
    db_version: Optional[str]
    url: Optional[str]
    reference_html: Optional[str]

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ExternalGeneIdentifier(SavedExternalGeneIdentifier):
    pass
