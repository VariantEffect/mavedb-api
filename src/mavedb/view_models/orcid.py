from typing import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class OrcidAuthTokenRequest(BaseModel):
    code: str
    redirect_uri: str


class OrcidAuthTokenResponse(BaseModel):
    access_token: str
    expires_in: int
    id_token: str
    token_type: str


class OrcidUser(BaseModel):
    record_type: str = None  # type: ignore
    orcid_id: str
    given_name: Optional[str]
    family_name: Optional[str]

    _record_type_factory = record_type_validator()(set_record_type)
