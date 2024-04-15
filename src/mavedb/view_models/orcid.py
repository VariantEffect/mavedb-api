from typing import Optional

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
    orcid_id: str
    first_name: Optional[str]
    last_name: Optional[str]
