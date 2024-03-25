from mavedb.view_models.base.base import BaseModel


class OrcidAuthTokenRequest(BaseModel):
    code: str
    redirect_uri: str


class OrcidAuthTokenResponse(BaseModel):
    access_token: str
    expires_in: int
    id_token: str
    token_type: str
