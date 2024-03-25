from fastapi import HTTPException
import httpx
import json
import os
from typing import Any

from fastapi import APIRouter

from mavedb.view_models import orcid

router = APIRouter(prefix="/api/v1/orcid", tags=["orcid"], responses={404: {"description": "Not found"}})

ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET")


@router.post(
    "/token", status_code=200, response_model=orcid.OrcidAuthTokenResponse, responses={404: {}, 500: {}}, include_in_schema=False
)
async def get_token_from_code(*, request: orcid.OrcidAuthTokenRequest) -> Any:
    """
    Perform the second step of three-legged Oauth2. The user has signed into ORCID in the browser, and the client has
    obtained a code. We must now send this code to ORCID with our client secret, and ORCID will reply with a token.
    """
    async with httpx.AsyncClient() as client:
        url = "https://orcid.org/oauth/token"
        data = {
            "client_id": ORCID_CLIENT_ID,
            "client_secret": ORCID_CLIENT_SECRET,
            "code": request.code,
            "grant_type": "authorization_code",
            "redirect_uri": request.redirect_uri
        }
        response = await client.post(url, data=data)
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data))
            token_type = data["token_type"] # Fixed. Should be "Bearer"
            access_token = data["access_token"]
            expires_in = data["expires_in"]
            id_token = data["id_token"]
            return {
                "access_token": access_token,
                "expires_in": expires_in,
                "id_token": id_token,
                "token_type": token_type,
            }
        else:
            data = response.json()
            print(json.dumps(data))
            raise HTTPException(status_code=401, detail=f"Authentication error")
