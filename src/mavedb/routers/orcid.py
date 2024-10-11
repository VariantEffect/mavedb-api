import logging
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import JSONResponse

from mavedb.lib.authorization import require_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.orcid import fetch_orcid_user
from mavedb.models.user import User
from mavedb.view_models import orcid

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/orcid",
    tags=["orcid"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET")


@router.get("/users/{orcid_id}", status_code=200, response_model=orcid.OrcidUser)
def lookup_orcid_user(
    orcid_id: str,
    user: User = Depends(require_current_user),
) -> Any:
    """
    Look an ORCID user up by ORCID ID.

    This capability is needed when adding contributors to an experiment or score set, who may not necessarily be MaveDB
    users.

    Access is limited to signed-in users to prevent abuse.
    """
    save_to_logging_context({"requested_resource": orcid_id})
    orcid_user = fetch_orcid_user(orcid_id)
    if orcid_user is None:
        return JSONResponse(
            status_code=404,
            content={},
        )
    else:
        return orcid_user


@router.post(
    "/token",
    status_code=200,
    response_model=orcid.OrcidAuthTokenResponse,
    responses={404: {}, 500: {}},
    include_in_schema=False,
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
            "redirect_uri": request.redirect_uri,
        }
        response = await client.post(url, data=data)
        if response.status_code == 200:
            data = response.json()
            token_type = data["token_type"]
            access_token = data["access_token"]
            expires_in = data["expires_in"]
            id_token = data["id_token"]

            save_to_logging_context({"token_type": token_type})

            if token_type is None or token_type.lower() != "bearer":
                logger.warning(
                    msg="Unexpected token type received from ORCID when exchanging code for token.",
                    extra=logging_context(),
                )

            return {
                "access_token": access_token,
                "expires_in": expires_in,
                "id_token": id_token,
                "token_type": token_type,
            }
        else:
            data = response.json()
            raise HTTPException(status_code=401, detail="Authentication error")
