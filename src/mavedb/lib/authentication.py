from dataclasses import dataclass
from datetime import datetime
import logging
import os
from typing import Optional
import os

from fastapi import Depends, HTTPException, Request, Security, Header
from fastapi.security import APIKeyCookie, APIKeyHeader, APIKeyQuery, HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.models.enums.user_role import UserRole
from mavedb.lib.orcid import fetch_orcid_user_email
from mavedb.models.access_key import AccessKey
from mavedb.models.user import User

ORCID_JWT_SIGNING_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAjxTIntA7YvdfnYkLSN4w
k//E2zf/wbb0SV/HLHFvh6a9ENVRD1/rHK0EijlBzikb+1rgDQihJETcgBLsMoZV
QqGj8fDUUuxnVHsuGav/bf41PA7E/58HXKPrB2C0cON41f7K3o9TStKpVJOSXBrR
WURmNQ64qnSSryn1nCxMzXpaw7VUo409ohybbvN6ngxVy4QR2NCC7Fr0QVdtapxD
7zdlwx6lEwGemuqs/oG5oDtrRuRgeOHmRps2R6gG5oc+JqVMrVRv6F9h4ja3UgxC
DBQjOVT1BFPWmMHnHCsVYLqbbXkZUfvP2sO1dJiYd/zrQhi+FtNth9qrLLv3gkgt
wQIDAQAB
-----END PUBLIC KEY-----
"""
ORCID_JWT_AUDIENCE = "APP-GXFVWWJT8H0F50WD"

ACCESS_TOKEN_NAME = "X-API-key"

logger = logging.getLogger(__name__)


@dataclass
class UserData:
    user: User
    active_roles: list[UserRole]


####################################################################################################
# JWT authentication
####################################################################################################


def decode_jwt(token: str) -> dict:
    try:
        decoded_token = jwt.decode(
            token,
            ORCID_JWT_SIGNING_PUBLIC_KEY,
            algorithms=["RS256"],
            audience=ORCID_JWT_AUDIENCE,
            # ORCID sends an at_hash when using the OpenID Connect implicit flow, even though there is no auth_token.
            options={"verify_at_hash": False},
        )
        return decoded_token
    # TODO: should catch specific exceptions, and should log them more usefully.
    except Exception as ex:
        print(ex)
        return {}


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: Optional[HTTPAuthorizationCredentials]
        try:
            credentials = await super(JWTBearer, self).__call__(request)
        except HTTPException:
            credentials = None
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(status_code=403, detail="Invalid authentication scheme.")
            token_payload = self.verify_jwt(credentials.credentials)
            if not token_payload:
                raise HTTPException(status_code=403, detail="Invalid token or expired token.")
            return token_payload
        else:
            return None

    @staticmethod
    def verify_jwt(token: str) -> dict:
        return decode_jwt(token)


####################################################################################################
# API key authentication
####################################################################################################

access_token_query = APIKeyQuery(name=ACCESS_TOKEN_NAME, auto_error=False)
access_token_header = APIKeyHeader(name=ACCESS_TOKEN_NAME, auto_error=False)
access_token_cookie = APIKeyCookie(name=ACCESS_TOKEN_NAME, auto_error=False)


async def get_access_token(
    # access_token_query: str = Security(access_token_query),
    access_token_header: Optional[str] = Security(access_token_header),
    access_token_cookie: Optional[str] = Security(access_token_cookie),
) -> Optional[str]:
    return access_token_header or access_token_cookie


async def get_current_user_data_from_api_key(
    db: Session = Depends(deps.get_db), access_token: str = Depends(get_access_token)
) -> Optional[UserData]:
    user = None
    roles: list[UserRole] = []
    if access_token is not None:
        access_key = db.query(AccessKey).filter(AccessKey.key_id == access_token).one_or_none()
        if access_key:
            user = access_key.user
            roles = [access_key.role] if access_key.role is not None else []

    return UserData(user, roles) if user else None


####################################################################################################
# Main authentication methods
####################################################################################################


async def get_current_user(
    api_key_user_data: Optional[UserData] = Depends(get_current_user_data_from_api_key),
    token_payload: dict = Depends(JWTBearer()),
    db: Session = Depends(deps.get_db),
    # Custom header for the role the authenticated user would like to assume.
    # Namespaced with x_ to indicate this is a custom application header.
    x_active_roles: Optional[str] = Header(default=None),
) -> Optional[UserData]:
    if api_key_user_data is not None:
        return api_key_user_data

    if token_payload is None:
        return None

    username: Optional[str] = token_payload.get("sub")
    if username is None:
        return None

    user = db.query(User).filter(User.username == username).one_or_none()

    # A new user has just connected an ORCID iD. Create the user account.
    if user is None:
        # A new user has just connected an ORCID iD. Fetch their email address if it's visible, and create the
        # user account.
        email = fetch_orcid_user_email(username)
        user = User(
            username=username,
            is_active=True,
            # TODO When we decouple from the old database, change first_name and last_name to be nullable, and
            # stop filling them with empty strings.
            first_name=token_payload["given_name"] if "given_name" in token_payload else "",
            last_name=token_payload["family_name"] if "family_name" in token_payload else "",
            date_joined=datetime.now(),
            email=email,
            is_first_login=True,
        )
        logger.info(f"Creating new user with username {user.username}")

        db.add(user)
        db.commit()
        db.refresh(user)

    elif not user.is_active:
        return None
    else:
        user.last_login = datetime.now()
        user.is_first_login = False

    db.add(user)
    db.commit()
    db.refresh(user)

    if x_active_roles is None:
        return UserData(user, user.roles)

    # FastAPI has poor support for headers of type list (really, they are just comma separated strings).
    # Parse out any requested roles manually.
    requested_roles = x_active_roles.split(",")

    active_roles: list[UserRole] = []
    for requested_role in requested_roles:
        # Disregard any requested roles if they do not correspond to one of our UserRole enumerations.
        #
        # NOTE that our permissions structure ensures every authenticated user has a minimum set of available actions.
        # Collectively, these are known to the client as the 'ordinary user' role. Because these permissions are supplied
        # by default, we do not need add the 'ordinary user' role to the list of active roles.
        if requested_role not in UserRole._member_names_:
            continue

        enumerated_role = UserRole[requested_role]
        if enumerated_role not in user.roles:
            raise HTTPException(status_code=403, detail="This user is not a member of the requested acting role.")
        else:
            active_roles.append(enumerated_role)

    return UserData(user, active_roles)
