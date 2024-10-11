import logging
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

from fastapi import Depends, Header, HTTPException, Request, Security
from fastapi.security import (
    APIKeyCookie,
    APIKeyHeader,
    APIKeyQuery,
    HTTPAuthorizationCredentials,
    HTTPBearer,
)
from jose import jwt
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.lib.orcid import fetch_orcid_user_email
from mavedb.models.access_key import AccessKey
from mavedb.models.enums.user_role import UserRole
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
ORCID_JWT_AUDIENCE = os.getenv("ORCID_CLIENT_ID")

ACCESS_TOKEN_NAME = "X-API-key"

logger = logging.getLogger(__name__)


class AuthenticationMethod(str, Enum):
    api_key = "api_key"
    jwt = "jwt"


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
        save_to_logging_context(format_raised_exception_info_as_dict(ex))
        logger.debug(msg="Failed to authenticate user; Could not decode user token.", extra=logging_context())
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
                save_to_logging_context({"scheme": credentials.scheme})
                logger.info(msg="Failed to authenticate user; Invalid authentication scheme.", extra=logging_context())
                raise HTTPException(status_code=403, detail="Invalid authentication scheme.")

            token_payload = self.verify_jwt(credentials.credentials)

            if not token_payload:
                logger.info(msg="Failed to authenticate user; Invalid or expired token.", extra=logging_context())
                raise HTTPException(status_code=403, detail="Invalid token or expired token.")

            logger.debug(msg="Successfully acquired JWT.", extra=logging_context())
            return token_payload

        else:
            logger.debug(msg="Failed to authenticate user; No credentials were provided.", extra=logging_context())
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
            if access_key.role is not None:
                roles = [access_key.role] if access_key.role is not None else []

            save_to_logging_context(
                {
                    "user": user.id,
                    "active_roles": [role.name for role in roles],
                    "available_roles": [role.name for role in user.roles],
                }
            )

    if user is not None:
        logger.debug(msg="Successfully authenticated API key for user.", extra=logging_context())
        return UserData(user, roles)

    logger.debug(msg="Failed to authenticate user via API key; No key provided.", extra=logging_context())
    return None


####################################################################################################
# Main authentication methods
####################################################################################################


async def get_current_user(
    api_key_user_data: Optional[UserData] = Depends(get_current_user_data_from_api_key),
    token_payload: Optional[dict] = Depends(JWTBearer()),
    db: Session = Depends(deps.get_db),
    # Custom header for the role the authenticated user would like to assume.
    # Namespaced with x_ to indicate this is a custom application header.
    x_active_roles: Optional[str] = Header(default=None),
) -> Optional[UserData]:
    save_to_logging_context({"requested_roles": x_active_roles})

    if api_key_user_data is not None:
        save_to_logging_context({"auth_method": AuthenticationMethod.api_key, "user_authenticated": True})
        logger.info(msg="Successfully authenticated user via API key.", extra=logging_context())
        return api_key_user_data

    if token_payload is None:
        save_to_logging_context({"auth_method": None, "user_authenticated": False})
        logger.info(
            msg="Failed to authenticate user; Could not acquire credentials via API key or JWT.",
            extra=logging_context(),
        )
        return None

    username: Optional[str] = token_payload.get("sub")
    if username is None:
        save_to_logging_context({"auth_method": AuthenticationMethod.jwt, "user_authenticated": False})
        logger.info(msg="Failed to authenticate user; Username not present in token payload.", extra=logging_context())
        return None

    user = db.query(User).filter(User.username == username).one_or_none()

    # If there was a token payload, auth method must be JWT.
    save_to_logging_context({"auth_method": AuthenticationMethod.jwt})

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
        save_to_logging_context(
            {
                "user": user.id,
                "first_login": user.is_first_login,
                "user_authenticated": True,
            }
        )

        logger.debug(msg="Created new user.", extra=logging_context())

    elif not user.is_active:
        save_to_logging_context({"user": user.id, "user_authenticated": True})
        logger.info(msg="Failed to authenticate user; User is inactive.", extra=logging_context())
        return None
    else:
        user.last_login = datetime.now()
        user.is_first_login = False
        save_to_logging_context(
            {
                "user": user.id,
                "first_login": user.is_first_login,
                "user_authenticated": True,
            }
        )

    db.add(user)
    db.commit()
    db.refresh(user)
    logger.info(msg="Successfully authenticated user via JWT.", extra=logging_context())

    # When no roles are requested, the user may act as any assigned role.
    save_to_logging_context({"available_roles": [role.name for role in user.roles]})
    if x_active_roles is None:
        save_to_logging_context({"active_roles": [role.name for role in user.roles]})
        logger.info(msg="Successfully assigned user roles to authenticated user.", extra=logging_context())
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
            logger.debug(msg=f"Ignoring unknown requested role {requested_role}.", extra=logging_context())
            continue

        enumerated_role = UserRole[requested_role]
        if enumerated_role not in user.roles:
            logger.warning(msg="User requested role to which they do not belong.", extra=logging_context())
            raise HTTPException(
                status_code=403,
                detail="This user is not a member of the requested acting role.",
            )
        else:
            active_roles.append(enumerated_role)

    save_to_logging_context({"active_roles": [role.name for role in active_roles]})
    logger.info(msg="Successfully assigned user roles to authenticated user.", extra=logging_context())
    return UserData(user, active_roles)
