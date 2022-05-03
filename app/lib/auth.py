from datetime import datetime
from typing import Optional

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt
from sqlalchemy.orm import Session
from starlette import status

from app import deps
from app.models.user import User

# See https://8gwifi.org/jwkconvertfunctions.jsp
ORCID_JWT_SIGNING_PUBLIC_KEY = '''-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAjxTIntA7YvdfnYkLSN4w
k//E2zf/wbb0SV/HLHFvh6a9ENVRD1/rHK0EijlBzikb+1rgDQihJETcgBLsMoZV
QqGj8fDUUuxnVHsuGav/bf41PA7E/58HXKPrB2C0cON41f7K3o9TStKpVJOSXBrR
WURmNQ64qnSSryn1nCxMzXpaw7VUo409ohybbvN6ngxVy4QR2NCC7Fr0QVdtapxD
7zdlwx6lEwGemuqs/oG5oDtrRuRgeOHmRps2R6gG5oc+JqVMrVRv6F9h4ja3UgxC
DBQjOVT1BFPWmMHnHCsVYLqbbXkZUfvP2sO1dJiYd/zrQhi+FtNth9qrLLv3gkgt
wQIDAQAB
-----END PUBLIC KEY-----
'''
ORCID_JWT_AUDIENCE = 'APP-GXFVWWJT8H0F50WD'


def decode_jwt(token: str) -> dict:
    try:
        decoded_token = jwt.decode(
            token,
            ORCID_JWT_SIGNING_PUBLIC_KEY,
            algorithms=['RS256'],
            audience=ORCID_JWT_AUDIENCE,
            # ORCID sends an at_hash when using the OpenID Connect implicit flow, even though there is no auth_token.
            options={'verify_at_hash': False}
        )
        print(decoded_token)
        # , options=None, audience=None, issuer=None, subject=None, access_token=None)
        return decoded_token
    except Exception as ex:
        print(ex)
        return {}


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials
        try:
            credentials = await super(JWTBearer, self).__call__(request)
        except HTTPException:
            credentials = None
        if credentials:
            if not credentials.scheme == 'Bearer':
                raise HTTPException(status_code=403, detail='Invalid authentication scheme.')
            token_payload = self.verify_jwt(credentials.credentials)
            if not token_payload:
                raise HTTPException(status_code=403, detail='Invalid token or expired token.')
            return token_payload  # credentials.credentials  # Return the JWT
        else:
            # raise HTTPException(status_code=403, detail='Invalid authorization credentials.')
            return None

    def verify_jwt(self, token: str) -> bool:
        is_token_valid: bool = False
        try:
            payload = decode_jwt(token)
        except:
            payload = None
        #if payload:
        #    is_token_valid = True
        return payload


async def get_current_user(token_payload: dict = Depends(JWTBearer()), db: Session = Depends(deps.get_db))\
        -> Optional[User]:
    user = None
    if token_payload is not None:
        username: str = token_payload['sub']
        if username is not None:
            print(username)
            user = db.query(User).filter(User.username == username).one_or_none()
            if user is None:
                # A new user has just connected an ORCID iD. Create the user account.
                user = User(
                    username=username,
                    first_name=token_payload['given_name'],
                    last_name=token_payload['family_name'],
                    is_active=True,
                    date_joined=datetime.now()
                )
                db.add(user)
                db.commit()
                db.refresh(user)
            elif not user.is_active:
                user = None
    return user


async def require_current_user(user: Optional[User] = Depends(get_current_user)) -> User:
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )
    user_value: User = user
    return user_value
