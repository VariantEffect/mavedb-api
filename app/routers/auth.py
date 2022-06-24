from fastapi import APIRouter
from sqlalchemy.orm import Session
from typing import Any, List, Optional
from app import deps
from app.models.user import User
from app.view_models import user
from sqlalchemy.orm.exc import MultipleResultsFound
from fastapi.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse

from fastapi import Depends
#from fastapi_oidc import IDToken

#from app.lib.auth import oauth

####################################################################################################
# Obsolete file, to be removed
####################################################################################################
router = APIRouter(
    prefix='/api/v1',
    tags=['authentication'],
    responses={404: {'description': 'Not found'}}
)

@router.get('/user/{username}', status_code=200, response_model=user.User, responses={404: {}, 500: {}})
def fetch_user(
    *,
    username: str,
    db: Session = Depends(deps.get_db)
) -> Any:
    """
    Fetch a single user by username.
    """
    try:
        #item = db.query(Scoreset).filter(Scoreset.urn == urn).filter(Scoreset.private.is_(False)).one_or_none()
        item = db.query(User).filter(User.username == username).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(
            status_code=500, detail=f'Multiple users with username {username} were found.'
        )
    if not item:
        raise HTTPException(
            status_code=404, detail=f'User with username {username} not found'
        )
    return item



#@router.get('/login')
#async def login(request: Request):
#    redirect_uri = request.url_for('auth')
#    return await oauth.orcid.authorize_redirect(request, redirect_uri)


#@router.get('/logout')
#async def logout(request: Request):
#    request.session.pop('user', None)
#    return RedirectResponse(url='/')


#@router.get("/protected")
#def protected(id_token: IDToken = Depends(authenticate_user)):
#    return {"Hello": "World", "user_email": id_token.email}
