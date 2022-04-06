from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import RedirectResponse

from fastapi import Depends
from fastapi_oidc import IDToken

from app.lib.auth import oauth

####################################################################################################
# Obsolete file, to be removed
####################################################################################################

#router = APIRouter(
#    prefix='/',
#    tags=['authentication'],
#    responses={404: {'description': 'Not found'}}
#)


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
