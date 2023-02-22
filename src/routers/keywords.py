from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func
from sqlalchemy.orm import Session

import src.view_models.doi_identifier
from src import deps
from src.lib.authorization import RoleRequirer
from src.models.controlled_keyword import ControlledKeyword
from src.models.user import User
from src.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/keywords",
    tags=["Keywords"],
    responses={404: {"description": "Not found"}}
)


@router.get('/', status_code=200, response_model=list[src.view_models.keyword.AdminKeyword], responses={404: {}})
async def list_keywords(
        *,
        db: Session = Depends(deps.get_db),
        user: User = Depends(RoleRequirer(['admin']))
) -> Any:
    """
    List all controlled vocabulary items.
    """
    items = db.query(ControlledKeyword).order_by(ControlledKeyword.key, ControlledKeyword.value).all()
    return items


@router.post("/", response_model=src.view_models.keyword.AdminKeyword, responses={422: {}})
async def create_keyword(
        *,
        item_create: src.view_models.keyword.KeywordCreate,
        db: Session = Depends(deps.get_db),
        user: User = Depends(RoleRequirer(['admin']))
) -> Any:
    """
    Create a controlled vocabulary item.
    """
    if item_create is None:
        return None
    item = ControlledKeyword(
        **jsonable_encoder(item_create, by_alias=False)
        #created_by=user,
        #modified_by=user
    )
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.put("/{id}", response_model=src.view_models.keyword.AdminKeyword, responses={422: {}})
async def update_keyword(
        *,
        item_update: src.view_models.keyword.KeywordUpdate,
        id: int,
        db: Session = Depends(deps.get_db),
        user: User = Depends(RoleRequirer(['admin']))
) -> Any:
    """
    Update a controlled vocabulary item.
    """
    if item_update is None:
        return None
    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    item = db.query(ControlledKeyword).filter(ControlledKeyword.id == id).one_or_none()
    if item is None:
        return None

    for var, value in vars(item_update).items():  # vars(item_update).items():
        setattr(item, var, value) if value else None
    # item.modified_by = user

    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@router.delete("/{id}", responses={422: {}})
async def delete_keyword(
        *,
        id: int,
        db: Session = Depends(deps.get_db),
        user: User = Depends(RoleRequirer(['admin']))
) -> Any:
    """
    Delete a controlled vocabulary item.
    """
    item = db.query(ControlledKeyword).filter(ControlledKeyword.id == id).one_or_none()
    if item is None:
        return None

    db.delete(item)
    db.commit()


@router.get(
    '/keys',
    status_code=200,
    response_model=list[str]
)
def get_keys(
        db: Session = Depends(deps.get_db)
) -> Any:
    """
    Get a list of controlled vocabulary keys.
    """

    query = db.query(ControlledKeyword.key).distinct()
    items = query.order_by(ControlledKeyword.key).all()
    if not items:
        items = []
    return items


@router.get(
    '/keys/{key}/values',
    status_code=200,
    response_model=list[src.view_models.keyword.Keyword]
)
def get_key_values(
        key: str,
        db: Session = Depends(deps.get_db)
) -> Any:
    """
    Get a list of controlled vocabulary items available for one key.
    """

    query = db.query(ControlledKeyword) \
        .filter(ControlledKeyword.key == key)

    items = query.order_by(ControlledKeyword.value).all()
    if not items:
        items = []
    return items


@router.post(
    '/search',
    status_code=200,
    response_model=List[src.view_models.keyword.Keyword]
)
def search_keywords(
        search: TextSearch,
        db: Session = Depends(deps.get_db)
) -> Any:
    """
    Search controlled vocabulary items.
    """

    query = db.query(ControlledKeyword)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(ControlledKeyword.value).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail='Search text is required')

    items = query.order_by(ControlledKeyword.key, ControlledKeyword.value) \
        .limit(50) \
        .all()
    if not items:
        items = []
    return items


@router.post(
    '/keys/{key}/search',
    status_code=200,
    response_model=list[src.view_models.keyword.Keyword]
)
def search_keywords(
        key: str,
        search: TextSearch,
        db: Session = Depends(deps.get_db)
) -> Any:
    """
    Search controlled vocabulary items for one key.
    """

    query = db.query(ControlledKeyword)

    if key is not None:
        query = query.filter(ControlledKeyword.key == key)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(ControlledKeyword.value).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail='Search text is required')

    items = query.order_by(ControlledKeyword.key, ControlledKeyword.value) \
        .limit(50) \
        .all()
    if not items:
        items = []
    return items
