from typing import Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session
from sqlalchemy.exc import MultipleResultsFound

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.identifiers import find_generic_article
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.models.user import User
from mavedb.view_models import publication_identifier
from mavedb.view_models import author
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/publication-identifiers",
    tags=["publication identifiers"],
    responses={404: {"description": "Not found"}},
)


@router.get("/", status_code=200, response_model=list[publication_identifier.PublicationIdentifier])
def list_publications(*, db: Session = Depends(deps.get_db)) -> Any:
    """
    List stored publications
    """
    items = db.query(PublicationIdentifier).all()
    return items


@router.get(
    "/{db_name}/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}},
)
def fetch_experiment(*, db_name: str, identifier: str, db: Session = Depends(deps.get_db)) -> PublicationIdentifier:
    """
    Fetch a single publication by db name and identifier.
    """
    item = (
        db.query(PublicationIdentifier)
        .filter(PublicationIdentifier.identifier == identifier and PublicationIdentifier.db_name == db_name)
        .first()
    )
    if not item:
        raise HTTPException(
            status_code=404, detail=f"Publication with identifier {identifier} in database {db_name} not found"
        )
    return item


@router.get("/journals", status_code=200, response_model=list[str], responses={404: {}})
def list_publication_journal_names(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct journal names, in alphabetical order.
    """

    items = db.query(PublicationIdentifier).all()
    journals = map(lambda item: item.publication_journal, items)
    return sorted(list(set(journals)))


@router.get("/databases", status_code=200, response_model=list[str], responses={404: {}})
def list_publication_database_names(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct database names, in alphabetical order.
    """

    items = db.query(PublicationIdentifier).all()
    databases = map(lambda item: item.db_name, items)
    return sorted(list(set(databases)))


@router.get(
    "/publications/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}, 500: {}},
)
async def search_publications_by_identifier(*, identifier: str, db: Session = Depends(deps.get_db)) -> Any:
    """
    Fetch a single scoreset by URN.
    """
    try:
        item = db.query(PublicationIdentifier).filter(PublicationIdentifier.identifier == identifier).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple publications with identifier {identifier} were found.")

    if not item:
        raise HTTPException(status_code=404, detail=f"Publication with identifier {identifier} not found")
    return item


@router.get(
    "/publications/{db_name}/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}, 500: {}},
)
async def search_publications_by_identifier_and_db(
    *, identifier: str, db_name: str, db: Session = Depends(deps.get_db)
) -> Any:
    """
    Fetch a single scoreset by URN.
    """
    try:
        item = (
            db.query(PublicationIdentifier)
            .filter(PublicationIdentifier.identifier == identifier and PublicationIdentifier.db_name == db_name)
            .one_or_none()
        )
    except MultipleResultsFound:
        raise HTTPException(
            status_code=500,
            detail=f"Multiple publications with identifier {identifier} and db_name {db_name} were found.",
        )

    if not item:
        raise HTTPException(
            status_code=404, detail=f"Publication with identifier {identifier} and db_name {db_name} not found"
        )
    return item


@router.post("/search", status_code=200, response_model=list[publication_identifier.PublicationIdentifier])
def search_publication_identifiers(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    List stored all stored publications.
    """
    items = db.query(PublicationIdentifier).all()
    return items


@router.get(
    "/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}},
)
def fetch_publication_by_identifier(
    *,
    identifier: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> PublicationIdentifier:
    """
    Fetch a single publication by identifier.
    """
    try:
        item = db.query(PublicationIdentifier).filter(PublicationIdentifier.identifier == identifier).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(
            status_code=500,
            detail=f"Multiple publications with identifier {identifier} were found.",
        )

    if not item:
        raise HTTPException(status_code=404, detail=f"No publication with identifier {identifier} were found.")
    return item


@router.get(
    "/{db_name}/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}},
)
def fetch_publication_by_dbname_and_identifier(
    *,
    db_name: str,
    identifier: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> PublicationIdentifier:
    """
    Fetch a single publication by db name and identifier.
    """
    try:
        item = (
            db.query(PublicationIdentifier)
            .filter(PublicationIdentifier.identifier == identifier and PublicationIdentifier.db_name == db_name)
            .one_or_none()
        )
    except MultipleResultsFound:
        raise HTTPException(
            status_code=500,
            detail=f"Multiple publications with identifier {identifier} and database name {db_name} were found.",
        )
    if not item:
        raise HTTPException(
            status_code=404,
            detail=f"No publication with identifier {identifier} and database name {db_name} were found.",
        )
    return item


@router.get("/journals", status_code=200, response_model=list[str], responses={404: {}})
def list_publication_journal_names(
    *,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    List distinct journal names, in alphabetical order.
    """

    items = db.query(PublicationIdentifier).all()
    journals = map(lambda item: item.publication_journal, items)
    return sorted(list(set(journals)))


@router.get("/databases", status_code=200, response_model=list[str], responses={404: {}})
def list_publication_database_names(
    *,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    List distinct database names, in alphabetical order.
    """

    items = db.query(PublicationIdentifier).all()
    databases = map(lambda item: item.db_name, items)
    return sorted(list(set(databases)))


@router.post("/search", status_code=200, response_model=list[publication_identifier.PublicationIdentifier])
def search_publication_identifiers(
    search: TextSearch,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Search publication identifiers via a TextSearch query.
    """

    query = db.query(PublicationIdentifier)

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        query = query.filter(func.lower(PublicationIdentifier.identifier).contains(lower_search_text))
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    items = query.order_by(PublicationIdentifier.identifier).limit(50).all()
    if not items:
        items = []
    return items


@router.get(
    "/search/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}, 500: {}},
)
async def search_publications_by_identifier(
    *, identifier: str, db: Session = Depends(deps.get_db), user: User = Depends(get_current_user)
) -> Any:
    """
    Search publication identifiers via their identifier.
    """
    query = db.query(PublicationIdentifier).filter(PublicationIdentifier.identifier == identifier).all()

    if not query:
        raise HTTPException(status_code=404, detail=f"No publications with identifier {identifier} were found.")
    return query


@router.get(
    "/search/{db_name}/{identifier}",
    status_code=200,
    response_model=publication_identifier.PublicationIdentifier,
    responses={404: {}, 500: {}},
)
async def search_publications_by_identifier_and_db(
    *,
    identifier: str,
    db_name: str,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Search publication identifiers via their identifier and database.
    """
    query = (
        db.query(PublicationIdentifier)
        .filter(PublicationIdentifier.identifier == identifier and PublicationIdentifier.db_name == db_name)
        .all()
    )

    if not query:
        raise HTTPException(
            status_code=404,
            detail=f"No publications with identifier {identifier} and database name {db_name} were found.",
        )
    return query


@router.post(
    "/search-external", status_code=200, response_model=List[publication_identifier.ExternalPublicationIdentifier]
)
async def search_external_publication_identifiers(
    search: TextSearch,
    db: Session = Depends(deps.get_db),
    user: User = Depends(get_current_user),
) -> Any:
    """
    Search external publication identifiers via a TextSearch query.
    Technically, this should be some sort of accepted publication identifier.
    """

    if search.text and len(search.text.strip()) > 0:
        lower_search_text = search.text.strip().lower()
        items = await find_generic_article(db, lower_search_text)
    else:
        raise HTTPException(status_code=500, detail="Search text is required")

    if not any(items.values()):
        return []
    return list(items.values())
