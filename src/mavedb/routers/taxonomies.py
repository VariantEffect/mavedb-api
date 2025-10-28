from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.taxonomies import search_NCBI_taxonomy
from mavedb.models.taxonomy import Taxonomy
from mavedb.view_models import taxonomy
from mavedb.view_models.search import TextSearch

router = APIRouter(
    prefix="/api/v1/taxonomies",
    tags=["Taxonomies"],
    responses={
        404: {"description": "Not found"},
        500: {"description": "Internal server error"},
    },
)


@router.get("/", status_code=200, response_model=List[taxonomy.Taxonomy], summary="List taxonomies")
def list_taxonomies(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List taxonomies.
    """
    items = db.query(Taxonomy).order_by(Taxonomy.organism_name).all()
    return items


@router.get("/speciesNames", status_code=200, response_model=List[str], summary="List species names")
def list_taxonomy_organism_names(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct species names, in alphabetical order.
    """

    items = db.query(Taxonomy).all()
    organism_names = map(lambda item: item.organism_name, items)
    return sorted(list(set(organism_names)))


@router.get("/commonNames", status_code=200, response_model=List[str], summary="List common names")
def list_taxonomy_common_names(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List distinct common names, in alphabetical order.
    """

    items = db.query(Taxonomy).all()
    common_names = map(lambda item: item.common_name, items)
    return sorted(list(set(common_names)))


@router.get("/{item_id}", status_code=200, response_model=taxonomy.Taxonomy, summary="Fetch taxonomy by ID")
def fetch_taxonomy(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single taxonomy by ID.
    """
    item = db.query(Taxonomy).filter(Taxonomy.id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Taxonomy with ID {item_id} not found")
    return item


@router.get("/code/{item_id}", status_code=200, response_model=taxonomy.Taxonomy, summary="Fetch taxonomy by code")
def fetch_taxonomy_by_code(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single taxonomy by code.
    """
    item = db.query(Taxonomy).filter(Taxonomy.code == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Taxonomy with code {item_id} not found")
    return item


@router.post("/search", status_code=200, response_model=List[taxonomy.Taxonomy], summary="Search taxonomies")
async def search_taxonomies(search: TextSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search Taxonomy.
    If no search text, return the whole taxonomy list so that front end Taxonomy component can get data to show in dropdown button.
    """
    query = db.query(Taxonomy)

    if search.text and len(search.text.strip()) > 0:
        if search.text.isnumeric() is False:
            lower_search_text = search.text.strip().lower()
            query = query.filter(
                or_(
                    func.lower(Taxonomy.organism_name).contains(lower_search_text),
                    func.lower(Taxonomy.common_name).contains(lower_search_text),
                )
            )
        else:
            query = query.filter(Taxonomy.code == int(search.text))
    items = query.order_by(Taxonomy.organism_name).all()

    if not items and search.text:
        search_taxonomy = await search_NCBI_taxonomy(db, search.text)
        if search_taxonomy:
            items = [search_taxonomy]
        else:
            items = []

    return items
