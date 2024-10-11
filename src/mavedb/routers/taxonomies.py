from typing import Any, List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.taxonomies import search_NCBI_taxonomy
from mavedb.models.taxonomy import Taxonomy
from mavedb.view_models import taxonomy
from mavedb.view_models.search import TextSearch

router = APIRouter(prefix="/api/v1/taxonomies", tags=["taxonomies"], responses={404: {"description": "Not found"}})


@router.get("/", status_code=200, response_model=List[taxonomy.Taxonomy], responses={404: {}})
def list_taxonomies(
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    List taxonomies.
    """
    items = db.query(Taxonomy).order_by(Taxonomy.organism_name).all()
    return items


@router.get("/speciesNames", status_code=200, response_model=List[str], responses={404: {}})
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


@router.get("/commonNames", status_code=200, response_model=List[str], responses={404: {}})
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


@router.get("/{item_id}", status_code=200, response_model=taxonomy.Taxonomy, responses={404: {}})
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


@router.get("/tax-id/{item_id}", status_code=200, response_model=taxonomy.Taxonomy, responses={404: {}})
def fetch_taxonomy_by_tax_id(
    *,
    item_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single taxonomy by tax_id.
    """
    item = db.query(Taxonomy).filter(Taxonomy.tax_id == item_id).first()
    if not item:
        raise HTTPException(status_code=404, detail=f"Taxonomy with tax_ID {item_id} not found")
    return item


@router.post("/search", status_code=200, response_model=List[taxonomy.Taxonomy])
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
            query = query.filter(Taxonomy.tax_id == int(search.text))
    items = query.order_by(Taxonomy.organism_name).all()

    if not items and search.text:
        search_taxonomy = await search_NCBI_taxonomy(db, search.text)
        if search_taxonomy:
            items = [search_taxonomy]
        else:
            items = []

    return items
