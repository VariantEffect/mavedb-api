from typing import Any, List, Optional

from fastapi import APIRouter, Depends
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import MultipleResultsFound

from mavedb import deps

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.view_models import mapped_variant

async def fetch_mapped_variant_by_variant_urn(db, urn: str) -> Optional[MappedVariant]:
    """
    We may combine this function back to show_mapped_variant if none of any new function call it.
    Fetch one mapped variant by variant URN.

    :param db: An active database session.
    :param urn: The variant URN.
    :return: The mapped variant, or None if the URL was not found or refers to a private score set not owned by the specified
        user.
    """
    try:
        item = db.query(MappedVariant).filter(Variant.urn == urn).filter(MappedVariant.variant_id == Variant.id).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple variants with URN {urn} were found.")
    if not item:
        raise HTTPException(status_code=404, detail=f"Mapped variant with URN {urn} not found")
    return item

router = APIRouter(prefix="/api/v1/mapped-variants", tags=["mapped variants"], responses={404: {"description": "Not found"}})

@router.get("/{urn}", status_code=200, response_model=mapped_variant.MappedVariant, responses={404: {}, 500: {}})
async def show_mapped_variant(
    *, urn: str, db: Session = Depends(deps.get_db)
) -> Any:
    """
    Fetch a mapped variant by URN.
    """

    return await fetch_mapped_variant_by_variant_urn(db, urn)