from typing import Any

from fastapi import APIRouter, Query
from ga4gh.core.models import MappableConcept

from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.logging.logged_route import LoggedRoute
from mavedb.lib.mondo import mondo_suggestion_to_mappable_concept
from mavedb.lib.mondo_ols import DEFAULT_SEARCH_LIMIT, search_mondo
from mavedb.routers.shared import GATEWAY_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX

TAG_NAME = "Diseases"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/diseases",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Search disease terms from the Monarch Disease Ontology (MONDO).",
}


@router.get(
    "/search",
    status_code=200,
    response_model=list[MappableConcept],
    summary="Search MONDO disease terms",
    responses={**GATEWAY_ERROR_RESPONSES},
)
async def search_diseases(
    *,
    q: str = Query(..., description="Free-text query for a MONDO disease term."),
    limit: int = Query(DEFAULT_SEARCH_LIMIT, ge=1, le=100, description="Maximum number of results."),
) -> Any:
    """Typeahead search for MONDO disease terms, returned as GA4GH ``MappableConcept`` suggestions."""
    save_to_logging_context({"query": q, "limit": limit})

    suggestions = await search_mondo(q, limit)
    return [mondo_suggestion_to_mappable_concept(suggestion) for suggestion in suggestions]
