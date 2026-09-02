import logging
from typing import Annotated
from urllib.parse import quote

from fastapi import APIRouter, Path, Request
from fastapi.responses import RedirectResponse
from ga4gh.core.identifiers import GA4GH_IR_REGEXP

from mavedb.lib.deprecation import MAPPED_VARIANT_SUNSET, deprecation_headers, record_deprecated_usage
from mavedb.lib.logging import LoggedRoute
from mavedb.routers.shared import ROUTER_BASE_PREFIX

TAG_NAME = "Variants"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/mapped-variants",
    tags=[TAG_NAME],
    route_class=LoggedRoute,
)


def _redirect(request: Request, target: str) -> RedirectResponse:
    """Redirect to the successor, tagging the response with RFC 8594 deprecation headers.

    The redirect status already moves a well-behaved client, but the headers announce the deprecation to
    tooling that follows the 301 silently: ``Deprecation``, a ``Link`` naming this exact successor, and a
    ``Warning`` a human sees in logs. The successor here is a genuine 1:1 replacement, so it is safe to name.
    """
    url = f"{target}?{request.url.query}" if request.url.query else target
    record_deprecated_usage(request.url.path, successor=target)
    headers = deprecation_headers(
        successor=target,
        warning=f"This resource has moved permanently to {target}; update your integration.",
        sunset=MAPPED_VARIANT_SUNSET,
    )
    return RedirectResponse(url=url, status_code=301, headers=headers)


@router.get(
    "/{urn}",
    status_code=301,
    deprecated=True,
    summary="Moved to GET /variants/{urn}",
)
def redirect_mapped_variant(*, urn: str, request: Request) -> RedirectResponse:
    """This resource has moved. Use ``GET /variants/{urn}`` instead."""
    return _redirect(request, f"{ROUTER_BASE_PREFIX}/variants/{quote(urn, safe='')}")


@router.get(
    "/{urn}/va/study-result",
    status_code=301,
    deprecated=True,
    summary="Moved to GET /variants/{urn}/va/study-result",
)
def redirect_mapped_variant_study_result(*, urn: str, request: Request) -> RedirectResponse:
    """This resource has moved. Use ``GET /variants/{urn}/va/study-result`` instead."""
    return _redirect(request, f"{ROUTER_BASE_PREFIX}/variants/{quote(urn, safe='')}/va/study-result")


@router.get(
    "/{urn}/va/functional-statement",
    status_code=301,
    deprecated=True,
    summary="Moved to GET /variants/{urn}/va/functional-statement",
)
def redirect_mapped_variant_functional_impact_statement(*, urn: str, request: Request) -> RedirectResponse:
    """This resource has moved. Use ``GET /variants/{urn}/va/functional-statement`` instead."""
    return _redirect(request, f"{ROUTER_BASE_PREFIX}/variants/{quote(urn, safe='')}/va/functional-statement")


@router.get(
    "/{urn}/va/pathogenicity-statement",
    status_code=301,
    deprecated=True,
    summary="Moved to GET /variants/{urn}/va/pathogenicity-statement",
)
def redirect_mapped_variant_acmg_evidence_line(*, urn: str, request: Request) -> RedirectResponse:
    """This resource has moved. Use ``GET /variants/{urn}/va/pathogenicity-statement`` instead."""
    return _redirect(request, f"{ROUTER_BASE_PREFIX}/variants/{quote(urn, safe='')}/va/pathogenicity-statement")


@router.get(
    "/vrs/{identifier}",
    status_code=301,
    deprecated=True,
    summary="Moved to GET /variants/vrs/{identifier}",
)
def redirect_mapped_variants_by_identifier(
    *,
    identifier: Annotated[
        str,
        Path(
            description="String, a valid GA4GH digest based identifier.",
            json_schema_extra={"example": "ga4gh:SQ.0123abcd"},
            regex=GA4GH_IR_REGEXP,
        ),
    ],
    request: Request,
) -> RedirectResponse:
    """This resource has moved. Use ``GET /variants/vrs/{identifier}`` instead.

    Note that the replacement's ``only_current`` boolean query parameter has been superseded by
    ``as_of``; a caller relying on ``only_current=false`` should switch to passing an explicit
    ``as_of`` timestamp rather than expecting it to carry over through this redirect.
    """
    return _redirect(request, f"{ROUTER_BASE_PREFIX}/variants/vrs/{quote(identifier, safe='')}")
