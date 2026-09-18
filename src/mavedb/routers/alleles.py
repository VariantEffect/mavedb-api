import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.exceptions import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from starlette.convertors import Convertor, register_url_convertor

from mavedb import deps
from mavedb.lib.allele_detail import get_allele_detail
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.models.allele import Allele
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models.allele_detail import AlleleDetail

TAG_NAME = "Alleles"

logger = logging.getLogger(__name__)


# One resource, three identifier forms — a GA4GH VRS digest (one allele), a ClinGen nucleotide id (CAID,
# the nt-canonical change) or protein id (PAID). Rather than split the path or add a query param, overload
# a single `/alleles/{identifier}` with a custom Starlette convertor (the same pattern the publication
# router uses for DOI/PubMed/…). The convertor matches any of the three forms; the handler dispatches by
# form. In the OpenAPI spec this is still just a string path parameter.
class AlleleIdentifierConverter(Convertor):
    # A GA4GH IR (ga4gh:<type>.<32 base64url>) OR a ClinGen allele id (CA…/PA… + digits).
    # NOTE: Multivariant CAIDs (CA…/PA… + digits + `,` + CA…/PA… + digits) are intentionally excluded.
    regex = r"(?:ga4gh:[^.]+\.[0-9A-Za-z_\-]{32})|(?:[CP]A[0-9]+)"

    def convert(self, value: str) -> str:
        return value

    def to_string(self, value: str) -> str:
        return str(value)


register_url_convertor("allele_identifier", AlleleIdentifierConverter())

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Retrieve deduplicated alleles — identity, cross-layer equivalence class, and "
    "external annotations — by VRS digest or ClinGen allele id (CAID / PAID).",
}


def _representative(alleles: list[Allele]) -> Allele:
    """The allele whose identity fills the flat anchor fields for a ClinGen-id fetch. A CAID names one nt
    change in up to two frames (genomic + coding) — prefer the coding frame, then genomic; a PAID matches
    the single protein allele, which falls through to ``alleles[0]``."""
    by_level = {a.level: a for a in alleles}
    return by_level.get(SequenceLevel.cdna.value) or by_level.get(SequenceLevel.genomic.value) or alleles[0]


@router.get(
    "/alleles/{identifier:allele_identifier}",
    status_code=200,
    response_model=AlleleDetail,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Fetch allele detail by VRS digest, CAID, or PAID",
)
def get_allele(
    *,
    response: Response,
    identifier: str = Path(
        description="A GA4GH VRS digest (one allele), or a ClinGen allele id: a nucleotide CAID "
        "(the nt-canonical change, its genomic + coding frames) or a protein PAID.",
        json_schema_extra={"example": "ga4gh:VA.0123abcd"},
    ),
    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Reconstruct the molecular layer (equivalence-class membership + VEP/gnomAD/ClinVar "
            "annotations) as it stood at this instant. ISO 8601, ideally timezone-aware. The focus "
            "allele's own identity is content-addressed and immutable, so it is unaffected. Defaults to "
            "current."
        ),
    ),
    db: Session = Depends(deps.get_db),
):
    """Fetch the detail envelope for a deduplicated allele, by any of its identifiers.

    The allele-grain counterpart of ``GET /variants/{urn}``. Flat anchor identity (digest, level, HGVS,
    ClinGen id, spec-pure VRS) plus the cross-layer equivalence class (each member labelled relative to
    the focus) and a digest-keyed annotation map. The ``identifier`` may be:

    - a **VRS digest** (``ga4gh:VA.…``) — focuses that one allele.
    - a **CAID** (``CA…``) — the nt-canonical change; The coding frame is the preferential focus,
      falling back to the genomic frame if no coding frame exists.
    - a **PAID** (``PA…``) — the protein change; the protein allele is focused and its nucleotide
      equivalents surface as reverse-translation candidates.

    This is a **public molecular resource**. It carries no score-set-level information. No scores,
    classifications, measurements, or version standing. Only the allele's own identity, its cross-layer
    equivalence class, and public reference annotations (VEP / gnomAD / ClinVar).
    """
    save_to_logging_context({"requested_resource": identifier, "as_of": as_of})
    response.headers["X-As-Of"] = as_of.isoformat() if as_of is not None else "current"

    if identifier.startswith("ga4gh:"):
        allele = db.scalar(select(Allele).where(Allele.vrs_digest == identifier))
        if allele is None:
            raise HTTPException(status_code=404, detail=f"allele with VRS digest '{identifier}' not found")
        return get_allele_detail(db, allele, focus_digests={identifier}, as_of=as_of)

    # A ClinGen allele id (CAID or PAID): resolve to its allele(s) — the nt-canonical change's genomic +
    # coding frames for a CAID, or the single protein allele for a PAID — and focus all of them.
    matches = list(db.scalars(select(Allele).where(Allele.clingen_allele_id == identifier)).all())
    if not matches:
        raise HTTPException(status_code=404, detail=f"no allele with ClinGen allele id '{identifier}' found")

    focus_digests = {a.vrs_digest for a in matches if a.vrs_digest is not None}
    return get_allele_detail(db, _representative(matches), focus_digests=focus_digests, as_of=as_of)
