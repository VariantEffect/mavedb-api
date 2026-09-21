"""MONDO disease-term search and validation against the EBI Ontology Lookup Service (OLS4).

Split out from :mod:`mavedb.lib.mondo`: this module resolves and validates a submitted MONDO code
against the live OLS4 service, which needs :mod:`mavedb.lib.logging.context` for structured request
logging — and that, in turn, needs ``starlette``, an optional "server" dependency (see
`#459 <https://github.com/VariantEffect/mavedb-api/issues/459>`_ for making logging context available to
core dependencies directly). Keeping the OLS calls here, separate from :mod:`mavedb.lib.mondo`'s
constants and serialization, lets callers that only need the static vocabulary stay importable without
the server extras.
"""

import logging
from typing import Any, Optional

import httpx
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import MondoServiceError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.lib.mondo import MONDO_GENERIC_CODE, MONDO_SYSTEM, MondoSuggestion, get_generic_disease_term, mondo_iri
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.mondo_term import MondoTerm

logger = logging.getLogger(__name__)

# EBI Ontology Lookup Service (OLS4) — the source for MONDO term search and validation.
OLS_BASE_URL = "https://www.ebi.ac.uk/ols4/api"
MONDO_ONTOLOGY = "mondo"
# OLS can be slow; the default httpx 5s timeout is too tight for its autocomplete on short queries.
OLS_TIMEOUT = 10.0

DEFAULT_SEARCH_LIMIT = 20


async def search_mondo(query: str, limit: int = DEFAULT_SEARCH_LIMIT) -> list[MondoSuggestion]:
    """Search MONDO disease classes by free text via OLS, for the disease typeahead."""
    if not query or not query.strip():
        return []

    # OLS4's /select endpoint is the autocomplete-optimized path; /search times out on broad prefixes
    # (e.g. a two-letter query), which is often what a typeahead sends.
    params: dict[str, Any] = {
        "q": query.strip(),
        "ontology": MONDO_ONTOLOGY,
        "rows": limit,
        "fieldList": "iri,label,obo_id",
    }
    save_to_logging_context({"mondo_search_query": query, "mondo_search_limit": limit})
    try:
        async with httpx.AsyncClient(timeout=OLS_TIMEOUT) as client:
            response = await client.get(f"{OLS_BASE_URL}/select", params=params)
            response.raise_for_status()
            docs = response.json().get("response", {}).get("docs", [])
    except httpx.HTTPError as exc:
        save_to_logging_context(format_raised_exception_info_as_dict(exc))
        logger.error(msg="MONDO/OLS search request failed.", exc_info=exc, extra=logging_context())
        raise MondoServiceError("Disease ontology service temporarily unavailable") from exc

    return [
        MondoSuggestion(code=doc["obo_id"], label=doc["label"], iri=doc.get("iri", mondo_iri(doc["obo_id"])))
        for doc in docs
        if doc.get("obo_id") and doc.get("label")
    ]


async def fetch_mondo_term(code: str) -> Optional[MondoSuggestion]:
    """Resolve one MONDO CURIE to its canonical label + IRI via OLS, or None if it does not exist."""
    params: dict[str, Any] = {
        "q": code,
        "ontology": MONDO_ONTOLOGY,
        "queryFields": "obo_id",
        "exact": "true",
        "rows": 1,
        "fieldList": "iri,label,obo_id",
    }
    try:
        async with httpx.AsyncClient(timeout=OLS_TIMEOUT) as client:
            response = await client.get(f"{OLS_BASE_URL}/search", params=params)
            response.raise_for_status()
            docs = response.json().get("response", {}).get("docs", [])
    except httpx.HTTPError as exc:
        save_to_logging_context(format_raised_exception_info_as_dict(exc))
        logger.error(msg="MONDO/OLS term lookup failed.", exc_info=exc, extra=logging_context())
        raise MondoServiceError("Disease ontology service temporarily unavailable") from exc

    if not docs:
        return None

    doc = docs[0]
    return MondoSuggestion(code=doc["obo_id"], label=doc["label"], iri=doc.get("iri", mondo_iri(code)))


async def find_or_create_mondo_term(db: Session, code: str) -> MondoTerm:
    """Find an existing MONDO term by code, or create one after validating it against OLS.

    A new code is validated against OLS and its canonical label stored. An unknown code raises
    ``ValidationError``; an OLS outage raises ``MondoServiceError`` (via :func:`fetch_mondo_term`) so a
    term is never persisted without a validated label — a controlled vocabulary must stay validated.
    """
    # The generic term is canonical and seeded — resolve it directly, never via OLS.
    if code == MONDO_GENERIC_CODE:
        return get_generic_disease_term(db)

    term = db.query(MondoTerm).filter(MondoTerm.system == MONDO_SYSTEM, MondoTerm.code == code).one_or_none()
    if term is not None:
        return term

    resolved = await fetch_mondo_term(code)
    if resolved is None:
        raise ValidationError(f"'{code}' is not a valid MONDO disease term.", custom_loc=["body", "disease"])

    # OLS may normalize the submitted code (case, or an alias/obsolete id) to a different canonical code,
    # so re-check existence by the resolved code before inserting: keying the lookup on the same value we
    # insert under keeps this idempotent and avoids a UNIQUE(system, code) violation when the canonical
    # row already exists under a different submitted spelling.
    canonical_code = resolved["code"]
    term = db.query(MondoTerm).filter(MondoTerm.system == MONDO_SYSTEM, MondoTerm.code == canonical_code).one_or_none()
    if term is not None:
        return term

    term = MondoTerm(code=canonical_code, system=MONDO_SYSTEM, label=resolved["label"])  # type: ignore[call-arg]
    db.add(term)
    db.flush()

    return term


async def resolve_disease_term(db: Session, code: Optional[str]) -> MondoTerm:
    """Resolve a calibration's submitted disease code to a stored MONDO term.

    ``None`` (no disease specified) resolves to the generic "disease or disorder" term; any other code
    is validated/created via :func:`find_or_create_mondo_term`. Shared by the score calibration create
    and update paths so the write-time default lives in one place.
    """
    if code is None:
        return get_generic_disease_term(db)

    return await find_or_create_mondo_term(db, code)
