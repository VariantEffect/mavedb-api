"""MONDO disease-term resolution and search.

Calibrations carry a disease/disorder context drawn from the Monarch Disease Ontology (MONDO). This
module owns the MONDO vocabulary integration: get-or-create of :class:`MondoTerm` rows (mirroring
:func:`mavedb.lib.taxonomies.find_or_create_taxonomy`), a typeahead search + validation against the
EBI OLS4 service, and conversion of a stored term to the GA4GH ``MappableConcept`` served on the wire.

The generic term (``MONDO:0000001``, "disease or disorder") is the write-time default for a calibration
with no specific disease, so ``disease_term_id`` is never null. The annotation layer serializes whatever
term a calibration carries — generic or specific — as its VA-Spec disease condition (see
:func:`mavedb.lib.annotation.condition.calibration_disease_condition`).
"""

import logging
from typing import Any, Optional, TypedDict

import httpx
from ga4gh.core.models import Coding, MappableConcept, iriReference
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import MondoServiceError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.mondo_term import MondoTerm

logger = logging.getLogger(__name__)

# The Monarch Disease Ontology (MONDO), the controlled vocabulary for disease terms used across MaveDB.
MONDO_SYSTEM = "https://purl.obolibrary.org/obo/mondo.owl"
# EBI Ontology Lookup Service (OLS4) — the source for MONDO term search and validation.
OLS_BASE_URL = "https://www.ebi.ac.uk/ols4/api"
MONDO_ONTOLOGY = "mondo"
# OLS can be slow; the default httpx 5s timeout is too tight for its autocomplete on short queries.
OLS_TIMEOUT = 10.0

# The generic "disease or disorder" root, used when a calibration names no specific disease.
MONDO_GENERIC_CODE = "MONDO:0000001"
MONDO_GENERIC_LABEL = "disease or disorder"

DEFAULT_SEARCH_LIMIT = 20


class MondoSuggestion(TypedDict):
    """A single MONDO term resolved from OLS: the CURIE code, its label, and its resolvable IRI."""

    code: str
    label: str
    iri: str


def mondo_iri(code: str) -> str:
    """The resolvable OBO IRI for a MONDO CURIE (e.g. ``MONDO:0015263`` → ``.../obo/MONDO_0015263``)."""
    return f"https://purl.obolibrary.org/obo/{code.replace(':', '_')}"


def mondo_term_to_mappable_concept(term: MondoTerm) -> MappableConcept:
    """Serialize a stored :class:`MondoTerm` as a GA4GH disease ``MappableConcept``."""
    return MappableConcept(
        conceptType="Disease",
        name=term.label,
        primaryCoding=Coding(
            code=term.code,
            system=term.system,
            systemVersion=term.system_version,
            iris=[iriReference(root=mondo_iri(term.code))],
        ),
    )


def mondo_suggestion_to_mappable_concept(suggestion: MondoSuggestion) -> MappableConcept:
    """Build a disease ``MappableConcept`` from an OLS search suggestion (for typeahead results)."""
    return MappableConcept(
        conceptType="Disease",
        name=suggestion["label"],
        primaryCoding=Coding(
            code=suggestion["code"],
            system=MONDO_SYSTEM,
            iris=[iriReference(root=suggestion["iri"])],
        ),
    )


def generic_disease_mappable_concept() -> MappableConcept:
    """The generic "disease or disorder" concept, MaveDB's single unspecified-disease sentinel."""
    return MappableConcept(
        conceptType="Disease",
        name=MONDO_GENERIC_LABEL,
        primaryCoding=Coding(
            code=MONDO_GENERIC_CODE,
            system=MONDO_SYSTEM,
            iris=[iriReference(root=mondo_iri(MONDO_GENERIC_CODE))],
        ),
    )


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


def get_generic_disease_term(db: Session) -> MondoTerm:
    """Get-or-create the generic disease term. Known-canonical, so it needs no OLS round trip."""
    term = (
        db.query(MondoTerm).filter(MondoTerm.system == MONDO_SYSTEM, MondoTerm.code == MONDO_GENERIC_CODE).one_or_none()
    )
    if term is None:
        term = MondoTerm(code=MONDO_GENERIC_CODE, system=MONDO_SYSTEM, label=MONDO_GENERIC_LABEL)  # type: ignore[call-arg]
        db.add(term)
        db.flush()

    return term


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

    term = MondoTerm(code=resolved["code"], system=MONDO_SYSTEM, label=resolved["label"])  # type: ignore[call-arg]
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
