"""MONDO disease-term vocabulary: constants, MappableConcept serialization, and the generic term.

Calibrations carry a disease/disorder context drawn from the Monarch Disease Ontology (MONDO). This
module owns the vocabulary's static surface: the generic "disease or disorder" root, conversion of a
stored :class:`MondoTerm` (or an OLS suggestion) to the GA4GH ``MappableConcept`` served on the wire, and
get-or-create of the generic term (mirroring :func:`mavedb.lib.taxonomies.find_or_create_taxonomy`).

The generic term (``MONDO:0000001``) is the write-time default for a calibration with no specific
disease, so ``disease_term_id`` is never null. The annotation layer serializes whatever term a
calibration carries — generic or specific — as its VA-Spec disease condition (see
:func:`mavedb.lib.annotation.condition.calibration_disease_condition`).

Typeahead search and OLS-backed validation of a submitted code live in :mod:`mavedb.lib.mondo_ols`,
kept separate because they need :mod:`mavedb.lib.logging.context` (an optional "server" dependency,
tracked for a core-compatible rework in
`#459 <https://github.com/VariantEffect/mavedb-api/issues/459>`_); this module has no such dependency,
so callers that only need the constants or serialization (e.g. the score calibration view model) stay
importable under core dependencies.
"""

from typing import TypedDict

from ga4gh.core.models import Coding, MappableConcept, iriReference
from sqlalchemy.orm import Session

from mavedb.models.mondo_term import MondoTerm

# The Monarch Disease Ontology (MONDO), the controlled vocabulary for disease terms used across MaveDB.
MONDO_SYSTEM = "https://purl.obolibrary.org/obo/mondo.owl"

# The generic "disease or disorder" root, used when a calibration names no specific disease.
MONDO_GENERIC_CODE = "MONDO:0000001"
MONDO_GENERIC_LABEL = "disease or disorder"


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
