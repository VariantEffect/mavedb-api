"""Live ClinVar-control lookups for a score set, over the allele-link substrate.

A score set's clinical controls are the ClinVar assertions whose allele is live-linked to one of the
score set's variants, reached by walking ``ClinvarAlleleLink → Allele → MappingRecordAllele →
MappingRecord → Variant`` with every ``ValidTime`` hop constrained to the same instant (``as_of``,
defaulting to currently-live). Both serving endpoints — the controls list
(``GET /score-sets/{urn}/clinical-controls``) and its options facet (``.../options``) walk the chain
that is defined here.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, TypeVar

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant

SelectT = TypeVar("SelectT", bound=Select[Any])


@dataclass(frozen=True)
class ControlVariantLink:
    """One (score-set variant, annotated allele) pair that a clinical control reaches.

    ``allele_digest`` is the VRS digest of the allele the control annotates. A client compares it to
    the variant's authoritative ``assay_level_digest``:
      - a match is a call *on the assayed level* (this call takes precedence)
      - a mismatch is a call on an *encoding* level (this call is only used when the assayed level is
        unannotated).

    Without the digest, the two are indistinguishable and precedence is unimplementable.
    """

    variant_urn: str
    allele_digest: str


def _scope_to_live_score_set_controls(stmt: SelectT, score_set_id: int, as_of: Optional[datetime]) -> SelectT:
    """Join ``stmt`` (already selecting from :class:`ClinvarControl`) down the allele-link chain to the
    score set's variants and constrain every ``ValidTime`` hop to ``as_of``. ``distinct`` because the
    chain fans a control out across each variant it links to."""
    return (
        stmt.join(ClinvarAlleleLink, ClinvarAlleleLink.clinvar_control_id == ClinvarControl.id)
        .join(Allele, Allele.id == ClinvarAlleleLink.allele_id)
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(ClinvarAlleleLink.live_at(as_of))
        .where(MappingRecordAllele.live_at(as_of))
        .where(MappingRecord.live_at(as_of))
        .where(Variant.score_set_id == score_set_id)
        .distinct()
    )


def get_clinical_controls_with_variant_urns(
    db: Session,
    score_set_id: int,
    *,
    as_of: Optional[datetime] = None,
    db_name: Optional[str] = None,
    db_version: Optional[str] = None,
) -> list[tuple[ClinvarControl, list[ControlVariantLink]]]:
    """The live ClinVar controls for a score set, each paired with the score-set variants that link to
    it. Every link carrying the digest of the allele the control annotates (see :class:`ControlVariantLink`).
    Optionally narrowed to one control DB (``db_name``) and/or release (``db_version``). Controls come back
    in first-seen order. Each control's links preserve query order."""
    stmt = _scope_to_live_score_set_controls(
        select(ClinvarControl, Variant.urn.label("variant_urn"), Allele.vrs_digest.label("allele_digest")),
        score_set_id,
        as_of,
    )
    if db_name is not None:
        stmt = stmt.where(ClinvarControl.db_name == db_name)
    if db_version is not None:
        stmt = stmt.where(ClinvarControl.db_version == db_version)

    controls: dict[int, ClinvarControl] = {}
    links_by_control: dict[int, list[ControlVariantLink]] = {}

    # A persisted variant reached through the join always carries a URN. Narrow the nullable column so the
    # URN matches the view model's required `str`. TODO(#372)
    for ctrl, variant_urn, allele_digest in db.execute(stmt).tuples():
        if variant_urn is None:
            continue

        if ctrl.id not in controls:
            controls[ctrl.id] = ctrl
            links_by_control[ctrl.id] = []
        links_by_control[ctrl.id].append(ControlVariantLink(variant_urn=variant_urn, allele_digest=allele_digest))

    return [(ctrl, links_by_control[ctrl.id]) for ctrl in controls.values()]


def get_clinical_control_options(
    db: Session, score_set_id: int, *, as_of: Optional[datetime] = None
) -> list[tuple[str, list[str]]]:
    """The options facet: each control DB live-linked to the score set, paired with its available
    versions newest-first."""
    stmt = _scope_to_live_score_set_controls(
        select(ClinvarControl.db_name, ClinvarControl.db_version), score_set_id, as_of
    )
    options: dict[str, list[str]] = {}
    for db_name, db_version in db.execute(stmt):
        options.setdefault(db_name, []).append(db_version)

    return [
        (db_name, sorted(versions, key=clinvar_version_sort_key, reverse=True)) for db_name, versions in options.items()
    ]


def clinvar_version_sort_key(version: str) -> tuple[int, int]:
    """Sort key for a ClinVar release string in ``MM_YYYY`` form, ordering by ``(year, month)``.
    Unparseable versions sort to the bottom as ``(0, 0)`` rather than raising."""
    try:
        month, year = version.split("_")
        return int(year), int(month)
    except (ValueError, AttributeError):
        return (0, 0)
