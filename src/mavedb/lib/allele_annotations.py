"""Digest-keyed annotation assembly over the deduplicated allele model.

Gathers a set of alleles' external annotations — VEP consequence, gnomAD frequency, ClinVar
assertions — keyed by ``vrs_digest``. The digest is the join key the serving envelopes use to ride
annotations *alongside* the spec-pure Cat-VRS members: Cat-VRS itself carries no annotation slot, so
annotations travel beside it in a flat digest-keyed map.

All three sources are ``ValidTime`` and ``allele_id``-keyed with no reverse collection on
``Allele`` (they are navigated set-wise from the link tables). ``as_of`` reconstructs them at a past
instant via ``live_at``, defaulting to the currently-live rows. Absence is normal and encoded as
``None``/empty — annotations are sparse and computed at all levels with no fixed level→source
mapping (D44/D44b): a genomic allele *may* carry VEP, a coding allele *may* match gnomAD.

VEP and gnomAD are one-live-per-allele, so each is at most one value; ClinVar is multi-live (one
live link per release), so it is a list.

This is the shared assembler for both ``GET /variants/{urn}`` (all of a variant's alleles) and
``GET /alleles/{digest}`` (a single allele) — pass whichever allele set the caller has in hand.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Sequence

from sqlalchemy import Result, select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


@dataclass(frozen=True)
class VepAnnotation:
    """A VEP molecular consequence for an allele and the Ensembl release it was resolved under."""

    consequence: Optional[str]
    source_version: str


@dataclass(frozen=True)
class GnomadAnnotation:
    """gnomAD population frequency for an allele, from its live gnomAD link."""

    allele_frequency: float
    allele_count: int
    allele_number: int
    faf95_max: Optional[float]
    db_version: str
    db_identifier: str


@dataclass(frozen=True)
class ClinvarAnnotation:
    """One ClinVar assertion for an allele (one per live release — an allele may carry several)."""

    clinical_significance: str
    clinical_review_status: str
    clinvar_variation_id: Optional[str]
    clinvar_allele_id: str
    db_version: str


@dataclass
class AlleleAnnotations:
    """The external annotations for a single allele. Every field is sparse — a source with no data
    for the allele is ``None`` (vep/gnomad) or an empty list (clinvar)."""

    vep: Optional[VepAnnotation] = None
    gnomad: Optional[GnomadAnnotation] = None
    clinvar: list[ClinvarAnnotation] = field(default_factory=list)


def get_allele_annotations(
    db: Session, alleles: Sequence[Allele], *, as_of: Optional[datetime] = None
) -> dict[str, AlleleAnnotations]:
    """Assemble the digest-keyed annotation map for ``alleles``.

    Returns one :class:`AlleleAnnotations` per distinct ``vrs_digest`` present in ``alleles``. An
    allele with no annotations still gets an (empty) entry, so the map's keys mirror the alleles
    handed in — the envelope can join every Cat-VRS member to a (possibly empty) annotation block.
    ``as_of`` reconstructs each source at a past instant; it defaults to the currently-live rows.
    """
    # id -> digest for the alleles we were handed; digest is the map key the envelope joins on.
    digest_by_id = {allele.id: allele.vrs_digest for allele in alleles}
    annotations: dict[str, AlleleAnnotations] = {
        digest: AlleleAnnotations() for digest in digest_by_id.values() if digest is not None
    }
    if not digest_by_id:
        return annotations

    allele_ids = list(digest_by_id.keys())

    # VEP — at most one live consequence per allele.
    vep_rows = db.execute(
        select(
            VepAlleleConsequence.allele_id,
            VepAlleleConsequence.functional_consequence,
            VepAlleleConsequence.source_version,
        )
        .where(VepAlleleConsequence.allele_id.in_(allele_ids))
        .where(VepAlleleConsequence.live_at(as_of))
    ).tuples()
    for allele_id, consequence, source_version in vep_rows:
        digest = digest_by_id[allele_id]
        if digest is not None:
            annotations[digest].vep = VepAnnotation(consequence=consequence, source_version=source_version)

    # gnomAD — at most one live link per allele, resolving to a frequency record.
    gnomad_rows: Result[tuple[int, GnomADVariant]] = db.execute(
        select(GnomadAlleleLink.allele_id, GnomADVariant)
        .join(GnomADVariant, GnomADVariant.id == GnomadAlleleLink.gnomad_variant_id)
        .where(GnomadAlleleLink.allele_id.in_(allele_ids))
        .where(GnomadAlleleLink.live_at(as_of))
    )
    for allele_id, gnomad_variant in gnomad_rows:
        digest = digest_by_id[allele_id]
        if digest is not None:
            annotations[digest].gnomad = GnomadAnnotation(
                allele_frequency=gnomad_variant.allele_frequency,
                allele_count=gnomad_variant.allele_count,
                allele_number=gnomad_variant.allele_number,
                faf95_max=gnomad_variant.faf95_max,
                db_version=gnomad_variant.db_version,
                db_identifier=gnomad_variant.db_identifier,
            )

    # ClinVar — multi-live (one live link per release), so each allele accumulates a list.
    clinvar_rows: Result[tuple[int, ClinvarControl]] = db.execute(
        select(ClinvarAlleleLink.allele_id, ClinvarControl)
        .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
        .where(ClinvarAlleleLink.allele_id.in_(allele_ids))
        .where(ClinvarAlleleLink.live_at(as_of))
    )
    for allele_id, clinvar_control in clinvar_rows:
        digest = digest_by_id[allele_id]
        if digest is not None:
            annotations[digest].clinvar.append(
                ClinvarAnnotation(
                    clinical_significance=clinvar_control.clinical_significance,
                    clinical_review_status=clinvar_control.clinical_review_status,
                    clinvar_variation_id=clinvar_control.clinvar_variation_id,
                    clinvar_allele_id=clinvar_control.db_identifier,
                    db_version=clinvar_control.db_version,
                )
            )

    return annotations
