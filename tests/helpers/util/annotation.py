"""Seeding helpers for the new-model annotation substrate.

The serving layer (the lean whole-set view, the ``/variants/{urn}`` detail envelope, allele
annotations, and the score-set clinical-controls routes) reads a variant's mapped alleles and
their annotations through the ``MappingRecord`` / ``Allele`` / ``MappingRecordAllele`` graph and
the per-source link tables (``ClinvarAlleleLink`` / ``GnomadAlleleLink`` / ``VepAlleleConsequence``)
— not the legacy ``MappedVariant`` association tables. These helpers build that graph in tests so a
score set's variants carry alleles and annotations the way the mapping/annotation pipeline would.

``seed_mapping_record`` is the single reusable builder; higher-level helpers (e.g.
``link_clinical_controls_to_alleles``) compose it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Sequence, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.allele import Allele
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


@dataclass
class AlleleSpec:
    """One allele on a mapping record, plus the annotations that hang off it.

    Digests are the allele's identity in the serving layer, so they must be distinct within a test.
    ``clinvar_control_ids`` / ``gnomad_variant_ids`` reference rows seeded elsewhere (the router
    fixtures seed ClinVar controls with ids 1 and 2); each id becomes a live link on this allele.
    """

    digest: str
    level: str = "cdna"
    is_authoritative: bool = False
    clingen_allele_id: Optional[str] = None
    hgvs_c: Optional[str] = None
    hgvs_p: Optional[str] = None
    hgvs_g: Optional[str] = None
    post_mapped: Optional[dict] = None
    vep_consequence: Optional[str] = None
    clinvar_control_ids: Sequence[int] = field(default_factory=tuple)
    gnomad_variant_ids: Sequence[int] = field(default_factory=tuple)


def seed_mapping_record(
    session: Session,
    variant: Union[Variant, str],
    *,
    alleles: Sequence[AlleleSpec],
    assay_level: str = "cdna",
    hgvs_assay_level: Optional[str] = None,
    mapping_api_version: str = "test.0.0",
    valid_from: Optional[datetime] = None,
) -> MappingRecord:
    """Give ``variant`` a live mapping record carrying ``alleles`` and their annotations.

    ``variant`` may be a ``Variant`` instance or its URN. When ``valid_from`` is set it stamps the
    record, its allele links, and every annotation link, so ``as_of`` reconstruction tests can place
    the whole graph at a chosen instant (absent it, ``valid_from`` defaults to ``now()`` and the row
    is live). Returns the created ``MappingRecord``.
    """
    if isinstance(variant, str):
        resolved = session.scalar(select(Variant).where(Variant.urn == variant))
        assert resolved is not None, f"variant with URN '{variant}' not found"
        variant = resolved

    record = MappingRecord(
        variant_id=variant.id,
        assay_level=assay_level,
        hgvs_assay_level=hgvs_assay_level,
        mapping_api_version=mapping_api_version,
    )
    if valid_from is not None:
        record.valid_from = valid_from
    session.add(record)
    session.commit()

    for spec in alleles:
        allele = Allele(
            vrs_digest=spec.digest,
            level=spec.level,
            clingen_allele_id=spec.clingen_allele_id,
            hgvs_c=spec.hgvs_c,
            hgvs_p=spec.hgvs_p,
            hgvs_g=spec.hgvs_g,
            post_mapped=spec.post_mapped if spec.post_mapped is not None else {"type": "Allele"},
        )
        session.add(allele)
        session.commit()

        links: list[Any] = [
            MappingRecordAllele(
                mapping_record_id=record.id, allele_id=allele.id, is_authoritative=spec.is_authoritative
            )
        ]
        if spec.vep_consequence is not None:
            links.append(
                VepAlleleConsequence(
                    allele_id=allele.id,
                    functional_consequence=spec.vep_consequence,
                    source_version="116",
                    access_date="2026-01-01",
                )
            )
        links.extend(ClinvarAlleleLink(allele_id=allele.id, clinvar_control_id=cid) for cid in spec.clinvar_control_ids)
        links.extend(GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gid) for gid in spec.gnomad_variant_ids)

        if valid_from is not None:
            for link in links:
                link.valid_from = valid_from
        session.add_all(links)

    session.commit()
    return record
