"""A score set seeded with every source the CSV exports read, shared by the export tests."""

from datetime import datetime, timedelta
from typing import NamedTuple

from sqlalchemy import func, insert, select

from mavedb.lib.csv.namespaces import calibration_namespace_for_urn
from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.variant import Variant
from tests.helpers.constants import TEST_GNOMAD_VARIANT
from tests.helpers.util.annotation import add_clinvar_control, seed_mapping_record
from tests.helpers.util.score_calibration import add_pathogenicity_calibration

CALIBRATION_URN = "urn:mavedb:calibration-11111111-1111-1111-1111-111111111111"
RESEARCH_CALIBRATION_URN = "urn:mavedb:calibration-22222222-2222-2222-2222-222222222222"
RETIRED_CALIBRATION_URN = "urn:mavedb:calibration-33333333-3333-3333-3333-333333333333"
CALIBRATION_NS = calibration_namespace_for_urn(CALIBRATION_URN)
RESEARCH_CALIBRATION_NS = calibration_namespace_for_urn(RESEARCH_CALIBRATION_URN)
RETIRED_CALIBRATION_NS = calibration_namespace_for_urn(RETIRED_CALIBRATION_URN)

BENIGN_ABSENT_VARIANT = 5
"""Mapped to a record with assay-level HGVS but no allele, as an intronic or synonymous variant is."""
FAILED_VARIANT = 12
"""Mapped to a record with nothing on it, as a failed mapping is."""

EXTRA_VARIANTS = [
    (2, "c.2A>T", "NA", None),
    (10, "c.10A>T", " na ", "NULL"),
    (3, "c.3A>T", None, ""),
    (11, "c.11A>T", "p.Thr11Ser", "n/a"),
    (4, "c.4A>T", "None", "  "),
    (BENIGN_ABSENT_VARIANT, "c.5+10A>T", None, None),
    (FAILED_VARIANT, "c.12A>T", None, None),
]
"""Variants beside the mapped `#1`. Numbered past 9 so variant-number order differs from URN order, with HGVS that
exercises the null tokens: `hgvs_splice` is null on every row, `hgvs_pro` on all but one."""


class CsvExportScoreSet(NamedTuple):
    score_set: ScoreSet
    before_update: datetime
    """When `#1`'s earlier gnomAD and ClinVar 2025_01 links were live, for reconstructing an export as it stood
    then."""


def _measured_allele_and_group(db, variant_id: int) -> tuple[Allele, ProjectionGroup]:
    allele, group = db.execute(
        select(Allele, ProjectionGroup)
        .join(MappingRecordAllele, MappingRecordAllele.allele_id == Allele.id)
        .join(ProjectionGroup, ProjectionGroup.id == MappingRecordAllele.projection_group)
        .join(MappingRecord, MappingRecord.id == MappingRecordAllele.mapping_record_id)
        .where(MappingRecord.variant_id == variant_id, MappingRecordAllele.is_authoritative.is_(True))
    ).one()
    return allele, group


def seed_csv_export_score_set(session, mapped) -> CsvExportScoreSet:
    """Give *mapped*'s score set every source a CSV export reads: `#1` is mapped and carries every annotation,
    `#5` and `#12` have a mapping record but no allele, and the rest are unmapped."""
    score_set = mapped.variant.score_set
    score_set.dataset_columns = {"score_columns": ["score", "se"], "count_columns": ["c_0"]}
    session.add(score_set)
    session.add(TargetGene(name="PTEN", category=TargetCategory.protein_coding, score_set_id=score_set.id))
    session.commit()

    session.execute(
        insert(Variant.__table__),
        [
            {
                "urn": f"{score_set.urn}#{number}",
                "scoreset_id": score_set.id,
                "data": {"score_data": {"score": number / 10, "se": "NA"}, "count_data": {"c_0": number}},
                "hgvs_nt": hgvs_nt,
                "hgvs_pro": hgvs_pro,
                "hgvs_splice": hgvs_splice,
            }
            for number, hgvs_nt, hgvs_pro, hgvs_splice in EXTRA_VARIANTS
        ],
    )
    session.commit()

    benign_absent = session.scalars(
        select(Variant).where(Variant.urn == f"{score_set.urn}#{BENIGN_ABSENT_VARIANT}")
    ).one()
    seed_mapping_record(
        session, benign_absent, alleles=[], assay_level="genomic", hgvs_assay_level="NC_000018.10:g.23536900A>T"
    )
    seed_mapping_record(session, f"{score_set.urn}#{FAILED_VARIANT}", alleles=[], assay_level="genomic")

    add_clinvar_control(session, mapped, "Pathogenic", "criteria provided, single submitter", "01_2024")
    add_clinvar_control(session, mapped, "Benign", "practice guideline", "01_2025")

    allele, group = _measured_allele_and_group(session, mapped.variant_id)
    allele.clingen_allele_id = "CA123456"
    group.hgvs_c = "NM_000314.8:c.100A>G"
    group.hgvs_p = "NP_000305.3:p.Lys34Glu"
    gnomad_variant = GnomADVariant(**TEST_GNOMAD_VARIANT)
    session.add_all([allele, group, gnomad_variant])
    session.commit()
    gnomad_link = GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gnomad_variant.id)
    session.add(gnomad_link)
    session.commit()

    # A later gnomAD release replaces `#1`'s record, and a ClinVar re-ingest links it to a different 2025_01
    # record. Both handoffs are stamped a second out so the earlier links are unambiguously the live ones at
    # `before_update`.
    before_update = session.scalar(select(func.now()))
    updated_at = before_update + timedelta(seconds=1)

    newer_gnomad = GnomADVariant(**{**TEST_GNOMAD_VARIANT, "db_version": "v2.pytest", "allele_count": 4})
    session.add(newer_gnomad)
    session.flush()
    gnomad_link.supersede_with(
        session, GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=newer_gnomad.id), at=updated_at
    )

    clinvar_link = session.scalars(
        select(ClinvarAlleleLink)
        .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
        .where(ClinvarAlleleLink.allele_id == allele.id, ClinvarControl.db_version == "01_2025")
    ).one()
    relinked = ClinvarControl(
        db_identifier="183059",
        gene_symbol="PTEN",
        clinical_significance="Likely benign",
        clinical_review_status="reviewed by expert panel",
        db_name="ClinVar",
        db_version="01_2025",
    )
    session.add(relinked)
    session.flush()
    clinvar_link.supersede_with(
        session, ClinvarAlleleLink(allele_id=allele.id, clinvar_control_id=relinked.id), at=updated_at
    )
    session.commit()

    calibration = add_pathogenicity_calibration(
        session, score_set, [mapped.variant, benign_absent], urn=CALIBRATION_URN, title="Calibration"
    )
    add_pathogenicity_calibration(
        session, score_set, [], urn=RESEARCH_CALIBRATION_URN, title="Research calibration", research_use_only=True
    )
    # Free text that needs every kind of CSV quoting: a comma, a quote and a newline.
    retired = add_pathogenicity_calibration(
        session, score_set, [mapped.variant], urn=RETIRED_CALIBRATION_URN, title='Retired "v1",\nreplaced'
    )
    calibration.superseded_calibration_id = retired.id
    session.add(calibration)
    session.commit()

    session.refresh(score_set)
    return CsvExportScoreSet(score_set, before_update)
