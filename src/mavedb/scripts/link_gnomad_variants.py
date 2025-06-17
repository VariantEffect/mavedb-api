import logging
from typing import Sequence

import click
from sqlalchemy import select, text, Select
from sqlalchemy.orm import Session

from mavedb.db.athena import engine as athena_engine
from mavedb.lib.gnomad import (
    allele_list_from_list_like_string,
    gnomad_table_name,
    gnomad_identifier,
    GNOMAD_DATA_VERSION,
    GNOMAD_DB_NAME,
)
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.scripts.environment import with_database_session


logger = logging.getLogger(__name__)


def only_current_filter(query: Select, only_current: bool) -> Select:
    """
    Apply a filter to the query to only include current mapped variants if `only_current` is True.
    """
    if only_current:
        return query.where(MappedVariant.current.is_(True))
    return query


@click.command()
@with_database_session
@click.option(
    "--score-set-urn", multiple=True, type=str, help="Score set URN(s) to process. Can be used multiple times."
)
@click.option("--all", "all_score_sets", is_flag=True, help="Process all score sets in the database.", default=False)
@click.option("--only-current", is_flag=True, help="Only process current mapped variants.", default=True)
def link_gnomad_variants(db: Session, score_set_urn: list[str], all_score_sets: bool, only_current: bool) -> None:
    """
    Query AWS Athena for gnomAD variants matching mapped variant CAIDs for one or more score sets.
    """
    # 1. Collect all CAIDs for mapped variants in the selected score sets
    if all_score_sets:
        score_sets = db.query(ScoreSet.id).all()
        score_set_ids = [s.id for s in score_sets]
    else:
        if not score_set_urn:
            logger.error("No score set URNs specified.")
            return

        score_sets = db.query(ScoreSet.id).filter(ScoreSet.urn.in_(score_set_urn)).all()
        score_set_ids = [s.id for s in score_sets]
        if len(score_set_ids) != len(score_set_urn):
            logger.warning("Some provided URNs were not found in the database.")

    if not score_set_ids:
        logger.error("No score sets found.")
        return

    caid_query = only_current_filter(
        (
            select(MappedVariant.clingen_allele_id)
            .join(Variant)
            .where(Variant.score_set_id.in_(score_set_ids), MappedVariant.clingen_allele_id.is_not(None))
        ),
        only_current,
    ).distinct()

    caids = db.scalars(caid_query.distinct()).all()
    if not caids:
        logger.error("No CAIDs found for the selected score sets.")
        return

    logger.info(f"Found {len(caids)} CAIDs for the selected score sets to link to gnomAD variants.")

    # 2. Query Athena for gnomAD variants matching the CAIDs
    caid_str = ",".join(f"'{caid}'" for caid in caids)
    athena_query = f"""
        SELECT
            "locus.contig",
            "locus.position",
            "alleles",
            "caid",
            "joint.freq.all.ac",
            "joint.freq.all.an",
            "joint.fafmax.faf95_max_gen_anc",
            "joint.fafmax.faf95_max"
        FROM
            {gnomad_table_name()}
        WHERE
            caid IN ({caid_str})
    """
    with athena_engine.connect() as athena_connection:
        result = athena_connection.execute(text(athena_query))
        rows = result.fetchall()

    if not rows:
        logger.error("No gnomAD records found for the provided CAIDs.")
        return

    logger.info(f"Fetched {len(rows)} gnomAD records from Athena.")

    # 3. Link gnomAD variants to mapped variants in the database
    for index, row in enumerate(rows, start=1):
        existing_clinical_control_query = only_current_filter(
            select(MappedVariant).where(MappedVariant.clingen_allele_id == row.caid), only_current
        )
        existing_clinical_controls: Sequence[MappedVariant] = db.scalars(existing_clinical_control_query).all()

        gnomad_identifier_for_variant = gnomad_identifier(
            row.__getattribute__("locus.contig"),
            row.__getattribute__("locus.position"),
            allele_list_from_list_like_string(row.__getattribute__("alleles")),
        )

        for mapped_variant in existing_clinical_controls:
            # Remove any existing gnomAD variants for this mapped variant that match the current gnomAD data version to avoid data duplication.
            # There should only be one gnomAD variant per mapped variant per gnomAD data version, since each gnomAD variant can only match to one
            # CAID.
            for linked_gnomad_variant in mapped_variant.gnomad_variants:
                if linked_gnomad_variant.db_version == GNOMAD_DATA_VERSION:
                    mapped_variant.gnomad_variants.remove(linked_gnomad_variant)

            existing_gnomad_variant = db.scalar(
                select(GnomADVariant).where(
                    GnomADVariant.db_name == "gnomAD",
                    GnomADVariant.db_identifier == gnomad_identifier_for_variant,
                    GnomADVariant.db_version == GNOMAD_DATA_VERSION,
                )
            )

            if existing_gnomad_variant is None:
                gnomad_variant = GnomADVariant(
                    db_name=GNOMAD_DB_NAME,
                    db_identifier=gnomad_identifier_for_variant,
                    db_version=GNOMAD_DATA_VERSION,
                    allele_count=int(row.__getattribute__("joint.freq.all.ac")),
                    allele_number=int(row.__getattribute__("joint.freq.all.an")),
                    allele_frequency=float(row.__getattribute__("joint.freq.all.ac"))
                    / float(row.__getattribute__("joint.freq.all.an")),  # type: ignore
                    faf95_max_ancestry=row.__getattribute__("joint.fafmax.faf95_max_gen_anc"),
                    faf95_max=float(row.__getattribute__("joint.fafmax.faf95_max")),  # type: ignore
                )
                mapped_variant.gnomad_variants.append(gnomad_variant)
            else:
                gnomad_variant = existing_gnomad_variant
                if gnomad_variant not in mapped_variant.gnomad_variants:
                    mapped_variant.gnomad_variants.append(gnomad_variant)

            db.add(gnomad_variant)

        logger.info(
            f"Linked {len(existing_clinical_controls)} mapped variants with CAID {row.caid} to gnomAD variant {gnomad_variant.db_identifier}. ({index}/{len(rows)})"
        )

    logger.info("Done linking gnomAD variants.")


if __name__ == "__main__":
    link_gnomad_variants()
