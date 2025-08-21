import os
import re
import logging
from typing import Any, Sequence, Union

from sqlalchemy import text, select, Row
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.db.athena import engine as athena_engine
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant

GNOMAD_DB_NAME = "gnomAD"
GNOMAD_DATA_VERSION = os.getenv("GNOMAD_DATA_VERSION")
logger = logging.getLogger(__name__)


def gnomad_identifier(contig: str, position: Union[str, int], alleles: list[str]) -> str:
    """
    Generate a gnomAD variant identifier based on contig, position, and alleles.
    """
    contig = contig.replace("chr", "")
    position = str(position)

    if len(alleles) != 2:
        raise ValueError("The allele list may only contain two alleles.")

    # Create the identifier in the format: contig-position-allele1-allele2
    return f"{contig}-{position}-{'-'.join(alleles)}"


def gnomad_table_name() -> str:
    """
    Generate the gnomAD table name based on the data version.
    """
    if not GNOMAD_DATA_VERSION:
        raise ValueError("GNOMAD_DATA_VERSION environment variable is not set.")

    table_name = GNOMAD_DATA_VERSION.replace(".", "_")

    save_to_logging_context({"gnomad_table_name": table_name})
    return table_name


def allele_list_from_list_like_string(alleles_string: str) -> list[str]:
    """
    Convert a list-like string representation of alleles into a Python list.

    eg:
    "[A, T]" -> ["A", "T"]
    "[A, TG]" -> ["A", "TG"]
    "" -> []
    "[A, T, C]" -> ValueError: "Invalid format for alleles string."
    """
    if not alleles_string:
        return []

    if not re.match(r"^\[\s*[AGTC]+(?:\s*,\s*[AGTC]+)\s*\]$", alleles_string):
        raise ValueError("Invalid format for alleles string.")

    alleles_string = alleles_string.strip().strip('"[]')
    alleles = [allele.strip() for allele in alleles_string.split(",")]

    return alleles


def gnomad_variant_data_for_caids(caids: Sequence[str]) -> Sequence[Row[Any]]:  # pragma: no cover
    """
    Fetches variant rows from the gnomAD table for a list of CAIDs.

    Args:
        caids (list[str]): A list of CAIDs (Canonical Allele Identifiers) to query.

    Returns:
        Sequence[Row[Any]]: A sequence of database rows containing variant information for the specified CAIDs.
            Each row includes:
                - locus.contig: Chromosome/contig name
                - locus.position: Genomic position
                - alleles: Allele information
                - caid: Canonical Allele Identifier
                - joint.freq.all.ac: Allele count across all samples
                - joint.freq.all.an: Allele number across all samples
                - joint.fafmax.faf95_max_gen_anc: Ancestry of maximum FAF (95% CI) across all populations
                - joint.fafmax.faf95_max: Maximum FAF (95% CI) across all populations

    Raises:
        sqlalchemy.exc.SQLAlchemyError: If there is an error executing the query.
    """

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

    save_to_logging_context({"num_caids": len(caids)})
    logger.debug(msg=f"Fetching gnomAD variants from Athena with query:\n{athena_query}", extra=logging_context())

    with athena_engine.connect() as athena_connection:
        logger.debug(msg="Connected to Athena", extra=logging_context())
        result = athena_connection.execute(text(athena_query))
        rows = result.fetchall()

    save_to_logging_context({"num_gnomad_variant_rows_fetched": len(rows)})
    logger.debug(msg="Done fetching gnomAD variants from Athena", extra=logging_context())

    return rows


def link_gnomad_variants_to_mapped_variants(
    db: Session, gnomad_variant_data: Sequence[Row[Any]], only_current: bool = True
) -> int:
    """
    Links gnomAD variants to mapped variants in the database based on CAIDs. Note that this function does
    not commit this data to the database; it only prepares the relationships.

    Args:
        caids (list[str]): A list of CAIDs to link with gnomAD variants.
    """
    save_to_logging_context({"num_gnomad_variant_rows": len(gnomad_variant_data)})
    save_to_logging_context({"only_current": only_current})
    logger.debug(msg="Linking gnomAD variants to mapped variants", extra=logging_context())

    linked_gnomad_variants = 0
    for index, row in enumerate(gnomad_variant_data, start=1):
        logger.info(
            msg=f"Processing gnomAD variant row {index}/{len(gnomad_variant_data)}: {row.caid}", extra=logging_context()
        )

        mapped_variants_with_caids_query = select(MappedVariant).where(MappedVariant.clingen_allele_id == row.caid)
        if only_current:
            mapped_variants_with_caids_query = mapped_variants_with_caids_query.where(MappedVariant.current.is_(True))
        mapped_variants_with_caids = db.scalars(mapped_variants_with_caids_query).all()

        gnomad_identifier_for_variant = gnomad_identifier(
            row.__getattribute__("locus.contig"),
            row.__getattribute__("locus.position"),
            allele_list_from_list_like_string(row.__getattribute__("alleles")),
        )
        allele_count = int(row.__getattribute__("joint.freq.all.ac"))
        allele_number = int(row.__getattribute__("joint.freq.all.an"))
        allele_frequency = float(allele_count) / float(allele_number)
        faf95_max_ancestry = row.__getattribute__("joint.fafmax.faf95_max_gen_anc")
        faf95_max = row.__getattribute__("joint.fafmax.faf95_max")

        if faf95_max is not None:
            faf95_max = float(faf95_max)

        for mapped_variant in mapped_variants_with_caids:
            # Remove any existing gnomAD variants for this mapped variant that match the current gnomAD data version to avoid data duplication.
            # There should only be one gnomAD variant per mapped variant per gnomAD data version, since each gnomAD variant can only match to one
            # CAID.
            for linked_gnomad_variant in mapped_variant.gnomad_variants:
                if linked_gnomad_variant.db_version == GNOMAD_DATA_VERSION:
                    logger.debug(
                        msg=f"Removing existing gnomAD variant {linked_gnomad_variant.db_identifier} from mapped variant {mapped_variant.id} ({mapped_variant.clingen_allele_id})",
                        extra=logging_context(),
                    )
                    mapped_variant.gnomad_variants.remove(linked_gnomad_variant)

            existing_gnomad_variant = db.scalar(
                select(GnomADVariant).where(
                    GnomADVariant.db_name == "gnomAD",
                    GnomADVariant.db_identifier == gnomad_identifier_for_variant,
                    GnomADVariant.db_version == GNOMAD_DATA_VERSION,
                )
            )

            if existing_gnomad_variant is None:
                logger.debug(
                    msg=f"Creating new gnomAD variant for identifier {gnomad_identifier_for_variant}",
                    extra=logging_context(),
                )
                gnomad_variant = GnomADVariant(
                    db_name=GNOMAD_DB_NAME,
                    db_identifier=gnomad_identifier_for_variant,
                    db_version=GNOMAD_DATA_VERSION,
                    allele_count=allele_count,
                    allele_number=allele_number,
                    allele_frequency=allele_frequency,  # type: ignore
                    faf95_max_ancestry=faf95_max_ancestry,
                    faf95_max=faf95_max,  # type: ignore
                )
            else:
                logger.debug(
                    msg=f"Found existing gnomAD variant for identifier {gnomad_identifier_for_variant}",
                    extra=logging_context(),
                )
                gnomad_variant = existing_gnomad_variant

            if gnomad_variant not in mapped_variant.gnomad_variants:
                mapped_variant.gnomad_variants.append(gnomad_variant)
                linked_gnomad_variants += 1

            db.add(gnomad_variant)

            logger.debug(
                msg=f"Linked gnomAD variant {gnomad_variant.db_identifier} to mapped variant {mapped_variant.id} ({mapped_variant.clingen_allele_id})",
                extra=logging_context(),
            )

        logger.info(
            f"Linked {len(mapped_variants_with_caids)} mapped variants with CAID {row.caid} to gnomAD variant {gnomad_identifier_for_variant}. ({index}/{len(gnomad_variant_data)})"
        )

    save_to_logging_context({"linked_gnomad_variants": linked_gnomad_variants})
    logger.info(
        msg=f"Linked a total of {linked_gnomad_variants} gnomAD variants to mapped variants.",
        extra=logging_context(),
    )
    return linked_gnomad_variants
