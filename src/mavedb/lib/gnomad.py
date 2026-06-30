import logging
import os
import re
from enum import Enum
from typing import Any, Sequence, Union

from sqlalchemy import Connection, Row, func, select, text
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.utils import batched
from mavedb.models.allele import Allele
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant

logger = logging.getLogger(__name__)


GNOMAD_DB_NAME = "gnomAD"
GNOMAD_DATA_VERSION = os.getenv("GNOMAD_DATA_VERSION", "v4.1")
_CAID_LEADING_ZERO_RE = r"^(CA)0+([0-9])"
"""
Strip leading zeros from a CAID's numeric portion, keeping at least one digit. 
Kept byte-for-byte in sync with the SQL form used to normalize Allele.clingen_allele_id 
in link_gnomad_variants_to_alleles.
"""


class GnomadLinkVerdict(str, Enum):
    """Per-allele outcome of a gnomAD linking run, returned for every allele the linker touched.

    The single source of truth for what happened to an allele's link this run — the caller derives
    annotation status directly from this, never by re-querying link state (which would be a second,
    drift-prone source of truth).
    """

    CREATED = "created"  # link created or superseded this run (a new/changed live link)
    UNCHANGED = "unchanged"  # a live link already pointed at the resolved variant; left untouched


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


def normalize_caid(caid: str) -> str:
    """Normalize a ClinGen CAID by stripping leading zeros from its numeric portion.

    The gnomAD Hail/Athena dump drops leading zeros from CAIDs — MaveDB's ``CA025094`` is recorded as
    ``CA25094`` — so an exact-string join silently misses every zero-padded CAID (issue #722). Both
    sides of the join are normalized to the unpadded form to repair the match. ``CA025094`` and
    ``CA25094`` denote the same ClinGen allele, so this can never collide distinct alleles. A value
    that is not a recognizable CAID (no ``CA`` prefix + digits) is returned unchanged.
    """
    return re.sub(_CAID_LEADING_ZERO_RE, r"\1\2", caid)


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


def gnomad_variant_data_for_caids(
    athena_session: Connection, caids: Sequence[str]
) -> Sequence[Row[Any]]:  # pragma: no cover
    """
    Fetches variant rows from the gnomAD table for a list of CAIDs. Athena has a maximum character limit of 262144
    in queries. CAIDs are about 12 characters long on average + 4 for two quotes, a comma and a space. Chunk our list
    into chunks of 260000/16=16250 so we are guaranteed to remain under the character limit.

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
    # Normalize to the unpadded form the dump stores so the IN-list matches zero-padded CAIDs (see issue #722).
    chunked_caids = batched(caids, 16250)
    caid_strs = [",".join(f"'{normalize_caid(caid)}'" for caid in chunk) for chunk in chunked_caids]
    save_to_logging_context({"num_caids": len(caids), "num_chunks": len(caid_strs)})

    result_rows: list[Row[Any]] = []
    for chunk_index, caid_str in enumerate(caid_strs):
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
        logger.debug(
            msg=f"Fetching gnomAD variants from Athena (batch {chunk_index}) with query:\n{athena_query}",
            extra=logging_context(),
        )

        result = athena_session.execute(text(athena_query))
        rows = result.fetchall()
        result_rows.extend(rows)

        logger.debug(f"Fetched {len(rows)} gnomAD variants from Athena (batch {chunk_index}).")

        save_to_logging_context({"num_gnomad_variant_rows_fetched": len(result_rows)})
        logger.debug(msg="Done fetching gnomAD variants from Athena", extra=logging_context())

    return result_rows


def link_gnomad_variants_to_alleles(
    db: Session, gnomad_variant_data: Sequence[Row[Any]]
) -> dict[int, GnomadLinkVerdict]:
    """Link gnomAD variants to deduplicated alleles by CAID, superseding only on change.

    Every ``Allele`` carrying the row's ``clingen_allele_id`` (populated by CAR) is linked through a
    valid-time :class:`GnomadAlleleLink`, so one gnomAD variant fans out to every allele sharing the
    CAID (cross-score-set dedup included). Each allele holds at most one live link, superseded **only
    on change**: a live link already pointing to the resolved variant is left untouched (an unchanged
    re-run writes no spurious valid-time boundary); a new/different/older-version target retires it and
    inserts a successor, keyed on ``allele_id`` so a version bump replaces rather than accumulates. The
    guard is load-bearing despite the job's upstream skip — shared CAIDs and ``force`` runs still reach
    it. A current-version link to a *different* identifier (a CAID re-resolved within one release) is
    logged and superseded newest-wins, not raised.

    Does not commit. Returns a verdict per allele *touched* this run (matched a CAID-bearing row):
    :attr:`GnomadLinkVerdict.CREATED` for a created/superseded link, :attr:`~GnomadLinkVerdict.UNCHANGED`
    for a live link left in place. Alleles absent from the map were matched by no row — the caller reads
    those as "gnomAD had no record". This is the single source of truth for per-allele status; callers
    must not re-derive it by re-querying link state.
    """
    save_to_logging_context({"num_gnomad_variant_rows": len(gnomad_variant_data)})
    logger.debug(msg="Linking gnomAD variants to alleles", extra=logging_context())

    verdicts: dict[int, GnomadLinkVerdict] = {}
    for index, row in enumerate(gnomad_variant_data, start=1):
        logger.info(
            msg=f"Processing gnomAD variant row {index}/{len(gnomad_variant_data)}: {row.caid}", extra=logging_context()
        )

        # Match on the unpadded CAID: the dump's caid is already stripped, while the stored CAID may
        # be zero-padded, so normalize both sides (issue #722). regexp_replace mirrors normalize_caid.
        alleles_with_caid = db.scalars(
            select(Allele).where(
                func.regexp_replace(Allele.clingen_allele_id, _CAID_LEADING_ZERO_RE, r"\1\2")
                == normalize_caid(row.caid)
            )
        ).all()
        if not alleles_with_caid:
            continue

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

        # One gnomAD variant per (identifier, version): get-or-create so repeated CAIDs and re-runs
        # reuse the same row. Flush so a freshly created variant has an id for the link below.
        gnomad_variant = db.scalar(
            select(GnomADVariant).where(
                GnomADVariant.db_name == GNOMAD_DB_NAME,
                GnomADVariant.db_identifier == gnomad_identifier_for_variant,
                GnomADVariant.db_version == GNOMAD_DATA_VERSION,
            )
        )
        if gnomad_variant is None:
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
            db.add(gnomad_variant)
            db.flush()
        else:
            logger.debug(
                msg=f"Found existing gnomAD variant for identifier {gnomad_identifier_for_variant}",
                extra=logging_context(),
            )

        for allele in alleles_with_caid:
            live_link = db.scalar(
                select(GnomadAlleleLink).where(
                    GnomadAlleleLink.allele_id == allele.id,
                    GnomadAlleleLink.current,
                )
            )

            # No change: live link already points here — leave it untouched (no spurious boundary).
            if live_link is not None and live_link.gnomad_variant_id == gnomad_variant.id:
                verdicts.setdefault(allele.id, GnomadLinkVerdict.UNCHANGED)
                continue

            if (
                live_link is not None
                and live_link.gnomad_variant.db_version == GNOMAD_DATA_VERSION
                and live_link.gnomad_variant.db_identifier != gnomad_identifier_for_variant
            ):
                logger.warning(
                    msg=(
                        f"CAID {allele.clingen_allele_id} for allele {allele.id} resolved to "
                        f"{gnomad_identifier_for_variant} at version {GNOMAD_DATA_VERSION}, but a live link "
                        f"already points to {live_link.gnomad_variant.db_identifier} at the same version. "
                        "Superseding (newest wins); investigate the gnomAD source for a re-resolved CAID."
                    ),
                    extra=logging_context(),
                )

            # Change: retire any live link for the allele, insert the successor (allele-keyed, so a
            # version bump replaces rather than accumulates).
            GnomadAlleleLink.supersede_live_where(
                db,
                [GnomadAlleleLink(allele_id=allele.id, gnomad_variant_id=gnomad_variant.id)],
                GnomadAlleleLink.allele_id == allele.id,
            )
            verdicts[allele.id] = GnomadLinkVerdict.CREATED  # created always wins over a same-run unchanged

            logger.debug(
                msg=f"Linked gnomAD variant {gnomad_variant.db_identifier} to allele {allele.id} ({allele.clingen_allele_id})",
                extra=logging_context(),
            )

        logger.info(
            f"Processed {len(alleles_with_caid)} alleles with CAID {row.caid} for gnomAD variant {gnomad_identifier_for_variant}. ({index}/{len(gnomad_variant_data)})"
        )

    changed_allele_count = sum(1 for v in verdicts.values() if v is GnomadLinkVerdict.CREATED)
    save_to_logging_context({"changed_allele_count": changed_allele_count})
    logger.info(
        msg=f"Created or superseded {changed_allele_count} allele links this run.",
        extra=logging_context(),
    )
    return verdicts
