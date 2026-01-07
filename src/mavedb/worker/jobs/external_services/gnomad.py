"""gnomAD variant linking jobs for population frequency annotation.

This module handles linking of mapped variants to gnomAD (Genome Aggregation Database)
variants to provide population frequency and other genomic context information.
This enrichment helps researchers understand the clinical significance and
rarity of variants in their datasets.
"""

import logging
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.gnomad import gnomad_variant_data_for_caids, link_gnomad_variants_to_mapped_variants
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.slack import send_slack_error, send_slack_message
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.job_state import setup_job_state

logger = logging.getLogger(__name__)


async def link_gnomad_variants(ctx: dict, correlation_id: str, score_set_id: int) -> dict:
    logging_context = {}
    score_set = None
    text = "Could not link mappings to gnomAD variants for score set %s. Mappings for this score set should be linked manually."
    try:
        db: Session = ctx["db"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logger.info(msg="Started gnomAD variant linkage", extra=logging_context)

        submission_urn = score_set.urn
        assert submission_urn, "A valid URN is needed to link gnomAD objects for this score set."

        logging_context["current_gnomad_linking_resource"] = submission_urn
        logger.debug(msg="Fetched score set metadata for gnomAD mapped resource linkage.", extra=logging_context)

    except Exception as e:
        send_slack_error(e)
        if score_set:
            send_slack_message(text=text % score_set.urn)
        else:
            send_slack_message(text=text % score_set_id)

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error during setup. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        # We filter out mapped variants that do not have a CAID, so this query is typed # as a Sequence[str]. Ignore MyPy's type checking here.
        variant_caids: Sequence[str] = db.scalars(
            select(MappedVariant.clingen_allele_id)
            .join(Variant)
            .join(ScoreSet)
            .where(
                ScoreSet.urn == score_set.urn,
                MappedVariant.current.is_(True),
                MappedVariant.clingen_allele_id.is_not(None),
            )
        ).all()  # type: ignore
        num_variant_caids = len(variant_caids)

        logging_context["num_variants_to_link_gnomad"] = num_variant_caids

        if not variant_caids:
            logger.warning(
                msg="No current mapped variants with CAIDs were found for this score set. Skipping gnomAD linkage (nothing to do).",
                extra=logging_context,
            )

            return {"success": True, "retried": False, "enqueued_job": None}

        logger.info(
            msg="Found current mapped variants with CAIDs for this score set. Attempting to link them to gnomAD variants.",
            extra=logging_context,
        )

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="gnomAD mapped resource linkage encountered an unexpected error while attempting to build linkage urn list. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        gnomad_variant_data = gnomad_variant_data_for_caids(variant_caids)
        num_gnomad_variants_with_caid_match = len(gnomad_variant_data)
        logging_context["num_gnomad_variants_with_caid_match"] = num_gnomad_variants_with_caid_match

        if not gnomad_variant_data:
            logger.warning(
                msg="No gnomAD variants with CAID matches were found for this score set. Skipping gnomAD linkage (nothing to do).",
                extra=logging_context,
            )

            return {"success": True, "retried": False, "enqueued_job": None}

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="gnomAD mapped resource linkage encountered an unexpected error while attempting to fetch gnomAD variant data from S3 via Athena. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    try:
        logger.info(msg="Attempting to link mapped variants to gnomAD variants.", extra=logging_context)
        num_linked_gnomad_variants = link_gnomad_variants_to_mapped_variants(db, gnomad_variant_data)
        db.commit()
        logging_context["num_mapped_variants_linked_to_gnomad_variants"] = num_linked_gnomad_variants

    except Exception as e:
        send_slack_error(e)
        send_slack_message(text=text % score_set.urn)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logger.error(
            msg="LDH mapped resource linkage encountered an unexpected error while attempting to link LDH submissions. This job will not be retried.",
            extra=logging_context,
        )

        return {"success": False, "retried": False, "enqueued_job": None}

    logger.info(msg="Done linking gnomAD variants to mapped variants.", extra=logging_context)
    return {"success": True, "retried": False, "enqueued_job": None}
