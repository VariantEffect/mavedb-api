"""UniProt ID mapping jobs for protein sequence annotation.

This module handles the submission and polling of UniProt ID mapping jobs
to enrich target gene metadata with UniProt identifiers. This enables
linking of genomic variants to protein-level functional information.

The mapping process is asynchronous, requiring both submission and polling
jobs to handle the UniProt API's batch processing workflow.
"""

import logging
from typing import Optional

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.lib.exceptions import UniProtPollingEnqueueError
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata
from mavedb.lib.slack import log_and_send_slack_message, send_slack_error
from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.job_state import setup_job_state

logger = logging.getLogger(__name__)


async def submit_uniprot_mapping_jobs_for_score_set(ctx, score_set_id: int, correlation_id: Optional[str] = None):
    logging_context = {}
    score_set = None
    spawned_mapping_jobs: dict[int, Optional[str]] = {}
    text = "Could not submit mapping jobs to UniProt for this score set %s. Mapping jobs for this score set should be submitted manually."
    try:
        db: Session = ctx["db"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()
        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logger.info(msg="Started UniProt mapping job", extra=logging_context)

        if not score_set or not score_set.target_genes:
            msg = f"No target genes for score set {score_set_id}. Skipped mapping targets to UniProt."
            log_and_send_slack_message(msg=msg, ctx=logging_context, level=logging.WARNING)

            return {"success": True, "retried": False, "enqueued_jobs": []}

    except Exception as e:
        send_slack_error(e)
        if score_set:
            msg = text % score_set.urn
        else:
            msg = text % score_set_id

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        log_and_send_slack_message(msg=msg, ctx=logging_context, level=logging.ERROR)

        return {"success": False, "retried": False, "enqueued_jobs": []}

    try:
        uniprot_api = UniProtIDMappingAPI()
        logging_context["total_target_genes_to_map_to_uniprot"] = len(score_set.target_genes)
        for target_gene in score_set.target_genes:
            spawned_mapping_jobs[target_gene.id] = None  # type: ignore

            acs = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
            if not acs:
                msg = f"No accession IDs found in post_mapped_metadata for target gene {target_gene.id} in score set {score_set.urn}. This target will be skipped."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            if len(acs) != 1:
                msg = f"More than one accession ID is associated with target gene {target_gene.id} in score set {score_set.urn}. This target will be skipped."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            ac_to_map = acs[0]
            from_db = infer_db_name_from_sequence_accession(ac_to_map)

            try:
                spawned_mapping_jobs[target_gene.id] = uniprot_api.submit_id_mapping(from_db, "UniProtKB", [ac_to_map])  # type: ignore
            except Exception as e:
                log_and_send_slack_message(
                    msg=f"Failed to submit UniProt mapping job for target gene {target_gene.id}: {e}. This target will be skipped.",
                    ctx=logging_context,
                    level=logging.WARNING,
                )

    except Exception as e:
        send_slack_error(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        log_and_send_slack_message(
            msg=f"UniProt mapping job encountered an unexpected error while attempting to submit mapping jobs for score set {score_set.urn}. This job will not be retried.",
            ctx=logging_context,
            level=logging.ERROR,
        )

        return {"success": False, "retried": False, "enqueued_jobs": []}

    new_job_id = None
    try:
        successfully_spawned_mapping_jobs = sum(1 for job in spawned_mapping_jobs.values() if job is not None)
        logging_context["successfully_spawned_mapping_jobs"] = successfully_spawned_mapping_jobs

        if not successfully_spawned_mapping_jobs:
            msg = f"No UniProt mapping jobs were successfully spawned for score set {score_set.urn}. Skipped enqueuing polling job."
            log_and_send_slack_message(msg, logging_context, logging.WARNING)
            return {"success": True, "retried": False, "enqueued_jobs": []}

        new_job = await redis.enqueue_job(
            "poll_uniprot_mapping_jobs_for_score_set",
            spawned_mapping_jobs,
            score_set_id,
            correlation_id,
        )

        if new_job:
            new_job_id = new_job.job_id

            logging_context["poll_uniprot_mapping_job_id"] = new_job_id
            logger.info(msg="Enqueued polling jobs for UniProt mapping jobs.", extra=logging_context)

        else:
            raise UniProtPollingEnqueueError()

    except Exception as e:
        send_slack_error(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        log_and_send_slack_message(
            msg="UniProt mapping job encountered an unexpected error while attempting to enqueue polling jobs for mapping jobs. This job will not be retried.",
            ctx=logging_context,
            level=logging.ERROR,
        )

        return {"success": False, "retried": False, "enqueued_jobs": [job for job in [new_job_id] if job]}

    return {"success": True, "retried": False, "enqueued_jobs": [job for job in [new_job_id] if job]}


async def poll_uniprot_mapping_jobs_for_score_set(
    ctx, mapping_jobs: dict[int, Optional[str]], score_set_id: int, correlation_id: Optional[str] = None
):
    logging_context = {}
    score_set = None
    text = "Could not poll mapping jobs from UniProt for this Target %s. Mapping jobs for this score set should be submitted manually."
    try:
        db: Session = ctx["db"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()
        logging_context = setup_job_state(ctx, None, score_set.urn, correlation_id)
        logger.info(msg="Started UniProt polling job", extra=logging_context)

        if not score_set or not score_set.target_genes:
            msg = f"No target genes for score set {score_set_id}. Skipped polling targets for UniProt mapping results."
            log_and_send_slack_message(msg=msg, ctx=logging_context, level=logging.WARNING)

            return {"success": True, "retried": False, "enqueued_jobs": []}

    except Exception as e:
        send_slack_error(e)
        if score_set:
            msg = text % score_set.urn
        else:
            msg = text % score_set_id

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        log_and_send_slack_message(msg=msg, ctx=logging_context, level=logging.ERROR)

        return {"success": False, "retried": False, "enqueued_jobs": []}

    try:
        uniprot_api = UniProtIDMappingAPI()
        for target_gene in score_set.target_genes:
            acs = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
            if not acs:
                msg = f"No accession IDs found in post_mapped_metadata for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            if len(acs) != 1:
                msg = f"More than one accession ID is associated with target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            mapped_ac = acs[0]
            job_id = mapping_jobs.get(target_gene.id)  # type: ignore

            if not job_id:
                msg = f"No job ID found for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
                # This issue has already been sent to Slack in the job submission function, so we just log it here.
                logger.debug(msg=msg, extra=logging_context)
                continue

            if not uniprot_api.check_id_mapping_results_ready(job_id):
                msg = f"Job {job_id} not ready for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target"
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            results = uniprot_api.get_id_mapping_results(job_id)
            mapped_ids = uniprot_api.extract_uniprot_id_from_results(results)

            if not mapped_ids:
                msg = f"No UniProt ID found for target gene {target_gene.id} in score set {score_set.urn}. Cannot add UniProt ID for this target."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            if len(mapped_ids) != 1:
                msg = f"Found ambiguous Uniprot ID mapping results for target gene {target_gene.id} in score set {score_set.urn}. Cannot add UniProt ID for this target."
                log_and_send_slack_message(msg, logging_context, logging.WARNING)
                continue

            mapped_uniprot_id = mapped_ids[0][mapped_ac]["uniprot_id"]
            target_gene.uniprot_id_from_mapped_metadata = mapped_uniprot_id
            db.add(target_gene)
            logger.info(
                msg=f"Updated target gene {target_gene.id} with UniProt ID {mapped_uniprot_id}", extra=logging_context
            )

    except Exception as e:
        send_slack_error(e)
        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        log_and_send_slack_message(
            msg="UniProt mapping job encountered an unexpected error while attempting to poll mapping jobs. This job will not be retried.",
            ctx=logging_context,
            level=logging.ERROR,
        )

        return {"success": False, "retried": False, "enqueued_jobs": []}

    db.commit()
    return {"success": True, "retried": False, "enqueued_jobs": []}
