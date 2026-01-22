"""UniProt ID mapping jobs for protein sequence annotation.

This module handles the submission and polling of UniProt ID mapping jobs
to enrich target gene metadata with UniProt identifiers. This enables
linking of genomic variants to protein-level functional information.

The mapping process is asynchronous, requiring both submission and polling
jobs to handle the UniProt API's batch processing workflow.
"""

import logging

from sqlalchemy import select

from mavedb.lib.exceptions import UniProtPollingEnqueueError
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata
from mavedb.lib.slack import log_and_send_slack_message
from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession
from mavedb.models.job_dependency import JobDependency
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def submit_uniprot_mapping_jobs_for_score_set(ctx: dict, job_manager: JobManager) -> JobResultData:
    """Submit UniProt ID mapping jobs for all target genes in a given ScoreSet.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing target genes to map.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Submits UniProt ID mapping jobs for each target gene in the ScoreSet.
        - Fetches the dependent job for this function, which is the polling job for UniProt results.
          Sets the parameter `mapping_jobs` on the polling job with a dictionary of target gene IDs to UniProt job IDs.
          TODO#XXX: Split mapping jobs into one per target gene so that polling can be more granular.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(job_manager, _job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "submit_uniprot_mapping_jobs_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting UniProt mapping job submission.")
    logger.info(msg="Started UniProt mapping job submission", extra=job_manager.logging_context())

    if not score_set or not score_set.target_genes:
        job_manager.update_progress(100, 100, "No target genes found. Skipped UniProt mapping job submission.")
        msg = f"No target genes for score set {score_set.id}. Skipped mapping targets to UniProt."
        log_and_send_slack_message(msg=msg, ctx=job_manager.logging_context(), level=logging.WARNING)
        return {"status": "ok", "data": {}, "exception_details": None}

    uniprot_api = UniProtIDMappingAPI()
    job_manager.save_to_context({"total_target_genes_to_map_to_uniprot": len(score_set.target_genes)})

    mapping_jobs = {}
    for idx, target_gene in enumerate(score_set.target_genes):
        acs = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
        if not acs:
            msg = f"No accession IDs found in post_mapped_metadata for target gene {target_gene.id} in score set {score_set.urn}. This target will be skipped."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        if len(acs) != 1:
            msg = f"More than one accession ID is associated with target gene {target_gene.id} in score set {score_set.urn}. This target will be skipped."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        ac_to_map = acs[0]
        from_db = infer_db_name_from_sequence_accession(ac_to_map)
        spawned_job = uniprot_api.submit_id_mapping(from_db, "UniProtKB", [ac_to_map])  # type: ignore
        mapping_jobs[target_gene.id] = {"job_id": spawned_job, "accession_mapped": ac_to_map}

        job_manager.save_to_context(
            {
                "submitted_uniprot_mapping_jobs": {
                    **job_manager.logging_context().get("submitted_uniprot_mapping_jobs", {}),
                    target_gene.id: mapping_jobs[target_gene.id],
                }
            }
        )
        logger.info(
            msg=f"Submitted UniProt ID mapping job for target gene {target_gene.id}.",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(
            int((idx + 1 / len(score_set.target_genes)) * 100),
            100,
            f"Submitted UniProt mapping job for target gene {target_gene.name}.",
        )

    # Set mapping jobs on dependent polling job. Only one polling job per score set should be created.
    dependent_polling_job = job_manager.db.scalars(
        select(JobDependency).where(JobDependency.depends_on_job_id == job.id)
    ).all()

    if not dependent_polling_job or len(dependent_polling_job) != 1:
        raise UniProtPollingEnqueueError(
            f"Could not find unique dependent polling job for UniProt mapping job {job.id}."
        )

    polling_job = dependent_polling_job[0].job_run
    polling_job.job_params = {
        **(polling_job.job_params or {}),
        "mapping_jobs": {
            target_gene_id: mapping_info["job_id"] for target_gene_id, mapping_info in mapping_jobs.items()
        },
    }
    job_manager.db.add(polling_job)
    job_manager.update_progress(100, 100, "Completed submission of UniProt mapping jobs.")
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}


@with_pipeline_management
async def poll_uniprot_mapping_jobs_for_score_set(ctx: dict, job_manager: JobManager) -> JobResultData:
    """Submit UniProt ID mapping jobs for all target genes in a given ScoreSet.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing target genes to map.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - mapping_jobs (dict): Dictionary of target gene IDs to UniProt job IDs.

    Args:
        ctx (dict): The job context dictionary.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    TODO#XXX: Split mapping jobs into one per target gene so that polling can be more granular.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id", "mapping_jobs"]
    validate_job_params(job_manager, _job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    mapping_jobs = job.job_params.get("mapping_jobs", {})  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "poll_uniprot_mapping_jobs_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting UniProt mapping job polling.")
    logger.info(msg="Started UniProt mapping job polling", extra=job_manager.logging_context())

    if not score_set or not score_set.target_genes:
        msg = f"No target genes for score set {score_set.id}. Skipped polling targets for UniProt mapping results."
        log_and_send_slack_message(msg=msg, ctx=job_manager.logging_context(), level=logging.WARNING)

        return {"status": "ok", "data": {}, "exception_details": None}

    # Poll each mapping job and update target genes with UniProt IDs
    uniprot_api = UniProtIDMappingAPI()
    for target_gene in score_set.target_genes:
        acs = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
        if not acs:
            msg = f"No accession IDs found in post_mapped_metadata for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        if len(acs) != 1:
            msg = f"More than one accession ID is associated with target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        mapped_ac = acs[0]
        job_id = mapping_jobs.get(target_gene.id)  # type: ignore

        if not job_id:
            msg = f"No job ID found for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target."
            # This issue has already been sent to Slack in the job submission function, so we just log it here.
            logger.debug(msg=msg, extra=job_manager.logging_context())
            continue

        if not uniprot_api.check_id_mapping_results_ready(job_id):
            msg = f"Job {job_id} not ready for target gene {target_gene.id} in score set {score_set.urn}. Skipped polling this target"
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        results = uniprot_api.get_id_mapping_results(job_id)
        mapped_ids = uniprot_api.extract_uniprot_id_from_results(results)

        if not mapped_ids:
            msg = f"No UniProt ID found for target gene {target_gene.id} in score set {score_set.urn}. Cannot add UniProt ID for this target."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        if len(mapped_ids) != 1:
            msg = f"Found ambiguous Uniprot ID mapping results for target gene {target_gene.id} in score set {score_set.urn}. Cannot add UniProt ID for this target."
            log_and_send_slack_message(msg, job_manager.logging_context(), logging.WARNING)
            continue

        mapped_uniprot_id = mapped_ids[0][mapped_ac]["uniprot_id"]
        target_gene.uniprot_id_from_mapped_metadata = mapped_uniprot_id
        job_manager.db.add(target_gene)
        logger.info(
            msg=f"Updated target gene {target_gene.id} with UniProt ID {mapped_uniprot_id}",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(
            int((list(score_set.target_genes).index(target_gene) + 1 / len(score_set.target_genes)) * 100),
            100,
            f"Polled UniProt mapping job for target gene {target_gene.name}.",
        )

    job_manager.update_progress(100, 100, "Completed polling of UniProt mapping jobs.")
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}
