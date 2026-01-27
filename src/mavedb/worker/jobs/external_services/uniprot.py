"""UniProt ID mapping jobs for protein sequence annotation.

This module handles the submission and polling of UniProt ID mapping jobs
to enrich target gene metadata with UniProt identifiers. This enables
linking of genomic variants to protein-level functional information.

The mapping process is asynchronous, requiring both submission and polling
jobs to handle the UniProt API's batch processing workflow.
"""

import logging
from typing import Optional, TypedDict

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from mavedb.lib.exceptions import (
    NonExistentTargetGeneError,
    UniprotAmbiguousMappingResultError,
    UniprotMappingResultNotFoundError,
    UniProtPollingEnqueueError,
)
from mavedb.lib.mapping import extract_ids_from_post_mapped_metadata
from mavedb.lib.uniprot.id_mapping import UniProtIDMappingAPI
from mavedb.lib.uniprot.utils import infer_db_name_from_sequence_accession
from mavedb.models.job_dependency import JobDependency
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


class MappingJob(TypedDict):
    job_id: Optional[str]
    accession: str


@with_pipeline_management
async def submit_uniprot_mapping_jobs_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Submit UniProt ID mapping jobs for all target genes in a given ScoreSet.

    NOTE: This function assumes that a dependent polling job has already been created
    for the same ScoreSet. It is the responsibility of this function to ensure that
    the polling job exists and to set the `mapping_jobs` parameter on the polling job.

    Without running the polling job, the results of the submitted UniProt mapping jobs
    will never be retrieved or processed, so running this function alone is insufficient
    to complete the UniProt mapping workflow.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing target genes to map.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Submits UniProt ID mapping jobs for each target gene in the ScoreSet.
        - Fetches the dependent job for this function, which is the polling job for UniProt results.
          Sets the parameter `mapping_jobs` on the polling job with a dictionary of target gene IDs to UniProt job IDs.
          TODO#XXX: Split mapping jobs into one per target gene so that polling can be more granular.

    Raises:
        - UniProtPollingEnqueueError: If the dependent polling job cannot be found.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

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

    # Preset submitted jobs metadata so it persists even if no jobs are submitted.
    job.metadata_["submitted_jobs"] = {}
    job_manager.db.commit()

    if not score_set.target_genes:
        job_manager.update_progress(100, 100, "No target genes found. Skipped UniProt mapping job submission.")
        logger.error(
            msg=f"No target genes found for score set {score_set.urn}. Skipped UniProt mapping job submission.",
            extra=job_manager.logging_context(),
        )

        return {"status": "ok", "data": {}, "exception_details": None}

    uniprot_api = UniProtIDMappingAPI()
    job_manager.save_to_context({"total_target_genes_to_map_to_uniprot": len(score_set.target_genes)})

    mapping_jobs: dict[str, MappingJob] = {}
    for idx, target_gene in enumerate(score_set.target_genes):
        acs = extract_ids_from_post_mapped_metadata(target_gene.post_mapped_metadata)  # type: ignore
        if not acs:
            logger.warning(
                msg=f"No accession IDs found in post_mapped_metadata for target gene {target_gene.id} in score set {score_set.urn}. Skipped mapping this target.",
                extra=job_manager.logging_context(),
            )
            continue

        if len(acs) != 1:
            logger.warning(
                msg=f"More than one accession ID is associated with target gene {target_gene.id} in score set {score_set.urn}. Skipped mapping this target.",
                extra=job_manager.logging_context(),
            )
            continue

        ac_to_map = acs[0]
        from_db = infer_db_name_from_sequence_accession(ac_to_map)
        spawned_job = uniprot_api.submit_id_mapping(from_db, "UniProtKB", [ac_to_map])  # type: ignore

        # Explicitly cast ints to strs in mapping job keys. These are converted to strings internally
        # by SQLAlchemy when storing job_params as JSON, so be explicit here to avoid confusion.
        mapping_jobs[str(target_gene.id)] = {"job_id": spawned_job, "accession": ac_to_map}

        job_manager.save_to_context(
            {
                "submitted_uniprot_mapping_jobs": {
                    **job_manager.logging_context().get("submitted_uniprot_mapping_jobs", {}),
                    str(target_gene.id): mapping_jobs[str(target_gene.id)],
                }
            }
        )
        job_manager.update_progress(
            int((idx + 1 / len(score_set.target_genes)) * 95),
            100,
            f"Submitted UniProt mapping job for target gene {target_gene.name}.",
        )
        logger.info(
            msg=f"Submitted UniProt ID mapping job for target gene {target_gene.id}.",
            extra=job_manager.logging_context(),
        )

    # Save submitted jobs to job metadata for auditing purposes
    job.metadata_["submitted_jobs"] = mapping_jobs
    flag_modified(job, "metadata_")
    job_manager.db.commit()

    # If no mapping jobs were submitted, log and exit early.
    if not mapping_jobs or not any((job_info["job_id"] for job_info in mapping_jobs.values())):
        job_manager.update_progress(100, 100, "No UniProt mapping jobs were submitted.")
        logger.warning(msg="No UniProt mapping jobs were submitted.", extra=job_manager.logging_context())

        return {"status": "ok", "data": {}, "exception_details": None}

    # It's an essential responsibility of the submit job (when submissions exist) to ensure that the polling job exists.
    dependent_polling_job = job_manager.db.scalars(
        select(JobDependency).where(JobDependency.depends_on_job_id == job.id)
    ).all()
    if not dependent_polling_job or len(dependent_polling_job) != 1:
        job_manager.update_progress(100, 100, "Failed to submit UniProt mapping jobs.")
        logger.error(
            msg=f"Could not find unique dependent polling job for UniProt mapping job {job.id}.",
            extra=job_manager.logging_context(),
        )

        raise UniProtPollingEnqueueError(
            f"Could not find unique dependent polling job for UniProt mapping job {job.id}."
        )

    # Set mapping jobs on dependent polling job. Only one polling job per score set should be created.
    polling_job = dependent_polling_job[0].job_run
    polling_job.job_params = {
        **(polling_job.job_params or {}),
        "mapping_jobs": mapping_jobs,
    }

    job_manager.update_progress(100, 100, "Completed submission of UniProt mapping jobs.")
    logger.info(msg="Completed UniProt mapping job submission", extra=job_manager.logging_context())
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}


@with_pipeline_management
async def poll_uniprot_mapping_jobs_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """Submit UniProt ID mapping jobs for all target genes in a given ScoreSet.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing target genes to map.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - mapping_jobs (dict): Dictionary of target gene IDs to UniProt job IDs.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being processed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Side Effects:
        - Polls UniProt ID mapping jobs for each target gene in the ScoreSet.
        - Updates target genes with mapped UniProt IDs in the database.

    TODO#XXX: Split mapping jobs into one per target gene so that polling can be more granular.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id", "mapping_jobs"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    mapping_jobs: dict[str, MappingJob] = job.job_params.get("mapping_jobs", {})  # type: ignore

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

    if not mapping_jobs or not any(mapping_jobs.values()):
        job_manager.update_progress(100, 100, "No mapping jobs found to poll.")
        logger.warning(
            msg=f"No mapping jobs found in job parameters for polling UniProt mapping jobs for score set {score_set.urn}.",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception_details": None}

    # Poll each mapping job and update target genes with UniProt IDs
    uniprot_api = UniProtIDMappingAPI()
    for target_gene_id, mapping_job in mapping_jobs.items():
        mapping_job_id = mapping_job["job_id"]

        if not mapping_job_id:
            logger.warning(
                msg=f"No UniProt mapping job ID found for target gene ID {target_gene_id}. Skipped polling this job.",
                extra=job_manager.logging_context(),
            )
            continue

        # Check if the mapping job is ready
        if not uniprot_api.check_id_mapping_results_ready(mapping_job_id):
            logger.warning(
                msg=f"Job {mapping_job_id} not ready. Skipped polling this job.",
                extra=job_manager.logging_context(),
            )
            # TODO#XXX: When results are not ready, we want to signal to the manager a desire to retry
            #           this polling job later. For now, we just skip and log.
            continue

        # Extract mapped UniProt IDs from results
        results = uniprot_api.get_id_mapping_results(mapping_job_id)
        mapped_ids = uniprot_api.extract_uniprot_id_from_results(results)
        mapped_ac = mapping_job["accession"]

        # Handle cases where no or ambiguous results are found
        if not mapped_ids:
            msg = f"No UniProt ID found for accession {mapped_ac}. Cannot add UniProt ID."
            job_manager.update_progress(100, 100, msg)
            logger.error(msg=msg, extra=job_manager.logging_context())
            raise UniprotMappingResultNotFoundError()

        if len(mapped_ids) != 1:
            msg = f"Ambiguous UniProt ID mapping results for accession {mapped_ac}. Cannot add UniProt ID."
            job_manager.update_progress(100, 100, msg)
            logger.error(msg=msg, extra=job_manager.logging_context())
            raise UniprotAmbiguousMappingResultError()

        mapped_uniprot_id = mapped_ids[0][mapped_ac]["uniprot_id"]

        # Update target gene with mapped UniProt ID
        target_gene = next(
            (tg for tg in score_set.target_genes if str(tg.id) == str(target_gene_id)),
            None,
        )
        if not target_gene:
            msg = f"Target gene ID {target_gene_id} not found in score set {score_set.urn}. Cannot add UniProt ID."
            job_manager.update_progress(100, 100, msg)
            logger.error(msg=msg, extra=job_manager.logging_context())
            raise NonExistentTargetGeneError()

        target_gene.uniprot_id_from_mapped_metadata = mapped_uniprot_id
        job_manager.db.add(target_gene)
        logger.info(
            msg=f"Updated target gene {target_gene.id} with UniProt ID {mapped_uniprot_id}",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(
            int((list(score_set.target_genes).index(target_gene) + 1) / len(score_set.target_genes) * 95),
            100,
            f"Polled UniProt mapping job for target gene {target_gene.name}.",
        )

    job_manager.update_progress(100, 100, "Completed polling of UniProt mapping jobs.")
    job_manager.db.commit()
    return {"status": "ok", "data": {}, "exception_details": None}
