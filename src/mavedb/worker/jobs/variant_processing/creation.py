"""Variant creation jobs for score sets.

This module contains jobs responsible for creating and validating variants
from uploaded score and count data. It handles the full variant creation
pipeline including data validation, standardization, and database persistence.
"""

import logging

from sqlalchemy import delete, null, select

from mavedb.data_providers.services import RESTDataProvider
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.score_sets import columns_for_dataset, create_variants, create_variants_data
from mavedb.lib.validation.dataframe.dataframe import validate_and_standardize_dataframe_pair
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def create_variants_for_score_set(ctx, job_manager: JobManager) -> JobResultData:
    """
    Create variants for a given ScoreSet based on uploaded score and count data.

    Args:
        ctx: The job context dictionary.
        job_manager: Manager for job lifecycle and DB operations.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet to create variants for.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - updater_id (int): The ID of the user performing the update.
        - scores (pd.DataFrame): DataFrame containing score data.
        - counts (pd.DataFrame): DataFrame containing count data.
        - score_columns_metadata (dict): Metadata for score columns.
        - count_columns_metadata (dict): Metadata for count columns.

    Side Effects:
        - Creates Variant and MappedVariant records in the database.

    Returns:
        dict: Result indicating success and any exception details
    """
    hdp: RESTDataProvider = ctx["hdp"]

    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = [
        "score_set_id",
        "correlation_id",
        "updater_id",
        "scores",
        "counts",
        "score_columns_metadata",
        "count_columns_metadata",
    ]
    validate_job_params(job_manager, _job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    updater_id = job.job_params["updater_id"]  # type: ignore
    scores = job.job_params["scores"]  # type: ignore
    counts = job.job_params["counts"]  # type: ignore
    score_columns_metadata = job.job_params["score_columns_metadata"]  # type: ignore
    count_columns_metadata = job.job_params["count_columns_metadata"]  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "create_variants_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting variant creation job.")
    logger.info(msg="Started variant creation job", extra=job_manager.logging_context())

    updated_by = job_manager.db.scalars(select(User).where(User.id == updater_id)).one()

    # Main processing block. Handled in a try/except to ensure we can set score set state appropriately,
    # which is handled independently of the job state.
    # TODO:XXX In a future iteration, we may want to move this logic into the job manager itself for better cohesion.
    try:
        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing
        score_set.mapping_state = MappingState.pending_variant_processing

        job_manager.save_to_context(
            {"processing_state": score_set.processing_state.name, "mapping_state": score_set.mapping_state.name}
        )

        job_manager.db.add(score_set)
        job_manager.db.commit()
        job_manager.db.refresh(score_set)

        job_manager.update_progress(10, 100, "Validated score set metadata and beginning data validation.")

        if not score_set.target_genes:
            job_manager.update_progress(100, 100, "Score set has no targets; cannot create variants.")
            logger.warning(
                msg="No targets are associated with this score set; could not create variants.",
                extra=job_manager.logging_context(),
            )
            raise ValueError("Can't create variants when score set has no targets.")

        validated_scores, validated_counts, validated_score_columns_metadata, validated_count_columns_metadata = (
            validate_and_standardize_dataframe_pair(
                scores_df=scores,
                counts_df=counts,
                score_columns_metadata=score_columns_metadata,
                count_columns_metadata=count_columns_metadata,
                targets=score_set.target_genes,
                hdp=hdp,
            )
        )

        job_manager.update_progress(80, 100, "Data validation complete; creating variants in database.")

        score_set.dataset_columns = {
            "score_columns": columns_for_dataset(validated_scores),
            "count_columns": columns_for_dataset(validated_counts),
            "score_columns_metadata": validated_score_columns_metadata
            if validated_score_columns_metadata is not None
            else {},
            "count_columns_metadata": validated_count_columns_metadata
            if validated_count_columns_metadata is not None
            else {},
        }

        job_manager.update_progress(90, 100, "Creating variants in database.")

        # Delete variants after validation occurs so we don't overwrite them in the case of a bad update.
        if score_set.variants:
            existing_variants = job_manager.db.scalars(
                select(Variant.id).where(Variant.score_set_id == score_set.id)
            ).all()
            job_manager.db.execute(delete(MappedVariant).where(MappedVariant.variant_id.in_(existing_variants)))
            job_manager.db.execute(delete(Variant).where(Variant.id.in_(existing_variants)))

            job_manager.save_to_context({"deleted_variants": len(existing_variants)})
            score_set.num_variants = 0

            logger.info(msg="Deleted existing variants from score set.", extra=job_manager.logging_context())

            job_manager.db.flush()
            job_manager.db.refresh(score_set)

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(job_manager.db, score_set, variants_data)

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all exceptions so we can log them and set score set state appropriately.
    except Exception as e:
        job_manager.db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": []}
        score_set.mapping_state = MappingState.not_attempted

        if score_set.num_variants:
            score_set.processing_errors["exception"] = (
                f"Update failed, variants were not updated. {score_set.processing_errors.get('exception', '')}"
            )

        job_manager.save_to_context(
            {
                "processing_state": score_set.processing_state.name,
                "mapping_state": score_set.mapping_state.name,
                **format_raised_exception_info_as_dict(e),
                "created_variants": 0,
            }
        )
        job_manager.update_progress(100, 100, "Variant creation job failed due to an internal error.")
        logger.error(
            msg="Encountered an internal exception while processing variants.", extra=job_manager.logging_context()
        )

        raise e

    else:
        score_set.processing_state = ProcessingState.success
        score_set.mapping_state = MappingState.queued
        score_set.processing_errors = null()

        job_manager.save_to_context(
            {
                "processing_state": score_set.processing_state.name,
                "mapping_state": score_set.mapping_state.name,
                "created_variants": score_set.num_variants,
            }
        )

    finally:
        job_manager.db.add(score_set)
        job_manager.db.commit()
        job_manager.db.refresh(score_set)

        job_manager.update_progress(100, 100, "Completed variant creation job.")
        logger.info(msg="Committed new variants to score set.", extra=job_manager.logging_context())

    return {"status": "ok", "data": {}, "exception_details": None}
