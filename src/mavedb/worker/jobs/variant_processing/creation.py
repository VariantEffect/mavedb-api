"""Variant creation jobs for score sets.

This module contains jobs responsible for creating and validating variants
from uploaded score and count data. It handles the full variant creation
pipeline including data validation, standardization, and database persistence.
"""

import logging
from typing import Optional

import pandas as pd
from arq import ArqRedis
from sqlalchemy import delete, null, select
from sqlalchemy.orm import Session

from mavedb.data_providers.services import RESTDataProvider
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.score_sets import columns_for_dataset, create_variants, create_variants_data
from mavedb.lib.slack import send_slack_error
from mavedb.lib.validation.dataframe.dataframe import validate_and_standardize_dataframe_pair
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.view_models.score_set_dataset_columns import DatasetColumnMetadata
from mavedb.worker.jobs.utils.constants import MAPPING_QUEUE_NAME
from mavedb.worker.jobs.utils.job_state import setup_job_state

logger = logging.getLogger(__name__)


async def create_variants_for_score_set(
    ctx,
    correlation_id: str,
    score_set_id: int,
    updater_id: int,
    scores: pd.DataFrame,
    counts: pd.DataFrame,
    score_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
    count_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None,
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    logging_context = {}
    try:
        db: Session = ctx["db"]
        hdp: RESTDataProvider = ctx["hdp"]
        redis: ArqRedis = ctx["redis"]
        score_set = db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

        logging_context = setup_job_state(ctx, updater_id, score_set.urn, correlation_id)
        logger.info(msg="Began processing of score set variants.", extra=logging_context)

        updated_by = db.scalars(select(User).where(User.id == updater_id)).one()

        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing
        score_set.mapping_state = MappingState.pending_variant_processing
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name

        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        if not score_set.target_genes:
            logger.warning(
                msg="No targets are associated with this score set; could not create variants.",
                extra=logging_context,
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

        # Delete variants after validation occurs so we don't overwrite them in the case of a bad update.
        if score_set.variants:
            existing_variants = db.scalars(select(Variant.id).where(Variant.score_set_id == score_set.id)).all()
            db.execute(delete(MappedVariant).where(MappedVariant.variant_id.in_(existing_variants)))
            db.execute(delete(Variant).where(Variant.id.in_(existing_variants)))
            logging_context["deleted_variants"] = score_set.num_variants
            score_set.num_variants = 0

            logger.info(msg="Deleted existing variants from score set.", extra=logging_context)

            db.flush()
            db.refresh(score_set)

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(db, score_set, variants_data)

    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}
        score_set.mapping_state = MappingState.not_attempted

        if score_set.num_variants:
            score_set.processing_errors["exception"] = (
                f"Update failed, variants were not updated. {score_set.processing_errors.get('exception', '')}"
            )

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.warning(msg="Encountered a validation error while processing variants.", extra=logging_context)

        return {"success": False}

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": []}
        score_set.mapping_state = MappingState.not_attempted

        if score_set.num_variants:
            score_set.processing_errors["exception"] = (
                f"Update failed, variants were not updated. {score_set.processing_errors.get('exception', '')}"
            )

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.warning(msg="Encountered an internal exception while processing variants.", extra=logging_context)

        send_slack_error(err=e)
        return {"success": False}

    # Catch all other exceptions. The exceptions caught here were intented to be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.mapping_state = MappingState.not_attempted
        db.commit()

        logging_context = {**logging_context, **format_raised_exception_info_as_dict(e)}
        logging_context["processing_state"] = score_set.processing_state.name
        logging_context["mapping_state"] = score_set.mapping_state.name
        logging_context["created_variants"] = 0
        logger.error(
            msg="Encountered an unhandled exception while creating variants for score set.", extra=logging_context
        )

        # Don't raise BaseExceptions so we may emit canonical logs (TODO: Perhaps they are so problematic we want to raise them anyway).
        return {"success": False}

    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()

        logging_context["created_variants"] = score_set.num_variants
        logging_context["processing_state"] = score_set.processing_state.name
        logger.info(msg="Finished creating variants in score set.", extra=logging_context)

        await redis.lpush(MAPPING_QUEUE_NAME, score_set.id)  # type: ignore
        await redis.enqueue_job("variant_mapper_manager", correlation_id, updater_id)
        score_set.mapping_state = MappingState.queued
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)
        logger.info(msg="Committed new variants to score set.", extra=logging_context)

    ctx["state"][ctx["job_id"]] = logging_context.copy()
    return {"success": True}
