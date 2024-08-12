import logging

import pandas as pd
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import delete, select, null
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, dump_context
from mavedb.lib.slack import send_slack_message
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.dataframe import (
    validate_and_standardize_dataframe_pair,
)
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)


def setup_job_state(ctx, invoker: int, resource: str, correlation_id: str):
    ctx["state"][ctx["job_id"]] = {
        "application": "mavedb-worker",
        "user": invoker,
        "resource": resource,
        "correlation_id": correlation_id,
    }
    return ctx["state"][ctx["job_id"]]


async def create_variants_for_score_set(
    ctx, correlation_id: str, score_set_urn: str, updater_id: int, scores: pd.DataFrame, counts: pd.DataFrame
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    try:
        log_ctx = setup_job_state(ctx, updater_id, score_set_urn, correlation_id)
        logger.info(dump_context(message="Began processing of score set variants.", local_ctx=log_ctx))

        db: Session = ctx["db"]
        hdp: RESTDataProvider = ctx["hdp"]

        score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
        updated_by = db.scalars(select(User).where(User.id == updater_id)).one()

        score_set.modified_by = updated_by
        score_set.processing_state = ProcessingState.processing
        log_ctx["processing_state"] = score_set.processing_state.name

        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        if not score_set.target_genes:
            logger.warning(
                dump_context(
                    message="No targets are associated with this score set; could not create variants.",
                    local_ctx=log_ctx,
                )
            )
            raise ValueError("Can't create variants when score set has no targets.")

        if score_set.variants:
            db.execute(delete(Variant).where(Variant.score_set_id == score_set.id))
            log_ctx["deleted_variants"] = score_set.num_variants
            score_set.num_variants = 0

            logger.info(dump_context(message="Deleted existing variants from score set.", local_ctx=log_ctx))

            db.commit()
            db.refresh(score_set)

        validated_scores, validated_counts = validate_and_standardize_dataframe_pair(
            scores, counts, score_set.target_genes, hdp
        )

        score_set.dataset_columns = {
            "score_columns": columns_for_dataset(validated_scores),
            "count_columns": columns_for_dataset(validated_counts),
        }

        variants_data = create_variants_data(validated_scores, validated_counts, None)
        create_variants(db, score_set, variants_data)

    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}

        log_ctx = {**log_ctx, **format_raised_exception_info_as_dict(e)}
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.warning(
            dump_context(message="Encountered a validation error while processing variants.", local_ctx=log_ctx)
        )

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": []}

        log_ctx = {**log_ctx, **format_raised_exception_info_as_dict(e)}
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.warning(
            dump_context(message="Encountered an internal exception while processing variants.", local_ctx=log_ctx)
        )
        send_slack_message(err=e)

    # Catch all other exceptions and raise them. The exceptions caught here will be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        db.commit()

        log_ctx = {**log_ctx, **format_raised_exception_info_as_dict(e)}
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.error(
            dump_context(
                message="Encountered an unhandled exception while creating variants for score set.", local_ctx=log_ctx
            )
        )
        raise e

    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()

        log_ctx["created_variants"] = score_set.num_variants
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.info(dump_context(message="Finished creating variants in score set.", local_ctx=log_ctx))

    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)
        logger.info(dump_context(message="Committed new variants to score set.", local_ctx=log_ctx))

    ctx["state"][ctx["job_id"]] = log_ctx.copy()
    return score_set.processing_state.name
