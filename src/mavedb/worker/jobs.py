import logging
import json

import pandas as pd
from cdot.hgvs.dataproviders import RESTDataProvider
from sqlalchemy import delete, select, null
from sqlalchemy.orm import Session

from mavedb.lib.score_sets import (
    columns_for_dataset,
    create_variants,
    create_variants_data,
)
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


def setup_job_state(ctx, invoker: int, resource: str, interaction_id: str):
    ctx["state"][ctx["job_id"]] = {
        "application": "mavedb-worker",
        "user": invoker,
        "resource": resource,
        "interaction_id": interaction_id,
    }
    return ctx["state"][ctx["job_id"]]


async def create_variants_for_score_set(
    ctx, interaction_id: str, score_set_urn: str, updater_id: int, scores: pd.DataFrame, counts: pd.DataFrame
):
    """
    Create variants for a score set. Intended to be run within a worker.
    On any raised exception, ensure ProcessingState of score set is set to `failed` prior
    to exiting.
    """
    log_ctx = setup_job_state(ctx, updater_id, score_set_urn, interaction_id)
    logger.info(f"Began processing of score set variants. {json.dumps(log_ctx)}")

    try:
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
                f"No targets are associated with this score set; could not create variants. {json.dumps(log_ctx)}"
            )
            raise ValueError("Can't create variants when score set has no targets.")

        if score_set.variants:
            db.execute(delete(Variant).where(Variant.score_set_id == score_set.id))
            log_ctx["deleted_variants"] = score_set.num_variants
            score_set.num_variants = 0

            logger.info(f"Deleted existing variants from score set. {json.dumps(log_ctx)}")

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
        created_variants = create_variants(db, score_set, variants_data)

    # Validation errors arise from problematic user data. These should be inserted into the database so failures can
    # be persisted to them.
    except ValidationError as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        score_set.processing_errors = {"exception": str(e), "detail": e.triggering_exceptions}

        log_ctx["validation_error"] = str(e)
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.warning(f"Encountered a validation error while processing variants. {json.dumps(log_ctx)}")

    # NOTE: Since these are likely to be internal errors, it makes less sense to add them to the DB and surface them to the end user.
    # Catch all non-system exiting exceptions.
    except Exception as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        logger.error(f"Encountered an exception while processing variants for {score_set.urn}", exc_info=e)
        score_set.processing_errors = {"exception": str(e), "detail": []}

        log_ctx["exception"] = str(e)
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.warning(f"Encountered an internal exception while processing variants. {json.dumps(log_ctx)}")

        send_slack_message(err=e)

    # Catch all other exceptions and raise them. The exceptions caught here will be system exiting.
    except BaseException as e:
        db.rollback()
        score_set.processing_state = ProcessingState.failed
        db.commit()

        log_ctx["exception"] = str(e)
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.error(f"Encountered an unhandled exception while creating variants for score set. {json.dumps(log_ctx)}")
        raise e
    else:
        score_set.processing_state = ProcessingState.success
        score_set.processing_errors = null()

        log_ctx["created_variants"] = created_variants
        log_ctx["processing_state"] = score_set.processing_state.name
        logger.info(f"Finished creating variants in score set. {json.dumps(log_ctx)}")
    finally:
        db.add(score_set)
        db.commit()
        db.refresh(score_set)

        logger.info(f"Committed new variants to score set. {json.dumps(log_ctx)}")

    return score_set.processing_state.name
